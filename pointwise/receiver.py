"""Receiver library: idempotent ingestion behind a fencing check on every batch.

Invariants
  * Before any batch is accepted, the credential token is validated with the
    scheduler. A stale/retired token => 409 and nothing is written. An old
    process therefore cannot submit progress for work it no longer owns.
  * (shard_id, record_id) is the primary key. Re-delivered records are counted as
    duplicates and never as new inserts, however often they are fetched.
  * Completeness is proven by reconciliation against the SOURCE manifest:
    exact count + checksum match and an explicit missing-id list, not by cursors.
"""
from __future__ import annotations
import os as _os
import sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))

import argparse
import hashlib
import logging
from http.server import ThreadingHTTPServer
from urllib.parse import urlparse

from common import DB, Router, gen_id, http, install_signal_trace, now_iso, setup_logging

log = logging.getLogger("receiver")
MISSING_SAMPLE_LIMIT = 200


def payload_hash(rid: int, payload: str) -> int:
    return int(hashlib.sha256(f"{rid}|{payload}".encode()).hexdigest(), 16)


class State:
    def __init__(self, db: DB, scheduler_url: str, source_url: str):
        self.db = db
        self.scheduler_url = scheduler_url
        self.source_url = source_url
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS records (
                shard_id    TEXT NOT NULL,
                record_id   INTEGER NOT NULL,
                payload     TEXT NOT NULL,
                payload_ok  INTEGER NOT NULL,
                epoch       INTEGER NOT NULL,
                batch_id    TEXT NOT NULL,
                ingested_at TEXT NOT NULL,
                PRIMARY KEY (shard_id, record_id)
            );
            CREATE TABLE IF NOT EXISTS batches (
                id           TEXT PRIMARY KEY,
                shard_id     TEXT NOT NULL,
                epoch        INTEGER NOT NULL,
                token        TEXT NOT NULL,
                offered      INTEGER NOT NULL,
                inserted     INTEGER NOT NULL,
                duplicates   INTEGER NOT NULL,
                rejected     INTEGER NOT NULL,
                ts           TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_records_rid ON records(record_id);
            """
        )

    def ingest(self, token: str, shard_id: str, epoch: int, records: list[dict]) -> dict:
        # ---- 1. fencing: the scheduler is the sole authority on the token
        status, body = http("POST", f"{self.scheduler_url}/validate-token", {"token": token})
        if status != 200 or not body.get("valid"):
            batch_id = gen_id("bat_")
            self.db.execute(
                "INSERT INTO batches(id, shard_id, epoch, token, offered, inserted, "
                "duplicates, rejected, ts) VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)",
                (batch_id, shard_id, epoch, token, len(records), len(records), now_iso()),
            )
            log.warning("FENCED batch rejected: shard=%s epoch=%d reason=%s",
                        shard_id, epoch, body.get("error"))
            return {"status": 409, "error": "fenced", "reason": body.get("error"),
                    "batch_id": batch_id, "inserted": 0, "duplicates": 0,
                    "rejected": len(records)}

        valid_shard = body["shard_id"]
        valid_epoch = body["epoch"]
        if valid_shard != shard_id or valid_epoch != epoch:
            return {"status": 409, "error": "credential_mismatch",
                    "expected_shard": valid_shard, "expected_epoch": valid_epoch}

        # ---- 2. idempotent upsert keyed on (shard, record id)
        inserted = duplicates = malformed = 0
        batch_id = gen_id("bat_")
        for rec in records:
            rid = rec.get("id")
            payload = rec.get("payload", "")
            if not isinstance(rid, int):
                malformed += 1
                continue
            expected = hashlib.sha256(f"pointwise/record/{rid}".encode()).hexdigest()[:32]
            ok = 1 if payload == expected else 0
            before = self.db.one(
                "SELECT 1 FROM records WHERE shard_id=? AND record_id=?", (shard_id, rid))
            self.db.execute(
                "INSERT OR IGNORE INTO records(shard_id, record_id, payload, payload_ok, epoch, "
                "batch_id, ingested_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (shard_id, rid, payload, ok, valid_epoch, batch_id, now_iso()),
            )
            if before:
                duplicates += 1
            else:
                inserted += 1
        self.db.execute(
            "INSERT INTO batches(id, shard_id, epoch, token, offered, inserted, duplicates, "
            "rejected, ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (batch_id, shard_id, valid_epoch, token, len(records), inserted, duplicates,
             malformed, now_iso()),
        )
        log.info("ingest shard=%s epoch=%d offered=%d inserted=%d duplicates=%d malformed=%d",
                 shard_id, valid_epoch, len(records), inserted, duplicates, malformed)
        return {"status": 200, "batch_id": batch_id, "shard_id": shard_id,
                "epoch": valid_epoch, "offered": len(records),
                "inserted": inserted, "duplicates": duplicates, "rejected": malformed}

    def coverage(self, shard: dict) -> dict:
        lo, hi = shard["lo"], shard["hi"]
        cond, params = ["shard_id = ?"], [shard["id"]]
        if lo is not None:
            cond.append("record_id >= ?")
            params.append(lo)
        if hi is not None:
            cond.append("record_id < ?")
            params.append(hi)
        where = " AND ".join(cond)
        agg = self.db.one(
            f"SELECT COUNT(*) AS count, COALESCE(SUM(payload_ok), 0) AS payload_ok, "
            f"COALESCE(MIN(record_id), 0) AS min_id, COALESCE(MAX(record_id), 0) AS max_id "
            f"FROM records WHERE {where}", params)
        checksum = 0
        for r in self.db.query(
                f"SELECT record_id, payload FROM records WHERE {where} ORDER BY record_id", params):
            checksum = (checksum + payload_hash(r["record_id"], r["payload"])) & 0xFFFFFFFF
        # authoritative source truth for the same range
        qs = f"lo={lo}" if lo is not None else ""
        if hi is not None:
            qs += ("&" if qs else "") + f"hi={hi}"
        status, manifest = http("GET", f"{self.source_url}/manifest?{qs}")
        source = manifest if status == 200 else None
        missing, extra = [], []
        complete = False
        if source:
            have = {r["record_id"] for r in self.db.query(
                f"SELECT record_id FROM records WHERE {where}", params)}
            want = set(source["ids"])
            missing = sorted(want - have)[:MISSING_SAMPLE_LIMIT]
            extra = sorted(have - want)[:MISSING_SAMPLE_LIMIT]
            complete = (
                agg["count"] == source["count"]
                and not missing and not extra
                and f"{checksum:08x}" == source["checksum"]
                and agg["payload_ok"] == source["count"]
            )
        return {
            "shard_id": shard["id"], "range": [lo, hi],
            "stored_count": agg["count"], "payload_ok": agg["payload_ok"],
            "stored_checksum": f"{checksum:08x}",
            "stored_min_id": agg["min_id"] or None, "stored_max_id": agg["max_id"] or None,
            "source": {k: source[k] for k in ("count", "checksum", "min_id", "max_id")}
                      if source else None,
            "missing_ids_sample": missing, "extra_ids_sample": extra,
            "missing_truncated": source is not None
                                  and len(source["ids"]) - agg["count"] > len(missing),
            "complete": complete,
        }

    def global_evidence(self) -> dict:
        # Global reconciliation regardless of shard boundaries (split/merge safe).
        rows = self.db.query("SELECT DISTINCT record_id FROM records ORDER BY record_id")
        distinct_ids = [r["record_id"] for r in rows]
        checksum = 0
        for r in self.db.query(
                "SELECT record_id, payload FROM records GROUP BY record_id ORDER BY record_id"):
            checksum = (checksum + payload_hash(r["record_id"], r["payload"])) & 0xFFFFFFFF
        status, manifest = http("GET", f"{self.source_url}/manifest")
        source = manifest if status == 200 else None
        want = set(source["ids"]) if source else set()
        have = set(distinct_ids)
        return {
            "receiver_distinct_records": len(distinct_ids),
            "receiver_checksum": f"{checksum:08x}",
            "source": {k: source[k] for k in ("count", "checksum", "max_id")}
                      if source else None,
            "missing_ids_sample": sorted(want - have)[:MISSING_SAMPLE_LIMIT],
            "extra_ids_sample": sorted(have - want)[:MISSING_SAMPLE_LIMIT],
            "reconciled": bool(source) and len(have) == source["count"]
                          and not (want - have) and not (have - want)
                          and f"{checksum:08x}" == source["checksum"],
        }


def make_handler(state: State, shards_lookup):
    class Handler(Router):
        routes = [
            ("GET", r"/health", "h_health"),
            ("POST", r"/ingest", "h_ingest"),
            ("GET", r"/coverage", "h_coverage"),
            ("GET", r"/evidence", "h_evidence"),
            ("GET", r"/stats", "h_stats"),
            ("GET", r"/records", "h_records"),
            ("GET", r"/batches", "h_batches"),
        ]

        def h_health(self, url, match):
            self.send_json(200, {"ok": True, "service": "receiver"})

        def h_ingest(self, url, match):
            body = self.read_json()
            required = ("token", "shard_id", "epoch", "records")
            if any(k not in body for k in required):
                self.send_json(400, {"error": "missing_fields", "required": list(required)})
                return
            result = state.ingest(body["token"], body["shard_id"],
                                  int(body["epoch"]), body["records"])
            status = result.pop("status")
            self.send_json(status, result)

        def h_coverage(self, url, match):
            shards = shards_lookup()
            report = [state.coverage(s) for s in shards]
            self.send_json(200, {
                "coverage": report,
                "all_complete": all(r["complete"] for r in report) if report else False,
            })

        def h_evidence(self, url, match):
            self.send_json(200, state.global_evidence())

        def h_stats(self, url, match):
            totals = state.db.one(
                "SELECT COUNT(*) AS rows, COALESCE(SUM(inserted),0) AS inserted, "
                "COALESCE(SUM(duplicates),0) AS duplicates, COALESCE(SUM(rejected),0) AS rejected, "
                "COUNT(DISTINCT shard_id) AS shards FROM batches")
            distinct = state.db.one("SELECT COUNT(DISTINCT record_id) AS c FROM records")
            self.send_json(200, {"batches": totals, "distinct_records": distinct["c"]})

        def h_records(self, url, match):
            rows = state.db.query(
                "SELECT shard_id, record_id, epoch, payload_ok, ingested_at FROM records "
                "ORDER BY record_id ASC, shard_id ASC LIMIT 200")
            self.send_json(200, {"records": rows})

        def h_batches(self, url, match):
            rows = state.db.query("SELECT * FROM batches ORDER BY rowid DESC LIMIT 100")
            self.send_json(200, {"batches": rows})

    return Handler


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8093)
    ap.add_argument("--db", default="data/receiver.db")
    ap.add_argument("--scheduler-url", default="http://127.0.0.1:8092")
    ap.add_argument("--source-url", default="http://127.0.0.1:8091")
    args = ap.parse_args()
    setup_logging()
    install_signal_trace(log)
    state = State(DB(args.db), args.scheduler_url, args.source_url)

    def shards_lookup():
        status, body = http("GET", f"{args.scheduler_url}/shards")
        if status != 200:
            return []
        return [s for s in body["shards"] if s["state"] in ("active", "done", "split", "merged")]

    server = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(state, shards_lookup))
    log.info("receiver listening on %d", args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
