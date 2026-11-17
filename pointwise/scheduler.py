"""Shard scheduler: topology (split/merge), fencing tokens, leases, resume cursors.

Invariants
  * Every topology change (split / merge / new registration) bumps an epoch and
    issues a fresh credential token. Older tokens are rejected everywhere, so a
    resumed old process can never submit progress for work it no longer owns.
  * Shard ids are never reused; split/merge retires old shards and creates new ones.
  * The resume cursor is only a hint. Authority on completeness lives in the
    receiver's id-dedup store and the source's authoritative manifest.
"""
from __future__ import annotations
import os as _os
import sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))

import argparse
import logging
import threading
from http.server import ThreadingHTTPServer
from urllib.parse import urlparse

from common import DB, Router, gen_id, http, install_signal_trace, now_iso, setup_logging

log = logging.getLogger("scheduler")
LEASE_SECONDS = 30


class State:
    def __init__(self, db: DB, source_url: str):
        self.db = db
        self.source_url = source_url
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS shards (
                id            TEXT PRIMARY KEY,
                lo            INTEGER NOT NULL,
                hi            INTEGER,
                state         TEXT NOT NULL,           -- active | split | merged | done
                epoch         INTEGER NOT NULL DEFAULT 1,
                resume_cursor INTEGER NOT NULL DEFAULT 0,
                lease_owner   TEXT,
                lease_addr    TEXT,
                lease_until   REAL,
                parent_ids    TEXT NOT NULL DEFAULT '',
                created_at    TEXT NOT NULL,
                updated_at    TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS credentials (
                token        TEXT PRIMARY KEY,
                shard_id     TEXT NOT NULL,
                epoch        INTEGER NOT NULL,
                collector_id TEXT NOT NULL,
                created_at   TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                ts      TEXT NOT NULL,
                kind    TEXT NOT NULL,
                shard_id TEXT,
                detail  TEXT NOT NULL
            );
            """
        )

    def event(self, kind: str, shard_id: str | None, detail: dict):
        import json
        self.db.execute(
            "INSERT INTO events(ts, kind, shard_id, detail) VALUES (?, ?, ?, ?)",
            (now_iso(), kind, shard_id, json.dumps(detail, ensure_ascii=False)),
        )

    # ------------------------------------------------------------- queries
    def get_shard(self, shard_id: str):
        return self.db.one("SELECT * FROM shards WHERE id = ?", (shard_id,))

    def list_shards(self):
        return self.db.query("SELECT * FROM shards ORDER BY lo ASC, created_at ASC")

    def active_shards(self):
        return [s for s in self.list_shards() if s["state"] in ("active", "done")]

    # ------------------------------------------------------------- topology
    def create_shard(self, lo: int, hi: int | None) -> dict:
        sid = gen_id("sh_")
        self.db.execute(
            "INSERT INTO shards(id, lo, hi, state, epoch, created_at, updated_at) "
            "VALUES (?, ?, ?, 'active', 1, ?, ?)",
            (sid, lo, hi, now_iso(), now_iso()),
        )
        self.event("shard_created", sid, {"lo": lo, "hi": hi})
        log.info("created shard %s range=[%s,%s)", sid, lo, hi if hi is not None else "open")
        return self.get_shard(sid)

    def _source_max_id(self) -> int:
        status, body = http("GET", f"{self.source_url}/stats")
        if status != 200:
            raise RuntimeError(f"source unreachable for boundary check: {body}")
        return int(body["max_id"])

    def split(self, shard_id: str) -> tuple[dict, dict]:
        shard = self.get_shard(shard_id)
        if not shard:
            raise KeyError("shard_not_found")
        if shard["state"] not in ("active", "done"):
            raise ValueError(f"shard not splittable in state={shard['state']}")
        lo, hi = shard["lo"], shard["hi"]
        if hi is None:
            hi = self._source_max_id() + 1
        if hi - lo < 2:
            raise ValueError("range too narrow to split")
        mid = lo + (hi - lo) // 2
        parent_epoch = shard["epoch"] + 1  # fence every credential issued so far
        self.db.execute(
            "UPDATE shards SET state='split', epoch=?, lease_owner=NULL, lease_until=NULL, "
            "updated_at=? WHERE id=?",
            (parent_epoch, now_iso(), shard_id),
        )
        left = self.create_shard(lo, mid)
        right = self.create_shard(mid, hi)
        # Hint cursors: left resumes where the parent was; right starts at mid-1.
        self.db.execute("UPDATE shards SET resume_cursor=? WHERE id=?",
                        (shard["resume_cursor"], left["id"]))
        self.db.execute("UPDATE shards SET resume_cursor=? WHERE id=?", (mid - 1, right["id"]))
        self.event("split", shard_id, {
            "parent": shard_id, "parent_epoch_old": shard["epoch"],
            "parent_epoch_new": parent_epoch, "mid": mid,
            "children": [left["id"], right["id"]],
        })
        log.warning("SPLIT %s -> %s + %s at mid=%d (parent epoch -> %d)",
                    shard_id, left["id"], right["id"], mid, parent_epoch)
        return self.get_shard(left["id"]), self.get_shard(right["id"])

    def merge(self, shard_a: str, shard_b: str) -> dict:
        a, b = self.get_shard(shard_a), self.get_shard(shard_b)
        if not a or not b:
            raise KeyError("shard_not_found")
        if a["state"] not in ("active", "done") or b["state"] not in ("active", "done"):
            raise ValueError("only active/done shards can merge")
        # adjacency: a ends where b begins (or vice versa), matching open end preserved
        if not (a["hi"] == b["lo"] or b["hi"] == a["lo"]):
            raise ValueError("shards are not adjacent")
        first, second = (a, b) if a["lo"] < b["lo"] else (b, a)
        lo, hi = first["lo"], second["hi"]
        for old in (a, b):
            self.db.execute(
                "UPDATE shards SET state='merged', epoch=epoch+1, lease_owner=NULL, "
                "lease_until=NULL, updated_at=? WHERE id=?",
                (now_iso(), old["id"]),
            )
        merged = self.create_shard(lo, hi)
        # Conservative hint: earliest cursor of the two, minus 1 to avoid boundary gaps.
        cursor = max(0, min(a["resume_cursor"], b["resume_cursor"]) - 1)
        self.db.execute("UPDATE shards SET resume_cursor=? WHERE id=?", (cursor, merged["id"]))
        self.db.execute("UPDATE shards SET parent_ids=? WHERE id=?",
                        (",".join([a["id"], b["id"]]), merged["id"]))
        self.event("merge", merged["id"], {
            "merged": merged["id"], "parents": [a["id"], b["id"]],
            "range": [lo, hi], "hint_cursor": cursor,
        })
        log.warning("MERGE %s + %s -> %s range=[%s,%s)", a["id"], b["id"], merged["id"], lo, hi)
        return merged

    # ----------------------------------------------------- credentials/lease
    def register(self, shard_id: str, collector_id: str, control_addr: str) -> dict:
        shard = self.get_shard(shard_id)
        if not shard:
            raise KeyError("shard_not_found")
        if shard["state"] not in ("active", "done"):
            raise ValueError(f"shard not active (state={shard['state']})")
        new_epoch = shard["epoch"] + 1
        token = gen_id("tok_")
        self.db.execute(
            "INSERT INTO credentials(token, shard_id, epoch, collector_id, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (token, shard_id, new_epoch, collector_id, now_iso()),
        )
        lease_until = self._now() + LEASE_SECONDS
        self.db.execute(
            "UPDATE shards SET epoch=?, lease_owner=?, lease_addr=?, lease_until=?, "
            "updated_at=? WHERE id=?",
            (new_epoch, collector_id, control_addr, lease_until, now_iso(), shard_id),
        )
        self.event("register", shard_id, {
            "collector_id": collector_id, "epoch": new_epoch,
            "fenced_previous_epoch": new_epoch - 1, "lease_seconds": LEASE_SECONDS,
        })
        log.info("REGISTER collector=%s shard=%s epoch=%d (fenced <=%d)",
                 collector_id, shard_id, new_epoch, new_epoch - 1)
        return self._credential_payload(self.get_shard(shard_id), token)

    def _credential_payload(self, shard: dict, token: str) -> dict:
        return {
            "token": token,
            "shard_id": shard["id"],
            "epoch": shard["epoch"],
            "lo": shard["lo"],
            "hi": shard["hi"],
            "resume_cursor": shard["resume_cursor"],
            "lease_seconds": LEASE_SECONDS,
        }

    @staticmethod
    def _now() -> float:
        import time
        return time.time()

    def _authenticate(self, token: str) -> dict | None:
        cred = self.db.one("SELECT * FROM credentials WHERE token = ?", (token,))
        if not cred:
            return None
        shard = self.get_shard(cred["shard_id"])
        if not shard or shard["state"] not in ("active", "done"):
            return {"stale": True, "reason": "shard_retired", "credential": cred, "shard": shard}
        if cred["epoch"] != shard["epoch"]:
            return {"stale": True, "reason": "epoch_superseded",
                    "credential_epoch": cred["epoch"], "current_epoch": shard["epoch"],
                    "shard": shard}
        return {"stale": False, "credential": cred, "shard": shard}

    def heartbeat(self, token: str) -> dict:
        auth = self._authenticate(token)
        if auth is None:
            return {"status": 404, "error": "unknown_token"}
        if auth["stale"]:
            return {"status": 409, "error": auth["reason"],
                    "shard_id": auth["shard"]["id"] if auth["shard"] else None}
        shard = auth["shard"]
        lease_until = self._now() + LEASE_SECONDS
        self.db.execute("UPDATE shards SET lease_until=? WHERE id=?", (lease_until, shard["id"]))
        return {
            "status": 200,
            "shard_id": shard["id"],
            "epoch": shard["epoch"],
            "state": shard["state"],
            "resume_cursor": shard["resume_cursor"],
            "lease_until": lease_until,
        }

    def checkpoint(self, token: str, cursor: int) -> dict:
        auth = self._authenticate(token)
        if auth is None:
            return {"status": 404, "error": "unknown_token"}
        if auth["stale"]:
            return {"status": 409, "error": auth["reason"],
                    "shard_id": auth["shard"]["id"] if auth["shard"] else None}
        shard = auth["shard"]
        if cursor > shard["resume_cursor"]:
            self.db.execute(
                "UPDATE shards SET resume_cursor=?, updated_at=? WHERE id=?",
                (cursor, now_iso(), shard["id"]),
            )
        return {"status": 200, "shard_id": shard["id"], "epoch": shard["epoch"],
                "resume_cursor": max(cursor, shard["resume_cursor"])}

    def validate_token(self, token: str) -> dict:
        """Called by the receiver on every ingest batch."""
        auth = self._authenticate(token)
        if auth is None:
            return {"status": 404, "valid": False, "error": "unknown_token"}
        if auth["stale"]:
            return {"status": 409, "valid": False, "error": auth["reason"],
                    "shard_id": auth["shard"]["id"] if auth["shard"] else None}
        return {"status": 200, "valid": True,
                "shard_id": auth["shard"]["id"], "epoch": auth["shard"]["epoch"]}

    def rewind_cursor(self, shard_id: str, cursor: int) -> dict:
        """Move the resume hint backwards.

        The cursor is only a hint (the receiver's id-dedup is the real
        guarantee), so rewinding is always safe: it just makes the next
        collector re-fetch pages that will be recognised as duplicates.
        Used by the overlap-handoff demonstration to make overlap observable.
        """
        shard = self.get_shard(shard_id)
        if not shard:
            return {"status": 404, "error": "shard_not_found"}
        if shard["state"] not in ("active", "done"):
            return {"status": 409, "error": f"shard not active (state={shard['state']})"}
        self.db.execute(
            "UPDATE shards SET resume_cursor=?, updated_at=? WHERE id=?",
            (max(0, int(cursor)), now_iso(), shard_id))
        self.event("rewind", shard_id, {"resume_cursor": max(0, int(cursor))})
        log.warning("REWIND shard=%s resume_cursor -> %d", shard_id, max(0, int(cursor)))
        return {"status": 200, "shard_id": shard_id,
                "resume_cursor": max(0, int(cursor))}


def make_handler(state: State):
    class Handler(Router):
        routes = [
            ("GET", r"/health", "h_health"),
            ("GET", r"/shards", "h_list_shards"),
            ("POST", r"/shards", "h_create_shard"),
            ("POST", r"/shards/([a-z0-9_]+)/split", "h_split"),
            ("POST", r"/shards/merge", "h_merge"),
            ("POST", r"/shards/([a-z0-9_]+)/register", "h_register"),
            ("POST", r"/heartbeat", "h_heartbeat"),
            ("POST", r"/checkpoint", "h_checkpoint"),
            ("POST", r"/validate-token", "h_validate"),
            ("POST", r"/shards/([a-z0-9_]+)/rewind", "h_rewind"),
            ("GET", r"/events", "h_events"),
            ("GET", r"/overview", "h_overview"),
        ]

        def h_health(self, url, match):
            self.send_json(200, {"ok": True, "service": "scheduler"})

        def h_list_shards(self, url, match):
            now = state._now()
            out = []
            for s in state.list_shards():
                row = dict(s)
                row["lease_held"] = bool(s["lease_until"] and s["lease_until"] > now)
                row["lease_remaining"] = max(0, s["lease_until"] - now) if s["lease_until"] else 0
                out.append(row)
            self.send_json(200, {"shards": out})

        def h_create_shard(self, url, match):
            body = self.read_json()
            shard = state.create_shard(int(body["lo"]), body.get("hi"))
            self.send_json(201, shard)

        def h_split(self, url, match):
            shard_id = match.group(1)
            try:
                left, right = state.split(shard_id)
            except KeyError:
                self.send_json(404, {"error": "shard_not_found"})
                return
            except ValueError as exc:
                self.send_json(409, {"error": str(exc)})
                return
            self.send_json(200, {"left": left, "right": right})

        def h_merge(self, url, match):
            body = self.read_json()
            try:
                merged = state.merge(body["shard_a"], body["shard_b"])
            except KeyError:
                self.send_json(404, {"error": "shard_not_found"})
                return
            except ValueError as exc:
                self.send_json(409, {"error": str(exc)})
                return
            self.send_json(200, {"merged": merged})

        def h_register(self, url, match):
            shard_id = match.group(1)
            body = self.read_json()
            try:
                payload = state.register(shard_id, body["collector_id"], body["control_addr"])
            except KeyError:
                self.send_json(404, {"error": "shard_not_found"})
                return
            except ValueError as exc:
                self.send_json(409, {"error": str(exc)})
                return
            self.send_json(201, payload)

        def h_heartbeat(self, url, match):
            body = self.read_json()
            result = state.heartbeat(body["token"])
            status = result.pop("status")
            self.send_json(status, result)

        def h_checkpoint(self, url, match):
            body = self.read_json()
            result = state.checkpoint(body["token"], int(body["cursor"]))
            status = result.pop("status")
            self.send_json(status, result)

        def h_validate(self, url, match):
            body = self.read_json()
            result = state.validate_token(body["token"])
            status = result.pop("status")
            self.send_json(status, result)

        def h_rewind(self, url, match):
            shard_id = match.group(1)
            body = self.read_json()
            result = state.rewind_cursor(shard_id, int(body.get("cursor", 0)))
            status = result.pop("status")
            self.send_json(status, result)

        def h_events(self, url, match):
            rows = state.db.query("SELECT * FROM events ORDER BY id DESC LIMIT 100")
            self.send_json(200, {"events": rows})

        def h_overview(self, url, match):
            now = state._now()
            shards = []
            for s in state.list_shards():
                row = dict(s)
                row["lease_held"] = bool(s["lease_until"] and s["lease_until"] > now)
                shards.append(row)
            events = state.db.query("SELECT * FROM events ORDER BY id DESC LIMIT 30")
            self.send_json(200, {"shards": shards, "events": events, "now": now})

    return Handler


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8092)
    ap.add_argument("--db", default="data/scheduler.db")
    ap.add_argument("--source-url", default="http://127.0.0.1:8091")
    args = ap.parse_args()
    setup_logging()
    install_signal_trace(log)
    state = State(DB(args.db), args.source_url)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(state))
    log.info("scheduler listening on %d", args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
