"""Source service: locally generated material exposed through real HTTP pagination.

Endpoints
  GET  /health
  GET  /records?lo=&hi=&cursor=&limit=   one page per request (real pagination)
  GET  /manifest?lo=&hi=                 authoritative id list + checksum for a range
  GET  /stats
  GET  /control/requests                 recent HTTP accesses (request-cost evidence)
  POST /control/inject {"count": n}      new material keeps arriving during migrations
  POST /control/refuse {"seconds": s, "probability": p, "retry_after": r}
  POST /control/clear-refuse
"""
from __future__ import annotations
import os as _os
import sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))

import argparse
import hashlib
import logging
import threading
import time
from http.server import ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from common import DB, Router, gen_id, install_signal_trace, now_iso, setup_logging

log = logging.getLogger("source")
DEFAULT_PAGE = 100
MAX_PAGE = 200
MAX_REQUEST_LOG = 500


def record_payload(rid: int) -> str:
    """Deterministic locally-generated material for a given record id."""
    return hashlib.sha256(f"pointwise/record/{rid}".encode()).hexdigest()[:32]


def payload_hash(rid: int, payload: str) -> int:
    return int(hashlib.sha256(f"{rid}|{payload}".encode()).hexdigest(), 16)


class State:
    def __init__(self, db: DB):
        self.db = db
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS records (
                id       INTEGER PRIMARY KEY,
                payload  TEXT NOT NULL,
                created  TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS request_log (
                ts TEXT NOT NULL, method TEXT NOT NULL, path TEXT NOT NULL, status INTEGER NOT NULL
            );
            """
        )
        self.refuse_until = 0.0
        self.refuse_retry_after = 2
        self.refuse_probability = 1.0
        self.refuse_lock = threading.Lock()
        self.max_id = self.db.one("SELECT COALESCE(MAX(id), 0) AS m FROM records")["m"]

    # ------------------------------------------------ material management
    def inject(self, count: int) -> list[int]:
        ids = []
        for _ in range(max(0, count)):
            rid = self.max_id + 1
            self.db.execute(
                "INSERT INTO records(id, payload, created) VALUES (?, ?, ?)",
                (rid, record_payload(rid), now_iso()),
            )
            self.max_id = rid
            ids.append(rid)
        if ids:
            log.info("injected %d new records (max_id=%d)", len(ids), self.max_id)
        return ids

    def page(self, lo, hi, cursor, limit):
        sql = "SELECT id, payload, created FROM records WHERE id > ?"
        params: list = [cursor]
        if lo is not None:
            sql += " AND id >= ?"
            params.append(lo)
        if hi is not None:
            sql += " AND id < ?"
            params.append(hi)
        sql += " ORDER BY id ASC LIMIT ?"
        params.append(limit + 1)
        rows = self.db.query(sql, params)
        has_more = len(rows) > limit
        rows = rows[:limit]
        next_cursor = rows[-1]["id"] if rows else cursor
        return rows, next_cursor, has_more

    def manifest(self, lo, hi):
        sql = "SELECT id, payload FROM records"
        conds, params = [], []
        if lo is not None:
            conds.append("id >= ?")
            params.append(lo)
        if hi is not None:
            conds.append("id < ?")
            params.append(hi)
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY id ASC"
        rows = self.db.query(sql, params)
        checksum = 0
        for r in rows:
            checksum = (checksum + payload_hash(r["id"], r["payload"])) & 0xFFFFFFFF
        return {
            "lo": lo,
            "hi": hi,
            "count": len(rows),
            "checksum": f"{checksum:08x}",
            "min_id": rows[0]["id"] if rows else None,
            "max_id": rows[-1]["id"] if rows else None,
            "ids": [r["id"] for r in rows],
            "generated_at": now_iso(),
        }

    # ------------------------------------------------ refusal injection
    def maybe_refuse(self) -> int | None:
        with self.refuse_lock:
            if time.time() < self.refuse_until:
                # deterministic-ish pseudo random without importing extra state
                if (self.refuse_probability >= 1.0) or (
                    (int(time.time() * 1000) % 100) < self.refuse_probability * 100
                ):
                    return self.refuse_retry_after
        return None

    def log_request(self, method, path, status):
        self.db.execute(
            "INSERT INTO request_log(ts, method, path, status) VALUES (?, ?, ?, ?)",
            (now_iso(), method, path, status),
        )
        self.db.execute(
            "DELETE FROM request_log WHERE rowid NOT IN "
            "(SELECT rowid FROM request_log ORDER BY rowid DESC LIMIT ?)",
            (MAX_REQUEST_LOG,),
        )


def make_handler(state: State):
    class Handler(Router):
        routes = [
            ("GET", r"/health", "h_health"),
            ("GET", r"/records", "h_records"),
            ("GET", r"/manifest", "h_manifest"),
            ("GET", r"/stats", "h_stats"),
            ("GET", r"/control/requests", "h_requests"),
            ("POST", r"/control/inject", "h_inject"),
            ("POST", r"/control/refuse", "h_refuse"),
            ("POST", r"/control/clear-refuse", "h_clear_refuse"),
        ]

        def _qs_int(self, url, key):
            vals = parse_qs(url.query).get(key)
            if not vals or vals[0] == "":
                return None
            return int(vals[0])

        def h_health(self, url, match):
            self.send_json(200, {"ok": True, "service": "source", "max_id": state.max_id})
            state.log_request("GET", url.path, 200)

        def h_records(self, url, match):
            retry = state.maybe_refuse()
            if retry is not None:
                self.send_json(
                    429,
                    {"error": "source_refusing", "retry_after": retry, "request_id": gen_id("req_")},
                )
                state.log_request("GET", url.path, 429)
                return
            try:
                lo = self._qs_int(url, "lo")
                hi = self._qs_int(url, "hi")
                cursor = self._qs_int(url, "cursor") or 0
                limit = min(self._qs_int(url, "limit") or DEFAULT_PAGE, MAX_PAGE)
            except ValueError:
                self.send_json(400, {"error": "bad_query_params"})
                state.log_request("GET", url.path, 400)
                return
            rows, next_cursor, has_more = state.page(lo, hi, cursor, limit)
            self.send_json(
                200,
                {
                    "lo": lo,
                    "hi": hi,
                    "cursor": cursor,
                    "next_cursor": next_cursor,
                    "has_more": has_more,
                    "page_size": len(rows),
                    "records": rows,
                },
            )
            state.log_request("GET", url.path, 200)

        def h_manifest(self, url, match):
            try:
                lo = self._qs_int(url, "lo")
                hi = self._qs_int(url, "hi")
            except ValueError:
                self.send_json(400, {"error": "bad_query_params"})
                return
            self.send_json(200, state.manifest(lo, hi))
            state.log_request("GET", url.path, 200)

        def h_stats(self, url, match):
            total = state.db.one("SELECT COUNT(*) AS c FROM records")["c"]
            refusing = time.time() < state.refuse_until
            self.send_json(
                200,
                {
                    "total_records": total,
                    "max_id": state.max_id,
                    "refusing_until": state.refuse_until if refusing else None,
                },
            )

        def h_requests(self, url, match):
            rows = state.db.query(
                "SELECT ts, method, path, status FROM request_log ORDER BY rowid DESC LIMIT ?",
                (MAX_REQUEST_LOG,),
            )
            summary = state.db.query(
                "SELECT method AS method, path AS path, status AS status, "
                "COUNT(*) AS n FROM request_log GROUP BY method, path, status"
            )
            self.send_json(200, {"requests": rows, "summary": summary})

        def h_inject(self, url, match):
            body = self.read_json()
            ids = state.inject(int(body.get("count", 0)))
            self.send_json(200, {"injected": len(ids), "first_id": ids[0] if ids else None,
                                 "last_id": ids[-1] if ids else None, "max_id": state.max_id})

        def h_refuse(self, url, match):
            body = self.read_json()
            seconds = float(body.get("seconds", 10))
            state.refuse_until = time.time() + seconds
            state.refuse_retry_after = int(body.get("retry_after", 2))
            state.refuse_probability = float(body.get("probability", 1.0))
            log.warning("source will refuse /records for %.1fs (retry_after=%d)",
                        seconds, state.refuse_retry_after)
            self.send_json(200, {"refuse_until": state.refuse_until,
                                 "retry_after": state.refuse_retry_after,
                                 "probability": state.refuse_probability})

        def h_clear_refuse(self, url, match):
            state.refuse_until = 0.0
            self.send_json(200, {"ok": True})

    return Handler


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8091)
    ap.add_argument("--db", default="data/source.db")
    ap.add_argument("--seed", type=int, default=0, help="generate this many records at boot")
    args = ap.parse_args()
    setup_logging()
    install_signal_trace(log)
    state = State(DB(args.db))
    if args.seed and state.max_id == 0:
        state.inject(args.seed)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(state))
    log.info("source listening on %d (records=%d)", args.port, state.max_id)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
