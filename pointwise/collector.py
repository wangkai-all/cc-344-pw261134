"""Collector: paginates the real source HTTP API and delivers batches to the receiver.

Safety model
  * On start it registers with the scheduler and receives a credential: fencing
    token + shard epoch + range + resume cursor. The credential is persisted so a
    restarted process can prove its continuation point.
  * Every 5s a heartbeat renews the 30s lease. If the scheduler answers 409 (the
    shard was split/merged and this epoch is retired), the collector stops itself:
    it can no longer checkpoint or ingest anything.
  * The receiver re-validates the token on every batch, so a stale collector that
    missed the heartbeat rejection is fenced at submission time too.
  * Source 429 / connection failures are retried with backoff honouring Retry-After:
    a temporary refusal never loses progress.
  * Pagination restarts from the persisted cursor; duplicate delivery is harmless
    because the receiver dedups on record id.
"""
from __future__ import annotations
import os as _os
import sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))

import argparse
import json
import logging
import os
import threading
import time
from http.server import ThreadingHTTPServer

from common import Router, gen_id, http, now_iso, setup_logging

log = logging.getLogger("collector")
HEARTBEAT_INTERVAL = 5
FETCH_IDLE_SECONDS = 2  # idle loop when the range is drained


class Collector:
    def __init__(self, args):
        self.args = args
        self.id = args.id
        self.shard_id = args.shard_id
        self.source_url = args.source_url
        self.scheduler_url = args.scheduler_url
        self.receiver_url = args.receiver_url
        self.control_addr = f"127.0.0.1:{args.control_port}"
        self.credential: dict | None = None
        self.cursor = 0
        self.stop_event = threading.Event()
        self.paused = threading.Event()
        self.status = "starting"
        self.status_detail = ""
        self.stats = {"pages_fetched": 0, "records_offered": 0, "inserts": 0,
                      "duplicates": 0, "rejected": 0, "refusals": 0,
                      "transient_errors": 0, "fenced_events": 0}
        self.last_activity = now_iso()
        self._lock = threading.RLock()
        os.makedirs(os.path.dirname(args.state_file), exist_ok=True)

    # --------------------------------------------------------- persistence
    def _save_state(self):
        tmp = self.args.state_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({"credential": self.credential, "cursor": self.cursor,
                       "collector_id": self.id, "shard_id": self.shard_id,
                       "updated_at": now_iso()}, fh)
        os.replace(tmp, self.args.state_file)

    def _load_state(self) -> dict | None:
        if not os.path.exists(self.args.state_file):
            return None
        with open(self.args.state_file, encoding="utf-8") as fh:
            return json.load(fh)

    # -------------------------------------------------------------- runtime
    def register(self):
        body = {"collector_id": self.id, "control_addr": self.control_addr}
        status, resp = http("POST",
                            f"{self.scheduler_url}/shards/{self.shard_id}/register", body)
        if status == 201:
            with self._lock:
                self.credential = resp
                self.cursor = resp["resume_cursor"]
            self._save_state()
            log.info("registered shard=%s epoch=%d cursor=%d range=[%s,%s)",
                     resp["shard_id"], resp["epoch"], self.cursor, resp["lo"],
                     resp["hi"] if resp["hi"] is not None else "open")
            return True
        log.error("registration failed status=%d body=%s", status, resp)
        # A persisted credential may still be valid if registration was only a
        # transient network failure — resume from it and let the heartbeat decide.
        saved = self._load_state()
        if saved and saved.get("shard_id") == self.shard_id and saved.get("credential"):
            with self._lock:
                self.credential = saved["credential"]
                self.cursor = saved["cursor"]
            log.warning("reusing persisted credential epoch=%s", self.credential.get("epoch"))
            return True
        self.status = "failed"
        self.status_detail = f"registration rejected: {resp}"
        return False

    def heartbeat_loop(self):
        while not self.stop_event.is_set():
            if self.stop_event.wait(HEARTBEAT_INTERVAL):
                return
            if not self.credential or self.paused.is_set():
                continue
            status, resp = http("POST", f"{self.scheduler_url}/heartbeat",
                                {"token": self.credential["token"]})
            if status == 409:
                with self._lock:
                    self.stats["fenced_events"] += 1
                    self.status = "fenced"
                    self.status_detail = (
                        f"epoch {self.credential['epoch']} retired by scheduler "
                        f"({resp.get('error')}); shard handed over, exiting")
                log.error("FENCED by scheduler: %s — shutting down this task", resp)
                self.stop_event.set()
                return
            if status != 200:
                log.warning("heartbeat transient failure: %s", resp)

    def _checkpoint(self):
        status, resp = http("POST", f"{self.scheduler_url}/checkpoint",
                            {"token": self.credential["token"], "cursor": self.cursor})
        if status == 409:
            return False, resp
        if status != 200:
            log.warning("checkpoint transient failure: %s", resp)
            return True, resp
        self._save_state()
        return True, resp

    def fetch_page(self):
        c = self.credential
        params = f"cursor={self.cursor}&limit={self.args.page_size}"
        if c["lo"] is not None:
            params += f"&lo={c['lo']}"
        if c["hi"] is not None:
            params += f"&hi={c['hi']}"
        status, body = http("GET", f"{self.source_url}/records?{params}",
                            timeout=self.args.request_timeout)
        return status, body

    def deliver(self, records) -> tuple[bool, dict]:
        status, body = http("POST", f"{self.receiver_url}/ingest", {
            "token": self.credential["token"],
            "shard_id": self.credential["shard_id"],
            "epoch": self.credential["epoch"],
            "records": records,
        })
        return status, body

    def run(self):
        if not self.register():
            return
        self.status = "running"
        hb = threading.Thread(target=self.heartbeat_loop, daemon=True)
        hb.start()
        empty_pages = 0
        while not self.stop_event.is_set():
            if self.paused.is_set():
                self.status = "paused"
                self.stop_event.wait(1)
                continue
            self.status = "running"
            status, body = self.fetch_page()
            # ---- source refuses / is unreachable: backoff, keep cursor
            if status == 429:
                wait = int(body.get("retry_after", 2))
                with self._lock:
                    self.stats["refusals"] += 1
                self.status_detail = f"source refusing; backing off {wait}s"
                log.warning("source 429, retry_after=%ds (cursor unchanged=%d)", wait, self.cursor)
                if self.stop_event.wait(wait):
                    break
                continue
            if status != 200:
                with self._lock:
                    self.stats["transient_errors"] += 1
                self.status_detail = f"fetch error {status}: {body.get('error')}"
                log.warning("transient fetch failure: %s — retry in 3s", body)
                if self.stop_event.wait(3):
                    break
                continue
            with self._lock:
                self.stats["pages_fetched"] += 1
                self.last_activity = now_iso()
            records = body["records"]
            # ---- drained page: idle-poll so newly arriving material is still seen
            if not records:
                empty_pages += 1
                self.status_detail = f"drained at cursor={self.cursor}; idle polling"
                if self.stop_event.wait(FETCH_IDLE_SECONDS):
                    break
                continue
            empty_pages = 0
            # ---- deliver through the fencing gate
            ingest_status, ingest = self.deliver(records)
            if ingest_status == 409:
                with self._lock:
                    self.stats["fenced_events"] += 1
                    self.stats["rejected"] += ingest.get("rejected", len(records))
                    self.status = "fenced"
                    self.status_detail = f"receiver fence: {ingest.get('reason')}"
                log.error("FENCED at ingest (%s) — stopping; no rows written by this batch",
                          ingest.get("reason"))
                self.stop_event.set()
                break
            if ingest_status != 200:
                log.warning("ingest transient failure: %s — retry page", ingest)
                if self.stop_event.wait(3):
                    break
                continue
            with self._lock:
                self.cursor = body["next_cursor"]
                self.stats["records_offered"] += ingest["offered"]
                self.stats["inserts"] += ingest["inserted"]
                self.stats["duplicates"] += ingest["duplicates"]
                self.stats["rejected"] += ingest["rejected"]
            ok, cp = self._checkpoint()
            if not ok:
                with self._lock:
                    self.status = "fenced"
                    self.status_detail = f"checkpoint fence: {cp.get('error')}"
                log.error("FENCED at checkpoint: %s", cp)
                self.stop_event.set()
                break
            if not body["has_more"]:
                if self.stop_event.wait(FETCH_IDLE_SECONDS):
                    break
        if self.status not in ("fenced", "failed"):
            self.status = "stopped"
        log.info("collector loop ended status=%s stats=%s", self.status, self.stats)

    def snapshot(self):
        with self._lock:
            return {
                "collector_id": self.id,
                "shard_id": self.shard_id,
                "status": self.status,
                "detail": self.status_detail,
                "paused": self.paused.is_set(),
                "cursor": self.cursor,
                "epoch": self.credential["epoch"] if self.credential else None,
                "token_prefix": (self.credential["token"][:10] + "…") if self.credential else None,
                "range": [self.credential["lo"], self.credential["hi"]] if self.credential else None,
                "stats": dict(self.stats),
                "last_activity": self.last_activity,
                "state_file": self.args.state_file,
            }


def make_control(collector: Collector):
    class Handler(Router):
        routes = [
            ("GET", r"/status", "h_status"),
            ("GET", r"/credential", "h_credential"),
            ("POST", r"/pause", "h_pause"),
            ("POST", r"/resume", "h_resume"),
            ("POST", r"/shutdown", "h_shutdown"),
        ]

        def h_status(self, url, match):
            self.send_json(200, collector.snapshot())

        def h_credential(self, url, match):
            # localhost-only demo aid: lets the handoff test capture the exact
            # token before it is retired, then prove replay is rejected.
            self.send_json(200, {"credential": collector.credential})

        def h_pause(self, url, match):
            collector.paused.set()
            self.send_json(200, {"ok": True, "paused": True})

        def h_resume(self, url, match):
            collector.paused.clear()
            self.send_json(200, {"ok": True, "paused": False})

        def h_shutdown(self, url, match):
            collector.stop_event.set()
            self.send_json(200, {"ok": True})

    return Handler


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default=gen_id("col_"))
    ap.add_argument("--shard-id", required=True)
    ap.add_argument("--control-port", type=int, required=True,
                    help="0 = let the OS assign a free ephemeral port")
    ap.add_argument("--port-file", default="",
                    help="if set, the bound control port is written here once ready")
    ap.add_argument("--state-file", required=True)
    ap.add_argument("--source-url", default="http://127.0.0.1:8091")
    ap.add_argument("--scheduler-url", default="http://127.0.0.1:8092")
    ap.add_argument("--receiver-url", default="http://127.0.0.1:8093")
    ap.add_argument("--page-size", type=int, default=100)
    ap.add_argument("--request-timeout", type=int, default=15)
    args = ap.parse_args()
    setup_logging()
    collector = Collector(args)
    control = ThreadingHTTPServer(("127.0.0.1", args.control_port), make_control(collector))
    collector.control_addr = f"127.0.0.1:{control.server_port}"
    if args.port_file:
        tmp = args.port_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(str(control.server_port))
        os.replace(tmp, args.port_file)
    threading.Thread(target=control.serve_forever, daemon=True).start()
    log.info("control server on port %d", control.server_port)
    try:
        collector.run()
    finally:
        time.sleep(0.3)  # let a final /status read the terminal state
        control.shutdown()


if __name__ == "__main__":
    main()
