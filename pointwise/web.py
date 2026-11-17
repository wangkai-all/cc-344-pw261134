"""Web console: trigger splits/merges/handoffs, supervise collector processes,
observe ownership per range and reconciliation evidence in one page.
"""
from __future__ import annotations
import os as _os
import sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))

import argparse
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from http.server import ThreadingHTTPServer
from urllib.parse import urlparse

from common import DB, Router, gen_id, http, now_iso, setup_logging

log = logging.getLogger("web")


class Supervisor:
    """Owns collector child processes. Popen + per-collector control HTTP port."""

    def __init__(self, base_dir: str, urls: dict):
        self.base_dir = base_dir
        self.urls = urls
        self.run_dir = os.path.join(base_dir, "run")
        self.log_dir = os.path.join(base_dir, "logs")
        os.makedirs(self.run_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)
        self.processes: dict[str, dict] = {}
        self.lock = threading.RLock()
        threading.Thread(target=self._reap_loop, daemon=True).start()

    def _reap_loop(self):
        """Drop dead child processes so the registry does not accumulate corpses."""
        while True:
            time.sleep(2)
            with self.lock:
                dead = [cid for cid, p in self.processes.items()
                        if p["proc"].poll() is not None]
                for cid in dead:
                    self.processes.pop(cid, None)
            if dead:
                log.info("reaped exited collectors: %s", dead)

    def _wait_port_file(self, port_file: str, timeout: float = 8.0) -> int | None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with open(port_file, encoding="utf-8") as fh:
                    return int(fh.read().strip())
            except (FileNotFoundError, ValueError):
                time.sleep(0.15)
        return None

    def start(self, shard_id: str, collector_id: str | None = None,
              allow_active: bool = False) -> dict:
        with self.lock:
            running = [p for p in self.processes.values()
                       if p["shard_id"] == shard_id and p["proc"].poll() is None]
            active = []
            for p in running:
                status, body = self._control(p, "/status", "GET")
                if status != 200 or not body.get("paused"):
                    active.append(p)
            if active and not allow_active:
                return {"ok": False, "error": "collector already running",
                        "collector_id": active[0]["collector_id"]}
            collector_id = collector_id or gen_id("col_")
            state_file = os.path.join(self.run_dir, f"{shard_id}.{collector_id}.state.json")
            port_file = os.path.join(self.run_dir, f"{collector_id}.port")
            if os.path.exists(port_file):
                os.remove(port_file)
            log_path = os.path.join(self.log_dir, f"{collector_id}.log")
            cmd = [
                sys.executable, "-I", os.path.join(self.base_dir, "collector.py"),
                "--id", collector_id, "--shard-id", shard_id,
                "--control-port", "0", "--port-file", port_file,
                "--state-file", state_file,
                "--source-url", self.urls["source"],
                "--scheduler-url", self.urls["scheduler"],
                "--receiver-url", self.urls["receiver"],
            ]
            log_fh = open(log_path, "ab")
            proc = subprocess.Popen(cmd, cwd=self.base_dir, stdout=log_fh, stderr=log_fh,
                                    start_new_session=True)
            port = self._wait_port_file(port_file)
            if port is None:
                proc.kill()
                return {"ok": False, "error": "collector control port never came up",
                        "collector_id": collector_id, "pid": proc.pid}
            entry = {"collector_id": collector_id, "shard_id": shard_id,
                     "control_port": port, "pid": proc.pid, "proc": proc,
                     "state_file": state_file, "port_file": port_file,
                     "log_path": log_path, "started_at": now_iso()}
            self.processes[collector_id] = entry
            log.info("started collector %s pid=%d shard=%s port=%d",
                     collector_id, proc.pid, shard_id, port)
            return {"ok": True, **{k: v for k, v in entry.items() if k != "proc"}}

    def _control(self, entry: dict, path: str, method: str = "POST"):
        return http(method, f"http://127.0.0.1:{entry['control_port']}{path}")

    def pause(self, collector_id: str):
        with self.lock:
            entry = self.processes.get(collector_id)
            if not entry:
                return None
            return self._control(entry, "/pause")

    def resume(self, collector_id: str):
        with self.lock:
            entry = self.processes.get(collector_id)
            if not entry:
                return None
            return self._control(entry, "/resume")

    def stop(self, collector_id: str, timeout: float = 8.0) -> dict:
        with self.lock:
            entry = self.processes.get(collector_id)
            if not entry:
                return {"ok": False, "error": "unknown_collector"}
            status, body = self._control(entry, "/shutdown")
            try:
                entry["proc"].wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(os.getpgid(entry["proc"].pid), signal.SIGTERM)
                except ProcessLookupError:
                    entry["proc"].send_signal(signal.SIGTERM)
                try:
                    entry["proc"].wait(timeout=3)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(os.getpgid(entry["proc"].pid), signal.SIGKILL)
                    except ProcessLookupError:
                        entry["proc"].kill()
            self.processes.pop(collector_id, None)
            log.info("stopped collector %s (exit=%s)", collector_id, entry["proc"].returncode)
            return {"ok": True, "exit_code": entry["proc"].returncode}

    def handoff(self, shard_id: str, mode: str) -> dict:
        """Replace the task owning a shard. Two documented strategies."""
        with self.lock:
            old = next((p for p in self.processes.values()
                        if p["shard_id"] == shard_id and p["proc"].poll() is None), None)
            old_id = old["collector_id"] if old else None
            if mode == "pause":
                # Lowest request cost: freeze the old task first so no overlap fetching.
                if old:
                    self._control(old, "/pause")
                    time.sleep(0.6)
                # old task is paused -> start() permits a replacement
                started = self.start(shard_id)
                new_id = started.get("collector_id")
                if old:
                    self._control(old, "/shutdown")  # non-blocking; reaper clears it
                return {"mode": mode, "old_collector": old_id, "new_collector": new_id,
                        "overlap_fetches": 0,
                        "note": "paused old task before handoff; brief gap possible"}
            # overlap: start the replacement while the old one still runs. The new
            # registration bumps the epoch and fences the old one; the scheduler and
            # receiver reject every subsequent request carrying the old token.
            started = self.start(shard_id, allow_active=True)
            if not started.get("ok"):
                return {"mode": mode, "error": "replacement failed", "detail": started}
            new_id = started.get("collector_id")
            if old:
                # the old collector's next heartbeat (within 5s) returns 409 and it
                # exits; wait briefly so the UI can observe the fence without
                # blocking the HTTP request long enough to time out
                deadline = time.time() + 7
                while time.time() < deadline:
                    status, body = self._control(old, "/status", "GET")
                    if status == 200 and body.get("status") == "fenced":
                        break
                    time.sleep(0.3)
                if old["proc"].poll() is None:
                    self._control(old, "/shutdown")  # non-blocking; reaper clears it
            return {"mode": mode, "old_collector": old_id, "new_collector": new_id,
                    "overlap_fetches": "bounded (old collector fenced at first 409)",
                    "note": "overlapping handoff: no gap, extra bounded duplicate fetches"}

    def statuses(self) -> list[dict]:
        out = []
        with self.lock:
            entries = list(self.processes.items())
        for cid, entry in entries:
            alive = entry["proc"].poll() is None
            row = {"collector_id": cid, "shard_id": entry["shard_id"],
                   "pid": entry["pid"], "alive": alive,
                   "control_port": entry["control_port"],
                   "started_at": entry["started_at"], "log_path": entry["log_path"]}
            if alive:
                status, body = self._control(entry, "/status", "GET")
                row["runtime"] = body if status == 200 else {"status": "unreachable", "detail": body}
            else:
                row["runtime"] = {"status": "exited", "exit_code": entry["proc"].returncode}
                with self.lock:
                    self.processes.pop(cid, None)
            out.append(row)
        return out

    def shutdown_all(self):
        with self.lock:
            ids = list(self.processes.keys())
        for cid in ids:
            self.stop(cid)


def make_handler(sup: Supervisor, db: DB, urls: dict, static_dir: str):
    class Handler(Router):
        routes = [
            ("GET", r"/health", "h_health"),
            ("GET", r"/", "h_index"),
            ("GET", r"/app\.js", "h_appjs"),
            ("GET", r"/api/state", "h_state"),
            ("GET", r"/api/collectors", "h_collectors"),
            ("POST", r"/api/shards", "h_create_shard"),
            ("POST", r"/api/shards/([a-z0-9_]+)/split", "h_split"),
            ("POST", r"/api/shards/merge", "h_merge"),
            ("POST", r"/api/collectors/start", "h_start"),
            ("POST", r"/api/collectors/([a-z0-9_]+)/(pause|resume|stop)", "h_lifecycle"),
            ("POST", r"/api/handoff", "h_handoff"),
            ("POST", r"/api/source/inject", "h_inject"),
            ("POST", r"/api/source/refuse", "h_refuse"),
            ("POST", r"/api/source/clear-refuse", "h_clear_refuse"),
            ("GET", r"/api/source/requests", "h_source_requests"),
            ("GET", r"/api/events", "h_events"),
            ("GET", r"/api/evidence", "h_evidence"),
            ("GET", r"/api/coverage", "h_coverage"),
            ("GET", r"/api/batches", "h_batches"),
            ("POST", r"/api/init-demo", "h_init_demo"),
        ]

        def _static(self, filename: str, content_type: str):
            try:
                with open(os.path.join(static_dir, filename), "rb") as fh:
                    payload = fh.read()
            except FileNotFoundError:
                self.send_json(404, {"error": "asset_missing"})
                return
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def h_health(self, url, match):
            self.send_json(200, {"ok": True, "service": "web"})

        def h_index(self, url, match):
            self._static("index.html", "text/html; charset=utf-8")
        def h_appjs(self, url, match):
            self._static("app.js", "application/javascript; charset=utf-8")

        def h_state(self, url, match):
            _, shards = http("GET", f"{urls['scheduler']}/shards")
            _, source = http("GET", f"{urls['source']}/stats")
            _, receiver = http("GET", f"{urls['receiver']}/stats")
            _, evidence = http("GET", f"{urls['receiver']}/evidence")
            self.send_json(200, {
                "shards": shards.get("shards", []),
                "source": source, "receiver": receiver,
                "evidence": evidence,
                "collectors": sup.statuses(),
                "ts": now_iso(),
            })

        def h_collectors(self, url, match):
            self.send_json(200, {"collectors": sup.statuses()})

        def h_create_shard(self, url, match):
            body = self.read_json()
            status, resp = http("POST", f"{urls['scheduler']}/shards", body)
            self.send_json(status, resp)

        def h_split(self, url, match):
            shard_id = match.group(1)
            status, resp = http("POST", f"{urls['scheduler']}/shards/{shard_id}/split", {})
            self.send_json(status, resp)

        def h_merge(self, url, match):
            status, resp = http("POST", f"{urls['scheduler']}/shards/merge", self.read_json())
            self.send_json(status, resp)

        def h_start(self, url, match):
            body = self.read_json()
            self.send_json(200, sup.start(body["shard_id"], body.get("collector_id")))

        def h_lifecycle(self, url, match):
            collector_id, action = match.group(1), match.group(2)
            if action == "pause":
                result = sup.pause(collector_id)
            elif action == "resume":
                result = sup.resume(collector_id)
            else:
                self.send_json(200, sup.stop(collector_id))
                return
            if result is None:
                self.send_json(404, {"error": "unknown_collector"})
            else:
                self.send_json(result[0], result[1])

        def h_handoff(self, url, match):
            body = self.read_json()
            mode = body.get("mode", "overlap")
            if mode not in ("overlap", "pause"):
                self.send_json(400, {"error": "mode must be overlap or pause"})
                return
            self.send_json(200, sup.handoff(body["shard_id"], mode))

        def h_inject(self, url, match):
            status, resp = http("POST", f"{urls['source']}/control/inject", self.read_json())
            self.send_json(status, resp)

        def h_refuse(self, url, match):
            status, resp = http("POST", f"{urls['source']}/control/refuse", self.read_json())
            self.send_json(status, resp)

        def h_clear_refuse(self, url, match):
            status, resp = http("POST", f"{urls['source']}/control/clear-refuse", {})
            self.send_json(status, resp)

        def h_source_requests(self, url, match):
            status, resp = http("GET", f"{urls['source']}/control/requests")
            self.send_json(status, resp)

        def h_events(self, url, match):
            status, resp = http("GET", f"{urls['scheduler']}/events")
            self.send_json(status, resp)

        def h_evidence(self, url, match):
            status, resp = http("GET", f"{urls['receiver']}/evidence")
            self.send_json(status, resp)

        def h_coverage(self, url, match):
            status, resp = http("GET", f"{urls['receiver']}/coverage")
            self.send_json(status, resp)

        def h_batches(self, url, match):
            status, resp = http("GET", f"{urls['receiver']}/batches")
            self.send_json(status, resp)

        def h_init_demo(self, url, match):
            _, stats = http("GET", f"{urls['source']}/stats")
            if stats.get("total_records", 0) == 0:
                http("POST", f"{urls['source']}/control/inject", {"count": 500})
            _, shards = http("GET", f"{urls['scheduler']}/shards")
            created = []
            if not [s for s in shards.get("shards", []) if s["state"] in ("active", "done")]:
                _, s1 = http("POST", f"{urls['scheduler']}/shards", {"lo": 1, "hi": 250})
                _, s2 = http("POST", f"{urls['scheduler']}/shards", {"lo": 250})
                created = [s1.get("id"), s2.get("id")]
            self.send_json(200, {"created_shards": created,
                                 "started": [sup.start(sid) for sid in created if sid]})

    return Handler


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--base-dir", default=os.path.dirname(os.path.abspath(__file__)))
    ap.add_argument("--db", default="data/web.db")
    ap.add_argument("--source-url", default="http://127.0.0.1:8091")
    ap.add_argument("--scheduler-url", default="http://127.0.0.1:8092")
    ap.add_argument("--receiver-url", default="http://127.0.0.1:8093")
    args = ap.parse_args()
    setup_logging()
    urls = {"source": args.source_url, "scheduler": args.scheduler_url,
            "receiver": args.receiver_url}
    sup = Supervisor(args.base_dir, urls)
    db = DB(args.db)  # reserved for web-local persistence
    static_dir = os.path.join(args.base_dir, "static")
    server = ThreadingHTTPServer(("127.0.0.1", args.port),
                                 make_handler(sup, db, urls, static_dir))
    log.info("web console listening on %d", args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        sup.shutdown_all()


if __name__ == "__main__":
    main()
