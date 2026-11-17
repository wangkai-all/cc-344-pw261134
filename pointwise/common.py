"""Shared helpers: JSON HTTP client, SQLite setup, tiny routing layer."""
from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def setup_logging(level=logging.INFO):
    logging.basicConfig(level=level, format=LOG_FORMAT)


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z"


def install_signal_trace(log):
    """Log every terminating signal so an externally-killed process leaves evidence.

    SIGKILL cannot be caught; its absence in the log plus a -9 return code is itself
    the diagnostic. After logging, restore the default disposition and re-raise so
    the process still dies with the conventional exit status.
    """
    import signal as _signal

    def make_handler(signum):
        def handler(_sig, _frame):
            ppid = os.getppid()
            log.error("received signal %d (%s); ppid=%d pid=%d — re-raising default",
                      signum, _signal.Signals(signum).name, ppid, os.getpid())
            _signal.signal(signum, _signal.SIG_DFL)
            os.kill(os.getpid(), signum)
        return handler

    for name in ("SIGTERM", "SIGHUP", "SIGINT", "SIGQUIT", "SIGUSR1", "SIGUSR2"):
        sig = getattr(_signal, name, None)
        if sig is not None:
            try:
                _signal.signal(sig, make_handler(sig))
            except (ValueError, OSError):
                pass
    try:
        import faulthandler
        faulthandler.enable()
    except Exception:
        pass


def gen_id(prefix: str = "") -> str:
    return f"{prefix}{uuid.uuid4().hex[:12]}"


def http(method: str, url: str, body=None, timeout: int = 10):
    """JSON HTTP call. Returns (status, parsed_body). Never raises on HTTP errors."""
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read().decode("utf-8") or "{}")
        except Exception:
            return exc.code, {"error": "unparseable_error_response"}
    except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
        return 0, {"error": "unreachable", "detail": str(exc)}


def open_db(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    return conn


class DB:
    """Thin wrapper serialising access with a lock (demo-scale traffic)."""

    def __init__(self, path: str):
        self.conn = open_db(path)
        self.lock = threading.RLock()

    def query(self, sql: str, params=()):
        with self.lock:
            return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    def one(self, sql: str, params=()):
        with self.lock:
            row = self.conn.execute(sql, params).fetchone()
            return dict(row) if row else None

    def execute(self, sql: str, params=()):
        with self.lock:
            cur = self.conn.execute(sql, params)
            self.conn.commit()
            return cur

    def executescript(self, script: str):
        with self.lock:
            self.conn.executescript(script)
            self.conn.commit()


class Router(BaseHTTPRequestHandler):
    """Route table: (HTTP method, regex path pattern, handler attribute name)."""

    routes: list[tuple[str, str, str]] = []

    def log_message(self, fmt, *args):  # silence default stderr noise
        logging.getLogger("http").debug(fmt, *args)

    def send_json(self, status: int, obj):
        payload = json.dumps(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def read_json(self):
        length = int(self.headers.get("Content-Length", 0) or 0)
        if not length:
            return {}
        try:
            return json.loads(self.rfile.read(length).decode("utf-8"))
        except json.JSONDecodeError:
            return {}

    def _dispatch(self, method: str):
        url = urlparse(self.path)
        for route_method, pattern, handler_name in self.routes:
            if route_method != method:
                continue
            match = re.fullmatch(pattern, url.path)
            if match:
                try:
                    getattr(self, handler_name)(url, match)
                except Exception as exc:  # noqa: BLE001 - surface as 500 JSON
                    logging.exception("handler failure: %s", exc)
                    self.send_json(500, {"error": "internal", "detail": str(exc)})
                return
        self.send_json(404, {"error": "not_found", "path": url.path})

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")
