"""Headless API server: a background service that needs no browser and no login.

It serves:
  GET /health               liveness and mirror status (no key needed)
  GET /api/v1/cursor        latest sequence number (API key)
  GET /api/v1/events        paged events, same envelopes as webhooks (API key)

By default it also keeps the mirror running, so events keep being recorded. Use
--no-mirror to serve the API from the database only.

Run in the background (no window):   start_api_background.bat
Stop:                                 stop_api_background.bat  (or: manage.py api stop)
Status:                               manage.py api status

Full reference: docs/API.md and docs/openapi.yaml
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

import ui
from config import ConfigError, Settings, load_settings
from logging_setup import configure_logging
from pull_api import handle_pull
from storage import Storage

log = logging.getLogger("api")

KEY_DESIRED = "api.desired"
KEY_STATE = "api.state"
KEY_PID = "api.pid"
KEY_HEARTBEAT = "api.heartbeat_utc"
KEY_STARTED = "api.started_utc"
POLL_SECONDS = 2
STALE_AFTER = timedelta(seconds=10)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def api_status(storage: Storage) -> dict[str, Any]:
    """State of the background API as recorded by the running server."""
    heartbeat = storage.kv_get(KEY_HEARTBEAT)
    fresh = False
    if heartbeat:
        fresh = datetime.now(timezone.utc) - datetime.fromisoformat(heartbeat) < STALE_AFTER
    state = storage.kv_get(KEY_STATE) or "stopped"
    return {
        "running": state == "running" and fresh,
        "state": state if (state != "running" or fresh) else "stale",
        "pid": storage.kv_get(KEY_PID),
        "started_utc": storage.kv_get(KEY_STARTED),
        "heartbeat_utc": heartbeat,
    }


class ApiHandler(BaseHTTPRequestHandler):
    """Read-only HTTP handler. Every route opens its own database connection."""

    server_version = "TelegramMirrorAPI/1.0"
    db_path = None  # bound per server in serve()

    def log_message(self, fmt: str, *args: Any) -> None:
        log.debug("%s - %s", self.address_string(), fmt % args)

    def _json(self, status: int, payload: Any, headers: dict[str, str] | None = None) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        query = {k: v[-1] for k, v in parse_qs(parsed.query, keep_blank_values=True).items()}
        storage = Storage(self.db_path)
        try:
            if parsed.path == "/health":
                return self._json(200, {"status": "ok", "api": api_status(storage),
                                        "mirror": ui.mirror_status(storage)})
            if parsed.path.startswith("/api/v1/"):
                status, payload = handle_pull(storage, parsed.path, query, self.headers.get("Authorization", ""))
                return self._json(status, payload)
            return self._json(404, {"error": "not found"})
        finally:
            storage.close()

    def _read_only(self) -> None:
        self._json(405, {"error": "read-only API: use GET"}, {"Allow": "GET"})

    do_POST = do_PUT = do_PATCH = do_DELETE = _read_only


def serve(settings: Settings, *, with_mirror: bool) -> int:
    storage = Storage(settings.db_path)
    try:
        existing = api_status(storage)
        if existing["running"]:
            log.error("API is already running (pid %s). Stop it first.", existing["pid"])
            return 3

        handler = type("BoundApiHandler", (ApiHandler,), {"db_path": settings.db_path})
        server = ThreadingHTTPServer((settings.api_host, settings.api_port), handler)

        storage.kv_set(KEY_DESIRED, "run")
        storage.kv_set(KEY_STATE, "running")
        storage.kv_set(KEY_PID, str(os.getpid()))
        storage.kv_set(KEY_STARTED, _now())
        storage.kv_set(KEY_HEARTBEAT, _now())

        if with_mirror:
            ui._settings = settings  # the mirror helpers in ui.py need the settings
            result = ui.start_mirror(storage)
            log.info("Mirror start requested (state=%s)", result.get("state"))

        thread = threading.Thread(target=server.serve_forever, name="api-http", daemon=True)
        thread.start()
        log.info("API listening on http://%s:%s (mirror=%s)", settings.api_host, settings.api_port, with_mirror)

        try:
            while storage.kv_get(KEY_DESIRED) != "stop":
                storage.kv_set(KEY_HEARTBEAT, _now())
                time.sleep(POLL_SECONDS)
            log.info("Stop requested")
        except KeyboardInterrupt:
            log.info("Interrupted")
        finally:
            server.shutdown()
            server.server_close()
            if with_mirror:
                ui.stop_mirror(storage)
            storage.kv_set(KEY_STATE, "stopped")
            storage.kv_set(KEY_PID, None)
            storage.kv_set(KEY_HEARTBEAT, _now())
            log.info("API stopped")
        return 0
    finally:
        storage.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Headless API server for the Telegram mirror.")
    parser.add_argument("--no-mirror", action="store_true", help="serve the API only; do not run the mirror")
    args = parser.parse_args(argv)
    try:
        settings = load_settings()
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    configure_logging(settings.log_dir, settings.log_level, settings.log_max_bytes, settings.log_backup_count)
    try:
        return serve(settings, with_mirror=not args.no_mirror)
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
