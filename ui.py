"""Local web UI for the Telegram mirror.

Run:  python ui.py          (or double-click start_ui.bat)

Standard library only. Binds to 127.0.0.1, so it is reachable only from this computer.

Login: the first time, the database gets one admin account (admin / 12345678, stored as a
salted hash). Sign in once per UI session. The session ends on logout or when this
process restarts.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import mimetypes
import os
import secrets
import subprocess
import sys
import threading
import time
import webbrowser
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from config import PROJECT_DIR, ConfigError, Settings, load_settings
from logging_setup import configure_logging
from mirror import (
    HEARTBEAT_SECONDS,
    KEY_DESIRED,
    KEY_HEARTBEAT,
    KEY_STARTED,
    KEY_STATE,
)
from security import DEFAULT_ADMIN_PASSWORD, DEFAULT_ADMIN_USERNAME, hash_password, verify_password
from storage import Storage
from webhook import webhook_api
from pull_api import handle_keys, handle_pull

log = logging.getLogger("ui")

UI_FILE = PROJECT_DIR / "ui" / "index.html"
TOKEN_TTL = timedelta(hours=12)
STALE_AFTER = timedelta(seconds=HEARTBEAT_SECONDS * 5)
STOP_TIMEOUT_SECONDS = 30

_tokens: dict[str, datetime] = {}
_tokens_lock = threading.Lock()
_mirror_proc: subprocess.Popen | None = None
_settings: Settings | None = None


# ---- auth ----------------------------------------------------------------------

def _issue_token() -> str:
    token = secrets.token_urlsafe(32)
    with _tokens_lock:
        _tokens[token] = datetime.now(timezone.utc) + TOKEN_TTL
    return token


def _token_valid(token: str | None) -> bool:
    if not token:
        return False
    with _tokens_lock:
        expires = _tokens.get(token)
        if expires is None or expires < datetime.now(timezone.utc):
            _tokens.pop(token, None)
            return False
        return True


def _drop_token(token: str | None) -> None:
    with _tokens_lock:
        if token:
            _tokens.pop(token, None)


def ensure_admin(storage: Storage) -> None:
    """Create the default admin on first run. Only the hash is stored."""
    if storage.get_app_user(DEFAULT_ADMIN_USERNAME) is None:
        storage.create_app_user(DEFAULT_ADMIN_USERNAME, hash_password(DEFAULT_ADMIN_PASSWORD))
        log.info("Created UI admin account '%s'", DEFAULT_ADMIN_USERNAME)


# ---- mirror control ------------------------------------------------------------

def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def mirror_status(storage: Storage) -> dict[str, Any]:
    state = storage.kv_get(KEY_STATE) or "stopped"
    heartbeat = _parse_iso(storage.kv_get(KEY_HEARTBEAT))
    fresh = heartbeat is not None and datetime.now(timezone.utc) - heartbeat < STALE_AFTER
    running = state == "running" and fresh
    return {
        "running": running,
        "state": state if running or state != "running" else "stale",
        "started_utc": storage.kv_get(KEY_STARTED),
        "heartbeat_utc": storage.kv_get(KEY_HEARTBEAT),
    }


def start_mirror(storage: Storage) -> dict[str, Any]:
    global _mirror_proc
    status = mirror_status(storage)
    if status["running"]:
        return status
    if _mirror_proc is not None and _mirror_proc.poll() is None:
        return {**status, "state": "starting"}

    storage.kv_set(KEY_DESIRED, "run")
    assert _settings is not None
    _settings.log_dir.mkdir(parents=True, exist_ok=True)
    stdout = open(_settings.log_dir / "mirror_stdout.log", "a", encoding="utf-8")
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    _mirror_proc = subprocess.Popen(
        [sys.executable, str(PROJECT_DIR / "mirror.py")],
        cwd=str(PROJECT_DIR),
        stdout=stdout,
        stderr=subprocess.STDOUT,
        creationflags=flags,
    )
    log.info("Started mirror process pid=%s", _mirror_proc.pid)
    return {**mirror_status(storage), "state": "starting"}


def stop_mirror(storage: Storage) -> dict[str, Any]:
    global _mirror_proc
    storage.kv_set(KEY_DESIRED, "stop")
    deadline = time.monotonic() + STOP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if not mirror_status(storage)["running"]:
            break
        time.sleep(0.5)
    if _mirror_proc is not None and _mirror_proc.poll() is None:
        if mirror_status(storage)["running"]:
            log.warning("Mirror did not stop in time; terminating process")
            _mirror_proc.terminate()
    _mirror_proc = None
    return mirror_status(storage)


# ---- query builders ------------------------------------------------------------

_MESSAGE_SELECT = """
SELECT m.chat_id,
       COALESCE(c.title, CAST(m.chat_id AS TEXT))         AS chat_title,
       c.username                                          AS chat_username,
       c.entity_type                                       AS chat_type,
       m.message_id,
       m.date_utc,
       m.sender_id,
       COALESCE(NULLIF(TRIM(COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')), ''),
                u.username, CAST(m.sender_id AS TEXT))     AS sender_name,
       u.username                                          AS sender_username,
       m.text,
       m.edited_utc,
       m.deleted_utc,
       m.has_media,
       mm.media_type,
       mm.file_name,
       mm.download_status,
       mm.local_path
FROM messages m
LEFT JOIN channels c ON c.chat_id = m.chat_id
LEFT JOIN users u ON u.user_id = m.sender_id
LEFT JOIN media_metadata mm ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
"""


def _message_filters(q: dict[str, str]) -> tuple[str, list[Any]]:
    where: list[str] = []
    params: list[Any] = []

    chat = q.get("chat", "").strip()
    if chat:
        like = f"%{chat.lstrip('@')}%"
        where.append("(CAST(m.chat_id AS TEXT) = ? OR c.username LIKE ? OR c.title LIKE ?)")
        params += [chat, like, f"%{chat}%"]

    sender = q.get("sender", "").strip()
    if sender:
        like = f"%{sender.lstrip('@')}%"
        where.append("(CAST(m.sender_id AS TEXT) = ? OR u.username LIKE ? "
                     "OR (COALESCE(u.first_name,'') || ' ' || COALESCE(u.last_name,'')) LIKE ?)")
        params += [sender, like, f"%{sender}%"]

    text = q.get("q", "").strip()
    if text:
        where.append("m.text LIKE ?")
        params.append(f"%{text}%")

    if q.get("from"):
        where.append("substr(m.date_utc, 1, 10) >= ?")
        params.append(q["from"][:10])
    if q.get("to"):
        where.append("substr(m.date_utc, 1, 10) <= ?")
        params.append(q["to"][:10])

    if q.get("status") == "deleted":
        where.append("m.deleted_utc IS NOT NULL")
    elif q.get("status") == "edited":
        where.append("m.edited_utc IS NOT NULL")
    elif q.get("status") == "live":
        where.append("m.deleted_utc IS NULL")

    clause = (" WHERE " + " AND ".join(where)) if where else ""
    return clause, params


def query_messages(storage: Storage, q: dict[str, str], *, limit: int, offset: int) -> list[dict[str, Any]]:
    clause, params = _message_filters(q)
    sql = _MESSAGE_SELECT + clause + " ORDER BY m.date_utc DESC, m.message_id DESC LIMIT ? OFFSET ?"
    return storage.fetch_all(sql, [*params, limit, offset])


def query_events(storage: Storage, *, since_id: int = 0, limit: int = 200, ascending: bool = False) -> list[dict[str, Any]]:
    order = "ASC" if ascending else "DESC"
    sql = (
        "SELECT e.id, e.occurred_utc, e.event_type, e.chat_id, c.title AS chat_title, "
        "c.username AS chat_username, e.message_id, e.actor_id, e.payload_json "
        "FROM events e LEFT JOIN channels c ON c.chat_id = e.chat_id "
        f"WHERE e.id > ? ORDER BY e.id {order} LIMIT ?"
    )
    rows = storage.fetch_all(sql, [since_id, limit])
    for row in rows:
        row["payload"] = json.loads(row.pop("payload_json") or "{}")
    return rows


# ---- HTTP handler --------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    server_version = "TelegramMirrorUI"

    def log_message(self, fmt: str, *args: Any) -> None:  # quieter than the default
        log.debug("%s - %s", self.address_string(), fmt % args)

    # helpers
    def _storage(self) -> Storage:
        assert _settings is not None
        return Storage(_settings.db_path)

    def _query(self) -> dict[str, str]:
        parsed = parse_qs(urlparse(self.path).query, keep_blank_values=True)
        return {k: v[-1] for k, v in parsed.items()}

    def _token(self, q: dict[str, str]) -> str | None:
        auth = self.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            return auth[7:]
        return q.get("token")  # used by links and EventSource, which cannot set headers

    def _json(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            return json.loads(raw or b"{}")
        except json.JSONDecodeError:
            return {}

    def _require_auth(self, q: dict[str, str]) -> bool:
        if _token_valid(self._token(q)):
            return True
        self._json(401, {"error": "login required"})
        return False

    # routes
    def do_GET(self) -> None:
        path = urlparse(self.path).path
        q = self._query()
        if path in ("/", "/index.html"):
            return self._serve_ui()
        if path.startswith("/media/"):
            return self._serve_media(path, q)
        if path.startswith("/api/v1/"):
            return self._pull(path, q)
        if not path.startswith("/api/"):
            return self._json(404, {"error": "not found"})
        if not self._require_auth(q):
            return
        if path.startswith("/api/webhooks"):
            return self._webhooks("GET", path, q, {})
        if path.startswith("/api/keys"):
            return self._keys("GET", path, {})

        storage = self._storage()
        try:
            if path == "/api/status":
                self._json(200, mirror_status(storage))
            elif path == "/api/messages":
                limit = min(int(q.get("limit", "100") or 100), 500)
                offset = max(int(q.get("offset", "0") or 0), 0)
                self._json(200, query_messages(storage, q, limit=limit, offset=offset))
            elif path == "/api/events":
                since = int(q.get("since", "0") or 0)
                self._json(200, query_events(storage, since_id=since, limit=min(int(q.get("limit", "200") or 200), 500)))
            elif path == "/api/chats":
                self._json(200, storage.fetch_all(
                    "SELECT c.chat_id, c.title, c.username, c.entity_type, c.participants_count, c.about, "
                    "(SELECT COUNT(*) FROM messages m WHERE m.chat_id = c.chat_id) AS message_count, "
                    "(SELECT COUNT(*) FROM user_chats uc WHERE uc.chat_id = c.chat_id) AS poster_count "
                    "FROM channels c ORDER BY c.title COLLATE NOCASE"))
            elif path == "/api/people":
                self._json(200, storage.fetch_all(
                    "SELECT u.user_id, u.first_name, u.last_name, u.username, u.is_bot, u.is_self, "
                    "(SELECT COUNT(*) FROM user_chats uc WHERE uc.user_id = u.user_id) AS chat_count, "
                    "(SELECT COUNT(*) FROM messages m WHERE m.sender_id = u.user_id) AS message_count "
                    "FROM users u ORDER BY message_count DESC, u.user_id LIMIT 1000"))
            elif path == "/api/user-chats":
                self._json(200, storage.fetch_all(
                    "SELECT uc.user_id, uc.chat_id, c.title AS chat_title, c.username AS chat_username, "
                    "uc.first_message_utc, uc.last_message_utc FROM user_chats uc "
                    "LEFT JOIN channels c ON c.chat_id = uc.chat_id WHERE uc.user_id = ? "
                    "ORDER BY uc.last_message_utc DESC", [int(q.get("user_id", "0") or 0)]))
            elif path == "/api/export":
                self._export(storage, q)
            elif path == "/api/stream":
                self._stream(q)
            else:
                self._json(404, {"error": "not found"})
        finally:
            if not path.endswith("/stream") and not path.endswith("/export"):
                storage.close()

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        body = self._read_json()

        if path == "/api/login":
            storage = self._storage()
            try:
                user = storage.get_app_user(str(body.get("username", "")))
                if user and verify_password(str(body.get("password", "")), user["password_hash"]):
                    return self._json(200, {"token": _issue_token(), "username": user["username"]})
            finally:
                storage.close()
            return self._json(401, {"error": "invalid username or password"})

        q = self._query()
        if not self._require_auth(q):
            return
        if path.startswith("/api/webhooks"):
            return self._webhooks("POST", path, q, body)
        if path.startswith("/api/keys"):
            return self._keys("POST", path, body)
        if path == "/api/logout":
            _drop_token(self._token(q))
            return self._json(200, {"ok": True})

        storage = self._storage()
        try:
            if path == "/api/mirror/start":
                self._json(200, start_mirror(storage))
            elif path == "/api/mirror/stop":
                self._json(200, stop_mirror(storage))
            else:
                self._json(404, {"error": "not found"})
        finally:
            storage.close()

    def _webhooks(self, method: str, path: str, q: dict[str, str], body: dict[str, Any]) -> None:
        storage = self._storage()
        try:
            status, payload = webhook_api(storage, method, path, q, body)
        finally:
            storage.close()
        self._json(status, payload)

    def _keys(self, method: str, path: str, body: dict[str, Any]) -> None:
        storage = self._storage()
        try:
            status, payload = handle_keys(storage, method, path, body)
        finally:
            storage.close()
        self._json(status, payload)

    def _pull(self, path: str, q: dict[str, str]) -> None:
        """Read-only feed for external clients. Authenticated by API key, not by UI login."""
        storage = self._storage()
        try:
            status, payload = handle_pull(storage, path, q, self.headers.get("Authorization", ""))
        finally:
            storage.close()
        self._json(status, payload)

    # UI and media
    def _serve_ui(self) -> None:
        body = UI_FILE.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_media(self, path: str, q: dict[str, str]) -> None:
        if not _token_valid(self._token(q)):
            return self._json(401, {"error": "login required"})
        parts = path.strip("/").split("/")
        if len(parts) != 3 or not parts[1].lstrip("-").isdigit() or not parts[2].isdigit():
            return self._json(400, {"error": "bad media path"})
        storage = self._storage()
        try:
            row = storage.get_media(int(parts[1]), int(parts[2]))
        finally:
            storage.close()
        if not row or not row.get("local_path"):
            return self._json(404, {"error": "no local file for this message"})

        # Serve only files inside the media folder, whatever the database says.
        assert _settings is not None
        target = Path(row["local_path"]).resolve()
        root = _settings.media_dir.resolve()
        if root not in target.parents or not target.is_file():
            return self._json(404, {"error": "file missing"})

        ctype = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
        size = target.stat().st_size
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(size))
        self.send_header("Content-Disposition", f'inline; filename="{target.name}"')
        self.end_headers()
        with target.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                self.wfile.write(chunk)

    # export and live stream
    def _export(self, storage: Storage, q: dict[str, str]) -> None:
        kind = q.get("type", "messages")
        buf = io.StringIO()
        writer = csv.writer(buf)
        if kind == "events":
            rows = query_events(storage, since_id=0, limit=1_000_000, ascending=True)
            writer.writerow(["id", "occurred_utc", "event_type", "chat_title", "chat_username", "message_id", "actor_id", "payload"])
            for r in rows:
                writer.writerow([r["id"], r["occurred_utc"], r["event_type"], r["chat_title"], r["chat_username"],
                                 r["message_id"], r["actor_id"], json.dumps(r["payload"], ensure_ascii=False)])
            filename = "events.csv"
        else:
            rows = query_messages(storage, q, limit=1_000_000, offset=0)
            cols = ["chat_title", "chat_username", "chat_id", "message_id", "date_utc", "sender_name",
                    "sender_username", "sender_id", "text", "edited_utc", "deleted_utc", "media_type",
                    "download_status", "local_path"]
            writer.writerow(cols)
            for r in rows:
                writer.writerow([r.get(c) for c in cols])
            filename = "messages.csv"
        body = buf.getvalue().encode("utf-8-sig")  # BOM so Excel reads Indonesian text correctly
        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        storage.close()

    def _stream(self, q: dict[str, str]) -> None:
        storage = self._storage()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        last_id = int(q.get("since", "0") or 0)
        if last_id == 0:
            rows = storage.fetch_all("SELECT COALESCE(MAX(id), 0) AS id FROM events")
            last_id = rows[0]["id"]
        try:
            while True:
                rows = query_events(storage, since_id=last_id, limit=200, ascending=True)
                for r in rows:
                    last_id = r["id"]
                    self.wfile.write(f"id: {r['id']}\ndata: {json.dumps(r, ensure_ascii=False, default=str)}\n\n".encode("utf-8"))
                if not rows:
                    self.wfile.write(b": ping\n\n")
                self.wfile.flush()
                time.sleep(1.5)
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            storage.close()


# ---- startup -------------------------------------------------------------------

def main() -> int:
    global _settings
    try:
        _settings = load_settings()
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    configure_logging(_settings.log_dir, _settings.log_level, _settings.log_max_bytes, _settings.log_backup_count)

    storage = Storage(_settings.db_path)
    ensure_admin(storage)
    storage.close()

    host, port = _settings.ui_host, _settings.ui_port
    if host != "127.0.0.1":
        log.warning("UI_HOST is not 127.0.0.1; the UI would be reachable from other machines")
    server = ThreadingHTTPServer((host, port), Handler)
    url = f"http://{host}:{port}/"
    log.info("UI running at %s  (login: %s / the initial password)", url, DEFAULT_ADMIN_USERNAME)
    print(f"\nTelegram Mirror UI: {url}\nTekan Ctrl+C untuk menutup.\n")
    threading.Timer(1.0, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("UI stopped")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
