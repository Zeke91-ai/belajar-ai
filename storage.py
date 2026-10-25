"""SQLite persistence: schema, migrations, transactional writes, checkpoints, activity log.

Every write runs in one explicit transaction. A failed write leaves no partial state,
so a resumed run never skips messages that were not saved.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Sequence
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from types import TracebackType
from typing import Any

from models import (
    ChannelRecord,
    CrawlState,
    EventRecord,
    MediaRecord,
    MessageRecord,
    UserRecord,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS channels (
    chat_id             INTEGER PRIMARY KEY,
    title               TEXT NOT NULL,
    username            TEXT,
    entity_type         TEXT NOT NULL,
    participants_count  INTEGER,
    about               TEXT,
    first_seen_utc      TEXT NOT NULL,
    updated_utc         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    user_id         INTEGER PRIMARY KEY,
    first_name      TEXT,
    last_name       TEXT,
    username        TEXT,
    is_bot          INTEGER NOT NULL DEFAULT 0 CHECK (is_bot IN (0, 1)),
    is_self         INTEGER NOT NULL DEFAULT 0 CHECK (is_self IN (0, 1)),
    first_seen_utc  TEXT NOT NULL,
    updated_utc     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user_chats (
    user_id            INTEGER NOT NULL REFERENCES users (user_id),
    chat_id            INTEGER NOT NULL REFERENCES channels (chat_id),
    first_message_utc  TEXT NOT NULL,
    last_message_utc   TEXT NOT NULL,
    PRIMARY KEY (user_id, chat_id)
);

CREATE TABLE IF NOT EXISTS messages (
    chat_id          INTEGER NOT NULL REFERENCES channels (chat_id),
    message_id       INTEGER NOT NULL,
    sender_id        INTEGER,
    date_utc         TEXT NOT NULL,
    text             TEXT,
    reply_to_msg_id  INTEGER,
    views            INTEGER,
    forwards         INTEGER,
    has_media        INTEGER NOT NULL DEFAULT 0 CHECK (has_media IN (0, 1)),
    fetched_utc      TEXT NOT NULL,
    PRIMARY KEY (chat_id, message_id)
);
CREATE INDEX IF NOT EXISTS ix_messages_date ON messages (chat_id, date_utc);
CREATE INDEX IF NOT EXISTS ix_messages_sender ON messages (sender_id);

CREATE TABLE IF NOT EXISTS media_metadata (
    chat_id     INTEGER NOT NULL,
    message_id  INTEGER NOT NULL,
    media_type  TEXT NOT NULL,
    mime_type   TEXT,
    file_name   TEXT,
    file_size   INTEGER,
    PRIMARY KEY (chat_id, message_id),
    FOREIGN KEY (chat_id, message_id) REFERENCES messages (chat_id, message_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS crawl_state (
    chat_id            INTEGER PRIMARY KEY REFERENCES channels (chat_id),
    newest_message_id  INTEGER NOT NULL DEFAULT 0,
    oldest_message_id  INTEGER,
    backfill_complete  INTEGER NOT NULL DEFAULT 0 CHECK (backfill_complete IN (0, 1)),
    updated_utc        TEXT NOT NULL
);

-- Append-only activity log. The UI's live feed reads rows with id > last seen id.
CREATE TABLE IF NOT EXISTS events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id       INTEGER,
    message_id    INTEGER,
    event_type    TEXT NOT NULL,
    actor_id      INTEGER,
    payload_json  TEXT NOT NULL,
    occurred_utc  TEXT NOT NULL,
    recorded_utc  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_events_chat ON events (chat_id, occurred_utc);

-- Webhook subscribers. cursor_seq is the last event this subscriber has settled
-- (delivered or dead-lettered). Events are sent strictly in seq order after it.
CREATE TABLE IF NOT EXISTS webhook_subscriptions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    name                TEXT NOT NULL,
    url                 TEXT NOT NULL,
    secret              TEXT NOT NULL,
    enabled             INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    cursor_seq          INTEGER NOT NULL DEFAULT 0,
    attempts            INTEGER NOT NULL DEFAULT 0,
    first_failure_utc   TEXT,
    next_attempt_utc    TEXT,
    last_error          TEXT,
    last_status         INTEGER,
    last_success_utc    TEXT,
    created_utc         TEXT NOT NULL,
    updated_utc         TEXT NOT NULL
);

-- Events that exhausted their retries. Kept with their exact body so they can be replayed.
CREATE TABLE IF NOT EXISTS webhook_dead_letters (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    subscription_id  INTEGER NOT NULL REFERENCES webhook_subscriptions (id) ON DELETE CASCADE,
    seq              INTEGER NOT NULL,
    event_id         TEXT NOT NULL,
    event_type       TEXT NOT NULL,
    body             TEXT NOT NULL,
    attempts         INTEGER NOT NULL,
    last_error       TEXT,
    failed_utc       TEXT NOT NULL,
    replayed_utc     TEXT
);
CREATE INDEX IF NOT EXISTS ix_dead_letters_sub ON webhook_dead_letters (subscription_id, failed_utc);

-- API keys for the read-only pull API (/api/v1). Only SHA-256 hashes are stored.
CREATE TABLE IF NOT EXISTS api_keys (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT NOT NULL,
    key_hash      TEXT NOT NULL UNIQUE,
    key_prefix    TEXT NOT NULL,
    created_utc   TEXT NOT NULL,
    last_used_utc TEXT,
    revoked_utc   TEXT
);

-- Login accounts for the local UI. Only salted PBKDF2 hashes are stored.
CREATE TABLE IF NOT EXISTS app_users (
    username       TEXT PRIMARY KEY,
    password_hash  TEXT NOT NULL,
    created_utc    TEXT NOT NULL
);

-- Key/value control channel between the UI process and the mirror process.
CREATE TABLE IF NOT EXISTS service_status (
    key          TEXT PRIMARY KEY,
    value        TEXT,
    updated_utc  TEXT NOT NULL
);
"""

# Columns added after the first release. Applied only when missing, so existing
# databases upgrade in place without losing data.
_MIGRATIONS: tuple[tuple[str, str, str], ...] = (
    ("messages", "edited_utc", "TEXT"),
    ("messages", "deleted_utc", "TEXT"),
    ("media_metadata", "local_path", "TEXT"),
    ("media_metadata", "download_status", "TEXT NOT NULL DEFAULT 'pending'"),
)

_UPSERT_CHANNEL = """
INSERT INTO channels (chat_id, title, username, entity_type, participants_count, about,
                      first_seen_utc, updated_utc)
VALUES (:chat_id, :title, :username, :entity_type, :participants_count, :about, :now, :now)
ON CONFLICT (chat_id) DO UPDATE SET
    title              = excluded.title,
    username           = COALESCE(excluded.username, channels.username),
    entity_type        = excluded.entity_type,
    participants_count = COALESCE(excluded.participants_count, channels.participants_count),
    about              = COALESCE(excluded.about, channels.about),
    updated_utc        = excluded.updated_utc
"""

_UPSERT_USER = """
INSERT INTO users (user_id, first_name, last_name, username, is_bot, is_self,
                   first_seen_utc, updated_utc)
VALUES (:user_id, :first_name, :last_name, :username, :is_bot, :is_self, :now, :now)
ON CONFLICT (user_id) DO UPDATE SET
    first_name  = COALESCE(excluded.first_name, users.first_name),
    last_name   = COALESCE(excluded.last_name, users.last_name),
    username    = COALESCE(excluded.username, users.username),
    is_bot      = MAX(users.is_bot, excluded.is_bot),
    is_self     = MAX(users.is_self, excluded.is_self),
    updated_utc = excluded.updated_utc
"""

_STUB_USER = """
INSERT OR IGNORE INTO users (user_id, is_bot, is_self, first_seen_utc, updated_utc)
VALUES (:user_id, 0, 0, :now, :now)
"""

_UPSERT_USER_CHAT = """
INSERT INTO user_chats (user_id, chat_id, first_message_utc, last_message_utc)
VALUES (:user_id, :chat_id, :first_message_utc, :last_message_utc)
ON CONFLICT (user_id, chat_id) DO UPDATE SET
    first_message_utc = MIN(user_chats.first_message_utc, excluded.first_message_utc),
    last_message_utc  = MAX(user_chats.last_message_utc, excluded.last_message_utc)
"""

# Keyed on (chat_id, message_id). Re-crawling refreshes mutable fields without
# duplicating the row, and never clears edited_utc or deleted_utc.
_UPSERT_MESSAGE = """
INSERT INTO messages (chat_id, message_id, sender_id, date_utc, text, reply_to_msg_id,
                      views, forwards, has_media, fetched_utc)
VALUES (:chat_id, :message_id, :sender_id, :date_utc, :text, :reply_to_msg_id,
        :views, :forwards, :has_media, :now)
ON CONFLICT (chat_id, message_id) DO UPDATE SET
    text        = excluded.text,
    views       = excluded.views,
    forwards    = excluded.forwards,
    has_media   = excluded.has_media,
    fetched_utc = excluded.fetched_utc
"""

# A downloaded file is never reset to pending by a later crawl pass.
_UPSERT_MEDIA = """
INSERT INTO media_metadata (chat_id, message_id, media_type, mime_type, file_name, file_size,
                            local_path, download_status)
VALUES (:chat_id, :message_id, :media_type, :mime_type, :file_name, :file_size,
        :local_path, :download_status)
ON CONFLICT (chat_id, message_id) DO UPDATE SET
    media_type      = excluded.media_type,
    mime_type       = excluded.mime_type,
    file_name       = excluded.file_name,
    file_size       = excluded.file_size,
    local_path      = COALESCE(media_metadata.local_path, excluded.local_path),
    download_status = CASE
        WHEN media_metadata.download_status = 'downloaded' THEN 'downloaded'
        ELSE excluded.download_status END
"""

_UPSERT_STATE = """
INSERT INTO crawl_state (chat_id, newest_message_id, oldest_message_id, backfill_complete, updated_utc)
VALUES (:chat_id, :newest_message_id, :oldest_message_id, :backfill_complete, :now)
ON CONFLICT (chat_id) DO UPDATE SET
    newest_message_id = excluded.newest_message_id,
    oldest_message_id = excluded.oldest_message_id,
    backfill_complete = excluded.backfill_complete,
    updated_utc       = excluded.updated_utc
"""

_INSERT_EVENT = """
INSERT INTO events (chat_id, message_id, event_type, actor_id, payload_json, occurred_utc, recorded_utc)
VALUES (:chat_id, :message_id, :event_type, :actor_id, :payload_json, :occurred_utc, :now)
"""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _user_chat_rows(messages: Sequence[MessageRecord]) -> list[dict[str, object]]:
    """Collapse messages into one membership row per (sender, chat) with the date span."""
    spans: dict[tuple[int, int], list[str]] = {}
    for m in messages:
        if m.sender_id is None or m.sender_id <= 0:
            continue  # channel posts and anonymous senders are not users
        key = (m.sender_id, m.chat_id)
        span = spans.get(key)
        if span is None:
            spans[key] = [m.date_utc, m.date_utc]
        else:
            span[0] = min(span[0], m.date_utc)
            span[1] = max(span[1], m.date_utc)
    return [
        {"user_id": user_id, "chat_id": chat_id, "first_message_utc": first, "last_message_utc": last}
        for (user_id, chat_id), (first, last) in spans.items()
    ]


class Storage:
    """Owns one SQLite connection. Use as a context manager so it always closes."""

    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        # isolation_level=None disables implicit transactions; BEGIN/COMMIT are explicit.
        self._conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA synchronous = NORMAL")
        self._conn.executescript(SCHEMA)
        self._migrate()

    def _migrate(self) -> None:
        for table, column, decl in _MIGRATIONS:
            existing = {row["name"] for row in self._conn.execute(f"PRAGMA table_info({table})")}
            if column not in existing:
                self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> Storage:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    # ---- generic read helper for the UI -------------------------------------

    def fetch_all(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        return [dict(row) for row in self._conn.execute(sql, params).fetchall()]

    # ---- crawl checkpoints ---------------------------------------------------

    def get_state(self, chat_id: int) -> CrawlState | None:
        row = self._conn.execute(
            "SELECT chat_id, newest_message_id, oldest_message_id, backfill_complete "
            "FROM crawl_state WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()
        if row is None:
            return None
        return CrawlState(
            chat_id=row["chat_id"],
            newest_message_id=row["newest_message_id"],
            oldest_message_id=row["oldest_message_id"],
            backfill_complete=bool(row["backfill_complete"]),
        )

    def write_batch(
        self,
        channel: ChannelRecord,
        messages: list[MessageRecord],
        media: list[MediaRecord],
        state: CrawlState,
        users: Iterable[UserRecord] = (),
    ) -> int:
        """Persist a crawl batch and its checkpoint atomically. Returns messages written."""
        self._write(channel, messages, media, list(users), state=state)
        return len(messages)

    def save_checkpoint(self, channel: ChannelRecord, state: CrawlState) -> None:
        self._write(channel, [], [], [], state=state)

    # ---- live mirror writes --------------------------------------------------

    def write_live(
        self,
        channel: ChannelRecord,
        messages: list[MessageRecord],
        media: list[MediaRecord],
        users: Iterable[UserRecord],
        events: Sequence[EventRecord],
    ) -> None:
        """Persist a live message (or service message) and its activity-log rows together."""
        self._write(channel, messages, media, list(users), events=events)

    def record_edit(
        self,
        channel: ChannelRecord,
        message: MessageRecord,
        users: Iterable[UserRecord],
        event: EventRecord,
    ) -> str | None:
        """Apply an edit. Returns the previous text, or None if the message was unknown."""
        now = _utc_now()
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(_UPSERT_CHANNEL, {**asdict(channel), "now": now})
            user_rows = [{**asdict(u), "now": now} for u in users]
            if user_rows:
                conn.executemany(_UPSERT_USER, user_rows)
            previous = conn.execute(
                "SELECT text FROM messages WHERE chat_id = ? AND message_id = ?",
                (message.chat_id, message.message_id),
            ).fetchone()
            conn.execute(_UPSERT_MESSAGE, {**asdict(message), "now": now})
            conn.execute(
                "UPDATE messages SET edited_utc = ? WHERE chat_id = ? AND message_id = ?",
                (event.occurred_utc, message.chat_id, message.message_id),
            )
            conn.execute(_INSERT_EVENT, {**asdict(event), "now": now})
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")
        return previous["text"] if previous else None

    def record_delete(self, chat_id: int, message_id: int, event: EventRecord) -> bool:
        """Mark a message deleted (the row is kept). Returns True if a row was marked."""
        now = _utc_now()
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                "UPDATE messages SET deleted_utc = COALESCE(deleted_utc, ?) "
                "WHERE chat_id = ? AND message_id = ?",
                (event.occurred_utc, chat_id, message_id),
            )
            marked = cur.rowcount > 0
            conn.execute(_INSERT_EVENT, {**asdict(event), "now": now})
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")
        return marked

    def find_undeleted_chats(self, message_id: int) -> list[int]:
        """Chats holding a live copy of this message id. Used when Telegram omits the chat id."""
        rows = self._conn.execute(
            "SELECT chat_id FROM messages WHERE message_id = ? AND deleted_utc IS NULL",
            (message_id,),
        ).fetchall()
        return [row["chat_id"] for row in rows]

    def record_events(self, events: Sequence[EventRecord]) -> None:
        if not events:
            return
        now = _utc_now()
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.executemany(_INSERT_EVENT, [{**asdict(e), "now": now} for e in events])
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")

    # ---- media ---------------------------------------------------------------

    def set_media_status(self, chat_id: int, message_id: int, local_path: str | None, status: str) -> None:
        self._conn.execute(
            "UPDATE media_metadata SET local_path = COALESCE(?, local_path), download_status = ? "
            "WHERE chat_id = ? AND message_id = ?",
            (local_path, status, chat_id, message_id),
        )

    def get_media(self, chat_id: int, message_id: int) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM media_metadata WHERE chat_id = ? AND message_id = ?",
            (chat_id, message_id),
        ).fetchone()
        return dict(row) if row else None

    # ---- key/value control and login accounts -------------------------------

    def kv_get(self, key: str) -> str | None:
        row = self._conn.execute("SELECT value FROM service_status WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else None

    def kv_set(self, key: str, value: str | None) -> None:
        self._conn.execute(
            "INSERT INTO service_status (key, value, updated_utc) VALUES (?, ?, ?) "
            "ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_utc = excluded.updated_utc",
            (key, value, _utc_now()),
        )

    def upsert_users(self, users: Iterable[UserRecord]) -> None:
        self._write_users_only(list(users))

    def get_app_user(self, username: str) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM app_users WHERE username = ?", (username,)).fetchone()
        return dict(row) if row else None

    def create_app_user(self, username: str, password_hash: str) -> None:
        self._conn.execute(
            "INSERT INTO app_users (username, password_hash, created_utc) VALUES (?, ?, ?)",
            (username, password_hash, _utc_now()),
        )

    # ---- webhook subscribers and dead letters -------------------------------

    _SUBSCRIPTION_FIELDS = frozenset({
        "name", "url", "secret", "enabled", "cursor_seq", "attempts", "first_failure_utc",
        "next_attempt_utc", "last_error", "last_status", "last_success_utc",
    })

    def subscriptions_list(self) -> list[dict[str, Any]]:
        return self.fetch_all("SELECT * FROM webhook_subscriptions ORDER BY id")

    def subscription_get(self, sub_id: int) -> dict[str, Any] | None:
        rows = self.fetch_all("SELECT * FROM webhook_subscriptions WHERE id = ?", (sub_id,))
        return rows[0] if rows else None

    def subscription_create(self, name: str, url: str, secret: str, cursor_seq: int) -> int:
        now = _utc_now()
        cur = self._conn.execute(
            "INSERT INTO webhook_subscriptions (name, url, secret, enabled, cursor_seq, attempts, "
            "created_utc, updated_utc) VALUES (?, ?, ?, 1, ?, 0, ?, ?)",
            (name, url, secret, cursor_seq, now, now),
        )
        return int(cur.lastrowid)

    def subscription_set(self, sub_id: int, **fields: Any) -> None:
        unknown = set(fields) - self._SUBSCRIPTION_FIELDS
        if unknown:
            raise ValueError(f"Unknown subscription fields: {sorted(unknown)}")
        if not fields:
            return
        assignments = ", ".join(f"{name} = ?" for name in fields) + ", updated_utc = ?"
        self._conn.execute(
            f"UPDATE webhook_subscriptions SET {assignments} WHERE id = ?",
            (*fields.values(), _utc_now(), sub_id),
        )

    def subscription_delete(self, sub_id: int) -> None:
        self._conn.execute("DELETE FROM webhook_subscriptions WHERE id = ?", (sub_id,))

    def due_subscriptions(self, now_iso: str) -> list[dict[str, Any]]:
        return self.fetch_all(
            "SELECT * FROM webhook_subscriptions WHERE enabled = 1 "
            "AND (next_attempt_utc IS NULL OR next_attempt_utc <= ?) ORDER BY id",
            (now_iso,),
        )

    def next_event_after(self, seq: int) -> dict[str, Any] | None:
        rows = self.fetch_all(
            "SELECT id, occurred_utc, event_type, chat_id, message_id, actor_id, payload_json "
            "FROM events WHERE id > ? ORDER BY id LIMIT 1",
            (seq,),
        )
        return rows[0] if rows else None

    def chat_info(self, chat_id: int) -> dict[str, Any]:
        rows = self.fetch_all(
            "SELECT title, username, entity_type AS type FROM channels WHERE chat_id = ?", (chat_id,)
        )
        base = {"chat_id": chat_id, "title": None, "username": None, "type": None}
        return {**base, **rows[0]} if rows else base

    def max_event_id(self) -> int:
        return int(self.fetch_all("SELECT COALESCE(MAX(id), 0) AS n FROM events")[0]["n"])

    def count_events_after(self, seq: int) -> int:
        return int(self.fetch_all("SELECT COUNT(*) AS n FROM events WHERE id > ?", (seq,))[0]["n"])

    def advance_subscription(
        self,
        sub_id: int,
        seq: int,
        now: str,
        *,
        status: int | None = None,
        error: str | None = None,
        dead: dict[str, Any] | None = None,
    ) -> None:
        """Settle one event for a subscriber: move its cursor and optionally dead-letter it."""
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "UPDATE webhook_subscriptions SET cursor_seq = ?, attempts = 0, first_failure_utc = NULL, "
                "next_attempt_utc = ?, last_error = ?, last_status = ?, updated_utc = ? WHERE id = ?",
                (seq, now, error, status, _utc_now(), sub_id),
            )
            if error is None:
                conn.execute(
                    "UPDATE webhook_subscriptions SET last_success_utc = ? WHERE id = ?", (now, sub_id)
                )
            if dead is not None:
                conn.execute(
                    "INSERT INTO webhook_dead_letters (subscription_id, seq, event_id, event_type, body, "
                    "attempts, last_error, failed_utc) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (sub_id, dead["seq"], dead["event_id"], dead["event_type"], dead["body"],
                     dead["attempts"], dead["error"], dead["failed_utc"]),
                )
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")

    def record_failure(
        self,
        sub_id: int,
        *,
        attempts: int,
        first_failure_utc: str,
        next_attempt_utc: str,
        error: str | None,
        status: int | None,
    ) -> None:
        self._conn.execute(
            "UPDATE webhook_subscriptions SET attempts = ?, first_failure_utc = ?, next_attempt_utc = ?, "
            "last_error = ?, last_status = ?, updated_utc = ? WHERE id = ?",
            (attempts, first_failure_utc, next_attempt_utc, error, status, _utc_now(), sub_id),
        )

    def dead_letters(self, subscription_id: int | None = None, limit: int = 200) -> list[dict[str, Any]]:
        if subscription_id is None:
            return self.fetch_all(
                "SELECT * FROM webhook_dead_letters ORDER BY id DESC LIMIT ?", (limit,)
            )
        return self.fetch_all(
            "SELECT * FROM webhook_dead_letters WHERE subscription_id = ? ORDER BY id DESC LIMIT ?",
            (subscription_id, limit),
        )

    def dead_letter_get(self, dead_id: int) -> dict[str, Any] | None:
        rows = self.fetch_all("SELECT * FROM webhook_dead_letters WHERE id = ?", (dead_id,))
        return rows[0] if rows else None

    def dead_letter_mark_replayed(self, dead_id: int, when: str) -> None:
        self._conn.execute(
            "UPDATE webhook_dead_letters SET replayed_utc = ? WHERE id = ?", (when, dead_id)
        )

    # ---- API keys ------------------------------------------------------------

    def api_key_create(self, name: str, key_hash: str, prefix: str) -> int:
        cur = self._conn.execute(
            "INSERT INTO api_keys (name, key_hash, key_prefix, created_utc) VALUES (?, ?, ?, ?)",
            (name, key_hash, prefix, _utc_now()),
        )
        return int(cur.lastrowid)

    def api_keys_list(self) -> list[dict[str, Any]]:
        return self.fetch_all(
            "SELECT id, name, key_prefix, created_utc, last_used_utc, revoked_utc FROM api_keys ORDER BY id"
        )

    def api_key_by_hash(self, key_hash: str) -> dict[str, Any] | None:
        rows = self.fetch_all(
            "SELECT * FROM api_keys WHERE key_hash = ? AND revoked_utc IS NULL", (key_hash,)
        )
        return rows[0] if rows else None

    def api_key_touch(self, key_id: int) -> None:
        self._conn.execute("UPDATE api_keys SET last_used_utc = ? WHERE id = ?", (_utc_now(), key_id))

    def api_key_revoke(self, key_id: int) -> bool:
        cur = self._conn.execute(
            "UPDATE api_keys SET revoked_utc = ? WHERE id = ? AND revoked_utc IS NULL", (_utc_now(), key_id)
        )
        return cur.rowcount > 0

    def count(self, table: str) -> int:
        allowed = {"channels", "users", "user_chats", "messages", "media_metadata",
                   "crawl_state", "events", "app_users"}
        if table not in allowed:
            raise ValueError(f"Unknown table: {table}")
        row = self._conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
        return int(row["n"])

    # ---- internal ------------------------------------------------------------

    def _write_users_only(self, users: list[UserRecord]) -> None:
        now = _utc_now()
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.executemany(_UPSERT_USER, [{**asdict(u), "now": now} for u in users])
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")

    def _write(
        self,
        channel: ChannelRecord,
        messages: list[MessageRecord],
        media: list[MediaRecord],
        users: list[UserRecord],
        *,
        state: CrawlState | None = None,
        events: Sequence[EventRecord] = (),
    ) -> None:
        now = _utc_now()
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(_UPSERT_CHANNEL, {**asdict(channel), "now": now})

            # Every sender needs a users row before user_chats can reference it.
            # Stubs satisfy the foreign key when Telethon gave no User object.
            known = {u.user_id for u in users}
            for m in messages:
                if m.sender_id and m.sender_id > 0 and m.sender_id not in known:
                    known.add(m.sender_id)
                    conn.execute(_STUB_USER, {"user_id": m.sender_id, "now": now})
            if users:
                conn.executemany(_UPSERT_USER, [{**asdict(u), "now": now} for u in users])

            if messages:
                conn.executemany(_UPSERT_MESSAGE, [{**asdict(m), "now": now} for m in messages])
            if media:
                conn.executemany(_UPSERT_MEDIA, [asdict(m) for m in media])

            memberships = _user_chat_rows(messages)
            if memberships:
                conn.executemany(_UPSERT_USER_CHAT, memberships)

            if events:
                conn.executemany(_INSERT_EVENT, [{**asdict(e), "now": now} for e in events])

            if state is not None:
                conn.execute(_UPSERT_STATE, {**asdict(state), "now": now})
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")
