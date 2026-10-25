"""Live mirror of the logged-in account.

Run standalone:  python mirror.py
Usually started and stopped from the local UI (ui.py).

What it records, in real time:
  - every new message you receive or send (all private chats and groups)
  - media on those messages: an event right away, then a 'media_update' event when the
    file is downloaded (large files never delay the feed)
  - edits (old text is kept in the activity log; the message row is updated)
  - deletions (the message row is kept and marked deleted_utc)
  - service events in groups: joins, leaves, title and photo changes
  - changes to your own profile name, username, photo and bio

On start it runs a catch-up crawl over all dialogs, so anything that happened while the
mirror was off is backfilled from the stored checkpoints.

Control: the UI writes mirror.desired = 'run' | 'stop' in the service_status table.
The mirror polls that flag every few seconds and shuts down cleanly on 'stop'.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import sys
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from telethon import TelegramClient, events, utils
from telethon.tl.custom import Message
from telethon.tl.types import UpdateUser, UpdateUserName, User

from auth import SessionLock, SessionLockedError, build_client
from config import ConfigError, Settings, load_settings
from crawler import CrawlStopped, NotAuthorizedError, TelegramCrawler
from logging_setup import configure_logging
from main import _install_signal_handlers, channel_record_for, crawl_entity
from media import download_media
from models import ChannelRecord, EventRecord, MediaRecord, UserRecord
from scraper import to_records, user_from_entity
from storage import Storage
from webhook import deliver_once

log = logging.getLogger("mirror")

HEARTBEAT_SECONDS = 2
WEBHOOK_POLL_SECONDS = 2
KEY_DESIRED = "mirror.desired"
KEY_STATE = "mirror.state"
KEY_PID = "mirror.pid"
KEY_HEARTBEAT = "mirror.heartbeat_utc"
KEY_STARTED = "mirror.started_utc"
KEY_ERROR = "mirror.last_error"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _event(
    event_type: str,
    *,
    chat_id: int | None,
    message_id: int | None = None,
    actor_id: int | None = None,
    payload: dict[str, Any] | None = None,
    occurred_utc: str | None = None,
) -> EventRecord:
    return EventRecord(
        chat_id=chat_id,
        message_id=message_id,
        event_type=event_type,
        actor_id=actor_id,
        payload_json=json.dumps(payload or {}, ensure_ascii=False),
        occurred_utc=occurred_utc or _now(),
    )


def media_summary(media: MediaRecord) -> dict[str, Any]:
    """Metadata plus a link. The link is served only with a valid token."""
    available = media.download_status == "downloaded" and bool(media.local_path)
    return {
        "type": media.media_type,
        "file_name": media.file_name,
        "mime_type": media.mime_type,
        "size": media.file_size,
        "status": media.download_status,
        "url": f"/media/{media.chat_id}/{media.message_id}" if available else None,
    }


def _first_username(update: Any) -> str | None:
    """Telethon versions differ: newer ones expose `usernames`, older ones `username`."""
    usernames = getattr(update, "usernames", None)
    if usernames:
        return usernames[0].username
    return getattr(update, "username", None) or None


class Mirror:
    def __init__(self, client: TelegramClient, storage: Storage, settings: Settings, me: User) -> None:
        self.client = client
        self.storage = storage
        self.settings = settings
        self.me = me
        self._tasks: set[asyncio.Task] = set()

    # ---- registration --------------------------------------------------------

    def register(self) -> None:
        self.client.add_event_handler(self.on_new_message, events.NewMessage())
        self.client.add_event_handler(self.on_edited, events.MessageEdited())
        self.client.add_event_handler(self.on_deleted, events.MessageDeleted())
        self.client.add_event_handler(self.on_service, events.ChatAction())
        self.client.add_event_handler(
            self.on_profile_update, events.Raw(types=(UpdateUserName, UpdateUser))
        )

    async def drain(self, timeout: float = 20) -> None:
        """Wait briefly for background media downloads before shutting down."""
        if self._tasks:
            await asyncio.wait(self._tasks, timeout=timeout)

    def _spawn(self, coro: Any) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    # ---- helpers -------------------------------------------------------------

    async def _chat_record(self, message: Message) -> ChannelRecord:
        chat = message.chat or await message.get_chat()
        return channel_record_for(chat)

    async def _sender(self, message: Message) -> UserRecord | None:
        sender = message.sender or await message.get_sender()
        if isinstance(sender, User):
            return user_from_entity(sender, is_self=sender.id == self.me.id)
        return None

    async def _persist_message(
        self,
        message: Message,
        event_type: str,
        payload: dict[str, Any],
        occurred_utc: str,
    ) -> None:
        """Write the message, its sender and a feed event now; fetch media in the background."""
        channel = await self._chat_record(message)
        record, media_record = to_records(channel.chat_id, message)
        sender = await self._sender(message)

        body = dict(payload)
        body["direction"] = "out" if message.out else "in"
        # Snapshot the state at the time of the change, so webhooks send what happened
        # then, not what the row says later.
        body["text"] = message.message or None
        body["reply_to_message_id"] = record.reply_to_msg_id
        body["has_media"] = media_record is not None
        body["sender"] = (
            {"user_id": sender.user_id, "first_name": sender.first_name, "last_name": sender.last_name,
             "username": sender.username, "is_bot": sender.is_bot}
            if sender else None
        )
        if media_record is not None:
            body["media"] = media_summary(media_record)

        feed_event = _event(
            event_type,
            chat_id=channel.chat_id,
            message_id=message.id,
            actor_id=message.sender_id,
            payload=body,
            occurred_utc=occurred_utc,
        )
        self.storage.write_live(
            channel,
            [record],
            [media_record] if media_record else [],
            [sender] if sender else [],
            [feed_event],
        )
        if media_record is not None:
            self._spawn(self._finish_media(message, channel.chat_id, media_record))

    async def _finish_media(self, message: Message, chat_id: int, pending: MediaRecord) -> None:
        """Download the file, then tell the feed where it is (or why it was skipped)."""
        try:
            path, status = await download_media(self.client, message, self.settings)
            self.storage.set_media_status(chat_id, message.id, path, status)
            done = replace(pending, local_path=path, download_status=status)
            self.storage.record_events([_event(
                "media_update",
                chat_id=chat_id,
                message_id=message.id,
                actor_id=message.sender_id,
                payload={"direction": "out" if message.out else "in", "media": media_summary(done)},
            )])
            log.info("Media %s for chat %s (msg %s): %s", pending.media_type, chat_id, message.id, status)
        except Exception:
            log.exception("Media handling failed for message %s", message.id)

    # ---- handlers ------------------------------------------------------------

    async def on_new_message(self, event: events.NewMessage.Event) -> None:
        message = event.message
        kind = "outgoing_message" if message.out else "new_message"
        try:
            await self._persist_message(
                message,
                kind,
                {"text_preview": (message.message or "")[:200]},
                message.date.astimezone(timezone.utc).isoformat(timespec="seconds"),
            )
            log.info("Recorded %s (msg %s)", kind, message.id)
        except Exception:
            log.exception("Failed to record new message %s", message.id)

    async def on_edited(self, event: events.MessageEdited.Event) -> None:
        message = event.message
        chat_id = utils.get_peer_id(message.peer_id)
        try:
            channel = await self._chat_record(message)
            rows = self.storage.fetch_all(
                "SELECT text FROM messages WHERE chat_id = ? AND message_id = ?", (chat_id, message.id)
            )
            old_text = rows[0]["text"] if rows else None
            record, _ = to_records(chat_id, message)
            sender = await self._sender(message)
            self.storage.record_edit(
                channel,
                record,
                [sender] if sender else [],
                _event(
                    "message_edited",
                    chat_id=chat_id,
                    message_id=message.id,
                    actor_id=message.sender_id,
                    payload={"old_text": old_text, "new_text": message.message,
                             "direction": "out" if message.out else "in"},
                ),
            )
            log.info("Recorded edit in chat %s (msg %s)", chat_id, message.id)
        except Exception:
            log.exception("Failed to record edit for message %s", message.id)

    async def on_deleted(self, event: events.MessageDeleted.Event) -> None:
        # Telegram omits the chat id for some private-chat deletions. Only mark a message
        # when exactly one live copy exists, so we never mark the wrong conversation.
        for message_id in event.deleted_ids:
            chat_id = event.chat_id
            if chat_id is None:
                candidates = self.storage.find_undeleted_chats(message_id)
                if len(candidates) != 1:
                    self.storage.record_events([_event(
                        "message_deleted_unresolved",
                        chat_id=None,
                        message_id=message_id,
                        payload={"candidate_chats": candidates},
                    )])
                    log.warning("Deletion of message %s could not be matched to one chat", message_id)
                    continue
                chat_id = candidates[0]
            try:
                marked = self.storage.record_delete(
                    chat_id, message_id, _event("message_deleted", chat_id=chat_id, message_id=message_id)
                )
                log.info("Recorded deletion in chat %s (msg %s, marked=%s)", chat_id, message_id, marked)
            except Exception:
                log.exception("Failed to record deletion of message %s", message_id)

    async def on_service(self, event: events.ChatAction.Event) -> None:
        message = event.action_message
        if message is None:
            return
        action = type(message.action).__name__ if message.action else "Unknown"
        try:
            await self._persist_message(
                message,
                "service_action",
                {"action": action},
                message.date.astimezone(timezone.utc).isoformat(timespec="seconds"),
            )
            log.info("Recorded service action %s", action)
        except Exception:
            log.exception("Failed to record service action")

    async def on_profile_update(self, update: Any) -> None:
        if getattr(update, "user_id", None) != self.me.id:
            return  # only your own profile changes are recorded
        if isinstance(update, UpdateUserName):
            payload = {"first_name": update.first_name, "last_name": update.last_name,
                       "username": _first_username(update)}
            kind = "profile_name_changed"
            self.storage.upsert_users([UserRecord(
                user_id=self.me.id, first_name=payload["first_name"] or None,
                last_name=payload["last_name"] or None, username=payload["username"],
                is_self=True,
            )])
        else:
            # UpdateUser covers photo and bio changes; Telegram does not say which field changed.
            payload, kind = {}, "profile_changed"
        self.storage.record_events([_event(kind, chat_id=None, actor_id=self.me.id, payload=payload)])
        log.info("Recorded profile change: %s", kind)

    # ---- catch-up ------------------------------------------------------------

    async def catch_up(self, crawler: TelegramCrawler, batch_size: int = 500) -> None:
        """Backfill and incrementally crawl every dialog, using stored checkpoints."""
        log.info("Catch-up crawl started")
        async for dialog in self.client.iter_dialogs():
            if crawler.stopping:
                return
            try:
                await crawl_entity(
                    crawler, self.storage, self.settings, dialog.entity,
                    limit=None, offset_date=None, batch_size=batch_size,
                )
            except CrawlStopped:
                return
            except Exception:
                log.exception("Catch-up failed for dialog %s", dialog.name)
        log.info("Catch-up crawl finished")


async def _webhook_loop(db_path: Path) -> None:
    """Deliver pending events to subscribers. Runs in a worker thread so HTTP waits never block capture."""
    while True:
        try:
            await asyncio.to_thread(deliver_once, db_path)
        except Exception:
            log.exception("Webhook delivery cycle failed; retrying")
        await asyncio.sleep(WEBHOOK_POLL_SECONDS)


async def run_mirror(settings: Settings) -> int:
    loop = asyncio.get_running_loop()
    with SessionLock(settings.session_dir, settings.session_name), Storage(settings.db_path) as storage:
        client = build_client(settings)
        crawler = TelegramCrawler(client)
        async with crawler:
            _install_signal_handlers(loop, crawler)
            me = crawler.me
            storage.upsert_users([user_from_entity(me, is_self=True)])
            storage.kv_set(KEY_DESIRED, "run")
            storage.kv_set(KEY_STATE, "running")
            storage.kv_set(KEY_PID, str(os.getpid()))
            storage.kv_set(KEY_STARTED, _now())
            storage.kv_set(KEY_ERROR, None)
            storage.kv_set("account.id", str(me.id))

            mirror = Mirror(client, storage, settings, me)
            mirror.register()
            catch_up = asyncio.create_task(mirror.catch_up(crawler))
            webhook_task = asyncio.create_task(_webhook_loop(settings.db_path))
            log.info("Mirror running for account id=%s", me.id)

            try:
                while not crawler.stopping:
                    storage.kv_set(KEY_HEARTBEAT, _now())
                    if storage.kv_get(KEY_DESIRED) == "stop":
                        log.info("Stop requested from UI")
                        crawler.request_stop()
                        break
                    if catch_up.done() and catch_up.exception() is not None:
                        log.error("Catch-up crashed; live capture continues")
                    await asyncio.sleep(HEARTBEAT_SECONDS)
            finally:
                webhook_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await webhook_task
                catch_up.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await catch_up
                await mirror.drain()
                storage.kv_set(KEY_STATE, "stopped")
                storage.kv_set(KEY_PID, None)
                storage.kv_set(KEY_HEARTBEAT, _now())
    log.info("Mirror stopped")
    return 0


def main() -> int:
    try:
        settings = load_settings()
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    configure_logging(settings.log_dir, settings.log_level, settings.log_max_bytes, settings.log_backup_count)
    try:
        return asyncio.run(run_mirror(settings))
    except (NotAuthorizedError, SessionLockedError) as exc:
        log.error("%s", exc)
        return 3
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
