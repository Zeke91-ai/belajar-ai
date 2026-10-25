"""Entry point: crawl Telegram channels/groups into SQLite with resumable checkpoints.

Examples:
    python main.py --channels @durov,@telegram --limit 1000
    python main.py --channels-file channels.txt --offset-date 2025-01-01

Each channel runs two passes:
    1. backfill    walks backwards from the oldest stored message (or --offset-date)
    2. incremental walks forward from the newest stored message
Re-running later only fetches what is new.

Note: this one-off crawler and the live mirror (mirror.py) share one Telegram session,
so only one of them can run at a time. The UI's start button runs the mirror.

Shutdown: Ctrl+C or SIGTERM finishes the current batch, commits it, and exits.
A second Ctrl+C exits immediately. Work already committed is kept either way.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import FrameType
from typing import Any

from telethon import utils

from auth import SessionLock, SessionLockedError, build_client
from config import ConfigError, Settings, load_settings
from crawler import CrawlStopped, NotAuthorizedError, TelegramCrawler
from logging_setup import configure_logging
from media import download_media
from models import ChannelRecord, CrawlState, UserRecord
from scraper import batched, sender_user, to_records, user_from_entity
from storage import Storage

log = logging.getLogger("main")


def _parse_date(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date/time: {value!r}") from exc
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl Telegram channels/groups into SQLite.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--channels", help="Comma-separated usernames or links, e.g. @durov,https://t.me/telegram")
    source.add_argument("--channels-file", type=Path, help="Text file with one channel per line; # starts a comment")
    parser.add_argument("--limit", type=int, default=1000, help="Max messages per channel per pass (0 = unlimited)")
    parser.add_argument(
        "--offset-date", type=_parse_date,
        help="ISO date/time. On a channel's first backfill, start at messages older than this.",
    )
    parser.add_argument("--batch-size", type=int, default=500, help="Messages per database transaction")
    args = parser.parse_args(argv)
    if args.limit < 0 or args.batch_size < 1:
        parser.error("--limit must be >= 0 and --batch-size must be >= 1")
    return args


def _load_channels(args: argparse.Namespace) -> list[str]:
    if args.channels:
        raw = args.channels.split(",")
    else:
        raw = args.channels_file.read_text(encoding="utf-8").splitlines()
    return [item.strip() for item in raw if item.strip() and not item.strip().startswith("#")]


def display_title(entity: Any) -> str:
    """Human-readable name: group title, or the full name for private chats."""
    title = getattr(entity, "title", None)
    if title:
        return title
    first = getattr(entity, "first_name", None) or ""
    last = getattr(entity, "last_name", None) or ""
    full = f"{first} {last}".strip()
    return full or getattr(entity, "username", None) or str(getattr(entity, "id", "unknown"))


def channel_record_for(entity: Any, participants: int | None = None, about: str | None = None) -> ChannelRecord:
    return ChannelRecord(
        chat_id=utils.get_peer_id(entity),
        title=display_title(entity),
        username=getattr(entity, "username", None),
        entity_type=type(entity).__name__,
        participants_count=participants,
        about=about,
    )


async def _crawl_pass(
    crawler: TelegramCrawler,
    storage: Storage,
    settings: Settings,
    entity: Any,
    channel: ChannelRecord,
    state: CrawlState,
    *,
    descending: bool,
    limit: int | None,
    offset_date: datetime | None,
    batch_size: int,
) -> CrawlState:
    label = "backfill" if descending else "incremental"
    if descending:
        source = crawler.crawl_channel(
            entity,
            limit=limit,
            descending=True,
            offset_id=state.oldest_message_id or 0,
            offset_date=offset_date if state.oldest_message_id is None else None,
        )
    else:
        source = crawler.crawl_channel(
            entity,
            limit=limit,
            descending=False,
            min_id=state.newest_message_id,
        )

    total = 0
    async for batch in batched(source, batch_size):
        messages = []
        media = []
        users: dict[int, UserRecord] = {}
        for raw in batch:
            record, media_record = to_records(channel.chat_id, raw)
            messages.append(record)
            if media_record is not None:
                media.append(media_record)
            sender = sender_user(raw)
            if sender is not None:
                users[sender.user_id] = sender

        ids = [m.message_id for m in messages]
        new_oldest = state.oldest_message_id
        if descending:
            new_oldest = min(ids) if new_oldest is None else min(new_oldest, *ids)
        state = replace(
            state,
            newest_message_id=max(state.newest_message_id, *ids),
            oldest_message_id=new_oldest,
        )
        storage.write_batch(channel, messages, media, state, users.values())

        # Download media after the batch is committed, so a slow download never holds
        # the database transaction open.
        for raw in batch:
            if raw.media is not None:
                path, status = await download_media(crawler.client, raw, settings)
                storage.set_media_status(channel.chat_id, raw.id, path, status)

        total += len(messages)
        log.info("Scanned %d messages from chat %s (%s)", total, channel.title, label)

    # A backfill that finished without hitting its limit has reached the start of history.
    if descending and not crawler.stopping and (limit is None or total < limit):
        state = replace(state, backfill_complete=True)
        storage.save_checkpoint(channel, state)
        log.info("Backfill complete for chat %s", channel.title)

    return state


async def crawl_entity(
    crawler: TelegramCrawler,
    storage: Storage,
    settings: Settings,
    entity: Any,
    *,
    limit: int | None,
    offset_date: datetime | None,
    batch_size: int,
) -> None:
    """Backfill then incrementally crawl one already-resolved chat."""
    participants, about = await crawler.chat_details(entity)
    channel = channel_record_for(entity, participants, about)
    state = storage.get_state(channel.chat_id) or CrawlState.empty(channel.chat_id)
    log.info("Crawling %s (chat_id=%s)", channel.title, channel.chat_id)

    if not state.backfill_complete and not crawler.stopping:
        state = await _crawl_pass(
            crawler, storage, settings, entity, channel, state,
            descending=True, limit=limit, offset_date=offset_date, batch_size=batch_size,
        )

    if not crawler.stopping:
        await _crawl_pass(
            crawler, storage, settings, entity, channel, state,
            descending=False, limit=limit, offset_date=None, batch_size=batch_size,
        )
        log.info("Finished %s", channel.title)


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, crawler: TelegramCrawler) -> None:
    """Route SIGINT and SIGTERM to a graceful stop. Works on Windows and POSIX."""

    def handle(signum: int, _frame: FrameType | None) -> None:
        if crawler.stopping:
            raise KeyboardInterrupt
        loop.call_soon_threadsafe(crawler.request_stop)

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, handle)


async def _run(settings: Settings, args: argparse.Namespace) -> int:
    channels = _load_channels(args)
    if not channels:
        log.error("No channels given")
        return 2

    limit = None if args.limit == 0 else args.limit
    loop = asyncio.get_running_loop()
    failures = 0

    with SessionLock(settings.session_dir, settings.session_name), Storage(settings.db_path) as storage:
        client = build_client(settings)
        async with TelegramCrawler(client) as crawler:
            _install_signal_handlers(loop, crawler)
            if crawler.me is not None:
                storage.upsert_users([user_from_entity(crawler.me, is_self=True)])
            for identifier in channels:
                if crawler.stopping:
                    break
                try:
                    entity = await crawler.resolve(identifier)
                    await crawl_entity(
                        crawler, storage, settings, entity,
                        limit=limit, offset_date=args.offset_date, batch_size=args.batch_size,
                    )
                except CrawlStopped:
                    break
                except Exception:
                    failures += 1
                    log.exception("Failed to crawl %s; continuing with the next channel", identifier)

    if failures:
        log.warning("Run finished with %d failed channel(s)", failures)
    else:
        log.info("Run finished successfully")
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        settings = load_settings()
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    configure_logging(settings.log_dir, settings.log_level, settings.log_max_bytes, settings.log_backup_count)
    try:
        return asyncio.run(_run(settings, args))
    except (NotAuthorizedError, SessionLockedError) as exc:
        log.error("%s", exc)
        return 3
    except KeyboardInterrupt:
        log.warning("Forced exit; committed batches are saved and will resume on the next run")
        return 130


if __name__ == "__main__":
    sys.exit(main())
