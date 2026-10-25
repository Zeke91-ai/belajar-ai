"""Async wrapper around TelegramClient: FloodWait handling, exponential backoff, resumable iteration."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime
from types import TracebackType
from typing import Any, TypeVar

from telethon import TelegramClient
from telethon.errors import FloodWaitError, ServerError
from telethon.tl.custom import Message
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest
from telethon.tl.types import Channel, Chat

from scraper import fetch_channel_history

log = logging.getLogger("crawler")

T = TypeVar("T")

# Network-level failures worth retrying. FloodWaitError is handled on its own path.
TRANSIENT_ERRORS: tuple[type[BaseException], ...] = (
    ConnectionError,
    OSError,
    TimeoutError,
    ServerError,
)


class NotAuthorizedError(RuntimeError):
    """The session file does not contain a logged-in user."""


class CrawlStopped(Exception):
    """A shutdown was requested while the crawler was waiting."""


class TelegramCrawler:
    """Wraps a TelegramClient with a single retry policy.

    FloodWaitError: sleeps for the server-specified time plus a small buffer.
    Transient network errors: exponential backoff with jitter, capped at max_backoff.
    Shutdown: request_stop() wakes any pending sleep and lets the current batch finish.
    """

    def __init__(
        self,
        client: TelegramClient,
        *,
        max_retries: int = 5,
        base_delay: float = 2.0,
        max_backoff: float = 300.0,
        max_flood_waits: int = 10,
        flood_buffer: float = 1.0,
    ) -> None:
        self.client = client
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_backoff = max_backoff
        self.max_flood_waits = max_flood_waits
        self.flood_buffer = flood_buffer
        self.me: Any = None
        self._stop = asyncio.Event()

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    def request_stop(self) -> None:
        if not self._stop.is_set():
            log.warning("Shutdown requested; finishing the current batch")
        self._stop.set()

    async def __aenter__(self) -> TelegramCrawler:
        try:
            await self.connect()
        except BaseException:
            await self.disconnect()
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.disconnect()

    async def connect(self) -> None:
        await self._call(self._ensure_connected, "connect")
        authorized = await self._call(self.client.is_user_authorized, "authorization check")
        if not authorized:
            raise NotAuthorizedError("Telegram session is not authorized. Run `python auth.py` first.")
        self.me = await self._call(self.client.get_me, "get_me")
        log.info("Connected to Telegram as user id=%s", self.me.id)

    async def chat_details(self, entity: Any) -> tuple[int | None, str | None]:
        """Return (participants_count, about) for a group or channel.

        Non-fatal: restricted or unavailable details yield (None, None) and the crawl continues.
        """
        try:
            if isinstance(entity, Channel):
                full = await self._call(
                    lambda: self.client(GetFullChannelRequest(entity)), "channel details"
                )
            elif isinstance(entity, Chat):
                full = await self._call(
                    lambda: self.client(GetFullChatRequest(entity.id)), "chat details"
                )
            else:
                return None, None
        except CrawlStopped:
            raise
        except Exception as exc:
            log.warning("Could not read group details: %s", exc)
            return None, None
        full_chat = full.full_chat
        return getattr(full_chat, "participants_count", None), getattr(full_chat, "about", None) or None

    async def disconnect(self) -> None:
        if self.client.is_connected():
            await self.client.disconnect()
            log.info("Disconnected from Telegram")

    async def resolve(self, identifier: str) -> Any:
        return await self._call(lambda: self.client.get_entity(identifier), f"resolve {identifier}")

    async def crawl_channel(
        self,
        entity: Any,
        *,
        limit: int | None,
        descending: bool,
        offset_id: int = 0,
        min_id: int = 0,
        offset_date: datetime | None = None,
    ) -> AsyncIterator[Message]:
        """Yield messages, resuming from the last yielded message after any retryable failure.

        descending=True walks backwards from offset_id (backfill).
        descending=False walks forward from min_id (incremental).
        The cursor moves with each yielded message, so a retry never re-yields.
        """
        yielded = 0
        cursor_offset = offset_id
        cursor_min = min_id
        pending_date = offset_date
        attempts = 0
        flood_waits = 0

        while True:
            remaining = None if limit is None else limit - yielded
            if remaining is not None and remaining <= 0:
                return
            try:
                await self._ensure_connected()
                source = fetch_channel_history(
                    self.client,
                    entity,
                    limit=remaining,
                    offset_date=pending_date,
                    offset_id=cursor_offset if descending else 0,
                    min_id=0 if descending else cursor_min,
                    reverse=not descending,
                )
                async for message in source:
                    yielded += 1
                    if descending:
                        cursor_offset = message.id
                    else:
                        cursor_min = message.id
                    pending_date = None
                    yield message
                    if self._stop.is_set():
                        return
                return

            except FloodWaitError as exc:
                flood_waits += 1
                if flood_waits > self.max_flood_waits:
                    log.error("FloodWait limit reached while crawling; giving up on this pass")
                    raise
                wait = exc.seconds + self.flood_buffer
                log.warning(
                    "FloodWait: Telegram requires %ds; sleeping %.0fs (%d/%d)",
                    exc.seconds, wait, flood_waits, self.max_flood_waits,
                )
                try:
                    await self._pause(wait)
                except CrawlStopped:
                    return

            except TRANSIENT_ERRORS as exc:
                attempts += 1
                if attempts > self.max_retries:
                    log.error("Giving up after %d transient failures: %s", attempts, exc)
                    raise
                delay = self._backoff(attempts)
                log.warning(
                    "Network error while crawling (%s); retry %d/%d in %.1fs",
                    exc, attempts, self.max_retries, delay,
                )
                try:
                    await self._pause(delay)
                except CrawlStopped:
                    return

    async def _call(self, factory: Callable[[], Awaitable[T]], description: str) -> T:
        """Run a single Telegram RPC with the same retry policy as crawl_channel."""
        attempts = 0
        flood_waits = 0
        while True:
            try:
                await self._ensure_connected()
                return await factory()

            except FloodWaitError as exc:
                flood_waits += 1
                if flood_waits > self.max_flood_waits:
                    log.error("FloodWait limit reached during %s", description)
                    raise
                wait = exc.seconds + self.flood_buffer
                log.warning(
                    "FloodWait during %s: sleeping %.0fs (%d/%d)",
                    description, wait, flood_waits, self.max_flood_waits,
                )
                await self._pause(wait)

            except TRANSIENT_ERRORS as exc:
                attempts += 1
                if attempts > self.max_retries:
                    log.error("Giving up on %s after %d attempts: %s", description, attempts, exc)
                    raise
                delay = self._backoff(attempts)
                log.warning(
                    "Transient error during %s (%s); retry %d/%d in %.1fs",
                    description, exc, attempts, self.max_retries, delay,
                )
                await self._pause(delay)

    async def _ensure_connected(self) -> None:
        if not self.client.is_connected():
            await self.client.connect()

    def _backoff(self, attempt: int) -> float:
        """Exponential backoff with equal jitter: half fixed, half random."""
        cap = min(self.max_backoff, self.base_delay * (2 ** (attempt - 1)))
        return cap / 2 + random.uniform(0, cap / 2)

    async def _pause(self, seconds: float) -> None:
        """Sleep, but wake immediately and raise CrawlStopped if shutdown is requested."""
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            return
        raise CrawlStopped()
