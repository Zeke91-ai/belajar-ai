"""Message fetching via Telethon and conversion of Telethon objects into storage records."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any

from telethon import TelegramClient
from telethon.tl.custom import Message

from models import MediaRecord, MessageRecord, UserRecord
from telethon.tl.types import User


def user_from_entity(entity: User, *, is_self: bool = False) -> UserRecord:
    """Convert a Telethon User into a storage record. Empty names are stored as NULL."""
    return UserRecord(
        user_id=entity.id,
        first_name=entity.first_name or None,
        last_name=entity.last_name or None,
        username=entity.username or None,
        is_bot=bool(entity.bot),
        is_self=is_self,
    )


def sender_user(message: Message) -> UserRecord | None:
    """Return the sender as a UserRecord when Telethon attached a User object.

    Channel posts and senders not returned in the response yield None; the storage
    layer still records their numeric id where it is needed.
    """
    sender = message.sender
    if isinstance(sender, User):
        return user_from_entity(sender)
    return None


async def fetch_channel_history(
    client: TelegramClient,
    chat_entity: Any,
    limit: int | None = 1000,
    offset_date: datetime | None = None,
    *,
    offset_id: int = 0,
    min_id: int = 0,
    reverse: bool = False,
) -> AsyncIterator[Message]:
    """Yield messages from a chat via client.iter_messages().

    Default order is newest-first. reverse=True yields oldest-first, which forward
    (incremental) crawling needs so the checkpoint advances monotonically.
    limit=None means no cap. offset_date returns messages older than that date.
    """
    async for message in client.iter_messages(
        chat_entity,
        limit=limit,
        offset_date=offset_date,
        offset_id=offset_id,
        min_id=min_id,
        reverse=reverse,
    ):
        yield message


def _iso_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def _media_kind(message: Message) -> str:
    # Order matters: video, gif and voice are also documents in Telegram's schema.
    if message.photo:
        return "photo"
    if message.video:
        return "video"
    if message.gif:
        return "gif"
    if message.sticker:
        return "sticker"
    if message.voice:
        return "voice"
    if message.audio:
        return "audio"
    if message.document:
        return "document"
    return "other"


def to_records(chat_id: int, message: Message) -> tuple[MessageRecord, MediaRecord | None]:
    """Convert a Telethon Message into a message row and an optional media row."""
    has_media = message.media is not None
    record = MessageRecord(
        chat_id=chat_id,
        message_id=message.id,
        sender_id=message.sender_id,
        date_utc=_iso_utc(message.date),
        text=message.message or None,
        reply_to_msg_id=message.reply_to_msg_id,
        views=message.views,
        forwards=message.forwards,
        has_media=has_media,
    )
    if not has_media:
        return record, None

    file_info = message.file
    media = MediaRecord(
        chat_id=chat_id,
        message_id=message.id,
        media_type=_media_kind(message),
        mime_type=file_info.mime_type if file_info else None,
        file_name=file_info.name if file_info else None,
        file_size=file_info.size if file_info else None,
    )
    return record, media


async def batched(source: AsyncIterator[Message], size: int) -> AsyncIterator[list[Message]]:
    """Group an async message stream into lists of up to `size` items.

    The final partial batch is still yielded, so stopping mid-stream loses nothing.
    """
    batch: list[Message] = []
    async for message in source:
        batch.append(message)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
