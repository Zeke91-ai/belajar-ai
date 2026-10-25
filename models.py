"""Plain data records passed between the scraper, the crawler and storage."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ChannelRecord:
    chat_id: int
    title: str
    username: str | None
    entity_type: str
    participants_count: int | None = None
    about: str | None = None


@dataclass(frozen=True, slots=True)
class UserRecord:
    """A Telegram user seen as a message sender, or the logged-in account itself."""

    user_id: int
    first_name: str | None = None
    last_name: str | None = None
    username: str | None = None
    is_bot: bool = False
    is_self: bool = False


@dataclass(frozen=True, slots=True)
class MessageRecord:
    chat_id: int
    message_id: int
    sender_id: int | None
    date_utc: str
    text: str | None
    reply_to_msg_id: int | None
    views: int | None
    forwards: int | None
    has_media: bool


@dataclass(frozen=True, slots=True)
class MediaRecord:
    chat_id: int
    message_id: int
    media_type: str
    mime_type: str | None
    file_name: str | None
    file_size: int | None
    local_path: str | None = None
    # pending | downloaded | skipped_large_video | failed | none
    download_status: str = "pending"


@dataclass(frozen=True, slots=True)
class EventRecord:
    """One row in the activity log (what happened on the account, and when)."""

    chat_id: int | None
    message_id: int | None
    event_type: str
    actor_id: int | None
    payload_json: str
    occurred_utc: str


@dataclass(frozen=True, slots=True)
class CrawlState:
    """Per-chat checkpoint.

    newest_message_id drives forward (incremental) crawling.
    oldest_message_id drives backward (backfill) crawling and is only valid
    while backfill_complete is False.
    """

    chat_id: int
    newest_message_id: int = 0
    oldest_message_id: int | None = None
    backfill_complete: bool = False

    @classmethod
    def empty(cls, chat_id: int) -> CrawlState:
        return cls(chat_id=chat_id)
