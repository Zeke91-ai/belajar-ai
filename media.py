"""Media download policy.

Everything is downloaded except videos larger than settings.max_video_bytes.
Those keep their metadata only, with download_status = 'skipped_large_video'.
Files are named <message_id><ext> under media/<chat_id>/ so they never collide.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from telethon import TelegramClient
from telethon.tl.types import MessageMediaWebPage

from config import Settings
from scraper import _media_kind

log = logging.getLogger("media")


async def download_media(client: TelegramClient, message: Any, settings: Settings) -> tuple[str | None, str]:
    """Return (local_path, download_status) for a message's media."""
    if message.media is None or isinstance(message.media, MessageMediaWebPage):
        return None, "none"

    file_info = message.file
    size = file_info.size if file_info else None
    if _media_kind(message) == "video" and size is not None and size > settings.max_video_bytes:
        return None, "skipped_large_video"

    ext = (file_info.ext if file_info and file_info.ext else "")
    target_dir = settings.media_dir / str(_safe_chat_dir(message.chat_id))
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{message.id}{ext}"

    try:
        saved = await client.download_media(message, file=str(target))
    except Exception as exc:
        log.warning("Media download failed for message %s: %s", message.id, exc)
        return None, "failed"

    if not saved:
        return None, "failed"
    return str(Path(saved).resolve()), "downloaded"


def _safe_chat_dir(chat_id: int | None) -> str:
    # Negative channel ids are fine on Windows, but avoid the leading minus in folder names.
    return f"neg{abs(chat_id)}" if chat_id is not None and chat_id < 0 else str(chat_id or "unknown")
