"""Webhook contract (tm.v1): envelope building, HMAC signing and verification.

Internal event rows are converted into the public envelope defined in
docs/EVENT_CONTRACT.md. The same signing functions are used by the delivery worker
(sender side) and by the reference receiver (verifier side).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from typing import Any

SCHEMA = "tm.v1"
MAX_SKEW_SECONDS = 300

# Fixed namespace, so the same event always maps to the same event_id on every machine.
_EVENT_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "telegram-mirror:tm.v1")

TYPE_MAP: dict[str, str] = {
    "new_message": "message.created",
    "outgoing_message": "message.created",
    "message_edited": "message.edited",
    "message_deleted": "message.deleted",
    "message_deleted_unresolved": "message.deleted_unresolved",
    "media_update": "media.ready",
    "service_action": "chat.service_action",
    "profile_name_changed": "profile.name_changed",
    "profile_changed": "profile.changed",
}


class ContractError(ValueError):
    """An internal event cannot be mapped to the public contract."""


def event_id_for(seq: int) -> str:
    """Stable UUID for an event. Same sequence number, same ID, forever."""
    return str(uuid.uuid5(_EVENT_NAMESPACE, f"event:{seq}"))


def _data_for(event_type: str, row: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    message_id = row.get("message_id")
    if event_type == "message.created":
        return {
            "message_id": message_id,
            "direction": payload.get("direction"),
            "sender": payload.get("sender"),
            "text": payload.get("text"),
            "reply_to_message_id": payload.get("reply_to_message_id"),
            "has_media": bool(payload.get("has_media")),
            "media": payload.get("media"),
        }
    if event_type == "message.edited":
        return {
            "message_id": message_id,
            "direction": payload.get("direction"),
            "old_text": payload.get("old_text"),
            "new_text": payload.get("new_text"),
        }
    if event_type == "message.deleted":
        return {"message_id": message_id}
    if event_type == "message.deleted_unresolved":
        return {"message_id": message_id, "candidate_chats": payload.get("candidate_chats", [])}
    if event_type == "media.ready":
        return {"message_id": message_id, "direction": payload.get("direction"), "media": payload.get("media")}
    if event_type == "chat.service_action":
        return {"message_id": message_id, "action": payload.get("action"), "actor_id": row.get("actor_id")}
    if event_type == "profile.name_changed":
        return {
            "first_name": payload.get("first_name"),
            "last_name": payload.get("last_name"),
            "username": payload.get("username"),
        }
    if event_type == "profile.changed":
        return {}
    raise ContractError(f"no data mapping for {event_type}")


def build_envelope(row: dict[str, Any], *, account_id: int, chat: dict[str, Any] | None) -> dict[str, Any]:
    """Convert one events-table row into a tm.v1 envelope.

    row needs: id, occurred_utc, event_type, message_id, actor_id, payload_json.
    chat is {chat_id, title, username, type} or None for account-level events.
    """
    internal = row["event_type"]
    if internal not in TYPE_MAP:
        raise ContractError(f"unknown internal event type: {internal}")
    event_type = TYPE_MAP[internal]
    payload = json.loads(row.get("payload_json") or "{}")
    return {
        "schema": SCHEMA,
        "event_id": event_id_for(row["id"]),
        "seq": row["id"],
        "type": event_type,
        "occurred_utc": row["occurred_utc"],
        "account_id": account_id,
        "chat": chat,
        "data": _data_for(event_type, row, payload),
    }


def serialize(envelope: dict[str, Any]) -> bytes:
    """Canonical body bytes. The signature is computed over exactly these bytes."""
    return json.dumps(envelope, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def sign(body: bytes, secret: str, timestamp: int) -> str:
    mac = hmac.new(secret.encode("utf-8"), f"{timestamp}.".encode("ascii") + body, hashlib.sha256)
    return "sha256=" + mac.hexdigest()


def verify(
    body: bytes,
    timestamp_header: str,
    signature_header: str,
    secret: str,
    *,
    now: float | None = None,
    max_skew: int = MAX_SKEW_SECONDS,
) -> bool:
    """Receiver-side check: freshness first, then constant-time signature comparison."""
    try:
        timestamp = int(timestamp_header)
    except (TypeError, ValueError):
        return False
    current = time.time() if now is None else now
    if abs(current - timestamp) > max_skew:
        return False
    expected = sign(body, secret, timestamp)
    return hmac.compare_digest(expected, signature_header or "")
