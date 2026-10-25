"""Pull API (/api/v1): a read-only, cursor-based event feed for clients that poll on a timer.

Clients call GET /api/v1/events?after=<seq>&limit=<n> with an API key. The response
contains the same tm.v1 envelopes that webhooks send, so one receiver can serve both options.

Authentication: "Authorization: Bearer tmk_..." (keys are created in the UI, shown once).
Only SHA-256 hashes of keys are stored.
"""

from __future__ import annotations

import hashlib
import secrets
from typing import Any

from contract import ContractError, build_envelope
from storage import Storage

API_PREFIX = "/api/v1"
KEYS_PATH = "/api/keys"
KEY_PREFIX = "tmk_"
MAX_PAGE = 500
DEFAULT_PAGE = 100
KV_ACCOUNT_ID = "account.id"


def hash_key(key: str) -> str:
    # Keys are 256-bit random tokens, so a fast hash is sufficient (no guessing to slow down).
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def new_api_key() -> tuple[str, str, str]:
    """Return (plaintext key, hash for storage, short prefix for display)."""
    key = KEY_PREFIX + secrets.token_urlsafe(32)
    return key, hash_key(key), key[:10]


def authenticate(storage: Storage, authorization_header: str) -> dict[str, Any] | None:
    if not authorization_header.startswith("Bearer "):
        return None
    token = authorization_header[len("Bearer "):].strip()
    if not token.startswith(KEY_PREFIX):
        return None
    row = storage.api_key_by_hash(hash_key(token))
    if row is not None:
        storage.api_key_touch(row["id"])
    return row


def _chat_for(storage: Storage, chat_id: int | None) -> dict[str, Any] | None:
    return storage.chat_info(chat_id) if chat_id is not None else None


def events_page(storage: Storage, account_id: int, after: int, limit: int) -> dict[str, Any]:
    """Events with seq > after, oldest first. Clients resume from next_after."""
    rows = storage.fetch_all(
        "SELECT id, occurred_utc, event_type, chat_id, message_id, actor_id, payload_json "
        "FROM events WHERE id > ? ORDER BY id LIMIT ?",
        (after, limit + 1),
    )
    has_more = len(rows) > limit
    rows = rows[:limit]
    envelopes = []
    for row in rows:
        try:
            envelopes.append(build_envelope(row, account_id=account_id, chat=_chat_for(storage, row["chat_id"])))
        except ContractError:
            continue  # cannot happen for events this mirror writes; skip rather than break the page
    return {
        "schema": "tm.v1",
        "events": envelopes,
        "next_after": rows[-1]["id"] if rows else after,
        "has_more": has_more,
    }


def handle_pull(storage: Storage, path: str, query: dict[str, str], authorization: str) -> tuple[int, Any]:
    """Serve /api/v1/... requests. Returns (http_status, json_payload)."""
    if not authorization:
        return 401, {"error": "API key required: Authorization: Bearer tmk_..."}
    if authenticate(storage, authorization) is None:
        return 401, {"error": "invalid or revoked API key"}

    raw_account = storage.kv_get(KV_ACCOUNT_ID)
    if raw_account is None:
        return 503, {"error": "mirror has not logged in yet"}
    account_id = int(raw_account)

    route = path[len(API_PREFIX):]
    if route == "/cursor":
        return 200, {"head_seq": storage.max_event_id()}
    if route == "/events":
        try:
            after = max(int(query.get("after", "0") or 0), 0)
            limit = min(max(int(query.get("limit", str(DEFAULT_PAGE)) or DEFAULT_PAGE), 1), MAX_PAGE)
        except ValueError:
            return 422, {"error": "after and limit must be whole numbers"}
        return 200, events_page(storage, account_id, after, limit)
    return 404, {"error": "not found"}


def handle_keys(storage: Storage, method: str, path: str, body: dict[str, Any]) -> tuple[int, Any]:
    """Key management for the logged-in UI user. Returns (http_status, json_payload)."""
    rest = path[len(KEYS_PATH):].strip("/")
    if method == "GET" and rest == "":
        return 200, storage.api_keys_list()
    if method == "POST" and rest == "":
        name = str(body.get("name", "")).strip()
        if not 1 <= len(name) <= 60:
            return 422, {"error": "nama key 1-60 karakter"}
        key, key_hash, prefix = new_api_key()
        key_id = storage.api_key_create(name, key_hash, prefix)
        # The key is returned once, here. Only its hash is stored.
        return 201, {"id": key_id, "name": name, "key": key, "key_prefix": prefix}
    if method == "POST" and rest.endswith("/revoke") and rest[:-len("/revoke")].isdigit():
        revoked = storage.api_key_revoke(int(rest[:-len("/revoke")]))
        return (200, {"ok": True}) if revoked else (404, {"error": "key not found or already revoked"})
    return 404, {"error": "not found"}
