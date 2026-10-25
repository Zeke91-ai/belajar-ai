"""Webhook subscriptions and the ordered, retried delivery worker.

Rules (also in docs/EVENT_CONTRACT.md):
  - Each subscriber has a cursor. Events are sent strictly in seq order after it.
  - A 2xx (or 409, meaning "already stored") settles the event and moves the cursor.
  - Any other result is retried with exponential backoff: 30 s, 60 s, 120 s ... capped at 1 h.
  - An event is dead-lettered after MAX_ATTEMPTS failures, or after MAX_FAILURE_AGE of
    failures, whichever comes first. The cursor then moves on to the next event.
  - Dead letters keep the exact body and can be replayed from the UI.
"""

from __future__ import annotations

import json
import logging
import re
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from contract import SCHEMA, ContractError, build_envelope, serialize, sign
from storage import Storage

log = logging.getLogger("webhook")

MAX_ATTEMPTS = 10
MAX_FAILURE_AGE = timedelta(hours=24)
BASE_BACKOFF_SECONDS = 30
MAX_BACKOFF_SECONDS = 3600
POST_TIMEOUT_SECONDS = 10
MAX_EVENTS_PER_CYCLE = 50
KV_ACCOUNT_ID = "account.id"
LOCAL_HOSTS = {"127.0.0.1", "localhost"}


@dataclass(frozen=True)
class PostResult:
    ok: bool
    status: int | None
    error: str | None


def backoff_seconds(attempts: int) -> int:
    return min(MAX_BACKOFF_SECONDS, BASE_BACKOFF_SECONDS * 2 ** max(attempts - 1, 0))


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def post_body(
    url: str,
    secret: str,
    body: bytes,
    event_id: str,
    event_type: str,
    *,
    timeout: float = POST_TIMEOUT_SECONDS,
    timestamp: int | None = None,
) -> PostResult:
    """Send one signed event. 2xx and 409 count as settled; everything else is a failure."""
    ts = int(time.time()) if timestamp is None else timestamp
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "telegram-mirror/1.0",
        "X-TM-Schema": SCHEMA,
        "X-TM-Event-Id": event_id,
        "X-TM-Event-Type": event_type,
        "X-TM-Timestamp": str(ts),
        "X-TM-Signature": sign(body, secret, ts),
    }
    request = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = response.status
    except urllib.error.HTTPError as exc:
        status = exc.code
    except (urllib.error.URLError, OSError) as exc:  # includes timeouts and refused connections
        return PostResult(False, None, f"{type(exc).__name__}: {exc}")

    if 200 <= status < 300 or status == 409:
        return PostResult(True, status, None)
    return PostResult(False, status, f"HTTP {status}")


def _drain(storage: Storage, sub: dict[str, Any], account_id: int, now: datetime) -> int:
    """Deliver as many due events as possible for one subscriber. Returns events settled."""
    settled = 0
    for _ in range(MAX_EVENTS_PER_CYCLE):
        row = storage.next_event_after(sub["cursor_seq"])
        if row is None:
            break

        try:
            chat = storage.chat_info(row["chat_id"]) if row["chat_id"] is not None else None
            envelope = build_envelope(row, account_id=account_id, chat=chat)
        except ContractError as exc:
            # Cannot be sent in any form. Dead-letter it straight away instead of blocking the queue.
            storage.advance_subscription(sub["id"], row["id"], _iso(now), dead={
                "seq": row["id"], "event_id": f"unmapped-{row['id']}", "event_type": row["event_type"],
                "body": row["payload_json"] or "{}", "attempts": 0, "error": str(exc),
                "failed_utc": _iso(now),
            })
            sub = {**sub, "cursor_seq": row["id"]}
            settled += 1
            continue

        body = serialize(envelope)
        result = post_body(sub["url"], sub["secret"], body, envelope["event_id"], envelope["type"])

        if result.ok:
            storage.advance_subscription(sub["id"], row["id"], _iso(now), status=result.status)
            sub = {**sub, "cursor_seq": row["id"], "attempts": 0, "first_failure_utc": None}
            settled += 1
            continue

        attempts = int(sub["attempts"] or 0) + 1
        first_failure = _parse(sub["first_failure_utc"]) or now
        too_old = now - first_failure >= MAX_FAILURE_AGE
        if attempts >= MAX_ATTEMPTS or too_old:
            reason = (
                f"gave up after {attempts} attempt(s) "
                f"({'time limit' if too_old else 'attempt limit'}): {result.error}"
            )
            storage.advance_subscription(
                sub["id"], row["id"], _iso(now), status=result.status, error=reason,
                dead={
                    "seq": row["id"], "event_id": envelope["event_id"], "event_type": envelope["type"],
                    "body": body.decode("utf-8"), "attempts": attempts, "error": reason,
                    "failed_utc": _iso(now),
                },
            )
            log.error("Dead-lettered seq %s for subscriber %s: %s", row["id"], sub["name"], reason)
            sub = {**sub, "cursor_seq": row["id"], "attempts": 0, "first_failure_utc": None}
            settled += 1
            continue

        storage.record_failure(
            sub["id"],
            attempts=attempts,
            first_failure_utc=_iso(first_failure),
            next_attempt_utc=_iso(now + timedelta(seconds=backoff_seconds(attempts))),
            error=result.error,
            status=result.status,
        )
        log.warning("Delivery of seq %s to %s failed (attempt %d/%d): %s",
                    row["id"], sub["name"], attempts, MAX_ATTEMPTS, result.error)
        break
    return settled


def deliver_once(db_path: Path, *, now: datetime | None = None) -> int:
    """One delivery cycle over all due subscribers.

    Opens its own database connection, so it can run in a worker thread while the
    mirror's event loop keeps running.
    """
    current = now or _now()
    with Storage(db_path) as storage:
        raw_account = storage.kv_get(KV_ACCOUNT_ID)
        if raw_account is None:
            return 0  # the mirror has not logged in yet
        account_id = int(raw_account)
        settled = 0
        for sub in storage.due_subscriptions(_iso(current)):
            settled += _drain(storage, sub, account_id, current)
        return settled


def replay_dead_letter(storage: Storage, dead_id: int, *, now: datetime | None = None) -> PostResult:
    """Re-send one dead letter with its original body and event_id. Receivers must dedupe."""
    dead = storage.dead_letter_get(dead_id)
    if dead is None:
        return PostResult(False, None, "dead letter not found")
    sub = storage.subscription_get(dead["subscription_id"])
    if sub is None:
        return PostResult(False, None, "subscriber no longer exists")
    body = dead["body"].encode("utf-8")
    result = post_body(sub["url"], sub["secret"], body, dead["event_id"], dead["event_type"])
    if result.ok:
        storage.dead_letter_mark_replayed(dead_id, _iso(now or _now()))
    return result


# ---- management API used by the UI -------------------------------------------------

_NAME_RE = re.compile(r"^[\w .-]{1,60}$")


def _validate_url(url: str) -> str | None:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        return "URL harus http:// atau https:// dan punya nama host"
    if parsed.scheme == "http" and parsed.hostname not in LOCAL_HOSTS:
        return "Untuk host selain localhost wajib https://"
    return None


def _public_subscription(storage: Storage, sub: dict[str, Any]) -> dict[str, Any]:
    out = {k: v for k, v in sub.items() if k != "secret"}
    out["pending_events"] = storage.count_events_after(sub["cursor_seq"])
    out["dead_letters"] = len(storage.dead_letters(sub["id"], limit=10_000))
    return out


def webhook_api(storage: Storage, method: str, path: str, query: dict[str, str], body: dict[str, Any]) -> tuple[int, Any]:
    """Route /api/webhooks... requests. Returns (http_status, json_payload)."""
    parts = [p for p in path.split("/") if p]
    if parts[:2] != ["api", "webhooks"]:
        return 404, {"error": "not found"}
    rest = parts[2:]

    if method == "GET" and rest == []:
        return 200, [_public_subscription(storage, s) for s in storage.subscriptions_list()]

    if method == "GET" and rest == ["dead"]:
        sub = int(query["sub"]) if query.get("sub", "").isdigit() else None
        return 200, storage.dead_letters(sub, limit=200)

    if method == "POST" and rest == []:
        name = str(body.get("name", "")).strip()
        url = str(body.get("url", "")).strip()
        if not _NAME_RE.match(name):
            return 422, {"error": "nama 1-60 karakter: huruf, angka, spasi, titik, minus"}
        problem = _validate_url(url)
        if problem:
            return 422, {"error": problem}
        cursor = 0 if body.get("start", "beginning") == "beginning" else storage.max_event_id()
        secret = secrets.token_urlsafe(32)
        sub_id = storage.subscription_create(name, url, secret, cursor)
        sub = storage.subscription_get(sub_id)
        # The secret is returned once, here. Afterwards it is never shown again.
        return 201, {**_public_subscription(storage, sub), "secret": secret}

    if method == "POST" and rest[:1] == ["dead"] and len(rest) == 3 and rest[2] == "replay":
        if not rest[1].isdigit():
            return 404, {"error": "not found"}
        result = replay_dead_letter(storage, int(rest[1]))
        return (200 if result.ok else 502), {"ok": result.ok, "status": result.status, "error": result.error}

    if method == "POST" and len(rest) == 2 and rest[0].isdigit():
        sub_id = int(rest[0])
        sub = storage.subscription_get(sub_id)
        if sub is None:
            return 404, {"error": "subscriber not found"}
        action = rest[1]

        if action in ("enable", "disable"):
            storage.subscription_set(sub_id, enabled=1 if action == "enable" else 0)
        elif action == "rotate-secret":
            secret = secrets.token_urlsafe(32)
            storage.subscription_set(sub_id, secret=secret)
            return 200, {"secret": secret}
        elif action == "rewind":
            seq = body.get("seq")
            if not isinstance(seq, int) or seq < 0:
                return 422, {"error": "seq harus bilangan bulat >= 0"}
            storage.subscription_set(sub_id, cursor_seq=seq, attempts=0,
                                     first_failure_utc=None, next_attempt_utc=None)
        elif action == "delete":
            storage.subscription_delete(sub_id)
            return 200, {"ok": True}
        else:
            return 404, {"error": "not found"}
        return 200, _public_subscription(storage, storage.subscription_get(sub_id))

    return 404, {"error": "not found"}
