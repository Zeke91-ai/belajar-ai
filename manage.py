r"""Command-line management for the Telegram mirror.

Examples (Windows Command Prompt, from the project folder):

    .venv\Scripts\python manage.py keys create sclab-pull
    .venv\Scripts\python manage.py keys list
    .venv\Scripts\python manage.py keys revoke 2
    .venv\Scripts\python manage.py subscribers list
    .venv\Scripts\python manage.py api status
    .venv\Scripts\python manage.py api stop

The API key is printed once by "keys create". Store it immediately.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from config import ConfigError, load_settings
from pull_api import handle_keys
from storage import Storage
from webhook import _public_subscription  # same view the UI uses (secrets are never shown)
from api_server import KEY_DESIRED, api_status


def _table(rows: list[dict[str, Any]], columns: list[str]) -> str:
    if not rows:
        return "(kosong)"
    widths = {c: max(len(c), *(len(str(r.get(c, ""))) for r in rows)) for c in columns}
    header = "  ".join(c.ljust(widths[c]) for c in columns)
    lines = [header, "  ".join("-" * widths[c] for c in columns)]
    for r in rows:
        lines.append("  ".join(str(r.get(c, "") if r.get(c) is not None else "").ljust(widths[c]) for c in columns))
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="manage.py", description="Manage the Telegram mirror.")
    top = parser.add_subparsers(dest="group", required=True)

    keys = top.add_parser("keys", help="API keys for the pull API")
    keys_sub = keys.add_subparsers(dest="action", required=True)
    create = keys_sub.add_parser("create", help="create a key (shown once)")
    create.add_argument("name")
    keys_sub.add_parser("list", help="list keys")
    revoke = keys_sub.add_parser("revoke", help="revoke a key by id")
    revoke.add_argument("id", type=int)

    subs = top.add_parser("subscribers", help="webhook subscribers")
    subs.add_subparsers(dest="action", required=True).add_parser("list", help="list subscribers")

    api = top.add_parser("api", help="background API server")
    api_sub = api.add_subparsers(dest="action", required=True)
    api_sub.add_parser("status", help="show status")
    api_sub.add_parser("stop", help="ask the background API to stop")

    args = parser.parse_args(argv)
    try:
        settings = load_settings()
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    storage = Storage(settings.db_path)
    try:
        if args.group == "keys":
            if args.action == "create":
                status, payload = handle_keys(storage, "POST", "/api/keys", {"name": args.name})
                if status != 201:
                    print(payload.get("error", "failed"), file=sys.stderr)
                    return 1
                print("API key (simpan sekarang, tidak ditampilkan lagi):")
                print(payload["key"])
                print(f"id={payload['id']}  prefix={payload['key_prefix']}")
            elif args.action == "list":
                print(_table(storage.api_keys_list(),
                             ["id", "name", "key_prefix", "created_utc", "last_used_utc", "revoked_utc"]))
            elif args.action == "revoke":
                status, payload = handle_keys(storage, "POST", f"/api/keys/{args.id}/revoke", {})
                print("dicabut" if status == 200 else payload.get("error", "failed"))
                return 0 if status == 200 else 1

        elif args.group == "subscribers":
            rows = [_public_subscription(storage, s) for s in storage.subscriptions_list()]
            print(_table(rows, ["id", "name", "url", "enabled", "cursor_seq", "pending_events", "dead_letters"]))

        elif args.group == "api":
            if args.action == "status":
                for key, value in api_status(storage).items():
                    print(f"{key}: {value}")
            elif args.action == "stop":
                storage.kv_set(KEY_DESIRED, "stop")
                print("Permintaan berhenti dikirim. Server akan berhenti dalam beberapa detik.")
    finally:
        storage.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
