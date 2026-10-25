"""Telegram session creation and verification.

Run once interactively to create the .session file:

    python auth.py

Later runs reuse the saved session silently. Telethon stores the session in SQLite,
so only one process may use it at a time; SessionLock enforces that.
"""

from __future__ import annotations

import asyncio
import getpass
import logging
import os
import sys
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError,
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
)

from config import ConfigError, Settings, load_settings
from logging_setup import configure_logging

log = logging.getLogger("auth")


class AuthError(RuntimeError):
    """Login could not be completed."""


class SessionLockedError(RuntimeError):
    """Another process already holds the session."""


class SessionLock:
    """Exclusive lock file guarding the Telethon session.

    Uses O_CREAT | O_EXCL so creation is atomic on both Windows and POSIX.
    The lock file is removed on normal exit. If the process is killed hard,
    the stale file must be deleted manually; the error message says where it is.
    """

    def __init__(self, session_dir: Path, session_name: str) -> None:
        self._path = session_dir / f"{session_name}.lock"

    def __enter__(self) -> SessionLock:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(self._path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            owner = self._read_owner()
            raise SessionLockedError(
                f"Session is in use (lock file: {self._path}, pid {owner}). "
                "If no crawler is running, delete the lock file and retry."
            ) from None
        with os.fdopen(fd, "w") as handle:
            handle.write(str(os.getpid()))
        return self

    def __exit__(self, *exc_info: object) -> None:
        try:
            self._path.unlink()
        except FileNotFoundError:
            pass

    def _read_owner(self) -> str:
        try:
            return self._path.read_text(encoding="utf-8").strip() or "unknown"
        except OSError:
            return "unknown"


def build_client(settings: Settings) -> TelegramClient:
    """Create a Telethon client bound to the project's session file.

    flood_sleep_threshold=0 disables Telethon's silent FloodWait sleep so the
    crawler can log and handle waits itself. connection_retries=0 hands the
    retry policy to TelegramCrawler.
    """
    settings.session_dir.mkdir(parents=True, exist_ok=True)
    return TelegramClient(
        str(settings.session_path),
        settings.api_id,
        settings.api_hash,
        flood_sleep_threshold=0,
        connection_retries=0,
    )


def _mask_phone(phone: str) -> str:
    return f"{phone[:4]}{'*' * max(len(phone) - 6, 0)}{phone[-2:]}"


async def ensure_authorized(client: TelegramClient, settings: Settings, *, interactive: bool) -> None:
    """Verify the session; run the login flow only when interactive is True."""
    if await client.is_user_authorized():
        me = await client.get_me()
        log.info("Session is valid for user id=%s", me.id)
        return

    if not interactive:
        raise AuthError("Session is not authorized. Run `python auth.py` to log in.")

    await _interactive_login(client, settings)


async def _interactive_login(client: TelegramClient, settings: Settings) -> None:
    log.info("Requesting a login code for %s", _mask_phone(settings.phone_number))
    try:
        await client.send_code_request(settings.phone_number)
    except FloodWaitError as exc:
        raise AuthError(f"Telegram rate-limited the login request; retry in {exc.seconds}s") from exc
    except PhoneNumberInvalidError as exc:
        raise AuthError("PHONE_NUMBER is invalid. Use international format, e.g. +6281234567890.") from exc

    code = await asyncio.to_thread(input, "Enter the login code sent by Telegram: ")
    try:
        await client.sign_in(phone=settings.phone_number, code=code.strip())
    except SessionPasswordNeededError:
        password = settings.twofa_password or await asyncio.to_thread(
            getpass.getpass, "Two-step verification password: "
        )
        try:
            await client.sign_in(password=password)
        except PasswordHashInvalidError as exc:
            raise AuthError("Two-step verification password is incorrect.") from exc
    except (PhoneCodeInvalidError, PhoneCodeExpiredError) as exc:
        raise AuthError("The login code was invalid or expired. Run auth.py again.") from exc

    log.info("Login successful; session saved")


async def _run_auth(settings: Settings) -> None:
    with SessionLock(settings.session_dir, settings.session_name):
        client = build_client(settings)
        await client.connect()
        try:
            await ensure_authorized(client, settings, interactive=True)
        finally:
            await client.disconnect()
    log.info("Session file ready at %s.session", settings.session_path)


def main() -> int:
    try:
        settings = load_settings()
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    configure_logging(settings.log_dir, settings.log_level, settings.log_max_bytes, settings.log_backup_count)
    try:
        asyncio.run(_run_auth(settings))
    except (AuthError, SessionLockedError) as exc:
        log.error("%s", exc)
        return 1
    except KeyboardInterrupt:
        log.warning("Login cancelled")
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
