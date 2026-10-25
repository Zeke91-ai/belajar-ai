"""Runtime configuration loaded from environment variables and an optional .env file.

Credentials are never hardcoded. Required variables: API_ID, API_HASH, PHONE_NUMBER.
Secret fields are excluded from ``repr`` so they do not leak into logs or tracebacks.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

PROJECT_DIR = Path(__file__).resolve().parent
_VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR"}


class ConfigError(RuntimeError):
    """Required configuration is missing or invalid."""


@dataclass(frozen=True)
class Settings:
    api_id: int
    api_hash: str = field(repr=False)
    phone_number: str = field(repr=False)
    twofa_password: str | None = field(default=None, repr=False)
    db_path: Path = PROJECT_DIR / "crawl.db3"
    session_dir: Path = PROJECT_DIR / "sessions"
    session_name: str = "crawler"
    log_dir: Path = PROJECT_DIR / "logs"
    log_level: str = "INFO"
    log_max_bytes: int = 10 * 1024 * 1024
    log_backup_count: int = 5
    media_dir: Path = PROJECT_DIR / "media"
    max_video_bytes: int = 100 * 1024 * 1024
    ui_host: str = "127.0.0.1"
    ui_port: int = 8765
    api_host: str = "127.0.0.1"
    api_port: int = 8766

    @property
    def session_path(self) -> Path:
        return self.session_dir / self.session_name


def _require(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def load_settings(env_file: Path | None = None) -> Settings:
    """Read settings from the process environment, falling back to the .env file."""
    load_dotenv(env_file or PROJECT_DIR / ".env", override=False)

    raw_api_id = _require("API_ID")
    try:
        api_id = int(raw_api_id)
    except ValueError as exc:
        raise ConfigError("API_ID must be a numeric value") from exc

    log_level = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    if log_level not in _VALID_LOG_LEVELS:
        raise ConfigError(f"LOG_LEVEL must be one of {sorted(_VALID_LOG_LEVELS)}")

    db_override = os.getenv("DB_PATH", "").strip()
    max_video_mb = _optional_int("MAX_VIDEO_DOWNLOAD_MB", 100)

    return Settings(
        api_id=api_id,
        api_hash=_require("API_HASH"),
        phone_number=_require("PHONE_NUMBER"),
        twofa_password=os.getenv("TELEGRAM_2FA_PASSWORD") or None,
        db_path=Path(db_override) if db_override else PROJECT_DIR / "crawl.db3",
        log_level=log_level,
        max_video_bytes=max_video_mb * 1024 * 1024,
        api_port=_optional_int("API_PORT", 8766),
    )


def _optional_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a whole number") from exc
