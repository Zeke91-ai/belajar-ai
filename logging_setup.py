"""Logging setup: rotating file handler plus console output."""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


def configure_logging(log_dir: Path, level: str, max_bytes: int, backup_count: int) -> None:
    """Attach a console handler and a rotating file handler to the root logger."""
    log_dir.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(LOG_FORMAT)

    root = logging.getLogger()
    root.setLevel(level)
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()

    file_handler = RotatingFileHandler(
        log_dir / "crawler.log",
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    root.addHandler(file_handler)
    # Background processes (pythonw) have no console; sys.stderr is None there.
    if sys.stderr is not None:
        console = logging.StreamHandler(sys.stderr)
        console.setFormatter(formatter)
        root.addHandler(console)

    # Telethon logs every RPC call at DEBUG, which drowns out crawl progress.
    logging.getLogger("telethon").setLevel(logging.WARNING)
