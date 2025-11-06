#!/usr/bin/env python3
"""
logging.py — centralized logging for mailbackup
"""

import logging
import sys
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
from typing import Optional

from mailbackup.config import Settings

_LOGGER: Optional[logging.Logger] = None
STATUS_LEVEL = 25


def setup_logger(settings: Settings) -> logging.Logger:
    """Initialize global logger once."""
    global _LOGGER
    if _LOGGER is not None:
        return _LOGGER

    logging.addLevelName(STATUS_LEVEL, "STATUS")

    def status(self, message, *args, **kwargs):
        if self.isEnabledFor(STATUS_LEVEL):
            self._log(STATUS_LEVEL, message, args, **kwargs)

    logging.Logger.status = status  # type: ignore[attr-defined]

    log = logging.getLogger("mailbackup")
    log.setLevel(settings.log_level)
    log.propagate = False

    # Clear any default handlers
    for h in log.handlers[:]:
        log.removeHandler(h)

    # --- Rotating File Handler ---
    if settings.rotate_by_time:
        # Rotate daily at midnight, keep 7 days by default
        file_handler = TimedRotatingFileHandler(
            settings.log_path, when="midnight", interval=1, backupCount=settings.max_log_files, encoding="utf-8"
        )
    else:
        # Rotate when file exceeds max_bytes
        file_handler = RotatingFileHandler(
            settings.log_path, maxBytes=settings.max_log_size, backupCount=settings.max_log_files, encoding="utf-8"
        )

    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s"
    )
    file_handler.setFormatter(file_formatter)
    log.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    console_handler.setFormatter(console_formatter)
    log.addHandler(console_handler)

    _LOGGER = log
    return log


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Retrieve a logger. If setup_logger() hasn't been called yet,
    return a temporary stderr-based logger.
    """
    global _LOGGER
    if _LOGGER is not None:
        return _LOGGER.getChild(name) if name else _LOGGER

    # Fallback: minimal stderr logger (safe for early imports)
    temp = logging.getLogger("mailbackup.temp")
    if not temp.handlers:
        h = logging.StreamHandler(sys.stderr)
        h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        temp.addHandler(h)
        temp.setLevel(logging.INFO)
    return temp.getChild(name) if name else temp
