#!/usr/bin/env python3
"""
logging.py — centralized logging for mailbackup
"""

import logging
import sys
from pathlib import Path
from typing import Optional

_LOGGER: Optional[logging.Logger] = None
STATUS_LEVEL = 25


def setup_logger(log_path: Path, level=logging.DEBUG) -> logging.Logger:
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
    log.setLevel(level)
    log.propagate = False

    # Clear any default handlers
    for h in log.handlers[:]:
        log.removeHandler(h)

    # File handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter("%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s")
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
