#!/usr/bin/env python3

"""
statistics.py

Thread-safe statistics tracking and status reporting for mailbackup.

Provides centralized statistics management with thread-safe counters
and periodic status reporting across multiple worker threads.
"""

from __future__ import annotations

import threading
from enum import Enum
from typing import Dict, Optional, Callable

from mailbackup.executor import TaskResult
from mailbackup.logger import get_logger


class StatKey(Enum):
    FETCHED = "Fetched"
    EXTRACTED = "Extracted"
    BACKED_UP = "Backed up"
    ARCHIVED = "Archived"
    VERIFIED = "Verified"
    REPAIRED = "Repaired"
    SKIPPED = "Skipped"
    PROCESSED = "Total processed"
    FAILED = "Failed"
    
    # Backward compatibility aliases
    UPLOADED = "Backed up"  # Alias for BACKED_UP


# String to StatKey mapping for backward compatibility
_STRING_TO_STATKEY = {
    "fetched": StatKey.FETCHED,
    "extracted": StatKey.EXTRACTED,
    "backed up": StatKey.BACKED_UP,
    "backed_up": StatKey.BACKED_UP,
    "uploaded": StatKey.BACKED_UP,  # Backward compatibility
    "archived": StatKey.ARCHIVED,
    "verified": StatKey.VERIFIED,
    "repaired": StatKey.REPAIRED,
    "skipped": StatKey.SKIPPED,
    "processed": StatKey.PROCESSED,
    "total processed": StatKey.PROCESSED,
    "total_processed": StatKey.PROCESSED,
    "failed": StatKey.FAILED,
}


def _normalize_key(key: StatKey | str) -> StatKey:
    """
    Normalize a key to StatKey enum.
    
    Supports both StatKey enum and string keys for backward compatibility.
    
    Args:
        key: StatKey enum or string key
        
    Returns:
        StatKey enum
        
    Raises:
        KeyError: If string key is not recognized
    """
    if isinstance(key, StatKey):
        return key
    if isinstance(key, str):
        normalized = key.lower().replace("-", "_")
        if normalized in _STRING_TO_STATKEY:
            return _STRING_TO_STATKEY[normalized]
        raise KeyError(f"Unknown statistic key: {key}")
    raise TypeError(f"Key must be StatKey or str, got {type(key)}")


class ThreadSafeStats:
    """
    Thread-safe statistics counter.
    
    Provides atomic increment operations and thread-safe access to counters.
    Used to track metrics like uploaded, extracted, verified, etc. across
    multiple worker threads.
    """

    def __init__(self):
        """Initialize with empty counters and a lock."""
        self._counters: Dict[StatKey | str, int] = {}
        self._lock = threading.Lock()

    def increment(self, key: StatKey | str, value: int = 1) -> None:
        """
        Atomically increment a counter.
        
        Args:
            key: Counter name (StatKey enum or string like 'uploaded', 'extracted')
            value: Amount to increment by (default 1)
        """
        # Normalize known keys, but allow arbitrary string keys for flexibility
        if isinstance(key, str) and key.lower().replace("-", "_") in _STRING_TO_STATKEY:
            key = _normalize_key(key)
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + value

    def set(self, key: StatKey | str, value: int) -> None:
        """
        Atomically set a counter value.
        
        Args:
            key: Counter name (StatKey enum or string)
            value: Value to set
        """
        # Normalize known keys, but allow arbitrary string keys for flexibility
        if isinstance(key, str) and key.lower().replace("-", "_") in _STRING_TO_STATKEY:
            key = _normalize_key(key)
        with self._lock:
            self._counters[key] = value

    def get(self, key: StatKey | str, default: int = 0) -> int:
        """
        Thread-safe get counter value.
        
        Args:
            key: Counter name (StatKey enum or string)
            default: Default value if key doesn't exist
            
        Returns:
            Counter value or default
        """
        # Normalize known keys, but allow arbitrary string keys for flexibility
        if isinstance(key, str) and key.lower().replace("-", "_") in _STRING_TO_STATKEY:
            key = _normalize_key(key)
        with self._lock:
            return self._counters.get(key, default)

    def get_all(self) -> Dict[str, int]:
        """
        Get a snapshot of all counters.
        
        Returns:
            Dictionary copy of all counters with string keys for compatibility
        """
        with self._lock:
            # Convert StatKey enums to strings for backward compatibility
            result = {}
            for key, value in self._counters.items():
                if isinstance(key, StatKey):
                    # Use lowercase string representation for backward compatibility
                    # Map BACKED_UP to "uploaded" for old tests
                    if key == StatKey.BACKED_UP:
                        result["uploaded"] = value
                    else:
                        result[key.value.lower()] = value
                else:
                    result[key] = value
            return result

    def to_dict(self) -> Dict[str, int]:
        """
        Convert to plain dictionary (for compatibility).
        
        Returns:
            Dictionary copy of all counters with string keys
        """
        return self.get_all()

    def reset(self) -> None:
        """Reset all counters to zero."""
        with self._lock:
            self._counters.clear()

    def __getitem__(self, key: StatKey | str) -> int:
        """Support dict-like access: stats['uploaded']"""
        return self.get(key, 0)

    def __setitem__(self, key: StatKey | str, value: int) -> None:
        """Support dict-like assignment: stats['uploaded'] = 5"""
        self.set(key, value)

    def format_status(self) -> str:
        """
        Format current statistics as a status string.
        
        Returns:
            Formatted status string with all counters
        """
        snapshot = self.get_all()
        txt = f" | "
        # Define display order and names
        display_keys = [
            ("fetched", "Fetched"),
            ("extracted", "Extracted"),
            ("uploaded", "Uploaded"),  # Use "Uploaded" instead of "Backed up"
            ("archived", "Archived"),
            ("verified", "Verified"),
            ("repaired", "Repaired"),
            ("skipped", "Skipped"),
            ("total processed", "Total processed"),
            ("failed", "Failed"),
        ]
        for key, label in display_keys:
            value = snapshot.get(key, 0)
            txt += f"{label}: {value} | "
        return txt


class StatusThread:
    """
    Background thread for periodic status reporting.
    
    Runs in the background and periodically logs the current statistics.
    Supports both dict and ThreadSafeStats for backward compatibility.
    """

    def __init__(self, interval: int, counters: ThreadSafeStats):
        """
        Initialize status reporter.
        
        Args:
            interval: Seconds between status reports
            counters: Statistics counters (dict or ThreadSafeStats)
        """
        self.logger = get_logger(__name__)
        self.interval = interval
        self.counters = counters
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self):
        """Start the status reporting thread."""
        if self._thread is not None:
            return

        def reporter():
            while not self._stop_event.wait(self.interval):
                # logger.status is registered at runtime by setup_logger; call via getattr
                fn = getattr(self.logger, "status", None)
                if callable(fn):
                    fn(self.get_status_summary())
                else:
                    self.logger.info("[STATUS] " + self.get_status_summary())

        t = threading.Thread(target=reporter, name="StatusReporter", daemon=True)
        t.start()
        self._thread = t

    def stop(self):
        """Stop the status reporting thread."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def get_status_summary(self) -> str:
        """
        Get formatted status summary.
        
        Returns:
            Formatted status string
        """
        return self.counters.format_status()


def log_status(stats: ThreadSafeStats | dict, stage: str = ""):
    """
    Log current statistics status.
    
    Args:
        stats: Statistics counters (ThreadSafeStats or dict)
        stage: Optional stage name to include in log message
    """
    logger = get_logger(__name__)

    prefix = f"[{stage}] " if stage else ""
    # Try to use status level if available, otherwise use info
    fn = getattr(logger, "status", None)
    
    # Format the status string
    if isinstance(stats, ThreadSafeStats):
        status_str = stats.format_status()
    else:
        status_str = format_stats_dict(stats)
    
    if callable(fn):
        fn(prefix + status_str)
    else:
        logger.info(prefix + status_str)


def format_stats_dict(stats: dict | ThreadSafeStats) -> str:
    """
    Format statistics dictionary as a status string.
    
    Args:
        stats: Statistics dictionary or ThreadSafeStats object
        
    Returns:
        Formatted status string
    """
    if isinstance(stats, ThreadSafeStats):
        return stats.format_status()
    
    # For plain dict, format manually
    txt = " | "
    display_keys = [
        ("fetched", "Fetched"),
        ("extracted", "Extracted"),
        ("uploaded", "Uploaded"),
        ("archived", "Archived"),
        ("verified", "Verified"),
        ("repaired", "Repaired"),
        ("skipped", "Skipped"),
        ("total processed", "Total processed"),
        ("total_processed", "Total processed"),
        ("processed", "Total processed"),
        ("failed", "Failed"),
    ]
    
    # Track which keys we've already added
    added = set()
    for key, label in display_keys:
        if label in added:
            continue
        value = stats.get(key, 0)
        if value or key in ["fetched", "extracted", "uploaded", "archived", "verified", "repaired", "skipped", "failed"]:
            txt += f"{label}: {value} | "
            added.add(label)
    return txt


def create_stats() -> ThreadSafeStats:
    """
    Factory function to create a thread-safe statistics counter.
    
    Returns:
        ThreadSafeStats instance
    """
    return ThreadSafeStats()


def create_increment_callback(
        stats: ThreadSafeStats,
        success_key: StatKey = StatKey.PROCESSED,
        failure_key: StatKey = StatKey.FAILED,
) -> Callable[[TaskResult], None]:
    def increment_callback(result: TaskResult) -> None:
        if result.success and (True if not isinstance(result.result, bool) else result.result):
            stats.increment(success_key)
        else:
            stats.increment(failure_key)

    return increment_callback
