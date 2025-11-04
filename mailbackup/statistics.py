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


class ThreadSafeStats:
    """
    Thread-safe statistics counter.
    
    Provides atomic increment operations and thread-safe access to counters.
    Used to track metrics like uploaded, extracted, verified, etc. across
    multiple worker threads.
    """

    def __init__(self):
        """Initialize with empty counters and a lock."""
        self._counters: Dict[StatKey, int] = {}
        self._lock = threading.Lock()

    def increment(self, key: StatKey, value: int = 1) -> None:
        """
        Atomically increment a counter.
        
        Args:
            key: Counter name (e.g., 'uploaded', 'extracted')
            value: Amount to increment by (default 1)
        """
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + value

    def set(self, key: StatKey, value: int) -> None:
        """
        Atomically set a counter value.
        
        Args:
            key: Counter name
            value: Value to set
        """
        with self._lock:
            self._counters[key] = value

    def get(self, key: StatKey, default: int = 0) -> int:
        """
        Thread-safe get counter value.
        
        Args:
            key: Counter name
            default: Default value if key doesn't exist
            
        Returns:
            Counter value or default
        """
        with self._lock:
            return self._counters.get(key, default)

    def get_all(self) -> Dict[StatKey, int]:
        """
        Get a snapshot of all counters.
        
        Returns:
            Dictionary copy of all counters
        """
        with self._lock:
            return self._counters.copy()

    def to_dict(self) -> Dict[StatKey, int]:
        """
        Convert to plain dictionary (for compatibility).
        
        Returns:
            Dictionary copy of all counters
        """
        return self.get_all()

    def reset(self) -> None:
        """Reset all counters to zero."""
        with self._lock:
            self._counters.clear()

    def __getitem__(self, key: StatKey) -> int:
        """Support dict-like access: stats['uploaded']"""
        return self.get(key, 0)

    def __setitem__(self, key: StatKey, value: int) -> None:
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
        for stat in StatKey:
            txt += f"{stat.value}: {snapshot.get(stat, 0)} | "
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


def log_status(stats: ThreadSafeStats, stage: str = ""):
    """
    Log current statistics status.
    
    Args:
        stats: Statistics counters
        stage: Optional stage name to include in log message
    """
    logger = get_logger(__name__)

    prefix = f"[{stage}] " if stage else ""
    logger.status(prefix + stats.format_status())


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
