#!/usr/bin/env python3

"""
statistics.py

Thread-safe statistics tracking and status reporting for mailbackup.

Provides centralized statistics management with thread-safe counters
and periodic status reporting across multiple worker threads.
"""

from __future__ import annotations

import threading
from typing import Dict, Optional, Any

from mailbackup.logger import get_logger


class ThreadSafeStats:
    """
    Thread-safe statistics counter.
    
    Provides atomic increment operations and thread-safe access to counters.
    Used to track metrics like uploaded, extracted, verified, etc. across
    multiple worker threads.
    """
    
    def __init__(self):
        """Initialize with empty counters and a lock."""
        self._counters: Dict[str, int] = {}
        self._lock = threading.Lock()
    
    def increment(self, key: str, value: int = 1) -> None:
        """
        Atomically increment a counter.
        
        Args:
            key: Counter name (e.g., 'uploaded', 'extracted')
            value: Amount to increment by (default 1)
        """
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + value
    
    def set(self, key: str, value: int) -> None:
        """
        Atomically set a counter value.
        
        Args:
            key: Counter name
            value: Value to set
        """
        with self._lock:
            self._counters[key] = value
    
    def get(self, key: str, default: int = 0) -> int:
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
    
    def get_all(self) -> Dict[str, int]:
        """
        Get a snapshot of all counters.
        
        Returns:
            Dictionary copy of all counters
        """
        with self._lock:
            return self._counters.copy()
    
    def to_dict(self) -> Dict[str, int]:
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
    
    def __getitem__(self, key: str) -> int:
        """Support dict-like access: stats['uploaded']"""
        return self.get(key, 0)
    
    def __setitem__(self, key: str, value: int) -> None:
        """Support dict-like assignment: stats['uploaded'] = 5"""
        self.set(key, value)
    
    def format_status(self) -> str:
        """
        Format current statistics as a status string.
        
        Returns:
            Formatted status string with all counters
        """
        snapshot = self.get_all()
        return (
            f"Uploaded: {snapshot.get('uploaded', 0)} | "
            f"Archived: {snapshot.get('archived', 0)} | "
            f"Verified: {snapshot.get('verified', 0)} | "
            f"Repaired: {snapshot.get('repaired', 0)} | "
            f"Skipped: {snapshot.get('skipped', 0)} | "
            f"Extracted: {snapshot.get('extracted', 0)}"
        )


class StatusThread:
    """
    Background thread for periodic status reporting.
    
    Runs in the background and periodically logs the current statistics.
    Supports both dict and ThreadSafeStats for backward compatibility.
    """
    
    def __init__(self, interval: int, counters: Dict[str, int] | ThreadSafeStats):
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
        # If it's a ThreadSafeStats object with format_status method, use it
        if isinstance(self.counters, ThreadSafeStats):
            return self.counters.format_status()
        
        # Otherwise, treat as dict
        return format_stats_dict(self.counters)


def format_stats_dict(stats: Dict[str, int]) -> str:
    """
    Format a statistics dictionary as a status string.
    
    Args:
        stats: Dictionary of statistics counters
        
    Returns:
        Formatted status string
    """
    return (
        f"Uploaded: {stats.get('uploaded', 0)} | "
        f"Archived: {stats.get('archived', 0)} | "
        f"Verified: {stats.get('verified', 0)} | "
        f"Repaired: {stats.get('repaired', 0)} | "
        f"Skipped: {stats.get('skipped', 0)} | "
        f"Extracted: {stats.get('extracted', 0)}"
    )


def log_status(stats: ThreadSafeStats | Dict[str, int], stage: str = ""):
    """
    Log current statistics status.
    
    Args:
        stats: Statistics counters
        stage: Optional stage name to include in log message
    """
    logger = get_logger(__name__)
    
    if isinstance(stats, ThreadSafeStats):
        status_str = stats.format_status()
    else:
        status_str = format_stats_dict(stats)
    
    prefix = f"[{stage}] " if stage else "[STATUS] "
    logger.info(prefix + status_str)


def create_stats() -> ThreadSafeStats:
    """
    Factory function to create a thread-safe statistics counter.
    
    Returns:
        ThreadSafeStats instance
    """
    return ThreadSafeStats()

