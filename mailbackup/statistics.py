#!/usr/bin/env python3

"""
statistics.py

Thread-safe statistics tracking for mailbackup.

Provides a thread-safe counter for tracking various metrics across
multiple threads without race conditions.
"""

from __future__ import annotations

import threading
from typing import Dict


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


def create_stats() -> ThreadSafeStats:
    """
    Factory function to create a thread-safe statistics counter.
    
    Returns:
        ThreadSafeStats instance
    """
    return ThreadSafeStats()
