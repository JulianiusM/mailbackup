#!/usr/bin/env python3
"""
Unit tests for statistics.py module.
"""

import pytest
import threading
import time
from mailbackup.statistics import ThreadSafeStats, create_stats


class TestThreadSafeStats:
    """Tests for ThreadSafeStats class."""
    
    def test_increment_single_thread(self):
        """Test increment in single thread."""
        stats = ThreadSafeStats()
        stats.increment("uploaded")
        assert stats.get("uploaded") == 1
        
        stats.increment("uploaded", 5)
        assert stats.get("uploaded") == 6
    
    def test_set_and_get(self):
        """Test set and get operations."""
        stats = ThreadSafeStats()
        stats.set("verified", 42)
        assert stats.get("verified") == 42
        assert stats.get("nonexistent") == 0
        assert stats.get("nonexistent", 10) == 10
    
    def test_get_all(self):
        """Test get_all returns snapshot."""
        stats = ThreadSafeStats()
        stats.increment("uploaded", 5)
        stats.increment("verified", 3)
        
        all_stats = stats.get_all()
        assert all_stats == {"uploaded": 5, "verified": 3}
        
        # Modify returned dict shouldn't affect stats
        all_stats["uploaded"] = 100
        assert stats.get("uploaded") == 5
    
    def test_dict_like_access(self):
        """Test dict-like access with []."""
        stats = ThreadSafeStats()
        stats["uploaded"] = 10
        assert stats["uploaded"] == 10
        
        stats["uploaded"] = 20
        assert stats["uploaded"] == 20
    
    def test_to_dict(self):
        """Test to_dict method."""
        stats = ThreadSafeStats()
        stats.increment("uploaded", 3)
        stats.increment("archived", 2)
        
        result = stats.to_dict()
        assert result == {"uploaded": 3, "archived": 2}
    
    def test_reset(self):
        """Test reset clears all counters."""
        stats = ThreadSafeStats()
        stats.increment("uploaded", 5)
        stats.increment("verified", 3)
        
        stats.reset()
        
        assert stats.get("uploaded") == 0
        assert stats.get("verified") == 0
        assert stats.get_all() == {}
    
    def test_thread_safety(self):
        """Test thread-safe increments from multiple threads."""
        stats = ThreadSafeStats()
        num_threads = 10
        increments_per_thread = 100
        
        def increment_worker():
            for _ in range(increments_per_thread):
                stats.increment("counter")
        
        threads = [threading.Thread(target=increment_worker) for _ in range(num_threads)]
        
        for t in threads:
            t.start()
        
        for t in threads:
            t.join()
        
        # Should be exactly num_threads * increments_per_thread
        expected = num_threads * increments_per_thread
        assert stats.get("counter") == expected
    
    def test_concurrent_different_keys(self):
        """Test concurrent increments to different keys."""
        stats = ThreadSafeStats()
        
        def increment_uploaded():
            for _ in range(50):
                stats.increment("uploaded")
        
        def increment_verified():
            for _ in range(30):
                stats.increment("verified")
        
        t1 = threading.Thread(target=increment_uploaded)
        t2 = threading.Thread(target=increment_verified)
        
        t1.start()
        t2.start()
        
        t1.join()
        t2.join()
        
        assert stats.get("uploaded") == 50
        assert stats.get("verified") == 30
    
    def test_get_all_thread_safe(self):
        """Test get_all is thread-safe."""
        stats = ThreadSafeStats()
        stats.increment("uploaded", 100)
        
        results = []
        
        def reader():
            for _ in range(10):
                snapshot = stats.get_all()
                results.append(snapshot.get("uploaded", 0))
                time.sleep(0.001)
        
        def writer():
            for i in range(10):
                stats.increment("uploaded")
                time.sleep(0.001)
        
        t1 = threading.Thread(target=reader)
        t2 = threading.Thread(target=writer)
        
        t1.start()
        t2.start()
        
        t1.join()
        t2.join()
        
        # All reads should return valid values (no corruption)
        assert all(r >= 100 for r in results)
        assert stats.get("uploaded") == 110


class TestCreateStats:
    """Tests for create_stats factory function."""
    
    def test_create_stats(self):
        """Test factory function creates ThreadSafeStats."""
        stats = create_stats()
        assert isinstance(stats, ThreadSafeStats)
        
        stats.increment("test")
        assert stats.get("test") == 1
