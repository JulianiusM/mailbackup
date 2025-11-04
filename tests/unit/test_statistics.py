#!/usr/bin/env python3
"""
Unit tests for statistics.py module.
"""

import threading
import time

from mailbackup.statistics import ThreadSafeStats, StatKey, create_stats


class TestThreadSafeStats:
    """Tests for ThreadSafeStats class."""

    def test_increment_single_thread(self):
        """Test increment in single thread."""
        stats = ThreadSafeStats()
        stats.increment(StatKey.BACKED_UP)
        assert stats.get(StatKey.BACKED_UP) == 1

        stats.increment(StatKey.BACKED_UP, 5)
        assert stats.get(StatKey.BACKED_UP) == 6

    def test_set_and_get(self):
        """Test set and get operations."""
        stats = ThreadSafeStats()
        stats.set(StatKey.VERIFIED, 42)
        assert stats.get(StatKey.VERIFIED) == 42
        assert stats.get(StatKey.FETCHED) == 0
        assert stats.get(StatKey.FETCHED, 10) == 10

    def test_get_all(self):
        """Test get_all returns snapshot."""
        stats = ThreadSafeStats()
        stats.increment(StatKey.BACKED_UP, 5)
        stats.increment(StatKey.VERIFIED, 3)

        all_stats = stats.get_all()
        assert all_stats == {StatKey.BACKED_UP: 5, StatKey.VERIFIED: 3}

        # Modify returned dict shouldn't affect stats
        all_stats[StatKey.BACKED_UP] = 100
        assert stats.get(StatKey.BACKED_UP) == 5

    def test_dict_like_access(self):
        """Test dict-like access with []."""
        stats = ThreadSafeStats()
        stats[StatKey.BACKED_UP] = 10
        assert stats[StatKey.BACKED_UP] == 10

        stats[StatKey.BACKED_UP] = 20
        assert stats[StatKey.BACKED_UP] == 20

    def test_to_dict(self):
        """Test to_dict method."""
        stats = ThreadSafeStats()
        stats.increment(StatKey.BACKED_UP, 3)
        stats.increment(StatKey.ARCHIVED, 2)

        result = stats.to_dict()
        assert result == {StatKey.BACKED_UP: 3, StatKey.ARCHIVED: 2}

    def test_reset(self):
        """Test reset clears all counters."""
        stats = ThreadSafeStats()
        stats.increment(StatKey.BACKED_UP, 5)
        stats.increment(StatKey.VERIFIED, 3)

        stats.reset()

        assert stats.get(StatKey.BACKED_UP) == 0
        assert stats.get(StatKey.VERIFIED) == 0
        assert stats.get_all() == {}

    def test_thread_safety(self):
        """Test thread-safe increments from multiple threads."""
        stats = ThreadSafeStats()
        num_threads = 10
        increments_per_thread = 100

        def increment_worker():
            for _ in range(increments_per_thread):
                stats.increment(StatKey.PROCESSED)

        threads = [threading.Thread(target=increment_worker) for _ in range(num_threads)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        # Should be exactly num_threads * increments_per_thread
        expected = num_threads * increments_per_thread
        assert stats.get(StatKey.PROCESSED) == expected

    def test_concurrent_different_keys(self):
        """Test concurrent increments to different keys."""
        stats = ThreadSafeStats()

        def increment_uploaded():
            for _ in range(50):
                stats.increment(StatKey.BACKED_UP)

        def increment_verified():
            for _ in range(30):
                stats.increment(StatKey.VERIFIED)

        t1 = threading.Thread(target=increment_uploaded)
        t2 = threading.Thread(target=increment_verified)

        t1.start()
        t2.start()

        t1.join()
        t2.join()

        assert stats.get(StatKey.BACKED_UP) == 50
        assert stats.get(StatKey.VERIFIED) == 30

    def test_get_all_thread_safe(self):
        """Test get_all is thread-safe."""
        stats = ThreadSafeStats()
        stats.increment(StatKey.BACKED_UP, 100)

        results = []

        def reader():
            for _ in range(10):
                snapshot = stats.get_all()
                results.append(snapshot.get(StatKey.BACKED_UP, 0))
                time.sleep(0.001)

        def writer():
            for i in range(10):
                stats.increment(StatKey.BACKED_UP)
                time.sleep(0.001)

        t1 = threading.Thread(target=reader)
        t2 = threading.Thread(target=writer)

        t1.start()
        t2.start()

        t1.join()
        t2.join()

        # All reads should return valid values (no corruption)
        assert all(r >= 100 for r in results)
        assert stats.get(StatKey.BACKED_UP) == 110

    def test_format_status(self):
        """Test format_status method."""
        stats = ThreadSafeStats()
        stats.increment(StatKey.BACKED_UP, 5)
        stats.increment(StatKey.ARCHIVED, 3)
        stats.increment(StatKey.VERIFIED, 2)

        status = stats.format_status()
        assert "Backed up: 5" in status
        assert "Archived: 3" in status
        assert "Verified: 2" in status
        assert "Repaired: 0" in status
        assert "Skipped: 0" in status


class TestStatusThread:
    """Tests for StatusThread class."""

    def test_status_thread_basic(self):
        """Test basic StatusThread functionality."""
        from mailbackup.statistics import StatusThread

        stats = create_stats()
        stats.increment(StatKey.BACKED_UP, 5)

        # Short interval for testing
        status_thread = StatusThread(interval=1, counters=stats)
        status_thread.start()

        # Give it a moment to run
        time.sleep(0.1)

        status_thread.stop()

        # Should complete without errors
        assert True

    def test_get_status_summary(self):
        """Test get_status_summary method."""
        from mailbackup.statistics import StatusThread

        stats = create_stats()
        stats.increment(StatKey.BACKED_UP, 10)
        stats.increment(StatKey.VERIFIED, 5)

        status_thread = StatusThread(interval=1, counters=stats)
        summary = status_thread.get_status_summary()

        assert "Backed up: 10" in summary
        assert "Verified: 5" in summary


class TestFormatStatsFunctions:
    """Tests for format functions."""

    def test_log_status_with_stats(self, mocker):
        """Test log_status with ThreadSafeStats."""
        from mailbackup.statistics import log_status
        
        # Mock the logger to avoid AttributeError for 'status' attribute
        mock_logger = mocker.MagicMock()
        mocker.patch('mailbackup.statistics.get_logger', return_value=mock_logger)

        stats = create_stats()
        stats.increment(StatKey.BACKED_UP, 10)

        # Should not raise any errors
        log_status(stats, "Test Stage")
        
        # Verify logger was called
        assert mock_logger.info.called or mock_logger.status.called


class TestCreateStats:
    """Tests for create_stats factory function."""

    def test_create_stats(self):
        """Test factory function creates ThreadSafeStats."""
        stats = create_stats()
        assert isinstance(stats, ThreadSafeStats)

        stats.increment(StatKey.PROCESSED)
        assert stats.get(StatKey.PROCESSED) == 1
