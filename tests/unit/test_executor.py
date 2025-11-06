#!/usr/bin/env python3
"""
Unit tests for executor.py module.
"""

import threading
import time

import pytest

from mailbackup.executor import (
    InterruptFlag,
    ManagedThreadPoolExecutor,
    TaskResult,
    create_managed_executor,
    get_global_interrupt_manager,
)


class TestInterruptFlag:
    """Tests for InterruptFlag class."""

    def test_initial_state(self):
        """Test initial state is not set."""
        flag = InterruptFlag()
        assert not flag.is_set()

    def test_set_and_check(self):
        """Test setting and checking flag."""
        flag = InterruptFlag()
        flag.set()
        assert flag.is_set()

    def test_clear(self):
        """Test clearing flag."""
        flag = InterruptFlag()
        flag.set()
        assert flag.is_set()
        flag.clear()
        assert not flag.is_set()

    def test_thread_safety(self):
        """Test flag is thread-safe."""
        flag = InterruptFlag()

        def set_flag():
            time.sleep(0.01)
            flag.set()

        def check_flag(results):
            while not flag.is_set():
                time.sleep(0.001)
            results.append(True)

        results = []
        t1 = threading.Thread(target=set_flag)
        t2 = threading.Thread(target=check_flag, args=(results,))

        t2.start()
        t1.start()

        t1.join(timeout=1.0)
        t2.join(timeout=1.0)

        assert len(results) == 1
        assert flag.is_set()


class TestTaskResult:
    """Tests for TaskResult class."""

    def test_success_result(self):
        """Test successful task result."""
        result = TaskResult(success=True, result=42, item="test")
        assert result.success
        assert result.result == 42
        assert result.exception is None
        assert result.item == "test"

    def test_failure_result(self):
        """Test failed task result."""
        exc = ValueError("test error")
        result = TaskResult(success=False, exception=exc, item="test")
        assert not result.success
        assert result.result is None
        assert result.exception == exc
        assert result.item == "test"


class TestManagedThreadPoolExecutor:
    """Tests for ManagedThreadPoolExecutor class."""

    def test_context_manager(self):
        """Test context manager protocol."""
        executor = ManagedThreadPoolExecutor(max_workers=2, name="Test")

        with executor as ex:
            assert ex._executor is not None

        assert executor._executor is None

    def test_submit_simple_task(self):
        """Test submitting a simple task."""

        def double(x):
            return x * 2

        with ManagedThreadPoolExecutor(max_workers=2, name="Test") as executor:
            future = executor.submit(double, 5)
            result = future.result(timeout=1.0)
            assert result == 10

    def test_submit_without_context_manager(self):
        """Test submit raises error without context manager."""
        executor = ManagedThreadPoolExecutor(max_workers=2, name="Test")

        with pytest.raises(RuntimeError, match="Executor not started"):
            executor.submit(lambda x: x, 1)

    def test_map_simple(self):
        """Test mapping function over items."""

        def square(x):
            return x * x

        with ManagedThreadPoolExecutor(max_workers=2, name="Test") as executor:
            results = executor.map(square, [1, 2, 3, 4, 5])

        assert len(results) == 5
        assert all(r.success for r in results)
        # Results may not be in order due to concurrency
        assert sorted([r.result for r in results]) == [1, 4, 9, 16, 25]

    def test_map_empty_items(self):
        """Test mapping with empty items."""
        with ManagedThreadPoolExecutor(max_workers=2, name="Test") as executor:
            results = executor.map(lambda x: x, [])

        assert len(results) == 0

    def test_map_with_exceptions(self):
        """Test mapping handles exceptions properly."""

        def failing_func(x):
            if x == 3:
                raise ValueError("Three is bad")
            return x * 2

        with ManagedThreadPoolExecutor(max_workers=2, name="Test") as executor:
            results = executor.map(failing_func, [1, 2, 3, 4, 5])

        assert len(results) == 5
        assert sum(1 for r in results if r.success) == 4
        assert sum(1 for r in results if not r.success) == 1

        # Find the failed result
        failed = [r for r in results if not r.success][0]
        assert isinstance(failed.exception, ValueError)
        assert failed.item == 3

    def test_interrupt_flag_stops_tasks(self):
        """Test interrupt flag stops new tasks."""

        def slow_task(x):
            time.sleep(0.1)
            return x

        with ManagedThreadPoolExecutor(max_workers=2, name="Test") as executor:
            # Set interrupt flag before mapping
            executor.interrupt_flag.set()

            with pytest.raises(InterruptedError):
                executor.submit(slow_task, 1)

    def test_interrupt_during_map(self):
        """Test interrupt during map operation raises KeyboardInterrupt."""

        def slow_task(x):
            if x > 2:
                time.sleep(0.5)
            return x

        with ManagedThreadPoolExecutor(max_workers=2, name="Test") as executor:
            # Start mapping and interrupt partway through
            def interrupt_after_delay():
                time.sleep(0.05)
                executor.interrupt_flag.set()

            interrupt_thread = threading.Thread(target=interrupt_after_delay)
            interrupt_thread.start()

            # The map should raise KeyboardInterrupt when interrupted
            with pytest.raises(KeyboardInterrupt):
                executor.map(slow_task, range(10))
            
            interrupt_thread.join()

    def test_max_workers_minimum(self):
        """Test max_workers is at least 1."""
        executor = ManagedThreadPoolExecutor(max_workers=0, name="Test")
        assert executor.max_workers == 1

        executor = ManagedThreadPoolExecutor(max_workers=-5, name="Test")
        assert executor.max_workers == 1

    def test_shutdown(self):
        """Test shutdown method."""
        executor = ManagedThreadPoolExecutor(max_workers=2, name="Test")

        with executor:
            pass  # Executor should shut down automatically

        assert executor._executor is None
        assert len(executor._futures) == 0

    def test_shutdown_idempotent(self):
        """Test shutdown can be called multiple times safely."""
        executor = ManagedThreadPoolExecutor(max_workers=2, name="Test")

        with executor:
            pass

        # Call shutdown again
        executor.shutdown()
        assert executor._executor is None

    def test_progress_logging(self, caplog):
        """Test progress logging at intervals."""
        import logging
        
        # Set propagate=True on mailbackup logger to allow caplog to capture
        mailbackup_logger = logging.getLogger("mailbackup")
        old_propagate = mailbackup_logger.propagate
        mailbackup_logger.propagate = True
        caplog.set_level(logging.INFO, logger="mailbackup")
        
        try:
            def simple_task(x):
                return x

            # Set progress interval to 3 for testing
            with ManagedThreadPoolExecutor(
                    max_workers=2,
                    name="ProgressTest",
                    progress_interval=3
            ) as executor:
                results = executor.map(simple_task, range(10))

            assert len(results) == 10
            # Should have progress messages
            progress_msgs = [r for r in caplog.records if "Progress" in r.getMessage()]
            assert len(progress_msgs) > 0
        finally:
            mailbackup_logger.propagate = old_propagate


class TestGlobalInterruptManager:
    """Tests for GlobalInterruptManager."""

    def test_get_executor_count(self):
        """Test getting executor count."""
        manager = get_global_interrupt_manager()
        manager.reset()

        assert manager.get_executor_count() == 0

        with create_managed_executor(max_workers=2, name="Test1"):
            assert manager.get_executor_count() == 1

            with create_managed_executor(max_workers=2, name="Test2"):
                assert manager.get_executor_count() == 2

            assert manager.get_executor_count() == 1

        assert manager.get_executor_count() == 0
        manager.reset()


class TestCreateManagedExecutor:
    """Tests for create_managed_executor factory function."""

    def test_create_executor(self):
        """Test factory function creates executor."""
        executor = create_managed_executor(max_workers=4, name="Factory")

        assert isinstance(executor, ManagedThreadPoolExecutor)
        assert executor.max_workers == 4
        assert executor.name == "Factory"

    def test_create_with_defaults(self):
        """Test factory with default parameters."""
        executor = create_managed_executor(max_workers=2)

        assert isinstance(executor, ManagedThreadPoolExecutor)
        assert executor.max_workers == 2
        assert executor.name == "Worker"
        assert executor.progress_interval == 25


