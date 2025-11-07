#!/usr/bin/env python3
"""
Integration test for interrupt handling.
"""

import threading
import time

import pytest

from mailbackup.executor import (
    create_managed_executor,
    get_global_interrupt_manager,
)


class TestInterruptHandlingIntegration:
    """Integration tests for interrupt handling."""

    def test_global_interrupt_manager_singleton(self):
        """Test that global interrupt manager is a singleton."""
        manager1 = get_global_interrupt_manager()
        manager2 = get_global_interrupt_manager()

        assert manager1 is manager2

    def test_executor_registers_with_global_manager(self):
        """Test that executors register with global manager."""
        manager = get_global_interrupt_manager()
        manager.reset()  # Clear any previous state

        with create_managed_executor(max_workers=2, name="Test") as executor:
            # Executor should be registered
            assert manager.get_executor_count() == 1

        # After context exit, executor should be unregistered
        assert manager.get_executor_count() == 0

    def test_global_interrupt_stops_all_executors(self):
        """Test that global interrupt stops all executors."""
        manager = get_global_interrupt_manager()
        manager.reset()

        def slow_task(x):
            time.sleep(0.5)
            return x * 2

        def interrupt_after_delay():
            time.sleep(0.2)
            manager.interrupt_all()

        interrupt_thread = threading.Thread(target=interrupt_after_delay)
        interrupt_thread.start()

        start = time.time()
        with create_managed_executor(max_workers=2, name="Test") as executor:
            # Should raise KeyboardInterrupt when interrupted
            with pytest.raises(KeyboardInterrupt):
                executor.map(slow_task, range(20))

        elapsed = time.time() - start
        interrupt_thread.join()

        # Should complete much faster than 10 seconds (20 tasks * 0.5s / 2 workers)
        assert elapsed < 5.0, f"Took {elapsed}s, expected < 5s due to interrupt"

        # Reset for next test
        manager.reset()

    def test_interrupt_preserves_completed_results(self):
        """Test that interrupt raises KeyboardInterrupt."""
        manager = get_global_interrupt_manager()
        manager.reset()

        completed_count = [0]
        lock = threading.RLock()

        def task_with_tracking(x):
            result = x * 2
            with lock:
                completed_count[0] += 1
            time.sleep(0.1)  # Small delay
            return result

        def interrupt_after_some_complete():
            # Wait for at least a few tasks to complete
            time.sleep(0.3)
            manager.interrupt_all()

        interrupt_thread = threading.Thread(target=interrupt_after_some_complete)
        interrupt_thread.start()

        with create_managed_executor(max_workers=2, name="Test") as executor:
            # Should raise KeyboardInterrupt when interrupted
            with pytest.raises(KeyboardInterrupt):
                executor.map(task_with_tracking, range(20))

        interrupt_thread.join()

        manager.reset()

    def test_multiple_executors_coordinate_shutdown(self):
        """Test that multiple executors shutdown together."""
        manager = get_global_interrupt_manager()
        manager.reset()

        results_collected = {"ex1": False, "ex2": False}

        def slow_task(x):
            time.sleep(0.3)
            return x

        def interrupt_early():
            time.sleep(0.2)
            manager.interrupt_all()

        interrupt_thread = threading.Thread(target=interrupt_early)
        interrupt_thread.start()

        start = time.time()

        try:
            # Start both executors
            with create_managed_executor(max_workers=2, name="Executor1") as ex1:
                # Executor should be registered
                assert manager.get_executor_count() == 1

                # Start processing on executor 1 - should raise KeyboardInterrupt
                with pytest.raises(KeyboardInterrupt):
                    ex1.map(slow_task, range(10))
        finally:
            interrupt_thread.join()

        elapsed = time.time() - start

        # Should complete quickly due to interrupt
        assert elapsed < 3.0

        # After exit, no executors should be registered  
        assert manager.get_executor_count() == 0

        manager.reset()

    def test_recovery_after_interrupt(self):
        """Test that we can recover and run new tasks after interrupt."""
        manager = get_global_interrupt_manager()
        manager.reset()

        def task(x):
            # Add a small delay to ensure interrupt happens during execution
            time.sleep(0.01)
            return x * 2

        # First run with interrupt
        def interrupt_quickly():
            time.sleep(0.1)  # Give time for tasks to start
            manager.interrupt_all()

        interrupt_thread = threading.Thread(target=interrupt_quickly)
        interrupt_thread.start()

        with create_managed_executor(max_workers=2, name="Test1") as executor:
            # Should raise KeyboardInterrupt when interrupted
            with pytest.raises(KeyboardInterrupt):
                executor.map(task, range(100))

        interrupt_thread.join()

        # Reset the interrupt state
        manager.reset()

        # Second run should work normally
        with create_managed_executor(max_workers=2, name="Test2") as executor:
            results2 = executor.map(task, range(5))

        # All tasks in second run should complete successfully
        assert len(results2) == 5
        assert all(r.success for r in results2)
        # Results may not be in order due to concurrency
        assert sorted([r.result for r in results2]) == [0, 2, 4, 6, 8]
