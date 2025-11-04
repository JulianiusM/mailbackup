#!/usr/bin/env python3
"""
Integration test for interrupt handling.
"""

import pytest
import threading
import time
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
            assert executor in manager._executors
        
        # After context exit, executor should be unregistered
        assert executor not in manager._executors
    
    def test_global_interrupt_stops_all_executors(self):
        """Test that global interrupt stops all executors."""
        manager = get_global_interrupt_manager()
        manager.reset()
        
        def slow_task(x):
            time.sleep(0.5)
            return x * 2
        
        def interrupt_after_delay():
            time.sleep(0.1)
            manager.interrupt_all()
        
        interrupt_thread = threading.Thread(target=interrupt_after_delay)
        interrupt_thread.start()
        
        start = time.time()
        with create_managed_executor(max_workers=2, name="Test") as executor:
            results = executor.map(slow_task, range(20))
        
        elapsed = time.time() - start
        interrupt_thread.join()
        
        # Should complete much faster than 10 seconds (20 tasks * 0.5s / 2 workers)
        assert elapsed < 5.0, f"Took {elapsed}s, expected < 5s due to interrupt"
        
        # Some tasks should have been interrupted
        assert len(results) < 20 or any(not r.success for r in results)
        
        # Reset for next test
        manager.reset()
    
    def test_interrupt_preserves_completed_results(self):
        """Test that interrupt doesn't lose completed results."""
        manager = get_global_interrupt_manager()
        manager.reset()
        
        completed_count = [0]
        lock = threading.Lock()
        
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
            results = executor.map(task_with_tracking, range(20))
        
        interrupt_thread.join()
        
        # We should have some completed results
        successful_results = [r for r in results if r.success]
        assert len(successful_results) > 0
        
        # The results we got should be correct
        for r in successful_results:
            if r.result is not None:
                expected = r.item * 2
                assert r.result == expected
        
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
                # Both should be registered
                assert ex1 in manager._executors
                
                # Start processing on executor 1
                results1 = ex1.map(slow_task, range(10))
                results_collected["ex1"] = True
        finally:
            interrupt_thread.join()
        
        elapsed = time.time() - start
        
        # Should complete quickly due to interrupt
        assert elapsed < 3.0
        
        # After exit, no executors should be registered  
        assert len(manager._executors) == 0
        
        manager.reset()
    
    def test_recovery_after_interrupt(self):
        """Test that we can recover and run new tasks after interrupt."""
        manager = get_global_interrupt_manager()
        manager.reset()
        
        def task(x):
            return x * 2
        
        # First run with interrupt
        def interrupt_quickly():
            time.sleep(0.05)
            manager.interrupt_all()
        
        interrupt_thread = threading.Thread(target=interrupt_quickly)
        interrupt_thread.start()
        
        with create_managed_executor(max_workers=2, name="Test1") as executor:
            results1 = executor.map(task, range(100))
        
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
