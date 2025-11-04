#!/usr/bin/env python3

"""
executor.py

Centralized multithreading module for mailbackup.

Provides a managed thread pool executor with proper interrupt handling,
graceful shutdown, exception propagation, and state recovery capabilities.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from dataclasses import dataclass
from typing import Callable, Iterable, TypeVar, Generic, Any, Optional

from mailbackup.logger import get_logger

T = TypeVar('T')
R = TypeVar('R')


@dataclass
class TaskResult(Generic[T]):
    """Result wrapper for task execution."""
    success: bool
    result: Optional[T] = None
    exception: Optional[Exception] = None
    item: Optional[Any] = None  # Original item being processed


class InterruptFlag:
    """Thread-safe interrupt flag for signaling shutdown."""

    def __init__(self):
        self._interrupted = threading.Event()
        self._lock = threading.Lock()

    def set(self):
        """Signal that an interrupt has occurred."""
        with self._lock:
            self._interrupted.set()

    def is_set(self) -> bool:
        """Check if interrupt has been signaled."""
        return self._interrupted.is_set()

    def clear(self):
        """Clear the interrupt flag."""
        with self._lock:
            self._interrupted.clear()


class GlobalInterruptManager:
    """
    Global interrupt manager to coordinate shutdown across all executors.
    
    This singleton provides a centralized way to handle interrupts and
    signal all active executors to shut down gracefully.
    """

    _instance: Optional[GlobalInterruptManager] = None
    _lock = threading.Lock()

    def __new__(cls):
        """Ensure only one instance exists (singleton pattern)."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Initialize the global interrupt manager."""
        if self._initialized:
            return

        self._initialized = True
        self._global_flag = InterruptFlag()
        self._executors: list[ManagedThreadPoolExecutor] = []
        self._executors_lock = threading.Lock()
        self.logger = get_logger(__name__)

    def register_executor(self, executor: ManagedThreadPoolExecutor):
        """Register an executor to be interrupted on global interrupt."""
        with self._executors_lock:
            if executor not in self._executors:
                self._executors.append(executor)

    def unregister_executor(self, executor: ManagedThreadPoolExecutor):
        """Unregister an executor."""
        with self._executors_lock:
            if executor in self._executors:
                self._executors.remove(executor)

    def interrupt_all(self):
        """Signal interrupt to all registered executors."""
        self.logger.warning("Global interrupt signaled - shutting down all executors...")
        self._global_flag.set()

        with self._executors_lock:
            executors_to_interrupt = list(self._executors)

        for executor in executors_to_interrupt:
            try:
                executor.interrupt()
            except Exception as e:
                self.logger.error(f"Error interrupting executor {executor.name}: {e}")

    def is_interrupted(self) -> bool:
        """Check if global interrupt has been signaled."""
        return self._global_flag.is_set()

    def get_executor_count(self) -> int:
        """Get the number of registered executors."""
        with self._executors_lock:
            return len(self._executors)

    def reset(self):
        """Reset the interrupt state (for testing or recovery)."""
        self._global_flag.clear()
        with self._executors_lock:
            self._executors.clear()


# Global instance
_global_interrupt_manager = GlobalInterruptManager()


class ManagedThreadPoolExecutor:
    """
    Thread pool executor with proper interrupt handling and graceful shutdown.
    
    Features:
    - Graceful shutdown on interrupt signals
    - Proper exception handling and propagation
    - Progress tracking and logging
    - Thread-safe state management
    - Support for recovery after interrupts
    - Integration with global interrupt manager
    """

    def __init__(
            self,
            max_workers: int,
            name: str = "Worker",
            progress_interval: int = 25,
    ):
        """
        Initialize managed thread pool executor.
        
        Args:
            max_workers: Maximum number of worker threads
            name: Name for logging and identification
            progress_interval: Log progress every N completed tasks
        """
        self.logger = get_logger(__name__)
        self.max_workers = max(1, max_workers)
        self.name = name
        self.progress_interval = progress_interval
        self.interrupt_flag = InterruptFlag()
        self._executor: Optional[ThreadPoolExecutor] = None
        self._futures: list[Future] = []
        self._completed = 0
        self._total = 0
        self._lock = threading.Lock()
        self._registered = False

    def __enter__(self):
        """Context manager entry."""
        self._executor = ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix=self.name
        )
        # Register with global interrupt manager
        _global_interrupt_manager.register_executor(self)
        self._registered = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with graceful shutdown."""
        if self._registered:
            _global_interrupt_manager.unregister_executor(self)
            self._registered = False
        self.shutdown(wait=True)
        return False  # Don't suppress exceptions

    def submit(
            self,
            fn: Callable[[T], R],
            item: T,
    ) -> Future[R]:
        """
        Submit a task to the thread pool.
        
        Args:
            fn: Callable to execute
            item: Item to pass to the callable
            
        Returns:
            Future representing the task
        """
        if self._executor is None:
            raise RuntimeError("Executor not started. Use context manager.")

        if self.interrupt_flag.is_set() or _global_interrupt_manager.is_interrupted():
            raise InterruptedError("Executor has been interrupted")

        # Wrap the function to check interrupt flag
        def wrapped():
            if self.interrupt_flag.is_set() or _global_interrupt_manager.is_interrupted():
                raise InterruptedError("Task cancelled due to interrupt")
            try:
                return fn(item)
            except Exception as e:
                self.logger.error(f"Task failed for item {item}: {e}", exc_info=True)
                raise

        future = self._executor.submit(wrapped)
        with self._lock:
            self._futures.append(future)
            self._total += 1

        return future

    def map(
            self,
            fn: Callable[[T], R],
            items: Iterable[T],
            increment_callback: Callable[[TaskResult[R]], None] = None,
    ) -> list[TaskResult[R]]:
        """
        Map a function over items with proper error handling.
        
        Args:
            fn: Callable to execute for each item
            items: Iterable of items to process
            increment_callback: Callable to increment the progress (once a task is completed)
            
        Returns:
            List of TaskResult objects
        """
        if self._executor is None:
            raise RuntimeError("Executor not started. Use context manager.")

        results: list[TaskResult[R]] = []
        items_list = list(items)
        total = len(items_list)

        if total == 0:
            return results

        self.logger.info(f"Starting {self.name} for {total} items with {self.max_workers} workers")

        # Submit all tasks
        futures_map: dict[Future[R], T] = {}
        for item in items_list:
            if self.interrupt_flag.is_set() or _global_interrupt_manager.is_interrupted():
                self.logger.warning(f"{self.name} interrupted before all tasks submitted")
                break

            future = self.submit(fn, item)
            futures_map[future] = item

        # Collect results as they complete
        try:
            for future in as_completed(futures_map.keys()):
                if self.interrupt_flag.is_set() or _global_interrupt_manager.is_interrupted():
                    self.logger.warning(f"{self.name} interrupted, stopping result collection")
                    break

                item = futures_map[future]
                task_res = None
                try:
                    result = future.result()
                    task_res = TaskResult(
                        success=True,
                        result=result,
                        item=item
                    )
                except InterruptedError:
                    self.logger.warning(f"Task interrupted for item: {item}")
                    task_res = TaskResult(
                        success=False,
                        exception=InterruptedError("Task was interrupted"),
                        item=item
                    )
                except Exception as e:
                    self.logger.error(f"Task failed for item {item}: {e}")
                    task_res = TaskResult(
                        success=False,
                        exception=e,
                        item=item
                    )

                results.append(task_res)
                increment_callback(task_res)

                with self._lock:
                    self._completed += 1
                    completed = self._completed

                # Progress logging
                if completed % self.progress_interval == 0 or completed == total:
                    remaining = total - completed
                    self.logger.info(
                        f"[{self.name} Progress] {completed}/{total} tasks completed "
                        f"({remaining} remaining)"
                    )

        except KeyboardInterrupt:
            self.logger.warning(f"{self.name} received KeyboardInterrupt")
            self.interrupt_flag.set()
            raise

        return results

    def shutdown(self, wait: bool = True, cancel_futures: bool = True):
        """
        Shutdown the executor gracefully.
        
        Args:
            wait: Wait for pending tasks to complete
            cancel_futures: Cancel pending futures
        """
        if self._executor is None:
            return

        if cancel_futures:
            with self._lock:
                for future in self._futures:
                    if not future.done():
                        future.cancel()

        try:
            self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)
        except Exception as e:
            self.logger.error(f"Error during executor shutdown: {e}")
        finally:
            self._executor = None
            self._futures.clear()

    def interrupt(self):
        """Signal interrupt to all running tasks."""
        self.logger.warning(f"Interrupting {self.name}...")
        self.interrupt_flag.set()
        self.shutdown(wait=False, cancel_futures=True)


def create_managed_executor(
        max_workers: int,
        name: str = "Worker",
        progress_interval: int = 25,
) -> ManagedThreadPoolExecutor:
    """
    Factory function to create a managed thread pool executor.
    
    Args:
        max_workers: Maximum number of worker threads
        name: Name for logging and identification
        progress_interval: Log progress every N completed tasks
        
    Returns:
        ManagedThreadPoolExecutor instance
    """
    return ManagedThreadPoolExecutor(
        max_workers=max_workers,
        name=name,
        progress_interval=progress_interval,
    )


def get_global_interrupt_manager() -> GlobalInterruptManager:
    """
    Get the global interrupt manager instance.
    
    Returns:
        GlobalInterruptManager singleton instance
    """
    return _global_interrupt_manager
