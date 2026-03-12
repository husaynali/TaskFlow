"""
executor.py – Worker pool executor for TaskFlow (threads or processes).
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from typing import Callable, Dict, List, Optional

from .queue import TaskQueue
from .retry import RetryManager
from .scheduler import Scheduler
from .task import Task, TaskStatus

logger = logging.getLogger("taskflow.executor")


# ---------------------------------------------------------------------------
# Helper: run a task's function in a worker (must be module-level for pickling)
# ---------------------------------------------------------------------------

def _run_fn(fn: Callable) -> object:
    """Thin wrapper executed inside a worker; returns the function's result."""
    return fn()


class Executor:
    """
    Pulls tasks from the :class:`TaskQueue`, dispatches them to a
    :class:`~concurrent.futures.Executor`, and feeds results back to
    the :class:`Scheduler`.

    Parameters:
        workers:   Number of parallel workers.
        mode:      ``"thread"`` (default) or ``"process"``.
        scheduler: The scheduler to notify on task completion.
        queue:     The queue to pull ready tasks from.
        retry_mgr: The retry manager to use on failures.
    """

    def __init__(
        self,
        workers: int,
        mode: str,
        scheduler: Scheduler,
        queue: TaskQueue,
        retry_mgr: RetryManager,
    ) -> None:
        self._workers = workers
        self._mode = mode
        self._scheduler = scheduler
        self._queue = queue
        self._retry_mgr = retry_mgr

        self._pool: Optional[ThreadPoolExecutor | ProcessPoolExecutor] = None
        self._futures: Dict[Future, Task] = {}
        self._lock = threading.Lock()
        self._stop_event = threading.Event()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Create the worker pool and begin pulling from the queue."""
        if self._mode == "process":
            self._pool = ProcessPoolExecutor(max_workers=self._workers)
        else:
            self._pool = ThreadPoolExecutor(max_workers=self._workers)

        logger.info(
            "Executor started (%s mode, %d workers).", self._mode, self._workers
        )
        self._dispatch_thread = threading.Thread(
            target=self._dispatch_loop, name="taskflow-executor", daemon=True
        )
        self._dispatch_thread.start()

    def stop(self) -> None:
        """Signal the dispatch loop to exit and shut down the pool."""
        self._stop_event.set()
        self._dispatch_thread.join(timeout=10)
        if self._pool:
            self._pool.shutdown(wait=True)
        logger.debug("Executor stopped.")

    # ------------------------------------------------------------------
    # Dispatch loop
    # ------------------------------------------------------------------

    def _dispatch_loop(self) -> None:
        """
        Continuously poll the queue for ready tasks and submit them to
        the worker pool, handling completed futures as they arrive.
        """
        while not self._stop_event.is_set():
            # --- process completed futures ---
            self._collect_done_futures()

            # --- fetch a new task ---
            task = self._queue.dequeue(timeout=0.05)
            if task is None:
                continue

            self._submit(task)

        # Final drain: wait for all in-flight futures
        self._drain()

    def _submit(self, task: Task) -> None:
        """Submit a task's function to the pool."""
        logger.info("Submitting task '%s' (attempt %d).", task.name, task.attempt + 1)
        task.attempt += 1
        task.status = TaskStatus.RUNNING

        future = self._pool.submit(_run_fn, task.fn)
        with self._lock:
            self._futures[future] = task

        future.add_done_callback(self._on_future_done)

    def _on_future_done(self, future: Future) -> None:
        """Called in the pool thread when a future resolves."""
        with self._lock:
            task = self._futures.pop(future, None)
        if task is None:
            return

        self._queue.task_done(task)

        exc = future.exception()
        if exc is None:
            task.result = future.result()
            task.status = TaskStatus.COMPLETED
            logger.info("Task '%s' completed successfully.", task.name)
            self._scheduler.notify_done(task)
        else:
            logger.warning("Task '%s' failed: %s", task.name, exc)
            will_retry = self._retry_mgr.handle_failure(task, exc)
            if will_retry:
                logger.info("Re-queuing task '%s' for retry.", task.name)
                self._queue.enqueue(task)
                self._scheduler.notify_done(task)  # scheduler re-evaluates
            else:
                logger.error(
                    "Task '%s' permanently failed after %d attempt(s).",
                    task.name,
                    task.attempt,
                )
                self._scheduler.notify_done(task)

    def _collect_done_futures(self) -> None:
        """Non-blocking sweep to clear any futures already resolved."""
        with self._lock:
            done = [f for f in self._futures if f.done()]
        # callbacks already fired; just remove stragglers
        with self._lock:
            for f in done:
                self._futures.pop(f, None)

    def _drain(self) -> None:
        """Wait for all remaining in-flight futures to finish."""
        logger.debug("Draining %d in-flight futures.", len(self._futures))
        with self._lock:
            remaining = list(self._futures.keys())
        for future in remaining:
            try:
                future.result()
            except Exception:
                pass  # already handled via callback

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def active_count(self) -> int:
        with self._lock:
            return len(self._futures)
