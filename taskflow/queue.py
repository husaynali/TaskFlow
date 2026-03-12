"""
queue.py – Thread-safe task queue for TaskFlow.
"""

from __future__ import annotations

import logging
import queue
import threading
from typing import List, Optional

from .task import Task, TaskStatus

logger = logging.getLogger("taskflow.queue")


class TaskQueue:
    """
    A thread-safe FIFO queue that holds tasks ready for execution.

    Internally uses :class:`queue.Queue` (backed by a collections.deque)
    so all operations are safe to call from multiple threads simultaneously.

    States managed here:
        PENDING   → task is waiting to be enqueued
        RUNNING   → task has been dispatched to a worker
        COMPLETED → task finished successfully
        FAILED    → task failed all retries
    """

    def __init__(self) -> None:
        self._q: queue.Queue[Task] = queue.Queue()
        self._running: List[Task] = []
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Enqueue / Dequeue
    # ------------------------------------------------------------------

    def enqueue(self, task: Task) -> None:
        """Add a task to the ready queue."""
        logger.debug("Enqueuing task '%s'.", task.name)
        task.status = TaskStatus.RUNNING   # mark optimistically
        self._q.put(task)

    def dequeue(self, timeout: float = 0.1) -> Optional[Task]:
        """
        Retrieve the next ready task, or None if the queue is empty
        within *timeout* seconds.
        """
        try:
            task = self._q.get(timeout=timeout)
            with self._lock:
                self._running.append(task)
            return task
        except queue.Empty:
            return None

    def task_done(self, task: Task) -> None:
        """Signal that the worker has finished processing *task*."""
        self._q.task_done()
        with self._lock:
            if task in self._running:
                self._running.remove(task)

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def pending_count(self) -> int:
        return self._q.qsize()

    @property
    def running_count(self) -> int:
        with self._lock:
            return len(self._running)

    @property
    def is_empty(self) -> bool:
        return self._q.empty()

    def join(self) -> None:
        """Block until all enqueued tasks have been processed."""
        self._q.join()

    def __repr__(self) -> str:
        return (
            f"TaskQueue(pending={self.pending_count}, "
            f"running={self.running_count})"
        )
