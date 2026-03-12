"""
scheduler.py – Dependency-aware task scheduler for TaskFlow.
"""

from __future__ import annotations

import logging
import threading
from typing import Callable, List, Set

from .dag import DAG
from .queue import TaskQueue
from .task import Task, TaskStatus

logger = logging.getLogger("taskflow.scheduler")


class Scheduler:
    """
    Continuously monitors the DAG and enqueues tasks whose dependencies
    have all completed successfully.

    The scheduler runs on a **dedicated background thread** so the main
    thread and the executor's worker pool can all proceed concurrently
    without blocking each other.

    Design:
        * When a task completes the executor calls :meth:`notify_done`.
        * The scheduler wakes up, scans for newly-ready tasks, and
          enqueues them.
        * It exits once every task in the DAG is in a terminal state
          (COMPLETED, FAILED, or SKIPPED).
    """

    def __init__(self, dag: DAG, task_queue: TaskQueue) -> None:
        self._dag = dag
        self._queue = task_queue
        self._completed_ids: Set[str] = set()
        self._lock = threading.Lock()
        self._event = threading.Event()      # wakes the scheduler loop
        self._stop_event = threading.Event()
        self._on_pipeline_done: List[Callable] = []

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the scheduler on a daemon background thread."""
        self._thread = threading.Thread(
            target=self._run, name="taskflow-scheduler", daemon=True
        )
        self._thread.start()
        logger.debug("Scheduler started.")
        # Seed initial tasks (those with no dependencies)
        self._schedule_ready()

    def stop(self) -> None:
        """Signal the scheduler to exit after the current pass."""
        self._stop_event.set()
        self._event.set()  # unblock any sleeping wait
        self._thread.join(timeout=5)
        logger.debug("Scheduler stopped.")

    # ------------------------------------------------------------------
    # Notification
    # ------------------------------------------------------------------

    def notify_done(self, task: Task) -> None:
        """
        Called by the executor when a task finishes (success or failure).

        If the task succeeded we record it as completed so its dependents
        can be unlocked.  Either way we wake the scheduling loop.
        """
        with self._lock:
            if task.status == TaskStatus.COMPLETED:
                self._completed_ids.add(task.id)
        self._event.set()

    # ------------------------------------------------------------------
    # Internal loop
    # ------------------------------------------------------------------

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self._event.wait(timeout=0.05)
            self._event.clear()
            self._schedule_ready()
            if self._all_done():
                logger.debug("All tasks terminal – scheduler exiting.")
                break

    def _schedule_ready(self) -> None:
        """Find all tasks that are now unblocked and enqueue them."""
        with self._lock:
            completed_snapshot = set(self._completed_ids)

        ready = self._dag.get_ready_tasks(completed_snapshot)
        for task in ready:
            # Double-check under no lock (benign race – enqueue is idempotent
            # because get_ready_tasks checks task.status == PENDING)
            logger.info("Scheduling task '%s'.", task.name)
            self._queue.enqueue(task)

    def _all_done(self) -> bool:
        return all(t.is_done for t in self._dag.tasks)
