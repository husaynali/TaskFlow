"""
retry.py – Retry policy and backoff logic for TaskFlow.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

from .task import Task, TaskStatus

logger = logging.getLogger("taskflow.retry")


class RetryManager:
    """
    Evaluates whether a failed task should be retried and applies
    exponential backoff before the next attempt.

    Backoff formula:
        delay = base_delay * (2 ** (attempt - 1))

    so attempt 1 → base_delay, attempt 2 → 2×, attempt 3 → 4×, …
    The delay is capped at *max_delay* seconds (default 60 s).
    """

    def __init__(self, max_delay: float = 60.0) -> None:
        self.max_delay = max_delay

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def should_retry(self, task: Task) -> bool:
        """
        Return True if the task has remaining retry attempts.

        The *attempt* counter is incremented by the executor before this
        is called, so we compare attempt <= retries (not <).
        """
        eligible = task.attempt <= task.retries
        if eligible:
            logger.debug(
                "Task '%s' will retry (attempt %d/%d).",
                task.name,
                task.attempt,
                task.retries,
            )
        else:
            logger.warning(
                "Task '%s' exhausted all %d retries.",
                task.name,
                task.retries,
            )
        return eligible

    def wait(self, task: Task) -> None:
        """
        Block the calling thread for the backoff period appropriate to the
        task's current attempt number.
        """
        delay = self._backoff(task)
        if delay > 0:
            logger.debug(
                "Backing off %.1fs before retrying task '%s' (attempt %d).",
                delay,
                task.name,
                task.attempt,
            )
            time.sleep(delay)

    def handle_failure(self, task: Task, error: Exception) -> bool:
        """
        Record the failure on the task and decide what to do next.

        Returns:
            True  → task will be retried (caller should re-queue it).
            False → task is permanently failed.
        """
        task.error = error

        if self.should_retry(task):
            self.wait(task)
            task.reset()          # back to PENDING so scheduler re-queues it
            return True

        task.status = TaskStatus.FAILED
        return False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _backoff(self, task: Task) -> float:
        """Compute capped exponential backoff for the given task."""
        if task.attempt <= 1:
            return 0.0
        raw = task.retry_delay * (2 ** (task.attempt - 2))
        return min(raw, self.max_delay)
