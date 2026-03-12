"""
task.py – Task definition and state management for TaskFlow.
"""

from __future__ import annotations

import uuid
from enum import Enum, auto
from typing import Any, Callable, List, Optional


class TaskStatus(Enum):
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    SKIPPED = auto()


class Task:
    """
    Represents a single unit of work in a TaskFlow pipeline.

    Attributes:
        id:           Unique identifier (auto-generated UUID).
        name:         Human-readable name (defaults to function name).
        fn:           The callable to execute.
        dependencies: List of Task objects this task depends on.
        retries:      Maximum number of retry attempts on failure.
        retry_delay:  Base delay (seconds) between retries – doubles each attempt.
        status:       Current TaskStatus.
        result:       Return value after successful execution.
        error:        Exception captured on failure.
        attempt:      How many times execution has been attempted.
    """

    def __init__(
        self,
        fn: Callable,
        dependencies: Optional[List["Task"]] = None,
        retries: int = 0,
        retry_delay: float = 1.0,
        name: Optional[str] = None,
    ) -> None:
        self.id: str = str(uuid.uuid4())
        self.name: str = name or fn.__name__
        self.fn: Callable = fn
        self.dependencies: List["Task"] = dependencies or []
        self.retries: int = retries
        self.retry_delay: float = retry_delay
        self.status: TaskStatus = TaskStatus.PENDING
        self.result: Any = None
        self.error: Optional[Exception] = None
        self.attempt: int = 0

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    @property
    def is_ready(self) -> bool:
        """True when all dependencies have completed successfully."""
        return all(dep.status == TaskStatus.COMPLETED for dep in self.dependencies)

    @property
    def is_done(self) -> bool:
        return self.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.SKIPPED)

    @property
    def can_retry(self) -> bool:
        return self.attempt <= self.retries

    def reset(self) -> None:
        """Reset state so the task can be retried."""
        self.status = TaskStatus.PENDING
        self.result = None
        self.error = None

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"Task(name={self.name!r}, status={self.status.name}, "
            f"attempt={self.attempt}, retries={self.retries})"
        )

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Task) and self.id == other.id
