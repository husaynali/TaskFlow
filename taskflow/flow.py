"""
flow.py – Primary public interface for TaskFlow pipelines.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Dict, List, Optional, Union

from .dag import DAG, CycleError
from .executor import Executor
from .queue import TaskQueue
from .retry import RetryManager
from .scheduler import Scheduler
from .task import Task, TaskStatus

logger = logging.getLogger("taskflow.flow")


class PipelineError(Exception):
    """Raised when one or more tasks in the pipeline fail permanently."""


class TaskFlow:
    """
    The main entry point for building and running task pipelines.

    Usage::

        from taskflow import TaskFlow

        def extract(): ...
        def transform(): ...
        def load(): ...

        flow = TaskFlow(workers=4, mode="thread")
        flow.add(extract)
        flow.add(transform, depends=[extract])
        flow.add(load, depends=[transform])
        flow.run()

    Parameters:
        workers:      Number of parallel workers (default 4).
        mode:         ``"thread"`` (default) or ``"process"``.
        max_delay:    Maximum backoff delay in seconds for retries (default 60).
        raise_on_failure: If True (default) raise :class:`PipelineError`
                          when any task fails permanently.
        log_level:    Logging level for taskflow loggers (default WARNING).
    """

    def __init__(
        self,
        workers: int = 4,
        mode: str = "thread",
        max_delay: float = 60.0,
        raise_on_failure: bool = True,
        log_level: int = logging.WARNING,
    ) -> None:
        self._workers = workers
        self._mode = mode
        self._max_delay = max_delay
        self._raise_on_failure = raise_on_failure

        self._fn_to_task: Dict[Callable, Task] = {}
        self._dag = DAG()

        self._configure_logging(log_level)

    # ------------------------------------------------------------------
    # Pipeline construction
    # ------------------------------------------------------------------

    def add(
        self,
        fn: Callable,
        depends: Optional[List[Callable]] = None,
        retries: int = 0,
        retry_delay: float = 1.0,
        name: Optional[str] = None,
    ) -> "TaskFlow":
        """
        Register a callable as a task in the pipeline.

        Parameters:
            fn:          The function to execute.
            depends:     List of *functions* this task depends on.
                         They must already have been added via :meth:`add`.
            retries:     Retry attempts on failure (0 = no retry).
            retry_delay: Base backoff in seconds between retries.
            name:        Optional display name; defaults to ``fn.__name__``.

        Returns:
            self, so calls can be chained.

        Raises:
            ValueError: if a listed dependency has not been added yet.
        """
        dep_tasks: List[Task] = []
        for dep_fn in (depends or []):
            if dep_fn not in self._fn_to_task:
                raise ValueError(
                    f"Dependency '{getattr(dep_fn, '__name__', dep_fn)}' "
                    f"has not been added to the pipeline yet. "
                    f"Call flow.add() for dependencies before dependents."
                )
            dep_tasks.append(self._fn_to_task[dep_fn])

        task = Task(
            fn=fn,
            dependencies=dep_tasks,
            retries=retries,
            retry_delay=retry_delay,
            name=name,
        )
        self._fn_to_task[fn] = task
        self._dag.add_task(task)
        logger.debug("Registered task '%s'.", task.name)
        return self

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(self, timeout: Optional[float] = None) -> Dict[str, object]:
        """
        Execute the pipeline.

        Validates the DAG, then starts the scheduler and executor.
        Blocks until all tasks are done or *timeout* seconds elapse.

        Parameters:
            timeout: Optional wall-clock limit in seconds.  If exceeded a
                     :class:`TimeoutError` is raised and workers are stopped.

        Returns:
            A dict mapping task name → result for all **completed** tasks.

        Raises:
            CycleError:      if the task graph contains a cycle.
            PipelineError:   if any task fails permanently (and
                             ``raise_on_failure=True``).
            TimeoutError:    if *timeout* is set and elapsed.
        """
        self._dag.validate()

        queue = TaskQueue()
        retry_mgr = RetryManager(max_delay=self._max_delay)
        scheduler = Scheduler(dag=self._dag, task_queue=queue)
        executor = Executor(
            workers=self._workers,
            mode=self._mode,
            scheduler=scheduler,
            queue=queue,
            retry_mgr=retry_mgr,
        )

        logger.info(
            "Starting pipeline: %d tasks, %d workers (%s mode).",
            len(self._dag),
            self._workers,
            self._mode,
        )

        start_time = time.monotonic()
        executor.start()
        scheduler.start()

        # --- wait for completion ---
        self._wait(scheduler, executor, start_time, timeout)

        scheduler.stop()
        executor.stop()

        elapsed = time.monotonic() - start_time
        self._log_summary(elapsed)

        if self._raise_on_failure:
            failed = [t for t in self._dag.tasks if t.status == TaskStatus.FAILED]
            if failed:
                names = ", ".join(t.name for t in failed)
                raise PipelineError(
                    f"Pipeline finished with {len(failed)} failed task(s): {names}"
                )

        return {
            t.name: t.result
            for t in self._dag.tasks
            if t.status == TaskStatus.COMPLETED
        }

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def tasks(self) -> List[Task]:
        """All registered :class:`Task` objects."""
        return self._dag.tasks

    def summary(self) -> str:
        """Human-readable summary of task statuses."""
        lines = ["TaskFlow Pipeline Summary", "-" * 40]
        for task in self._dag.tasks:
            dep_names = ", ".join(d.name for d in task.dependencies) or "none"
            lines.append(
                f"  {task.name:<25} status={task.status.name:<10} "
                f"attempts={task.attempt}  deps=[{dep_names}]"
            )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _wait(
        self,
        scheduler: Scheduler,
        executor: Executor,
        start_time: float,
        timeout: Optional[float],
    ) -> None:
        poll_interval = 0.05  # seconds
        while True:
            if all(t.is_done for t in self._dag.tasks):
                break
            if timeout is not None and (time.monotonic() - start_time) >= timeout:
                raise TimeoutError(
                    f"Pipeline timed out after {timeout:.1f}s."
                )
            time.sleep(poll_interval)

    def _log_summary(self, elapsed: float) -> None:
        completed = sum(
            1 for t in self._dag.tasks if t.status == TaskStatus.COMPLETED
        )
        failed = sum(1 for t in self._dag.tasks if t.status == TaskStatus.FAILED)
        total = len(self._dag.tasks)
        logger.info(
            "Pipeline done in %.2fs – %d/%d completed, %d failed.",
            elapsed,
            completed,
            total,
            failed,
        )

    @staticmethod
    def _configure_logging(level: int) -> None:
        logging.getLogger("taskflow").setLevel(level)
        if not logging.getLogger("taskflow").handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter("[%(levelname)s] %(name)s – %(message)s")
            )
            logging.getLogger("taskflow").addHandler(handler)
