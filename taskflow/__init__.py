"""
TaskFlow – Parallel Pipeline Execution Framework
================================================

Minimal public API::

    from taskflow import TaskFlow

    flow = TaskFlow(workers=4)
    flow.add(task_a)
    flow.add(task_b, depends=[task_a])
    flow.run()
"""

from .dag import CycleError, DAG
from .flow import PipelineError, TaskFlow
from .task import Task, TaskStatus

__all__ = [
    "TaskFlow",
    "Task",
    "TaskStatus",
    "DAG",
    "CycleError",
    "PipelineError",
]

__version__ = "0.1.0"
