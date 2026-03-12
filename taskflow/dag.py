"""
dag.py – Directed Acyclic Graph builder and validator for TaskFlow.
"""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Dict, List, Set

from .task import Task


class CycleError(Exception):
    """Raised when a dependency cycle is detected in the task graph."""


class DAG:
    """
    Builds and validates the dependency graph for a set of tasks.

    Internally represents the graph as an adjacency list (task → dependents)
    and also keeps a reverse map (task → dependencies) for scheduling.
    """

    def __init__(self) -> None:
        # task.id → Task
        self._tasks: Dict[str, Task] = {}

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def add_task(self, task: Task) -> None:
        """Register a task (and implicitly its dependencies)."""
        if task.id in self._tasks:
            return
        self._tasks[task.id] = task
        # Ensure all dependencies are also registered
        for dep in task.dependencies:
            self.add_task(dep)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self) -> None:
        """
        Validate the DAG.

        Raises:
            CycleError: if a cycle is detected.
            ValueError:  if a dependency is not part of the graph.
        """
        self._check_missing_deps()
        self._check_cycles()

    def _check_missing_deps(self) -> None:
        for task in self._tasks.values():
            for dep in task.dependencies:
                if dep.id not in self._tasks:
                    raise ValueError(
                        f"Task '{task.name}' depends on '{dep.name}' "
                        f"which is not registered in the pipeline."
                    )

    def _check_cycles(self) -> None:
        """Detect cycles via DFS with coloring (white/grey/black)."""
        WHITE, GREY, BLACK = 0, 1, 2
        color: Dict[str, int] = {tid: WHITE for tid in self._tasks}
        path: List[str] = []

        def dfs(tid: str) -> None:
            color[tid] = GREY
            path.append(self._tasks[tid].name)
            for dep in self._tasks[tid].dependencies:
                if color[dep.id] == GREY:
                    cycle = " → ".join(path + [dep.name])
                    raise CycleError(f"Cycle detected: {cycle}")
                if color[dep.id] == WHITE:
                    dfs(dep.id)
            path.pop()
            color[tid] = BLACK

        for tid in list(self._tasks):
            if color[tid] == WHITE:
                dfs(tid)

    # ------------------------------------------------------------------
    # Topology
    # ------------------------------------------------------------------

    def topological_order(self) -> List[Task]:
        """
        Return tasks in a valid execution order (Kahn's algorithm).

        Tasks with no dependencies come first; the relative ordering among
        siblings is deterministic (sorted by name) so pipelines are
        reproducible.
        """
        in_degree: Dict[str, int] = {tid: 0 for tid in self._tasks}
        dependents: Dict[str, List[str]] = defaultdict(list)

        for task in self._tasks.values():
            for dep in task.dependencies:
                dependents[dep.id].append(task.id)
                in_degree[task.id] += 1

        # Start with tasks that have no dependencies, sorted for determinism
        queue: deque[str] = deque(
            sorted(
                [tid for tid, deg in in_degree.items() if deg == 0],
                key=lambda tid: self._tasks[tid].name,
            )
        )
        order: List[Task] = []

        while queue:
            tid = queue.popleft()
            order.append(self._tasks[tid])
            for dep_tid in sorted(dependents[tid], key=lambda t: self._tasks[t].name):
                in_degree[dep_tid] -= 1
                if in_degree[dep_tid] == 0:
                    queue.append(dep_tid)

        if len(order) != len(self._tasks):
            raise CycleError("Graph has a cycle – topological sort failed.")

        return order

    def root_tasks(self) -> List[Task]:
        """Tasks with no dependencies."""
        return [t for t in self._tasks.values() if not t.dependencies]

    def get_ready_tasks(self, completed: Set[str]) -> List[Task]:
        """
        Return tasks whose every dependency id is in *completed* and which
        are still PENDING (not yet running or done).
        """
        from .task import TaskStatus

        ready = []
        for task in self._tasks.values():
            if task.status != TaskStatus.PENDING:
                continue
            deps_done = all(dep.id in completed for dep in task.dependencies)
            if deps_done:
                ready.append(task)
        return ready

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def tasks(self) -> List[Task]:
        return list(self._tasks.values())

    def __len__(self) -> int:
        return len(self._tasks)

    def __repr__(self) -> str:
        return f"DAG(tasks={len(self._tasks)})"
