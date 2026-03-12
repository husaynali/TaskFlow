"""
tests/test_dag.py – Unit tests for the DAG builder.
"""

import pytest

from taskflow.dag import CycleError, DAG
from taskflow.task import Task


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_task(name: str, deps=None):
    return Task(fn=lambda: name, dependencies=deps or [], name=name)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDAGBasics:
    def test_add_single_task(self):
        dag = DAG()
        t = make_task("a")
        dag.add_task(t)
        assert len(dag) == 1
        assert t in dag.tasks

    def test_add_dependency_auto_registers(self):
        dag = DAG()
        t1 = make_task("t1")
        t2 = make_task("t2", deps=[t1])
        dag.add_task(t2)
        assert len(dag) == 2

    def test_duplicate_add_is_idempotent(self):
        dag = DAG()
        t = make_task("a")
        dag.add_task(t)
        dag.add_task(t)
        assert len(dag) == 1

    def test_root_tasks_no_deps(self):
        dag = DAG()
        t1 = make_task("t1")
        t2 = make_task("t2")
        dag.add_task(t1)
        dag.add_task(t2)
        roots = dag.root_tasks()
        assert t1 in roots and t2 in roots

    def test_root_tasks_with_deps(self):
        dag = DAG()
        t1 = make_task("t1")
        t2 = make_task("t2", deps=[t1])
        dag.add_task(t2)
        roots = dag.root_tasks()
        assert t1 in roots
        assert t2 not in roots


class TestDAGValidation:
    def test_valid_dag_passes(self):
        dag = DAG()
        t1 = make_task("t1")
        t2 = make_task("t2", deps=[t1])
        dag.add_task(t2)
        dag.validate()  # no exception

    def test_cycle_two_nodes(self):
        dag = DAG()
        t1 = make_task("t1")
        t2 = make_task("t2")
        # Manually wire a cycle
        t1.dependencies = [t2]
        t2.dependencies = [t1]
        dag._tasks[t1.id] = t1
        dag._tasks[t2.id] = t2
        with pytest.raises(CycleError):
            dag.validate()

    def test_cycle_three_nodes(self):
        dag = DAG()
        t1 = make_task("t1")
        t2 = make_task("t2")
        t3 = make_task("t3")
        t1.dependencies = [t3]
        t2.dependencies = [t1]
        t3.dependencies = [t2]
        for t in (t1, t2, t3):
            dag._tasks[t.id] = t
        with pytest.raises(CycleError):
            dag.validate()


class TestTopologicalOrder:
    def test_linear_chain(self):
        dag = DAG()
        t1 = make_task("t1")
        t2 = make_task("t2", deps=[t1])
        t3 = make_task("t3", deps=[t2])
        dag.add_task(t3)
        order = dag.topological_order()
        assert order.index(t1) < order.index(t2) < order.index(t3)

    def test_diamond(self):
        dag = DAG()
        t1 = make_task("t1")
        t2 = make_task("t2", deps=[t1])
        t3 = make_task("t3", deps=[t1])
        t4 = make_task("t4", deps=[t2, t3])
        dag.add_task(t4)
        order = dag.topological_order()
        assert order.index(t1) < order.index(t2)
        assert order.index(t1) < order.index(t3)
        assert order.index(t2) < order.index(t4)
        assert order.index(t3) < order.index(t4)

    def test_independent_tasks(self):
        dag = DAG()
        t1 = make_task("t1")
        t2 = make_task("t2")
        dag.add_task(t1)
        dag.add_task(t2)
        order = dag.topological_order()
        assert len(order) == 2


class TestGetReadyTasks:
    def test_ready_when_no_deps(self):
        from taskflow.task import TaskStatus
        dag = DAG()
        t1 = make_task("t1")
        dag.add_task(t1)
        ready = dag.get_ready_tasks(completed=set())
        assert t1 in ready

    def test_not_ready_when_dep_incomplete(self):
        dag = DAG()
        t1 = make_task("t1")
        t2 = make_task("t2", deps=[t1])
        dag.add_task(t2)
        ready = dag.get_ready_tasks(completed=set())
        assert t2 not in ready

    def test_ready_after_dep_completed(self):
        dag = DAG()
        t1 = make_task("t1")
        t2 = make_task("t2", deps=[t1])
        dag.add_task(t2)
        ready = dag.get_ready_tasks(completed={t1.id})
        assert t2 in ready
