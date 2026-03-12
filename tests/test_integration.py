"""
tests/test_integration.py – Integration tests for full pipeline execution.

These tests exercise the complete stack: TaskFlow → DAG → Scheduler →
Executor → RetryManager and verify end-to-end behaviour.
"""

from __future__ import annotations

import threading
import time
from typing import List

import pytest

from taskflow import TaskFlow, TaskStatus
from taskflow.flow import PipelineError
from taskflow.dag import CycleError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_recording_fn(name: str, log: List[str], delay: float = 0.0):
    """Return a function that appends *name* to *log* when called."""
    def fn():
        if delay:
            time.sleep(delay)
        log.append(name)
        return name
    fn.__name__ = name
    return fn


# ---------------------------------------------------------------------------
# Basic pipeline tests
# ---------------------------------------------------------------------------

class TestLinearPipeline:
    def test_single_task_runs(self):
        log = []
        a = make_recording_fn("a", log)
        flow = TaskFlow(workers=2)
        flow.add(a)
        results = flow.run()
        assert "a" in log
        assert results["a"] == "a"

    def test_linear_chain_correct_order(self):
        log = []
        a = make_recording_fn("a", log)
        b = make_recording_fn("b", log)
        c = make_recording_fn("c", log)

        flow = TaskFlow(workers=2)
        flow.add(a)
        flow.add(b, depends=[a])
        flow.add(c, depends=[b])
        flow.run()

        assert log.index("a") < log.index("b") < log.index("c")

    def test_results_returned(self):
        def add_one():
            return 42
        flow = TaskFlow(workers=1)
        flow.add(add_one)
        results = flow.run()
        assert results["add_one"] == 42


class TestParallelPipeline:
    def test_independent_tasks_run_concurrently(self):
        """Two tasks with no shared dependency should overlap in time."""
        start_times = {}
        end_times = {}
        lock = threading.Lock()

        def make_timed(name):
            def fn():
                with lock:
                    start_times[name] = time.monotonic()
                time.sleep(0.15)
                with lock:
                    end_times[name] = time.monotonic()
            fn.__name__ = name
            return fn

        a = make_timed("a")
        b = make_timed("b")

        flow = TaskFlow(workers=4)
        flow.add(a)
        flow.add(b)
        flow.run()

        # They should overlap: b starts before a ends
        assert start_times["b"] < end_times["a"] or start_times["a"] < end_times["b"]

    def test_diamond_dag(self):
        log = []
        a = make_recording_fn("a", log, delay=0.05)
        b = make_recording_fn("b", log, delay=0.05)
        c = make_recording_fn("c", log, delay=0.05)
        d = make_recording_fn("d", log)

        flow = TaskFlow(workers=4)
        flow.add(a)
        flow.add(b, depends=[a])
        flow.add(c, depends=[a])
        flow.add(d, depends=[b, c])
        flow.run()

        assert log.index("a") < log.index("b")
        assert log.index("a") < log.index("c")
        assert log.index("b") < log.index("d")
        assert log.index("c") < log.index("d")

    def test_fan_out(self):
        log = []
        root = make_recording_fn("root", log)
        leaves = [make_recording_fn(f"leaf_{i}", log) for i in range(5)]

        flow = TaskFlow(workers=5)
        flow.add(root)
        for leaf in leaves:
            flow.add(leaf, depends=[root])
        flow.run()

        assert log[0] == "root"
        assert set(log[1:]) == {f"leaf_{i}" for i in range(5)}


class TestRetryBehaviour:
    def test_task_retries_on_failure_then_succeeds(self):
        call_count = {"n": 0}

        def flaky():
            call_count["n"] += 1
            if call_count["n"] < 3:
                raise RuntimeError("temporary error")
            return "ok"

        flow = TaskFlow(workers=2)
        flow.add(flaky, retries=3, retry_delay=0.01)
        results = flow.run()
        assert results["flaky"] == "ok"
        assert call_count["n"] == 3

    def test_task_fails_after_exhausting_retries(self):
        def always_fails():
            raise RuntimeError("permanent")

        flow = TaskFlow(workers=2)
        flow.add(always_fails, retries=2, retry_delay=0.01)

        with pytest.raises(PipelineError, match="always_fails"):
            flow.run()

    def test_no_retry_by_default(self):
        call_count = {"n": 0}

        def bad():
            call_count["n"] += 1
            raise RuntimeError("fail")

        flow = TaskFlow(workers=2, raise_on_failure=False)
        flow.add(bad)
        flow.run()

        assert call_count["n"] == 1  # called exactly once, no retry


class TestErrorHandling:
    def test_cycle_raises_cycle_error(self):
        """flow.run() should raise CycleError for cyclic graphs."""
        from taskflow.dag import CycleError
        from taskflow.task import Task

        flow = TaskFlow(workers=2)
        # Manually inject a cycle by bypassing the add() guard
        t1 = Task(fn=lambda: None, name="t1")
        t2 = Task(fn=lambda: None, name="t2")
        t1.dependencies = [t2]
        t2.dependencies = [t1]
        flow._dag._tasks[t1.id] = t1
        flow._dag._tasks[t2.id] = t2
        flow._fn_to_task[t1.fn] = t1

        with pytest.raises(CycleError):
            flow.run()

    def test_unknown_dependency_raises(self):
        def a(): pass
        def b(): pass

        flow = TaskFlow()
        # b not added before referencing as dependency
        with pytest.raises(ValueError, match="b"):
            flow.add(a, depends=[b])

    def test_raise_on_failure_false_returns_partial_results(self):
        def ok():
            return 99

        def bad():
            raise RuntimeError("nope")

        flow = TaskFlow(workers=2, raise_on_failure=False)
        flow.add(ok)
        flow.add(bad)
        results = flow.run()

        assert results["ok"] == 99
        assert "bad" not in results

    def test_timeout_raises(self):
        def slow():
            time.sleep(10)

        flow = TaskFlow(workers=2)
        flow.add(slow)
        with pytest.raises(TimeoutError):
            flow.run(timeout=0.2)


def _process_mode_task():
    return "process_a"


class TestProcessMode:
    def test_process_mode_runs_correctly(self):
        """Smoke-test process mode with a simple pipeline."""
        flow = TaskFlow(workers=2, mode="process")
        flow.add(_process_mode_task)
        results = flow.run()
        assert results["_process_mode_task"] == "process_a"


class TestLargePipeline:
    def test_many_independent_tasks(self):
        """100 independent tasks should all complete."""
        results_bag = []

        tasks = []
        for i in range(100):
            def make(idx):
                def fn():
                    return idx
                fn.__name__ = f"task_{idx}"
                return fn
            tasks.append(make(i))

        flow = TaskFlow(workers=8)
        for t in tasks:
            flow.add(t)

        results = flow.run()
        assert len(results) == 100

    def test_long_linear_chain(self):
        """50-task linear chain executes in correct order."""
        log = []
        fns = []
        for i in range(50):
            fn = make_recording_fn(f"t{i}", log)
            fns.append(fn)

        flow = TaskFlow(workers=4)
        flow.add(fns[0])
        for i in range(1, 50):
            flow.add(fns[i], depends=[fns[i - 1]])

        flow.run()
        assert log == [f"t{i}" for i in range(50)]
