"""
tests/test_task.py – Unit tests for the Task model.
"""

import pytest

from taskflow.task import Task, TaskStatus


def noop():
    pass


class TestTaskDefaults:
    def test_default_status_is_pending(self):
        t = Task(fn=noop)
        assert t.status == TaskStatus.PENDING

    def test_name_defaults_to_fn_name(self):
        t = Task(fn=noop)
        assert t.name == "noop"

    def test_custom_name(self):
        t = Task(fn=noop, name="my_task")
        assert t.name == "my_task"

    def test_id_is_string(self):
        t = Task(fn=noop)
        assert isinstance(t.id, str)

    def test_unique_ids(self):
        t1 = Task(fn=noop)
        t2 = Task(fn=noop)
        assert t1.id != t2.id

    def test_no_dependencies_by_default(self):
        t = Task(fn=noop)
        assert t.dependencies == []

    def test_retries_default_zero(self):
        t = Task(fn=noop)
        assert t.retries == 0

    def test_attempt_starts_at_zero(self):
        t = Task(fn=noop)
        assert t.attempt == 0


class TestTaskIsReady:
    def test_ready_no_deps(self):
        t = Task(fn=noop)
        assert t.is_ready is True

    def test_not_ready_pending_dep(self):
        dep = Task(fn=noop, name="dep")
        t = Task(fn=noop, dependencies=[dep])
        assert t.is_ready is False

    def test_ready_completed_dep(self):
        dep = Task(fn=noop, name="dep")
        dep.status = TaskStatus.COMPLETED
        t = Task(fn=noop, dependencies=[dep])
        assert t.is_ready is True

    def test_not_ready_if_any_dep_incomplete(self):
        dep1 = Task(fn=noop, name="dep1")
        dep2 = Task(fn=noop, name="dep2")
        dep1.status = TaskStatus.COMPLETED
        dep2.status = TaskStatus.PENDING
        t = Task(fn=noop, dependencies=[dep1, dep2])
        assert t.is_ready is False


class TestTaskIsDone:
    @pytest.mark.parametrize("status,expected", [
        (TaskStatus.PENDING, False),
        (TaskStatus.RUNNING, False),
        (TaskStatus.COMPLETED, True),
        (TaskStatus.FAILED, True),
        (TaskStatus.SKIPPED, True),
    ])
    def test_is_done(self, status, expected):
        t = Task(fn=noop)
        t.status = status
        assert t.is_done is expected


class TestTaskCanRetry:
    def test_can_retry_before_attempts_exhausted(self):
        t = Task(fn=noop, retries=3)
        t.attempt = 2
        assert t.can_retry is True

    def test_cannot_retry_when_exhausted(self):
        t = Task(fn=noop, retries=2)
        t.attempt = 3
        assert t.can_retry is False


class TestTaskReset:
    def test_reset_clears_status_and_result(self):
        t = Task(fn=noop)
        t.status = TaskStatus.FAILED
        t.result = "old"
        t.error = ValueError("boom")
        t.reset()
        assert t.status == TaskStatus.PENDING
        assert t.result is None
        assert t.error is None

    def test_reset_preserves_attempt(self):
        t = Task(fn=noop)
        t.attempt = 5
        t.reset()
        assert t.attempt == 5  # attempt is NOT cleared on reset


class TestTaskEquality:
    def test_same_task_equal(self):
        t = Task(fn=noop)
        assert t == t

    def test_different_tasks_not_equal(self):
        t1 = Task(fn=noop)
        t2 = Task(fn=noop)
        assert t1 != t2

    def test_hashable(self):
        t = Task(fn=noop)
        s = {t}
        assert t in s
