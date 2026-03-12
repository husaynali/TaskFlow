"""
tests/test_retry.py – Unit tests for the RetryManager.
"""

import pytest

from taskflow.retry import RetryManager
from taskflow.task import Task, TaskStatus


def noop():
    pass


class TestRetryManagerShouldRetry:
    def test_no_retries_configured(self):
        mgr = RetryManager()
        t = Task(fn=noop, retries=0)
        t.attempt = 1
        # attempt 1 > retries 0 → no retry
        assert mgr.should_retry(t) is False

    def test_should_retry_within_limit(self):
        mgr = RetryManager()
        t = Task(fn=noop, retries=3)
        t.attempt = 2
        assert mgr.should_retry(t) is True

    def test_should_not_retry_at_limit(self):
        mgr = RetryManager()
        t = Task(fn=noop, retries=3)
        t.attempt = 4  # attempt > retries
        assert mgr.should_retry(t) is False


class TestRetryManagerBackoff:
    def test_first_attempt_no_delay(self):
        mgr = RetryManager()
        t = Task(fn=noop, retry_delay=1.0)
        t.attempt = 1
        assert mgr._backoff(t) == 0.0

    def test_second_attempt_base_delay(self):
        mgr = RetryManager()
        t = Task(fn=noop, retry_delay=2.0)
        t.attempt = 2
        # 2.0 * (2 ** 0) = 2.0
        assert mgr._backoff(t) == pytest.approx(2.0)

    def test_third_attempt_doubled(self):
        mgr = RetryManager()
        t = Task(fn=noop, retry_delay=1.0)
        t.attempt = 3
        # 1.0 * (2 ** 1) = 2.0
        assert mgr._backoff(t) == pytest.approx(2.0)

    def test_backoff_capped_at_max_delay(self):
        mgr = RetryManager(max_delay=5.0)
        t = Task(fn=noop, retry_delay=10.0)
        t.attempt = 5
        assert mgr._backoff(t) <= 5.0


class TestHandleFailure:
    def test_returns_true_when_retry_available(self):
        mgr = RetryManager()
        t = Task(fn=noop, retries=2)
        t.attempt = 1  # executor already set this
        result = mgr.handle_failure(t, ValueError("oops"))
        assert result is True
        # task should be reset to PENDING for re-queuing
        assert t.status == TaskStatus.PENDING

    def test_returns_false_when_exhausted(self):
        mgr = RetryManager()
        t = Task(fn=noop, retries=1)
        t.attempt = 2  # exhausted: attempt > retries
        result = mgr.handle_failure(t, ValueError("oops"))
        assert result is False
        assert t.status == TaskStatus.FAILED

    def test_error_is_recorded(self):
        mgr = RetryManager()
        t = Task(fn=noop, retries=0)
        t.attempt = 1
        err = RuntimeError("boom")
        mgr.handle_failure(t, err)
        assert t.error is err

    def test_attempt_incremented(self):
        # handle_failure does NOT increment attempt (executor does)
        mgr = RetryManager()
        t = Task(fn=noop, retries=5)
        t.attempt = 1
        mgr.handle_failure(t, Exception("x"))
        assert t.attempt == 1  # unchanged by handle_failure
