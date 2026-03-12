"""
examples/retry_example.py – Demonstrates retry behaviour with flaky tasks.

Run:
    cd taskflow
    python examples/retry_example.py
"""

import logging
import random
import time

from taskflow import TaskFlow

logging.basicConfig(level=logging.WARNING, format="[%(levelname)s] %(message)s")


# ---------------------------------------------------------------------------
# Simulated flaky tasks
# ---------------------------------------------------------------------------

_call_counts: dict = {}


def make_flaky(name: str, fail_times: int, delay: float = 0.05):
    """Return a function that fails the first *fail_times* calls."""
    _call_counts[name] = 0

    def fn():
        _call_counts[name] += 1
        attempt = _call_counts[name]
        print(f"  [{name}] attempt {attempt}…", end=" ")
        time.sleep(delay)
        if attempt <= fail_times:
            print("FAILED ✗")
            raise RuntimeError(f"{name} simulated failure (attempt {attempt})")
        print("OK ✓")
        return f"{name}_result"

    fn.__name__ = name
    return fn


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== TaskFlow Retry Demo ===\n")

    reliable = make_flaky("reliable_task", fail_times=0)
    flaky2 = make_flaky("flaky_2tries", fail_times=2)
    flaky1 = make_flaky("flaky_1try", fail_times=1)
    final = make_flaky("final_step", fail_times=0)

    flow = TaskFlow(workers=4)
    flow.add(reliable)
    flow.add(flaky2, depends=[reliable], retries=3, retry_delay=0.01)
    flow.add(flaky1, depends=[reliable], retries=2, retry_delay=0.01)
    flow.add(final, depends=[flaky2, flaky1])

    results = flow.run()

    print("\n--- Results ---")
    for name, value in results.items():
        print(f"  {name}: {value}")

    print(f"\n--- Attempt counts ---")
    for name, count in _call_counts.items():
        print(f"  {name}: {count} call(s)")
