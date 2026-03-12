"""
examples/etl_pipeline.py – Classic ETL pipeline using TaskFlow.

Run:
    cd taskflow
    python examples/etl_pipeline.py
"""

import logging
import time

from taskflow import TaskFlow

logging.basicConfig(level=logging.INFO, format="%(message)s")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

def extract_users():
    print("[extract_users] Fetching users from source DB...")
    time.sleep(0.1)
    return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]


def extract_orders():
    print("[extract_orders] Fetching orders from source DB...")
    time.sleep(0.15)
    return [{"order_id": 100, "user_id": 1}]


def transform_users():
    print("[transform_users] Normalising user records...")
    time.sleep(0.1)


def transform_orders():
    print("[transform_orders] Enriching order records...")
    time.sleep(0.1)


def validate():
    print("[validate] Running data quality checks...")
    time.sleep(0.05)


def load_warehouse():
    print("[load_warehouse] Writing to data warehouse ✓")
    time.sleep(0.1)


def notify():
    print("[notify] Sending pipeline-complete notification ✓")


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    flow = TaskFlow(workers=4, mode="thread")

    # Extraction runs in parallel
    flow.add(extract_users)
    flow.add(extract_orders)

    # Transforms depend on their respective extracts
    flow.add(transform_users, depends=[extract_users])
    flow.add(transform_orders, depends=[extract_orders])

    # Validate requires both transforms
    flow.add(validate, depends=[transform_users, transform_orders])

    # Load after validation
    flow.add(load_warehouse, depends=[validate])

    # Notify after load
    flow.add(notify, depends=[load_warehouse])

    start = time.monotonic()
    flow.run()
    elapsed = time.monotonic() - start

    print(f"\n{flow.summary()}")
    print(f"\nTotal wall-clock time: {elapsed:.2f}s")
