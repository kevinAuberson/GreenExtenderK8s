"""
File:        dag_etl_batch.py
Author:      Kevin Auberson
Created:     2026-06-28
Description: Production-like ETL pipeline DAG — carbon class: batch.
             Simulates an extract → transform → load pipeline processing
             synthetic sales records. Suitable for carbon-aware delay:
             no strict SLA, only needs to complete before the next run.

             Carbon class: batch (can be delayed when CI is high)
             Schedule: every 30 minutes
"""

import hashlib
import os
import random
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s

BENCHMARK_PHASE = os.getenv("BENCHMARK_PHASE", "baseline")
STEP_DURATION = int(os.getenv("BENCHMARK_TASK_DURATION", "120"))  # 2 min per step


def _cpu_work(seconds: int) -> int:
    """Run CPU-bound SHA-256 hashing for `seconds` seconds. Returns iteration count."""
    end = time.monotonic() + seconds
    data = b"etl-benchmark-payload"
    iterations = 0
    while time.monotonic() < end:
        for _ in range(5_000):
            data = hashlib.sha256(data).digest()
            iterations += 1
    return iterations


def extract_data(**context):
    """Simulate extracting 50 000 sales records from a source database."""
    print("Extracting records from source database...")
    records = [
        {
            "id": i,
            "product": random.choice(["A", "B", "C", "D"]),
            "region": random.choice(["EU", "US", "APAC"]),
            "amount": round(random.uniform(10.0, 500.0), 2),
        }
        for i in range(50_000)
    ]
    n = _cpu_work(STEP_DURATION)
    print(f"Extracted {len(records)} records — {n} hash iterations")
    context["ti"].xcom_push(key="record_count", value=len(records))


def transform_data(**context):
    """Aggregate records by product and region, compute totals and averages."""
    record_count = context["ti"].xcom_pull(key="record_count", task_ids="extract_data") or 50_000
    print(f"Transforming {record_count} records...")

    # Simulate aggregation CPU cost
    aggregates: dict[tuple, dict] = {}
    rng = random.Random(42)
    for _ in range(record_count):
        key = (rng.choice(["A", "B", "C", "D"]), rng.choice(["EU", "US", "APAC"]))
        amount = rng.uniform(10.0, 500.0)
        if key not in aggregates:
            aggregates[key] = {"total": 0.0, "count": 0}
        aggregates[key]["total"] += amount
        aggregates[key]["count"] += 1

    n = _cpu_work(STEP_DURATION)
    print(f"Produced {len(aggregates)} aggregated rows — {n} hash iterations")
    context["ti"].xcom_push(key="agg_count", value=len(aggregates))


def load_data(**context):
    """Write aggregated results to the target data warehouse (simulated)."""
    agg_count = context["ti"].xcom_pull(key="agg_count", task_ids="transform_data") or 12
    print(f"Loading {agg_count} aggregated rows to data warehouse...")
    n = _cpu_work(STEP_DURATION)
    print(f"Load complete — {n} hash iterations, {agg_count} rows written")


default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

_pod_override = None
if BENCHMARK_PHASE == "carbon":
    _pod_override = k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(labels={"carbon-class": "batch"}),
        spec=k8s.V1PodSpec(
            scheduler_name="carbon-aware",
            scheduling_gates=[k8s.V1PodSchedulingGate(name="carbon-aware-gate")],
            containers=[k8s.V1Container(name="base")],
        ),
    )

with DAG(
    dag_id="etl_sales_pipeline",
    default_args=default_args,
    description="ETL: extract/transform/load sales records (batch, carbon-aware)",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2026, 6, 28),
    catchup=False,
    max_active_runs=2,
    tags=["benchmark", "batch", "etl"],
) as dag:
    executor_cfg = {"pod_override": _pod_override} if _pod_override else {}

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        executor_config=executor_cfg,
    )
    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        executor_config=executor_cfg,
    )
    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        executor_config=executor_cfg,
    )

    extract >> transform >> load
