"""
File:        dag_data_quality.py
Author:      Kevin Auberson
Created:     2026-06-28
Description: Production-like data quality monitoring DAG — carbon class: latency-sensitive.
             Simulates freshness checks, schema validation, and anomaly detection
             that must run on schedule to meet SLA. Delays are not acceptable:
             stale quality checks would miss data incidents.

             Carbon class: latency-sensitive (must not be delayed)
             Schedule: every 15 minutes
"""

import hashlib
import os
import random
import statistics
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s

BENCHMARK_PHASE = os.getenv("BENCHMARK_PHASE", "baseline")
STEP_DURATION = int(os.getenv("BENCHMARK_TASK_DURATION", "60"))  # 1 min per check


def _cpu_work(seconds: int) -> int:
    """Run CPU-bound SHA-256 hashing for `seconds` seconds."""
    end = time.monotonic() + seconds
    data = b"dq-benchmark-payload"
    iterations = 0
    while time.monotonic() < end:
        for _ in range(5_000):
            data = hashlib.sha256(data).digest()
            iterations += 1
    return iterations


def check_freshness(**context):
    """Verify that all source tables have received data within the last 15 minutes."""
    tables = ["orders", "inventory", "user_events", "payments"]
    now = datetime.utcnow()
    print(f"Checking freshness at {now.isoformat()} for {len(tables)} tables...")

    rng = random.Random(int(now.timestamp()))
    for table in tables:
        lag_seconds = rng.randint(30, 600)
        status = "OK" if lag_seconds < 600 else "STALE"
        print(f"  {table}: lag={lag_seconds}s [{status}]")

    n = _cpu_work(STEP_DURATION)
    print(f"Freshness check complete — {n} hash iterations")


def validate_schema(**context):
    """Validate column types, null rates, and value ranges on recent partitions."""
    checks = {
        "orders.amount": {"type": "float", "null_rate": 0.001, "range": (0.01, 9999.99)},
        "orders.status": {"type": "enum", "values": ["pending", "paid", "cancelled"]},
        "user_events.user_id": {"type": "int", "null_rate": 0.005},
        "payments.currency": {"type": "enum", "values": ["USD", "EUR", "CHF"]},
    }

    rng = random.Random(42)
    passed = 0
    for field, spec in checks.items():
        # Simulate measuring null rate on a 10k sample
        null_rate = rng.uniform(0.0, 0.005)
        expected = spec.get("null_rate", 0.01)
        ok = null_rate <= expected
        print(f"  {field}: null_rate={null_rate:.4f} (max={expected}) — {'PASS' if ok else 'FAIL'}")
        if ok:
            passed += 1

    n = _cpu_work(STEP_DURATION)
    print(f"Schema validation: {passed}/{len(checks)} checks passed — {n} hash iterations")


def detect_anomalies(**context):
    """Run statistical anomaly detection on the last hour of order values."""
    print("Generating synthetic hourly order sample for anomaly detection...")
    rng = random.Random(int(time.time()) // 3600)  # stable within the same hour
    values = [rng.gauss(150.0, 40.0) for _ in range(2_000)]

    mean = statistics.mean(values)
    stdev = statistics.stdev(values)
    threshold = mean + 4 * stdev
    anomalies = [v for v in values if v > threshold or v < mean - 4 * stdev]

    print(f"  Sample: n={len(values)}, mean={mean:.2f}, stdev={stdev:.2f}")
    print(f"  Anomaly threshold: >{threshold:.2f} or <{mean - 4 * stdev:.2f}")
    print(f"  Anomalies detected: {len(anomalies)}")

    n = _cpu_work(STEP_DURATION)
    print(f"Anomaly detection complete — {n} hash iterations")


default_args = {
    "owner": "data-quality",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
}

_pod_override = None
if BENCHMARK_PHASE == "carbon":
    _pod_override = k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(labels={"carbon-class": "latency-sensitive"}),
        spec=k8s.V1PodSpec(
            scheduler_name="carbon-aware",
            containers=[k8s.V1Container(name="base")],
        ),
    )

with DAG(
    dag_id="data_quality_monitor",
    default_args=default_args,
    description="Data quality checks: freshness, schema, anomalies (latency-sensitive)",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 6, 28),
    catchup=False,
    max_active_runs=1,
    tags=["benchmark", "latency-sensitive", "quality"],
) as dag:
    executor_cfg = {"pod_override": _pod_override} if _pod_override else {}

    freshness = PythonOperator(
        task_id="check_freshness",
        python_callable=check_freshness,
        executor_config=executor_cfg,
    )
    schema = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema,
        executor_config=executor_cfg,
    )
    anomalies = PythonOperator(
        task_id="detect_anomalies",
        python_callable=detect_anomalies,
        executor_config=executor_cfg,
    )

    freshness >> schema >> anomalies
