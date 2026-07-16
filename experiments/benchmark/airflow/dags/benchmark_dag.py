"""
File:        benchmark_dag.py
Author:      Kevin Auberson
Created:     2026-06-24
Description: Airflow DAG for the carbon-aware scheduler benchmark.
             Submits a CPU-bound task every 15 minutes via KubernetesExecutor.
             The task computes SHA-256 hashes in a loop for ~5 minutes,
             ensuring consistent and reproducible energy consumption.

             The diversity of carbon classes comes from the Airflow stack itself:
             - Deployment (webserver, scheduler)   → latency-sensitive
             - StatefulSet (PostgreSQL)            → latency-sensitive
             - Job (DB migrations)                 → batch
             - Worker pods (this DAG)              → batch
             - CronJob (cleanup)                   → best-effort
"""

import hashlib
import os
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from kubernetes.client import models as k8s

TASK_DURATION_SECONDS = int(os.getenv("BENCHMARK_TASK_DURATION", "300"))
BENCHMARK_PHASE = os.getenv("BENCHMARK_PHASE", "baseline")


def cpu_burn():
    """CPU-bound workload: compute SHA-256 hashes for a fixed duration."""
    end = time.monotonic() + TASK_DURATION_SECONDS
    data = b"carbon-aware-benchmark-payload"
    iterations = 0
    while time.monotonic() < end:
        for _ in range(10_000):
            data = hashlib.sha256(data).digest()
            iterations += 1
    print(f"Completed {iterations} SHA-256 iterations in {TASK_DURATION_SECONDS}s")


default_args = {
    "owner": "benchmark",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
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
    dag_id="carbon_benchmark",
    default_args=default_args,
    description="Carbon-aware scheduler benchmark — CPU task every 15 min",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 6, 25),
    catchup=False,
    max_active_runs=4,
    tags=["benchmark", "carbon"],
) as dag:
    executor_cfg = {"pod_override": _pod_override} if _pod_override else {}
    cpu_task = PythonOperator(
        task_id="cpu_burn",
        python_callable=cpu_burn,
        executor_config=executor_cfg,
    )
