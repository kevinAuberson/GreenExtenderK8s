"""
File:        dag_log_cleanup.py
Author:      Kevin Auberson
Created:     2026-06-28
Description: Production-like log archival and cleanup DAG — carbon class: best-effort.
             Simulates scanning application logs, compressing old files, and purging
             expired data. No SLA: runs opportunistically when grid carbon is lowest.

             Carbon class: best-effort (delay as much as needed for green windows)
             Schedule: every 2 hours
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
STEP_DURATION = int(os.getenv("BENCHMARK_TASK_DURATION", "90"))  # 90 s per step


def _cpu_work(seconds: int) -> int:
    """Run CPU-bound SHA-256 hashing for `seconds` seconds."""
    end = time.monotonic() + seconds
    data = b"cleanup-benchmark-payload"
    iterations = 0
    while time.monotonic() < end:
        for _ in range(5_000):
            data = hashlib.sha256(data).digest()
            iterations += 1
    return iterations


def scan_logs(**context):
    """Scan log directories and identify files older than 7 days for archival."""
    services = ["api-gateway", "auth-service", "payment-service", "notification-service"]
    rng = random.Random(int(datetime.utcnow().timestamp()) // 86400)

    total_files = 0
    total_size_mb = 0.0
    for service in services:
        file_count = rng.randint(20, 80)
        size_mb = rng.uniform(50.0, 500.0)
        total_files += file_count
        total_size_mb += size_mb
        print(f"  {service}: {file_count} files, {size_mb:.1f} MB to archive")

    print(f"Scan complete: {total_files} files totalling {total_size_mb:.1f} MB")
    n = _cpu_work(STEP_DURATION)
    print(f"Scan CPU work done — {n} hash iterations")
    context["ti"].xcom_push(key="total_size_mb", value=total_size_mb)
    context["ti"].xcom_push(key="total_files", value=total_files)


def compress_logs(**context):
    """Compress identified log files with gzip (simulated via CPU-bound hashing)."""
    total_files = context["ti"].xcom_pull(key="total_files", task_ids="scan_logs") or 120
    total_size_mb = context["ti"].xcom_pull(key="total_size_mb", task_ids="scan_logs") or 800.0

    print(f"Compressing {total_files} log files ({total_size_mb:.1f} MB)...")
    n = _cpu_work(STEP_DURATION)

    compressed_mb = total_size_mb * 0.15  # typical 85% compression on text logs
    saved_mb = total_size_mb - compressed_mb
    print(
        f"Compression complete: {total_size_mb:.1f} MB → {compressed_mb:.1f} MB "
        f"(saved {saved_mb:.1f} MB, {saved_mb / total_size_mb * 100:.0f}%) "
        f"— {n} hash iterations"
    )
    context["ti"].xcom_push(key="compressed_mb", value=compressed_mb)


def purge_expired(**context):
    """Delete log archives older than 90 days and update retention index."""
    compressed_mb = context["ti"].xcom_pull(key="compressed_mb", task_ids="compress_logs") or 120.0
    rng = random.Random(42)
    purge_count = rng.randint(5, 30)
    purge_mb = rng.uniform(10.0, 200.0)

    print(f"Purging {purge_count} expired archives ({purge_mb:.1f} MB > 90 days old)...")
    n = _cpu_work(STEP_DURATION)
    total_freed = compressed_mb + purge_mb
    print(
        f"Purge complete: {purge_count} archives deleted, "
        f"{total_freed:.1f} MB freed total — {n} hash iterations"
    )


default_args = {
    "owner": "platform-ops",
    "retries": 0,
    "execution_timeout": timedelta(minutes=20),
}

_pod_override = None
if BENCHMARK_PHASE == "carbon":
    _pod_override = k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(labels={"carbon-class": "best-effort"}),
        spec=k8s.V1PodSpec(
            scheduler_name="carbon-aware",
            scheduling_gates=[k8s.V1PodSchedulingGate(name="carbon-aware-gate")],
            containers=[k8s.V1Container(name="base")],
        ),
    )

with DAG(
    dag_id="log_archive_cleanup",
    default_args=default_args,
    description="Log archival and purge pipeline (best-effort, maximum carbon delay)",
    schedule_interval="0 */2 * * *",
    start_date=datetime(2026, 6, 28),
    catchup=False,
    max_active_runs=1,
    tags=["benchmark", "best-effort", "cleanup"],
) as dag:
    executor_cfg = {"pod_override": _pod_override} if _pod_override else {}

    scan = PythonOperator(
        task_id="scan_logs",
        python_callable=scan_logs,
        executor_config=executor_cfg,
    )
    compress = PythonOperator(
        task_id="compress_logs",
        python_callable=compress_logs,
        executor_config=executor_cfg,
    )
    purge = PythonOperator(
        task_id="purge_expired",
        python_callable=purge_expired,
        executor_config=executor_cfg,
    )

    scan >> compress >> purge
