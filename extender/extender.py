"""
File:        extender.py
Author:      Kevin Auberson
Created:     2026-05-02
Description: HTTP entry point for the carbon-aware scheduler extender.
             Exposes /filter (temporal shifting — delays pods to greener
             windows) and /prioritize (node scoring by marginal carbon cost).
             Also serves /metrics (Prometheus), /healthz and /debug/* endpoints.
"""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from prometheus_client import make_asgi_app

from gate_controller import gate_controller_loop
from metrics import (
    CI_AT_DECISION,
    DIRTY_THRESHOLD_METRIC,
    GREEN_THRESHOLD_METRIC,
    GRID_INTENSITY,
    MARGINAL_COST,
    NODE_CO2_G_PER_S,
    NODE_SCORE,
    NODE_SELECTED,
    NODE_WATTS,
    PRIORITIZE_LATENCY,
    SCHEDULING_DECISIONS,
    SIGNAL_AGE,
)
from scoring import NEUTRAL_SCORE, CarbonScorer
from signal_loader import SignalLoader
from temporal import TemporalScheduler
from workload_classifier import classify

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("extender")

signal_loader = SignalLoader()
scorer = CarbonScorer(signal_loader)
temporal = TemporalScheduler(signal_loader)


async def _refresh_metrics_loop() -> None:
    """Background task: push signal-level gauges every 30 s.

    Decouples Grafana liveness from pod scheduling events so dashboards
    stay fresh even when no workloads are being scheduled.
    """
    while True:
        try:
            signal = signal_loader.load()
            if signal:
                ci = signal["grid_intensity_g_per_kwh"]
                GRID_INTENSITY.set(ci)
                SIGNAL_AGE.set(signal_loader.age_seconds() or 0.0)

                green = signal.get("green_threshold_g_per_kwh")
                dirty = signal.get("dirty_threshold_g_per_kwh")
                if green is not None:
                    GREEN_THRESHOLD_METRIC.set(green)
                if dirty is not None:
                    DIRTY_THRESHOLD_METRIC.set(dirty)

                for node in signal.get("nodes", []):
                    name = node["name"]
                    NODE_WATTS.labels(node=name).set(node.get("watts", 0.0))
                    NODE_CO2_G_PER_S.labels(node=name).set(node.get("co2_g_per_s", 0.0))
        except Exception as exc:
            log.warning(f"Background metrics refresh failed: {exc}")
        await asyncio.sleep(30)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Init kubernetes client for the gate controller
    try:
        if os.environ.get("IN_CLUSTER", "false").lower() == "true":
            from kubernetes import config

            config.load_incluster_config()
            log.info("Loaded in-cluster Kubernetes config for gate controller")
        else:
            from kubernetes import config

            config.load_kube_config()
            log.info("Loaded local kubeconfig for gate controller")
    except Exception as exc:
        log.warning(f"Kubernetes config not available, gate controller disabled: {exc}")

    metrics_task = asyncio.create_task(_refresh_metrics_loop())
    gate_task = asyncio.create_task(gate_controller_loop(signal_loader, temporal))
    try:
        yield
    finally:
        metrics_task.cancel()
        gate_task.cancel()


app = FastAPI(lifespan=lifespan)
app.mount("/metrics", make_asgi_app())


@app.post("/filter")
async def filter_nodes(request: Request):
    """
    Pass-through filter — temporal shifting is handled by the gate controller
    (schedulingGates) BEFORE the pod reaches the scheduler. By the time a pod
    arrives here, the gate has already been removed and the pod is ready to be
    scheduled. This endpoint records scheduling metrics and passes all nodes.
    """
    body = await request.json()
    pod = body.get("Pod", {})
    pod_name = pod.get("metadata", {}).get("name", "?")
    nodes = body.get("Nodes", {}).get("items", [])

    carbon_class = classify(pod).value
    signal = signal_loader.load()
    ci = signal["grid_intensity_g_per_kwh"] if signal else 0.0

    SCHEDULING_DECISIONS.labels(carbon_class=carbon_class, decision="schedule_now").inc()
    CI_AT_DECISION.labels(carbon_class=carbon_class, decision="schedule_now").observe(ci)

    log.info(f"Pod {pod_name}: schedule_now (class={carbon_class}, CI={ci:.0f})")

    return {
        "Nodes": {"items": nodes},
        "FailedNodes": {},
        "Error": "",
    }


@app.post("/prioritize")
async def prioritize(request: Request):
    """Score each candidate node and return the scheduler extender's HostPriorityList format."""
    body = await request.json()
    pod = body.get("Pod", {})
    pod_name = pod.get("metadata", {}).get("name", "?")
    node_names = body.get("NodeNames") or [
        n["metadata"]["name"] for n in body.get("Nodes", {}).get("items", [])
    ]

    _start = time.monotonic()
    scores = scorer.score_nodes(pod, node_names)
    PRIORITIZE_LATENCY.observe(time.monotonic() - _start)
    results = [{"Host": name, "Score": scores.get(name, NEUTRAL_SCORE)} for name in node_names]

    carbon_class = classify(pod).value
    signal = signal_loader.load()

    for name, score in scores.items():
        NODE_SCORE.labels(node=name, carbon_class=carbon_class).set(score)

    if signal:
        for node_data in signal.get("nodes", []):
            if node_data["name"] in scores:
                MARGINAL_COST.labels(node=node_data["name"]).observe(
                    node_data.get("co2_g_per_s", 0.0)
                )

    if results:
        best = max(results, key=lambda x: x["Score"])
        NODE_SELECTED.labels(node=best["Host"], carbon_class=carbon_class).inc()
        log.info(f"Pod {pod_name} → best: {best['Host']} (score={best['Score']})")

    return {"HostPriorityList": results}


@app.get("/healthz")
async def health():
    """Report liveness plus signal availability/freshness for readiness probes."""
    signal = signal_loader.load()
    age = signal_loader.age_seconds()
    return {
        "status": "ok",
        "signal_available": signal is not None,
        "signal_age_seconds": age,
        "green_threshold": temporal.green_threshold,
    }


@app.get("/debug/decide")
async def debug_decide():
    """Show the decision for synthetic test pods (debug)."""
    fake_pods = {
        "deployment-pod": {
            "metadata": {
                "name": "test",
                "ownerReferences": [{"kind": "ReplicaSet", "controller": True}],
            },
            "status": {"qosClass": "Burstable"},
        },
        "batch-rigid": {
            "metadata": {
                "name": "test",
                "ownerReferences": [{"kind": "Job", "controller": True}],
            },
            "status": {"qosClass": "Burstable"},
        },
        "batch-flexible": {
            "metadata": {
                "name": "test",
                "annotations": {"carbon-aware/flexible": "true"},
                "ownerReferences": [{"kind": "Job", "controller": True}],
            },
            "status": {"qosClass": "Burstable"},
        },
        "best-effort": {
            "metadata": {
                "name": "test",
                "ownerReferences": [{"kind": "ReplicaSet", "controller": True}],
            },
            "status": {"qosClass": "BestEffort"},
        },
    }
    return {
        name: {"decision": d[0].value, "reason": d[1]}
        for name, pod in fake_pods.items()
        for d in [temporal.decide(pod)]
    }


@app.get("/debug/forecast")
async def debug_forecast():
    """Show the forecast and the optimal scheduling window."""
    signal = signal_loader.load()
    if not signal:
        return {"error": "no signal"}

    optimal = temporal.find_optimal_window()
    return {
        "current": {
            "datetime": signal["timestamp"],
            "carbon_intensity": signal["grid_intensity_g_per_kwh"],
        },
        "forecast_24h": signal.get("forecast_24h", []),
        "optimal_window": optimal,
        "thresholds": {
            "green": temporal.green_threshold,
            "dirty": temporal.dirty_threshold,
        },
    }
