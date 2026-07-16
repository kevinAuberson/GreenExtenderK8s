"""
File:        gate_controller.py
Author:      Kevin Auberson
Created:     2026-06-17
Description: Background controller that manages carbon-aware schedulingGates.
             Polls gated pods every 30s, evaluates temporal conditions via
             temporal.decide(), and removes the gate when conditions are met.
             Replaces the /filter-based delay hack with a Kubernetes-native
             mechanism.
"""

import asyncio
import logging
from datetime import UTC, datetime

from kubernetes import client

from metrics import CI_AT_DECISION, DELAY_GAIN, GATE_DELAY_DURATION, SCHEDULING_DECISIONS
from temporal import DelayDecision
from workload_classifier import classify

log = logging.getLogger("gate_controller")

GATE_NAME = "carbon-aware-gate"
POLL_INTERVAL_SECONDS = 30


def _pod_to_dict(pod) -> dict:
    """Convert a kubernetes client V1Pod to the dict format expected by temporal.decide()."""
    metadata = pod.metadata
    owners = []
    for ref in metadata.owner_references or []:
        owners.append(
            {
                "kind": ref.kind,
                "name": ref.name,
                "controller": ref.controller,
            }
        )

    return {
        "metadata": {
            "name": metadata.name,
            "namespace": metadata.namespace,
            "labels": metadata.labels or {},
            "annotations": metadata.annotations or {},
            "ownerReferences": owners,
            "creationTimestamp": (
                metadata.creation_timestamp.isoformat() if metadata.creation_timestamp else None
            ),
        },
        "status": {
            "qosClass": pod.status.qos_class if pod.status else None,
        },
    }


def _remove_gate(v1: client.CoreV1Api, pod) -> None:
    """Patch the pod to remove the carbon-aware scheduling gate."""
    remaining = [{"name": g.name} for g in (pod.spec.scheduling_gates or []) if g.name != GATE_NAME]
    body = {"spec": {"schedulingGates": remaining or None}}
    v1.patch_namespaced_pod(
        name=pod.metadata.name,
        namespace=pod.metadata.namespace,
        body=body,
    )


async def gate_controller_loop(signal_loader, temporal_scheduler) -> None:
    """Background loop: evaluate gated pods and release them when CI conditions allow."""
    v1 = client.CoreV1Api()

    while True:
        try:
            pods = v1.list_pod_for_all_namespaces(field_selector="status.phase=Pending")

            gated = []
            for pod in pods.items:
                gates = pod.spec.scheduling_gates or []
                if any(g.name == GATE_NAME for g in gates):
                    gated.append(pod)

            if gated:
                log.info(f"Found {len(gated)} gated pod(s)")

            for pod in gated:
                pod_dict = _pod_to_dict(pod)
                pod_id = f"{pod.metadata.namespace}/{pod.metadata.name}"

                decision, reason = temporal_scheduler.decide(pod_dict)

                carbon_class = classify(pod_dict).value
                signal = signal_loader.load()
                ci = signal["grid_intensity_g_per_kwh"] if signal else 0.0

                if decision == DelayDecision.SCHEDULE_NOW:
                    _remove_gate(v1, pod)
                    SCHEDULING_DECISIONS.labels(
                        carbon_class=carbon_class, decision="schedule_now"
                    ).inc()
                    CI_AT_DECISION.labels(
                        carbon_class=carbon_class, decision="schedule_now"
                    ).observe(ci)

                    if pod.metadata.creation_timestamp:
                        delay_s = (
                            datetime.now(UTC) - pod.metadata.creation_timestamp
                        ).total_seconds()
                        GATE_DELAY_DURATION.labels(carbon_class=carbon_class).observe(delay_s)

                    log.info(f"Gate removed for {pod_id}: {reason}")
                else:
                    SCHEDULING_DECISIONS.labels(carbon_class=carbon_class, decision="delay").inc()
                    CI_AT_DECISION.labels(carbon_class=carbon_class, decision="delay").observe(ci)

                    optimal = temporal_scheduler.find_optimal_window()
                    if optimal and optimal["potential_gain"] > 0:
                        DELAY_GAIN.labels(carbon_class=carbon_class).observe(
                            optimal["potential_gain"]
                        )

                    log.info(f"Gate kept for {pod_id}: {reason}")

        except Exception as exc:
            log.warning(f"Gate controller cycle failed: {exc}")

        await asyncio.sleep(POLL_INTERVAL_SECONDS)
