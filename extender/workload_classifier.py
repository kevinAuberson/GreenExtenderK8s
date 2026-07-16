"""
File:        workload_classifier.py
Author:      Kevin Auberson
Created:     2026-06-03
Description: Classifies a pod into one of three carbon classes
             (LATENCY_SENSITIVE, BATCH, BEST_EFFORT) based on priority:
             explicit label > DaemonSet > BestEffort QoS > ReplicaSet/
             StatefulSet > Job > orphan pod heuristic.
"""

import logging
from enum import StrEnum

log = logging.getLogger("classifier")


class CarbonClass(StrEnum):
    LATENCY_SENSITIVE = "latency-sensitive"
    BATCH = "batch"
    BEST_EFFORT = "best-effort"


PENALTY_FACTORS = {
    CarbonClass.LATENCY_SENSITIVE: 0.0,
    CarbonClass.BATCH: 0.5,
    CarbonClass.BEST_EFFORT: 1.0,
}

# Valid labels for manual override
VALID_LABELS = {c.value for c in CarbonClass}


LONG_RUNNING_KINDS = {"ReplicaSet", "StatefulSet"}
BATCH_KINDS = {"Job"}
DAEMON_KINDS = {"DaemonSet"}


def classify(pod: dict) -> CarbonClass:
    """
    Classify a pod into a carbon class.

    Priority:
    1. Explicit `carbon-class` label on the pod (manual override)
    2. Automatic classification based on owner kind + QoS class

    Returns:
        CarbonClass: latency-sensitive | batch | best-effort
    """
    metadata = pod.get("metadata", {})
    pod_name = metadata.get("name", "?")

    # Step 1: explicit label
    labels = metadata.get("labels", {})
    explicit = labels.get("carbon-class")
    if explicit in VALID_LABELS:
        log.info(f"Pod {pod_name}: explicit label carbon-class={explicit}")
        return CarbonClass(explicit)

    if explicit:
        log.warning(
            f"Pod {pod_name}: invalid carbon-class label '{explicit}', "
            f"falling back to automatic classification"
        )

    # Step 2: automatic classification
    owner_kind = _get_controller_kind(metadata)
    qos = pod.get("status", {}).get("qosClass", "BestEffort")

    log.debug(f"Pod {pod_name}: owner={owner_kind}, qos={qos}")

    # 2a. DaemonSet: always latency-sensitive
    if owner_kind in DAEMON_KINDS:
        log.info(f"Pod {pod_name}: DaemonSet → latency-sensitive")
        return CarbonClass.LATENCY_SENSITIVE

    # 2b. BestEffort QoS: always best-effort (DaemonSet handled above)
    if qos == "BestEffort":
        log.info(f"Pod {pod_name}: QoS BestEffort → best-effort")
        return CarbonClass.BEST_EFFORT

    # 2c. Long-running (Deployment / StatefulSet / ReplicaSet)
    if owner_kind in LONG_RUNNING_KINDS:
        log.info(f"Pod {pod_name}: {owner_kind} + {qos} → latency-sensitive")
        return CarbonClass.LATENCY_SENSITIVE

    # 2d. Batch (Job / CronJob)
    if owner_kind in BATCH_KINDS:
        log.info(f"Pod {pod_name}: {owner_kind} + {qos} → batch")
        return CarbonClass.BATCH

    # 2e. Fallback: orphan pod or unknown kind
    # Guaranteed/Burstable QoS implies resource requests → latency-sensitive
    if qos in ("Guaranteed", "Burstable"):
        log.info(f"Pod {pod_name}: orphan/unknown owner + QoS={qos} → latency-sensitive")
        return CarbonClass.LATENCY_SENSITIVE

    log.warning(f"Pod {pod_name}: orphan/unknown owner + QoS BestEffort → best-effort")
    return CarbonClass.BEST_EFFORT


def _get_controller_kind(metadata: dict) -> str | None:
    """
    Return the kind of the pod's controller owner.

    A pod may have several owners, but only one is marked `controller: true`
    — that is the one managing its lifecycle.

    Returns:
        The controller kind (e.g. "ReplicaSet", "Job"), or None if orphan.
    """
    refs = metadata.get("ownerReferences", [])

    # Look for the one marked controller=true
    for ref in refs:
        if ref.get("controller", False):
            return ref.get("kind")

    # Fallback: first owner if none is marked controller
    return refs[0].get("kind") if refs else None
