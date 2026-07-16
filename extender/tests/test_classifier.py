"""
File:        test_classifier.py
Author:      Kevin Auberson
Created:     2026-06-03
Description: Unit tests for workload_classifier.py — verifies that pods are
             correctly classified into LATENCY_SENSITIVE, BATCH or BEST_EFFORT
             based on labels, owner references and QoS class.
"""

from workload_classifier import CarbonClass, classify

# Explicit label override


def test_explicit_label_overrides_classification():
    """The carbon-class label must take precedence over automatic classification."""
    pod = {
        "metadata": {
            "name": "test",
            "labels": {"carbon-class": "best-effort"},
            "ownerReferences": [{"kind": "ReplicaSet", "controller": True}],
        },
        "status": {"qosClass": "Guaranteed"},
    }
    # Without label -> latency-sensitive; with label -> best-effort
    assert classify(pod) == CarbonClass.BEST_EFFORT


def test_invalid_label_falls_back_to_auto():
    """An invalid label is ignored, falls back to automatic classification."""
    pod = {
        "metadata": {
            "name": "test",
            "labels": {"carbon-class": "invalid-value"},
            "ownerReferences": [{"kind": "ReplicaSet", "controller": True}],
        },
        "status": {"qosClass": "Guaranteed"},
    }
    assert classify(pod) == CarbonClass.LATENCY_SENSITIVE


# controller: true vs [0]


def test_picks_controller_owner_not_first():
    """Must pick the owner with controller=true, not just [0]."""
    pod = {
        "metadata": {
            "name": "test",
            "ownerReferences": [
                {"kind": "SomeOther", "controller": False},
                {"kind": "Job", "controller": True},
            ],
        },
        "status": {"qosClass": "Burstable"},
    }
    assert classify(pod) == CarbonClass.BATCH


# Classification logic


def test_daemonset_is_latency_sensitive():
    pod = {
        "metadata": {
            "name": "test",
            "ownerReferences": [{"kind": "DaemonSet", "controller": True}],
        },
        "status": {"qosClass": "BestEffort"},  # even BestEffort
    }
    assert classify(pod) == CarbonClass.LATENCY_SENSITIVE


def test_deployment_via_replicaset():
    """A Deployment pod appears with owner=ReplicaSet."""
    pod = {
        "metadata": {
            "name": "test",
            "ownerReferences": [{"kind": "ReplicaSet", "controller": True}],
        },
        "status": {"qosClass": "Guaranteed"},
    }
    assert classify(pod) == CarbonClass.LATENCY_SENSITIVE


def test_statefulset_burstable_is_latency_sensitive():
    pod = {
        "metadata": {
            "name": "test",
            "ownerReferences": [{"kind": "StatefulSet", "controller": True}],
        },
        "status": {"qosClass": "Burstable"},
    }
    assert classify(pod) == CarbonClass.LATENCY_SENSITIVE


def test_job_guaranteed_is_batch():
    pod = {
        "metadata": {
            "name": "test",
            "ownerReferences": [{"kind": "Job", "controller": True}],
        },
        "status": {"qosClass": "Guaranteed"},
    }
    assert classify(pod) == CarbonClass.BATCH


def test_job_besteffort_is_best_effort():
    pod = {
        "metadata": {
            "name": "test",
            "ownerReferences": [{"kind": "Job", "controller": True}],
        },
        "status": {"qosClass": "BestEffort"},
    }
    assert classify(pod) == CarbonClass.BEST_EFFORT


def test_deployment_besteffort_is_best_effort():
    pod = {
        "metadata": {
            "name": "test",
            "ownerReferences": [{"kind": "ReplicaSet", "controller": True}],
        },
        "status": {"qosClass": "BestEffort"},
    }
    assert classify(pod) == CarbonClass.BEST_EFFORT


def test_standalone_pod_defaults_to_best_effort():
    """Pod without owner (kubectl run) -> best-effort by default."""
    pod = {
        "metadata": {"name": "test"},
        "status": {"qosClass": "BestEffort"},
    }
    assert classify(pod) == CarbonClass.BEST_EFFORT
