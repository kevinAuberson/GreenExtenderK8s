"""
File:        test_extender.py
Author:      Kevin Auberson
Created:     2026-05-02
Description: Integration tests for the extender HTTP endpoints (/filter,
             /prioritize, /healthz, /debug/*) using FastAPI TestClient.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


def make_signal(ci=50, forecast=None, nodes=None):
    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "grid_intensity_g_per_kwh": ci,
        "forecast_24h": forecast or [],
        "nodes": nodes
        or [
            {"name": "node-a", "watts": 2.0, "cpu_millicores": 500},
            {"name": "node-b", "watts": 4.0, "cpu_millicores": 1000},
        ],
    }


def make_filter_payload(node_names, pod=None):
    pod = pod or {
        "metadata": {"name": "test-pod"},
        "status": {"qosClass": "Burstable"},
    }
    return {
        "Pod": pod,
        "Nodes": {"items": [{"metadata": {"name": n}} for n in node_names]},
    }


@pytest.fixture
def mock_loader():
    loader = MagicMock()
    loader.load.return_value = make_signal(ci=50)
    loader.age_seconds.return_value = 10.0
    return loader


@pytest.fixture
def client(mock_loader):
    with (
        patch("extender.signal_loader", mock_loader),
        patch("extender.scorer.signal_loader", mock_loader),
        patch("extender.temporal.signal_loader", mock_loader),
        patch("gate_controller.gate_controller_loop", return_value=None),
    ):
        import importlib

        import extender

        importlib.reload(extender)
        extender.signal_loader = mock_loader
        extender.scorer.signal_loader = mock_loader
        extender.temporal.signal_loader = mock_loader
        yield TestClient(extender.app), mock_loader


# /healthz


def test_healthz_ok(client):
    c, loader = client
    resp = c.get("/healthz")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["signal_available"] is True
    assert data["signal_age_seconds"] == 10.0


def test_healthz_no_signal(client):
    c, loader = client
    loader.load.return_value = None
    loader.age_seconds.return_value = None
    resp = c.get("/healthz")
    assert resp.status_code == 200
    assert resp.json()["signal_available"] is False


# /filter


def test_filter_passes_all_nodes_when_schedule_now(client):
    """Green grid -> schedule_now -> all nodes pass through."""
    c, loader = client
    loader.load.return_value = make_signal(ci=20)  # below GREEN_THRESHOLD=40
    payload = make_filter_payload(["node-a", "node-b"])
    resp = c.post("/filter", json=payload)
    assert resp.status_code == 200
    result = resp.json()
    assert len(result["Nodes"]["items"]) == 2
    assert result["FailedNodes"] == {}


def test_filter_passes_all_nodes_even_on_red_grid(client):
    """Red grid + best-effort -> /filter is a pass-through (temporal shifting
    is handled by the gate controller via schedulingGates, not by /filter)."""
    c, loader = client
    loader.load.return_value = make_signal(ci=85)
    be_pod = {
        "metadata": {
            "name": "be-pod",
            "creationTimestamp": datetime.now(UTC).isoformat(),
            "annotations": {},
            "ownerReferences": [{"kind": "ReplicaSet", "controller": True}],
        },
        "status": {"qosClass": "BestEffort"},
    }
    payload = make_filter_payload(["node-a", "node-b"], pod=be_pod)
    resp = c.post("/filter", json=payload)
    assert resp.status_code == 200
    result = resp.json()
    assert len(result["Nodes"]["items"]) == 2
    assert result["FailedNodes"] == {}


def test_filter_never_delays_latency_sensitive(client):
    """Deployment Guaranteed -> latency-sensitive -> never delayed."""
    c, loader = client
    loader.load.return_value = make_signal(ci=120)
    ls_pod = {
        "metadata": {
            "name": "web",
            "ownerReferences": [{"kind": "ReplicaSet", "controller": True}],
        },
        "status": {"qosClass": "Guaranteed"},
    }
    payload = make_filter_payload(["node-a"], pod=ls_pod)
    resp = c.post("/filter", json=payload)
    assert resp.json()["Nodes"]["items"] != []


# /prioritize


def test_prioritize_scores_all_nodes(client):
    c, loader = client
    pod = {"metadata": {"name": "p"}, "status": {"qosClass": "Burstable"}}
    payload = {
        "Pod": pod,
        "NodeNames": ["node-a", "node-b"],
    }
    resp = c.post("/prioritize", json=payload)
    assert resp.status_code == 200
    results = resp.json()["HostPriorityList"]
    assert len(results) == 2
    hosts = {r["Host"] for r in results}
    assert hosts == {"node-a", "node-b"}
    for r in results:
        assert 0 <= r["Score"] <= 100


def test_prioritize_no_signal_returns_neutral(client):
    c, loader = client
    loader.load.return_value = None
    payload = {
        "Pod": {"metadata": {"name": "p"}, "status": {"qosClass": "Burstable"}},
        "NodeNames": ["node-a"],
    }
    resp = c.post("/prioritize", json=payload)
    assert resp.json()["HostPriorityList"][0]["Score"] == 50


# /debug/forecast


def test_debug_forecast_no_signal(client):
    c, loader = client
    loader.load.return_value = None
    resp = c.get("/debug/forecast")
    assert resp.status_code == 200
    assert "error" in resp.json()


def test_debug_forecast_with_signal(client):
    c, loader = client
    resp = c.get("/debug/forecast")
    assert resp.status_code == 200
    data = resp.json()
    assert "current" in data
    assert "thresholds" in data
