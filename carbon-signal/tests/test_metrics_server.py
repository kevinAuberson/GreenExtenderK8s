"""
File:        test_metrics_server.py
Author:      Kevin Auberson
Created:     2026-05-21
Description: Unit tests for the metrics-server client, focusing on the
             CPU and memory unit parsing helpers and the node usage
             aggregation logic (with a mocked Kubernetes API).
"""

from unittest.mock import MagicMock, patch

from metrics_server import MetricsServer


def test_parse_cpu_nanocores():
    """Nanocores ('n' suffix) are converted to millicores."""
    ms = MetricsServer()
    # 1'500'000'000 nanocores = 1500 millicores = 1.5 cores
    assert ms._parse_cpu("1500000000n") == 1500


def test_parse_cpu_microcores():
    """Microcores ('u' suffix) are converted to millicores."""
    ms = MetricsServer()
    # 1'500'000 microcores = 1500 millicores
    assert ms._parse_cpu("1500000u") == 1500


def test_parse_cpu_millicores():
    """Millicores ('m' suffix) are returned as-is."""
    ms = MetricsServer()
    assert ms._parse_cpu("250m") == 250


def test_parse_cpu_whole_cores():
    """A plain number is interpreted as whole cores."""
    ms = MetricsServer()
    assert ms._parse_cpu("2") == 2000  # 2 cores = 2000 millicores


def test_parse_cpu_fractional_cores():
    """A decimal value is converted to millicores."""
    ms = MetricsServer()
    assert ms._parse_cpu("0.5") == 500


def test_parse_memory_mebibytes():
    """Mebibytes (Mi) map directly to MiB."""
    ms = MetricsServer()
    assert ms._parse_memory("512Mi") == 512


def test_parse_memory_gibibytes():
    """Gibibytes (Gi) are converted to MiB."""
    ms = MetricsServer()
    assert ms._parse_memory("2Gi") == 2048


def test_parse_memory_kibibytes():
    """Kibibytes (Ki) are converted to MiB (rounded down)."""
    ms = MetricsServer()
    # 2048 Ki = 2 Mi
    assert ms._parse_memory("2048Ki") == 2


def test_parse_memory_invalid_returns_zero():
    """An unparseable string returns 0 instead of crashing."""
    ms = MetricsServer()
    assert ms._parse_memory("garbage") == 0


def _fake_node_metrics():
    """Return a fake metrics.k8s.io response with two nodes."""
    return {
        "items": [
            {
                "metadata": {"name": "node-1"},
                "usage": {"cpu": "500m", "memory": "1024Mi"},
            },
            {
                "metadata": {"name": "node-2"},
                "usage": {"cpu": "2", "memory": "2Gi"},
            },
        ]
    }


@patch("metrics_server.client")
def test_get_node_usage_parses_all_nodes(mock_client):
    """get_node_usage returns parsed CPU/RAM for every node in the response."""

    mock_api = MagicMock()
    mock_api.list_cluster_custom_object.return_value = _fake_node_metrics()
    mock_client.CustomObjectsApi.return_value = mock_api

    ms = MetricsServer()

    from cache import cache

    cache._store.clear()

    result = ms.get_node_usage()

    assert result["node-1"]["cpu_millicores"] == 500
    assert result["node-1"]["memory_mib"] == 1024
    assert result["node-2"]["cpu_millicores"] == 2000
    assert result["node-2"]["memory_mib"] == 2048
