"""
File:        test_electricity_maps.py
Author:      Kevin Auberson
Created:     2026-05-21
Description: Unit tests for the Electricity Maps client. The HTTP layer
             (requests.get) is mocked so the tests run offline and without
             a real API token.
"""

import os
from unittest.mock import MagicMock, patch

os.environ.setdefault("EMAPS_TOKEN", "test-token")
os.environ.setdefault("EMAPS_ZONE", "CH")

from cache import cache
from electricity_maps import ElectricityMaps


def _fake_response(json_payload):
    """Build a fake requests.Response that returns the given JSON."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = json_payload
    mock_resp.raise_for_status.return_value = None  # simulate HTTP 200
    return mock_resp


@patch("electricity_maps.requests.get")
def test_get_current_parses_intensity(mock_get):
    """get_current extracts carbon intensity, datetime and zone."""

    mock_get.return_value = _fake_response(
        {
            "carbonIntensity": 92,
            "datetime": "2026-05-21T10:00:00Z",
            "zone": "CH",
        }
    )
    cache._store.clear()

    em = ElectricityMaps()

    result = em.get_current()

    assert result["carbon_intensity"] == 92
    assert result["datetime"] == "2026-05-21T10:00:00Z"
    assert result["zone"] == "CH"


@patch("electricity_maps.requests.get")
def test_get_current_uses_cache_on_second_call(mock_get):
    """The second call hits the cache and does not call the API again."""

    mock_get.return_value = _fake_response(
        {
            "carbonIntensity": 50,
            "datetime": "2026-05-21T10:00:00Z",
            "zone": "CH",
        }
    )
    cache._store.clear()

    em = ElectricityMaps()

    em.get_current()  # first call -> hits the API
    em.get_current()  # second call -> should hit the cache

    # Assert: requests.get was called only ONCE
    assert mock_get.call_count == 1


@patch("electricity_maps.requests.get")
def test_get_current_calls_correct_url(mock_get):
    """The request targets the latest endpoint with the configured zone."""

    mock_get.return_value = _fake_response(
        {
            "carbonIntensity": 10,
            "datetime": "2026-05-21T10:00:00Z",
            "zone": "CH",
        }
    )
    cache._store.clear()

    em = ElectricityMaps()

    em.get_current()

    # Assert: inspect the URL passed to requests.get
    called_url = mock_get.call_args[0][0]
    assert "carbon-intensity/latest" in called_url
    assert "zone=CH" in called_url


@patch("electricity_maps.requests.get")
def test_get_forecast_returns_list(mock_get):
    """get_forecast_24h returns the forecast list from the API payload."""

    mock_get.return_value = _fake_response(
        {
            "forecast": [
                {"datetime": "2026-05-21T11:00:00Z", "carbonIntensity": 40},
                {"datetime": "2026-05-21T12:00:00Z", "carbonIntensity": 35},
            ]
        }
    )
    cache._store.clear()

    em = ElectricityMaps()

    forecast = em.get_forecast_24h()

    assert len(forecast) == 2
    assert forecast[0]["carbonIntensity"] == 40


@patch("electricity_maps.requests.get")
def test_get_forecast_empty_when_no_data(mock_get):
    """An API response without a 'forecast' key yields an empty list."""

    mock_get.return_value = _fake_response({})
    cache._store.clear()

    em = ElectricityMaps()

    forecast = em.get_forecast_24h()

    assert forecast == []
