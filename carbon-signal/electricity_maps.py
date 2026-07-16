"""
File:        electricity_maps.py
Author:      Kevin Auberson
Created:     2026-05-10
Description: Client for the Electricity Maps API. Fetches current grid
             carbon intensity (gCO2eq/kWh) and 24-hour forecast for the
             configured zone. Results are cached to respect the API quota.
             Cache TTLs — tune to match your API plan quota:
                - Academic / unlimited : current=300 s, forecast=3600 s  (defaults)
                - Commercial (~1000/day): current=120 s, forecast=3600 s
                - Free tier (~100/day)  : current=900 s, forecast=86400 s
"""

import os

import requests
from cache import cache


class ElectricityMaps:
    """Client for the Electricity Maps REST API."""

    def __init__(self):
        self.token = os.environ["EMAPS_TOKEN"]
        self.zone = os.environ.get("EMAPS_ZONE", "CH")
        self.base_url = "https://api.electricitymap.org/v3"
        self.ttl = int(os.environ.get("EMAPS_CURRENT_TTL", "300"))
        self.forecast_ttl = int(os.environ.get("EMAPS_FORECAST_TTL", "3600"))

    def _headers(self):
        """Build the auth headers required by the API."""
        return {"auth-token": self.token}

    def get_current(self):
        """
        Fetch the current grid carbon intensity for the configured zone.

        Returns:
            A dict with keys 'carbon_intensity' (gCO2eq/kWh),
            'datetime' (ISO timestamp) and 'zone' (e.g. "CH").

        Raises:
            requests.HTTPError: If the API call fails.
        """
        cached = cache.get("emaps_current")
        if cached is not None:
            return cached

        url = f"{self.base_url}/carbon-intensity/latest?zone={self.zone}&emissionFactorType=lifecycle"
        response = requests.get(url, headers=self._headers(), timeout=10)
        response.raise_for_status()
        data = response.json()

        result = {
            "carbon_intensity": data["carbonIntensity"],
            "datetime": data["datetime"],
            "zone": self.zone,
        }
        cache.set("emaps_current", result, self.ttl)
        return result

    def get_forecast_24h(self):
        """
        Fetch the 24-hour carbon intensity forecast.

        Useful for shifting workloads to greener time periods.

        Returns:
            A list of forecast points, each containing 'datetime' and
            'carbonIntensity'. Empty list if the API has no data.

        Raises:
            requests.HTTPError: If the API call fails.
        """
        cached = cache.get("emaps_forecast")
        if cached is not None:
            return cached

        url = (
            f"{self.base_url}/carbon-intensity/forecast"
            f"?zone={self.zone}&emissionFactorType=lifecycle"
        )
        response = requests.get(url, headers=self._headers(), timeout=10)
        response.raise_for_status()

        forecast = response.json().get("forecast", [])
        cache.set("emaps_forecast", forecast, self.forecast_ttl)
        return forecast


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    em = ElectricityMaps()
    print(em.get_forecast_24h())
