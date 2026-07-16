"""
File:        temporal.py
Author:      Kevin Auberson
Created:     2026-06-03
Description: Carbon-aware temporal shifting engine. Decides whether a pod
             should be scheduled immediately or delayed until a greener window
             by analysing the 24 h CI forecast from ElectricityMaps.
             Respects per-pod deadlines and a configurable max-delay horizon.
"""

import logging
import os
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from workload_classifier import CarbonClass, classify

log = logging.getLogger("temporal")


# Fallback thresholds — used only in local dev (no aggregator).
# In production, dynamic thresholds from the carbon-signal ConfigMap
# (P15/P85 of forecast or monthly historical table) always take precedence.
GREEN_THRESHOLD = int(os.getenv("GREEN_THRESHOLD_G_PER_KWH", "40"))
DIRTY_THRESHOLD = int(os.getenv("DIRTY_THRESHOLD_G_PER_KWH", "70"))

# By default a flexible pod will never be delayed more than 24 hours
DEFAULT_MAX_DELAY_HOURS = int(os.getenv("DEFAULT_MAX_DELAY_HOURS", "24"))

# Minimum gain (gCO2eq/kWh) to justify delaying a pod.
# Avoids delaying for a negligible 2 gCO2/kWh improvement.
MIN_GAIN_TO_DELAY = int(os.getenv("MIN_GAIN_TO_DELAY_G_PER_KWH", "2"))


ANN_FLEXIBLE = "carbon-aware/flexible"
ANN_DEADLINE = "carbon-aware/deadline"
ANN_MAX_DELAY = "carbon-aware/max-delay-hours"


class DelayDecision(StrEnum):
    SCHEDULE_NOW = "schedule_now"
    DELAY = "delay"


class TemporalScheduler:
    """
    Decides whether a pod should be scheduled now or delayed.

    Strategy:
    1. Latency-sensitive pods -> never delayed
    2. Non-flexible pods -> never delayed
    3. No signal/forecast -> fail-safe to SCHEDULE_NOW
    4. With forecast:
       - Find the optimal window (lowest CI) before the deadline
       - If the current window is already optimal or near-optimal -> SCHEDULE_NOW
       - Otherwise, wait (the controller will re-evaluate in ~30s)
    5. If deadline reached or max_delay exceeded -> force SCHEDULE_NOW
    """

    def __init__(
        self,
        signal_loader,
        green_threshold: int = GREEN_THRESHOLD,
        dirty_threshold: int = DIRTY_THRESHOLD,
        min_gain_to_delay: int = MIN_GAIN_TO_DELAY,
    ):
        self.signal_loader = signal_loader
        self.green_threshold = green_threshold
        self.dirty_threshold = dirty_threshold
        self.min_gain_to_delay = min_gain_to_delay

    def decide(self, pod: dict) -> tuple[DelayDecision, str]:
        """Decide for a given pod: schedule now or delay."""
        carbon_class = classify(pod)

        # Not flexible -> never delayed.
        # _is_flexible handles all classes:
        #   - LATENCY_SENSITIVE without annotation -> not flexible (immediate schedule)
        #   - LATENCY_SENSITIVE with deadline or flexible=true -> flexible (explicit opt-in)
        #   - BATCH / BEST_EFFORT -> flexible by default (opt-out via flexible=false)
        if not self._is_flexible(pod, carbon_class):
            return self._now(f"{carbon_class.value}: not flexible")

        # Load the carbon signal
        signal = self.signal_loader.load()
        if not signal:
            return self._now("no signal available (fail-safe)")

        current_ci = signal["grid_intensity_g_per_kwh"]

        # Dynamic thresholds: signal > env vars (fallback)
        green_threshold = signal.get("green_threshold_g_per_kwh", self.green_threshold)
        dirty_threshold = signal.get("dirty_threshold_g_per_kwh", self.dirty_threshold)

        # Already clean -> schedule immediately
        if current_ci <= green_threshold:
            return self._now(f"grid already green ({current_ci} ≤ {green_threshold:.0f})")

        # Check deadline
        deadline = self._parse_deadline(pod)
        max_delay_end = self._compute_max_delay_end(pod)
        effective_deadline = self._min_dt(deadline, max_delay_end)

        if effective_deadline and datetime.now(UTC) >= effective_deadline:
            return self._now("deadline/max-delay reached, forcing schedule")

        # Analyse forecast to find the optimal window
        forecast = signal.get("forecast_24h", [])
        if not forecast:
            # No forecast: fall back to simple 2-zone logic
            return self._decide_without_forecast(pod, carbon_class, current_ci, dirty_threshold)

        return self._decide_with_forecast(
            pod, carbon_class, current_ci, forecast, effective_deadline, dirty_threshold
        )

    def _decide_with_forecast(
        self,
        pod: dict,
        carbon_class: CarbonClass,
        current_ci: float,
        forecast: list[dict],
        effective_deadline: datetime | None,
        dirty_threshold: float | None = None,
    ) -> tuple[DelayDecision, str]:
        """Find the optimal execution time for the pod within the forecast window."""
        # Keep only forecast points before the deadline
        valid_forecast = self._filter_before_deadline(forecast, effective_deadline)

        if not valid_forecast:
            return self._now("no forecast point before deadline")

        # Find the lowest CI in the available window
        min_point = min(valid_forecast, key=lambda p: p["carbon_intensity"])
        min_ci = min_point["carbon_intensity"]
        min_dt = min_point["datetime"]

        gain = current_ci - min_ci

        # Case 1: already at or near optimal -> schedule now
        if gain < self.min_gain_to_delay:
            return self._now(
                f"current CI={current_ci:.0f} is already near-optimal "
                f"(min forecast={min_ci:.0f}, gain={gain:.0f} < {self.min_gain_to_delay})"
            )

        # Case 2: worth waiting -> zone-based logic
        effective_dirty = dirty_threshold if dirty_threshold is not None else self.dirty_threshold

        if current_ci > effective_dirty:
            # Red zone -> all flexible pods wait
            return self._delay(
                f"{carbon_class.value} in red zone: waiting "
                f"(now={current_ci:.0f} > {effective_dirty:.0f}, "
                f"optimal={min_ci:.0f} at {min_dt}, gain={gain:.0f})"
            )

        if carbon_class == CarbonClass.BEST_EFFORT:
            # Orange zone -> only best-effort waits
            return self._delay(
                f"best-effort in orange zone: waiting "
                f"(now={current_ci:.0f}, optimal={min_ci:.0f} at {min_dt}, "
                f"gain={gain:.0f})"
            )

        # Orange zone -> batch can proceed
        return self._now(
            f"{carbon_class.value} in orange zone (CI={current_ci:.0f}), scheduling now"
        )

    def _decide_without_forecast(
        self,
        pod: dict,
        carbon_class: CarbonClass,
        current_ci: float,
        dirty_threshold: float | None = None,
    ) -> tuple[DelayDecision, str]:
        """
        Without forecast, make a simple threshold-based decision:
        - Red zone -> delay best-effort + batch
        - Orange zone -> delay best-effort only
        - Green zone -> schedule all (already handled in decide())
        """
        effective_dirty = dirty_threshold if dirty_threshold is not None else self.dirty_threshold
        if current_ci > effective_dirty:
            return self._delay(
                f"red zone (CI={current_ci:.0f} > {effective_dirty:.0f}), no forecast available"
            )

        # Orange zone (between green and dirty)
        if carbon_class == CarbonClass.BEST_EFFORT:
            return self._delay(f"orange zone (CI={current_ci:.0f}), delaying best-effort")

        return self._now(f"orange zone (CI={current_ci:.0f}), batch can proceed")

    def _is_flexible(self, pod: dict, carbon_class: CarbonClass) -> bool:
        """
        A pod is flexible (can be delayed) according to these rules, by priority:
        - carbon-aware/flexible=false -> never delayed (explicit opt-out)
        - carbon-aware/flexible=true  -> always delayed if possible (explicit opt-in)
        - carbon-aware/deadline present -> flexible (implicit consent)
        - BEST_EFFORT or BATCH -> flexible by default

        Maximum delay duration is bounded by:
        - the carbon-aware/max-delay-hours annotation on the pod
        - otherwise DEFAULT_MAX_DELAY_HOURS (env var, defaults to 24h)
        """
        annotations = pod.get("metadata", {}).get("annotations", {})
        explicit = annotations.get(ANN_FLEXIBLE, "").lower()

        if explicit == "false":
            return False
        if explicit == "true":
            return True
        if ANN_DEADLINE in annotations:
            return True
        return carbon_class in (CarbonClass.BEST_EFFORT, CarbonClass.BATCH)

    def _parse_deadline(self, pod: dict) -> datetime | None:
        """Parse the carbon-aware/deadline annotation into a UTC datetime."""
        deadline_str = pod.get("metadata", {}).get("annotations", {}).get(ANN_DEADLINE)
        if not deadline_str:
            return None
        try:
            dt = datetime.fromisoformat(deadline_str.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt
        except ValueError as e:
            log.warning(f"Invalid deadline '{deadline_str}': {e}")
            return None

    def _compute_max_delay_end(self, pod: dict) -> datetime:
        """Compute max-delay end = creationTimestamp + max_delay_hours.

        Always returns a datetime to guarantee no pod waits indefinitely.
        Falls back to now if creationTimestamp is absent.
        """
        metadata = pod.get("metadata", {})
        created_str = metadata.get("creationTimestamp")
        if created_str:
            try:
                created = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
                if created.tzinfo is None:
                    created = created.replace(tzinfo=UTC)
            except ValueError:
                created = datetime.now(UTC)
        else:
            created = datetime.now(UTC)

        custom = metadata.get("annotations", {}).get(ANN_MAX_DELAY)
        max_hours = DEFAULT_MAX_DELAY_HOURS
        if custom:
            try:
                max_hours = float(custom)
            except ValueError:
                log.warning(f"Invalid max-delay-hours '{custom}'")

        return created + timedelta(hours=max_hours)

    def _min_dt(self, a: datetime | None, b: datetime | None) -> datetime | None:
        """Return the earliest non-None datetime."""
        if a is None:
            return b
        if b is None:
            return a
        return min(a, b)

    def _filter_before_deadline(
        self, forecast: list[dict], deadline: datetime | None
    ) -> list[dict]:
        """Keep only forecast points before the deadline."""
        if not deadline:
            return forecast
        result = []
        for point in forecast:
            try:
                dt = datetime.fromisoformat(point["datetime"].replace("Z", "+00:00"))
                if dt < deadline:
                    result.append(point)
            except (ValueError, KeyError):
                continue
        return result

    def _now(self, reason: str) -> tuple[DelayDecision, str]:
        return (DelayDecision.SCHEDULE_NOW, reason)

    def _delay(self, reason: str) -> tuple[DelayDecision, str]:
        return (DelayDecision.DELAY, reason)

    def find_optimal_window(self, hours_ahead: int = 24) -> dict | None:
        """Debug tool: return the optimal moment within the next N hours."""
        signal = self.signal_loader.load()
        if not signal:
            return None

        forecast = signal.get("forecast_24h", [])
        if not forecast:
            return None

        horizon = datetime.now(UTC) + timedelta(hours=hours_ahead)
        valid = self._filter_before_deadline(forecast, horizon)
        if not valid:
            return None

        min_point = min(valid, key=lambda p: p["carbon_intensity"])
        return {
            "optimal_datetime": min_point["datetime"],
            "optimal_carbon_intensity": min_point["carbon_intensity"],
            "current_carbon_intensity": signal["grid_intensity_g_per_kwh"],
            "potential_gain": signal["grid_intensity_g_per_kwh"] - min_point["carbon_intensity"],
        }
