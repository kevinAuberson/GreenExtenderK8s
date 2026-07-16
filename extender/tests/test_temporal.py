"""
File:        test_temporal.py
Author:      Kevin Auberson
Created:     2026-06-03
Description: Unit tests for temporal.py — verifies scheduling decisions
             (delay vs schedule now) for all carbon classes, deadline handling,
             max-delay expiry, and forecast-based optimal window selection.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from temporal import DelayDecision, TemporalScheduler


def make_forecast(current_ci, hourly_values):
    """Generate a forecast from a list of hourly intensities."""
    now = datetime.now(UTC)
    return [
        {
            "datetime": (now + timedelta(hours=i + 1)).isoformat(),
            "carbon_intensity": ci,
        }
        for i, ci in enumerate(hourly_values)
    ]


def make_signal(ci, forecast=None):
    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "grid_intensity_g_per_kwh": ci,
        "forecast_24h": forecast or [],
        "nodes": [],
    }


def make_pod_besteffort(annotations=None, age_hours=0):
    created = (datetime.now(UTC) - timedelta(hours=age_hours)).isoformat()
    return {
        "metadata": {
            "name": "test-be",
            "creationTimestamp": created,
            "annotations": annotations or {},
            "ownerReferences": [{"kind": "ReplicaSet", "controller": True}],
        },
        "status": {"qosClass": "BestEffort"},
    }


def make_pod_batch(annotations=None, age_hours=0):
    created = (datetime.now(UTC) - timedelta(hours=age_hours)).isoformat()
    return {
        "metadata": {
            "name": "test-batch",
            "creationTimestamp": created,
            "annotations": annotations or {},
            "ownerReferences": [{"kind": "Job", "controller": True}],
        },
        "status": {"qosClass": "Burstable"},
    }


@pytest.fixture
def scheduler():
    loader = MagicMock()
    return TemporalScheduler(loader), loader


def test_latency_sensitive_never_delayed_even_red(scheduler):
    sched, loader = scheduler
    loader.load.return_value = make_signal(ci=120)
    pod = {
        "metadata": {
            "name": "web",
            "ownerReferences": [{"kind": "ReplicaSet", "controller": True}],
        },
        "status": {"qosClass": "Guaranteed"},
    }
    decision, _ = sched.decide(pod)
    assert decision == DelayDecision.SCHEDULE_NOW


# Best-effort: forecast analysis


def test_besteffort_delayed_when_better_window_exists(scheduler):
    sched, loader = scheduler
    # Currently at 65, drops to 30 in 3h
    forecast = make_forecast(65, [60, 50, 30, 35, 40, 45])
    loader.load.return_value = make_signal(ci=65, forecast=forecast)

    decision, reason = sched.decide(make_pod_besteffort())
    assert decision == DelayDecision.DELAY
    assert "waiting" in reason.lower()


def test_besteffort_scheduled_when_already_optimal(scheduler):
    """Best-effort: scheduled when already at minimum."""
    sched, loader = scheduler
    # At 45, forecast only goes up -> schedule now
    forecast = make_forecast(45, [50, 60, 70, 65, 55, 50])
    loader.load.return_value = make_signal(ci=45, forecast=forecast)

    decision, reason = sched.decide(make_pod_besteffort())
    assert decision == DelayDecision.SCHEDULE_NOW
    assert "near-optimal" in reason.lower() or "green" in reason.lower()


def test_besteffort_scheduled_when_gain_is_small(scheduler):
    """Best-effort: not delayed when the gain is too small."""
    sched, loader = scheduler
    # 45 now, 42 in 5h -> gain of 3 only < MIN_GAIN_TO_DELAY (10)
    forecast = make_forecast(45, [44, 43, 43, 42, 42])
    loader.load.return_value = make_signal(ci=45, forecast=forecast)

    decision, reason = sched.decide(make_pod_besteffort())
    assert decision == DelayDecision.SCHEDULE_NOW
    assert "near-optimal" in reason.lower() or "green" in reason.lower()


# Batch: only delayed in red zone


def test_batch_flexible_delayed_in_red_zone(scheduler):
    """Batch flexible: delayed only in red zone."""
    sched, loader = scheduler
    forecast = make_forecast(85, [80, 60, 40, 50, 60])
    loader.load.return_value = make_signal(ci=85, forecast=forecast)

    pod = make_pod_batch(annotations={"carbon-aware/flexible": "true"})
    decision, _ = sched.decide(pod)
    assert decision == DelayDecision.DELAY


def test_batch_flexible_not_delayed_in_orange(scheduler):
    """Batch flexible: not delayed in orange zone (40-70)."""
    sched, loader = scheduler
    forecast = make_forecast(55, [50, 40, 35, 40, 50])
    loader.load.return_value = make_signal(ci=55, forecast=forecast)

    pod = make_pod_batch(annotations={"carbon-aware/flexible": "true"})
    decision, _ = sched.decide(pod)
    assert decision == DelayDecision.SCHEDULE_NOW


def test_batch_flexible_by_default(scheduler):
    """Batch without annotation is flexible by default and can be delayed."""
    sched, loader = scheduler
    forecast = make_forecast(120, [80, 60, 40, 50, 60])
    loader.load.return_value = make_signal(ci=120, forecast=forecast)

    decision, _ = sched.decide(make_pod_batch())
    assert decision == DelayDecision.DELAY


def test_batch_opt_out_never_delayed(scheduler):
    """Batch with flexible=false is never delayed."""
    sched, loader = scheduler
    forecast = make_forecast(120, [80, 60, 40, 50, 60])
    loader.load.return_value = make_signal(ci=120, forecast=forecast)

    pod = make_pod_batch(annotations={"carbon-aware/flexible": "false"})
    decision, _ = sched.decide(pod)
    assert decision == DelayDecision.SCHEDULE_NOW


# Deadlines


def test_deadline_in_past_forces_schedule(scheduler):
    sched, loader = scheduler
    forecast = make_forecast(80, [60, 40, 30])
    loader.load.return_value = make_signal(ci=80, forecast=forecast)

    past = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    pod = make_pod_besteffort(annotations={"carbon-aware/deadline": past})

    decision, reason = sched.decide(pod)
    assert decision == DelayDecision.SCHEDULE_NOW
    assert "deadline" in reason.lower()


def test_deadline_excludes_far_future_optimal(scheduler):
    """Forecast is filtered by the deadline."""
    sched, loader = scheduler
    # Optimal at +5h, but deadline in +2h -> only +1h and +2h are considered
    forecast = make_forecast(70, [65, 60, 55, 50, 30])  # min at +5h (30)
    loader.load.return_value = make_signal(ci=70, forecast=forecast)

    deadline = (datetime.now(UTC) + timedelta(hours=2)).isoformat()
    pod = make_pod_besteffort(annotations={"carbon-aware/deadline": deadline})

    decision, reason = sched.decide(pod)
    # Accessible min before deadline is 60 (+2h), gain=10
    # With gain=10 and MIN_GAIN_TO_DELAY=10, "< 10" is False -> delay
    assert decision == DelayDecision.DELAY


# Max delay


def test_max_delay_exceeded_forces_schedule(scheduler):
    """If the pod has been waiting too long, force scheduling."""
    sched, loader = scheduler
    forecast = make_forecast(100, [80, 60, 40])
    loader.load.return_value = make_signal(ci=100, forecast=forecast)

    # Pod created 25h ago, default max-delay = 24h -> expired
    pod = make_pod_besteffort(age_hours=25)
    decision, reason = sched.decide(pod)
    assert decision == DelayDecision.SCHEDULE_NOW
    assert "deadline" in reason.lower() or "max-delay" in reason.lower()


# No forecast (fallback)


def test_no_forecast_falls_back_to_zone_logic(scheduler):
    """Without forecast, falls back to simple 2-zone logic."""
    sched, loader = scheduler
    loader.load.return_value = make_signal(ci=85, forecast=None)

    decision, _ = sched.decide(make_pod_besteffort())
    assert decision == DelayDecision.DELAY


# No signal


def test_no_signal_fails_safe(scheduler):
    """Without signal, schedules by default (fail-safe)."""
    sched, loader = scheduler
    loader.load.return_value = None

    decision, _ = sched.decide(make_pod_besteffort())
    assert decision == DelayDecision.SCHEDULE_NOW


# Grid already green


def test_green_grid_always_schedules(scheduler):
    """Green grid -> schedule everyone."""
    sched, loader = scheduler
    forecast = make_forecast(30, [25, 20, 15, 20, 25])
    loader.load.return_value = make_signal(ci=30, forecast=forecast)

    decision, reason = sched.decide(make_pod_besteffort())
    assert decision == DelayDecision.SCHEDULE_NOW
    assert "green" in reason.lower()
