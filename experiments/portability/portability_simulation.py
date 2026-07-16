"""
File:        portability_simulation.py
Author:      Kevin Auberson
Created:     2026-07-13
Description: Offline replay of the carbon-aware temporal shifting engine
             against historical grid carbon-intensity traces from zones
             other than Switzerland, to check whether the mechanism's
             behaviour generalises beyond the CH grid it was designed and
             tuned on (Section 4.2.3).

             temporal.py and workload_classifier.py are imported and used
             unchanged from the production codebase — no logic is
             duplicated or re-implemented for the simulation. scoring.py
             is intentionally NOT exercised here: since alpha and CI_norm
             cancel in node scoring for a single-site cluster (Section
             4.2.3), node placement is not expected to change between
             zones, only the decision to delay a pod. compute_thresholds()
             is copied verbatim from main.py rather than imported, since
             importing main.py directly would pull in the Kubernetes,
             vSphere and Electricity Maps clients, none of which are
             needed or usable offline.

             Node power (watts) is held constant across time in this
             replay, since no per-zone hardware telemetry exists. CO2 is
             therefore proportional to CI(t) with a constant factor that
             cancels out of every reduction percentage reported below —
             results are intensity-weighted and do not depend on which
             constant is chosen.

             Zones cover a deliberate mix of grid profiles: DE, ES and PL
             for Europe, and three US regions with different generation
             mixes (see TRACE_FILES below for the rationale behind each
             pick).

Usage:       Place one CSV export per zone in ./traces/ (see TRACE_FILES
             below), matching the Electricity Maps historical export
             format, then run: python portability_simulation.py
"""

from __future__ import annotations

import bisect
import csv
import json
import statistics
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from datetime import datetime as _RealDateTime

# temporal.py and workload_classifier.py live in the extender/ folder,
# one level up from this simulation script — adjust the path below if
# your layout differs.
EXTENDER_DIR = Path(__file__).resolve().parent.parent / "extender"
sys.path.insert(0, str(EXTENDER_DIR))

import temporal as _temporal_module
from temporal import TemporalScheduler, DelayDecision
from workload_classifier import CarbonClass


class _SimClock(_RealDateTime):
    """
    Subclass of datetime.datetime whose now() returns a settable simulated
    time instead of the real wall clock.

    temporal.py's decide() calls datetime.now(UTC) directly to check
    pod deadlines (see _compute_max_delay_end / decide()). That is correct
    for production, but makes decide() unusable as-is for a historical
    replay: any deadline computed from a January 2026 pod creation
    timestamp would already be "in the past" relative to today's real date,
    forcing every pod to schedule_now immediately.

    Patching temporal.py's `datetime` name to this class — rather than
    editing temporal.py — keeps the production decision logic byte-for-byte
    unchanged while letting the replay control what "now" means.
    """

    _sim_now: _RealDateTime | None = None

    @classmethod
    def now(cls, tz=None):
        if cls._sim_now is None:
            raise RuntimeError("Simulated clock not set — call _SimClock.set(now) first")
        return cls._sim_now if tz is None else cls._sim_now.astimezone(tz)

    @classmethod
    def set(cls, now: _RealDateTime) -> None:
        cls._sim_now = now


_temporal_module.datetime = _SimClock

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

# TODO Kevin: drop your Electricity Maps historical exports into traces/,
# one CSV per zone (see discover_trace_files() below — no code change
# needed to add a zone, just add the file).
# Expected CSV columns (matches the Electricity Maps "Life cycle" export format):
#   "Datetime (UTC)", "Carbon intensity gCO₂eq/kWh (Life cycle)"
# Note the "direct" column also present in the export is NOT used here — this
# project uses life-cycle emission factors throughout (Section 5.1.2), so the
# "Life cycle" column is the one that stays consistent with the live aggregator.
#
# Suggested zones, kept here as a rationale for Section 6.4 rather than as a
# fixed list (any zone can be added simply by dropping its trace in traces/):
#   DE          — large, well-documented grid, coal+renewables mix, moderate CoV.
#   ES          — high solar/wind share, strong diurnal swing, good CoV contrast to PL.
#   PL          — coal-dominated, low diurnal variation.
#   US-CAL-CISO — California: solar-heavy, strong daytime dip, highest expected CoV.
#   US-TEX-ERCO — Texas (ERCOT): wind-heavy but volatile, different diurnal shape than solar.
#   US-MIDA-PJM — PJM Interconnection (mid-Atlantic): coal/gas-heavy, low CoV, plays the
#                 same "low variability" role as PL but on the US grid for comparison.
# Anchored on the script's own location, not on the current working
# directory: VS Code's "Run Python File" (and some other launchers) start
# the process with the CWD set to the workspace root, not to this file's
# folder, which made a plain Path("traces/...") fail to resolve.
TRACES_DIR = Path(__file__).resolve().parent / "traces"


def discover_trace_files() -> dict[str, Path]:
    """
    Automatically pick up every CSV file present in traces/, instead of a
    fixed hardcoded list of zones. Adding a new zone means dropping a new
    CSV export into traces/ and rerunning the script — no code change
    needed here.

    The zone label used in the results/plots is derived from the filename:
    a trailing "_<4-digit year>" is stripped if present (DE_2026.csv -> DE),
    otherwise the filename stem is used as-is.
    """
    trace_files: dict[str, Path] = {}
    for path in sorted(TRACES_DIR.glob("*.csv")):
        stem = path.stem
        prefix, _, suffix = stem.rpartition("_")
        zone = prefix if prefix and suffix.isdigit() and len(suffix) == 4 else stem
        trace_files[zone] = path
    return trace_files


TRACE_FILES = discover_trace_files()
DATETIME_COL = "Datetime (UTC)"
CI_COL = "Carbon intensity gCO₂eq/kWh (Life cycle)"

# Same three DAGs as the live benchmark, replayed as
# recurring pod-creation events over the full trace instead of on a real
# Airflow instance.
DAG_SCHEDULE = {
    CarbonClass.LATENCY_SENSITIVE: timedelta(minutes=15),  # data_quality_monitor
    CarbonClass.BATCH: timedelta(minutes=30),  # etl_sales_pipeline
    CarbonClass.BEST_EFFORT: timedelta(hours=2),  # log_archive_cleanup
}

# Gate controller re-evaluates delayed pods every 30s in production
# (Section 5.2.6). Trace resolution is hourly, so re-evaluating more often
# than the trace itself changes would add no information; the tick below
# is set to the trace's own resolution instead.
CONTROLLER_TICK = timedelta(hours=1)

OUTPUT_FILE = Path(__file__).resolve().parent / "results" / "portability_results.json"


def compute_thresholds(monthly_table: dict, forecast_24h: list) -> tuple[float, float, str]:
    if forecast_24h:
        intensities = sorted(p["carbon_intensity"] for p in forecast_24h)
        n = len(intensities)
        if n >= 8:
            p15 = intensities[min(int(n * 0.15), n - 1)]
            p85 = intensities[min(int(n * 0.85), n - 1)]
            if p85 - p15 >= 5:
                return p15, p85, "forecast_p15_p85"

    month = datetime.now(UTC).month
    entry = monthly_table.get(month)
    if entry and entry.get("green") and entry.get("dirty"):
        return float(entry["green"]), float(entry["dirty"]), f"monthly_table_month_{month}"

    return None, None, "unavailable"



def load_trace(path: Path) -> list[dict]:
    """
    Load a historical CI trace from a CSV export.

    Returns:
        A list of {"datetime": datetime, "carbon_intensity": float},
        sorted chronologically.
    """
    points = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            dt = datetime.fromisoformat(row[DATETIME_COL].replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            points.append({"datetime": dt, "carbon_intensity": float(row[CI_COL])})
    points.sort(key=lambda p: p["datetime"])
    return points


# --------------------------------------------------------------------------
# Historical signal loader — same interface as SignalLoader (load() ->
# dict | None), backed by a CSV trace instead of the carbon-signal
# ConfigMap. temporal.TemporalScheduler is unaware of the difference.
# --------------------------------------------------------------------------


class HistoricalSignalLoader:
    def __init__(self, trace: list[dict]):
        self.trace = trace
        self.times = [p["datetime"] for p in trace]
        self.now: datetime | None = None

    def advance_to(self, dt: datetime) -> None:
        self.now = dt

    def _current_index(self) -> int | None:
        if self.now is None:
            return None
        # Nearest point at or before `now`, via binary search instead of a
        # full linear scan — this is called on every gate-controller tick
        # for every pod, so with an 8760-point hourly trace and tens of
        # thousands of simulated pods a linear scan makes the whole replay
        # too slow to finish. bisect keeps each lookup O(log n).
        idx = bisect.bisect_right(self.times, self.now) - 1
        return idx if idx >= 0 else None

    def load(self) -> dict | None:
        idx = self._current_index()
        if idx is None:
            return None

        current = self.trace[idx]
        # Perfect-foresight forecast: the next 24h of the trace itself.
        # This is a known simplification vs. the production system, which
        # uses Electricity Maps' own forecast and therefore carries real
        # forecast error (see Section 6.4 discussion / 6.5 limitations).
        horizon = current["datetime"] + timedelta(hours=24)
        forecast_24h = [
            {"datetime": p["datetime"].isoformat(), "carbon_intensity": p["carbon_intensity"]}
            for p in self.trace[idx + 1 :]
            if p["datetime"] <= horizon
        ]

        green, dirty, source = compute_thresholds({}, forecast_24h)

        signal: dict = {
            "timestamp": current["datetime"].isoformat(),
            "grid_intensity_g_per_kwh": current["carbon_intensity"],
            "forecast_24h": forecast_24h,
            "nodes": [],  # not used: scoring.py is not exercised in this replay
        }
        if green is not None:
            signal["green_threshold_g_per_kwh"] = green
            signal["dirty_threshold_g_per_kwh"] = dirty
            signal["threshold_source"] = source
        return signal


# --------------------------------------------------------------------------
# Pod generation
# --------------------------------------------------------------------------


def make_pod(carbon_class: CarbonClass, created_at: datetime) -> dict:
    return {
        "metadata": {
            "name": f"{carbon_class.value}-{created_at.isoformat()}",
            "creationTimestamp": created_at.isoformat(),
            "labels": {"carbon-class": carbon_class.value},
            "annotations": {},
        },
        "status": {"qosClass": "Burstable"},
    }


def generate_pod_stream(trace: list[dict]) -> list[tuple[CarbonClass, datetime]]:
    """Recreate the three DAGs' creation schedule over the trace's time span."""
    start, end = trace[0]["datetime"], trace[-1]["datetime"]
    stream = []
    for carbon_class, interval in DAG_SCHEDULE.items():
        t = start
        while t <= end:
            stream.append((carbon_class, t))
            t += interval
    stream.sort(key=lambda x: x[1])
    return stream


# --------------------------------------------------------------------------
# Simulation
# --------------------------------------------------------------------------


@dataclass
class PodResult:
    carbon_class: CarbonClass
    created_at: datetime
    scheduled_at: datetime
    ci_at_creation: float
    ci_at_schedule: float
    delayed: bool
    delay_seconds: float


@dataclass
class ZoneResult:
    zone: str
    pods: list[PodResult] = field(default_factory=list)


def simulate_pod(
    carbon_class: CarbonClass,
    created_at: datetime,
    loader: HistoricalSignalLoader,
    scheduler: TemporalScheduler,
    trace_end: datetime,
) -> PodResult:
    pod = make_pod(carbon_class, created_at)

    loader.advance_to(created_at)
    ci_at_creation = loader.load()["grid_intensity_g_per_kwh"]

    now = created_at
    while True:
        loader.advance_to(now)
        _SimClock.set(now)
        decision, _reason = scheduler.decide(pod)
        if decision == DelayDecision.SCHEDULE_NOW or now >= trace_end:
            ci_at_schedule = loader.load()["grid_intensity_g_per_kwh"]
            return PodResult(
                carbon_class=carbon_class,
                created_at=created_at,
                scheduled_at=now,
                ci_at_creation=ci_at_creation,
                ci_at_schedule=ci_at_schedule,
                delayed=(now > created_at),
                delay_seconds=(now - created_at).total_seconds(),
            )
        now += CONTROLLER_TICK


def simulate_zone(zone: str, trace: list[dict]) -> ZoneResult:
    loader = HistoricalSignalLoader(trace)
    scheduler = TemporalScheduler(loader)
    trace_end = trace[-1]["datetime"]

    result = ZoneResult(zone=zone)
    for carbon_class, created_at in generate_pod_stream(trace):
        result.pods.append(
            simulate_pod(carbon_class, created_at, loader, scheduler, trace_end)
        )
    return result


# --------------------------------------------------------------------------
# Analysis
# --------------------------------------------------------------------------


# Buckets used to summarise how long delayed pods actually waited — gives a
# distribution instead of only an average, useful to show how the gate
# controller behaves rather than just its mean effect.
DELAY_BUCKETS = [(0, 2), (2, 4), (4, 8), (8, 16), (16, 24), (24, float("inf"))]


def _bucket_label(lo: float, hi: float) -> str:
    return f"{lo:g}-{hi:g}h" if hi != float("inf") else f"{lo:g}h+"


def analyze_zone(result: ZoneResult) -> dict:
    by_class: dict[str, list[PodResult]] = {}
    for p in result.pods:
        by_class.setdefault(p.carbon_class.value, []).append(p)

    summary = {}
    for cls, pods in by_class.items():
        baseline_ci = [p.ci_at_creation for p in pods]
        aware_ci = [p.ci_at_schedule for p in pods]
        delayed = [p for p in pods if p.delayed]

        reduction_pct = None
        if sum(baseline_ci) > 0:
            reduction_pct = 100 * (1 - sum(aware_ci) / sum(baseline_ci))

        delay_histogram_hours = {
            _bucket_label(lo, hi): sum(
                1 for p in delayed if lo <= p.delay_seconds / 3600 < hi
            )
            for lo, hi in DELAY_BUCKETS
        }

        summary[cls] = {
            "n_pods": len(pods),
            "n_delayed": len(delayed),
            "delay_rate_pct": 100 * len(delayed) / len(pods) if pods else None,
            "avg_delay_hours": (
                statistics.mean(p.delay_seconds for p in delayed) / 3600 if delayed else 0.0
            ),
            "avg_ci_baseline": statistics.mean(baseline_ci) if baseline_ci else None,
            "avg_ci_carbon_aware": statistics.mean(aware_ci) if aware_ci else None,
            "co2_reduction_pct": reduction_pct,
            "delay_histogram_hours": delay_histogram_hours,
        }
    return summary


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------


def main():
    if not TRACE_FILES:
        print(f"No CSV files found in {TRACES_DIR} — nothing to simulate. "
              f"Drop your Electricity Maps exports there and rerun.")
        return

    print(f"Found {len(TRACE_FILES)} zone(s) in {TRACES_DIR}: {', '.join(TRACE_FILES)}")

    all_results = {}

    for zone, path in TRACE_FILES.items():
        print(f"\n=== {zone} ===")
        trace = load_trace(path)
        print(f"Loaded {len(trace)} points, {trace[0]['datetime']} -> {trace[-1]['datetime']}")

        # Grid variability for this zone's trace, used to check the
        # hypothesis (Section 3.1.2, CarbonFlex/CarbonScaler) that
        # CO2 reduction correlates with how much a grid's carbon
        # intensity varies over time.
        ci_values = [p["carbon_intensity"] for p in trace]
        mean_ci = statistics.mean(ci_values)
        stdev_ci = statistics.pstdev(ci_values)
        cov_ci = stdev_ci / mean_ci if mean_ci else None
        print(f"Grid CI: mean={mean_ci:.1f}  stdev={stdev_ci:.1f}  CoV={cov_ci:.3f}")

        zone_result = simulate_zone(zone, trace)
        summary = analyze_zone(zone_result)
        summary["_trace_stats"] = {
            "mean_ci_g_per_kwh": mean_ci,
            "stdev_ci_g_per_kwh": stdev_ci,
            "cov": cov_ci,
        }
        all_results[zone] = summary

        for cls, stats in summary.items():
            if cls.startswith("_"):
                continue
            print(f"  {cls}:")
            print(f"    pods={stats['n_pods']}  delayed={stats['n_delayed']} "
                  f"({stats['delay_rate_pct']:.1f}%)")
            print(f"    avg delay: {stats['avg_delay_hours']:.2f} h")
            print(f"    CI baseline={stats['avg_ci_baseline']:.1f}  "
                  f"carbon-aware={stats['avg_ci_carbon_aware']:.1f}  "
                  f"reduction={stats['co2_reduction_pct']:.1f}%")

    OUTPUT_FILE.write_text(json.dumps(all_results, indent=2))
    print(f"\nResults written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()