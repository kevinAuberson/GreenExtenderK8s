"""
File:        metrics.py
Author:      Kevin Auberson
Created:     2026-06-09
Description: Prometheus metric definitions for the carbon-aware scheduler
             extender. Covers scheduling decisions, CI at decision time,
             delay gains, node scores, and continuous signal-level gauges
             refreshed every 30 s by a background task in extender.py.
"""

from prometheus_client import Counter, Gauge, Histogram

# Continuous signal gauges (refreshed every 30s in background)

GRID_INTENSITY = Gauge(
    "carbon_grid_intensity_current_g_per_kwh",
    "Current grid carbon intensity from the carbon-signal ConfigMap",
)

SIGNAL_AGE = Gauge(
    "carbon_signal_age_seconds",
    "Age in seconds of the last loaded carbon signal",
)

GREEN_THRESHOLD_METRIC = Gauge(
    "carbon_green_threshold_g_per_kwh",
    "Current green threshold used by the extender (dynamic or static fallback)",
)

DIRTY_THRESHOLD_METRIC = Gauge(
    "carbon_dirty_threshold_g_per_kwh",
    "Current dirty threshold used by the extender (dynamic or static fallback)",
)

NODE_WATTS = Gauge(
    "carbon_node_watts",
    "Last known power consumption of a node (from vSphere via aggregator)",
    ["node"],
)

NODE_CO2_G_PER_S = Gauge(
    "carbon_node_co2_g_per_s",
    "Last known CO2 emission rate of a node (watts × grid intensity / 3_600_000)",
    ["node"],
)

# Scheduling decisions

SCHEDULING_DECISIONS = Counter(
    "carbon_scheduling_decisions_total",
    "Total scheduling decisions made by the extender",
    ["carbon_class", "decision"],  # decision: schedule_now | delay
)

# Carbon intensity at decision time — allows comparing the average CI
# at which carbon-aware vs default scheduler places pods
CI_AT_DECISION = Histogram(
    "carbon_ci_at_decision_g_per_kwh",
    "Grid carbon intensity (gCO2eq/kWh) at the moment of the scheduling decision",
    ["carbon_class", "decision"],
    buckets=[10, 20, 30, 40, 50, 60, 70, 80, 100, 150, 200, 300],
)

GATE_DELAY_DURATION = Histogram(
    "carbon_gate_delay_duration_seconds",
    "Time a pod spent gated (between creation and gate removal)",
    ["carbon_class"],
    buckets=[30, 60, 120, 300, 600, 1800, 3600, 7200, 14400, 28800, 43200, 86400],
)

# Potential gain when delaying (current_CI - optimal_CI)
DELAY_GAIN = Histogram(
    "carbon_delay_gain_g_per_kwh",
    "Potential CI gain (gCO2eq/kWh) when a pod is delayed to a greener window",
    ["carbon_class"],
    buckets=[5, 10, 15, 20, 30, 40, 50, 75, 100],
)

# Node scoring (/prioritize)

NODE_SELECTED = Counter(
    "carbon_node_selected_total",
    "Number of times a node was chosen (highest score) during prioritization",
    ["node", "carbon_class"],
)

NODE_SCORE = Gauge(
    "carbon_node_score",
    "Carbon score (0-100) assigned to a node during last prioritization",
    ["node", "carbon_class"],
)

MARGINAL_COST = Histogram(
    "carbon_marginal_cost",
    "Marginal carbon cost of placing a pod on a node",
    ["node"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0],
)

# Scheduling latency — time spent inside /prioritize computing carbon scores
PRIORITIZE_LATENCY = Histogram(
    "carbon_prioritize_duration_seconds",
    "Time spent inside the /prioritize endpoint computing carbon-aware node scores",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)
