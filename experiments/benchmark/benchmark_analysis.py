"""
File:        benchmark_analysis.py
Author:      Kevin Auberson
Created:     2026-07-07
Description: Compares the baseline run (default-scheduler) with the
             carbon-aware run (extender) over two 48h time windows.

             CO2 is reconstructed from raw watts + grid intensity for
             BOTH runs using the exact same formula, so the comparison
             stays fair even though the baseline never goes through the
             extender's own carbon_node_co2_g_per_s metric.

             Requires: pip install requests
"""

import json
import math
from datetime import UTC, datetime

import requests


PROM_URL = "http://10.190.133.118:30990"  # port-forward to your Prometheus

BASELINE_START = "2026-07-12T22:30:00Z"
BASELINE_END = "2026-07-14T22:30:00Z"

CARBON_AWARE_START = "2026-07-10T21:00:00Z"
CARBON_AWARE_END = "2026-07-12T21:00:00Z"

STEP = "60s"  # matches vSphere's own sensor interval


def query_range(promql: str, start: str, end: str, step: str) -> list[dict]:
    """
    Run a Prometheus range query and return the raw result matrix.

    Args:
        promql: The PromQL expression to evaluate.
        start: ISO 8601 start timestamp.
        end: ISO 8601 end timestamp.
        step: Resolution step (e.g. "60s").

    Returns:
        A list of Prometheus result series, each with "metric" (labels)
        and "values" (list of [timestamp, value] pairs).
    """
    resp = requests.get(
        f"{PROM_URL}/api/v1/query_range",
        params={"query": promql, "start": start, "end": end, "step": step},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload["status"] != "success":
        raise RuntimeError(f"Prometheus query failed: {payload}")
    return payload["data"]["result"]


def co2_step_series(start: str, end: str, step: str) -> list[tuple[str, float]]:
    """
    Build a (timestamp, total_co2_grams_at_this_step) series across all nodes.

    This is the shared building block for the cumulative curve (Ch.6
    figure) — one entry per step, not yet cumulative.

    Formula (same for baseline and carbon-aware, per node, per step):
        co2_g = watts * grid_intensity_g_per_kwh / 3_600_000 * step_seconds

    Args:
        start: ISO 8601 start timestamp.
        end: ISO 8601 end timestamp.
        step: Resolution step (e.g. "60s").

    Returns:
        List of (timestamp, co2_grams_this_step), sorted by time.
    """
    step_seconds = int(step.rstrip("s"))

    watts_series = query_range("carbon_node_watts", start, end, step)
    intensity_series = query_range(
        "carbon_grid_intensity_current_g_per_kwh", start, end, step
    )

    intensity_by_ts = {}
    if intensity_series:
        for ts, val in intensity_series[0]["values"]:
            intensity_by_ts[ts] = float(val)

    total_by_ts: dict[str, float] = {}
    for series in watts_series:
        for ts, val in series["values"]:
            ci = intensity_by_ts.get(ts)
            if ci is None:
                continue  # missing sample, skip rather than guess
            watts = float(val)
            total_by_ts[ts] = total_by_ts.get(ts, 0.0) + watts * ci / 3_600_000 * step_seconds

    return sorted(total_by_ts.items(), key=lambda item: item[0])


def total_co2_grams(start: str, end: str, step: str) -> tuple[float, dict]:
    """
    Reconstruct total CO2 (grams) emitted over a time window, per node.

    Args:
        start: ISO 8601 start timestamp.
        end: ISO 8601 end timestamp.
        step: Resolution step (must match STEP above).

    Returns:
        (total_co2_grams, per_node_breakdown_dict)
    """
    step_seconds = int(step.rstrip("s"))

    watts_series = query_range("carbon_node_watts", start, end, step)
    intensity_series = query_range(
        "carbon_grid_intensity_current_g_per_kwh", start, end, step
    )

    intensity_by_ts = {}
    if intensity_series:
        for ts, val in intensity_series[0]["values"]:
            intensity_by_ts[ts] = float(val)

    per_node = {}
    total = 0.0
    for series in watts_series:
        node_name = series["metric"].get("node", "unknown")
        node_total = 0.0
        for ts, val in series["values"]:
            watts = float(val)
            ci = intensity_by_ts.get(ts)
            if ci is None:
                continue
            node_total += watts * ci / 3_600_000 * step_seconds
        per_node[node_name] = node_total
        total += node_total

    return total, per_node


def _instant_query(promql: str, at_time: str) -> list[dict]:
    """Run a Prometheus instant query at a given timestamp."""
    resp = requests.get(
        f"{PROM_URL}/api/v1/query",
        params={"query": promql, "time": at_time},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["data"]["result"]


def _iso_range_to_prometheus_duration(start: str, end: str) -> str:
    """Convert two ISO 8601 timestamps into a PromQL duration like '48h'."""
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    delta = datetime.strptime(end, fmt) - datetime.strptime(start, fmt)
    hours = int(delta.total_seconds() // 3600)
    return f"{hours}h"


def node_selection_distribution(start: str, end: str) -> dict[str, float]:
    """
    Count how many times each node was chosen during prioritization.

    Uses increase() over the window so the result is the number of
    selections during the run, not the raw counter value since scrape start.

    Args:
        start: ISO 8601 start timestamp.
        end: ISO 8601 end timestamp.

    Returns:
        Dict { node_name: selection_count }.
    """
    window = _iso_range_to_prometheus_duration(start, end)
    result = _instant_query(f"increase(carbon_node_selected_total[{window}])", end)

    distribution: dict[str, float] = {}
    for series in result:
        node = series["metric"].get("node", "unknown")
        distribution[node] = distribution.get(node, 0.0) + float(series["value"][1])
    return distribution


def gate_delay_by_class(start: str, end: str) -> dict[str, float | None]:
    """
    Average gating delay per workload class (carbon-aware run only).

    Lets you check that latency-sensitive workloads were barely (or never)
    delayed, while batch/best-effort absorbed most of the temporal shift —
    i.e. that the SLA-driven classification actually held during the run.

    Args:
        start: ISO 8601 start timestamp.
        end: ISO 8601 end timestamp.

    Returns:
        Dict { carbon_class: avg_delay_seconds_or_None }.
    """
    sum_result = _instant_query("carbon_gate_delay_duration_seconds_sum", end)
    count_result = _instant_query("carbon_gate_delay_duration_seconds_count", end)

    sums: dict[str, float] = {}
    for series in sum_result:
        cls = series["metric"].get("carbon_class", "unknown")
        sums[cls] = sums.get(cls, 0.0) + float(series["value"][1])

    counts: dict[str, float] = {}
    for series in count_result:
        cls = series["metric"].get("carbon_class", "unknown")
        counts[cls] = counts.get(cls, 0.0) + float(series["value"][1])

    return {
        cls: (sums[cls] / counts[cls] if counts.get(cls) else None) for cls in sums
    }


def avg_gate_delay_seconds(start: str, end: str) -> float | None:
    """
    Average temporal-shift delay applied by the Gate Controller.

    Only meaningful for the carbon-aware run (baseline has no gates).

    Returns:
        Average delay in seconds, or None if no gated pods in the window.
    """
    # Pull the raw histogram sum/count at the end of the window instead of a
    # rate, since this is a one-shot post-hoc analysis, not a live dashboard.
    resp = requests.get(
        f"{PROM_URL}/api/v1/query",
        params={
            "query": "carbon_gate_delay_duration_seconds_sum",
            "time": end,
        },
        timeout=30,
    )
    resp.raise_for_status()
    sum_result = resp.json()["data"]["result"]

    resp = requests.get(
        f"{PROM_URL}/api/v1/query",
        params={
            "query": "carbon_gate_delay_duration_seconds_count",
            "time": end,
        },
        timeout=30,
    )
    resp.raise_for_status()
    count_result = resp.json()["data"]["result"]

    if not sum_result or not count_result:
        return None

    total_sum = sum(float(r["value"][1]) for r in sum_result)
    total_count = sum(float(r["value"][1]) for r in count_result)
    return total_sum / total_count if total_count else None


def avg_grid_ci(start: str, end: str, step: str) -> float | None:
    """Plain time-averaged grid CI over the window (unweighted by decisions)."""
    series = query_range("carbon_grid_intensity_current_g_per_kwh", start, end, step)
    if not series:
        return None
    values = [float(v) for _, v in series[0]["values"]]
    return sum(values) / len(values) if values else None


def avg_ci_at_decision_by_class(start: str, end: str) -> dict[str, float | None]:
    """
    Average CI at the moment of schedule_now decisions, broken down by
    carbon class (Grafana panel 14/17, split instead of aggregated).

    Aggregating across classes dilutes the signal: latency-sensitive pods
    are always schedule_now regardless of CI (α=0, never gated), so mixing
    them with batch/best-effort — which only reach schedule_now after the
    gate controller releases them — pulls the average toward "whenever the
    grid happens to be" and understates the temporal-shifting effect.

    Compared per-class against avg_grid_ci() for the same window:
    - latency-sensitive is expected to land close to the reference CI
      (control group — proves the class is genuinely never gated)
    - batch/best-effort landing below the reference CI is direct evidence
      that temporal shifting placed them during cleaner windows

    Returns:
        Dict { carbon_class: avg_ci_g_per_kwh_or_None }.
    """
    window = _iso_range_to_prometheus_duration(start, end)
    sum_result = _instant_query(
        f'sum(increase(carbon_ci_at_decision_g_per_kwh_sum{{decision="schedule_now"}}[{window}])) '
        f"by (carbon_class)",
        end,
    )
    count_result = _instant_query(
        f'sum(increase(carbon_ci_at_decision_g_per_kwh_count{{decision="schedule_now"}}[{window}])) '
        f"by (carbon_class)",
        end,
    )

    sums: dict[str, float] = {
        r["metric"].get("carbon_class", "unknown"): float(r["value"][1]) for r in sum_result
    }
    counts: dict[str, float] = {
        r["metric"].get("carbon_class", "unknown"): float(r["value"][1]) for r in count_result
    }

    return {cls: (sums[cls] / counts[cls] if counts.get(cls) else None) for cls in sums}


def delay_rate_vs_ci(start: str, end: str, step: str = "10m") -> list[dict]:
    """
    Paired (CI, delay_rate) series for correlating grid intensity with the
    fraction of decisions that resulted in a delay, over 10-minute buckets.

    A working gating mechanism should show delay_rate rising alongside CI.
    Returned as a list of dicts rather than a single number, since the
    interesting result here is the shape of the relationship (Section 6.2.2),
    best shown as a scatter plot or reported as a correlation coefficient.

    Buckets where rate(delay)/rate(total) is a 0/0 division (no scoring
    decisions at all in that bucket) come back from Prometheus as NaN and
    are dropped here, rather than being passed through: a single NaN
    poisons every downstream sum in correlation_coefficient(), which is
    why that function used to silently return nan on real data.
    """
    ci_series = query_range("carbon_grid_intensity_current_g_per_kwh", start, end, step)
    delay_series = query_range(
        f'sum(rate(carbon_scheduling_decisions_total{{decision="delay"}}[{step}])) / '
        f"sum(rate(carbon_scheduling_decisions_total[{step}]))",
        start,
        end,
        step,
    )

    ci_by_ts = {ts: float(val) for ts, val in ci_series[0]["values"]} if ci_series else {}
    delay_by_ts = (
        {ts: float(val) for ts, val in delay_series[0]["values"]} if delay_series else {}
    )

    paired = []
    for ts in sorted(set(ci_by_ts) & set(delay_by_ts)):
        ci, delay_rate = ci_by_ts[ts], delay_by_ts[ts]
        if math.isnan(ci) or math.isnan(delay_rate):
            continue
        paired.append({"timestamp": ts, "ci": ci, "delay_rate": delay_rate})
    return paired


def correlation_coefficient(paired: list[dict]) -> float | None:
    """Pearson correlation between CI and delay_rate from delay_rate_vs_ci()."""
    if len(paired) < 2:
        return None
    xs = [p["ci"] for p in paired]
    ys = [p["delay_rate"] for p in paired]
    n = len(xs)
    mean_x, mean_y = sum(xs) / n, sum(ys) / n
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    var_x = sum((x - mean_x) ** 2 for x in xs)
    var_y = sum((y - mean_y) ** 2 for y in ys)
    denom = (var_x * var_y) ** 0.5
    # var_x or var_y can be exactly 0 if CI or delay_rate was constant across
    # every remaining bucket (e.g. a short window with no CI movement) —
    # guard explicitly rather than let a 0/0 produce nan silently.
    if not denom or math.isnan(denom):
        return None
    return cov / denom


def prioritize_latency_p95(start: str, end: str) -> float | None:
    """
    p95 of /prioritize endpoint response time (Grafana panel 25/26).

    Only meaningful for the carbon-aware run: the extender is not
    registered as a scheduler extension during the baseline run, so this
    metric has no data outside the carbon-aware window.
    """
    window = _iso_range_to_prometheus_duration(start, end)
    result = _instant_query(
        f"histogram_quantile(0.95, sum(rate(carbon_prioritize_duration_seconds_bucket[{window}])) by (le))",
        end,
    )
    if not result:
        return None
    return float(result[0]["value"][1])


def signal_staleness_pct(start: str, end: str, step: str, threshold_s: int = 600) -> float | None:
    """
    Share of the run (in %) during which the carbon signal exceeded the
    staleness threshold (MAX_SIGNAL_AGE in signal_loader.py, default 600s).
    """
    series = query_range("carbon_signal_age_seconds", start, end, step)
    if not series:
        return None
    values = [float(v) for _, v in series[0]["values"]]
    if not values:
        return None
    stale = sum(1 for v in values if v > threshold_s)
    return stale / len(values) * 100


def realized_delay_gain(start: str, end: str) -> dict[str, float | None]:
    """
    p50/p90 of the CI gain actually achieved by delaying flexible pods
    (carbon_delay_gain_g_per_kwh), i.e. Grafana panel 20 as a one-shot value.
    """
    window = _iso_range_to_prometheus_duration(start, end)
    result = {}
    for q in (0.5, 0.9):
        res = _instant_query(
            f"histogram_quantile({q}, sum(rate(carbon_delay_gain_g_per_kwh_bucket[{window}])) by (le))",
            end,
        )
        result[f"p{int(q * 100)}"] = float(res[0]["value"][1]) if res else None
    return result


def scheduled_task_count(start: str, end: str) -> tuple[float | None, str]:
    """
    Total pods scheduled over the window, using kube-scheduler's own native
    metric rather than carbon_scheduling_decisions_total. This is the
    sanity check from Section 6.1: it must be equal between baseline and
    carbon-aware runs, and unlike the carbon_* metrics, it is populated in
    BOTH runs since it comes from kube-scheduler itself, not the extender
    (which isn't registered as an extension point during baseline).

    Returns (count, diagnosis). If count is None, diagnosis distinguishes
    two failure modes that look identical from the query result alone but
    have different fixes:
      - "metric_absent": scheduler_schedule_attempts_total has no series at
        all in Prometheus. kube-scheduler exposes metrics on its own secure
        port (10259, authenticated) — if no ServiceMonitor/scrape config
        targets it (only extender-servicemonitor.yaml exists in this
        project's manifests), the metric is simply never collected.
      - "window_empty": the metric exists but returned no samples for this
        specific time range — check BASELINE_START/END and
        CARBON_AWARE_START/END against the actual run windows.
    """
    window = _iso_range_to_prometheus_duration(start, end)
    result = _instant_query(
        f'sum(increase(scheduler_schedule_attempts_total{{result="scheduled"}}[{window}]))', end
    )
    if result:
        return float(result[0]["value"][1]), "ok"

    exists = _instant_query("scheduler_schedule_attempts_total", end)
    return None, ("window_empty" if exists else "metric_absent")


def expected_task_count(start: str, end: str) -> int:
    """
    Analytical fallback for the sanity check, independent of whether
    kube-scheduler's own metrics are being scraped at all.

    Computes the deterministic number of task pods the three benchmark
    DAGs (Section 6.1) should have produced over the window, from their
    fixed schedule_interval alone: data_quality_monitor (3 tasks / 15 min),
    etl_sales_pipeline (3 tasks / 30 min), log_archive_cleanup
    (3 tasks / 2 h). This does not account for retries or failed runs, so
    it is an expected count to compare against, not a ground truth.
    """
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    window_minutes = (datetime.strptime(end, fmt) - datetime.strptime(start, fmt)).total_seconds() / 60

    dag_task_counts = {
        "data_quality_monitor": (15, 3),
        "etl_sales_pipeline": (30, 3),
        "log_archive_cleanup": (120, 3),
    }
    total = 0
    for _dag, (interval_min, n_tasks) in dag_task_counts.items():
        runs = int(window_minutes // interval_min)
        total += runs * n_tasks
    return total


def plot_cumulative_co2(out_path: str = "results/cumulative_co2.png"):
    """
    Plot cumulative CO2 over time, baseline vs carbon-aware, overlaid.

    Saves a PNG (matplotlib required: pip install matplotlib).
    """
    import matplotlib.pyplot as plt

    baseline_series = co2_step_series(BASELINE_START, BASELINE_END, STEP)
    carbon_series = co2_step_series(CARBON_AWARE_START, CARBON_AWARE_END, STEP)

    def cumulative(series):
        # Use relative hours since run start on the x-axis so the two runs
        # overlay even though they happened on different calendar days.
        hours, running_total, total = [], [], 0.0
        for i, (_, step_g) in enumerate(series):
            total += step_g
            hours.append(i * int(STEP.rstrip("s")) / 3600)
            running_total.append(total)
        return hours, running_total

    baseline_hours, baseline_cum = cumulative(baseline_series)
    carbon_hours, carbon_cum = cumulative(carbon_series)

    plt.figure(figsize=(9, 5))
    plt.plot(baseline_hours, baseline_cum, label="Baseline (default-scheduler)")
    plt.plot(carbon_hours, carbon_cum, label="Carbon-aware (extender)")
    plt.xlabel("Time since run start (hours)")
    plt.ylabel("Cumulative CO2 (g)")
    plt.title("Cumulative CO2 emissions: baseline vs carbon-aware")
    plt.legend()
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    print(f"Saved cumulative CO2 plot to {out_path}")


def main():
    """Run all analyses and print a comparison summary."""
    print("Querying baseline window...")
    baseline_co2, baseline_per_node = total_co2_grams(
        BASELINE_START, BASELINE_END, STEP
    )
    baseline_task_count, baseline_diag = scheduled_task_count(BASELINE_START, BASELINE_END)

    print("Querying carbon-aware window...")
    carbon_co2, carbon_per_node = total_co2_grams(
        CARBON_AWARE_START, CARBON_AWARE_END, STEP
    )
    carbon_task_count, carbon_diag = scheduled_task_count(CARBON_AWARE_START, CARBON_AWARE_END)

    baseline_expected = expected_task_count(BASELINE_START, BASELINE_END)
    carbon_expected = expected_task_count(CARBON_AWARE_START, CARBON_AWARE_END)

    reduction_pct = (
        (baseline_co2 - carbon_co2) / baseline_co2 * 100 if baseline_co2 else 0.0
    )

    avg_delay = avg_gate_delay_seconds(CARBON_AWARE_START, CARBON_AWARE_END)
    delay_by_class = gate_delay_by_class(CARBON_AWARE_START, CARBON_AWARE_END)
    node_distribution = node_selection_distribution(
        CARBON_AWARE_START, CARBON_AWARE_END
    )

    # --- Newly added metrics (Sections 6.2.1 to 6.2.4) ---
    baseline_avg_ci = avg_grid_ci(BASELINE_START, BASELINE_END, STEP)
    ref_ci = avg_grid_ci(CARBON_AWARE_START, CARBON_AWARE_END, STEP)
    ci_window_diff = (
        (ref_ci - baseline_avg_ci) if (ref_ci is not None and baseline_avg_ci is not None) else None
    )
    decision_ci_by_class = avg_ci_at_decision_by_class(CARBON_AWARE_START, CARBON_AWARE_END)
    ci_gap_by_class = {
        cls: (ref_ci - v) if (ref_ci is not None and v is not None) else None
        for cls, v in decision_ci_by_class.items()
    }

    delay_vs_ci = delay_rate_vs_ci(CARBON_AWARE_START, CARBON_AWARE_END)
    delay_ci_corr = correlation_coefficient(delay_vs_ci)

    latency_p95 = prioritize_latency_p95(CARBON_AWARE_START, CARBON_AWARE_END)
    staleness_pct = signal_staleness_pct(CARBON_AWARE_START, CARBON_AWARE_END, STEP)
    delay_gain = realized_delay_gain(CARBON_AWARE_START, CARBON_AWARE_END)

    print("\n=== Benchmark comparison ===")
    print(f"Baseline total CO2:      {baseline_co2:.2f} g")
    print(f"Carbon-aware total CO2:  {carbon_co2:.2f} g")
    print(f"Reduction:               {reduction_pct:.2f} %")
    if avg_delay is not None:
        print(f"Average gate delay:      {avg_delay:.0f} s")
    else:
        print("Average gate delay:      no gated pods in this window")

    print("\n=== Sanity check: task count (should match) ===")
    print(f"Baseline tasks scheduled:     {baseline_task_count} (metric: {baseline_diag}, expected ~{baseline_expected})")
    print(f"Carbon-aware tasks scheduled: {carbon_task_count} (metric: {carbon_diag}, expected ~{carbon_expected})")
    if baseline_diag == "metric_absent" or carbon_diag == "metric_absent":
        print(
            "  [WARN] scheduler_schedule_attempts_total has no series in Prometheus at all. "
            "kube-scheduler exposes metrics on its own secure port (10259); check that a "
            "ServiceMonitor/scrape config actually targets it — only extender-servicemonitor.yaml "
            "was found among this project's manifests, which covers the extender, not kube-scheduler."
        )
    elif baseline_diag == "window_empty" or carbon_diag == "window_empty":
        print(
            "  [WARN] The metric exists but returned nothing for this window — "
            "double check BASELINE_START/END and CARBON_AWARE_START/END against the actual run times."
        )

    print("\n=== Grid condition comparability (temporal confound check) ===")
    print(f"Baseline window avg CI:        {baseline_avg_ci:.1f} gCO2/kWh" if baseline_avg_ci else "n/a")
    print(f"Carbon-aware window avg CI:    {ref_ci:.1f} gCO2/kWh" if ref_ci else "n/a")
    print(
        f"Difference (aware - baseline): {ci_window_diff:+.1f} gCO2/kWh"
        if ci_window_diff is not None
        else "n/a"
    )

    print("\n=== Mechanism validation (6.2.2) ===")
    print(f"Reference grid CI (avg):       {ref_ci:.1f} gCO2/kWh" if ref_ci else "n/a")
    print("CI at decision by class:")
    for cls, val in sorted(decision_ci_by_class.items()):
        print(f"  {cls}: {val:.1f} gCO2/kWh" if val is not None else f"  {cls}: n/a")
    print("Gap vs reference by class (mechanism benefit):")
    for cls, gap in sorted(ci_gap_by_class.items()):
        print(f"  {cls}: {gap:.1f} gCO2/kWh" if gap is not None else f"  {cls}: n/a")
    print(f"Delay rate ↔ CI correlation:   {delay_ci_corr:.2f}" if delay_ci_corr else "n/a")

    print("\n=== Cost of the mechanism (6.2.3) ===")
    print(f"/prioritize p95 latency:       {latency_p95 * 1000:.1f} ms" if latency_p95 else "n/a")

    print("\n=== Gate delay by workload class ===")
    for cls, delay in delay_by_class.items():
        label = f"{delay:.0f} s" if delay is not None else "n/a"
        print(f"  {cls}: {label}")

    print("\n=== Node selection distribution (carbon-aware) ===")
    for node, count in sorted(node_distribution.items()):
        print(f"  {node}: {count:.0f} selections")

    print("\n=== Data quality controls (6.2.4) ===")
    print(f"Signal staleness (>600s):      {staleness_pct:.2f} %" if staleness_pct is not None else "n/a")
    print(f"Realized delay gain p50/p90:   {delay_gain}")

    results = {
        "generated_at": datetime.now(UTC).isoformat(),
        "baseline": {
            "window": [BASELINE_START, BASELINE_END],
            "total_co2_g": baseline_co2,
            "per_node_g": baseline_per_node,
            "tasks_scheduled": baseline_task_count,
            "tasks_scheduled_diagnosis": baseline_diag,
            "tasks_scheduled_expected": baseline_expected,
        },
        "carbon_aware": {
            "window": [CARBON_AWARE_START, CARBON_AWARE_END],
            "total_co2_g": carbon_co2,
            "per_node_g": carbon_per_node,
            "tasks_scheduled": carbon_task_count,
            "tasks_scheduled_diagnosis": carbon_diag,
            "tasks_scheduled_expected": carbon_expected,
            "avg_gate_delay_s": avg_delay,
            "gate_delay_by_class_s": delay_by_class,
            "node_selection_distribution": node_distribution,
            "reference_grid_ci_avg": ref_ci,
            "ci_at_decision_by_class": decision_ci_by_class,
            "ci_gap_by_class": ci_gap_by_class,
            "delay_rate_ci_correlation": delay_ci_corr,
            "delay_rate_vs_ci_series": delay_vs_ci,
            "prioritize_latency_p95_s": latency_p95,
            "signal_staleness_pct": staleness_pct,
            "realized_delay_gain_g_per_kwh": delay_gain,
        },
        "grid_condition_comparability": {
            "baseline_window_avg_ci": baseline_avg_ci,
            "carbon_aware_window_avg_ci": ref_ci,
            "difference_g_per_kwh": ci_window_diff,
        },
        "reduction_pct": reduction_pct,
    }

    out_path = "results/benchmark_results.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved detailed results to {out_path}")

    try:
        plot_cumulative_co2()
    except ImportError:
        print("[INFO] matplotlib not installed, skipping the cumulative CO2 plot")


if __name__ == "__main__":
    main()