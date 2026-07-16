#!/usr/bin/env python3
"""
File:        compute_thresholds.py
Author:      Kevin Auberson
Created:     2026-06-10
Description: Generates the carbon-signal-thresholds ConfigMap from an
             ElectricityMaps hourly CSV export (lifecycle CI column).
             Computes P25/P75 per month and outputs a ready-to-apply YAML.

Generates a carbon-signal-thresholds ConfigMap from an ElectricityMaps
hourly CSV export (lifecycle carbon intensity column).

Usage:
    python scripts/compute_thresholds.py <csv_file> [--zone CH] [--output <path>]

Examples:
    # Print to stdout
    python scripts/compute_thresholds.py docs/snapshots_2026-02-10_CH-2025-hourly.csv

    # Write directly to the manifest
    python scripts/compute_thresholds.py docs/snapshots_*_DE-*.csv --zone DE \\
        --output manifests/aggregator/aggregator-thresholds-configmap.yaml

The CSV must be an ElectricityMaps hourly export with columns:
    "Datetime (UTC)" and "Carbon intensity gCO2eq/kWh (Life cycle)"

Download your zone's data at: https://www.electricitymaps.com/data-portal
"""

import argparse
import csv
import sys
from collections import defaultdict
from datetime import date

MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def percentile(values: list[float], p: int) -> float:
    values = sorted(values)
    idx = int(len(values) * p / 100)
    return round(values[min(idx, len(values) - 1)], 1)


def load_csv(path: str) -> dict[int, list[float]]:
    by_month: dict[int, list[float]] = defaultdict(list)
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                ci = float(row["Carbon intensity gCO₂eq/kWh (Life cycle)"])
                month = int(row["Datetime (UTC)"][5:7])
                by_month[month].append(ci)
            except (ValueError, KeyError):
                continue
    return by_month


def generate_configmap(by_month: dict, zone: str) -> str:
    today = date.today().isoformat()
    total = sum(len(v) for v in by_month.values())

    lines = [
        "apiVersion: v1",
        "kind: ConfigMap",
        "metadata:",
        "  name: carbon-signal-thresholds",
        "  namespace: carbon-scheduler",
        "data:",
        "  thresholds.yaml: |",
        f"    # Monthly carbon intensity thresholds for zone: {zone} (lifecycle gCO2eq/kWh)",
        f"    # Source: ElectricityMaps hourly export — {total} data points",
        f"    # Generated: {today}",
        "    # Method: green = P25, dirty = P75 of hourly lifecycle CI per month",
        f"    # Regenerate: python scripts/compute_thresholds.py <csv> --zone {zone} --output manifests/aggregator/aggregator-thresholds-configmap.yaml",
        "    thresholds:",
    ]

    for m in range(1, 13):
        values = by_month.get(m, [])
        if not values:
            lines.append(f"      {m:>2}: {{ green: null, dirty: null }}  # {MONTH_NAMES[m-1]} — no data")
            continue
        p25 = percentile(values, 25)
        p75 = percentile(values, 75)
        lines.append(
            f"      {m:>2}: {{ green: {p25:>5}, dirty: {p75:>6} }}"
            f"  # {MONTH_NAMES[m-1]} (n={len(values)}, P50={percentile(values,50)}, P90={percentile(values,90)})"
        )

    return "\n".join(lines) + "\n"


def print_stats(by_month: dict, zone: str) -> None:
    all_values = [ci for values in by_month.values() for ci in values]
    print(f"\nZone: {zone} — {len(all_values)} hourly points", file=sys.stderr)
    print(f"{'':>5} {'P10':>6} {'P25':>6} {'P50':>6} {'P75':>6} {'P90':>6} {'Min':>6} {'Max':>6}", file=sys.stderr)
    print("-" * 55, file=sys.stderr)
    for m in range(1, 13):
        v = by_month.get(m, [])
        if not v:
            continue
        print(
            f"{MONTH_NAMES[m-1][:3]:>5}"
            f" {percentile(v,10):>6} {percentile(v,25):>6} {percentile(v,50):>6}"
            f" {percentile(v,75):>6} {percentile(v,90):>6}"
            f" {min(v):>6.1f} {max(v):>6.1f}",
            file=sys.stderr,
        )
    print("-" * 55, file=sys.stderr)
    print(
        f"{'ALL':>5}"
        f" {percentile(all_values,10):>6} {percentile(all_values,25):>6} {percentile(all_values,50):>6}"
        f" {percentile(all_values,75):>6} {percentile(all_values,90):>6}"
        f" {min(all_values):>6.1f} {max(all_values):>6.1f}",
        file=sys.stderr,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate carbon-signal-thresholds ConfigMap from ElectricityMaps CSV"
    )
    parser.add_argument("csv_file", help="Path to the ElectricityMaps hourly CSV export")
    parser.add_argument("--zone", default="CH", help="Zone ID (e.g. CH, DE, FR). Default: CH")
    parser.add_argument("--output", default="-", help="Output file path or '-' for stdout")
    args = parser.parse_args()

    by_month = load_csv(args.csv_file)
    if not by_month:
        print(f"[ERROR] No valid data found in {args.csv_file}", file=sys.stderr)
        sys.exit(1)

    print_stats(by_month, args.zone)
    configmap = generate_configmap(by_month, args.zone)

    if args.output == "-":
        print(configmap)
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(configmap)
        print(f"\nWritten to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
