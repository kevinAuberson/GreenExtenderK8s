# benchmark

Real-cluster comparison of the default Kubernetes scheduler (baseline)
against the carbon-aware extender, over two 48h windows on the same
5-node vSphere cluster. CO2 is reconstructed from raw watts + grid
intensity identically for both runs, so the comparison stays fair even
though the baseline never goes through the extender's own
`carbon_node_co2_g_per_s` metric.

## Contents

- `airflow/` — KubernetesExecutor setup generating the synthetic
  workload: one DAG per carbon class (`latency-sensitive`, `batch`,
  `best-effort`), each simulating a realistic production pattern
  (data-quality checks, ETL, log cleanup) instead of a trivial no-op.
- `benchmark_analysis.py` — queries Prometheus for both windows, computes
  total CO2, gate delay, node selection distribution, and the data
  quality controls from report section 6.2.4.
- `results/benchmark_results.json` — raw output of the last run.

## Running the benchmark

1. **Deploy Airflow and generate load**, from the repo root:

   ```bash
   bash experiments/benchmark/airflow/manifests/install.sh baseline   # window 1
   #  ... let it run for ~48h, then tear down and switch phase ...
   bash experiments/benchmark/airflow/manifests/install.sh carbon     # window 2
   ```

   `install.sh` deploys PostgreSQL + Airflow via Helm, and in `carbon`
   phase patches the Airflow deployments to use the `carbon-aware`
   scheduler. See the script for details (namespace, UI credentials).

2. **Record the exact start/end timestamps** of each window (UTC, ISO
   8601) — Airflow's scheduling isn't tied to your analysis window, so
   these must be read off `kubectl` / Airflow UI at deploy/teardown time.

3. **Configure `benchmark_analysis.py`**: edit the constants at the top
   of the file —

   ```python
   PROM_URL = "http://<host>:<port>"   # port-forward to your Prometheus
   BASELINE_START = "..."
   BASELINE_END = "..."
   CARBON_AWARE_START = "..."
   CARBON_AWARE_END = "..."
   ```

4. **Run it**:

   ```bash
   pip install requests matplotlib   # matplotlib optional, for the CO2 plot
   python experiments/benchmark/benchmark_analysis.py
   ```

   Writes `results/benchmark_results.json` and, if matplotlib is
   installed, `results/cumulative_co2.png` (paths relative to the
   script's own directory).
