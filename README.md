# GreenExtenderK8s

Carbon-aware scheduling extender for Kubernetes. A scheduler extender defers
or places pods based on real-time and forecast grid carbon intensity, using a
custom carbon signal aggregator as the data source.

![Architecture overview](docs/architecture.png)

The system runs two independent cycles: a **collection cycle** 
where the Carbon Signal Aggregator continuously polls vSphere,
metrics-server, and Electricity Maps every 30-300s and publishes the
result to a ConfigMap; and a **service cycle** triggered on each pod 
scheduling event, where the extender reads that ConfigMap to answer 
the scheduler's `/prioritize` calls, and exposes `/metrics` for observability.

## Components

**Deployable system:**

| Path | What it is |
|---|---|
| `extender/` | The scheduler extender service (FastAPI/uvicorn). Scores and gates pod placement based on carbon signal, workload class, and node telemetry. See [extender/README.md](extender/README.md) for configuration. |
| `carbon-signal/` | The carbon signal aggregator. Combines Electricity Maps grid intensity with vSphere power telemetry and Kubernetes metrics-server node load, and exposes it for the extender. See [carbon-signal/README.md](carbon-signal/README.md) for configuration. |
| `manifests/` | Kubernetes manifests (deployments, RBAC, configmaps, service monitors) for the aggregator and the extender. |
| `monitoring/` | Grafana dashboard definition for visualizing carbon signal, scoring, and gating behavior. |
| `scripts/compute_thresholds.py` | Generates the carbon-intensity threshold ConfigMap (`manifests/aggregator/aggregator-thresholds-configmap.yaml`) from historical grid data. |

**Evaluation / experiments** (evidence behind the thesis results, not needed to run the system) — see [experiments/README.md](experiments/README.md):

| Path | What it is |
|---|---|
| `experiments/benchmark/` | The real-cluster benchmark: `airflow/` (KubernetesExecutor setup generating synthetic `latency-sensitive`/`batch`/`best-effort` workloads), `benchmark_analysis.py` (baseline vs. carbon-aware comparison from Prometheus data), `results/benchmark_results.json` (raw output). See [experiments/benchmark/README.md](experiments/benchmark/README.md). |
| `experiments/portability/` | Offline simulation evaluating portability to other grid zones using public trace data: `portability_simulation.py`, `traces/`, `results/portability_results.json`. Not part of the deployed system — a standalone analysis. See [experiments/portability/README.md](experiments/portability/README.md). |
| `docs/` | Architecture diagram, plus reference datasets (the `*.csv` exports are gitignored — regenerate locally, see `scripts/compute_thresholds.py`). |

Figures generated from the `results/` data are included directly in the
thesis report and not duplicated in this repository.

## CI/CD

Each service has its own pipeline (`.github/workflows/build-aggregator.yaml`,
`build-extender.yaml`): lint (ruff), security scan (bandit), unit tests
(pytest), Kubernetes manifest validation (kubeconform/yamllint), then build
and push a container image to GHCR, followed by an automated image-tag bump
in the corresponding manifest.

## Running locally

Each service is independently containerized:

```bash
cd carbon-signal && docker build -t carbon-signal .
cd extender && docker build -t extender .
```

See each service's `requirements-dev.txt` for local dev/test dependencies,
and its `README.md` / `.env.example` for configuration.

## Deploying to a cluster

The extender doesn't replace the default Kubernetes scheduler — it runs
alongside it as a second scheduler (`carbon-aware`) that pods opt into.

### Prerequisites

Beyond a Kubernetes cluster and `kubectl` access, the following must
already be installed — none of them ship with this repo:

- **metrics-server** — provides the per-node CPU/RAM load the aggregator
  reads via the `metrics.k8s.io` API. Each of the aggregator's three data
  sources is queried independently and failures don't crash it, so the
  system still runs without metrics-server, but node scoring becomes
  CPU/RAM-blind (`SCORING_W_CPU`/`SCORING_W_MEM` have nothing to weight).
- **Prometheus**, ideally via the **Prometheus Operator** — required to
  actually scrape the extender's `/metrics` endpoint (step 6 below) and
  to run `experiments/benchmark/benchmark_analysis.py`, which queries
  Prometheus directly (`PROM_URL`).
- **Grafana** — optional, only needed to import `monitoring/grafana-dashboard.json`.
- Network access to a **vCenter** instance and an **Electricity Maps**
  API token (see [carbon-signal/README.md](carbon-signal/README.md#environment-variables)).

### Install order

1. **Namespace**

   ```bash
   kubectl apply -f manifests/namespace.yaml
   ```

2. **Image pull secret** (GHCR images are private by default) and **app
   secrets** — see [carbon-signal/README.md](carbon-signal/README.md#environment-variables)
   for what each value means:

   ```bash
   kubectl create secret docker-registry ghcr-auth \
     --docker-server=ghcr.io \
     --docker-username=<your-github-username> \
     --docker-password=<a GitHub PAT with read:packages> \
     -n carbon-scheduler

   kubectl create secret generic carbon-signal-secrets -n carbon-scheduler \
     --from-literal=EMAPS_TOKEN=<your-electricitymaps-token> \
     --from-literal=VCENTER_HOST=<your-vcenter-host> \
     --from-literal=VCENTER_USER=<your-vcenter-user> \
     --from-literal=VCENTER_PASSWORD=<your-vcenter-password>
   ```

3. **RBAC**

   ```bash
   kubectl apply -f manifests/aggregator/aggregator-rbac.yaml
   kubectl apply -f manifests/extender/extender-rbac.yaml
   kubectl apply -f manifests/scheduler-rbac.yaml
   ```

4. **ConfigMaps** — `aggregator-configmap.yaml` maps Kubernetes node
   names to vCenter host names for *this* cluster; edit it for yours.
   `aggregator-thresholds-configmap.yaml` is checked in as-is; regenerate
   it for a different grid zone or period with `scripts/compute_thresholds.py`
   (see the script's docstring). `scheduler-configmap.yaml` registers the
   `carbon-aware` scheduler profile and points it at the extender's
   `/filter` and `/prioritize` endpoints.

   ```bash
   kubectl apply -f manifests/aggregator/aggregator-configmap.yaml
   kubectl apply -f manifests/aggregator/aggregator-thresholds-configmap.yaml
   kubectl apply -f manifests/scheduler-configmap.yaml
   ```

5. **Aggregator** — deploy first so the `carbon-signal` ConfigMap it
   publishes exists before the extender reads it (the extender fails
   safe and schedules immediately if the signal is missing or stale, so
   this isn't a hard ordering requirement, just the sane one):

   ```bash
   kubectl apply -f manifests/aggregator/aggregator-deployment.yaml
   kubectl get configmap carbon-signal -n carbon-scheduler -o yaml   # verify it's populated
   ```

6. **Extender**

   ```bash
   kubectl apply -f manifests/extender/extender-deployment.yaml
   kubectl apply -f manifests/extender/extender-service.yaml
   # registers the /metrics endpoint with the Prometheus Operator's CRD-based
   # discovery; skip if your Prometheus uses static scrape config instead:
   kubectl apply -f manifests/extender/extender-servicemonitor.yaml
   ```

7. **The carbon-aware scheduler itself**

   ```bash
   kubectl apply -f manifests/scheduler-deployment.yaml
   ```

8. **Use it** — set `schedulerName: carbon-aware` on a pod spec. See
   [extender/README.md#usage](extender/README.md#usage) for the label/annotation
   reference and a full example.

Optionally, import `monitoring/grafana-dashboard.json` into Grafana to
visualize carbon signal, scoring, and gating behavior.
