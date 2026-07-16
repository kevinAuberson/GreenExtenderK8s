#!/bin/bash
# Deploy Airflow for the carbon-aware scheduler benchmark.
# Run from the repo root: bash experiments/benchmark/airflow/manifests/install.sh [baseline|carbon]
#
# baseline (default): workers use default-scheduler, no scheduling gates
# carbon:             all pods use carbon-aware scheduler + gates on batch/best-effort

set -e

PHASE="${1:-baseline}"
if [[ "$PHASE" != "baseline" && "$PHASE" != "carbon" ]]; then
  echo "Usage: $0 [baseline|carbon]"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

echo "==> Benchmark phase: $PHASE"

echo "==> Cleaning up previous install..."
helm uninstall airflow -n airflow 2>/dev/null || true
kubectl delete pvc --all -n airflow 2>/dev/null || true
kubectl delete pods --all -n airflow --force 2>/dev/null || true

echo "==> Creating namespace..."
kubectl create namespace airflow 2>/dev/null || true

echo "==> Deploying PostgreSQL..."
kubectl apply -f "$SCRIPT_DIR/postgres.yaml"
echo "Waiting for PostgreSQL to be ready..."
kubectl wait --for=condition=ready pod -l app=postgres -n airflow --timeout=120s

echo "==> Installing Airflow via Helm..."
helm repo add apache-airflow https://airflow.apache.org 2>/dev/null || true
helm repo update
helm install airflow apache-airflow/airflow \
  -n airflow \
  -f "$REPO_ROOT/experiments/benchmark/airflow/values-airflow.yaml" \
  --version 1.16.0 \
  --set env[0].value="$PHASE" \
  --timeout 10m

echo "==> Waiting for Airflow to be ready..."
kubectl wait --for=condition=ready pod -l component=webserver -n airflow --timeout=300s

if [[ "$PHASE" == "carbon" ]]; then
  echo "==> Patching Airflow deployments to use carbon-aware scheduler..."
  for deploy in airflow-webserver airflow-scheduler airflow-statsd; do
    kubectl patch deployment "$deploy" -n airflow \
      --type=json \
      -p='[{"op":"add","path":"/spec/template/spec/schedulerName","value":"carbon-aware"}]'
    echo "    Patched $deploy"
  done
  echo "Waiting for patched pods to be ready..."
  kubectl rollout status deployment/airflow-webserver -n airflow --timeout=120s
  kubectl rollout status deployment/airflow-scheduler -n airflow --timeout=120s
fi

echo ""
echo "==> Airflow is ready! (phase=$PHASE)"
echo "    UI:    http://$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[0].address}'):30104"
echo "    Login: admin / admin"
echo ""
echo "    To switch phase later:"
echo "      baseline → carbon: bash experiments/benchmark/airflow/manifests/install.sh carbon"
echo "      carbon → baseline: bash experiments/benchmark/airflow/manifests/install.sh baseline"
