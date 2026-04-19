#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TIMEZONE="${HELIOS_TIMEZONE:-${TZ:-Asia/Kolkata}}"
RUN_GO_TESTS="${HELIOS_PRECHECK_GO_TESTS:-true}"
RUN_PYTHON_COMPILE="${HELIOS_PRECHECK_PYTHON_COMPILE:-true}"
RUN_PROMTOOL="${HELIOS_PRECHECK_PROMTOOL:-true}"
KUSTOMIZE_OVERLAYS="${HELIOS_PRECHECK_OVERLAYS:-base dev prod-like}"

pass_count=0
skip_count=0

section() {
  echo
  echo "== $1 =="
}

pass() {
  pass_count=$((pass_count + 1))
  echo "[PASS] $1"
}

skip() {
  skip_count=$((skip_count + 1))
  echo "[SKIP] $1"
}

fail() {
  echo "[FAIL] $1" >&2
  exit 1
}

require() {
  if command -v "$1" >/dev/null 2>&1; then
    pass "found command: $1"
  else
    fail "missing required command: $1"
  fi
}

run_check() {
  local label="$1"
  shift
  echo "running: $*"
  "$@"
  pass "$label"
}

render_kustomize() {
  local overlay="$1"
  local path
  local output
  if [[ "$overlay" == "base" ]]; then
    path="deploy/k8s/base"
  else
    path="deploy/k8s/overlays/$overlay"
  fi
  output="/tmp/helios-kustomize-${overlay}.yaml"
  [[ -d "$ROOT_DIR/$path" ]] || fail "kustomize path not found: $path"
  echo "running: kubectl kustomize $path > $output"
  kubectl kustomize "$ROOT_DIR/$path" >"$output"
  pass "rendered Kubernetes manifest: $path"
  echo "rendered_output=$output"
}

cd "$ROOT_DIR"

echo "Helios pre-deployment checks"
echo "workspace=$ROOT_DIR"
echo "timezone=$TIMEZONE"
echo "timestamp=$(TZ="$TIMEZONE" date +%Y-%m-%dT%H:%M:%S%z)"

section "Tooling"
require go
require python3
require docker
require kubectl

section "Source Checks"
if [[ "$RUN_GO_TESTS" == "true" ]]; then
  run_check "Go tests passed" go test ./...
else
  skip "Go tests disabled via HELIOS_PRECHECK_GO_TESTS=false"
fi

if [[ "$RUN_PYTHON_COMPILE" == "true" ]]; then
  run_check "planner Python compile passed" python3 -m py_compile planner/main.py
else
  skip "Python compile disabled via HELIOS_PRECHECK_PYTHON_COMPILE=false"
fi

section "Deployment Manifests"
run_check "Docker Compose config rendered" docker compose -f deploy/compose.yaml config

for overlay in $KUSTOMIZE_OVERLAYS; do
  render_kustomize "$overlay"
done

section "Prometheus Rules"
if [[ "$RUN_PROMTOOL" == "true" ]]; then
  run_check "Prometheus config and alerts are valid" \
    docker run --rm --entrypoint promtool \
    -v "$ROOT_DIR/deploy:/etc/prometheus:ro" \
    prom/prometheus:v2.54.1 \
    check config /etc/prometheus/prometheus.yml
else
  skip "promtool validation disabled via HELIOS_PRECHECK_PROMTOOL=false"
fi

section "Result"
echo "pre_deploy_checks=passed"
echo "passed_checks=$pass_count"
echo "skipped_checks=$skip_count"
