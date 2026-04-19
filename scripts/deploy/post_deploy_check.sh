#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TIMEZONE="${HELIOS_TIMEZONE:-${TZ:-Asia/Kolkata}}"
API_URL="${HELIOS_API_URL:-http://localhost:8080}"
ADMIN_TOKEN="${HELIOS_ADMIN_TOKEN:-change-me-admin-token}"
PLANNER_URL="${HELIOS_CHECK_PLANNER_URL:-http://localhost:8090}"
PROMETHEUS_URL="${HELIOS_CHECK_PROMETHEUS_URL:-http://localhost:9090}"
RUN_SMOKE="${HELIOS_POSTCHECK_SMOKE:-true}"
RUN_AI_DRY_RUN="${HELIOS_SMOKE_AI_DRY_RUN:-false}"
CHECK_PROMETHEUS="${HELIOS_POSTCHECK_PROMETHEUS:-true}"
CHECK_PLANNER="${HELIOS_POSTCHECK_PLANNER:-true}"

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

json_file_get() {
  python3 - "$1" "$2" <<'PY'
import json
import sys

path = sys.argv[1].split(".")
with open(sys.argv[2], "r", encoding="utf-8") as handle:
    value = json.load(handle)
for part in path:
    value = value[part]
print(value)
PY
}

curl_json() {
  local label="$1"
  local url="$2"
  local output="$3"
  shift 3
  echo "request: $url"
  curl -fsS "$url" "$@" >"$output"
  pass "$label"
}

cd "$ROOT_DIR"

echo "Helios post-deployment checks"
echo "workspace=$ROOT_DIR"
echo "timezone=$TIMEZONE"
echo "timestamp=$(TZ="$TIMEZONE" date +%Y-%m-%dT%H:%M:%S%z)"
echo "api_url=$API_URL"

section "Tooling"
require curl
require python3

section "Control Plane"
curl_json "control-plane readiness endpoint returned success" "$API_URL/readyz" /tmp/helios-post-ready.json
ready_status="$(json_file_get status /tmp/helios-post-ready.json)"
echo "ready_status=$ready_status"
[[ "$ready_status" == "ok" ]] || fail "control-plane readiness status was $ready_status"
pass "control-plane readiness status is ok"

section "Workers"
curl_json "worker registry returned success" "$API_URL/api/v1/workers" /tmp/helios-post-workers.json \
  -H "authorization: Bearer ${ADMIN_TOKEN}"
python3 - /tmp/helios-post-workers.json <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    workers = json.load(handle)
healthy = [worker for worker in workers if worker.get("health") == "healthy"]
dead = [worker for worker in workers if worker.get("health") == "dead"]
print(f"registered_workers={len(workers)}")
print(f"healthy_workers={len(healthy)}")
print(f"dead_workers={len(dead)}")
if not healthy:
    raise SystemExit("no healthy workers registered")
PY
pass "at least one healthy worker is registered"

section "AI Planner"
if [[ "$CHECK_PLANNER" == "true" ]]; then
  curl_json "planner health endpoint returned success" "$PLANNER_URL/healthz" /tmp/helios-post-planner.json
  planner_backend="$(json_file_get active_backend /tmp/helios-post-planner.json)"
  gemini_configured="$(json_file_get gemini_configured /tmp/helios-post-planner.json)"
  echo "planner_backend=$planner_backend"
  echo "gemini_configured=$gemini_configured"
  [[ "$planner_backend" == "gemini" ]] || fail "planner active backend is $planner_backend, expected gemini"
  pass "planner is using Gemini backend"
else
  skip "planner health check disabled via HELIOS_POSTCHECK_PLANNER=false"
fi

section "Prometheus"
if [[ "$CHECK_PROMETHEUS" == "true" ]]; then
  curl_json "Prometheus rules API returned success" "$PROMETHEUS_URL/api/v1/rules" /tmp/helios-post-prom-rules.json
  python3 - /tmp/helios-post-prom-rules.json <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    body = json.load(handle)
groups = body.get("data", {}).get("groups", [])
names = sorted(group.get("name", "") for group in groups)
alert_count = sum(len(group.get("rules", [])) for group in groups)
print(f"prometheus_rule_groups={names}")
print(f"prometheus_alert_count={alert_count}")
required = {"helios-control-plane", "helios-ai-planner"}
missing = required.difference(names)
if missing:
    raise SystemExit(f"missing Prometheus rule groups: {sorted(missing)}")
if alert_count < 10:
    raise SystemExit(f"expected at least 10 alert rules, got {alert_count}")
PY
  pass "Prometheus loaded Helios alert rules"
else
  skip "Prometheus check disabled via HELIOS_POSTCHECK_PROMETHEUS=false"
fi

section "Workflow Smoke Test"
if [[ "$RUN_SMOKE" == "true" ]]; then
  echo "running smoke test with HELIOS_SMOKE_AI_DRY_RUN=$RUN_AI_DRY_RUN"
  HELIOS_API_URL="$API_URL" \
    HELIOS_ADMIN_TOKEN="$ADMIN_TOKEN" \
    HELIOS_SMOKE_AI_DRY_RUN="$RUN_AI_DRY_RUN" \
    "$ROOT_DIR/scripts/deploy/smoke_test.sh"
  pass "workflow smoke test passed"
else
  skip "smoke test disabled via HELIOS_POSTCHECK_SMOKE=false"
fi

section "Result"
echo "post_deploy_checks=passed"
echo "passed_checks=$pass_count"
echo "skipped_checks=$skip_count"
