#!/usr/bin/env bash
set -euo pipefail

API_URL="${HELIOS_API_URL:-http://localhost:8080}"
ADMIN_TOKEN="${HELIOS_ADMIN_TOKEN:-change-me-admin-token}"
WORKFLOW_FILE="${HELIOS_SMOKE_WORKFLOW_FILE:-examples/workflow.json}"
TIMEOUT_SECONDS="${HELIOS_SMOKE_TIMEOUT_SECONDS:-90}"
POLL_SECONDS="${HELIOS_SMOKE_POLL_SECONDS:-3}"
RUN_AI_DRY_RUN="${HELIOS_SMOKE_AI_DRY_RUN:-false}"
INTENT_FILE="${HELIOS_SMOKE_INTENT_FILE:-examples/intent_request.json}"

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

json_get() {
  python3 - "$1" "$2" <<'PY'
import json
import sys

path = sys.argv[1].split(".")
with open(sys.argv[2], "r", encoding="utf-8") as handle:
    body = json.load(handle)
value = body
for part in path:
    value = value[part]
print(value)
PY
}

require curl
require python3

echo "checking control-plane readiness at ${API_URL}/readyz"
curl -fsS "${API_URL}/readyz" >/tmp/helios-ready.json

echo "checking worker registry"
curl -fsS "${API_URL}/api/v1/workers" \
  -H "authorization: Bearer ${ADMIN_TOKEN}" >/tmp/helios-workers.json
python3 - /tmp/helios-workers.json <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    workers = json.load(handle)
healthy = [worker for worker in workers if worker.get("health") == "healthy"]
if not healthy:
    raise SystemExit("no healthy Helios workers registered")
print(f"healthy_workers={len(healthy)}")
PY

if [ "${RUN_AI_DRY_RUN}" = "true" ]; then
  echo "checking Gemini-backed planner dry-run"
  curl -fsS "${API_URL}/api/v1/planner/dry-run" \
    -H "authorization: Bearer ${ADMIN_TOKEN}" \
    -H "content-type: application/json" \
    -d @"${INTENT_FILE}" >/tmp/helios-dry-run.json
  python3 - /tmp/helios-dry-run.json <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    body = json.load(handle)
if body.get("valid") is not True:
    raise SystemExit(f"planner dry-run was not valid: {body}")
print("planner_dry_run=valid")
PY
fi

echo "submitting smoke workflow from ${WORKFLOW_FILE}"
curl -fsS "${API_URL}/api/v1/workflows" \
  -H "authorization: Bearer ${ADMIN_TOKEN}" \
  -H "content-type: application/json" \
  -d @"${WORKFLOW_FILE}" >/tmp/helios-submit.json
WORKFLOW_ID="$(json_get workflow_id /tmp/helios-submit.json)"
echo "workflow_id=${WORKFLOW_ID}"

deadline=$((SECONDS + TIMEOUT_SECONDS))
state=""
while [ "$SECONDS" -lt "$deadline" ]; do
  curl -fsS "${API_URL}/api/v1/workflows/${WORKFLOW_ID}" \
    -H "authorization: Bearer ${ADMIN_TOKEN}" >/tmp/helios-workflow.json
  state="$(json_get state /tmp/helios-workflow.json)"
  echo "workflow_state=${state}"
  case "$state" in
    succeeded)
      echo "smoke_test=passed"
      exit 0
      ;;
    failed|cancelled)
      echo "workflow reached terminal failure state: ${state}" >&2
      cat /tmp/helios-workflow.json >&2
      exit 1
      ;;
  esac
  sleep "${POLL_SECONDS}"
done

echo "timed out waiting for workflow ${WORKFLOW_ID}; last_state=${state}" >&2
cat /tmp/helios-workflow.json >&2
exit 1
