#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
TOKEN="${TOKEN:-${HELIOS_ADMIN_TOKEN:-change-me-admin-token}}"
WORKFLOW_FILE="${WORKFLOW_FILE:-examples/workflow.json}"
COUNT="${COUNT:-10}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-180}"
POLL_SECONDS="${POLL_SECONDS:-2}"
RESULT_DIR="${RESULT_DIR:-docs/benchmarks/results}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
RESULT_FILE="${RESULT_FILE:-${RESULT_DIR}/benchmark-${RUN_ID}.json}"

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

now_ms() {
  python3 -c 'import time; print(time.time_ns() // 1_000_000)'
}

json_get() {
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

json_summary() {
  python3 - "$@" <<'PY'
import json
import math
import statistics
import sys

result_file = sys.argv[1]
api_url = sys.argv[2]
workflow_file = sys.argv[3]
count = int(sys.argv[4])
submit_started_ms = int(sys.argv[5])
submit_finished_ms = int(sys.argv[6])
finished_ms = int(sys.argv[7])
workflow_files = sys.argv[8:]

latencies = []
states = {}
total_tasks = 0
total_attempts = 0
retry_tasks = 0
failed_workflows = 0
succeeded_workflows = 0

for item in workflow_files:
    workflow_id, submitted_ms, path = item.split("=", 2)
    submitted_ms = int(submitted_ms)
    with open(path, "r", encoding="utf-8") as handle:
        workflow = json.load(handle)
    state = workflow.get("state", "unknown")
    states[state] = states.get(state, 0) + 1
    if state == "succeeded":
        succeeded_workflows += 1
    if state in {"failed", "cancelled"}:
        failed_workflows += 1
    latency_ms = max(0, finished_ms - submitted_ms)
    latencies.append(latency_ms)
    tasks = workflow.get("tasks", [])
    total_tasks += len(tasks)
    for task in tasks:
        attempts = int(task.get("attempt", 0) or 0)
        total_attempts += attempts
        if attempts > 1:
            retry_tasks += 1

def percentile(values, pct):
    if not values:
        return 0
    values = sorted(values)
    index = math.ceil((pct / 100) * len(values)) - 1
    return values[max(0, min(index, len(values) - 1))]

submit_elapsed = max(1, submit_finished_ms - submit_started_ms)
total_elapsed = max(1, finished_ms - submit_started_ms)
summary = {
    "run_id": result_file.rsplit("/", 1)[-1].replace("benchmark-", "").replace(".json", ""),
    "api_url": api_url,
    "workflow_file": workflow_file,
    "submitted_workflows": count,
    "succeeded_workflows": succeeded_workflows,
    "failed_workflows": failed_workflows,
    "workflow_states": states,
    "total_tasks": total_tasks,
    "total_task_attempts": total_attempts,
    "retry_tasks": retry_tasks,
    "submission_elapsed_ms": submit_elapsed,
    "total_elapsed_ms": total_elapsed,
    "submission_rate_workflows_per_min": round(count * 60000 / submit_elapsed, 2),
    "completion_rate_workflows_per_min": round(succeeded_workflows * 60000 / total_elapsed, 2),
    "task_completion_rate_tasks_per_sec": round(total_tasks * 1000 / total_elapsed, 2),
    "attempt_rate_attempts_per_sec": round(total_attempts * 1000 / total_elapsed, 2),
    "workflow_latency_ms": {
        "avg": round(statistics.mean(latencies), 2) if latencies else 0,
        "p50": percentile(latencies, 50),
        "p95": percentile(latencies, 95),
        "max": max(latencies) if latencies else 0,
    },
}

with open(result_file, "w", encoding="utf-8") as handle:
    json.dump(summary, handle, indent=2, sort_keys=True)
    handle.write("\n")

for key, value in summary.items():
    if isinstance(value, (dict, list)):
        print(f"{key}={json.dumps(value, sort_keys=True)}")
    else:
        print(f"{key}={value}")
PY
}

require curl
require python3

if [[ -z "${TOKEN}" ]]; then
  echo "TOKEN or HELIOS_ADMIN_TOKEN is required" >&2
  exit 1
fi

mkdir -p "${RESULT_DIR}"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

echo "benchmark_run_id=${RUN_ID}"
echo "api_url=${API_URL}"
echo "workflow_file=${WORKFLOW_FILE}"
echo "workflow_count=${COUNT}"

curl -fsS "${API_URL}/readyz" >/dev/null

submit_started_ms="$(now_ms)"
workflow_refs=()
for index in $(seq 1 "${COUNT}"); do
  submit_file="${tmp_dir}/submit-${index}.json"
  submitted_ms="$(now_ms)"
  curl -fsS -X POST "${API_URL}/api/v1/workflows" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Content-Type: application/json" \
    --data @"${WORKFLOW_FILE}" >"${submit_file}"
  workflow_id="$(json_get workflow_id "${submit_file}")"
  workflow_refs+=("${workflow_id}=${submitted_ms}")
  echo "submitted_workflow=${workflow_id}"
done
submit_finished_ms="$(now_ms)"

deadline=$((SECONDS + TIMEOUT_SECONDS))
pending=("${workflow_refs[@]}")
finished_files=()
while ((${#pending[@]} > 0)); do
  if [[ "${SECONDS}" -ge "${deadline}" ]]; then
    echo "benchmark timed out waiting for ${#pending[@]} workflow(s)" >&2
    exit 1
  fi

  next_pending=()
  for ref in "${pending[@]}"; do
    workflow_id="${ref%%=*}"
    submitted_ms="${ref#*=}"
    workflow_file="${tmp_dir}/workflow-${workflow_id}.json"
    curl -fsS "${API_URL}/api/v1/workflows/${workflow_id}" \
      -H "Authorization: Bearer ${TOKEN}" >"${workflow_file}"
    state="$(json_get state "${workflow_file}")"
    case "${state}" in
      succeeded|failed|cancelled)
        finished_files+=("${workflow_id}=${submitted_ms}=${workflow_file}")
        echo "terminal_workflow=${workflow_id}:${state}"
        ;;
      *)
        next_pending+=("${ref}")
        ;;
    esac
  done
  pending=()
  if ((${#next_pending[@]} > 0)); then
    pending=("${next_pending[@]}")
  fi
  if ((${#pending[@]} > 0)); then
    sleep "${POLL_SECONDS}"
  fi
done

finished_ms="$(now_ms)"
json_summary "${RESULT_FILE}" "${API_URL}" "${WORKFLOW_FILE}" "${COUNT}" "${submit_started_ms}" "${submit_finished_ms}" "${finished_ms}" "${finished_files[@]}"
echo "result_file=${RESULT_FILE}"
