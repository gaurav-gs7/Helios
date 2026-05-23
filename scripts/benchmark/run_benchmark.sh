#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
TOKEN="${TOKEN:-${HELIOS_ADMIN_TOKEN:-change-me-admin-token}}"
WORKFLOW_FILE="${WORKFLOW_FILE:-examples/workflow.json}"
COUNT="${COUNT:-100}"
BENCHMARK_COUNTS="${BENCHMARK_COUNTS:-}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-1800}"
POLL_SECONDS="${POLL_SECONDS:-2}"
RESULT_DIR="${RESULT_DIR:-docs/benchmarks/results}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
RESULT_FILE="${RESULT_FILE:-}"
PROGRESS_EVERY="${PROGRESS_EVERY:-25}"
SUBMIT_RETRIES="${SUBMIT_RETRIES:-12}"
SUBMIT_RETRY_SLEEP_SECONDS="${SUBMIT_RETRY_SLEEP_SECONDS:-5}"

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

submit_workflow() {
  local submit_file="$1"
  local attempt=1
  local status

  while true; do
    status="$(
      curl -sS -o "${submit_file}" -w "%{http_code}" -X POST "${API_URL}/api/v1/workflows" \
        -H "Authorization: Bearer ${TOKEN}" \
        -H "Content-Type: application/json" \
        --data @"${WORKFLOW_FILE}" || true
    )"
    if [[ "${status}" == "202" ]]; then
      return 0
    fi
    if [[ "${status}" == "429" && "${attempt}" -lt "${SUBMIT_RETRIES}" ]]; then
      echo "submission_rate_limited=attempt:${attempt};sleep:${SUBMIT_RETRY_SLEEP_SECONDS}s" >&2
      sleep "${SUBMIT_RETRY_SLEEP_SECONDS}"
      attempt=$((attempt + 1))
      continue
    fi
    echo "workflow submission failed with HTTP ${status}" >&2
    cat "${submit_file}" >&2 || true
    return 1
  done
}

run_single_benchmark() {
  local count="$1"
  local run_id="$2"
  local result_file="$3"
  local tmp_dir manifest_file worker_sample_dir submit_started_ms submit_finished_ms finished_ms

  tmp_dir="$(mktemp -d)"
  manifest_file="${tmp_dir}/manifest.tsv"
  worker_sample_dir="${tmp_dir}/worker-samples"
  mkdir -p "${worker_sample_dir}"
  : >"${manifest_file}"

  echo "benchmark_run_id=${run_id}"
  echo "api_url=${API_URL}"
  echo "workflow_file=${WORKFLOW_FILE}"
  echo "workflow_count=${count}"
  echo "result_file=${result_file}"

  curl -fsS "${API_URL}/readyz" >/dev/null

  submit_started_ms="$(now_ms)"
  workflow_refs=()
  for index in $(seq 1 "${count}"); do
    submit_file="${tmp_dir}/submit-${index}.json"
    submitted_ms="$(now_ms)"
    submit_workflow "${submit_file}"
    workflow_id="$(json_get workflow_id "${submit_file}")"
    workflow_refs+=("${workflow_id}=${submitted_ms}")
    if ((index == 1 || index == count || index % PROGRESS_EVERY == 0)); then
      echo "submitted_workflows=${index}/${count}"
    fi
  done
  submit_finished_ms="$(now_ms)"

  deadline=$((SECONDS + TIMEOUT_SECONDS))
  pending=("${workflow_refs[@]}")
  poll_index=0
  while ((${#pending[@]} > 0)); do
    if [[ "${SECONDS}" -ge "${deadline}" ]]; then
      echo "benchmark timed out waiting for ${#pending[@]} workflow(s)" >&2
      exit 1
    fi

    poll_index=$((poll_index + 1))
    curl -fsS "${API_URL}/api/v1/workers" \
      -H "Authorization: Bearer ${TOKEN}" >"${worker_sample_dir}/workers-${poll_index}.json"

    next_pending=()
    completed_this_poll=0
    for ref in "${pending[@]}"; do
      workflow_id="${ref%%=*}"
      submitted_ms="${ref#*=}"
      workflow_summary_file="${tmp_dir}/workflow-${workflow_id}.json"
      curl -fsS "${API_URL}/api/v1/workflows/${workflow_id}" \
        -H "Authorization: Bearer ${TOKEN}" >"${workflow_summary_file}"
      state="$(json_get state "${workflow_summary_file}")"
      case "${state}" in
        succeeded|failed|cancelled)
          terminal_ms="$(now_ms)"
          workflow_tasks_file="${tmp_dir}/tasks-${workflow_id}.json"
          workflow_events_file="${tmp_dir}/events-${workflow_id}.json"
          curl -fsS "${API_URL}/api/v1/workflows/${workflow_id}/tasks" \
            -H "Authorization: Bearer ${TOKEN}" >"${workflow_tasks_file}"
          curl -fsS "${API_URL}/api/v1/workflows/${workflow_id}/events" \
            -H "Authorization: Bearer ${TOKEN}" >"${workflow_events_file}"
          printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
            "${workflow_id}" "${submitted_ms}" "${terminal_ms}" \
            "${workflow_summary_file}" "${workflow_tasks_file}" "${workflow_events_file}" >>"${manifest_file}"
          completed_this_poll=$((completed_this_poll + 1))
          ;;
        *)
          next_pending+=("${ref}")
          ;;
      esac
    done
    pending=("${next_pending[@]}")
    echo "poll=${poll_index} completed=${completed_this_poll} pending=${#pending[@]}"
    if ((${#pending[@]} > 0)); then
      sleep "${POLL_SECONDS}"
    fi
  done

  finished_ms="$(now_ms)"
  python3 scripts/benchmark/summarize_benchmark.py \
    --result-file "${result_file}" \
    --api-url "${API_URL}" \
    --workflow-file "${WORKFLOW_FILE}" \
    --workflow-count "${count}" \
    --submit-started-ms "${submit_started_ms}" \
    --submit-finished-ms "${submit_finished_ms}" \
    --finished-ms "${finished_ms}" \
    --manifest-file "${manifest_file}" \
    --worker-sample-dir "${worker_sample_dir}"

  rm -rf "${tmp_dir}"
}

require curl
require python3

if [[ -z "${TOKEN}" ]]; then
  echo "TOKEN or HELIOS_ADMIN_TOKEN is required" >&2
  exit 1
fi

mkdir -p "${RESULT_DIR}"

if [[ -n "${BENCHMARK_COUNTS}" ]]; then
  for count in ${BENCHMARK_COUNTS}; do
    run_single_benchmark "${count}" "${RUN_ID}-${count}w" "${RESULT_DIR}/benchmark-${RUN_ID}-${count}w.json"
  done
else
  run_single_benchmark "${COUNT}" "${RUN_ID}" "${RESULT_FILE:-${RESULT_DIR}/benchmark-${RUN_ID}.json}"
fi
