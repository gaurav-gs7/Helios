#!/usr/bin/env bash
set -euo pipefail

LOG_DIR="${HELIOS_LOG_DIR:-logs}"

mkdir -p "${LOG_DIR}"
touch "${LOG_DIR}/control-plane.log" "${LOG_DIR}/worker.log" "${LOG_DIR}/planner.log"

echo "streaming application logs from ${LOG_DIR}"
tail -F "${LOG_DIR}/control-plane.log" "${LOG_DIR}/worker.log" "${LOG_DIR}/planner.log"
