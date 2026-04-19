#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${HELIOS_K8S_NAMESPACE:-helios}"
TIMEOUT="${HELIOS_ROLLOUT_TIMEOUT:-180s}"

kubectl rollout status deployment/control-plane -n "${NAMESPACE}" --timeout="${TIMEOUT}"
kubectl rollout status deployment/worker -n "${NAMESPACE}" --timeout="${TIMEOUT}"
kubectl rollout status deployment/planner -n "${NAMESPACE}" --timeout="${TIMEOUT}"

kubectl get pods -n "${NAMESPACE}" -o wide
