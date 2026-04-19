#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${HELIOS_K8S_NAMESPACE:-helios}"

kubectl rollout undo deployment/control-plane -n "${NAMESPACE}"
kubectl rollout undo deployment/worker -n "${NAMESPACE}"
kubectl rollout undo deployment/planner -n "${NAMESPACE}"

kubectl rollout status deployment/control-plane -n "${NAMESPACE}" --timeout=180s
kubectl rollout status deployment/worker -n "${NAMESPACE}" --timeout=180s
kubectl rollout status deployment/planner -n "${NAMESPACE}" --timeout=180s
