#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TIMEZONE="${HELIOS_TIMEZONE:-${TZ:-Asia/Kolkata}}"
TARGET="${HELIOS_DEPLOY_TARGET:-${1:-docker-compose}}"
OVERLAY="${HELIOS_DEPLOY_OVERLAY:-${2:-dev}}"
RUN_PRECHECK="${HELIOS_DEPLOY_PRECHECK:-true}"
RUN_POSTCHECK="${HELIOS_DEPLOY_POSTCHECK:-true}"
RUN_AI_DRY_RUN="${HELIOS_SMOKE_AI_DRY_RUN:-false}"
NAMESPACE="${HELIOS_K8S_NAMESPACE:-helios}"
COMPOSE_FILE="${HELIOS_COMPOSE_FILE:-deploy/compose.yaml}"
ENV_FILE="${HELIOS_ENV_FILE:-.env}"

CONTROL_PLANE_PORT_FORWARD_PID=""
PLANNER_PORT_FORWARD_PID=""

section() {
  echo
  echo "== $1 =="
}

pass() {
  echo "[PASS] $1"
}

fail() {
  echo "[FAIL] $1" >&2
  exit 1
}

cleanup() {
  if [[ -n "$CONTROL_PLANE_PORT_FORWARD_PID" ]] && kill -0 "$CONTROL_PLANE_PORT_FORWARD_PID" >/dev/null 2>&1; then
    kill "$CONTROL_PLANE_PORT_FORWARD_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$PLANNER_PORT_FORWARD_PID" ]] && kill -0 "$PLANNER_PORT_FORWARD_PID" >/dev/null 2>&1; then
    kill "$PLANNER_PORT_FORWARD_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

run_precheck() {
  if [[ "$RUN_PRECHECK" == "true" ]]; then
    section "Pre-Deployment Checks"
    "$ROOT_DIR/scripts/deploy/pre_deploy_check.sh"
  else
    echo "[SKIP] pre-deployment checks disabled via HELIOS_DEPLOY_PRECHECK=false"
  fi
}

run_postcheck_compose() {
  if [[ "$RUN_POSTCHECK" == "true" ]]; then
    section "Post-Deployment Checks"
    HELIOS_API_URL="${HELIOS_API_URL:-http://localhost:8080}" \
      HELIOS_CHECK_PLANNER_URL="${HELIOS_CHECK_PLANNER_URL:-http://localhost:8090}" \
      HELIOS_CHECK_PROMETHEUS_URL="${HELIOS_CHECK_PROMETHEUS_URL:-http://localhost:9090}" \
      HELIOS_SMOKE_AI_DRY_RUN="$RUN_AI_DRY_RUN" \
      HELIOS_POSTCHECK_PLANNER="${HELIOS_POSTCHECK_PLANNER:-true}" \
      HELIOS_POSTCHECK_PROMETHEUS="${HELIOS_POSTCHECK_PROMETHEUS:-true}" \
      HELIOS_POSTCHECK_SMOKE="${HELIOS_POSTCHECK_SMOKE:-true}" \
      "$ROOT_DIR/scripts/deploy/post_deploy_check.sh"
  else
    echo "[SKIP] post-deployment checks disabled via HELIOS_DEPLOY_POSTCHECK=false"
  fi
}

run_postcheck_kubernetes() {
  if [[ "$RUN_POSTCHECK" == "true" ]]; then
    section "Port Forward Services"
    kubectl port-forward -n "$NAMESPACE" svc/control-plane 8080:8080 >/tmp/helios-control-plane-port-forward.log 2>&1 &
    CONTROL_PLANE_PORT_FORWARD_PID="$!"
    echo "control_plane_port_forward_pid=$CONTROL_PLANE_PORT_FORWARD_PID"
    kubectl port-forward -n "$NAMESPACE" svc/planner 8090:8090 >/tmp/helios-planner-port-forward.log 2>&1 &
    PLANNER_PORT_FORWARD_PID="$!"
    echo "planner_port_forward_pid=$PLANNER_PORT_FORWARD_PID"
    sleep 5

    section "Post-Deployment Checks"
    HELIOS_API_URL="http://localhost:8080" \
      HELIOS_CHECK_PLANNER_URL="http://localhost:8090" \
      HELIOS_SMOKE_AI_DRY_RUN="$RUN_AI_DRY_RUN" \
      HELIOS_POSTCHECK_PLANNER="true" \
      HELIOS_POSTCHECK_PROMETHEUS="false" \
      HELIOS_POSTCHECK_SMOKE="${HELIOS_POSTCHECK_SMOKE:-true}" \
      "$ROOT_DIR/scripts/deploy/post_deploy_check.sh"
  else
    echo "[SKIP] post-deployment checks disabled via HELIOS_DEPLOY_POSTCHECK=false"
  fi
}

deploy_compose() {
  section "Docker Compose Deployment"
  if [[ -f "$ROOT_DIR/$ENV_FILE" ]]; then
    echo "running: docker compose --env-file $ENV_FILE -f $COMPOSE_FILE up -d --build"
    docker compose --env-file "$ROOT_DIR/$ENV_FILE" -f "$ROOT_DIR/$COMPOSE_FILE" up -d --build
  else
    echo "[WARN] $ENV_FILE not found; using current shell environment for Compose variable substitution"
    echo "running: docker compose -f $COMPOSE_FILE up -d --build"
    docker compose -f "$ROOT_DIR/$COMPOSE_FILE" up -d --build
  fi
  pass "Docker Compose deployment completed"
}

deploy_kubernetes() {
  section "Direct Kubernetes Deployment"
  local overlay_path="$ROOT_DIR/deploy/k8s/overlays/$OVERLAY"
  [[ -d "$overlay_path" ]] || fail "overlay not found: $overlay_path"
  echo "running: kubectl apply -k deploy/k8s/overlays/$OVERLAY"
  kubectl apply -k "$overlay_path"
  pass "Kustomize overlay applied: $OVERLAY"

  section "Rollout Status"
  HELIOS_K8S_NAMESPACE="$NAMESPACE" "$ROOT_DIR/scripts/deploy/wait_for_rollout.sh"
}

deploy_argocd() {
  section "Argo CD GitOps Bootstrap"
  local app_file
  case "$OVERLAY" in
    dev)
      app_file="$ROOT_DIR/deploy/argocd/helios-dev-application.yaml"
      ;;
    prod-like)
      app_file="$ROOT_DIR/deploy/argocd/helios-prod-like-application.yaml"
      ;;
    *)
      fail "unsupported Argo CD overlay: $OVERLAY"
      ;;
  esac
  [[ -f "$app_file" ]] || fail "Argo CD application manifest not found: $app_file"
  echo "running: kubectl apply -f ${app_file#$ROOT_DIR/}"
  kubectl apply -f "$app_file"
  pass "Argo CD Application applied for overlay: $OVERLAY"

  section "Rollout Status"
  echo "waiting for Argo CD to sync Kubernetes workloads managed by the Application"
  HELIOS_K8S_NAMESPACE="$NAMESPACE" "$ROOT_DIR/scripts/deploy/wait_for_rollout.sh"
}

cd "$ROOT_DIR"

echo "Helios selectable deployment"
echo "target=$TARGET"
echo "overlay=$OVERLAY"
echo "namespace=$NAMESPACE"
echo "timezone=$TIMEZONE"
echo "timestamp=$(TZ="$TIMEZONE" date +%Y-%m-%dT%H:%M:%S%z)"

case "$TARGET" in
  docker-compose|compose)
    run_precheck
    deploy_compose
    run_postcheck_compose
    ;;
  kubernetes|k8s)
    run_precheck
    deploy_kubernetes
    run_postcheck_kubernetes
    ;;
  argocd|argo-cd)
    run_precheck
    deploy_argocd
    run_postcheck_kubernetes
    ;;
  *)
    fail "unknown deployment target '$TARGET'; expected docker-compose, kubernetes, or argocd"
    ;;
esac

section "Result"
echo "deployment_target=$TARGET"
echo "deployment_overlay=$OVERLAY"
echo "deployment_status=passed"
