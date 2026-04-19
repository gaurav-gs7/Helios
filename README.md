# Helios

Helios is an AI-native distributed task orchestration control plane for DAG-based workloads. This repository ships a production-minded v1 designed around correctness first: PostgreSQL-backed durable state, NATS task dispatch, lease-based scheduling, worker heartbeats, retry/backoff, timeout recovery, structured logs, Prometheus metrics, Grafana dashboards, Kubernetes manifests, benchmark/chaos assets, and a FastAPI planner service backed by Gemini structured-output planning.

## Architecture

```text
CLI / curl -> Control Plane API -> PostgreSQL (source of truth)
                             \-> NATS dispatch -> Worker Runtime
                              \-> Planner API (AI planning, RAG runbooks, failure analysis)
                              \-> /metrics for Prometheus
```

Supporting architecture artifacts live in:

- `docs/design/helios-v1-design.md`
- `docs/contracts/api-contract.md`
- `docs/architecture/architecture.md`
- `docs/architecture/diagrams.md`
- `docs/operations/state-machine.md`
- `docs/operations/failure-semantics.md`
- `docs/benchmarks/benchmark-report.md`
- `docs/interview/resume-and-pitch.md`

## What v1 supports

- Static DAG submission and validation
- Dependency-aware execution
- Least-loaded healthy-worker scheduling
- Worker registration and heartbeats
- At-least-once execution semantics
- Retry with exponential backoff
- Lease expiry and dead-worker recovery
- Workflow, task, and audit inspection through the API

## Trusted Workload Handlers

Workers execute trusted, built-in handlers rather than arbitrary user code. The demo handler set now models production-style platform workloads:

- `validate_records`: validates transaction batches and rejects malformed records
- `enrich_risk_features`: builds deterministic fraud/risk features
- `score_fraud_risk`: scores records and can simulate retryable model-serving failures
- `aggregate_risk_results`: aggregates score distributions and decisions
- `embed_text_batch`: creates deterministic local embeddings for AI pipeline demos
- `persist_artifact`: simulates idempotent artifact persistence with checksums

`failure_probe` remains available as a controlled reliability-test handler for retry and recovery demos.

## Local quickstart

1. Start the full stack:

```bash
make infra-up
```

2. Submit the sample workflow:

```bash
go run ./cmd/heliosctl submit -api http://localhost:8080 -file examples/workflow.json -token change-me-admin-token
```

3. Inspect workflows, current task state, and audit events:

```bash
export HELIOS_ADMIN_TOKEN=change-me-admin-token

go run ./cmd/heliosctl workflows list
go run ./cmd/heliosctl workflow show <workflow-id>
go run ./cmd/heliosctl workflow tasks <workflow-id>
go run ./cmd/heliosctl workflow events <workflow-id>
go run ./cmd/heliosctl workers list
```

4. Open observability:

```bash
open http://localhost:9090
open http://localhost:3000
```

## Native development loop

1. `cp .env.example .env`
2. `set -a && source .env && set +a`
3. `make infra-up`
4. `make run-control-plane`
5. In another terminal, `set -a && source .env && set +a && make run-worker`

## GitOps deployment automation

Helios includes a production-style GitOps path:

- `.github/workflows/ci.yml` validates Go, Python, Docker Compose, and Kustomize manifests.
- `.github/workflows/docker-publish.yml` builds and pushes `control-plane`, `worker`, and `planner` images to GHCR with immutable SHA tags.
- `.github/workflows/gitops-update-dev.yml` opens a PR that promotes the dev Kustomize overlay to a published image tag.
- `.github/workflows/deploy-smoke.yml` provides a manual dropdown for `docker-compose`, `kubernetes`, or `argocd` deployment targets, then runs rollout checks, smoke tests, and rollback where applicable.
- `deploy/k8s/overlays/dev` and `deploy/k8s/overlays/prod-like` provide environment-specific Kustomize overlays.
- `deploy/argocd` contains Argo CD `Application` manifests for continuous delivery and drift correction.

Render the Kubernetes manifests locally:

```bash
kubectl kustomize deploy/k8s/overlays/dev
kubectl kustomize deploy/k8s/overlays/prod-like
```

One-touch local deployment targets:

```bash
make compose-deploy
make k8s-deploy-dev
make argocd-bootstrap-dev
```

Or use the generic target selector:

```bash
make deploy DEPLOY_TARGET=docker-compose DEPLOY_OVERLAY=dev
make deploy DEPLOY_TARGET=kubernetes DEPLOY_OVERLAY=dev
make deploy DEPLOY_TARGET=argocd DEPLOY_OVERLAY=dev
```

Deployment target behavior:

- `docker-compose` builds and starts the local Compose stack, then runs post-deploy checks.
- `kubernetes` applies the selected Kustomize overlay directly with `kubectl apply -k`.
- `argocd` applies the Argo CD `Application`, then Argo CD owns Kubernetes synchronization from Git.

Deploy manually with Argo CD:

```bash
kubectl apply -f deploy/argocd/helios-dev-application.yaml
```

Run the post-deploy smoke test against a reachable control-plane API:

```bash
HELIOS_API_URL=http://localhost:8080 \
HELIOS_ADMIN_TOKEN=change-me-admin-token \
scripts/deploy/smoke_test.sh
```

Run deployment checks with terminal pass/fail output:

```bash
make pre-deploy-check
make post-deploy-check
```

The pre-deploy check validates tooling, Go tests, planner compilation, Docker Compose config, Kustomize rendering, and Prometheus alert rules before any rollout. The post-deploy check validates control-plane readiness, worker registration, Gemini planner health, Prometheus alert loading, and an end-to-end workflow smoke test.

To use the manual `deploy-smoke` GitHub Action, choose `docker-compose`, `kubernetes`, or `argocd` from the `deployment_target` dropdown. The Kubernetes and Argo CD paths require a `KUBE_CONFIG_B64` repository secret containing a base64-encoded kubeconfig for the target cluster. The Docker Compose path runs fully inside the GitHub Actions runner.

## AI planner and operations assistant

Generate a DAG from a higher-level intent:

```bash
curl -s http://localhost:8080/api/v1/planner/intent \
  -H 'content-type: application/json' \
  -d '{
    "name": "merchant-risk-pipeline",
    "intent": "fetch validate score aggregate persist"
  }' | jq
```

Dry-run an AI-generated workflow without submitting it. The control plane asks the planner for a DAG, validates it with the Go DAG validator, and returns graph analysis:

```bash
curl -s http://localhost:8080/api/v1/planner/dry-run \
  -H 'authorization: Bearer change-me-admin-token' \
  -H 'content-type: application/json' \
  -d '{
    "name": "merchant-risk-pipeline",
    "intent": "validate enrich score aggregate embed persist"
  }' | jq
```

Analyze a failed or degraded workflow with runbook retrieval:

```bash
curl -s http://localhost:8080/api/v1/workflows/<workflow-id>/ai/failure-analysis \
  -H 'authorization: Bearer change-me-admin-token' \
  -H 'content-type: application/json' \
  -d '{"runbook_query":"task timeout retry worker heartbeat"}' | jq
```

The failure analyzer retrieves local runbooks from `planner/runbooks`, sends the workflow snapshot and matched runbook excerpts to Gemini, classifies the incident, and proposes operator-reviewed recovery actions. It never mutates workflow state.

The same planner flow is available through the CLI:

```bash
go run ./cmd/heliosctl planner intent -file examples/intent_request.json
go run ./cmd/heliosctl planner dry-run -file examples/intent_request.json
```

## AI observability

The planner exposes `/metrics` and Prometheus scrapes it as `helios-planner`. Useful metrics include:

- `helios_planner_requests_total`
- `helios_genai_requests_total`
- `helios_genai_token_estimate_total`
- `helios_genai_latency_seconds`
- `helios_rag_retrievals_total`

## Key endpoints

- `POST /api/v1/workflows`
- `GET /api/v1/workflows/{workflowID}`
- `GET /api/v1/workflows/{workflowID}/tasks`
- `GET /api/v1/tasks/{taskID}`
- `POST /api/v1/workflows/{workflowID}/cancel`
- `POST /api/v1/workers/register`
- `POST /api/v1/workers/{workerID}/heartbeat`
- `GET /api/v1/workers`
- `POST /api/v1/planner/intent`
- `POST /api/v1/planner/dry-run`
- `POST /api/v1/workflows/{workflowID}/ai/failure-analysis`
- `GET /healthz`
- `GET /readyz`
- `GET /metrics`

## Design notes

- PostgreSQL is the only durable source of truth.
- NATS is transport only and can be replayed or lost without corrupting control-plane state.
- Execution semantics are intentionally at-least-once, so task handlers should be idempotent.
- The scheduler stays deterministic; planner output never mutates correctness-critical paths on the fly.
- Worker registration requires a bootstrap token and returns a unique per-worker token used for heartbeat and task-state mutations.
- Admin APIs require a bearer token; this keeps workflow mutation and inspection endpoints off-limits to unauthenticated callers.

## Benchmarks and chaos drills

- Benchmark driver: `scripts/benchmark/run_benchmark.sh`
- Benchmark report with local MacBook Air M2 results: `docs/benchmarks/benchmark-report.md`
- Chaos drills: `scripts/chaos/kill_worker.sh`, `scripts/chaos/restart_control_plane.sh`, `scripts/chaos/pause_heartbeats.md`
- Prometheus alert rules: `deploy/alerts/helios-alerts.yml`
- Kubernetes base manifests: `deploy/k8s/base`
