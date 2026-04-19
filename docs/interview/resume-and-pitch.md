# Resume and Interview Packaging

## Resume Bullets

- Built Helios, a Go-based distributed task orchestration control plane that executes DAG workflows with durable PostgreSQL state, NATS task dispatch, lease-based scheduling, retries, timeout recovery, and worker heartbeats.
- Implemented a trusted worker runtime in Go with authenticated worker registration, per-worker task-result reporting, built-in execution handlers, and liveness-driven dead-worker recovery.
- Designed explicit task and workflow state machines, audit/event history, versioned SQL migrations, Prometheus metrics, and hardened API middleware including auth, readiness checks, body limits, and rate limiting.
- Developed a Python FastAPI planner service that converts high-level workload intent into executable DAG specs and scheduling hints using deterministic heuristics with optional Gemini structured-output integration, demonstrating AI-native workflow orchestration design.

## 90-Second Pitch

Helios is an AI-native distributed orchestration engine for DAG workflows. I built it as a control plane that accepts a static workflow, persists it durably in PostgreSQL, schedules ready tasks onto healthy workers using leases, and recovers correctly from timeouts or worker failure. The worker side is a trusted runtime that heartbeats, executes known handlers, and reports results with attempt awareness. I also added auth, readiness checks, versioned migrations, metrics, Docker/Kubernetes packaging, and a planner service that can either use deterministic heuristics or optional Gemini structured output, so it reads like a real internal platform service rather than a toy scheduler.

## 5-Minute Walkthrough

1. Submit a workflow DAG through the control-plane API.
2. Show DAG validation and initial task state assignment.
3. Walk through the scheduler leasing a ready task and dispatching it through NATS.
4. Show the worker acknowledging start, executing a built-in handler, and reporting success or retryable failure.
5. Explain heartbeat-driven worker health and lease-expiry recovery.
6. Show `/metrics`, readiness, docs, and deployment assets.

## Hardest Engineering Challenges

- keeping the task lifecycle explicit and auditable
- making retry and recovery logic deterministic
- validating worker/task/attempt identity so stale results cannot corrupt state
- balancing local laptop ergonomics with production-style service boundaries

## Tradeoffs I Made

- single active control plane instead of distributed leadership
- at-least-once execution instead of exactly-once complexity
- trusted handlers only instead of arbitrary code execution
- planner suggestions outside the correctness path

## What I Would Build Next

- active-passive or leader-elected control plane
- richer integration and chaos testing in CI
- latency histograms and Grafana screenshots from benchmark runs
- multi-tenant RBAC and secret management integration
