# Architecture

Helios consists of four primary planes:

- Control plane: REST API, scheduler, retry/timeout recovery, worker registry
- Durable state: PostgreSQL as the source of truth
- Dispatch plane: NATS for task assignment transport
- Execution plane: trusted Go workers with built-in handlers

The planner service is intentionally outside the correctness path. It can suggest DAGs and optimization hints, but runtime correctness still depends only on persisted state, leases, and worker/task contracts.

## Component Responsibilities

- API server: submission, inspection, cancellation, worker lifecycle endpoints
- Scheduler: scan ready tasks, pick healthy workers, create leases, dispatch assignments
- Recovery loop: promote retryable tasks, recover dead-worker or expired-lease attempts
- Workers: register, heartbeat, execute handlers, report start/result/failure
- Planner: turn intent into DAG suggestions and optimization advice

## Storage Model

- `workflows`: workflow metadata and overall state
- `tasks`: task state, retry config, payloads, lease and attempt metadata
- `task_dependencies`: DAG edges
- `task_attempts`: per-attempt execution history
- `task_results`: attempt-level result records
- `task_state_history`: explicit state transition history
- `workers`: worker registry and auth metadata
- `task_events`: audit/event stream for inspection
