# Helios v1 Design Doc

## Purpose

Helios v1 is a production-minded distributed task orchestration control plane for static DAG workloads. The v1 goal is correctness first: durable state, explicit failure semantics, lease-based scheduling, trusted workers, retries, timeout recovery, and operational visibility.

## Frozen v1 Decisions

- Execution semantics: at-least-once
- Control plane topology: single active control plane
- Durable source of truth: PostgreSQL
- Dispatch transport: NATS
- Worker execution model: trusted built-in task handlers only
- Workflow model: static DAG submitted upfront
- Scheduler policy: least-loaded healthy eligible worker
- Failure recovery: heartbeat + lease expiry + retry policy

## Supported Workflow Model

- Static DAG only
- No cycles, loops, nested workflows, or dynamic DAG mutation
- Tasks define `task_id`, `task_type`, dependencies, timeout, retry policy, and input payload

## Task Lifecycle

- `pending`
- `ready`
- `leased`
- `running`
- `retry_wait`
- `succeeded`
- `failed`
- `timed_out`
- `cancelled`

## Workflow Lifecycle

- `submitted`
- `running`
- `succeeded`
- `failed`
- `cancelled`

## Worker Model

- Workers register on startup
- Workers advertise supported task types and capacity
- Workers heartbeat periodically
- Workers execute trusted built-in task handlers only
- Workers report start, success, and failure with attempt awareness

## Failure Contract

- Worker crash or dead heartbeat leads to lease expiry and recovery
- Control-plane restart is tolerated because durable state lives in PostgreSQL
- Timeout and duplicate delivery are expected under at-least-once semantics
- Late or stale worker reports must not corrupt the latest task state

## Out of Scope

- Exactly-once execution
- Active-active control plane
- Consensus or leader election
- Dynamic DAG expansion
- Arbitrary user code execution
- Rich UI authoring studio
- GPU-aware placement
- Tenant fairness engine

## Why v1 Matters

Helios v1 demonstrates control-plane design, durable state machines, scheduling, failure recovery, worker coordination, and operational thinking in one coherent system.
