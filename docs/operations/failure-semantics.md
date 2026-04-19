# Failure Semantics

## Core Guarantee

Helios v1 provides at-least-once execution, not exactly-once execution.

## Failure Cases

### Worker crash mid-task

- Heartbeats stop
- Worker becomes `stale` then `dead`
- Active lease expires
- Recovery loop reschedules or terminally fails the task based on retry policy

### Task timeout

- The worker-side execution context times out
- The control plane records failure or recovery state
- Retry budget decides retry vs terminal failure

### Late result report

- Worker/task/attempt identity is validated
- Stale or mismatched attempt reports are rejected

### Control-plane restart

- Durable state remains in PostgreSQL
- Scheduler and recovery loops resume after restart
- Ready and recoverable work is rediscovered

## Operational Tradeoffs

- Single active control plane reduces correctness complexity but limits HA
- Trusted built-in handlers reduce sandboxing/security complexity
- Planner suggestions never mutate correctness-critical paths automatically
