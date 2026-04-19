# NATS Dispatch Issue

## Signal

Ready tasks exist in PostgreSQL but assignments are not reaching workers, or scheduler dispatch logs show transport errors.

## Impact

NATS is the task transport, not the source of truth. Workflow and task state remain durable in PostgreSQL and can be rediscovered after transport recovery.

## Recovery

Check NATS health, scheduler logs, assignment metrics, and worker subscriptions. Restart NATS only after confirming the control plane can reconnect and ready tasks remain persisted.

## Prevention

Monitor scheduler assignment rate, ready queue depth, and NATS health endpoint status.
