# API Contract

## Admin API

All admin endpoints require `Authorization: Bearer <HELIOS_ADMIN_TOKEN>`.

### `POST /api/v1/workflows`

- Submit a static DAG workflow
- Validates DAG shape and persists workflow/tasks transactionally

Request schema: [workflow-submit.schema.json](/Users/gauravgs7/Documents/Projects/Helios-AI/Test_Helios/docs/api/schemas/workflow-submit.schema.json)

### `GET /api/v1/workflows`

- Lists recent workflows
- Supports optional filters:
  - `state`: workflow state such as `running`, `succeeded`, `failed`, or `cancelled`
  - `limit`: max number of workflows to return, capped server-side

### `GET /api/v1/workflows/{workflowID}`

- Returns workflow-level metadata only:
  - workflow ID
  - name
  - current workflow state
  - labels and metadata
  - created/updated timestamps

Task state and audit events are intentionally exposed through separate endpoints so current state is not confused with historical state transitions.

### `GET /api/v1/workflows/{workflowID}/tasks`

- Returns current task records for one workflow
- Use this endpoint when checking whether tasks are currently `pending`, `ready`, `leased`, `running`, `retry_wait`, `succeeded`, `failed`, `timed_out`, or `cancelled`

### `GET /api/v1/workflows/{workflowID}/events`

- Returns recent workflow/task audit events for one workflow
- Events are immutable lifecycle transitions such as `ready -> leased`, `leased -> running`, and `running -> succeeded`
- Use this endpoint for debugging, incident timelines, and explaining retries/recovery

### `GET /api/v1/tasks/{taskID}`

- Returns one task record with dependency metadata

### `POST /api/v1/workflows/{workflowID}/cancel`

- Cancels all non-terminal tasks and marks the workflow cancelled

### `GET /api/v1/workers`

- Returns the worker registry view with health and running-task counts
- Includes scheduler resource telemetry:
  - CPU load
  - memory used/capacity
  - free execution slots
  - local queue depth
  - allocated task CPU/memory reservations

### `POST /api/v1/planner/intent`

- Proxies a planning request to the planner service and returns an executable DAG suggestion
- Planner can operate in deterministic heuristic mode or optional Gemini structured-output mode

## Worker API

Worker registration requires `Authorization: Bearer <HELIOS_WORKER_BOOTSTRAP_TOKEN>`.

### `POST /api/v1/workers/register`

- Registers a worker and returns:
  - worker metadata
  - a unique per-worker bearer token

Request schema: [worker-register.schema.json](/Users/gauravgs7/Documents/Projects/Helios-AI/Test_Helios/docs/api/schemas/worker-register.schema.json)

Subsequent worker calls require the returned worker token.

### `POST /api/v1/workers/{workerID}/heartbeat`

- Updates liveness state and last heartbeat timestamp
- Accepts resource telemetry from the worker runtime:

```json
{
  "cpu_load": 0.42,
  "memory_used_mb": 96,
  "free_slots": 1,
  "queue_depth": 0,
  "running_task_count": 1
}
```

### `POST /api/v1/workflows/{workflowID}/tasks/{taskID}/start`

- Records worker acknowledgement of execution start

### `POST /api/v1/workflows/{workflowID}/tasks/{taskID}/complete`

- Records task success with attempt-aware validation

### `POST /api/v1/workflows/{workflowID}/tasks/{taskID}/fail`

- Records task failure and decides retry or terminalization

## Health and Readiness

### `GET /healthz`

- Process health

### `GET /readyz`

- Dependency readiness for PostgreSQL and planner reachability

### `GET /metrics`

- Prometheus metrics scrape endpoint
