CREATE TABLE IF NOT EXISTS workflows (
    workflow_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    state TEXT NOT NULL,
    labels JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    spec JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id) ON DELETE CASCADE,
    task_id TEXT NOT NULL,
    task_type TEXT NOT NULL,
    state TEXT NOT NULL,
    input_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_payload JSONB,
    timeout_seconds INTEGER NOT NULL,
    retry_policy JSONB NOT NULL,
    max_attempts INTEGER NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    current_attempt_id TEXT,
    assigned_worker TEXT,
    lease_expires_at TIMESTAMPTZ,
    next_run_at TIMESTAMPTZ,
    last_error TEXT,
    labels JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    cpu_units INTEGER NOT NULL DEFAULT 0,
    memory_mb INTEGER NOT NULL DEFAULT 0,
    expected_duration_seconds INTEGER NOT NULL DEFAULT 0,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (workflow_id, task_id)
);

CREATE TABLE IF NOT EXISTS task_dependencies (
    workflow_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    dependency_task_id TEXT NOT NULL,
    PRIMARY KEY (workflow_id, task_id, dependency_task_id),
    FOREIGN KEY (workflow_id, task_id) REFERENCES tasks(workflow_id, task_id) ON DELETE CASCADE,
    FOREIGN KEY (workflow_id, dependency_task_id) REFERENCES tasks(workflow_id, task_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS task_attempts (
    attempt_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    state TEXT NOT NULL,
    lease_expires_at TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (workflow_id, task_id) REFERENCES tasks(workflow_id, task_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS workers (
    worker_id TEXT PRIMARY KEY,
    hostname TEXT NOT NULL,
    version TEXT NOT NULL,
    supported_task_types TEXT[] NOT NULL,
    capacity INTEGER NOT NULL,
    cpu_load DOUBLE PRECISION NOT NULL DEFAULT 0,
    memory_used_mb INTEGER NOT NULL DEFAULT 0,
    memory_capacity_mb INTEGER NOT NULL DEFAULT 1024,
    cpu_capacity_units INTEGER NOT NULL DEFAULT 1000,
    free_slots INTEGER NOT NULL DEFAULT 0,
    queue_depth INTEGER NOT NULL DEFAULT 0,
    last_heartbeat_at TIMESTAMPTZ NOT NULL,
    health TEXT NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS task_events (
    event_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    task_id TEXT,
    attempt_id TEXT,
    actor TEXT NOT NULL,
    old_state TEXT,
    new_state TEXT,
    reason TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    description TEXT NOT NULL DEFAULT '',
    occurred_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (workflow_id) REFERENCES workflows(workflow_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tasks_state_next_run ON tasks (state, next_run_at);
CREATE INDEX IF NOT EXISTS idx_tasks_priority_ready ON tasks (priority DESC, created_at ASC) WHERE state = 'ready';
CREATE INDEX IF NOT EXISTS idx_tasks_assigned_worker_state ON tasks (assigned_worker, state);
CREATE INDEX IF NOT EXISTS idx_tasks_resource_reservations ON tasks (assigned_worker, state, cpu_units, memory_mb) WHERE assigned_worker IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_workers_health ON workers (health, last_heartbeat_at);
CREATE INDEX IF NOT EXISTS idx_workers_resource_scheduling ON workers (health, free_slots DESC, cpu_load ASC, queue_depth ASC);
CREATE INDEX IF NOT EXISTS idx_task_attempts_lookup ON task_attempts (workflow_id, task_id, attempt);
CREATE INDEX IF NOT EXISTS idx_task_events_workflow ON task_events (workflow_id, occurred_at DESC);
