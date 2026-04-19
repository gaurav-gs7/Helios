CREATE TABLE IF NOT EXISTS task_results (
    result_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    status TEXT NOT NULL,
    output_payload JSONB,
    error_message TEXT,
    recorded_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (workflow_id, task_id) REFERENCES tasks(workflow_id, task_id) ON DELETE CASCADE,
    FOREIGN KEY (attempt_id) REFERENCES task_attempts(attempt_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS task_state_history (
    history_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    attempt_id TEXT,
    old_state TEXT,
    new_state TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT,
    recorded_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (workflow_id, task_id) REFERENCES tasks(workflow_id, task_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_task_results_workflow_task ON task_results (workflow_id, task_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_task_state_history_workflow_task ON task_state_history (workflow_id, task_id, recorded_at DESC);
