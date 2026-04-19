ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS cpu_units INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS memory_mb INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS expected_duration_seconds INTEGER NOT NULL DEFAULT 0;

ALTER TABLE workers
    ADD COLUMN IF NOT EXISTS cpu_load DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS memory_used_mb INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS memory_capacity_mb INTEGER NOT NULL DEFAULT 1024,
    ADD COLUMN IF NOT EXISTS cpu_capacity_units INTEGER NOT NULL DEFAULT 1000,
    ADD COLUMN IF NOT EXISTS free_slots INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS queue_depth INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_tasks_priority_ready
    ON tasks (priority DESC, created_at ASC)
    WHERE state = 'ready';

CREATE INDEX IF NOT EXISTS idx_tasks_resource_reservations
    ON tasks (assigned_worker, state, cpu_units, memory_mb)
    WHERE assigned_worker IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_workers_resource_scheduling
    ON workers (health, free_slots DESC, cpu_load ASC, queue_depth ASC);
