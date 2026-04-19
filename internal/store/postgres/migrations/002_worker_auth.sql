ALTER TABLE workers
    ADD COLUMN IF NOT EXISTS auth_token_hash TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_workers_registered_at ON workers (registered_at DESC);
