CREATE TABLE IF NOT EXISTS habit_dedupe_state (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    last_run_date DATE NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_habit_dedupe_state_last_run_date
ON habit_dedupe_state (last_run_date DESC);
