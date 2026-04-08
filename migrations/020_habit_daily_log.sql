ALTER TABLE loops
    ADD COLUMN IF NOT EXISTS hint TEXT;

CREATE TABLE IF NOT EXISTS habit_daily_log (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    habit_id UUID NOT NULL REFERENCES loops(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    completed BOOLEAN NOT NULL DEFAULT FALSE,
    nudged BOOLEAN NOT NULL DEFAULT FALSE,
    user_response TEXT,
    inferred_from TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (habit_id, date)
);

CREATE INDEX IF NOT EXISTS idx_habit_daily_log_user_date
ON habit_daily_log (user_id, date DESC);

CREATE INDEX IF NOT EXISTS idx_habit_daily_log_habit_date
ON habit_daily_log (habit_id, date DESC);
