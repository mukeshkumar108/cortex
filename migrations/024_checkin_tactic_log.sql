CREATE TABLE IF NOT EXISTS checkin_tactic_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    thread_key TEXT NOT NULL,
    stale_entity TEXT NOT NULL,
    session_id TEXT,
    fired_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_checkin_tactic_log_lookup
    ON checkin_tactic_log (tenant_id, user_id, thread_key, fired_at DESC);
