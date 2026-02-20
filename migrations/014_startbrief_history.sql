CREATE TABLE IF NOT EXISTS startbrief_history (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    session_id TEXT NULL,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    time_of_day_label TEXT NULL,
    time_gap_human TEXT NULL,
    bridge_text TEXT NULL,
    items JSONB NOT NULL DEFAULT '[]'::jsonb,
    context JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_startbrief_history_tenant_user_requested
ON startbrief_history (tenant_id, user_id, requested_at DESC);

