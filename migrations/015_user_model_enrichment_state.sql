CREATE TABLE IF NOT EXISTS user_model_enrichment_state (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    last_enriched_at TIMESTAMPTZ,
    last_daily_enriched_at TIMESTAMPTZ,
    last_weekly_enriched_at TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    retry_attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    last_mode TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_user_model_enrichment_state_next_retry
ON user_model_enrichment_state (next_retry_at ASC);

CREATE INDEX IF NOT EXISTS idx_user_model_enrichment_state_updated
ON user_model_enrichment_state (updated_at DESC);
