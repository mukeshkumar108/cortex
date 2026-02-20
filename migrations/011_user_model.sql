CREATE TABLE IF NOT EXISTS user_model (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    model JSONB NOT NULL DEFAULT '{}'::jsonb,
    version INT NOT NULL DEFAULT 1,
    last_source TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (tenant_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_user_model_updated_at
ON user_model(updated_at DESC);
