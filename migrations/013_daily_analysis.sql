CREATE TABLE IF NOT EXISTS daily_analysis (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    analysis_date DATE NOT NULL,
    themes JSONB NOT NULL DEFAULT '[]'::jsonb,
    scores JSONB NOT NULL DEFAULT '{}'::jsonb,
    steering_note TEXT,
    confidence REAL,
    source TEXT NOT NULL DEFAULT 'llm',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, user_id, analysis_date)
);

CREATE INDEX IF NOT EXISTS idx_daily_analysis_tenant_user_date
ON daily_analysis (tenant_id, user_id, analysis_date DESC);

CREATE INDEX IF NOT EXISTS idx_daily_analysis_date
ON daily_analysis (analysis_date DESC);
