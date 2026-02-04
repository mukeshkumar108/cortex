CREATE TABLE IF NOT EXISTS session_transcript (
    tenant_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    messages JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (tenant_id, session_id)
);

CREATE INDEX IF NOT EXISTS idx_session_transcript_tenant_session
ON session_transcript(tenant_id, session_id);
