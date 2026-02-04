-- Graphiti outbox for sliding window eviction reliability
CREATE TABLE IF NOT EXISTS graphiti_outbox (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    role TEXT NOT NULL,
    text TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', -- pending|sent|failed
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    sent_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_graphiti_outbox_status_tenant_user
    ON graphiti_outbox(status, tenant_id, user_id);

CREATE INDEX IF NOT EXISTS idx_graphiti_outbox_tenant_session
    ON graphiti_outbox(tenant_id, session_id);
