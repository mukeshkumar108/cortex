-- T12b: live shadow-read diffing/audit substrate

CREATE TABLE IF NOT EXISTS retrieval_shadow_diffs (
    shadow_id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    served_intent TEXT,
    request_fingerprint TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ok',
    served_latency_ms DOUBLE PRECISION,
    shadow_latency_ms DOUBLE PRECISION,
    latency_delta_ms DOUBLE PRECISION,
    error_text TEXT,
    diff_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    metrics_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT retrieval_shadow_diffs_status_check CHECK (status IN ('ok', 'shadow_error', 'skipped')),
    CONSTRAINT retrieval_shadow_diffs_endpoint_nonempty_check CHECK (btrim(endpoint) <> ''),
    CONSTRAINT retrieval_shadow_diffs_request_fingerprint_nonempty_check CHECK (btrim(request_fingerprint) <> '')
);

CREATE INDEX IF NOT EXISTS idx_retrieval_shadow_diffs_created
    ON retrieval_shadow_diffs (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_retrieval_shadow_diffs_tenant_created
    ON retrieval_shadow_diffs (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_retrieval_shadow_diffs_status_created
    ON retrieval_shadow_diffs (status, created_at DESC);
