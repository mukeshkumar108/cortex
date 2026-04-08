ALTER TABLE user_model_enrichment_state
ADD COLUMN IF NOT EXISTS last_hygiene_at TIMESTAMPTZ;

ALTER TABLE user_model_enrichment_state
ADD COLUMN IF NOT EXISTS next_hygiene_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_user_model_enrichment_state_hygiene
ON user_model_enrichment_state (next_hygiene_at ASC, last_hygiene_at ASC);

CREATE TABLE IF NOT EXISTS user_model_write_claims (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    claim_owner TEXT NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_user_model_write_claims_expires
ON user_model_write_claims (expires_at ASC);
