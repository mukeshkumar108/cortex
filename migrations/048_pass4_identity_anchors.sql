-- Structured identity anchors for Pass 4.
-- Declared truth continues to live in user_identity / identity_cache.
-- This table stores lower-authority durable profile facts derived from evidence.

CREATE TABLE IF NOT EXISTS durable_profile_facts (
  tenant_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  fact_key TEXT NOT NULL,
  fact_type TEXT NOT NULL,
  fact_value TEXT NOT NULL,
  source_type TEXT NOT NULL,
  confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  evidence_count INTEGER NOT NULL DEFAULT 0,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, user_id, fact_key)
);

CREATE INDEX IF NOT EXISTS idx_durable_profile_facts_user
  ON durable_profile_facts (tenant_id, user_id, fact_type, updated_at DESC);
