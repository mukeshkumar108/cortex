-- Prepared always-on memory packet artifact for Sophie runtime.
-- Additive only. Does not change serving response shape of existing endpoints.

CREATE TABLE IF NOT EXISTS always_on_memory_packets (
  tenant_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  packet_version TEXT NOT NULL,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  source_fingerprint TEXT NOT NULL,
  profile_truth_used BOOLEAN NOT NULL DEFAULT FALSE,
  sections JSONB NOT NULL DEFAULT '{}'::jsonb,
  packet_text TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_always_on_memory_packets_generated
  ON always_on_memory_packets (tenant_id, generated_at DESC);
