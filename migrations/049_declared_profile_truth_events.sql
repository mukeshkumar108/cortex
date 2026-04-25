-- Audit trail for user-declared profile truth edits.
-- Canonical truth document remains stored in user_identity.data->'declared_profile_truth'.

CREATE TABLE IF NOT EXISTS declared_profile_truth_events (
  id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  source_surface TEXT,
  updated_by TEXT,
  reason TEXT,
  change_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  previous_data JSONB NOT NULL DEFAULT '{}'::jsonb,
  new_data JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_declared_profile_truth_events_user
  ON declared_profile_truth_events (tenant_id, user_id, created_at DESC);
