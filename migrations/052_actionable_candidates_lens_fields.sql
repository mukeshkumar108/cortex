-- Add lens/generalization fields to actionable_candidates.
-- Additive only.

ALTER TABLE actionable_candidates
  ADD COLUMN IF NOT EXISTS relevant_from_iso TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS relevant_until_iso TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS suggested_action TEXT,
  ADD COLUMN IF NOT EXISTS linked_external_id TEXT,
  ADD COLUMN IF NOT EXISTS linked_external_type TEXT;

CREATE INDEX IF NOT EXISTS idx_actionable_candidates_user_status_relevance
  ON actionable_candidates (tenant_id, user_id, status, relevant_from_iso NULLS LAST, relevant_until_iso NULLS LAST, updated_at DESC);
