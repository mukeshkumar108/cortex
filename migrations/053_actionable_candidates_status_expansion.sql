-- Expand actionable candidate lifecycle statuses for reconciliation.
-- Additive-only migration.

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'actionable_candidates'::regclass
      AND conname = 'actionable_candidates_status_check'
  ) THEN
    ALTER TABLE actionable_candidates
      DROP CONSTRAINT actionable_candidates_status_check;
  END IF;
END $$;

ALTER TABLE actionable_candidates
  ADD CONSTRAINT actionable_candidates_status_check
  CHECK (status IN ('detected','needs_review','confirmed','dismissed','acted_on','superseded','stale'));
