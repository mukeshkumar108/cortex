-- Add pass2a action/tool subtype support without changing the coarse record_type model.
ALTER TABLE actionable_candidates
  ADD COLUMN IF NOT EXISTS candidate_subtype TEXT,
  ADD COLUMN IF NOT EXISTS waiting_on TEXT,
  ADD COLUMN IF NOT EXISTS needs_response BOOLEAN,
  ADD COLUMN IF NOT EXISTS cadence_text TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'actionable_candidates'::regclass
      AND conname = 'actionable_candidates_candidate_subtype_check'
  ) THEN
    ALTER TABLE actionable_candidates
      ADD CONSTRAINT actionable_candidates_candidate_subtype_check
      CHECK (candidate_subtype IS NULL OR candidate_subtype IN (
        'todo','reminder','calendar_event','habit','follow_up','waiting_on','nudge'
      ));
  END IF;
END $$;
