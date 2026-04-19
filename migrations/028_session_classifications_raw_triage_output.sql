ALTER TABLE session_classifications
ADD COLUMN IF NOT EXISTS raw_triage_output JSONB;
