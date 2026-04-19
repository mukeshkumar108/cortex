-- Ensure raw_triage_output stores a JSON object, not a JSON string payload.
UPDATE session_classifications
SET raw_triage_output = (raw_triage_output #>> '{}')::jsonb
WHERE raw_triage_output IS NOT NULL
  AND jsonb_typeof(raw_triage_output) = 'string';
