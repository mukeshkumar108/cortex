-- Expose only non-empty transcript sessions for downstream processing.
-- This is the canonical working set for corpus passes.
DROP VIEW IF EXISTS processable_sessions;

CREATE VIEW processable_sessions AS
SELECT
    tenant_id,
    session_id,
    user_id,
    created_at,
    updated_at,
    messages,
    jsonb_array_length(messages) AS message_count
FROM session_transcript
WHERE tenant_id = 'default'
  AND jsonb_typeof(messages) = 'array'
  AND jsonb_array_length(messages) > 0
ORDER BY created_at ASC, session_id ASC;

CREATE TABLE IF NOT EXISTS session_classifications (
  session_id        TEXT PRIMARY KEY,
  user_id           TEXT,
  session_date      TIMESTAMPTZ,
  is_memory_worthy  BOOLEAN,
  session_kind      TEXT,
  one_line_summary  TEXT,
  entity_mentions   TEXT[],
  run_entity_pass   BOOLEAN,
  run_threads_pass  BOOLEAN,
  identity_relevant BOOLEAN,
  emotional_weight  TEXT,
  emotional_note    TEXT,
  processed_at      TIMESTAMPTZ,
  model_used        TEXT
);
