ALTER TABLE session_transcript
    ADD COLUMN IF NOT EXISTS user_id TEXT;

UPDATE session_transcript st
SET user_id = sb.user_id
FROM session_buffer sb
WHERE st.user_id IS NULL
  AND sb.tenant_id = st.tenant_id
  AND sb.session_id = st.session_id;

CREATE INDEX IF NOT EXISTS idx_session_transcript_tenant_user
ON session_transcript(tenant_id, user_id);
