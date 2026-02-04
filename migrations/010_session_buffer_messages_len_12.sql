-- Update sliding window length to 12

-- Clamp any existing rows that exceed the sliding window limit
WITH trimmed AS (
    SELECT
        tenant_id,
        session_id,
        jsonb_agg(elem ORDER BY ord) AS new_messages
    FROM session_buffer,
         jsonb_array_elements(messages) WITH ORDINALITY AS t(elem, ord)
    WHERE jsonb_typeof(messages) = 'array'
      AND ord <= 12
    GROUP BY tenant_id, session_id
    HAVING jsonb_array_length(messages) > 12
)
UPDATE session_buffer sb
SET messages = trimmed.new_messages
FROM trimmed
WHERE sb.tenant_id = trimmed.tenant_id
  AND sb.session_id = trimmed.session_id;

ALTER TABLE session_buffer
    DROP CONSTRAINT IF EXISTS session_buffer_messages_len_check;

ALTER TABLE session_buffer
    ADD CONSTRAINT session_buffer_messages_len_check
    CHECK (jsonb_array_length(COALESCE(messages, '[]'::jsonb)) <= 12);
