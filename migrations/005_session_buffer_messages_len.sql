-- Clamp any existing rows that exceed the sliding window limit
UPDATE session_buffer
SET messages = '[]'::jsonb
WHERE messages IS NULL
   OR jsonb_typeof(messages) <> 'array';

WITH trimmed AS (
    SELECT
        tenant_id,
        session_id,
        jsonb_agg(elem ORDER BY ord) AS new_messages
    FROM session_buffer,
         jsonb_array_elements(messages) WITH ORDINALITY AS t(elem, ord)
    WHERE jsonb_typeof(messages) = 'array'
      AND ord <= 6
    GROUP BY tenant_id, session_id
    HAVING jsonb_array_length(messages) > 6
)
UPDATE session_buffer sb
SET messages = trimmed.new_messages
FROM trimmed
WHERE sb.tenant_id = trimmed.tenant_id
  AND sb.session_id = trimmed.session_id;

-- Enforce sliding window length at the DB level
ALTER TABLE session_buffer
    ADD CONSTRAINT session_buffer_messages_len_check
    CHECK (jsonb_array_length(COALESCE(messages, '[]'::jsonb)) <= 6);
