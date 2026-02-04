-- Reset stuck outbox rows caused by metadata kwarg mismatch
UPDATE graphiti_outbox
SET
    status = 'pending',
    attempts = 0,
    last_error = NULL,
    next_attempt_at = NOW()
WHERE last_error ILIKE '%unexpected keyword argument%'
  AND last_error ILIKE '%metadata%';
