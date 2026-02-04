-- Repair pending rows that have attempts but no next_attempt_at
UPDATE graphiti_outbox
SET next_attempt_at = NOW()
WHERE status = 'pending'
  AND attempts > 0
  AND next_attempt_at IS NULL;

-- Enforce invariant: pending rows with attempts must have next_attempt_at
ALTER TABLE graphiti_outbox
    ADD CONSTRAINT graphiti_outbox_pending_attempts_next_attempt
    CHECK (
        status <> 'pending'
        OR attempts = 0
        OR next_attempt_at IS NOT NULL
    );
