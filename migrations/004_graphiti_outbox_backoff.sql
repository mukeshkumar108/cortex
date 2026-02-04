-- Add folding + retry backoff fields to outbox
ALTER TABLE graphiti_outbox
    ADD COLUMN IF NOT EXISTS folded_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_graphiti_outbox_next_attempt
    ON graphiti_outbox(status, next_attempt_at);
