ALTER TABLE graphiti_outbox
    ADD COLUMN IF NOT EXISTS job_type TEXT NOT NULL DEFAULT 'turn',
    ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS dedupe_key TEXT;

WITH ranked AS (
    SELECT id, dedupe_key,
           ROW_NUMBER() OVER (PARTITION BY dedupe_key ORDER BY id DESC) AS rn
    FROM graphiti_outbox
    WHERE dedupe_key IS NOT NULL
)
DELETE FROM graphiti_outbox g
USING ranked r
WHERE g.id = r.id
  AND r.rn > 1;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'graphiti_outbox_dedupe_key_unique'
    ) THEN
        ALTER TABLE graphiti_outbox
            ADD CONSTRAINT graphiti_outbox_dedupe_key_unique UNIQUE (dedupe_key);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_graphiti_outbox_status_job_next_attempt
    ON graphiti_outbox(status, job_type, next_attempt_at);
