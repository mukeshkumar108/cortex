ALTER TABLE memory_relationship_links
  ADD COLUMN IF NOT EXISTS source_domain TEXT,
  ADD COLUMN IF NOT EXISTS target_domain TEXT,
  ADD COLUMN IF NOT EXISTS status TEXT,
  ADD COLUMN IF NOT EXISTS strength DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS valid_from TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS valid_until TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;

UPDATE memory_relationship_links
SET status = 'active'
WHERE status IS NULL;

ALTER TABLE memory_relationship_links
  ALTER COLUMN status SET DEFAULT 'active',
  ALTER COLUMN status SET NOT NULL;

UPDATE memory_relationship_links
SET source_domain = CASE
        WHEN source_type = 'entity' THEN 'people'
        WHEN source_type = 'thread' THEN 'workstream'
        ELSE source_domain
    END,
    target_domain = CASE
        WHEN target_type = 'entity' THEN 'people'
        WHEN target_type = 'session' THEN 'evidence'
        ELSE target_domain
    END
WHERE source_domain IS NULL
   OR target_domain IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'memory_relationship_links_status_check'
    ) THEN
        ALTER TABLE memory_relationship_links
        ADD CONSTRAINT memory_relationship_links_status_check
        CHECK (status IN ('detected', 'confirmed', 'active', 'stale', 'dismissed', 'archived', 'contradicted'));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_memory_relationship_links_user_status_domains
  ON memory_relationship_links (tenant_id, user_id, status, source_domain, target_domain);
