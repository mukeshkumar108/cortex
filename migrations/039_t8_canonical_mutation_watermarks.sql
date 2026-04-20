-- T8: Canonical mutation log + explicit watermark semantics.
-- Watermark model: per-tenant monotonic sequence.

CREATE TABLE IF NOT EXISTS canonical_tenant_watermarks (
    tenant_id TEXT PRIMARY KEY,
    last_sequence BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT canonical_tenant_watermarks_nonnegative_check CHECK (last_sequence >= 0)
);

ALTER TABLE canonical_mutations
    ADD COLUMN IF NOT EXISTS user_id TEXT;

ALTER TABLE canonical_mutations
    ADD COLUMN IF NOT EXISTS object_type TEXT;

ALTER TABLE canonical_mutations
    ADD COLUMN IF NOT EXISTS object_id TEXT;

ALTER TABLE canonical_mutations
    ADD COLUMN IF NOT EXISTS source_run_id TEXT;

ALTER TABLE canonical_mutations
    ADD COLUMN IF NOT EXISTS resolver_version TEXT;

ALTER TABLE canonical_mutations
    ADD COLUMN IF NOT EXISTS tenant_sequence BIGINT;

ALTER TABLE canonical_mutations
    ADD COLUMN IF NOT EXISTS commit_status TEXT;

UPDATE canonical_mutations
SET object_type = CASE
    WHEN claim_id IS NOT NULL THEN 'claim'
    WHEN entity_id IS NOT NULL THEN 'entity'
    ELSE 'system'
END
WHERE object_type IS NULL OR btrim(object_type) = '';

UPDATE canonical_mutations
SET object_id = CASE
    WHEN claim_id IS NOT NULL THEN claim_id::text
    WHEN entity_id IS NOT NULL THEN entity_id::text
    ELSE mutation_id::text
END
WHERE object_id IS NULL OR btrim(object_id) = '';

UPDATE canonical_mutations
SET source_run_id = COALESCE(source_run_id, source_extract_result_id::text)
WHERE source_run_id IS NULL;

UPDATE canonical_mutations
SET resolver_version = COALESCE(
    NULLIF(resolver_version, ''),
    NULLIF(payload->>'resolver_version', ''),
    NULLIF(metadata->>'resolver_version', ''),
    'legacy.unknown'
)
WHERE resolver_version IS NULL OR btrim(resolver_version) = '';

UPDATE canonical_mutations
SET commit_status = 'committed'
WHERE commit_status IS NULL OR btrim(commit_status) = '';

WITH ranked AS (
    SELECT
      tenant_id,
      mutation_id,
      row_number() OVER (
        PARTITION BY tenant_id
        ORDER BY mutation_id ASC
      )::bigint AS seq
    FROM canonical_mutations
)
UPDATE canonical_mutations m
SET tenant_sequence = r.seq
FROM ranked r
WHERE m.tenant_id = r.tenant_id
  AND m.mutation_id = r.mutation_id
  AND m.tenant_sequence IS NULL;

INSERT INTO canonical_tenant_watermarks (tenant_id, last_sequence, updated_at)
SELECT
  tenant_id,
  MAX(tenant_sequence) AS last_sequence,
  NOW()
FROM canonical_mutations
GROUP BY tenant_id
ON CONFLICT (tenant_id)
DO UPDATE SET
  last_sequence = GREATEST(canonical_tenant_watermarks.last_sequence, EXCLUDED.last_sequence),
  updated_at = EXCLUDED.updated_at;

ALTER TABLE canonical_mutations
    ALTER COLUMN object_type SET NOT NULL;

ALTER TABLE canonical_mutations
    ALTER COLUMN object_id SET NOT NULL;

ALTER TABLE canonical_mutations
    ALTER COLUMN resolver_version SET NOT NULL;

ALTER TABLE canonical_mutations
    ALTER COLUMN tenant_sequence SET NOT NULL;

ALTER TABLE canonical_mutations
    ALTER COLUMN commit_status SET NOT NULL;

ALTER TABLE canonical_mutations
    ALTER COLUMN commit_status SET DEFAULT 'committed';

ALTER TABLE canonical_mutations
    ADD CONSTRAINT canonical_mutations_object_type_nonempty_check
    CHECK (btrim(object_type) <> '');

ALTER TABLE canonical_mutations
    ADD CONSTRAINT canonical_mutations_object_id_nonempty_check
    CHECK (btrim(object_id) <> '');

ALTER TABLE canonical_mutations
    ADD CONSTRAINT canonical_mutations_commit_status_check
    CHECK (commit_status IN ('committed'));

CREATE UNIQUE INDEX IF NOT EXISTS idx_canonical_mutations_tenant_sequence
    ON canonical_mutations (tenant_id, tenant_sequence);

CREATE INDEX IF NOT EXISTS idx_canonical_mutations_committed_sequence
    ON canonical_mutations (tenant_id, commit_status, tenant_sequence);
