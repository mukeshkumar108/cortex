-- T4b: quarantine routing contract hardening
-- Additive only. Extends claims_quarantine for deterministic auditable routing.

ALTER TABLE claims_quarantine
  ADD COLUMN IF NOT EXISTS session_id TEXT;

ALTER TABLE claims_quarantine
  ADD COLUMN IF NOT EXISTS extract_run_id UUID;

ALTER TABLE claims_quarantine
  ADD COLUMN IF NOT EXISTS reason_code TEXT;

ALTER TABLE claims_quarantine
  ADD COLUMN IF NOT EXISTS confidence DOUBLE PRECISION;

ALTER TABLE claims_quarantine
  ADD COLUMN IF NOT EXISTS grounding_score DOUBLE PRECISION;

ALTER TABLE claims_quarantine
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- Backfill linkage from extract_results for pre-existing quarantine rows.
UPDATE claims_quarantine cq
SET
  session_id = er.session_id,
  extract_run_id = er.extract_run_id
FROM extract_results er
WHERE cq.extract_result_id IS NOT NULL
  AND cq.tenant_id = er.tenant_id
  AND cq.extract_result_id = er.extract_result_id
  AND (cq.session_id IS NULL OR cq.extract_run_id IS NULL);

UPDATE claims_quarantine
SET reason_code = COALESCE(NULLIF(btrim(reason_code), ''), 'quarantine_rule')
WHERE reason_code IS NULL OR btrim(reason_code) = '';

ALTER TABLE claims_quarantine
  ALTER COLUMN reason_code SET DEFAULT 'quarantine_rule';

ALTER TABLE claims_quarantine
  ALTER COLUMN reason_code SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'claims_quarantine'::regclass
      AND conname = 'claims_quarantine_reason_code_nonempty_check'
  ) THEN
    ALTER TABLE claims_quarantine
      ADD CONSTRAINT claims_quarantine_reason_code_nonempty_check
      CHECK (btrim(reason_code) <> '');
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'claims_quarantine'::regclass
      AND conname = 'claims_quarantine_confidence_range_check'
  ) THEN
    ALTER TABLE claims_quarantine
      ADD CONSTRAINT claims_quarantine_confidence_range_check
      CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'claims_quarantine'::regclass
      AND conname = 'claims_quarantine_grounding_range_check'
  ) THEN
    ALTER TABLE claims_quarantine
      ADD CONSTRAINT claims_quarantine_grounding_range_check
      CHECK (grounding_score IS NULL OR (grounding_score >= 0 AND grounding_score <= 1));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'claims_quarantine'::regclass
      AND conname = 'claims_quarantine_updated_at_order_check'
  ) THEN
    ALTER TABLE claims_quarantine
      ADD CONSTRAINT claims_quarantine_updated_at_order_check
      CHECK (updated_at >= created_at);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'claims_quarantine'::regclass
      AND conname = 'claims_quarantine_session_fk'
  ) THEN
    ALTER TABLE claims_quarantine
      ADD CONSTRAINT claims_quarantine_session_fk
      FOREIGN KEY (tenant_id, session_id)
      REFERENCES sessions_v2 (tenant_id, session_id)
      ON DELETE SET NULL;
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'claims_quarantine'::regclass
      AND conname = 'claims_quarantine_extract_run_fk'
  ) THEN
    ALTER TABLE claims_quarantine
      ADD CONSTRAINT claims_quarantine_extract_run_fk
      FOREIGN KEY (tenant_id, extract_run_id)
      REFERENCES extract_results (tenant_id, extract_run_id)
      ON DELETE SET NULL;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_claims_quarantine_extract_run
  ON claims_quarantine (tenant_id, extract_run_id);

CREATE INDEX IF NOT EXISTS idx_claims_quarantine_scope
  ON claims_quarantine (tenant_id, user_id, session_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_claims_quarantine_reason_status
  ON claims_quarantine (tenant_id, reason_code, quarantine_status, created_at DESC);
