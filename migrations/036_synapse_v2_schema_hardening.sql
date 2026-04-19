-- T2 close-out: Synapse v2 additive schema hardening
-- Scope-limited hardening for structural integrity discovered post-T2 audit.
-- Additive only; no legacy table deletion/rewrite.
--
-- Operational note:
-- - This migration relies on pgcrypto for gen_random_uuid().
-- - Managed Postgres roles may require pre-provisioning pgcrypto by DB admin.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1) Enforce session/turn user integrity -------------------------------------

-- Support composite FK shape from turns_v2 -> sessions_v2 (tenant_id, session_id, user_id)
ALTER TABLE sessions_v2
  ADD CONSTRAINT sessions_v2_tenant_session_user_unique
  UNIQUE (tenant_id, session_id, user_id);

-- Prevent empty source_turn_id strings when provided.
ALTER TABLE turns_v2
  ADD CONSTRAINT turns_v2_source_turn_nonempty_check
  CHECK (source_turn_id IS NULL OR btrim(source_turn_id) <> '');

-- Ensure each turn's user_id matches session owner user_id.
ALTER TABLE turns_v2
  ADD CONSTRAINT turns_v2_session_user_fk
  FOREIGN KEY (tenant_id, session_id, user_id)
  REFERENCES sessions_v2 (tenant_id, session_id, user_id)
  ON DELETE CASCADE;

-- 2) Enforce entity merge lineage integrity ----------------------------------

ALTER TABLE entities
  ADD CONSTRAINT entities_merged_into_fk
  FOREIGN KEY (tenant_id, merged_into_entity_id)
  REFERENCES entities (tenant_id, entity_id)
  ON DELETE SET NULL;

ALTER TABLE entities
  ADD CONSTRAINT entities_not_merged_into_self_check
  CHECK (merged_into_entity_id IS NULL OR merged_into_entity_id <> entity_id);

-- 3) Anchor predicate policy version relationally ----------------------------

CREATE TABLE IF NOT EXISTS predicate_policy_versions (
    policy_version TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'draft',
    description TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    activated_at TIMESTAMPTZ,
    retired_at TIMESTAMPTZ,
    CONSTRAINT predicate_policy_versions_status_check
      CHECK (status IN ('draft', 'active', 'retired')),
    CONSTRAINT predicate_policy_versions_activation_window_check
      CHECK (
        retired_at IS NULL
        OR activated_at IS NULL
        OR retired_at >= activated_at
      )
);

-- Ensure at most one active policy version at a time.
CREATE UNIQUE INDEX IF NOT EXISTS idx_predicate_policy_versions_single_active
  ON predicate_policy_versions ((status))
  WHERE status = 'active';

-- Backfill existing policy versions into registry before adding FK.
INSERT INTO predicate_policy_versions (policy_version, status, description)
SELECT DISTINCT p.policy_version, 'draft', 'backfilled from predicate_policy rows'
FROM predicate_policy p
WHERE p.policy_version IS NOT NULL
ON CONFLICT (policy_version) DO NOTHING;

ALTER TABLE predicate_policy
  ADD CONSTRAINT predicate_policy_version_fk
  FOREIGN KEY (policy_version)
  REFERENCES predicate_policy_versions (policy_version)
  ON DELETE RESTRICT;

ALTER TABLE extract_results
  ADD CONSTRAINT extract_results_policy_version_fk
  FOREIGN KEY (predicate_policy_version)
  REFERENCES predicate_policy_versions (policy_version)
  ON DELETE RESTRICT;

ALTER TABLE claims
  ADD CONSTRAINT claims_policy_version_fk
  FOREIGN KEY (predicate_policy_version)
  REFERENCES predicate_policy_versions (policy_version)
  ON DELETE RESTRICT;

-- 4) T3 idempotency substrate hardening --------------------------------------

-- Dedicated idempotency key ledger for dual-write ingest.
-- This keeps ingestion contract enforceable without requiring T3 logic in this ticket.
CREATE TABLE IF NOT EXISTS turn_ingest_idempotency (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    turn_id BIGINT NOT NULL,
    source TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (tenant_id, user_id, session_id, idempotency_key),
    CONSTRAINT turn_ingest_idempotency_key_nonempty_check CHECK (btrim(idempotency_key) <> ''),
    CONSTRAINT turn_ingest_idempotency_turn_fk
      FOREIGN KEY (tenant_id, turn_id)
      REFERENCES turns_v2 (tenant_id, turn_id)
      ON DELETE CASCADE,
    CONSTRAINT turn_ingest_idempotency_session_user_fk
      FOREIGN KEY (tenant_id, session_id, user_id)
      REFERENCES sessions_v2 (tenant_id, session_id, user_id)
      ON DELETE CASCADE
);

-- One idempotency registration maps to at most one turn row.
CREATE UNIQUE INDEX IF NOT EXISTS idx_turn_ingest_idempotency_turn_unique
  ON turn_ingest_idempotency (tenant_id, turn_id);

CREATE INDEX IF NOT EXISTS idx_turn_ingest_idempotency_lookup
  ON turn_ingest_idempotency (tenant_id, user_id, session_id, created_at DESC);
