-- T2: Synapse v2 additive canonical schema substrate
-- Additive only. No legacy table drops/rewrites.
-- Lock risk notes:
-- - New table creation is low risk.
-- - New indexes on new tables are low risk.
-- - Foreign keys are scoped to new tables; no legacy back-validation locks.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Canonical evidence layer ---------------------------------------------------

CREATE TABLE IF NOT EXISTS sessions_v2 (
    tenant_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'open',
    source TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, session_id),
    CONSTRAINT sessions_v2_status_check CHECK (status IN ('open', 'closed', 'archived'))
);

CREATE INDEX IF NOT EXISTS idx_sessions_v2_tenant_user_started
    ON sessions_v2 (tenant_id, user_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_sessions_v2_tenant_status_started
    ON sessions_v2 (tenant_id, status, started_at DESC);

CREATE TABLE IF NOT EXISTS turns_v2 (
    turn_id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    turn_index INT NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    source_turn_id TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT turns_v2_role_check CHECK (role IN ('system', 'user', 'assistant', 'tool')),
    CONSTRAINT turns_v2_turn_index_check CHECK (turn_index >= 0),
    CONSTRAINT turns_v2_session_fk
      FOREIGN KEY (tenant_id, session_id)
      REFERENCES sessions_v2 (tenant_id, session_id)
      ON DELETE CASCADE,
    CONSTRAINT turns_v2_session_turn_unique UNIQUE (tenant_id, session_id, turn_index),
    CONSTRAINT turns_v2_source_turn_unique UNIQUE (tenant_id, session_id, source_turn_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_turns_v2_tenant_turn_id
    ON turns_v2 (tenant_id, turn_id);

CREATE INDEX IF NOT EXISTS idx_turns_v2_tenant_user_occurred
    ON turns_v2 (tenant_id, user_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_turns_v2_tenant_session_occurred
    ON turns_v2 (tenant_id, session_id, occurred_at ASC);

-- Canonical signal layer -----------------------------------------------------

CREATE TABLE IF NOT EXISTS entities (
    entity_id UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    canonical_name_normalized TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    merged_into_entity_id UUID,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, entity_id),
    CONSTRAINT entities_status_check CHECK (status IN ('active', 'merged', 'archived')),
    CONSTRAINT entities_user_canonical_name_unique UNIQUE (tenant_id, user_id, canonical_name_normalized)
);

CREATE INDEX IF NOT EXISTS idx_entities_tenant_user_type
    ON entities (tenant_id, user_id, entity_type);

CREATE INDEX IF NOT EXISTS idx_entities_tenant_status_updated
    ON entities (tenant_id, status, updated_at DESC);

CREATE TABLE IF NOT EXISTS entity_aliases (
    alias_id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    entity_id UUID NOT NULL,
    alias_text TEXT NOT NULL,
    alias_normalized TEXT NOT NULL,
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    confidence DOUBLE PRECISION,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT entity_aliases_confidence_check CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
    CONSTRAINT entity_aliases_entity_fk
      FOREIGN KEY (tenant_id, entity_id)
      REFERENCES entities (tenant_id, entity_id)
      ON DELETE CASCADE,
    CONSTRAINT entity_aliases_unique UNIQUE (tenant_id, entity_id, alias_normalized)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_aliases_primary_per_entity
    ON entity_aliases (tenant_id, entity_id)
    WHERE is_primary;

CREATE INDEX IF NOT EXISTS idx_entity_aliases_lookup
    ON entity_aliases (tenant_id, user_id, alias_normalized);

CREATE TABLE IF NOT EXISTS predicate_policy (
    predicate_policy_id BIGSERIAL PRIMARY KEY,
    policy_version TEXT NOT NULL,
    predicate TEXT NOT NULL,
    cardinality TEXT NOT NULL,
    conflict_rule TEXT NOT NULL,
    expected_object_type TEXT,
    equivalence_rule TEXT,
    is_active BOOLEAN NOT NULL DEFAULT FALSE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT predicate_policy_cardinality_check CHECK (cardinality IN ('one', 'many')),
    CONSTRAINT predicate_policy_unique UNIQUE (policy_version, predicate)
);

CREATE INDEX IF NOT EXISTS idx_predicate_policy_active
    ON predicate_policy (is_active, predicate);

CREATE TABLE IF NOT EXISTS extract_results (
    extract_result_id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    turn_id BIGINT,
    extract_run_id UUID NOT NULL DEFAULT gen_random_uuid(),
    model_version TEXT,
    prompt_version TEXT,
    predicate_policy_version TEXT,
    status TEXT NOT NULL DEFAULT 'succeeded',
    raw_output JSONB,
    candidates JSONB,
    error_text TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT extract_results_status_check CHECK (status IN ('succeeded', 'partial', 'failed', 'quarantined')),
    CONSTRAINT extract_results_tenant_extract_unique UNIQUE (tenant_id, extract_result_id),
    CONSTRAINT extract_results_run_unique UNIQUE (tenant_id, extract_run_id),
    CONSTRAINT extract_results_session_fk
      FOREIGN KEY (tenant_id, session_id)
      REFERENCES sessions_v2 (tenant_id, session_id)
      ON DELETE CASCADE,
    CONSTRAINT extract_results_turn_fk
      FOREIGN KEY (tenant_id, turn_id)
      REFERENCES turns_v2 (tenant_id, turn_id)
      ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_extract_results_scope
    ON extract_results (tenant_id, user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_extract_results_session
    ON extract_results (tenant_id, session_id, created_at DESC);

CREATE TABLE IF NOT EXISTS claims (
    claim_id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    claim_slot_key TEXT NOT NULL,
    claim_event_key TEXT NOT NULL,
    predicate TEXT NOT NULL,
    subject_entity_id UUID,
    subject_text TEXT,
    object_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    lifecycle_status TEXT NOT NULL DEFAULT 'active',
    extraction_confidence DOUBLE PRECISION,
    truth_confidence DOUBLE PRECISION,
    predicate_policy_version TEXT,
    source_extract_result_id BIGINT,
    occurred_at TIMESTAMPTZ,
    valid_from TIMESTAMPTZ,
    valid_to TIMESTAMPTZ,
    superseded_by_claim_id BIGINT,
    retracted_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT claims_lifecycle_status_check CHECK (lifecycle_status IN ('active', 'superseded', 'retracted')),
    CONSTRAINT claims_extraction_confidence_check CHECK (extraction_confidence IS NULL OR (extraction_confidence >= 0 AND extraction_confidence <= 1)),
    CONSTRAINT claims_truth_confidence_check CHECK (truth_confidence IS NULL OR (truth_confidence >= 0 AND truth_confidence <= 1)),
    CONSTRAINT claims_tenant_claim_unique UNIQUE (tenant_id, claim_id),
    CONSTRAINT claims_event_key_unique UNIQUE (tenant_id, claim_event_key),
    CONSTRAINT claims_subject_entity_fk
      FOREIGN KEY (tenant_id, subject_entity_id)
      REFERENCES entities (tenant_id, entity_id)
      ON DELETE SET NULL,
    CONSTRAINT claims_extract_result_fk
      FOREIGN KEY (tenant_id, source_extract_result_id)
      REFERENCES extract_results (tenant_id, extract_result_id)
      ON DELETE SET NULL,
    CONSTRAINT claims_superseded_by_fk
      FOREIGN KEY (tenant_id, superseded_by_claim_id)
      REFERENCES claims (tenant_id, claim_id)
      ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_claims_factual_lookup
    ON claims (tenant_id, user_id, lifecycle_status, predicate, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_claims_slot_lookup
    ON claims (tenant_id, claim_slot_key, lifecycle_status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_claims_subject_lookup
    ON claims (tenant_id, subject_entity_id, lifecycle_status);

CREATE TABLE IF NOT EXISTS claim_evidence (
    claim_evidence_id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    claim_id BIGINT NOT NULL,
    session_id TEXT NOT NULL,
    turn_id BIGINT,
    evidence_kind TEXT NOT NULL DEFAULT 'span',
    evidence_text TEXT,
    evidence_start_char INT,
    evidence_end_char INT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT claim_evidence_char_range_check CHECK (
      evidence_start_char IS NULL
      OR evidence_end_char IS NULL
      OR evidence_start_char <= evidence_end_char
    ),
    CONSTRAINT claim_evidence_claim_fk
      FOREIGN KEY (tenant_id, claim_id)
      REFERENCES claims (tenant_id, claim_id)
      ON DELETE CASCADE,
    CONSTRAINT claim_evidence_session_fk
      FOREIGN KEY (tenant_id, session_id)
      REFERENCES sessions_v2 (tenant_id, session_id)
      ON DELETE CASCADE,
    CONSTRAINT claim_evidence_turn_fk
      FOREIGN KEY (tenant_id, turn_id)
      REFERENCES turns_v2 (tenant_id, turn_id)
      ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_claim_evidence_claim
    ON claim_evidence (tenant_id, claim_id);

CREATE INDEX IF NOT EXISTS idx_claim_evidence_session_turn
    ON claim_evidence (tenant_id, session_id, turn_id);

CREATE TABLE IF NOT EXISTS claims_quarantine (
    quarantine_id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    extract_result_id BIGINT,
    candidate_payload JSONB NOT NULL,
    reason TEXT NOT NULL,
    quarantine_status TEXT NOT NULL DEFAULT 'pending',
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT claims_quarantine_status_check CHECK (quarantine_status IN ('pending', 'approved', 'rejected', 'expired')),
    CONSTRAINT claims_quarantine_extract_result_fk
      FOREIGN KEY (tenant_id, extract_result_id)
      REFERENCES extract_results (tenant_id, extract_result_id)
      ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_claims_quarantine_pending
    ON claims_quarantine (tenant_id, quarantine_status, created_at DESC);

CREATE TABLE IF NOT EXISTS canonical_mutations (
    mutation_id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    mutation_type TEXT NOT NULL,
    claim_id BIGINT,
    entity_id UUID,
    source_extract_result_id BIGINT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    committed_by TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT canonical_mutations_tenant_mutation_unique UNIQUE (tenant_id, mutation_id),
    CONSTRAINT canonical_mutations_claim_fk
      FOREIGN KEY (tenant_id, claim_id)
      REFERENCES claims (tenant_id, claim_id)
      ON DELETE SET NULL,
    CONSTRAINT canonical_mutations_entity_fk
      FOREIGN KEY (tenant_id, entity_id)
      REFERENCES entities (tenant_id, entity_id)
      ON DELETE SET NULL,
    CONSTRAINT canonical_mutations_extract_result_fk
      FOREIGN KEY (tenant_id, source_extract_result_id)
      REFERENCES extract_results (tenant_id, extract_result_id)
      ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_canonical_mutations_tenant_mutation
    ON canonical_mutations (tenant_id, mutation_id);

CREATE INDEX IF NOT EXISTS idx_canonical_mutations_tenant_committed
    ON canonical_mutations (tenant_id, committed_at DESC);

-- Derived projection layer ---------------------------------------------------

CREATE TABLE IF NOT EXISTS projection_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    projection_type TEXT NOT NULL,
    projection_version TEXT NOT NULL,
    source_mutation_id BIGINT,
    payload JSONB NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT projection_snapshots_mutation_fk
      FOREIGN KEY (tenant_id, source_mutation_id)
      REFERENCES canonical_mutations (tenant_id, mutation_id)
      ON DELETE SET NULL,
    CONSTRAINT projection_snapshots_unique
      UNIQUE (tenant_id, user_id, projection_type, projection_version, source_mutation_id)
);

CREATE INDEX IF NOT EXISTS idx_projection_snapshots_lookup
    ON projection_snapshots (tenant_id, user_id, projection_type, generated_at DESC);

CREATE TABLE IF NOT EXISTS projection_latest (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    projection_type TEXT NOT NULL,
    projection_version TEXT NOT NULL,
    source_mutation_id BIGINT,
    payload JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (tenant_id, user_id, projection_type, projection_version),
    CONSTRAINT projection_latest_mutation_fk
      FOREIGN KEY (tenant_id, source_mutation_id)
      REFERENCES canonical_mutations (tenant_id, mutation_id)
      ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_projection_latest_lookup
    ON projection_latest (tenant_id, user_id, projection_type, updated_at DESC);

-- Pipeline/control checkpointing --------------------------------------------

CREATE TABLE IF NOT EXISTS v2_pipeline_checkpoints (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    pipeline_name TEXT NOT NULL,
    last_session_id TEXT,
    last_turn_id BIGINT,
    last_extract_result_id BIGINT,
    last_mutation_id BIGINT,
    last_run_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, user_id, pipeline_name),
    CONSTRAINT v2_pipeline_checkpoints_session_fk
      FOREIGN KEY (tenant_id, last_session_id)
      REFERENCES sessions_v2 (tenant_id, session_id)
      ON DELETE SET NULL,
    CONSTRAINT v2_pipeline_checkpoints_turn_fk
      FOREIGN KEY (tenant_id, last_turn_id)
      REFERENCES turns_v2 (tenant_id, turn_id)
      ON DELETE SET NULL,
    CONSTRAINT v2_pipeline_checkpoints_extract_fk
      FOREIGN KEY (tenant_id, last_extract_result_id)
      REFERENCES extract_results (tenant_id, extract_result_id)
      ON DELETE SET NULL,
    CONSTRAINT v2_pipeline_checkpoints_mutation_fk
      FOREIGN KEY (tenant_id, last_mutation_id)
      REFERENCES canonical_mutations (tenant_id, mutation_id)
      ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_v2_pipeline_checkpoints_pipeline
    ON v2_pipeline_checkpoints (tenant_id, pipeline_name, updated_at DESC);
