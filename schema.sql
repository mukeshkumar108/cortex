-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Identity table (canonical facts - always present, never searched)
CREATE TABLE IF NOT EXISTS user_identity (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    data JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (tenant_id, user_id)
);

-- Identity cache table (Graphiti-derived, canonical for user-facing identity)
CREATE TABLE IF NOT EXISTS identity_cache (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    preferred_name TEXT,
    timezone TEXT,
    facts JSONB DEFAULT '{}'::jsonb,
    last_synced_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (tenant_id, user_id)
);

-- Loops table (procedural memory - commitments, habits, frictions, threads)
DO $$
BEGIN
    CREATE TYPE loop_type_enum AS ENUM ('commitment', 'decision', 'friction', 'habit', 'thread');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END$$;

DO $$
BEGIN
    CREATE TYPE loop_status_enum AS ENUM ('active', 'completed', 'dropped', 'snoozed');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END$$;

CREATE TABLE IF NOT EXISTS loops (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    persona_id TEXT NOT NULL,
    graphiti_entity_id TEXT,  -- Link to Graphiti graph entity
    type loop_type_enum NOT NULL,
    status loop_status_enum NOT NULL DEFAULT 'active',
    text TEXT NOT NULL,
    confidence REAL,
    salience INT,
    time_horizon TEXT,
    source_turn_ts TIMESTAMPTZ,
    due_date DATE,
    entity_refs JSONB DEFAULT '[]'::jsonb,
    tags JSONB DEFAULT '[]'::jsonb,
    embedding VECTOR(1536),  -- For semantic matching on completion
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ,
    completed_at TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb,
    CONSTRAINT loops_confidence_check CHECK (confidence >= 0 AND confidence <= 1),
    CONSTRAINT loops_salience_check CHECK (salience >= 1 AND salience <= 5),
    CONSTRAINT loops_time_horizon_check CHECK (time_horizon IN ('today', 'this_week', 'ongoing'))
);

-- Indexes for loops
CREATE INDEX IF NOT EXISTS idx_loops_tenant_user_status 
ON loops(tenant_id, user_id, status);

CREATE INDEX IF NOT EXISTS idx_loops_persona_status 
ON loops(persona_id, status);

CREATE INDEX IF NOT EXISTS idx_loops_created 
ON loops(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_loops_embedding 
ON loops USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);

-- Episodic semantic retrieval index (transcript windows, Graphiti-linked)
CREATE TABLE IF NOT EXISTS episodic_memory_embeddings (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    episode_uuid TEXT,
    unit_kind TEXT NOT NULL DEFAULT 'transcript_window',
    window_index INT NOT NULL DEFAULT 0,
    unit_text TEXT NOT NULL,
    reference_time TIMESTAMPTZ NOT NULL,
    embedding_model TEXT NOT NULL DEFAULT 'text-embedding-3-small',
    embedding VECTOR(1536) NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (tenant_id, user_id, session_id, window_index)
);

CREATE INDEX IF NOT EXISTS idx_episodic_memory_embeddings_scope
ON episodic_memory_embeddings(tenant_id, user_id, reference_time DESC);

CREATE INDEX IF NOT EXISTS idx_episodic_memory_embeddings_session
ON episodic_memory_embeddings(tenant_id, user_id, session_id);

CREATE INDEX IF NOT EXISTS idx_episodic_memory_embeddings_vector
ON episodic_memory_embeddings USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);

-- Session buffer (working memory - current conversation)
CREATE TABLE IF NOT EXISTS session_buffer (
    tenant_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    messages JSONB NOT NULL DEFAULT '[]'::jsonb,
    session_state JSONB NOT NULL DEFAULT '{}'::jsonb,
    rolling_summary TEXT,
    closed_at TIMESTAMPTZ,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (tenant_id, session_id),
    CONSTRAINT session_buffer_messages_len_check
        CHECK (jsonb_array_length(COALESCE(messages, '[]'::jsonb)) <= 12)
);

CREATE INDEX IF NOT EXISTS idx_session_buffer_user 
ON session_buffer(tenant_id, user_id);

-- Session transcript archive (evicted turns)
CREATE TABLE IF NOT EXISTS session_transcript (
    tenant_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    messages JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (tenant_id, session_id)
);

CREATE INDEX IF NOT EXISTS idx_session_transcript_tenant_session
ON session_transcript(tenant_id, session_id);

CREATE INDEX IF NOT EXISTS idx_session_transcript_tenant_user
ON session_transcript(tenant_id, user_id);

-- Graphiti outbox (reliable delivery for evicted turns)
CREATE TABLE IF NOT EXISTS graphiti_outbox (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    role TEXT NOT NULL,
    text TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', -- pending|sent|failed
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    folded_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    sent_at TIMESTAMPTZ,
    CONSTRAINT graphiti_outbox_pending_attempts_next_attempt
        CHECK (
            status <> 'pending'
            OR attempts = 0
            OR next_attempt_at IS NOT NULL
        )
);

CREATE INDEX IF NOT EXISTS idx_graphiti_outbox_status_tenant_user
ON graphiti_outbox(status, tenant_id, user_id);

CREATE INDEX IF NOT EXISTS idx_graphiti_outbox_tenant_session
ON graphiti_outbox(tenant_id, session_id);

CREATE INDEX IF NOT EXISTS idx_graphiti_outbox_next_attempt
ON graphiti_outbox(status, next_attempt_at);

-- ---------------------------------------------------------------------------
-- Synapse v2 additive canonical substrate (T2)
-- Implemented via migrations/035_synapse_v2_additive_schema.sql
-- This section documents table contracts only; rollout is controlled by flags.
-- ---------------------------------------------------------------------------

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
    PRIMARY KEY (tenant_id, session_id)
);

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
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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
    PRIMARY KEY (tenant_id, entity_id)
);

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
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS predicate_policy (
    predicate_policy_id BIGSERIAL PRIMARY KEY,
    policy_version TEXT NOT NULL,
    predicate TEXT NOT NULL,
    cardinality TEXT NOT NULL,
    conflict_rule TEXT NOT NULL,
    conflict_mode TEXT,
    expected_subject_kind TEXT,
    expected_object_type TEXT,
    expected_object_kind TEXT,
    equivalence_rule TEXT,
    object_equivalence_rule TEXT,
    is_active BOOLEAN NOT NULL DEFAULT FALSE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS claims_quarantine (
    quarantine_id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    session_id TEXT,
    extract_result_id BIGINT,
    extract_run_id UUID,
    candidate_payload JSONB NOT NULL,
    reason TEXT NOT NULL,
    reason_code TEXT NOT NULL DEFAULT 'quarantine_rule',
    confidence DOUBLE PRECISION,
    grounding_score DOUBLE PRECISION,
    quarantine_status TEXT NOT NULL DEFAULT 'pending',
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS canonical_mutations (
    mutation_id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT,
    mutation_type TEXT NOT NULL,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    claim_id BIGINT,
    entity_id UUID,
    source_extract_result_id BIGINT,
    source_run_id TEXT,
    resolver_version TEXT NOT NULL,
    tenant_sequence BIGINT NOT NULL,
    commit_status TEXT NOT NULL DEFAULT 'committed',
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    committed_by TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT canonical_mutations_object_type_nonempty_check CHECK (btrim(object_type) <> ''),
    CONSTRAINT canonical_mutations_object_id_nonempty_check CHECK (btrim(object_id) <> ''),
    CONSTRAINT canonical_mutations_commit_status_check CHECK (commit_status IN ('committed'))
);

CREATE TABLE IF NOT EXISTS canonical_tenant_watermarks (
    tenant_id TEXT PRIMARY KEY,
    last_sequence BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT canonical_tenant_watermarks_nonnegative_check CHECK (last_sequence >= 0)
);

CREATE TABLE IF NOT EXISTS projection_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    projection_type TEXT NOT NULL,
    projection_version TEXT NOT NULL,
    source_mutation_id BIGINT,
    payload JSONB NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS projection_latest (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    projection_type TEXT NOT NULL,
    projection_version TEXT NOT NULL,
    source_mutation_id BIGINT,
    payload JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (tenant_id, user_id, projection_type, projection_version)
);

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
    PRIMARY KEY (tenant_id, user_id, pipeline_name)
);

-- T2 hardening additions (post-audit)
-- Requires pgcrypto for gen_random_uuid() where UUID defaults are used.

CREATE TABLE IF NOT EXISTS predicate_policy_versions (
    policy_version TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'draft',
    description TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    activated_at TIMESTAMPTZ,
    retired_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS turn_ingest_idempotency (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    turn_id BIGINT NOT NULL,
    source TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (tenant_id, user_id, session_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS retrieval_shadow_diffs (
    shadow_id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    served_intent TEXT,
    request_fingerprint TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ok',
    served_latency_ms DOUBLE PRECISION,
    shadow_latency_ms DOUBLE PRECISION,
    latency_delta_ms DOUBLE PRECISION,
    error_text TEXT,
    diff_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    metrics_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT retrieval_shadow_diffs_status_check CHECK (status IN ('ok', 'shadow_error', 'skipped')),
    CONSTRAINT retrieval_shadow_diffs_endpoint_nonempty_check CHECK (btrim(endpoint) <> ''),
    CONSTRAINT retrieval_shadow_diffs_request_fingerprint_nonempty_check CHECK (btrim(request_fingerprint) <> '')
);

CREATE INDEX IF NOT EXISTS idx_retrieval_shadow_diffs_created
    ON retrieval_shadow_diffs (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_retrieval_shadow_diffs_tenant_created
    ON retrieval_shadow_diffs (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_retrieval_shadow_diffs_status_created
    ON retrieval_shadow_diffs (status, created_at DESC);

CREATE TABLE IF NOT EXISTS invariant_violations (
    violation_id BIGSERIAL PRIMARY KEY,
    invariant_code TEXT NOT NULL,
    severity TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    user_id TEXT,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'open',
    requires_human_review BOOLEAN NOT NULL DEFAULT TRUE,
    first_detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    repaired_at TIMESTAMPTZ,
    occurrence_count INT NOT NULL DEFAULT 1,
    fingerprint TEXT NOT NULL,
    detected_by TEXT NOT NULL DEFAULT 't13.invariants.v1',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT invariant_violations_severity_check CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    CONSTRAINT invariant_violations_status_check CHECK (status IN ('open', 'review_required', 'auto_repaired', 'ignored', 'failed')),
    CONSTRAINT invariant_violations_invariant_nonempty_check CHECK (btrim(invariant_code) <> ''),
    CONSTRAINT invariant_violations_object_type_nonempty_check CHECK (btrim(object_type) <> ''),
    CONSTRAINT invariant_violations_object_id_nonempty_check CHECK (btrim(object_id) <> ''),
    CONSTRAINT invariant_violations_fingerprint_nonempty_check CHECK (btrim(fingerprint) <> ''),
    CONSTRAINT invariant_violations_occurrence_positive_check CHECK (occurrence_count >= 1)
);

CREATE INDEX IF NOT EXISTS idx_invariant_violations_tenant_status_detected
    ON invariant_violations (tenant_id, status, last_detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_invariant_violations_code_status_detected
    ON invariant_violations (invariant_code, status, last_detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_invariant_violations_fingerprint_open
    ON invariant_violations (fingerprint)
    WHERE status IN ('open', 'review_required');

CREATE TABLE IF NOT EXISTS invariant_repair_actions (
    repair_action_id BIGSERIAL PRIMARY KEY,
    violation_id BIGINT NOT NULL,
    tenant_id TEXT NOT NULL,
    action_code TEXT NOT NULL,
    action_mode TEXT NOT NULL,
    status TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_at TIMESTAMPTZ,
    CONSTRAINT invariant_repair_actions_violation_fk
      FOREIGN KEY (violation_id)
      REFERENCES invariant_violations (violation_id)
      ON DELETE CASCADE,
    CONSTRAINT invariant_repair_actions_mode_check CHECK (action_mode IN ('auto', 'manual', 'review')),
    CONSTRAINT invariant_repair_actions_status_check CHECK (status IN ('applied', 'blocked', 'failed', 'skipped')),
    CONSTRAINT invariant_repair_actions_action_nonempty_check CHECK (btrim(action_code) <> '')
);

CREATE INDEX IF NOT EXISTS idx_invariant_repair_actions_tenant_created
    ON invariant_repair_actions (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_invariant_repair_actions_status_created
    ON invariant_repair_actions (status, created_at DESC);

CREATE TABLE IF NOT EXISTS retrieval_rollout_control (
    control_id BOOLEAN PRIMARY KEY DEFAULT TRUE,
    mode TEXT NOT NULL DEFAULT 'shadow_only',
    cohort_tenants JSONB NOT NULL DEFAULT '[]'::jsonb,
    cohort_users JSONB NOT NULL DEFAULT '[]'::jsonb,
    cohort_percentage INT NOT NULL DEFAULT 0,
    threshold_evidence_regression DOUBLE PRECISION NOT NULL DEFAULT 0.08,
    threshold_active_slot_conflicts INT NOT NULL DEFAULT 1,
    threshold_replay_divergence INT NOT NULL DEFAULT 1,
    threshold_latency_regression DOUBLE PRECISION NOT NULL DEFAULT 0.20,
    threshold_quality_regression DOUBLE PRECISION NOT NULL DEFAULT 0.20,
    rollback_active BOOLEAN NOT NULL DEFAULT FALSE,
    rollback_reason TEXT,
    updated_by TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT retrieval_rollout_control_mode_check CHECK (mode IN ('legacy_only', 'shadow_only', 'cohort_v2', 'v2_all')),
    CONSTRAINT retrieval_rollout_control_percentage_check CHECK (cohort_percentage >= 0 AND cohort_percentage <= 100)
);

CREATE TABLE IF NOT EXISTS retrieval_rollout_events (
    event_id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT retrieval_rollout_events_type_check CHECK (event_type IN ('state_update', 'evaluation', 'rollback_triggered'))
);

CREATE INDEX IF NOT EXISTS idx_retrieval_rollout_events_created
    ON retrieval_rollout_events (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_retrieval_rollout_events_type_created
    ON retrieval_rollout_events (event_type, created_at DESC);

-- Phase 2 meaning-first memory primitives.
-- Additive only. These tables support synthesis surfaces; they do not replace the
-- six-pass derived tables and they are not canonical serving authority.

CREATE TABLE IF NOT EXISTS low_confidence_items (
  item_id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id TEXT NOT NULL,
  surface TEXT NOT NULL,
  statement_text TEXT NOT NULL,
  question_text TEXT,
  confidence DOUBLE PRECISION,
  status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','asked','answered','dismissed','expired')),
  source_session_ids TEXT[] NOT NULL DEFAULT '{}',
  source_turn_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  run_id BIGINT REFERENCES pipeline_runs(run_id),
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_asked_at TIMESTAMPTZ,
  resolved_at TIMESTAMPTZ,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_low_confidence_items_user_status
  ON low_confidence_items (tenant_id, user_id, status, last_seen_at DESC);

CREATE TABLE IF NOT EXISTS memory_contradictions (
  contradiction_id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id TEXT NOT NULL,
  topic TEXT NOT NULL,
  earlier_view TEXT NOT NULL,
  recent_view TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','resolved','superseded')),
  source_session_ids TEXT[] NOT NULL DEFAULT '{}',
  source_turn_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at TIMESTAMPTZ,
  run_id BIGINT REFERENCES pipeline_runs(run_id),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_memory_contradictions_user_status
  ON memory_contradictions (tenant_id, user_id, status, last_seen_at DESC);

CREATE TABLE IF NOT EXISTS memory_events (
  event_id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id TEXT NOT NULL,
  event_type TEXT NOT NULL CHECK (event_type IN ('past_event','upcoming_event','commitment','anniversary','deadline')),
  title TEXT NOT NULL,
  description TEXT,
  event_time TIMESTAMPTZ,
  time_confidence DOUBLE PRECISION,
  lifecycle_state TEXT NOT NULL DEFAULT 'active' CHECK (lifecycle_state IN ('active','resolved','superseded')),
  source_session_ids TEXT[] NOT NULL DEFAULT '{}',
  source_turn_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  run_id BIGINT REFERENCES pipeline_runs(run_id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_memory_events_user_state_time
  ON memory_events (tenant_id, user_id, lifecycle_state, event_time NULLS LAST, updated_at DESC);

CREATE TABLE IF NOT EXISTS memory_silence_flags (
  flag_id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id TEXT NOT NULL,
  target_type TEXT NOT NULL CHECK (target_type IN ('entity','thread')),
  target_id TEXT NOT NULL,
  target_name TEXT NOT NULL,
  last_seen_at TIMESTAMPTZ NOT NULL,
  silence_days INT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','acknowledged','dismissed','resolved')),
  source TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (tenant_id, user_id, target_type, target_id, status)
);

CREATE INDEX IF NOT EXISTS idx_memory_silence_flags_user_status
  ON memory_silence_flags (tenant_id, user_id, status, silence_days DESC, updated_at DESC);

CREATE TABLE IF NOT EXISTS memory_relationship_links (
  link_id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id TEXT NOT NULL,
  source_type TEXT NOT NULL,
  source_id TEXT NOT NULL,
  target_type TEXT NOT NULL,
  target_id TEXT NOT NULL,
  relationship_type TEXT NOT NULL,
  confidence DOUBLE PRECISION,
  source_session_ids TEXT[] NOT NULL DEFAULT '{}',
  source_turn_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (tenant_id, user_id, source_type, source_id, target_type, target_id, relationship_type)
);

CREATE INDEX IF NOT EXISTS idx_memory_relationship_links_user_source
  ON memory_relationship_links (tenant_id, user_id, source_type, source_id);

-- Lightweight temporal reinforcement for derived memory.
ALTER TABLE entity_profiles
  ADD COLUMN IF NOT EXISTS distinct_session_count INT NOT NULL DEFAULT 0;

ALTER TABLE entity_profiles
  ADD COLUMN IF NOT EXISTS reinforcement_count INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_reinforced_at TIMESTAMPTZ;

ALTER TABLE open_threads
  ADD COLUMN IF NOT EXISTS distinct_session_count INT NOT NULL DEFAULT 0;

ALTER TABLE derived_assertions
  ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS distinct_session_count INT NOT NULL DEFAULT 0;
