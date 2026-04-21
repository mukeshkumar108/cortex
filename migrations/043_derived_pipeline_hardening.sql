-- Phase 1/2 derived pipeline hardening for the Gemma/Postgres synthesis path.
-- Additive only: runtime startbrief/handover source tables remain unchanged.

CREATE TABLE IF NOT EXISTS pipeline_runs (
  run_id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id TEXT NOT NULL,
  session_id TEXT,
  pass_name TEXT NOT NULL,
  model_version TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  policy_version TEXT,
  input_window_start TIMESTAMPTZ,
  input_window_end TIMESTAMPTZ,
  input_watermark TEXT,
  input_hash TEXT NOT NULL,
  output_hash TEXT,
  status TEXT NOT NULL CHECK (status IN ('pending','running','succeeded','failed')),
  attempt_count INT NOT NULL DEFAULT 0,
  error_code TEXT,
  error_message TEXT,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_runs_idempotency
  ON pipeline_runs (
    tenant_id,
    user_id,
    pass_name,
    input_hash,
    model_version,
    prompt_version,
    COALESCE(policy_version, '')
  );

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_user_pass_created
  ON pipeline_runs (tenant_id, user_id, pass_name, created_at DESC);

ALTER TABLE pipeline_checkpoints
  ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default',
  ADD COLUMN IF NOT EXISTS last_input_watermark TEXT,
  ADD COLUMN IF NOT EXISTS last_success_run_id BIGINT,
  ADD COLUMN IF NOT EXISTS last_output_hash TEXT,
  ADD COLUMN IF NOT EXISTS identity_signal_count_since_last INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS context_delta_count_since_last INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_pipeline_checkpoints_tenant_user_pipeline
  ON pipeline_checkpoints (tenant_id, user_id, pipeline_name);

CREATE TABLE IF NOT EXISTS derived_assertions (
  assertion_id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id TEXT NOT NULL,
  pass_name TEXT NOT NULL,
  surface TEXT NOT NULL,
  slot_key TEXT,
  statement_text TEXT NOT NULL,
  lifecycle_state TEXT NOT NULL CHECK (lifecycle_state IN ('active','superseded','resolved')),
  superseded_by_assertion_id BIGINT REFERENCES derived_assertions(assertion_id),
  resolved_at TIMESTAMPTZ,
  salience DOUBLE PRECISION,
  importance DOUBLE PRECISION,
  confidence_extraction DOUBLE PRECISION,
  confidence_validity DOUBLE PRECISION,
  source_session_ids TEXT[] NOT NULL DEFAULT '{}',
  source_turn_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  run_id BIGINT NOT NULL REFERENCES pipeline_runs(run_id),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  memory_layer TEXT CHECK (memory_layer IN ('LML','SML')),
  semantic_category TEXT,
  retention_floor DOUBLE PRECISION DEFAULT 0.0,
  last_accessed_at TIMESTAMPTZ,
  access_count INT NOT NULL DEFAULT 0,
  staleness_review_at TIMESTAMPTZ,
  staleness_status TEXT DEFAULT 'fresh',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_derived_assertions_user_surface_active
  ON derived_assertions (tenant_id, user_id, surface, lifecycle_state, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_derived_assertions_staleness
  ON derived_assertions (tenant_id, staleness_status, staleness_review_at)
  WHERE staleness_status IS DISTINCT FROM 'fresh';

CREATE TABLE IF NOT EXISTS derived_quarantine (
  quarantine_id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id TEXT NOT NULL,
  pass_name TEXT NOT NULL,
  session_id TEXT,
  reason_code TEXT NOT NULL CHECK (reason_code IN ('parse_failure','missing_evidence_refs','invalid_lifecycle_transition')),
  payload JSONB NOT NULL,
  run_id BIGINT REFERENCES pipeline_runs(run_id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_derived_quarantine_user_pass
  ON derived_quarantine (tenant_id, user_id, pass_name, created_at DESC);

ALTER TABLE session_classifications
  ADD COLUMN IF NOT EXISTS context_relevant BOOLEAN DEFAULT false;

ALTER TABLE open_threads
  ADD COLUMN IF NOT EXISTS lifecycle_state TEXT NOT NULL DEFAULT 'active',
  ADD COLUMN IF NOT EXISTS superseded_by_thread_id TEXT,
  ADD COLUMN IF NOT EXISTS evidence_turn_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS confidence_extraction DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS confidence_validity DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS memory_layer TEXT CHECK (memory_layer IN ('LML','SML')),
  ADD COLUMN IF NOT EXISTS semantic_category TEXT,
  ADD COLUMN IF NOT EXISTS retention_floor DOUBLE PRECISION DEFAULT 0.0,
  ADD COLUMN IF NOT EXISTS last_accessed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS access_count INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS staleness_review_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS staleness_status TEXT DEFAULT 'fresh';

ALTER TABLE identity_profile
  ADD COLUMN IF NOT EXISTS assertions JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE living_context
  ADD COLUMN IF NOT EXISTS assertions JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE entity_profiles
  ADD COLUMN IF NOT EXISTS memory_layer TEXT CHECK (memory_layer IN ('LML','SML')),
  ADD COLUMN IF NOT EXISTS last_accessed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS access_count INT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS consolidated_insights (
  insight_id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id TEXT NOT NULL,
  insight_type TEXT NOT NULL,
  statement_text TEXT NOT NULL,
  lifecycle_state TEXT NOT NULL DEFAULT 'active',
  source_assertion_ids BIGINT[] NOT NULL DEFAULT '{}',
  source_session_ids TEXT[] NOT NULL DEFAULT '{}',
  source_turn_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  salience DOUBLE PRECISION,
  importance DOUBLE PRECISION,
  confidence_validity DOUBLE PRECISION,
  promoted_to TEXT[] NOT NULL DEFAULT '{}',
  run_id BIGINT REFERENCES pipeline_runs(run_id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_consolidated_insights_user_active
  ON consolidated_insights (tenant_id, user_id, insight_type, lifecycle_state, updated_at DESC);
