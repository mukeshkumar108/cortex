-- Actionable user-state candidates extracted during ingest.
-- Additive only: no external action execution.

CREATE TABLE IF NOT EXISTS actionable_candidates (
  candidate_id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id TEXT NOT NULL,
  session_id TEXT,
  run_id BIGINT REFERENCES pipeline_runs(run_id),
  candidate_key TEXT NOT NULL,
  record_type TEXT NOT NULL
    CHECK (record_type IN ('event_candidate','task_candidate','reminder_candidate','commitment')),
  title TEXT NOT NULL,
  summary TEXT,
  due_iso TIMESTAMPTZ,
  relevant_from_iso TIMESTAMPTZ,
  relevant_until_iso TIMESTAMPTZ,
  suggested_action TEXT,
  linked_external_id TEXT,
  linked_external_type TEXT,
  source TEXT NOT NULL DEFAULT 'chat',
  provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
  confidence_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  confidence_label TEXT NOT NULL DEFAULT 'medium'
    CHECK (confidence_label IN ('low','medium','high')),
  status TEXT NOT NULL DEFAULT 'detected'
    CHECK (status IN ('detected','needs_review','confirmed','dismissed','acted_on','superseded','stale')),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, user_id, candidate_key)
);

CREATE INDEX IF NOT EXISTS idx_actionable_candidates_user_status_due
  ON actionable_candidates (tenant_id, user_id, status, due_iso NULLS LAST, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_actionable_candidates_user_type_status
  ON actionable_candidates (tenant_id, user_id, record_type, status, updated_at DESC);
