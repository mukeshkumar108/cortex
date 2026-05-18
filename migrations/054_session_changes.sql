CREATE TABLE IF NOT EXISTS session_changes (
  change_id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id TEXT NOT NULL,
  session_id TEXT,
  run_id BIGINT REFERENCES pipeline_runs(run_id),
  change_key TEXT NOT NULL,
  kind TEXT NOT NULL
    CHECK (kind IN ('factual_update','state_change','decision_change','schedule_change','focus_change')),
  title TEXT NOT NULL,
  summary TEXT,
  effective_iso TIMESTAMPTZ,
  source TEXT NOT NULL DEFAULT 'chat',
  provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
  confidence_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  confidence_label TEXT NOT NULL DEFAULT 'medium'
    CHECK (confidence_label IN ('low','medium','high')),
  status TEXT NOT NULL DEFAULT 'detected'
    CHECK (status IN ('detected','needs_review','confirmed','dismissed','superseded','stale')),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, user_id, change_key)
);

CREATE INDEX IF NOT EXISTS idx_session_changes_user_status_effective
  ON session_changes (tenant_id, user_id, status, effective_iso NULLS LAST, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_session_changes_user_kind_status
  ON session_changes (tenant_id, user_id, kind, status, updated_at DESC);
