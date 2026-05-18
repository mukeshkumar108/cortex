CREATE TABLE IF NOT EXISTS entity_candidates (
  candidate_id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id TEXT NOT NULL,
  session_id TEXT,
  run_id BIGINT REFERENCES pipeline_runs(run_id),
  candidate_key TEXT NOT NULL,
  name TEXT NOT NULL,
  candidate_type TEXT NOT NULL
    CHECK (candidate_type IN ('person','project','place','other')),
  summary TEXT,
  source TEXT NOT NULL DEFAULT 'chat',
  provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
  confidence_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  confidence_label TEXT NOT NULL DEFAULT 'medium'
    CHECK (confidence_label IN ('low','medium','high')),
  status TEXT NOT NULL DEFAULT 'detected'
    CHECK (status IN ('detected','needs_review','resolved','dismissed','superseded')),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, user_id, candidate_key)
);

CREATE INDEX IF NOT EXISTS idx_entity_candidates_user_status_updated
  ON entity_candidates (tenant_id, user_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_entity_candidates_user_type_status
  ON entity_candidates (tenant_id, user_id, candidate_type, status, updated_at DESC);
