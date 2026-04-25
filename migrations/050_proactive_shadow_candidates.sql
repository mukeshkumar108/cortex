-- Shadow-mode proactive candidate queues.
-- These are additive, read/write only for internal evaluation; no user-facing delivery path.

CREATE TABLE IF NOT EXISTS follow_up_candidates (
  candidate_id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id TEXT NOT NULL,
  candidate_key TEXT NOT NULL,
  title TEXT NOT NULL,
  reason TEXT,
  suggested_prompt TEXT,
  source_surface TEXT NOT NULL DEFAULT 'open_thread',
  source_ref TEXT,
  priority_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  due_at TIMESTAMPTZ,
  source_session_ids TEXT[] NOT NULL DEFAULT '{}',
  source_turn_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  status TEXT NOT NULL DEFAULT 'shadow_open'
    CHECK (status IN ('shadow_open','shadow_stale','shadow_dismissed','shadow_sent')),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, user_id, candidate_key)
);

CREATE INDEX IF NOT EXISTS idx_follow_up_candidates_user_status
  ON follow_up_candidates (tenant_id, user_id, status, priority_score DESC, due_at NULLS LAST, updated_at DESC);

CREATE TABLE IF NOT EXISTS clarification_candidates (
  candidate_id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id TEXT NOT NULL,
  candidate_key TEXT NOT NULL,
  title TEXT NOT NULL,
  reason TEXT,
  suggested_prompt TEXT,
  source_surface TEXT NOT NULL DEFAULT 'low_confidence_item',
  source_ref TEXT,
  priority_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  due_at TIMESTAMPTZ,
  source_session_ids TEXT[] NOT NULL DEFAULT '{}',
  source_turn_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  status TEXT NOT NULL DEFAULT 'shadow_open'
    CHECK (status IN ('shadow_open','shadow_stale','shadow_dismissed','shadow_sent')),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, user_id, candidate_key)
);

CREATE INDEX IF NOT EXISTS idx_clarification_candidates_user_status
  ON clarification_candidates (tenant_id, user_id, status, priority_score DESC, updated_at DESC);

CREATE TABLE IF NOT EXISTS recent_change_candidates (
  candidate_id BIGSERIAL PRIMARY KEY,
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id TEXT NOT NULL,
  candidate_key TEXT NOT NULL,
  title TEXT NOT NULL,
  reason TEXT,
  suggested_prompt TEXT,
  source_surface TEXT NOT NULL DEFAULT 'derived_assertion',
  source_ref TEXT,
  priority_score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  due_at TIMESTAMPTZ,
  source_session_ids TEXT[] NOT NULL DEFAULT '{}',
  source_turn_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  status TEXT NOT NULL DEFAULT 'shadow_open'
    CHECK (status IN ('shadow_open','shadow_stale','shadow_dismissed','shadow_sent')),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  last_computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, user_id, candidate_key)
);

CREATE INDEX IF NOT EXISTS idx_recent_change_candidates_user_status
  ON recent_change_candidates (tenant_id, user_id, status, priority_score DESC, updated_at DESC);
