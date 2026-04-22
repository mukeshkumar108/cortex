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
