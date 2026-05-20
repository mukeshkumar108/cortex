CREATE TABLE IF NOT EXISTS session_summaries (
  summary_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  summary_text TEXT,
  bridge_text TEXT,
  key_points JSONB NOT NULL DEFAULT '[]'::jsonb,
  decisions JSONB NOT NULL DEFAULT '[]'::jsonb,
  unresolved_items JSONB NOT NULL DEFAULT '[]'::jsonb,
  pending_actions JSONB NOT NULL DEFAULT '[]'::jsonb,
  people_mentioned JSONB NOT NULL DEFAULT '[]'::jsonb,
  events_mentioned JSONB NOT NULL DEFAULT '[]'::jsonb,
  state_signals JSONB NOT NULL DEFAULT '[]'::jsonb,
  topics JSONB NOT NULL DEFAULT '[]'::jsonb,
  do_not_overinfer JSONB NOT NULL DEFAULT '[]'::jsonb,
  evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  extra_attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
  model TEXT,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_session_summaries_tenant_user_session
  ON session_summaries(tenant_id, user_id, session_id);
