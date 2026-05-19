CREATE TABLE IF NOT EXISTS session_handover_packets (
  packet_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  summary TEXT NOT NULL,
  open_questions JSONB NOT NULL DEFAULT '[]'::jsonb,
  unresolved_decisions JSONB NOT NULL DEFAULT '[]'::jsonb,
  pending_actions JSONB NOT NULL DEFAULT '[]'::jsonb,
  recent_state_note TEXT,
  important_people JSONB NOT NULL DEFAULT '[]'::jsonb,
  active_topics JSONB NOT NULL DEFAULT '[]'::jsonb,
  do_not_overdo JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ NOT NULL,
  source_turn_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'expired')),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, user_id, session_id)
);

CREATE INDEX IF NOT EXISTS idx_session_handover_packets_user_created
  ON session_handover_packets (tenant_id, user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_session_handover_packets_user_expires
  ON session_handover_packets (tenant_id, user_id, expires_at DESC);
