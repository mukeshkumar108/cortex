CREATE TABLE IF NOT EXISTS timeline_events (
  event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  timeline_type TEXT NOT NULL
    CHECK (
      timeline_type IN (
        'interaction',
        'user_life',
        'calendar_event',
        'relationship',
        'system'
      )
    ),
  event_type TEXT NOT NULL,
  domain TEXT,
  title TEXT,
  summary TEXT,
  occurred_at TIMESTAMPTZ,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  valid_from TIMESTAMPTZ,
  valid_until TIMESTAMPTZ,
  expires_at TIMESTAMPTZ,
  status TEXT NOT NULL DEFAULT 'active'
    CHECK (
      status IN (
        'active',
        'confirmed',
        'provisional',
        'stale',
        'historical',
        'suppressed',
        'superseded',
        'dismissed',
        'completed',
        'archived'
      )
    ),
  confidence DOUBLE PRECISION,
  salience DOUBLE PRECISION,
  actor TEXT,
  subject TEXT,
  object_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  source_table TEXT,
  source_id TEXT,
  evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  user_corrected BOOLEAN NOT NULL DEFAULT FALSE,
  user_visible BOOLEAN NOT NULL DEFAULT TRUE,
  effect TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_timeline_events_user_type_occurred
  ON timeline_events (tenant_id, user_id, timeline_type, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_timeline_events_user_event_observed
  ON timeline_events (tenant_id, user_id, event_type, observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_timeline_events_user_status_type
  ON timeline_events (tenant_id, user_id, status, timeline_type);

CREATE INDEX IF NOT EXISTS idx_timeline_events_user_source
  ON timeline_events (tenant_id, user_id, source_table, source_id);
