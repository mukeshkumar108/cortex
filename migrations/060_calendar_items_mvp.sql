-- Calendar state MVP: canonical calendar items only.

CREATE TABLE IF NOT EXISTS calendar_items (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id TEXT NOT NULL,
  title TEXT NOT NULL,
  description TEXT,
  notes TEXT,
  starts_at TIMESTAMPTZ NOT NULL,
  ends_at TIMESTAMPTZ,
  timezone TEXT NOT NULL DEFAULT 'UTC',
  all_day BOOLEAN NOT NULL DEFAULT FALSE,
  location TEXT,
  participants JSONB NOT NULL DEFAULT '[]'::jsonb,
  organizer JSONB,
  rsvp_status TEXT NOT NULL DEFAULT 'unknown'
    CHECK (rsvp_status IN ('unknown','needs_action','accepted','declined','tentative')),
  status TEXT NOT NULL DEFAULT 'confirmed'
    CHECK (status IN ('confirmed','cancelled','archived')),
  source_kind TEXT NOT NULL DEFAULT 'manual'
    CHECK (source_kind IN ('manual','chat','email','whatsapp','instagram','google_calendar','system','candidate_promotion')),
  source_ref JSONB,
  evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  provenance_summary TEXT,
  confidence NUMERIC,
  external_provider TEXT,
  external_id TEXT,
  external_calendar_id TEXT,
  external_etag TEXT,
  external_updated_at TIMESTAMPTZ,
  sync_status TEXT NOT NULL DEFAULT 'not_synced'
    CHECK (sync_status IN ('not_synced','synced','pending_create','pending_update','pending_delete','conflict','imported')),
  recurrence_rule TEXT,
  recurrence_parent_id UUID REFERENCES calendar_items(id) ON DELETE SET NULL,
  dedupe_key TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  cancelled_at TIMESTAMPTZ,
  archived_at TIMESTAMPTZ,
  CHECK (ends_at IS NULL OR ends_at > starts_at)
);

CREATE INDEX IF NOT EXISTS idx_calendar_items_user_range
  ON calendar_items (tenant_id, user_id, starts_at, ends_at);

CREATE INDEX IF NOT EXISTS idx_calendar_items_user_status_range
  ON calendar_items (tenant_id, user_id, status, starts_at);

CREATE INDEX IF NOT EXISTS idx_calendar_items_user_source
  ON calendar_items (tenant_id, user_id, source_kind, updated_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS calendar_items_external_unique
  ON calendar_items (tenant_id, user_id, external_provider, external_id)
  WHERE external_provider IS NOT NULL AND external_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS calendar_items_dedupe_unique
  ON calendar_items (tenant_id, user_id, dedupe_key)
  WHERE dedupe_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS calendar_audit_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id TEXT NOT NULL,
  object_type TEXT NOT NULL CHECK (object_type IN ('calendar_item','calendar_sync')),
  object_id TEXT NOT NULL,
  action TEXT NOT NULL,
  old_value JSONB,
  new_value JSONB,
  actor TEXT NOT NULL CHECK (actor IN ('sophie','synapse','system','user','external_provider')),
  reason TEXT,
  provenance_summary TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_calendar_audit_log_user_created
  ON calendar_audit_log (tenant_id, user_id, created_at DESC);
