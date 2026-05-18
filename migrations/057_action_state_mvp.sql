-- Phase 1 Action State MVP

CREATE TABLE IF NOT EXISTS action_items (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('todo','reminder','habit')),
  title TEXT NOT NULL,
  notes TEXT,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','done','cancelled','dismissed')),
  due_at TIMESTAMPTZ,
  remind_at TIMESTAMPTZ,
  recurrence_rule TEXT,
  source_type TEXT NOT NULL DEFAULT 'direct_user' CHECK (source_type IN ('direct_user','candidate_promotion','external_import','system')),
  source_ref JSONB,
  confidence NUMERIC,
  provenance_summary TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  dismissed_at TIMESTAMPTZ,
  cancelled_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_action_items_user_status_due
  ON action_items (tenant_id, user_id, status, due_at NULLS LAST, remind_at NULLS LAST, updated_at DESC);

ALTER TABLE actionable_candidates
  ADD COLUMN IF NOT EXISTS proposed_due_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS proposed_remind_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS promoted_action_item_id UUID,
  ADD COLUMN IF NOT EXISTS provenance_summary TEXT,
  ADD COLUMN IF NOT EXISTS confidence NUMERIC;

ALTER TABLE actionable_candidates
  DROP CONSTRAINT IF EXISTS actionable_candidates_status_check;

ALTER TABLE actionable_candidates
  ADD CONSTRAINT actionable_candidates_status_check
  CHECK (status IN ('detected','confirmed','dismissed','expired','needs_review','acted_on','superseded','stale'));

CREATE TABLE IF NOT EXISTS action_updates (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id TEXT NOT NULL,
  target_action_item_id UUID NOT NULL REFERENCES action_items(id) ON DELETE CASCADE,
  proposed_change TEXT NOT NULL CHECK (proposed_change IN ('mark_done','cancel','reschedule','snooze','dismiss')),
  proposed_due_at TIMESTAMPTZ,
  proposed_remind_at TIMESTAMPTZ,
  evidence_summary TEXT,
  confidence NUMERIC,
  status TEXT NOT NULL DEFAULT 'proposed' CHECK (status IN ('proposed','applied','rejected','expired')),
  source_type TEXT,
  source_ref JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  applied_at TIMESTAMPTZ,
  rejected_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_action_updates_user_status
  ON action_updates (tenant_id, user_id, status, created_at DESC);

CREATE TABLE IF NOT EXISTS action_audit_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL DEFAULT 'default',
  user_id TEXT NOT NULL,
  object_type TEXT NOT NULL CHECK (object_type IN ('action_item','action_candidate','action_update')),
  object_id TEXT NOT NULL,
  action TEXT NOT NULL,
  old_value JSONB,
  new_value JSONB,
  actor TEXT NOT NULL CHECK (actor IN ('sophie','synapse','system','user','extractor')),
  reason TEXT,
  provenance_summary TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_action_audit_log_user_created
  ON action_audit_log (tenant_id, user_id, created_at DESC);
