CREATE TABLE IF NOT EXISTS attention_outcomes (
  outcome_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  companion_id TEXT,
  attention_item_id TEXT NOT NULL,
  source_table TEXT,
  source_id TEXT,
  outcome_type TEXT NOT NULL
    CHECK (
      outcome_type IN (
        'surfaced',
        'acknowledged',
        'ignored',
        'dismissed',
        'snoozed',
        'completed',
        'helpful',
        'not_helpful',
        'dont_bring_up_again',
        'stale',
        'converted_to_task',
        'converted_to_draft',
        'expired'
      )
    ),
  outcome_reason TEXT,
  surface_mode TEXT,
  action_policy TEXT,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  snoozed_until TIMESTAMPTZ,
  suppress_until TIMESTAMPTZ,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_attention_outcomes_user_item_occurred
  ON attention_outcomes (tenant_id, user_id, attention_item_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_attention_outcomes_user_type_occurred
  ON attention_outcomes (tenant_id, user_id, outcome_type, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_attention_outcomes_user_source
  ON attention_outcomes (tenant_id, user_id, source_table, source_id);
