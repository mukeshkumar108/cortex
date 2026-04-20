-- T14: cohort rollout + rollback controls

CREATE TABLE IF NOT EXISTS retrieval_rollout_control (
    control_id BOOLEAN PRIMARY KEY DEFAULT TRUE,
    mode TEXT NOT NULL DEFAULT 'shadow_only',
    cohort_tenants JSONB NOT NULL DEFAULT '[]'::jsonb,
    cohort_users JSONB NOT NULL DEFAULT '[]'::jsonb,
    cohort_percentage INT NOT NULL DEFAULT 0,
    threshold_evidence_regression DOUBLE PRECISION NOT NULL DEFAULT 0.08,
    threshold_active_slot_conflicts INT NOT NULL DEFAULT 1,
    threshold_replay_divergence INT NOT NULL DEFAULT 1,
    threshold_latency_regression DOUBLE PRECISION NOT NULL DEFAULT 0.20,
    threshold_quality_regression DOUBLE PRECISION NOT NULL DEFAULT 0.20,
    rollback_active BOOLEAN NOT NULL DEFAULT FALSE,
    rollback_reason TEXT,
    updated_by TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT retrieval_rollout_control_mode_check CHECK (mode IN ('legacy_only', 'shadow_only', 'cohort_v2', 'v2_all')),
    CONSTRAINT retrieval_rollout_control_percentage_check CHECK (cohort_percentage >= 0 AND cohort_percentage <= 100)
);

INSERT INTO retrieval_rollout_control (control_id)
VALUES (TRUE)
ON CONFLICT (control_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS retrieval_rollout_events (
    event_id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT retrieval_rollout_events_type_check CHECK (event_type IN ('state_update', 'evaluation', 'rollback_triggered'))
);

CREATE INDEX IF NOT EXISTS idx_retrieval_rollout_events_created
    ON retrieval_rollout_events (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_retrieval_rollout_events_type_created
    ON retrieval_rollout_events (event_type, created_at DESC);
