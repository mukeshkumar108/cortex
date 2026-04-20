-- T13: continuous invariants + repair governance

CREATE TABLE IF NOT EXISTS invariant_violations (
    violation_id BIGSERIAL PRIMARY KEY,
    invariant_code TEXT NOT NULL,
    severity TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    user_id TEXT,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'open',
    requires_human_review BOOLEAN NOT NULL DEFAULT TRUE,
    first_detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    repaired_at TIMESTAMPTZ,
    occurrence_count INT NOT NULL DEFAULT 1,
    fingerprint TEXT NOT NULL,
    detected_by TEXT NOT NULL DEFAULT 't13.invariants.v1',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT invariant_violations_severity_check CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    CONSTRAINT invariant_violations_status_check CHECK (status IN ('open', 'review_required', 'auto_repaired', 'ignored', 'failed')),
    CONSTRAINT invariant_violations_invariant_nonempty_check CHECK (btrim(invariant_code) <> ''),
    CONSTRAINT invariant_violations_object_type_nonempty_check CHECK (btrim(object_type) <> ''),
    CONSTRAINT invariant_violations_object_id_nonempty_check CHECK (btrim(object_id) <> ''),
    CONSTRAINT invariant_violations_fingerprint_nonempty_check CHECK (btrim(fingerprint) <> ''),
    CONSTRAINT invariant_violations_occurrence_positive_check CHECK (occurrence_count >= 1)
);

CREATE INDEX IF NOT EXISTS idx_invariant_violations_tenant_status_detected
    ON invariant_violations (tenant_id, status, last_detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_invariant_violations_code_status_detected
    ON invariant_violations (invariant_code, status, last_detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_invariant_violations_fingerprint_open
    ON invariant_violations (fingerprint)
    WHERE status IN ('open', 'review_required');

CREATE TABLE IF NOT EXISTS invariant_repair_actions (
    repair_action_id BIGSERIAL PRIMARY KEY,
    violation_id BIGINT NOT NULL,
    tenant_id TEXT NOT NULL,
    action_code TEXT NOT NULL,
    action_mode TEXT NOT NULL,
    status TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_at TIMESTAMPTZ,
    CONSTRAINT invariant_repair_actions_violation_fk
      FOREIGN KEY (violation_id)
      REFERENCES invariant_violations (violation_id)
      ON DELETE CASCADE,
    CONSTRAINT invariant_repair_actions_mode_check CHECK (action_mode IN ('auto', 'manual', 'review')),
    CONSTRAINT invariant_repair_actions_status_check CHECK (status IN ('applied', 'blocked', 'failed', 'skipped')),
    CONSTRAINT invariant_repair_actions_action_nonempty_check CHECK (btrim(action_code) <> '')
);

CREATE INDEX IF NOT EXISTS idx_invariant_repair_actions_tenant_created
    ON invariant_repair_actions (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_invariant_repair_actions_status_created
    ON invariant_repair_actions (status, created_at DESC);
