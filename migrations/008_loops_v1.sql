DO $$
BEGIN
    CREATE TYPE loop_type_enum AS ENUM ('commitment', 'decision', 'friction', 'habit', 'thread');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END$$;

DO $$
BEGIN
    CREATE TYPE loop_status_enum AS ENUM ('active', 'completed', 'dropped', 'snoozed');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END$$;

ALTER TABLE loops
    DROP CONSTRAINT IF EXISTS loops_type_check,
    DROP CONSTRAINT IF EXISTS loops_status_check;

ALTER TABLE loops
    ALTER COLUMN status DROP DEFAULT;

ALTER TABLE loops
    ALTER COLUMN type TYPE loop_type_enum
    USING (type::loop_type_enum);

ALTER TABLE loops
    ALTER COLUMN status TYPE loop_status_enum
    USING (
        CASE status
            WHEN 'pending' THEN 'active'
            WHEN 'completed' THEN 'completed'
            WHEN 'skipped' THEN 'dropped'
            WHEN 'archived' THEN 'dropped'
            ELSE 'active'
        END::loop_status_enum
    );

ALTER TABLE loops
    ALTER COLUMN status SET DEFAULT 'active';

ALTER TABLE loops
    ADD COLUMN IF NOT EXISTS confidence REAL,
    ADD COLUMN IF NOT EXISTS salience INT,
    ADD COLUMN IF NOT EXISTS time_horizon TEXT,
    ADD COLUMN IF NOT EXISTS source_turn_ts TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS due_date DATE,
    ADD COLUMN IF NOT EXISTS entity_refs JSONB DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS tags JSONB DEFAULT '[]'::jsonb;

UPDATE loops
SET confidence = COALESCE(confidence, 0.7),
    salience = COALESCE(salience, 3),
    time_horizon = COALESCE(time_horizon, 'ongoing'),
    source_turn_ts = COALESCE(source_turn_ts, created_at),
    updated_at = COALESCE(updated_at, created_at),
    last_seen_at = COALESCE(last_seen_at, created_at);

ALTER TABLE loops
    ADD CONSTRAINT loops_confidence_check
    CHECK (confidence >= 0 AND confidence <= 1);

ALTER TABLE loops
    ADD CONSTRAINT loops_salience_check
    CHECK (salience >= 1 AND salience <= 5);

ALTER TABLE loops
    ADD CONSTRAINT loops_time_horizon_check
    CHECK (time_horizon IN ('today', 'this_week', 'ongoing'));
