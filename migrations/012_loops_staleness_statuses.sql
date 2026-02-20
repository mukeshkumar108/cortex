DO $$
BEGIN
    ALTER TYPE loop_status_enum ADD VALUE IF NOT EXISTS 'stale';
EXCEPTION
    WHEN duplicate_object THEN NULL;
END$$;

DO $$
BEGIN
    ALTER TYPE loop_status_enum ADD VALUE IF NOT EXISTS 'needs_review';
EXCEPTION
    WHEN duplicate_object THEN NULL;
END$$;
