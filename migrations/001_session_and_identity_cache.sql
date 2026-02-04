-- Migration 001: Session Buffer PK Change + Identity Cache
--
-- This migration:
-- 1. Changes session_buffer PK to (tenant_id, session_id)
-- 2. Adds rolling_summary and closed_at to session_buffer
-- 3. Creates identity_cache table
--
-- IMPORTANT: Run during low-traffic period. This recreates session_buffer.

BEGIN;

-- Step 1: Create new session_buffer table with correct PK
CREATE TABLE session_buffer_new (
    tenant_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    messages JSONB NOT NULL DEFAULT '[]'::jsonb,
    rolling_summary TEXT,  -- LLM-compressed history
    session_state JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    closed_at TIMESTAMP,  -- NULL if active, set when gap > 30 min detected
    PRIMARY KEY (tenant_id, session_id)
);

-- Step 2: Create indexes for new session_buffer
CREATE INDEX idx_session_buffer_new_user
    ON session_buffer_new(tenant_id, user_id);

CREATE INDEX idx_session_buffer_new_closed
    ON session_buffer_new(closed_at) WHERE closed_at IS NOT NULL;

CREATE INDEX idx_session_buffer_new_updated
    ON session_buffer_new(updated_at DESC);

-- Step 3: Migrate data from old session_buffer (if it exists)
-- This assumes tenant_id can be inferred or defaulted
-- Adjust based on your data migration needs
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_name = 'session_buffer') THEN
        -- If you have a way to infer tenant_id, add it here
        -- For now, we'll skip migration and let new sessions create fresh data
        -- INSERT INTO session_buffer_new
        -- SELECT 'default-tenant', session_id, user_id, messages,
        --        NULL, session_state, created_at, updated_at, NULL
        -- FROM session_buffer;

        -- Drop old table
        DROP TABLE session_buffer;
    END IF;
END $$;

-- Step 4: Rename new table to session_buffer
ALTER TABLE session_buffer_new RENAME TO session_buffer;

-- Step 5: Create identity_cache table
-- This is derived FROM Graphiti, never from regex extraction
CREATE TABLE IF NOT EXISTS identity_cache (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    preferred_name TEXT,  -- Only if different from auth name
    timezone TEXT,        -- User setting, not inferred
    facts JSONB DEFAULT '{}'::jsonb,  -- Derived from Graphiti
    last_synced_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (tenant_id, user_id)
);

-- Step 6: Create indexes for identity_cache
CREATE INDEX idx_identity_cache_synced
    ON identity_cache(last_synced_at DESC);

-- Step 7: Update user_identity to remove conflicting data
-- Keep user_identity for backward compatibility but deprecate extraction
-- Add a flag to track if it's from extraction or real data
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_name = 'user_identity') THEN
        -- Add isDefault flag if it doesn't exist
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                      WHERE table_name = 'user_identity'
                      AND column_name = 'is_deprecated') THEN
            ALTER TABLE user_identity ADD COLUMN is_deprecated BOOLEAN DEFAULT FALSE;
        END IF;

        -- Mark all existing extracted identities as deprecated
        UPDATE user_identity
        SET is_deprecated = TRUE
        WHERE data->>'isDefault' = 'true';
    END IF;
END $$;

COMMIT;

-- Verification queries (run these after migration)
-- SELECT COUNT(*) FROM session_buffer WHERE tenant_id IS NOT NULL;
-- SELECT COUNT(*) FROM identity_cache;
-- SELECT * FROM session_buffer LIMIT 5;
-- SELECT * FROM identity_cache LIMIT 5;
