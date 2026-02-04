-- Phase 4 schema updates

-- Create identity_cache table
CREATE TABLE IF NOT EXISTS identity_cache (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    preferred_name TEXT,
    timezone TEXT,
    facts JSONB DEFAULT '{}',
    last_synced_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (tenant_id, user_id)
);

-- Add columns to session_buffer
ALTER TABLE session_buffer 
    ADD COLUMN IF NOT EXISTS rolling_summary TEXT,
    ADD COLUMN IF NOT EXISTS closed_at TIMESTAMPTZ;

-- Fix session_buffer PK (requires table recreation)
-- Step 1: Create new table with correct PK
CREATE TABLE IF NOT EXISTS session_buffer_new (
    tenant_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    messages JSONB DEFAULT '[]',
    session_state JSONB DEFAULT '{}',
    rolling_summary TEXT,
    closed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (tenant_id, session_id)
);

-- Step 2: Copy data
INSERT INTO session_buffer_new 
SELECT tenant_id, session_id, user_id, messages, session_state, 
       NULL as rolling_summary, NULL as closed_at, 
       created_at, updated_at
FROM session_buffer
ON CONFLICT DO NOTHING;

-- Step 3: Drop old table and rename
DROP TABLE session_buffer;
ALTER TABLE session_buffer_new RENAME TO session_buffer;

-- Create index for user lookups
CREATE INDEX IF NOT EXISTS idx_session_buffer_user 
ON session_buffer(tenant_id, user_id);
