-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Identity table (canonical facts - always present, never searched)
CREATE TABLE IF NOT EXISTS user_identity (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    data JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (tenant_id, user_id)
);

-- Identity cache table (Graphiti-derived, canonical for user-facing identity)
CREATE TABLE IF NOT EXISTS identity_cache (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    preferred_name TEXT,
    timezone TEXT,
    facts JSONB DEFAULT '{}'::jsonb,
    last_synced_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (tenant_id, user_id)
);

-- Loops table (procedural memory - commitments, habits, frictions, threads)
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

CREATE TABLE IF NOT EXISTS loops (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    persona_id TEXT NOT NULL,
    graphiti_entity_id TEXT,  -- Link to Graphiti graph entity
    type loop_type_enum NOT NULL,
    status loop_status_enum NOT NULL DEFAULT 'active',
    text TEXT NOT NULL,
    confidence REAL,
    salience INT,
    time_horizon TEXT,
    source_turn_ts TIMESTAMPTZ,
    due_date DATE,
    entity_refs JSONB DEFAULT '[]'::jsonb,
    tags JSONB DEFAULT '[]'::jsonb,
    embedding VECTOR(1536),  -- For semantic matching on completion
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ,
    completed_at TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb,
    CONSTRAINT loops_confidence_check CHECK (confidence >= 0 AND confidence <= 1),
    CONSTRAINT loops_salience_check CHECK (salience >= 1 AND salience <= 5),
    CONSTRAINT loops_time_horizon_check CHECK (time_horizon IN ('today', 'this_week', 'ongoing'))
);

-- Indexes for loops
CREATE INDEX IF NOT EXISTS idx_loops_tenant_user_status 
ON loops(tenant_id, user_id, status);

CREATE INDEX IF NOT EXISTS idx_loops_persona_status 
ON loops(persona_id, status);

CREATE INDEX IF NOT EXISTS idx_loops_created 
ON loops(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_loops_embedding 
ON loops USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);

-- Session buffer (working memory - current conversation)
CREATE TABLE IF NOT EXISTS session_buffer (
    tenant_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    messages JSONB NOT NULL DEFAULT '[]'::jsonb,
    session_state JSONB NOT NULL DEFAULT '{}'::jsonb,
    rolling_summary TEXT,
    closed_at TIMESTAMPTZ,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (tenant_id, session_id),
    CONSTRAINT session_buffer_messages_len_check
        CHECK (jsonb_array_length(COALESCE(messages, '[]'::jsonb)) <= 12)
);

CREATE INDEX IF NOT EXISTS idx_session_buffer_user 
ON session_buffer(tenant_id, user_id);

-- Session transcript archive (evicted turns)
CREATE TABLE IF NOT EXISTS session_transcript (
    tenant_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    messages JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (tenant_id, session_id)
);

CREATE INDEX IF NOT EXISTS idx_session_transcript_tenant_session
ON session_transcript(tenant_id, session_id);

CREATE INDEX IF NOT EXISTS idx_session_transcript_tenant_user
ON session_transcript(tenant_id, user_id);

-- Graphiti outbox (reliable delivery for evicted turns)
CREATE TABLE IF NOT EXISTS graphiti_outbox (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    role TEXT NOT NULL,
    text TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', -- pending|sent|failed
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    folded_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    sent_at TIMESTAMPTZ,
    CONSTRAINT graphiti_outbox_pending_attempts_next_attempt
        CHECK (
            status <> 'pending'
            OR attempts = 0
            OR next_attempt_at IS NOT NULL
        )
);

CREATE INDEX IF NOT EXISTS idx_graphiti_outbox_status_tenant_user
ON graphiti_outbox(status, tenant_id, user_id);

CREATE INDEX IF NOT EXISTS idx_graphiti_outbox_tenant_session
ON graphiti_outbox(tenant_id, session_id);

CREATE INDEX IF NOT EXISTS idx_graphiti_outbox_next_attempt
ON graphiti_outbox(status, next_attempt_at);
