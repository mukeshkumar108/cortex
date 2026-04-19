CREATE TABLE IF NOT EXISTS episodic_memory_embeddings (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    episode_uuid TEXT,
    unit_kind TEXT NOT NULL DEFAULT 'transcript_window',
    window_index INT NOT NULL DEFAULT 0,
    unit_text TEXT NOT NULL,
    reference_time TIMESTAMPTZ NOT NULL,
    embedding_model TEXT NOT NULL DEFAULT 'text-embedding-3-small',
    embedding VECTOR(1536) NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (tenant_id, user_id, session_id, window_index)
);

CREATE INDEX IF NOT EXISTS idx_episodic_memory_embeddings_scope
ON episodic_memory_embeddings(tenant_id, user_id, reference_time DESC);

CREATE INDEX IF NOT EXISTS idx_episodic_memory_embeddings_session
ON episodic_memory_embeddings(tenant_id, user_id, session_id);

CREATE INDEX IF NOT EXISTS idx_episodic_memory_embeddings_vector
ON episodic_memory_embeddings USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);
