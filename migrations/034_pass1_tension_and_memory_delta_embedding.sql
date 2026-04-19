ALTER TABLE session_classifications
ADD COLUMN IF NOT EXISTS tension_signal TEXT;

ALTER TABLE session_classifications
ADD COLUMN IF NOT EXISTS memory_delta_embedding VECTOR(1536);

CREATE INDEX IF NOT EXISTS idx_sc_embedding
ON session_classifications
USING ivfflat (memory_delta_embedding vector_cosine_ops)
WHERE memory_delta_embedding IS NOT NULL;
