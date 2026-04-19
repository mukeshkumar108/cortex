-- Add scoring fields to entity_profiles
ALTER TABLE entity_profiles
ADD COLUMN IF NOT EXISTS importance_score FLOAT DEFAULT 0.5,
ADD COLUMN IF NOT EXISTS salience_score FLOAT DEFAULT 0.5,
ADD COLUMN IF NOT EXISTS core_importance FLOAT DEFAULT 0.5,
ADD COLUMN IF NOT EXISTS operational_priority FLOAT DEFAULT 0.5;

-- Add scoring and metadata fields to open_threads
ALTER TABLE open_threads
ADD COLUMN IF NOT EXISTS times_mentioned INT DEFAULT 1,
ADD COLUMN IF NOT EXISTS importance_score FLOAT DEFAULT 0.5,
ADD COLUMN IF NOT EXISTS salience_score FLOAT DEFAULT 0.5,
ADD COLUMN IF NOT EXISTS category_updated BOOLEAN DEFAULT false;

-- Backfill times_mentioned from source_session_ids
UPDATE open_threads
SET times_mentioned = cardinality(source_session_ids)
WHERE times_mentioned = 1
  AND cardinality(source_session_ids) > 1;

-- Valid thread categories are documented in pipeline logic:
-- health / relationship / goal / commitment / worry / project /
-- assistant_feedback / other
