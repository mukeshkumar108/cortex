-- Entity profiles registry
CREATE TABLE IF NOT EXISTS entity_profiles (
  entity_id                   TEXT PRIMARY KEY
                                DEFAULT gen_random_uuid()::text,
  user_id                     TEXT NOT NULL,
  canonical_name              TEXT NOT NULL,
  canonical_name_normalized   TEXT NOT NULL,
  type                        TEXT NOT NULL,
  -- person / project / place / other
  aliases                     TEXT[] DEFAULT '{}',
  status                      TEXT NOT NULL DEFAULT 'tentative',
  -- tentative / active / dormant / archived
  relationship_to_user        TEXT,
  -- keep coarse: girlfriend / daughter / active_project / friend
  profile_text                TEXT,
  key_facts                   JSONB DEFAULT '[]',
  open_questions              JSONB DEFAULT '[]',
  last_known_status           TEXT,
  confidence                  FLOAT DEFAULT 0.4,
  mention_count               INT DEFAULT 1,
  first_seen_at               TIMESTAMPTZ,
  last_seen_at                TIMESTAMPTZ,
  last_updated_at             TIMESTAMPTZ,
  last_processed_session_date TIMESTAMPTZ,
  source_session_ids          TEXT[] DEFAULT '{}',
  created_at                  TIMESTAMPTZ DEFAULT now(),
  UNIQUE(user_id, canonical_name_normalized)
);

CREATE INDEX IF NOT EXISTS idx_entity_profiles_user
  ON entity_profiles(user_id);
CREATE INDEX IF NOT EXISTS idx_entity_profiles_status
  ON entity_profiles(user_id, status);
CREATE INDEX IF NOT EXISTS idx_entity_profiles_type
  ON entity_profiles(user_id, type);

-- Pipeline checkpoints — tracks last processed date per pipeline
CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
  user_id          TEXT NOT NULL,
  pipeline_name    TEXT NOT NULL,
  last_processed   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, pipeline_name)
);
