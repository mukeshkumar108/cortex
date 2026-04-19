CREATE TABLE IF NOT EXISTS open_threads (
  thread_id            TEXT PRIMARY KEY
                         DEFAULT gen_random_uuid()::text,
  user_id              TEXT NOT NULL,
  title                TEXT NOT NULL,
  detail               TEXT,
  status               TEXT NOT NULL DEFAULT 'open',
  -- open / resolved / snoozed / archived
  priority             TEXT NOT NULL DEFAULT 'medium',
  -- high / medium / low
  category             TEXT,
  -- health / relationship / goal / commitment /
  -- worry / project / other
  related_entities     TEXT[],
  -- canonical_names from entity_profiles
  source_session_ids   TEXT[] DEFAULT '{}',
  first_seen_at        TIMESTAMPTZ,
  last_updated_at      TIMESTAMPTZ,
  last_mentioned_at    TIMESTAMPTZ,
  follow_up_after      TIMESTAMPTZ,
  -- when Sophie should surface this
  resolved_at          TIMESTAMPTZ,
  resolution_note      TEXT,
  created_at           TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_open_threads_user
  ON open_threads(user_id);
CREATE INDEX IF NOT EXISTS idx_open_threads_status
  ON open_threads(user_id, status);
CREATE INDEX IF NOT EXISTS idx_open_threads_followup
  ON open_threads(user_id, follow_up_after)
  WHERE status = 'open';
CREATE INDEX IF NOT EXISTS idx_open_threads_priority
  ON open_threads(user_id, priority)
  WHERE status = 'open';
