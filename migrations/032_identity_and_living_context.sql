-- Identity profile — who this person really is
CREATE TABLE IF NOT EXISTS identity_profile (
  profile_id              TEXT PRIMARY KEY
                            DEFAULT gen_random_uuid()::text,
  user_id                 TEXT NOT NULL UNIQUE,

  -- Core identity
  who_they_are            TEXT,
  -- synthesized prose: values, beliefs, worldview,
  -- how they see themselves

  core_values             JSONB DEFAULT '[]',
  -- array of {value, evidence, confidence}

  recurring_patterns      JSONB DEFAULT '[]',
  -- behaviours and ways of being that keep showing up
  -- {pattern, evidence, first_seen, frequency}

  family_history          TEXT,
  -- key biographical facts about their background

  faith_and_beliefs       TEXT,
  -- spiritual/philosophical worldview

  -- Inward-facing
  what_they_want          TEXT,
  -- deeper aspirations beyond stated goals

  recurring_fears         JSONB DEFAULT '[]',
  -- {fear, evidence, confidence}

  what_they_avoid         TEXT,
  -- patterns of avoidance or deflection

  how_they_relate         TEXT,
  -- how they show up in relationships
  -- patterns: approval-seeking, caretaking, etc

  -- Persistent goals (distinct from threads)
  persistent_goals        JSONB DEFAULT '[]',
  -- goals stated multiple times as core to who they are
  -- {goal, stated_times, first_stated, evidence}

  -- Meta
  current_chapter         TEXT,
  -- what season of life are they in right now

  last_synthesized_at     TIMESTAMPTZ,
  source_session_count    INT DEFAULT 0,
  synthesis_model         TEXT,
  created_at              TIMESTAMPTZ DEFAULT now(),
  updated_at              TIMESTAMPTZ DEFAULT now()
);

-- Living context — what's happening right now
CREATE TABLE IF NOT EXISTS living_context (
  context_id              TEXT PRIMARY KEY
                            DEFAULT gen_random_uuid()::text,
  user_id                 TEXT NOT NULL UNIQUE,

  -- Surface layer
  current_focus           TEXT,
  recent_narrative        TEXT,
  relationship_pulse      TEXT,
  emotional_texture       TEXT,

  -- Tension map
  primary_tension         TEXT,
  what_theyre_avoiding    TEXT,
  unspoken_goal           TEXT,
  why_it_matters          TEXT,

  -- Contradictions
  active_contradictions   JSONB DEFAULT '[]',
  -- {topic, earlier_view, recent_view, first_stated, last_stated}

  last_synthesized_at     TIMESTAMPTZ,
  source_session_count    INT DEFAULT 0,
  synthesis_model         TEXT,
  created_at              TIMESTAMPTZ DEFAULT now(),
  updated_at              TIMESTAMPTZ DEFAULT now()
);

-- Thread lifecycle
ALTER TABLE open_threads
ADD COLUMN IF NOT EXISTS thread_type TEXT DEFAULT 'situational',
ADD COLUMN IF NOT EXISTS absorbed_into TEXT DEFAULT NULL;
