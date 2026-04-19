CREATE TABLE IF NOT EXISTS living_context (
  context_id              TEXT PRIMARY KEY
                            DEFAULT gen_random_uuid()::text,
  user_id                 TEXT NOT NULL UNIQUE,

  -- Surface layer: what's happening
  current_focus           TEXT,
  -- what they're actively working on right now

  recent_narrative        TEXT,
  -- prose summary of last 2 weeks
  -- events, changes, what happened

  relationship_pulse      TEXT,
  -- how key relationships feel right now
  -- Ashley, Jasmine, anyone active

  emotional_texture       TEXT,
  -- the general vibe and energy right now
  -- not a diagnosis, just honest texture

  -- Tension map: what's underneath
  primary_tension         TEXT,
  -- the main unresolved friction right now
  -- the thing that hasn't been named directly

  what_theyre_avoiding    TEXT,
  -- what keeps not getting addressed
  -- patterns of deflection or delay

  unspoken_goal           TEXT,
  -- what this week/period is really about
  -- beneath the stated tasks

  why_it_matters          TEXT,
  -- the deeper stakes of what's happening now
  -- why Bluum isn't just a product etc

  -- Contradictions: where things have shifted
  active_contradictions   JSONB DEFAULT '[]',
  -- [{
  --   topic: "relationship with Ashley",
  --   earlier_view: "questioning sustainability",
  --   recent_view: "reconciled, feeling hopeful",
  --   first_stated: "2026-04-07",
  --   last_stated: "2026-04-07",
  --   resolved: false
  -- }]

  -- Sophie behavioral directives
  -- things Sophie should do differently based on context
  sophie_directives       JSONB DEFAULT '[]',
  -- [{
  --   directive: "do not be superficial",
  --   reason: "user expressed intense frustration
  --             with short responses",
  --   confidence: 0.98
  -- }]

  -- Meta
  last_synthesized_at     TIMESTAMPTZ,
  sessions_since_last     INT DEFAULT 0,
  -- how many new sessions since last synthesis
  source_session_count    INT DEFAULT 0,
  synthesis_model         TEXT,
  created_at              TIMESTAMPTZ DEFAULT now(),
  updated_at              TIMESTAMPTZ DEFAULT now()
);

-- Backward-compatible upgrades if living_context already exists.
ALTER TABLE living_context
ADD COLUMN IF NOT EXISTS sophie_directives JSONB DEFAULT '[]',
ADD COLUMN IF NOT EXISTS sessions_since_last INT DEFAULT 0;

-- Tracking support for checkpoint-based triggering.
ALTER TABLE pipeline_checkpoints
ADD COLUMN IF NOT EXISTS sessions_since_last INT DEFAULT 0;
