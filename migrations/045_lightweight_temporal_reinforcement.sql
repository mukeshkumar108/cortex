-- Lightweight temporal reinforcement for derived memory.
-- Additive only. This supports promotion/packet decisions without introducing a
-- new pattern framework.

ALTER TABLE entity_profiles
  ADD COLUMN IF NOT EXISTS distinct_session_count INT NOT NULL DEFAULT 0;

UPDATE entity_profiles
SET distinct_session_count = GREATEST(
  COALESCE(distinct_session_count, 0),
  COALESCE(cardinality(source_session_ids), 0),
  CASE WHEN last_seen_at IS NOT NULL THEN 1 ELSE 0 END
);

ALTER TABLE open_threads
  ADD COLUMN IF NOT EXISTS distinct_session_count INT NOT NULL DEFAULT 0;

UPDATE open_threads
SET distinct_session_count = GREATEST(
  COALESCE(distinct_session_count, 0),
  COALESCE(cardinality(source_session_ids), 0),
  CASE WHEN last_mentioned_at IS NOT NULL THEN 1 ELSE 0 END
);

ALTER TABLE derived_assertions
  ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS distinct_session_count INT NOT NULL DEFAULT 0;

UPDATE derived_assertions
SET distinct_session_count = GREATEST(
  COALESCE(distinct_session_count, 0),
  COALESCE(cardinality(source_session_ids), 0),
  1
);
