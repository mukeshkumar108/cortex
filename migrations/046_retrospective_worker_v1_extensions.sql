-- Conservative retrospective worker V1 extensions.
-- Additive only. This stores durable-anchor reinforcement metadata without
-- changing serving response shape.

ALTER TABLE entity_profiles
  ADD COLUMN IF NOT EXISTS reinforcement_count INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_reinforced_at TIMESTAMPTZ;

UPDATE entity_profiles
SET reinforcement_count = GREATEST(
      COALESCE(reinforcement_count, 0),
      CASE
        WHEN replace(lower(COALESCE(relationship_to_user, '')), ' ', '_') IN (
          'daughter','son','child','mother','father','parent',
          'sister','brother','girlfriend','boyfriend','partner','spouse'
        )
        THEN COALESCE(distinct_session_count, 0)
        ELSE COALESCE(reinforcement_count, 0)
      END
    ),
    last_reinforced_at = CASE
      WHEN last_reinforced_at IS NOT NULL THEN last_reinforced_at
      WHEN replace(lower(COALESCE(relationship_to_user, '')), ' ', '_') IN (
        'daughter','son','child','mother','father','parent',
        'sister','brother','girlfriend','boyfriend','partner','spouse'
      )
      THEN last_seen_at
      ELSE last_reinforced_at
    END;
