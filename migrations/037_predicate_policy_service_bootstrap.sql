-- T5: Predicate policy service + versioning bootstrap
-- Scope:
-- - policy contract columns for subject/object kind + conflict mode + equivalence rule hook
-- - fail-closed policy constraints
-- - seeded versioned policy records for deterministic lookup

ALTER TABLE predicate_policy
  ADD COLUMN IF NOT EXISTS conflict_mode TEXT;

ALTER TABLE predicate_policy
  ADD COLUMN IF NOT EXISTS expected_subject_kind TEXT;

ALTER TABLE predicate_policy
  ADD COLUMN IF NOT EXISTS expected_object_kind TEXT;

ALTER TABLE predicate_policy
  ADD COLUMN IF NOT EXISTS object_equivalence_rule TEXT;

UPDATE predicate_policy
SET
  conflict_mode = CASE
    WHEN lower(btrim(conflict_rule)) IN ('supersede', 'coexist', 'retract_only')
      THEN lower(btrim(conflict_rule))
    ELSE 'coexist'
  END,
  expected_subject_kind = COALESCE(NULLIF(btrim(expected_subject_kind), ''), 'entity'),
  expected_object_kind = COALESCE(NULLIF(btrim(expected_object_type), ''), 'unknown'),
  object_equivalence_rule = COALESCE(NULLIF(btrim(equivalence_rule), ''), 'strict')
WHERE
  conflict_mode IS NULL
  OR expected_subject_kind IS NULL
  OR expected_object_kind IS NULL
  OR object_equivalence_rule IS NULL;

ALTER TABLE predicate_policy
  ALTER COLUMN conflict_mode SET NOT NULL;

ALTER TABLE predicate_policy
  ALTER COLUMN expected_subject_kind SET NOT NULL;

ALTER TABLE predicate_policy
  ALTER COLUMN expected_object_kind SET NOT NULL;

ALTER TABLE predicate_policy
  ALTER COLUMN object_equivalence_rule SET NOT NULL;

ALTER TABLE predicate_policy
  ADD CONSTRAINT predicate_policy_conflict_mode_check
  CHECK (conflict_mode IN ('supersede', 'coexist', 'retract_only'));

ALTER TABLE predicate_policy
  ADD CONSTRAINT predicate_policy_expected_subject_kind_nonempty_check
  CHECK (btrim(expected_subject_kind) <> '');

ALTER TABLE predicate_policy
  ADD CONSTRAINT predicate_policy_expected_object_kind_nonempty_check
  CHECK (btrim(expected_object_kind) <> '');

-- Keep legacy columns synchronized to avoid split-brain configuration.
UPDATE predicate_policy
SET
  conflict_rule = conflict_mode,
  expected_object_type = expected_object_kind,
  equivalence_rule = object_equivalence_rule
WHERE
  conflict_rule IS DISTINCT FROM conflict_mode
  OR expected_object_type IS DISTINCT FROM expected_object_kind
  OR equivalence_rule IS DISTINCT FROM object_equivalence_rule;

INSERT INTO predicate_policy_versions (policy_version, status, description, activated_at)
VALUES (
  'v2.p1',
  'draft',
  'Synapse v2 predicate policy baseline',
  NULL
)
ON CONFLICT (policy_version) DO NOTHING;

INSERT INTO predicate_policy_versions (policy_version, status, description)
VALUES (
  'v2.p2',
  'draft',
  'Synapse v2 predicate policy compatibility candidate'
)
ON CONFLICT (policy_version) DO NOTHING;

-- Ensure there is exactly one active policy version; keep pre-existing active version if present.
UPDATE predicate_policy_versions
SET status = 'active',
    activated_at = COALESCE(activated_at, NOW()),
    retired_at = NULL
WHERE policy_version = 'v2.p1'
  AND NOT EXISTS (
    SELECT 1
    FROM predicate_policy_versions
    WHERE status = 'active'
      AND policy_version <> 'v2.p1'
  );

INSERT INTO predicate_policy (
  policy_version,
  predicate,
  cardinality,
  conflict_rule,
  conflict_mode,
  expected_subject_kind,
  expected_object_type,
  expected_object_kind,
  equivalence_rule,
  object_equivalence_rule,
  is_active,
  metadata
)
VALUES
  (
    'v2.p1',
    'relationship.status',
    'one',
    'supersede',
    'supersede',
    'entity',
    'entity',
    'entity',
    'normalized_text',
    'normalized_text',
    true,
    '{"ticket":"T5","seeded":true}'::jsonb
  ),
  (
    'v2.p1',
    'user.location',
    'one',
    'supersede',
    'supersede',
    'user',
    'literal',
    'literal',
    'normalized_text',
    'normalized_text',
    true,
    '{"ticket":"T5","seeded":true}'::jsonb
  ),
  (
    'v2.p1',
    'user.preference',
    'many',
    'coexist',
    'coexist',
    'user',
    'literal',
    'literal',
    'canonical_json',
    'canonical_json',
    true,
    '{"ticket":"T5","seeded":true}'::jsonb
  ),
  (
    'v2.p2',
    'relationship.status',
    'many',
    'coexist',
    'coexist',
    'entity',
    'entity',
    'entity',
    'normalized_text',
    'normalized_text',
    false,
    '{"ticket":"T5","seeded":true,"purpose":"versioned_behavior_test"}'::jsonb
  ),
  (
    'v2.p2',
    'user.location',
    'one',
    'supersede',
    'supersede',
    'user',
    'literal',
    'literal',
    'normalized_text',
    'normalized_text',
    false,
    '{"ticket":"T5","seeded":true}'::jsonb
  ),
  (
    'v2.p2',
    'user.preference',
    'many',
    'coexist',
    'coexist',
    'user',
    'literal',
    'literal',
    'canonical_json',
    'canonical_json',
    false,
    '{"ticket":"T5","seeded":true}'::jsonb
  )
ON CONFLICT (policy_version, predicate) DO UPDATE
SET
  cardinality = EXCLUDED.cardinality,
  conflict_rule = EXCLUDED.conflict_rule,
  conflict_mode = EXCLUDED.conflict_mode,
  expected_subject_kind = EXCLUDED.expected_subject_kind,
  expected_object_type = EXCLUDED.expected_object_type,
  expected_object_kind = EXCLUDED.expected_object_kind,
  equivalence_rule = EXCLUDED.equivalence_rule,
  object_equivalence_rule = EXCLUDED.object_equivalence_rule,
  is_active = EXCLUDED.is_active,
  metadata = EXCLUDED.metadata,
  updated_at = NOW();
