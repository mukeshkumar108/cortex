WITH cleaned AS (
    SELECT
        um.tenant_id,
        um.user_id,
        COALESCE(
            (
                SELECT jsonb_agg(rel)
                FROM jsonb_array_elements(COALESCE(um.model->'key_relationships', '[]'::jsonb)) rel
                WHERE (rel->>'name') ~ '^[^():\n][^():\n]{0,79}$'
                  AND (rel->>'who') ~* '^[a-z][a-z0-9 _-]{1,39}$'
                  AND (rel->>'status') ~ '^[^:\n][^\n]{0,79}$'
                  AND lower(trim(rel->>'name')) NOT IN ('and', 'or', 'but', 'the', 'a', 'an', 'of', 'in', 'to')
            ),
            '[]'::jsonb
        ) AS normalized_relationships
    FROM user_model um
)
UPDATE user_model um
SET model = jsonb_set(COALESCE(um.model, '{}'::jsonb), '{key_relationships}', cleaned.normalized_relationships, true),
    updated_at = NOW()
FROM cleaned
WHERE um.tenant_id = cleaned.tenant_id
  AND um.user_id = cleaned.user_id
  AND COALESCE(um.model->'key_relationships', '[]'::jsonb) IS DISTINCT FROM cleaned.normalized_relationships;
