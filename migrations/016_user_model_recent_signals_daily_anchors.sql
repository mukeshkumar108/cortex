UPDATE user_model
SET model = jsonb_set(
    jsonb_set(
        model,
        '{recent_signals}',
        COALESCE(model->'recent_signals', '[]'::jsonb),
        true
    ),
    '{daily_anchors}',
    COALESCE(model->'daily_anchors', '{}'::jsonb),
    true
)
WHERE model IS NOT NULL;
