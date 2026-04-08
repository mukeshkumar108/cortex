ALTER TABLE user_model
ADD COLUMN IF NOT EXISTS narrative_stable TEXT;

ALTER TABLE user_model
ADD COLUMN IF NOT EXISTS narrative_current TEXT;

UPDATE user_model
SET narrative_stable = COALESCE(narrative_stable, model->>'narrative_stable'),
    narrative_current = COALESCE(narrative_current, model->>'narrative_current', model->>'narrative')
WHERE narrative_stable IS NULL
   OR narrative_current IS NULL;
