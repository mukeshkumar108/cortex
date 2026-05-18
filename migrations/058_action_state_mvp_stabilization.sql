-- Action State MVP stabilization:
-- - FK for promoted_action_item_id (nullable, ON DELETE SET NULL)
-- - allow system_auto_promote actor in audit log

-- Null out broken references before adding FK.
UPDATE actionable_candidates ac
SET promoted_action_item_id = NULL
WHERE promoted_action_item_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM action_items ai
    WHERE ai.id = ac.promoted_action_item_id
  );

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'actionable_candidates_promoted_action_item_id_fkey'
  ) THEN
    ALTER TABLE actionable_candidates
      ADD CONSTRAINT actionable_candidates_promoted_action_item_id_fkey
      FOREIGN KEY (promoted_action_item_id)
      REFERENCES action_items(id)
      ON DELETE SET NULL;
  END IF;
END $$;

ALTER TABLE action_audit_log
  DROP CONSTRAINT IF EXISTS action_audit_log_actor_check;

ALTER TABLE action_audit_log
  ADD CONSTRAINT action_audit_log_actor_check
  CHECK (actor IN ('sophie','synapse','system','user','extractor','system_auto_promote'));
