-- Allow soft-delete archival status for action_items.
ALTER TABLE action_items
  DROP CONSTRAINT IF EXISTS action_items_status_check;

ALTER TABLE action_items
  ADD CONSTRAINT action_items_status_check
  CHECK (status IN ('pending','done','cancelled','dismissed','archived'));
