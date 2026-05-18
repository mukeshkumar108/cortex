# Checkpoint — Action State MVP (2026-04-29)

## 1) Current State (Shipped)
Synapse now has a minimal Sophie-native Action State MVP with Action Management API support.

Implemented:
- Canonical `action_items` operational state (todo/reminder/habit).
- Candidate promotion path from `actionable_candidates` to `action_items`.
- Daily agenda serving lens over canonical state.
- Action audit logging for create/status/promotion/edit paths.
- Manual action editing API (`PATCH /actions/items/{id}`) with actor-aware audit.
- Pass2 actionable extractor updated to emit structured fields for safe operational handling.

## 2) Migrations / Schema Status
Action-state relevant migrations:
- `057_action_state_mvp.sql`
- `058_action_state_mvp_stabilization.sql`

Key schema outcomes:
- Added `action_items`, `action_updates`, `action_audit_log`.
- Extended `actionable_candidates` with promotion/time/provenance-confidence fields.
- Added FK: `actionable_candidates.promoted_action_item_id -> action_items(id) ON DELETE SET NULL`.
- Audit actor check includes `system_auto_promote`.

## 3) API Surface (Action Management)
Live endpoints:
- `GET /actions/items`
- `POST /actions/items`
- `PATCH /actions/items/{id}`
- `PATCH /actions/items/{id}/status`
- `POST /actions/items/{id}/done`
- `POST /actions/items/{id}/dismiss`
- `POST /actions/items/{id}/cancel`
- `POST /actions/candidates/promote`
- `POST /actions/candidates/{candidate_id}/auto-promote`
- `GET /actions/daily-agenda`

List filters currently supported:
- `status`
- `kind`
- `include_done`
- `date_from`, `date_to` (applies to `due_at` or `remind_at`)
- `limit`, `offset`

## 4) Operational Rules in Force
Lifecycle rules enforced in service layer:
- `action_item`: `pending -> done|cancelled|dismissed`
- `action_candidate`: promotion allowed from `detected|needs_review` to `confirmed`
- `action_update`: schema lifecycle defined (`proposed|applied|rejected|expired`), full mutation endpoints not yet exposed

Delete behavior:
- Soft delete via `dismissed` (no hard delete API path)

Auto-promote policy (controlled):
- Only structured fields are used; no keyword fallback logic.
- Requires high confidence, low risk, direct user expression, and subtype eligibility.
- Calendar events are never auto-created.

## 5) Extractor Contract (Pass2 Actionable)
Pass2 actionable candidates now emit structured fields used by Action State:
- `candidate_subtype`
- numeric `confidence`
- `provenance_summary`
- `risk_tags[]`
- `has_direct_user_expression`
- `requires_confirmation`
- `suggested_action` (`create_action_item|ask_user|ignore`)
- `proposed_due_at`, `proposed_remind_at`
- `cadence_text`, `waiting_on`, `needs_response`

Service layer is intentionally stateful and deterministic; semantic judgment is delegated to LLM extraction output.

## 6) Verification Commands
Run these after pulling/applying migrations:

```bash
pytest -q tests/test_action_state_mvp.py
pytest -q tests/test_actionable_candidates_pipeline.py
pytest -q tests/test_daily_candidates_endpoint.py
```

Expected at checkpoint time:
- `tests/test_action_state_mvp.py`: passing
- `tests/test_actionable_candidates_pipeline.py`: passing
- `tests/test_daily_candidates_endpoint.py`: passing

## 7) Known Gaps / Follow-Ups
1. `action_updates` endpoints (apply/reject/expire) are not yet exposed.
2. Add one DB-backed integration test path for promotion + audit + FK (current tests are mostly service-level/fake-DB).
3. Optional: add thin response model for action item list/patch responses for stronger API typing parity.

## 8) Recommended Next Session Start
1. Apply migrations and run the 3 test commands above.
2. Smoke test API:
   - create/list/patch/done/dismiss/cancel action items
   - promote candidate
   - check daily agenda lens
3. Decide whether to ship `action_updates` mutation endpoints in same phase or explicitly defer to next phase.
