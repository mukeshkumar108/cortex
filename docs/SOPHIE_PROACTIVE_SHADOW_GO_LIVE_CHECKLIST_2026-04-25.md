# Sophie Proactive Shadow Go-Live Checklist - 2026-04-25

## Purpose

This is a strict go/no-go checklist for moving from baseline hardening into proactive queue behavior.

Rule:
- No "mostly done."
- Every gate is binary pass/fail with evidence.
- Ship only when all required gates pass.

## Scope

Shadow mode only for proactive surfacing:
- `follow_up_candidates`
- `clarification_candidates`
- `recent_change_candidates`

Actionable user-state extraction (ingest-time, non-acting):
- action/tool candidates lane, stored in `actionable_candidates`
- record types:
  - `event_candidate`
  - `task_candidate`
  - `reminder_candidate`
  - `commitment`
- candidate subtypes:
  - `todo`
  - `reminder`
  - `calendar_event`
  - `habit`
  - `follow_up`
  - `waiting_on`
  - `nudge`
- status lifecycle:
  - `detected`
  - `needs_review`
  - `confirmed`
  - `dismissed`
  - `acted_on`
  - `superseded`
  - `stale`
- source-model fields include:
  - `candidate_subtype`
  - `due_iso`
  - `relevant_from_iso`
  - `relevant_until_iso`
  - `waiting_on`
  - `needs_response`
  - `cadence_text`
  - `suggested_action`
  - `linked_external_id`
  - `linked_external_type`

Shadow mode means candidates are generated, scored, and logged, but not user-delivered.
No auto-confirmation and no external action execution.

## Ingest Placement

Actionable extraction runs in a dedicated derived Pass 2 during session ingest:
- session ingest enqueues `post_ingest_hook:pass1_triage`
- pass1 triage stays routing/triage only (boolean flags only; no entity/thread/actionable/memory/identity extraction payloads)
- pass1 enqueues `post_ingest_hook:pass2_actionable` only when `run_actionable_pass=true`
- pass1 enqueues `post_ingest_hook:pass2b_session_changes` only when `run_session_changes_pass=true`
- pass1 enqueues `post_ingest_hook:pass2c_entity_candidates` only when `run_entity_pass=true`
- pass2 extracts and persists action/tool candidates to `actionable_candidates`
- pass2b extracts and persists factual/contextual updates to `session_changes`
- pass2c extracts and persists durable named-entity candidates to `entity_candidates`
- pass1.5 resolves entity candidates into `entity_profiles`
- pass2 is LLM-based extraction; keyword/heuristic fallback is disabled by policy
- pass2 extraction standard is strict:
  - emit only standalone useful items
  - do not emit unresolved pronoun fragments like `do it` / `send it` / `get it delivered` unless the referent is resolved from context
  - `needs_review` is for meaningful but unconfirmed items, not meaningless fragments
- pass2 temporal grounding is reference-time based:
  - prompts receive `now_iso`, `timezone`, `local_date`, `local_day`, `time_of_day`
  - relative phrases such as `tomorrow`, `Friday`, and `tonight` resolve from source/session time
  - vague phrases such as `soon`, `later`, `next week`, `sometime` keep ISO fields null and preserve raw phrasing in `due_text` / `time_text`
- read lenses:
  - `GET /daily-candidates` (date-scoped lens, not source of truth)
  - `GET /review-queue` (status lens for `needs_review`, date-agnostic)
  - `GET /session-changes` (inspectable lens over factual/contextual changes)
  - `GET /entity-candidates` (inspectable lens over unresolved/resolved entity candidates)

## Lens Model Clarification

Source of truth:
- `actionable_candidates` table

Lens 1:
- `GET /daily-candidates`
- scoped to a target day (`now` or `date`)
- includes only candidates relevant in that date window
- default lens excludes undated low-confidence detected chatter
- default lens prefers scheduled, due, overdue, waiting-on, response-needed, confirmed, or `needs_review` items
- default lens caps output to a small useful set unless a limit is explicitly passed

Lens 2:
- `GET /review-queue`
- includes all `needs_review` candidates regardless of due date
- used to catch future invitations/events/reminders early

## Cross-Source Reconciliation (Conservative)

Principles:
- source-agnostic: WhatsApp/email/chat/calendar-like extracts all reconcile into one user-level candidate graph
- provenance-preserving: prior evidence is never deleted; new evidence is appended
- conservative merge rules only (avoid over-joining unrelated items)

Rules:
- same `record_type` + high title/topic overlap + overlapping time:
  - merge into existing open candidate
  - raise confidence conservatively
  - if non-authoritative confirmation arrives, move `detected` -> `needs_review`
- same topic but materially changed time:
  - mark old candidate `superseded`
  - create/update a new candidate for the changed time
- explicit cancellation language:
  - mark matched candidate `stale` (or `dismissed` if authoritative cancellation source)
- authoritative external linkage (calendar-like source + external id):
  - may mark candidate `confirmed`
  - may set `linked_external_id` / `linked_external_type`

Explicit non-goals:
- no auto-act
- no external API calls
- no automatic creation of calendar/reminder/task objects

## One-Week Plan (Parallel Tracks)

Track A: remaining conservative baseline gates  
Track B: proactive candidate shadow validation

Day 1-2:
- Enable/verify shadow candidate generation and logging.
- Run targeted baseline tests and gather first shadow outputs.

Day 3-4:
- Fix false positives/priority mistakes from shadow review.
- Complete remaining retrospective/temporal reinforcement tasks queued in tracker.

Day 5:
- Re-run checklist gates and capture artifacts.

Day 6:
- Final pass/fail review against this checklist.

Day 7:
- Go/No-Go decision.

## Required Go-Live Gates

### Gate 1: Pass 4 output is plain identification, not interpretation

Pass condition:
- Pass 4 outputs avoid motive inference / personality verdict phrasing.
- Identity statements are evidence-grounded and role/commitment oriented.

Evidence required:
- Test pass for prompt/validator constraints.
- At least 10 sampled pass4 outputs reviewed with no disallowed style regressions.

Status: [ ]

### Gate 2: Relationship tiers exclude contextual entities from packets unless promoted

Pass condition:
- Tier 3 contextual entities are excluded unless promotion criteria are met.
- Packet people list is sorted/filtered by tier + importance logic.

Evidence required:
- Automated test pass for contextual filter behavior.
- Manual sample review of packet people sections across at least 10 users/sessions.

Status: [ ]

### Gate 3: Declared truth overrides derived state

Pass condition:
- When declared truth conflicts with inferred/derived data, declared truth wins in packet and pass4 grounding.

Evidence required:
- Automated tests for declared truth lane + packet precedence pass.
- Manual conflict case review (at least 5 cases) with expected precedence.

Status: [ ]

### Gate 4: Always-on packet is compact and runtime-useful

Pass condition:
- Packet text remains under target envelope (roughly <=500 tokens).
- Sections remain operationally useful and stable.

Evidence required:
- Automated checks for packet build path and persistence.
- Sampled token-length checks on at least 20 generated packets.
- Manual quality spot-check notes attached.

Status: [ ]

### Gate 5: Proactive candidate views populate correctly in shadow

Pass condition:
- All three candidate views populate with expected rows.
- Scores/ranking are present and interpretable.
- No user-facing proactive emissions occur from shadow-only runs.

Evidence required:
- SQL/output snapshots for:
  - `follow_up_candidates`
  - `clarification_candidates`
  - `recent_change_candidates`
- At least 1 week shadow log review with daily summary.

Status: [ ]

### Gate 6: Relationship-state facts are not softened

Pass condition:
- Concrete terms like "estranged" / "long distance" are preserved where present.
- No euphemizing or diplomatic rewrite drift in pass4/pass5/packet outputs.

Evidence required:
- Prompt-constraint tests pass.
- Manual review of at least 20 outputs containing relationship-state terms.

Status: [ ]

## Baseline Exit Gates (Must Also Be Queued/Tracked)

These are already identified in implementation tracker and must be explicitly queued before proactive rollout:
- Broader tentative-entity reinterpretation.
- Broader contradiction/transition reinterpretation.
- Thread-level reinterpretation for weak mistaken premises.
- Temporal reinforcement completion:
  - high-impact single-event anchors,
  - anti-recency dominance,
  - anti-stale-frequency dominance.

Reference:
- `docs/SOPHIE_MEMORY_IMPLEMENTATION_TRACKER.md`

## Go/No-Go Decision Record

Date:
- 

Decision:
- GO / NO-GO

Failed gates (if any):
- 

## 2026-04-28 Operational Notes

What changed in this round:
- Kept `google/gemma-4-26b-a4b-it` as the default derived-lane model after comparative probes
- Added temporal grounding to `pass2_actionable`, `pass2b_session_changes`, and actionable audit prompts
- Tightened pass2 actionable extraction so candidates must be standalone useful items
- Tightened `daily-candidates` serving so noisy undated detected chatter does not surface by default
- Added LLM-primary candidate cleanup/audit flow and used it to dismiss the historical `We need to get it delivered.` fragment
- Cleaned the remaining surfaced user candidate into:
  - `Establish morning walk routine`
  - subtype `habit`
  - cadence `daily`
  - confidence `high`

What remains true:
- Historical `actionable_candidates` rows may still contain legacy-shape records with `candidate_subtype=null`
- Serving output for the default test user is currently clean enough to continue from, even though the historical table is not fully re-audited

Follow-up actions:
- 

## Sample Transcript -> Records

Input transcript snippets:
- "I have a dentist appointment Tuesday at 3"
- "remind me to call John tomorrow"
- "we should meet Friday evening"
- "I need to send that deck"

Example extracted rows (shape):
- `{ "record_type": "event_candidate", "title": "I have a dentist appointment Tuesday at 3", "due_iso": "2026-04-28T15:00:00Z", "source": "chat", "status": "detected", "confidence_label": "medium" }`
- `{ "record_type": "reminder_candidate", "title": "remind me to call John tomorrow", "due_iso": "2026-04-28T09:00:00Z", "source": "chat", "status": "detected", "confidence_label": "medium" }`
- `{ "record_type": "commitment", "title": "we should meet Friday evening", "due_iso": "2026-05-01T18:00:00Z", "source": "chat", "status": "needs_review", "confidence_label": "medium" }`
- `{ "record_type": "task_candidate", "title": "I need to send that deck", "due_iso": null, "source": "chat", "status": "detected", "confidence_label": "medium" }`
