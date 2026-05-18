# Memory Data Model

Purpose: quick reference for Synapse user-state primitives, lifecycle surfaces, and runtime retrieval inputs.

Canonical reference docs:
- `/opt/synapse/docs/core.md`
- `/opt/synapse/docs/synapse.md`

## Core Tables

### `session_classifications`
- One row per processed session.
- Key fields:
  - `is_memory_worthy`, `session_kind`, `identity_relevant`
  - `raw_triage_output` (routing metadata and lightweight hints, not durable extraction payloads)
  - `tension_signal` (optional subtle under-surface cue from user turns)
  - `memory_delta_embedding` (`vector(1536)`) for semantic episodic retrieval

### `action_candidates`
- Unconfirmed action candidates extracted from user/source text.
- Key fields:
  - candidate shape: `record_type`, `candidate_subtype`, `title`, `summary`
  - timing: `due_iso`, `relevant_from_iso`, `relevant_until_iso`
  - support fields: `waiting_on`, `needs_response`, `cadence_text`
  - lifecycle: `status`, `confidence_label`, `confidence_score`
  - provenance: `source`, `provenance`, `linked_external_id`, `linked_external_type`

### `action_items`
- Confirmed actions owned semantically by Synapse when Sophie-native, or projected from external tools with source references.
- Key fields:
  - shape: `action_type`, `title`, `summary`, `status`
  - timing: `due_iso`, `scheduled_iso`, `completed_at`
  - ownership: `origin` (`sophie_native` or external system namespace), `external_ref`
  - provenance/freshness: `source`, `provenance`, `confidence`, `updated_at`, `freshness_status`

### `action_updates`
- Proposed mutations against existing action items.
- Key fields:
  - linkage: `target_action_id`
  - change: `update_type`, `patch`
  - lifecycle: `status` (`proposed`/`applied`/`rejected`/`expired`)
  - provenance: `source`, `provenance`, `confidence`

### `commitments`
- Explicit promises/agreements/intentions/obligations that may or may not become todos.
- Key fields:
  - content: `title`, `detail`, `status`
  - lifecycle: `status` (`active`/`fulfilled`/`dropped`/`superseded`/`stale`)
  - linkage: `entity_id`, `thread_id`, optional `action_item_id`
  - provenance/freshness: `source`, `provenance`, `confidence`, `updated_at`, `freshness_status`

### `signals`
- Short-lived behavioral or wellbeing signals used for interpretation and timing.
- Key fields:
  - signal: `kind`, `value`, `intensity`, `status`
  - lifecycle: `status` (`active`/`decayed`/`dismissed`)
  - freshness: `ttl_policy`, `decay_at`, `last_revalidated_at`
  - provenance: `source`, `provenance`, `confidence`

### `entity_profiles`
- Canonicalized long-lived entities (people/projects/places/etc.).
- Key fields:
  - identity/profile: `canonical_name`, `type`, `relationship_to_user`, `profile_text`
  - state: `last_known_status`, `status`, `aliases`
  - memory quality: `mention_count`, `key_facts`, `open_questions`
  - scoring: `core_importance`, `operational_priority`, `importance_score`, `salience_score`

### `open_threads`
- Unresolved situations that need continuity over time.
- Key fields:
  - thread content: `title`, `detail`, `category`, `priority`
  - lifecycle: `status` (`open`/`monitoring`/`decision_needed`/`resolved`/`archived`)
  - timing: `first_seen_at`, `last_mentioned_at`, `follow_up_after`
  - persistence: `times_mentioned`, `source_session_ids`
  - type: `thread_type` (`situational`/`persistent_goal`/`assistant_feedback`)
  - merge lineage: `absorbed_into`
  - scoring: `importance_score`, `salience_score`

### `identity_profile`
- Enduring user synthesis (durable identity, not momentary state).
- Key fields:
  - narrative: `who_they_are`, `family_history`, `faith_and_beliefs`, `current_chapter`
  - structured: `core_values`, `recurring_patterns`, `recurring_fears`, `persistent_goals`
  - inward-facing: `what_they_want`, `what_they_avoid`, `how_they_relate`
  - metadata: `last_synthesized_at`, `source_session_count`, `synthesis_model`

### `living_context`
- Current-state synthesis (surface + tension map).
- Key fields:
  - surface: `current_focus`, `recent_narrative`, `relationship_pulse`, `emotional_texture`
  - underneath: `primary_tension`, `what_theyre_avoiding`, `unspoken_goal`, `why_it_matters`
  - safety/context integrity: `active_contradictions`, `sophie_directives`
  - metadata: `last_synthesized_at`, `sessions_since_last`, `source_session_count`, `synthesis_model`

### `pipeline_checkpoints`
- Per-user/per-pipeline processing cursor.
- Key fields:
  - `pipeline_name`, `last_processed`
  - `sessions_since_last` (used to trigger living-context refresh cadence)

## Retrieval Path Used at Runtime

1. `living_context` drives conversational framing.
2. `open_threads` contributes top unresolved/persistent items.
3. `entity_profiles` contributes salient people/projects.
4. `identity_profile` provides durable anchors.
5. `session_classifications.memory_delta_embedding` remains a legacy episodic recall surface and should not be treated as a Pass 1 extraction contract.

## Derived Attention Moments
- Follow-ups, nudges, check-ins, and review prompts are derived runtime artifacts.
- They are generated by policy/attention logic from primitives (`action_items`, `commitments`, `open_threads`, `signals`).
- They must include source references to originating state objects.
- They are not first-class long-lived semantic truth and should not become a separate canonical followups store.

## Notes

- Synthesis tables (`identity_profile`, `living_context`) are replaceable derived state and are intentionally re-generated from evidence.
- Contradictions are preserved explicitly in living context to avoid silent overwrite behavior.
- Persistent goals are modeled as thread lifecycle state (`thread_type='persistent_goal'`) and should not be auto-snoozed.
