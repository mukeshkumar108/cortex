# Memory Data Model

Purpose: quick reference for the memory tables powering triage, entities, threads, synthesis, and handover.

## Core Tables

### `session_classifications`
- One row per processed session.
- Key fields:
  - `is_memory_worthy`, `session_kind`, `identity_relevant`
  - `raw_triage_output` (JSON object with `memory_deltas`, `identity_signals`, `thread_signals`, etc.)
  - `tension_signal` (optional subtle under-surface cue from user turns)
  - `memory_delta_embedding` (`vector(1536)`) for semantic episodic retrieval

### `entity_profiles`
- Canonicalized long-lived entities (people/projects/places/etc.).
- Key fields:
  - identity/profile: `canonical_name`, `type`, `relationship_to_user`, `profile_text`
  - state: `last_known_status`, `status`, `aliases`
  - memory quality: `mention_count`, `key_facts`, `open_questions`
  - scoring: `core_importance`, `operational_priority`, `importance_score`, `salience_score`

### `open_threads`
- Actionable unresolved threads derived from sessions.
- Key fields:
  - thread content: `title`, `detail`, `category`, `priority`
  - lifecycle: `status` (`open`/`resolved`/`snoozed`)
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
5. `session_classifications.memory_delta_embedding` supports semantic episodic recall (`/memory/search` and handover enrichment).

## Notes

- Synthesis tables (`identity_profile`, `living_context`) are replaceable derived state and are intentionally re-generated from evidence.
- Contradictions are preserved explicitly in living context to avoid silent overwrite behavior.
- Persistent goals are modeled as thread lifecycle state (`thread_type='persistent_goal'`) and should not be auto-snoozed.
