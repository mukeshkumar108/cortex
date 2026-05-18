# Memory System Overview

Purpose: define what is now implemented in Synapse memory, what each pass does, and how data flows into runtime handover.

## Scope

Current memory stack combines:
- Session-level triage and structured classification (`session_classifications`)
- Action/tool candidate extraction (`actionable_candidates`)
- Entity profiling (`entity_profiles`)
- Open-thread tracking (`open_threads`)
- Identity synthesis (`identity_profile`)
- Living context synthesis (`living_context`)
- Runtime handover packet (`GET /session/handover`)
- Semantic episodic recall over memory deltas (`POST /memory/search`)

## End-to-End Flow

1. Pass 1 triage (`scripts/run_pass1_memory_triage_batch.py`)
- Reads processable sessions.
- Produces routing metadata only: `is_memory_worthy`, `session_kind`, and booleans for downstream lanes.
- Stores raw model output in `raw_triage_output`.
- Adds `tension_signal` for subtle under-the-surface cues (new).

2. Pass 2 action/tool candidates
- Runs only when Pass 1 sets `run_actionable_pass=true`.
- Extracts attention-worthy items from messy user language into `actionable_candidates`.
- Keeps `record_type` coarse and uses `candidate_subtype` for more specific tool/action shape.
- Only emits standalone useful items; unresolved pronoun fragments should be skipped rather than stored as weak review rows.
- Uses reference-time temporal grounding for relative dates/times.
- Stores only unconfirmed candidate state; no auto-act and no external API calls.

3. Entity pipeline (`scripts/run_entity_pipeline.py`)
- Resolves and canonicalizes entities.
- Merges aliases and fixes fragmentation via audit.
- Builds rich profile text plus key facts/open questions.

4. Threads pipeline (`scripts/run_threads_pipeline.py`)
- Extracts unresolved follow-up threads from memory-worthy sessions.
- Applies stronger dedup and category correction audit.
- Supports assistant-feedback thread consolidation.

5. Scoring + lifecycle (`scripts/run_scoring_update.py`)
- Computes importance/salience for entities and threads.
- Applies temporal salience behavior near follow-up windows.
- Promotes repeated goal/commitment/health threads to `persistent_goal`.
- Auto-snoozes stale non-persistent threads.

6. Identity synthesis (`scripts/run_identity_synthesis.py`)
- Re-synthesizes from raw identity evidence (observer-effect guarded).
- Writes `identity_profile` (values, patterns, fears, chapter, etc.).

7. Living context synthesis (`scripts/run_living_context.py`)
- Re-synthesizes recent context window (30d/60d fallback).
- Produces tension map, contradictions, and Sophie directives.
- Uses existing context only as hypothesis.

8. Runtime handover
- `GET /session/handover?user_id=...` assembles:
  - living context
  - directives + contradictions
  - top open threads
  - salient people
  - clustered projects
  - identity anchors
  - episodic recall snippets

## Design Principles

- User turns are the source of truth for fact extraction.
- Pass 1 is routing only; extraction belongs in dedicated downstream lanes.
- LLMs decide meaning; deterministic code enforces contract, validation, persistence, lifecycle, and serving boundaries.
- Re-synthesis over patching to reduce stale narrative lock-in.
- Idempotent pipeline behavior where possible (skip-if-already-correct).
- Conservative merge policy for people/relationships.
- Distinct separation:
  - Identity = enduring characteristics
  - Living context = current tension and active reality
  - Threads = actionable unresolved items

## Operational Cadence

- Pass 1: on new sessions.
- Actionable / Entity / Thread pipelines: periodic or after notable batch ingest.
- Scoring: after thread/entity updates.
- Identity: daily/weekly or after meaningful identity-relevant growth.
- Living context: weekly minimum or after 3-5 substantive sessions.
