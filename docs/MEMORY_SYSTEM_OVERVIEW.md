# Memory System Overview

Purpose: define what is now implemented in Synapse memory, what each pass does, and how data flows into runtime handover.

## Scope

Current memory stack combines:
- Session-level triage and structured classification (`session_classifications`)
- Entity profiling (`entity_profiles`)
- Open-thread tracking (`open_threads`)
- Identity synthesis (`identity_profile`)
- Living context synthesis (`living_context`)
- Runtime handover packet (`GET /session/handover`)
- Semantic episodic recall over memory deltas (`POST /memory/search`)

## End-to-End Flow

1. Pass 1 triage (`scripts/run_pass1_memory_triage_batch.py`)
- Reads processable sessions.
- Produces `is_memory_worthy`, `session_kind`, `memory_deltas`, entity and thread signals.
- Stores raw model output in `raw_triage_output`.
- Adds `tension_signal` for subtle under-the-surface cues (new).
- Embeds memory deltas into `memory_delta_embedding` for semantic recall (new for future sessions).

2. Entity pipeline (`scripts/run_entity_pipeline.py`)
- Resolves and canonicalizes entities.
- Merges aliases and fixes fragmentation via audit.
- Builds rich profile text plus key facts/open questions.

3. Threads pipeline (`scripts/run_threads_pipeline.py`)
- Extracts unresolved follow-up threads from memory-worthy sessions.
- Applies stronger dedup and category correction audit.
- Supports assistant-feedback thread consolidation.

4. Scoring + lifecycle (`scripts/run_scoring_update.py`)
- Computes importance/salience for entities and threads.
- Applies temporal salience behavior near follow-up windows.
- Promotes repeated goal/commitment/health threads to `persistent_goal`.
- Auto-snoozes stale non-persistent threads.

5. Identity synthesis (`scripts/run_identity_synthesis.py`)
- Re-synthesizes from raw identity evidence (observer-effect guarded).
- Writes `identity_profile` (values, patterns, fears, chapter, etc.).

6. Living context synthesis (`scripts/run_living_context.py`)
- Re-synthesizes recent context window (30d/60d fallback).
- Produces tension map, contradictions, and Sophie directives.
- Uses existing context only as hypothesis.

7. Runtime handover
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
- Re-synthesis over patching to reduce stale narrative lock-in.
- Idempotent pipeline behavior where possible (skip-if-already-correct).
- Conservative merge policy for people/relationships.
- Distinct separation:
  - Identity = enduring characteristics
  - Living context = current tension and active reality
  - Threads = actionable unresolved items

## Operational Cadence

- Pass 1: on new sessions.
- Entity/Thread pipelines: periodic or after notable batch ingest.
- Scoring: after thread/entity updates.
- Identity: daily/weekly or after meaningful identity-relevant growth.
- Living context: weekly minimum or after 3-5 substantive sessions.

