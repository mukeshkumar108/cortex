# Synapse Current State (2026-04-10)

Purpose: describe what exists now in production code. This is not a future-build spec.

## 1) High-level state

Synapse currently runs a 3-layer memory stack:
1. **Exact layer**: canonical Graphiti entities + relationships + facts.
2. **Episodic layer**: Graphiti episodes + transcript-window embeddings for fuzzy prior-conversation recall.
3. **Operational layer**: Postgres durability, replay, orchestration state, and background compute.

Current recall interface is unified:
- `POST /memory/query` with `memoryIntent=exact|episodic|hybrid`.

## 2) What is in Graphiti now

### 2.1 Canonical ontology used for retrieval truth
Node types:
- `person`
- `project`
- `goal`
- `loop`
- `preference`
- `event`

Edge types:
- `related_to_user_as`
- `working_on`
- `pursuing`
- `about`
- `prefers`
- `involved_in`
- `cares_about`
- `evidence_for`

Graphiti extraction and filtering enforce this ontology as the runtime memory truth surface.

### 2.2 Episodic graph objects
- Raw session transcript episodes are written as Graphiti `Episodic` nodes.
- These are queryable as episodic candidates and linked to retrieval evidence.

### 2.3 Internal/legacy graph labels that may still exist
- `SessionSummary`, `Tension`, `Observation`, `Environment`, `MentalState`, `UserFocus`, `Episodic`, `Community`.
- Runtime query path now filters/suppresses internal artifacts where needed for canonical recall behavior.
- `SessionSummary` can still appear as a supporting evidence source, but canonical entities/edges remain authoritative.

## 3) What is in Postgres now

Operational and derived memory tables used in runtime:
- `session_transcript`: durable conversation transcript (source of truth for replay/evidence).
- `session_buffer`: rolling short window for active session context.
- `graphiti_outbox`: async ingest queue and post-ingest hooks.
- `episodic_memory_embeddings`: transcript-window embeddings for episodic candidate lookup.
- `loops`: procedural commitments/threads/habits/frictions.
- `user_model`: synthesized durable profile and relationship context.
- `daily_analysis`: periodic reflective synthesis artifacts.
- `startbrief_history`: startbrief trace/debug history.

## 4) What each main endpoint does today

### 4.1 `GET /session/startbrief`
- Startup continuity packet.
- Returns bridge/handover plus compact grounding (`entity_hints`, loops/freshness/ranking evidence).
- Primary startup surface for backend runtime.

### 4.2 `POST /memory/query`
- Unified semantic recall endpoint.
- `exact`: fact/entity-centric retrieval.
- `episodic`: conversation-moment retrieval using episodes + embeddings + evidence lines.
- `hybrid`: combines both.
- Returns:
  - `factItems`, `entities`, `episodes`
  - metadata with episodic guardrails (`episodicWeakRecall`), ranking audit, and embedding coverage.

### 4.3 `POST /entities/profile`
- Canonical entity deep-dive card (on demand), with factual grounding and optional loop relevance.

### 4.4 `GET /memory/loops`
- Prioritized procedural memory list (active loops), user-scoped.

### 4.5 `GET /user/model`
- Synthesized durable user profile and completeness/staleness metadata.

### 4.6 `POST /session/ingest` (and `/ingest`)
- Canonical writeback path for transcript durability and Graphiti/outbox processing.

## 5) Exact vs episodic behavior (current)

### Exact layer (strongest for)
- identity and role questions
- relationship grounding
- durable project/goal/preference queries

Current mechanism:
- graph facts + graph nodes (+ selected supporting summary/user_model evidence)
- provenance/domain/intent metadata for ranking and routing.

### Episodic layer (strongest for)
- “remember that thread/idea/conversation” prompts
- continuation prompts with weak lexical overlap

Current mechanism:
- candidate episodes from Graphiti
- candidate windows from `episodic_memory_embeddings`
- hybrid ranking signals: embedding similarity, lexical overlap, recency, linked entity overlap, continuation behavior, user-turn density
- weak-recall guardrails to reduce overconfident misses.

## 6) Episode summaries: do they exist, and do they belong?

- Yes, session-summary artifacts exist in the system.
- They are useful as **supporting evidence and continuity hints**.
- They are **not** the canonical truth layer.
- Canonical truth remains entity/edge/fact grounding plus evidence-backed episodes.

## 7) Current strengths

1. Canonical Graphiti-first memory model is in place.
2. Exact and episodic retrieval are both operational under one endpoint.
3. Hybrid mode gives practical default behavior for mixed prompts.
4. Weak-recall and embedding-coverage telemetry provide safety and observability.
5. Transcript durability remains intact in Postgres for replay and evidence.

## 8) Current weak points

1. Reflective/philosophical fuzzy prompts are still less stable than entity-anchored prompts.
2. Quality is sensitive to embedding coverage and ingest health.
3. Session-summary/internal artifact bleed-through still requires active filtering discipline.
4. Confidence behavior needs continuous monitoring to avoid subtle overclaiming.

## 9) What is still missing (high level)

Not missing in architecture, but still maturing in quality:
1. More reliable reconstruction quality for fuzzy episodic continuation from top windows.
2. Continued calibration by prompt type (relationship vs reflective vs continuation).
3. Strong production feedback loops (weak-recall rate, correction rate, candidate health) to prevent silent drift.

## 10) Practical bottom line

Current Synapse state is:
- **Graph truth for exact memory**
- **Graph+embedding retrieval for episodic memory**
- **Postgres durability/replay substrate**

The system is no longer summary-first memory. It is now canonical-graph-first with episodic evidence retrieval and operational guardrails.
