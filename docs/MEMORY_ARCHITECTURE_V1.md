# Synapse Memory Architecture v1 (Baseline)

## 1) What this repo is
Synapse is the memory backend for the voice assistant product. It provides:
- ingestion and long-term memory persistence
- semantic retrieval and procedural retrieval APIs
- start-brief and continuity context surfaces
- background enrichment/analysis loops

It is not just a storage layer; it is a retrieval and synthesis system used by runtime orchestration.

## 2) Core stores
- Postgres (operational + derived memory):
  - `session_transcript`, `session_buffer`
  - `graphiti_outbox` (+ post-ingest hooks)
  - `user_model`
  - `loops`
  - `daily_analysis`
  - `startbrief_history`
- Graphiti/FalkorDB (semantic graph memory):
  - episodes
  - facts/edges
  - entities/nodes (including `SessionSummary`)

## 3) End-to-end flow
1. App sends turns to `/ingest` or full session to `/session/ingest`.
2. Session pipeline persists transcript and enqueues Graphiti outbox jobs.
3. Outbox drain writes raw episodes and post-ingest artifacts (`SessionSummary`, loop extraction).
4. Background jobs update `user_model`, enrichment narratives, daily analysis, and hygiene.
5. Retrieval APIs combine surfaces for runtime use:
   - `/memory/query` (semantic recall)
   - `/memory/loops` (procedural commitments/threads/habits)
   - `/user/model` (synthesized durable profile)
   - `/session/brief` and `/session/startbrief` (startup continuity)

## 4) Memory surfaces and intended usage
- `/memory/query`:
  - targeted recall for user questions and continuity prompts
  - now fuses Graphiti facts + `SessionSummary` evidence + query-relevant `user_model` evidence
  - returns provenance and domain distribution metadata
- `/memory/loops`:
  - durable actionable threads (procedural memory)
- `/user/model`:
  - synthesized user picture over time (north-star goals, relationships, patterns, current focus)
- `/session/brief` and `/session/startbrief`:
  - compact startup context/handover for low-latency voice

## 5) Entity and schema tracking (current)
- Graphiti custom narrative entities:
  - `MentalState`, `Tension`, `Environment`, `Observation`, `UserFocus`
- Procedural loop types:
  - `commitment`, `decision`, `friction`, `habit`, `thread`
- User model domains:
  - `relationships`, `work`, `health`, `spirituality`, `general`

## 6) Current strengths
- Multi-surface memory with both semantic and procedural retrieval.
- Outbox architecture reduces ingest latency and supports retries.
- Startbrief pipeline includes freshness and evidence diagnostics.
- Tenant alias normalization exists and read-path fallback now covers aliases.

## 7) Current gaps
- Domain model is still split across different subsystems (`career` in loops vs `work` in user model).
- Freshness still depends on drain health/SLOs; lag can create cross-surface drift.
- No single always-on quality evaluator for recall precision/coverage/latency.
- Contract docs are partly out of sync with latest response shape.

## 8) Immediate hardening priorities
1. Standardize retrieval contract and provenance semantics.
2. Canonicalize domain taxonomy across loops, user model, and recall.
3. Add repeatable memory quality evaluation (fixture-driven).
4. Define operational freshness SLOs and drift alerts.
