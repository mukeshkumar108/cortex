# Synapse Architecture Decisions (Source of Truth)

Last updated: 2026-04-09

## Core Principles

### 1) Separation of Concerns (Graphiti‑native)
- **Postgres**: operational state only (session_buffer, session_transcript, outbox, caches)
- **Graphiti**: semantic memory (episodes, facts, entities, relationships, temporal reasoning)
- **No local semantic extraction** in Synapse

### 2) Graceful Degradation
- /ingest and /brief must succeed even if Graphiti fails
- Graphiti queries are on-demand and best-effort

### 3) Tenant Isolation
- All Postgres queries filter by `tenant_id`
- Graphiti group_id = `f"{tenant_id}__{user_id}"`
- Tenant aliases are canonicalized at ingress (query + JSON body), currently:
  - `sophie-prod` -> `default`
- Read-path compatibility fan-out:
  - `/memory/query` and `/user/model` read canonical + known aliases
- Physical consolidation:
  - migration `025_tenant_alias_consolidation.sql` merges historical alias rows to canonical

### 4) Cost Discipline
- Sliding window keeps hot context bounded (last 12 messages)
- Rolling summary compresses older turns (local only)
- Graphiti ingestion is per-session (raw transcript on close)

## Memory Architecture Decisions

### Identity
- Identity is derived from Graphiti on-demand (no local extractor)
- Optional caching can be added later if needed

### Session Buffer (Working Memory)
```
session_buffer (tenant_id, session_id)
  - messages: last 6 turns (jsonb array)
  - rolling_summary: compressed history (text)
  - session_state: jsonb (episode name/text, flags)
  - closed_at: timestamp when closed
```

### Session Close
- Sessions close after inactivity (idle close loop, config gated)
- Close sends **raw transcript** as a Graphiti episode (for extraction)
- Close enqueues remaining turns to outbox (operational)

### Outbox
- `graphiti_outbox` is the reliable delivery queue
- Rows are folded once, then sent (or dead-lettered)
- Error classification: permanent → failed; transient → retry with backoff

### Procedural Memory
- Not implemented in Synapse v1; should be modeled in Graphiti (custom entities/edges) if needed

### Narrative Continuity (Graphiti custom types)
- Synapse defines custom entities: MentalState, Tension, Environment
- Custom edges: FEELS, STRUGGLING_WITH, LOCATED_IN
- /session/brief provides a narrative start‑brief derived from these entities

### Graphiti narrative entities (schema + usage)
**Entities**
- `MentalState`: mood, energy_level (user’s emotional/cognitive disposition)
- `Tension`: description, status (unresolved problem/task/friction)
- `Environment`: location_type, vibe (physical/situational context)
- `Observation`: detail (incidental anchor like “raining” or “cold matcha”)

**Edges**
- `Person` → `MentalState`: FEELS
- `Person` → `Tension`: STRUGGLING_WITH
- `Person` → `Environment`: LOCATED_IN
- `Person` → `Observation`: OBSERVED

**Retrieval**
- `/session/brief` uses node-centric search to surface these types.
- Attribute values are read from `node.attributes` (Graphiti stores custom fields there).

## /brief Contract
- Minimal: temporalAuthority, workingMemory, rollingSummary
- No semantic context here; orchestrator queries Graphiti via /memory/query

## /ingest Contract
- sessionId can be top-level or metadata; auto-generated if missing
- Writes to session_buffer immediately; background tasks handle heavy work

## Model Routing Policy v1
- summary → `OPENROUTER_MODEL_SUMMARY` (default `amazon/nova-micro-v1`)
- fallback → `OPENROUTER_MODEL_FALLBACK` (default `mistral/ministral-3b`)
- reasoning toggle: `OPENROUTER_REASONING_ENABLED` (default false)

## Defaults (Config)
- `session_close_gap_minutes`: 15
- `idle_close_interval_seconds`: 300
- `idle_close_enabled`: false
- `outbox_drain_enabled`: false
- `GRAPHITI_PER_TURN`: false (session episodes only)

## Known Gaps (Intentional)
- No local semantic extraction; Graphiti is required for semantic memory
- No automatic recall; orchestrator must call /memory/query
- No metrics/alerts (logging only)

## Recent Validation Decisions (2026-04-09)
- Semantic rerank stays semantic-first:
  - lexical query guardrails are not primary logic
  - query semantics come from semantic classifier path, not keyword fallback
- Confidence calibration improvements were made in semantic classification scoring
  to reduce confidence compression and improve bucket spread.
- Draft schemas were added for:
  - `DerivedUserModel`
  - `RuntimeSteeringPacket`
  These are schema-only and not runtime-critical dependencies yet.

## Startbrief Precedence and Freshness Policy (2026-04-09)
- `/session/startbrief` selection is score-based and evidence-driven.
- Both session-summary claims and loop claims use explicit components:
  - `recency`: newer evidence is weighted higher
  - `salience`: immediate urgency/intensity
  - `importance`: durable relevance across recurrence and loop alignment
  - `confidence`: trust estimate for claim quality
  - `contradiction_penalty`: demotion when claim conflicts with newer evidence
- Contradiction precedence:
  - newer reconciliation evidence takes precedence over older breakup framing
  - stale contradictory relationship claims are penalized in ranking
- Timestamp skew tolerance:
  - near-future timestamp drift (clock skew) is tolerated to avoid dropping fresh summaries
- Traceability:
  - `/session/startbrief` returns ranking evidence (`claim_ranking`, `loop_ranking`, and definitions)
  - `/internal/debug/startbrief/ranking` returns full candidate rankings for diagnostics

## Entity Grounding Surfaces (2026-04-09)
- Startup ambient grounding is provided via `/session/startbrief.entity_hints` (compact only).
- Deep entity inspection is provided via `POST /entities/profile`.
- Scope:
  - supports person/project/company/place/other
  - relationship detail remains inside person profile cards
- Non-goals for now:
  - no `/entities` list endpoint
  - no `/memory/entities`
  - no standalone `/relationship/summary`
- Implementation reuses existing memory surfaces:
  - Graphiti nodes/facts
  - user_model relationship context
  - loops for procedural relevance
