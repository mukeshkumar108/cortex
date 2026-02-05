# Synapse Architecture Decisions (Source of Truth)

Last updated: 2026-02-04

## Core Principles

### 1) Separation of Concerns
- **Postgres**: canonical procedural state (session_buffer, user_identity, loops, outbox)
- **Graphiti**: derived semantic memory (episodes, facts, entities)
- **LLM-first loops**: procedural state is decided by LLM output, not regex rules

### 2) Graceful Degradation
- /ingest and /brief must succeed even if Graphiti/LLM fails
- Tier 2 (Graphiti) is best-effort and optional

### 3) Tenant Isolation
- All Postgres queries filter by `tenant_id`
- Graphiti group_id = `f"{tenant_id}__{user_id}"`

### 4) Cost Discipline
- Sliding window keeps hot context bounded (last 6 turns)
- Rolling summary compresses older turns
- Graphiti ingestion is per-session (default) to reduce cost

## Memory Architecture Decisions

### Identity
- Canonical identity is derived from Graphiti and cached in `identity_cache`
- `user_identity` is legacy and no longer updated by ingestion
- Identity cache is best-effort; if unknown, orchestrator should avoid using a name in replies

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
- Close creates a session summary episode (Graphiti) best-effort
- Close enqueues remaining turns to outbox

### Outbox
- `graphiti_outbox` is the reliable delivery queue
- Rows are folded once, then sent (or dead-lettered)
- Error classification: permanent → failed; transient → retry with backoff

### Loops (Procedural Memory)
- LLM-first output controls create/reinforce/complete/drop
- Evidence is stored in metadata
- Implicit completion → nudge candidate, not auto-complete
- Explicit completion (confidence ≥ 0.85) → auto-complete

## /brief Contract
- Tier 1: identity, temporalAuthority, workingMemory, rollingSummary, activeLoops
- Tier 2: semanticContext/entities/episodeBridge only if `query` provided
- No "magic" recall — orchestrator decides when to query Graphiti

## /ingest Contract
- sessionId can be top-level or metadata; auto-generated if missing
- Writes to session_buffer immediately; background tasks handle heavy work

## Model Routing Policy v1
- summary → `OPENROUTER_MODEL_SUMMARY` (default `amazon/nova-micro-v1`)
- loops → `OPENROUTER_MODEL_LOOPS` (default `xiaomi/mimo-v2-flash`)
- session_episode → `OPENROUTER_MODEL_SESSION_EPISODE` (default `xiaomi/mimo-v2-flash`)
- fallback → `OPENROUTER_MODEL_FALLBACK` (default `mistral/ministral-3b`)
- reasoning toggle: `OPENROUTER_REASONING_ENABLED` (default false)

## Defaults (Config)
- `session_close_gap_minutes`: 15
- `idle_close_interval_seconds`: 300
- `idle_close_enabled`: false
- `outbox_drain_enabled`: false
- `GRAPHITI_PER_TURN`: false (session episodes only)

## Known Gaps (Intentional)
- No deterministic identity extraction from regex beyond lightweight hints
- No automatic recall; orchestrator must pass `query`
- No metrics/alerts (logging only)
