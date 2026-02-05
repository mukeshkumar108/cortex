# Synapse Memory Audit v1 (As-Built)

Last updated: 2026-02-05

## 0) System definition (10 lines max)
- Synapse is a FastAPI memory service with a sliding-window session buffer and Graphiti-backed semantic memory.
- Postgres is the system of record for session_buffer, identity_cache, loops, and graphiti_outbox.
- Session buffer keeps rolling_summary + last 12 messages (6 user+assistant turns); evicted turns go to graphiti_outbox.
- Background janitor/drain folds outbox rows into rolling_summary and (optionally) writes Graphiti episodes.
- Graphiti is best-effort; failures never block /ingest or /brief.
- Loop extraction is LLM-first and best-effort; procedural state lives in Postgres.
- Tenant isolation uses composite group_id `f"{tenant_id}__{user_id}"` in Graphiti.
- /brief returns Postgres Tier 1 data; Graphiti enriches facts/entities/episodeBridge if available.
- Idle session close runs in background (configurable) to generate session summary episodes.
- Outbox ensures turns are not lost and retries are backoff-controlled.

## 1) Architecture truth table (canonical vs derived vs best-effort)
| Component | Storage | Source of truth | Notes |
| --- | --- | --- | --- |
| `session_buffer` | Postgres | Canonical | Sliding window: `rolling_summary` + `messages` (<=12). `src/session.py::SessionManager` |
| `graphiti_outbox` | Postgres | Canonical for delivery | Guarantees eventual Graphiti delivery; drain loop optional. `src/session.py::drain_outbox` |
| `identity_cache` | Postgres | Derived cache | Graphiti-derived identity cache used by /brief. `src/identity_cache.py` |
| `loops` | Postgres | Canonical | LLM-first procedural memory; evidence stored in metadata. `src/loops.py::LoopManager` |
| `session_transcript` | Postgres | Derived | Archive of evicted turns (best-effort). `src/session.py::_append_transcript_turn` |
| Graphiti episodes | FalkorDB | Derived (best-effort) | Session summary episodes (per-session by default). `src/graphiti_client.py` |
| Graphiti facts/entities | FalkorDB | Derived (best-effort) | Queried during /brief only if `query` provided. |
| Rolling summary | Postgres | Derived | Lossy summary, not canonical. `src/session.py::_fold_into_summary` |

## 2) Postgres schema audit (tables, PKs, indices)
### Tables
- `session_buffer` (PK: tenant_id, session_id)
- `session_transcript` (PK: tenant_id, session_id)
- `graphiti_outbox` (PK: id)
- `loops` (PK: id)
- `identity_cache` (PK: tenant_id, user_id)
- `schema_migrations`
- `identity_cache` (legacy; present but unused)

### Key columns
- `session_buffer`: messages (jsonb array), rolling_summary (text), closed_at (timestamptz)
- `graphiti_outbox`: status, attempts, next_attempt_at, folded_at, last_error
- `loops`: type/status/text/confidence/salience/time_horizon/source_turn_ts + metadata (jsonb)
- `identity_cache`: preferred_name, timezone, facts (jsonb)
- `session_transcript`: messages (jsonb array)

### Indices
- Outbox: status/tenant/user, tenant/session, next_attempt_at
- Session buffer: tenant/user, PK index (tenant_id, session_id)
- Loops: tenant/user/status + embedding index (pgvector)

## 3) /ingest write-path sequence diagram + code pointers
**Sequence (sliding window + outbox):**
1) Resolve session_id (top-level, metadata, or auto-generate). `src/ingestion.py::ingest`
2) Check session close gap (in-request, based on last user message). `src/session.py::should_close_session`
3) Add turn to buffer; evict oldest if >12. `src/session.py::add_turn`
4) If evicted, enqueue janitor (background). `src/session.py::janitor_process`
5) Identity updates are Graphiti-derived and cached (best-effort). `src/identity_cache.py`
6) Loop extraction (background, user turns). `src/loops.py::extract_and_create_loops`
7) Return immediately.

**Code pointers:**
- `src/ingestion.py::ingest`
- `src/session.py::SessionManager.add_turn`
- `src/session.py::SessionManager.janitor_process`
- `src/loops.py::extract_and_create_loops`

## 4) Outbox reliability contract + failure modes
**Contract**
- Every evicted turn is inserted into `graphiti_outbox` before buffer truncation.
- Outbox rows are folded exactly once into rolling_summary (`folded_at` guard).
- Sending to Graphiti is retried with exponential backoff (`next_attempt_at`).
- Permanent errors dead-letter immediately; transient errors reschedule.

**Graphiti down**
- Outbox rows remain pending with backoff.
- /ingest and /brief still succeed.

**LLM down**
- Folding falls back to safe behavior or reschedules; outbox retries continue.

## 5) /brief read-path contract + Graphiti query strategy
- Tier 1: Postgres (identity from `identity_cache`, workingMemory, rollingSummary, activeLoops, nudgeCandidates)
- Tier 2: Graphiti (facts/entities) only if `query` is provided
- `reference_time=now` is passed to Graphiti search calls when supported
- `episodeBridge` is fetched from latest session summary episode

**Code pointers:**
- `src/briefing.py::build_briefing`
- `src/graphiti_client.py::search_facts`, `search_nodes`, `get_latest_session_summary`

## 6) Graphiti levers audit
- group_id: `f"{tenant_id}__{user_id}"`
- rerank: enabled by default in search
- reference_time: passed from /brief
- episode naming: deterministic for session summary episodes
- per-turn episodes: behind `GRAPHITI_PER_TURN` (default false)

## 6.5) Extraction responsibilities (who does what)
- **Entities/edges/facts** are extracted by **Graphiti’s LLM pipeline**, not Synapse.
- Synapse **only creates episodes** (session summaries by default, per-turn optional).
- Postgres stores **procedural memory only** (sessions, loops, outbox, identity_cache).
- **We do not mirror entities/edges in Postgres**; they remain in Graphiti and are queried on demand.

## 7) Tests audit (what is covered)
- Sliding window invariant (<=12) and outbox behaviors
- Outbox error classification + backoff
- Session close flush + session summary episode
- Loop creation/reinforcement/completion/drop (LLM-first)
- Nudge candidates behavior
- Ingest session_id auto-create + metadata fallback

**Gaps (nice-to-have tests)**
- Identity cache sync from Graphiti (if/when needed)
- Idle close loop behavior
- Outbox drain loop behavior under no-traffic

## 8) Ops audit
- Internal drain endpoint + optional drain loop (config gated)
- Idle close loop (config gated) to close stale sessions
- No metrics/alerts (logging only)

## 9) Capability inventory
**Can do**
- Sliding window memory
- Reliable outbox delivery
- Session summary episodes
- LLM-first loop memory with evidence + nudges
- Tenant isolation

**Cannot do yet / known gaps**
- Deterministic identity extraction (by requirement)
- Guaranteed semantic recall if Graphiti is down
- Automated metrics/alerts

## 10) Known decisions (pragmatic defaults)
- `session_close_gap_minutes`: 15 by default
- `idle_close_interval_seconds`: 300
- `outbox_drain_enabled`: off by default
- `GRAPHITI_PER_TURN`: false (session summary episodes only)

## 11) How memory retrieval actually works (no magic)
- Synapse does **not** automatically push semantic memory into prompts.
- The orchestrator must call `/brief` and include `query` if it wants Graphiti recall.
- Example: user says “I just had a fight with Ashley.”
  - If Ashley exists in Graphiti, orchestrator should call `/brief` with `query: "Ashley"` or a short user query.
  - /brief will return facts/entities if Graphiti has them; otherwise only local memory.
