# Synapse Memory Audit v1 (As-Built)

Last updated: 2026-02-05

## 0) System definition (10 lines max)
- Synapse is a FastAPI memory service with a sliding-window session buffer and Graphiti‑native semantic memory.
- Postgres is the system of record for session_buffer, session_transcript, and graphiti_outbox (operational only).
- Session buffer keeps rolling_summary + last 12 messages (6 user+assistant turns); evicted turns go to graphiti_outbox.
- Background janitor/drain folds outbox rows into rolling_summary; raw transcripts are sent to Graphiti on session close.
- Graphiti is best-effort; failures never block /ingest or /brief.
- Loop extraction is LLM-first and best-effort; procedural state lives in Postgres.
- Tenant isolation uses composite group_id `f"{tenant_id}__{user_id}"` in Graphiti.
- /brief returns only minimal Postgres data; semantic memory is queried via /memory/query.
- Idle session close runs in background (configurable) to send raw transcript episodes to Graphiti.
- Outbox ensures turns are not lost and retries are backoff-controlled.

## 1) Architecture truth table (canonical vs derived vs best-effort)
| Component | Storage | Source of truth | Notes |
| --- | --- | --- | --- |
| `session_buffer` | Postgres | Canonical | Sliding window: `rolling_summary` + `messages` (<=12). `src/session.py::SessionManager` |
| `graphiti_outbox` | Postgres | Canonical for delivery | Guarantees eventual Graphiti delivery; drain loop optional. `src/session.py::drain_outbox` |
| `session_transcript` | Postgres | Canonical (operational) | Raw session transcript archive. |
| `session_transcript` | Postgres | Derived | Archive of evicted turns (best-effort). `src/session.py::_append_transcript_turn` |
| Graphiti episodes | FalkorDB | Derived (best-effort) | Raw session transcript episodes (per-session by default). `src/graphiti_client.py` |
| Graphiti facts/entities | FalkorDB | Derived (best-effort) | Queried via /memory/query on demand. |
| Rolling summary | Postgres | Derived | Lossy summary, not canonical. `src/session.py::_fold_into_summary` |

## 2) Postgres schema audit (tables, PKs, indices)
### Tables
- `session_buffer` (PK: tenant_id, session_id)
- `session_transcript` (PK: tenant_id, session_id)
- `graphiti_outbox` (PK: id)
- `session_transcript` (PK: tenant_id, session_id)
- `schema_migrations`

### Key columns
- `session_buffer`: messages (jsonb array), rolling_summary (text), closed_at (timestamptz)
- `graphiti_outbox`: status, attempts, next_attempt_at, folded_at, last_error
- `session_transcript`: messages (jsonb array)
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
5) No local semantic extraction. Graphiti handles facts/entities from episodes.
7) Return immediately.

**Code pointers:**
- `src/ingestion.py::ingest`
- `src/session.py::SessionManager.add_turn`
- `src/session.py::SessionManager.janitor_process`

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
- /brief returns only temporal authority + workingMemory + rollingSummary
- Semantic memory is fetched via /memory/query (Graphiti)
- /session/brief returns narrative start‑brief from Graphiti nodes + edge facts
- `episodeBridge` is fetched from latest local session summary (Postgres)

**Code pointers:**
- `src/briefing.py::build_briefing`
- `src/graphiti_client.py::search_facts`, `search_nodes`, `get_latest_session_summary`

## 6) Graphiti levers audit
- group_id: `f"{tenant_id}__{user_id}"`
- rerank: enabled by default in search
- reference_time: passed from /brief
- episode naming: deterministic for raw session transcript episodes
- per-turn episodes: behind `GRAPHITI_PER_TURN` (default false)

## 6.5) Extraction responsibilities (who does what)
- **Entities/edges/facts** are extracted by **Graphiti’s LLM pipeline**, not Synapse.
- Synapse **only creates episodes** (session summaries by default, per-turn optional).
- Postgres stores **operational memory only** (sessions, transcripts, outbox).
- **We do not mirror entities/edges in Postgres**; they remain in Graphiti and are queried on demand.

## 6.6) Graphiti narrative entities (what /session/brief uses)
**Custom entity types**
- `MentalState`: mood, energy_level
- `Tension`: description, status (unresolved by default)
- `Environment`: location_type, vibe

**Custom edge types**
- `Person` → `MentalState`: FEELS
- `Person` → `Tension`: STRUGGLING_WITH
- `Person` → `Environment`: LOCATED_IN

**Notes**
- Attributes are stored in `node.attributes` and must be read from there.
- `/session/brief` uses Graphiti node-centric search to surface these nodes.

## 7) Tests audit (what is covered)
- Sliding window invariant (<=12) and outbox behaviors
- Outbox error classification + backoff
- Session close flush + local summary + raw transcript episode
- Loop creation/reinforcement/completion/drop (LLM-first)
- Nudge candidates behavior
- Ingest session_id auto-create + metadata fallback

**Gaps (nice-to-have tests)**
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
- Tenant isolation

**Cannot do yet / known gaps**
- Guaranteed semantic recall if Graphiti is down
- Automated metrics/alerts

## 10) Known decisions (pragmatic defaults)
- `session_close_gap_minutes`: 15 by default
- `idle_close_interval_seconds`: 300
- `outbox_drain_enabled`: off by default
- `GRAPHITI_PER_TURN`: false (raw transcript episodes only)

## 11) How memory retrieval actually works (no magic)
- Synapse does **not** automatically push semantic memory into prompts.
- The orchestrator must call `/memory/query` for Graphiti recall.
- Example: user says “I just had a fight with Ashley.”
  - If Ashley exists in Graphiti, orchestrator should call `/memory/query` with `query: "Ashley"` or a short user query.
  - /memory/query returns facts/entities if Graphiti has them; otherwise empty.
