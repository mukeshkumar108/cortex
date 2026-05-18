# Synapse System Audit Report
**Date:** May 18, 2026
**Location:** `/opt/synapse`

## 1. Executive summary
- **Usability:** Synapse is usable today but extremely brittle and over-engineered, suffering from a split-brain architecture.
- **State of pipelines:** It is mostly working, but running two parallel memory paradigms (v1 Graphiti/FalkorDB vs. v2 Postgres/Derived Pipeline).
- **Service to Sophie:** It *does* serve context back to Sophie, but the retrieval surface is fragmented across legacy adapters.
- **Connection end-to-end:** Pipelines are connected but bloated, relying on a monolithic `main.py` (700KB) and `derived_pipeline.py` (340KB).
- **Background Jobs:** Scheduled jobs are active but implemented as infinite `asyncio.create_task` loops inside the FastAPI process, making them highly susceptible to silent failure.
- **Dead Code:** Yes. Example: `/memory/search` returns a hardcoded HTTP 410 Deprecated. Legacy routes and Graphiti adapters exist alongside v2 Postgres implementations.
- **Over-engineering:** Extremely high. There are 8 distinct "passes" (1, 1.5, 2a, 2b, 2c, 3, 4, 5) orchestrated in a single file with heavy JSONB schemas.
- **Top 5 Risks:** 
  1. No authentication on primary ingest/retrieval routes (e.g., `/session/startbrief`, `/session/ingest`).
  2. Background tasks managed directly by `asyncio` inside the main process without a proper message broker or worker queue.
  3. Monolithic codebase structure creating severe bottleneck and memory usage risks.
  4. Split-brain DB (FalkorDB and Postgres) running simultaneously for overlapping memory concepts.
  5. Excessive reliance on JSONB columns without strong schema enforcement at the DB level.
- **Top 5 Fixes:**
  1. Enforce authentication on all public-facing and Sophie-facing routes.
  2. Decouple background jobs into a dedicated worker service (e.g., Celery/Temporal).
  3. Refactor `main.py` and `derived_pipeline.py` into smaller, logical domains.
  4. Fully sunset FalkorDB and Graphiti legacy code, standardizing entirely on Postgres v2.
  5. Add database indexes to frequently queried JSONB fields.

---

## 2. System map

| Area | File/path | Purpose | Status | Evidence |
|---|---|---|---|---|
| Main API | `src/main.py` | Core FastAPI application handling all routes and loops. | Working but bloated | 700KB file containing all endpoints. |
| Memory Logic | `src/derived_pipeline.py` | Orchestrates the multi-pass extraction and pipeline logic. | Working | 340KB file containing passes 1 through 5. |
| Graph Client | `src/graphiti_client.py` | Interface to legacy v1 Graphiti semantic memory. | Legacy/Active | References FalkorDB driver. |
| Database Init | `schema.sql` | Base initialization for the Postgres database. | Active | Defines >40 tables. |
| Docker Config | `docker-compose.yml` | Manages `synapse`, `postgres`, and `falkordb` containers. | Working | `docker compose ps` shows 3 healthy containers. |
| DB Migrations | `migrations/` | Schema evolutions. | Active | 62 `.sql` files. |
| Auth Layer | `src/main.py` (`_require_internal_token`) | Secures internal endpoints. | Partially Broken | Not applied to primary endpoints like `/ingest`. |

---

## 3. Inputs audit

| Input | Endpoint/script | Payload | Auth | Writes to | Triggers | Connected to Sophie? | Status |
|---|---|---|---|---|---|---|---|
| V2 Ingest | `POST /session/ingest` | `SessionIngestRequest` | None | `session_buffer` | `enqueue_session_ingest` | Probably | Active |
| Legacy Ingest | `POST /ingest` | `IngestRequest` | None | `session_buffer` | Background janitors | Yes (legacy) | Active |
| Close Session | `POST /session/close` | `SessionCloseRequest` | None | DB | Flushes graphiti | Yes | Active |
| Actions | `POST /actions/items` | Unknown | None | DB | - | Unclear | Active |
| Calendar Import | `POST /integrations/google/calendar/import` | Unknown | None | DB | - | Unclear | Active |
| Internal Drain | `POST /internal/drain` | Parameters | None | Outbox/DB | Drain Loop | No | Active |

**Missing Inputs:**
- Missing secure ingestion path (all primary paths lack auth checks based on code inspection).

---

## 4. Outputs audit

| Output | Endpoint | Source tables | Selection logic | Payload quality | Sophie-ready? | Status |
|---|---|---|---|---|---|---|
| Start Brief | `GET /session/startbrief` | `derived_postgres` | `STARTBRIEF_TOPK_CLAIMS`, min scores | Structured sheet, open loops | Yes | Active |
| Legacy Query | `POST /memory/search` | - | - | Returns 410 Gone | No | Disabled |
| V2 Query | `POST /memory/query` | `sessions_v2`, `claims`, etc. | `factual`, `episodic`, `continuity` lanes | Hybrid extraction | Yes | Active |
| Signals Pack | `GET /signals/pack` | Unknown | Unknown | Unknown | Unclear | Active |
| User Model | `GET /user/model` | DB | Returns `UserModelResponse` | Unknown | Yes | Active |
| Day Brief | `GET /day/brief` | DB | Unknown | Unknown | Unclear | Active |

---

## 5. Pipeline audit

The pipeline is implemented as synchronous code paths in `src/derived_pipeline.py`. 

| Stage | Files | Trigger | Input | Output/table | Called in runtime? | Status | Notes |
|---|---|---|---|---|---|---|---|
| Pass 1: Triage | `derived_pipeline.py` | Ingestion | Messages | Payload | Yes | Active | Triage / router |
| Pass 1.5: Entities | `derived_pipeline.py` | Triage | Messages | `entities` | Yes | Active | Entity extraction |
| Pass 2a: Actionable | `derived_pipeline.py` | Post-triage | Messages | `actionable_candidates` | Yes | Active | Action logic |
| Pass 2b: Changes | `derived_pipeline.py` | Post-triage | Messages | `session_changes` | Yes | Active | Session states |
| Pass 2c: Entity Cands | `derived_pipeline.py` | Post-triage | Messages | `entity_candidates` | Yes | Active | Entity tracking |
| Pass 3: Threads | `derived_pipeline.py` | - | Messages | `open_threads` | Yes | Active | Open loops |
| Pass 4: Identity | `derived_pipeline.py` | Conditions | Messages | `identity_profile` | Yes | Active | Identity synth |
| Pass 5: Living Ctx | `derived_pipeline.py` | Conditions | Messages | `living_context` | Yes | Active | Living context |

**End-to-End Flow:**
```text
User/Sophie message
  → POST /session/ingest
  → added to session_buffer
  → Background task (idle_close_loop / drain_loop) picks it up
  → derived_pipeline.py (Passes 1-5 executed sequentially/conditionally)
  → Data persisted to v2 tables and/or falkordb via graphiti_client
  → Sophie calls GET /session/startbrief or POST /memory/query
  → Data retrieved and formatted
```

---

## 6. Storage/database audit

| Table | Purpose | Writers | Readers | Status | Issues |
|---|---|---|---|---|---|
| `session_buffer` | Transient message store | `/ingest` | Background workers | Active | JSONB array usage |
| `graphiti_outbox` | V1 queue | Workers | Workers | Active | Legacy overlap |
| `sessions_v2` | V2 session store | Pipeline | Retrieval | Active | |
| `claims` | Factual memory | Pipeline | Retrieval | Active | Heavy JSONB metadata |
| `actionable_candidates` | V2 Actions | Pipeline Pass 2a | Action endpoints | Active | |
| `entity_candidates` | V2 Entities | Pipeline Pass 2c | Entity endpoints | Active | |

**Issues:**
- High reliance on `JSONB` for `metadata`, `messages`, `payload`, `details`, creating schema enforcement risks.
- Duplicate data representations (Graphiti nodes in FalkorDB vs. Postgres v2 tables).

---

## 7. Scheduling and background services audit

All background processes are implemented inside `src/main.py` utilizing `asyncio.create_task` bound to the FastAPI `app.state`. There is no external scheduler.

| Job/service | Schedule/trigger | Runner | Command/file | Active? | Status |
|---|---|---|---|---|---|
| `idle_close_loop` | Continual (`INTERVAL_SECONDS`) | FastAPI Process | `main.py` / `session.py` | Yes | Brittle |
| `drain_loop` | Continual | FastAPI Process | `main.py` / `session.py` | Yes | Brittle |
| `user_model_updater_loop` | Continual | FastAPI Process | `main.py` | Yes | Brittle |
| `user_model_enrichment_loop` | Continual | FastAPI Process | `main.py` | Yes | Brittle |
| `loop_staleness_janitor_loop` | Continual | FastAPI Process | `main.py` | Yes | Brittle |
| `daily_analysis_loop` | Continual | FastAPI Process | `main.py` | Yes | Brittle |
| `v2_invariant_checker_loop` | Continual | FastAPI Process | `main.py` | Yes | Brittle |

**Verdict:** 
Daily, weekly, and pipeline services are theoretically running, but because they are trapped inside the synchronous memory space of an async API server, a crash or resource leak will take down both the API and the workers. 

---

## 8. Sophie integration audit

| Sophie need | Synapse endpoint | Payload | Response | Status | Problem |
|---|---|---|---|---|---|
| Session start | `GET /session/startbrief` | query params | `SessionStartBriefResponse` | Connected | Unauthenticated |
| After msg | `POST /session/ingest` | `SessionIngestRequest` | `SessionIngestResponse` | Connected | Unauthenticated |
| Session close | `POST /session/close` | `SessionCloseRequest` | Boolean | Connected | Unauthenticated |
| Context Retrieval | `POST /memory/query` | `MemoryQueryRequest` | `MemoryQueryResponse` | Connected | Legacy adapter bridge |

---

## 9. Retrieval and serving audit

| Retrieval surface | Logic | Strength | Weakness | Status |
|---|---|---|---|---|
| `/session/startbrief` | Fetches recent derived continuity, facts, and open loops. Top K claims and loops gated by `0.50` and `0.55` scores. | Fast, direct Postgres query. | Hardcoded threshold values, unauthenticated. | Active |
| `/memory/query` | Employs legacy adapter pointing to v2 lanes (`factual`, `episodic`, `continuity`, `hybrid`). | Clear logical lanes. | Complex adapter pattern obscures actual performance. | Active |

**Product Verdict:**
As of now, Sophie **would** be able to rely on Synapse for continuity, but the output is likely a mix of legacy and v2 data formats, and the system is operating without foundational security controls.

---

## 10. Overlap, duplication, and over-engineering audit

| Area | Duplicate/overlap | Files | Risk | Recommendation |
|---|---|---|---|---|
| V1 vs V2 | Graphiti (FalkorDB) vs Derived Pipeline (Postgres) | `graphiti_client.py` vs `derived_pipeline.py` | High | Retire Graphiti/FalkorDB. Freeze pipeline and move all writes to Postgres. |
| Pipelines | Passes 1 through 5 | `derived_pipeline.py` | High | Decouple passes. A 340KB python file cannot be safely maintained. |
| Background Jobs | `asyncio.create_task` | `main.py` | High | Move to a Celery/Temporal-based dedicated worker. |

---

## 11. Broken areas audit

| Severity | Issue | Evidence | Why it matters | Suggested fix |
|---|---|---|---|---|
| **Critical** | Missing Auth on Ingest | `main.py` routing lacking `_require_internal_token` | Anyone can inject memories. | Add token validation to public endpoints. |
| High | Monolithic architecture | 700KB `main.py`, 340KB pipeline | Maintainability nightmare. | Refactor into modular structure. |
| Medium | Deprecated Endpoints | `POST /memory/search` returning 410 | Clutters routing logic. | Delete the route. |
| Medium | Brittle Async Tasks | `asyncio.create_task` loops | Memory leaks, silent death. | Use a dedicated task queue. |

---

## 12. Security audit

| Severity | Security issue | File/path | Evidence | Fix |
|---|---|---|---|---|
| **Critical** | Unauthenticated endpoints | `src/main.py` | `/session/startbrief`, `/ingest` routes | Wrap in auth middleware. |
| High | Excessive JSONB Data | `schema.sql` | `payload`, `details` lacking constraints | Implement JSON Schema validation inside Postgres or Pydantic. |

**Is the API safe to expose publicly?** 
**NO.** It must be behind a private network/VPN immediately until authentication is standardized across all endpoints.

---

## 13. Performance and efficiency audit

| Issue | Evidence | Impact | Fix |
|---|---|---|---|
| Background Task Bloat | `main.py` | High memory footprint in a single pod. | Split API from Workers. |
| DB connection overhead | `main.py` | All jobs and web requests share the same pool. | Connection pooling optimization and splitting apps. |
| FalkorDB memory | `docker-compose.yml` | Graphiti is memory hungry. | Sunset Graphiti if V2 is ready. |

---

## 14. Observability audit

| Observability need | Exists? | Where | Missing |
|---|---|---|---|
| Pipeline run IDs | Partial | Logs | Dedicated Trace ID propagation. |
| Error logs | Yes | Console logs | Sentry/Datadog integration. |
| Job status tables | Yes | `graphiti_outbox`, `invariant_repair_actions` | Dashboard or clear UI. |

---

## 15. Testing audit

| Test file | What it tests | Still relevant? | Gaps |
|---|---|---|---|
| `test_session_startbrief.py` | Startbrief logic | Yes | Need end-to-end security/auth tests. |
| `test_derived_pipeline_hardening.py`| V2 extraction passes | Yes | Extremely large (165KB), indicates complex mocks. |
| `test_graphiti_native.py` | Legacy memory graph | Yes | Can be deleted once Graphiti is retired. |

**Minimum Tests Required:**
- Session Ingestion with auth enforcement.
- Startbrief retrieval payload validation.

---

## 16. Runtime/VPS audit

| Runtime area | Current state | Risk | Recommendation |
|---|---|---|---|
| Containers | `api`, `postgres`, `falkordb` | Low | Good baseline. |
| Backups | `/opt/synapse/backups` is empty | High | Implement `pg_dump` cron jobs. |
| Memory Limits | `docker-compose.yml` | Med | Define `deploy.resources.limits` to prevent OOM kills. |

---

## 17. MVP readiness verdict

**Verdict: C. partially usable but needs reconnection.**
Synapse works fundamentally but suffers from severe technical debt (split v1/v2 architecture) and unacceptable security omissions (lack of authentication on main endpoints).

**What to keep:** The Postgres V2 schema, `derived_pipeline` logic (once refactored).
**What to delete:** Graphiti, FalkorDB, `/memory/search`, all legacy adapter abstractions.
**What to rebuild:** Background workers using a proper queue system; authentication middleware.

---

## 18. Recovery plan

**Phase 0: Freeze and inspect**
- Action: Backup database immediately. `pg_dump`.
- Goal: Secure current state.

**Phase 1: Make runtime safe and observable**
- Action: Implement internal token or API key checks on `/ingest`, `/session/ingest`, `/session/startbrief`, and `/memory/query`.
- Goal: Prevent unauthorized data contamination.

**Phase 2: Establish one clean ingestion path**
- Action: Reroute all ingest strictly to the Postgres V2 pipeline. Disable Graphiti dual-writes.

**Phase 3: Establish one clean serving path**
- Action: Remove `_memory_query_legacy_adapter` and route `/memory/query` directly to `_memory_query_v2_service`.

**Phase 4: Reconnect Sophie**
- Action: Validate payload contracts between Sophie and Synapse over the authenticated endpoints.

**Phase 5: Simplify/delete stale architecture**
- Action: Remove FalkorDB container, drop `graphiti_client.py`. Split `main.py` into `api.py` and `worker.py`.

---

## 19. Exact checks to run next

**Safe read-only commands:**
1. `docker compose logs --tail=200 synapse` (Checks for async loop failures)
2. `curl -I http://localhost:8000/health` (Validates uptime)

**Careful commands:**
1. `docker exec -it synapse-postgres psql -U synapse -c "\d"` (Inspect live schema footprint)

**Do-not-run-yet commands:**
1. Any DB migration or data-dropping script.
2. `docker compose restart` (May lose transient in-memory async task states).

---

## 20. Final output requirements

**The 10 most important findings:**
1. No auth on primary endpoints.
2. 700KB `main.py` monolith.
3. Split-brain DB (Postgres/FalkorDB).
4. Background jobs are brittle internal `asyncio` loops.
5. `/memory/search` is dead code (410).
6. 340KB `derived_pipeline.py` is too large to maintain safely.
7. V2 heavily relies on JSONB instead of structured tables.
8. No backups exist in `/opt/synapse/backups`.
9. Legacy adapter obscures retrieval performance.
10. System is functionally serving context, but dangerously.

**The 10 highest-priority fixes:**
1. Add Auth to `/ingest` and `/session/startbrief`.
2. Setup Postgres backups.
3. Split `main.py` into separate routers.
4. Move async loops to Celery/Temporal.
5. Sunset FalkorDB in docker-compose.
6. Delete Graphiti client.
7. Refactor passes into individual files.
8. Delete `/memory/search`.
9. Enforce JSON schema validation on DB writes.
10. Cap memory limits in Docker.

**The 10 files I should inspect first:**
1. `src/main.py`
2. `src/derived_pipeline.py`
3. `schema.sql`
4. `docker-compose.yml`
5. `src/session.py`
6. `src/graphiti_client.py`
7. `tests/test_session_startbrief.py`
8. `src/models.py`
9. `src/config.py`
10. `src/ingestion.py`

**The safest next action:**
Execute `docker compose logs --tail=500 synapse` to verify if any background tasks (like `idle_close_loop`) have crashed silently and to observe live endpoint usage patterns.
