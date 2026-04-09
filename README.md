# Cortex (Synapse Memory API)

A FastAPI memory service with a sliding‑window session buffer and Graphiti‑native semantic memory. Designed to be reliable, tenant‑isolated, and cheap to run.

Implementation evolution log: [docs/IMPROVEMENT_LOG.md](/opt/synapse/docs/IMPROVEMENT_LOG.md)

## What it does (short)
- **/ingest**: writes turns to Postgres (rolling summary + last 12 messages). Never blocks.
- **/session/startbrief**: canonical startup packet (`handover_text`, `handover_depth`, `time_context`, `resume`, `ops_context`, `evidence`). Optional `sessionId`, `personaId`, `timezone`.
- **/brief**: internal minimal/fallback startup mode (time + working memory + rolling summary).
- **/session/brief**: legacy/internal compatibility startup mode.
- **/memory/query**: on‑demand Graphiti semantic recall (clean facts/entities payload by default).
- **/memory/loops**: prioritized procedural loops (active + needs_review commitments/threads/habits/frictions/decisions; stale hidden).
- **/user/model**: persistent structured user model with domain completeness scores.
- **/analysis/daily**: nightly user-level synthesis (themes + Sophie behavior scores + steering note).
- **/session/close**: legacy/admin-safe finalize endpoint; enqueue-only (idempotent) and marks `session_buffer.closed_at` when present.
- **/session/ingest**: durably upsert full transcript + enqueue internal outbox jobs (`session_raw_episode` then post-ingest hooks).
- **Outbox**: reliable delivery of evicted turns and session ingest jobs.
- **SessionSummary quality**: summaries are generated from full transcript with fallback tiers so `summary_text` and `bridge_text` are always non-empty.

## Quickstart
```bash
cp .env.example .env  # if you add one
# edit env vars as needed

docker compose up --build
```

Health check:
```bash
curl -s http://localhost:8000/health
```

## API: minimal usage
**Recommended loop**
1. `/session/startbrief` once at session start (canonical startup packet)
2. LLM responds
3. `/memory/query` on demand for targeted recall
4. `/memory/loops` on demand for procedural loops
5. `/user/model` on demand for durable user picture
6. Canonical write-back/finalization: `/session/ingest`; `/session/close` remains legacy/admin-safe and enqueue-only.

## Quick API examples
**POST /brief (internal minimal/fallback)**
```bash
curl -s http://localhost:8000/brief -H 'Content-Type: application/json' -d '{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "personaId": "persona_1",
  "sessionId": "session-demo-001",
  "now": "2026-02-06T10:15:00Z"
}'
```

**GET /session/brief (legacy/internal compatibility)**
```bash
curl -s "http://localhost:8000/session/brief?tenantId=tenant_a&userId=user_1&now=2026-02-06T10:15:00Z"
```

**GET /session/startbrief**
```bash
curl -s "http://localhost:8000/session/startbrief?tenantId=tenant_a&userId=user_1&now=2026-02-06T10:15:00Z&timezone=America/Los_Angeles"
```

Exact response payload shape:
```json
{
  "handover_text": "string",
  "narrative": "string|null",
  "handover_depth": "continuation|yesterday|weekly",
  "time_context": {
    "local_time": "HH:MM",
    "time_of_day": "MORNING|AFTERNOON|EVENING|NIGHT",
    "gap_minutes": 0,
    "sessions_today": 0,
    "first_session_today": true
  },
  "resume": {
    "use_bridge": false,
    "bridge_text": "string|null"
  },
  "ops_context": {
    "top_loops_today": [],
    "waiting_on": [],
    "user_model_hints": [],
    "yesterday_themes": [],
    "steering_note": "string|null"
  },
  "evidence": {
    "session_summary_ids_used": [],
    "session_summary_ids_fetched": [],
    "claim_ranking": [
      {
        "session_id": "string",
        "score": 0.0,
        "recency": 0.0,
        "salience": 0.0,
        "importance": 0.0,
        "confidence": 0.0,
        "contradiction_penalty": 0.0
      }
    ],
    "claim_ranking_defs": {
      "salience": "Immediate intensity/urgency of a session claim (short-horizon prominence).",
      "importance": "Durable relevance inferred from recurrence across recent sessions and alignment with active loops.",
      "confidence": "Trust in claim quality based on summary quality and salience band."
    },
    "loop_ranking": [
      {
        "id": "string|null",
        "text": "string",
        "type": "string|null",
        "score": 0.0,
        "recency": 0.0,
        "salience": 0.0,
        "importance": 0.0,
        "confidence": 0.0,
        "contradiction_penalty": 0.0
      }
    ],
    "loop_ranking_defs": {
      "salience": "Immediate urgency/intensity of the loop signal.",
      "importance": "Durable relevance by loop type/time horizon and commitment durability.",
      "confidence": "Trust in loop signal quality derived from explicit confidence or salience proxy."
    },
    "summary_fetch_count": 0,
    "summary_used_count": 0,
    "summary_content_quality": "ok|none_fetched|empty_after_normalization",
    "fallback_used": false,
    "fallback_success": false,
    "daily_analysis_date_used": "YYYY-MM-DD|null",
    "freshness": {
      "has_pending_session_ingest_jobs": false,
      "pending_raw_episode_jobs": 0,
      "pending_post_ingest_hook_jobs": 0,
      "oldest_pending_age_seconds": 0,
      "latest_pending_session_id": "string|null"
    }
  },
  "entity_hints": [
    {
      "entityId": "string|null",
      "name": "string",
      "type": "person|project|company|place|other",
      "role": "string|null",
      "importance": 0.0,
      "salience": 0.0,
      "lastSeenAt": "ISO-8601|null"
    }
  ],
  "entity_profiles": []
}
```

`entity_hints` is the compact ambient grounding surface. `entity_profiles` is a legacy field kept temporarily for compatibility.

**POST /entities/profile**
```bash
curl -s http://localhost:8000/entities/profile -H 'Content-Type: application/json' -d '{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "name": "Ashley",
  "includeOpenLoops": true,
  "factsLimit": 6,
  "loopsLimit": 3
}'
```

Response shape:
```json
{
  "entity": {
    "entityId": "string|null",
    "canonicalName": "string",
    "type": "person|project|company|place|other",
    "aliases": [],
    "summary": "string",
    "role": "string|null",
    "relationship": "string|null",
    "importance": 0.0,
    "salience": 0.0,
    "recency": {"lastSeenAt": "ISO-8601|null", "daysSinceSeen": 0}
  },
  "keyFacts": [{"text": "string", "confidence": 0.0, "validAt": "ISO-8601|null", "invalidAt": "ISO-8601|null"}],
  "openLoops": [{"id": "string", "type": "string", "text": "string", "status": "string|null", "salience": 0}],
  "provenance": {"sources": [], "resolvedBy": "entityId|name", "queryUsed": "string", "generatedAt": "ISO-8601"}
}
```

**POST /memory/query**
```bash
curl -s http://localhost:8000/memory/query -H 'Content-Type: application/json' -d '{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "query": "What is the user stressed about?",
  "limit": 10,
  "referenceTime": "2026-02-06T10:15:00Z"
}'
```

Exact single-query request payload:
```json
{
  "tenantId": "string",
  "userId": "string",
  "query": "string",
  "limit": 10,
  "referenceTime": "ISO-8601 string (optional)",
  "includeContext": false
}
```

Exact response payload shape:
```json
{
  "facts": ["string"],
  "factItems": [{"text": "string", "relevance": 0.0, "source": "graphiti"}],
  "entities": [{"summary": "string", "type": "string|null", "uuid": "string|null"}],
  "metadata": {"query": "string", "responseMode": "recall|context", "facts": 0, "entities": 0, "limit": 10}
}
```

Optional compatibility mode:
- Set `"includeContext": true` to include `openLoops`, `commitments`, `contextAnchors`, `userStatedState`, `currentFocus`, `recallSheet`, and `supplementalContext`.

## Internal debug
- `GET /internal/debug/startbrief/ranking` (requires `x-internal-token`)
  - Returns full summary and loop candidate rankings with score components.
  - Use when diagnosing stale/contradictory startbrief context selection.
- `POST /internal/debug/entities/profile` (requires `x-internal-token`)
  - Returns computed entity profile plus candidate ranking, fact keep/drop reasons, and loop match diagnostics.

**GET /memory/loops**
```bash
curl -s "http://localhost:8000/memory/loops?tenantId=tenant_a&userId=user_1&limit=5&domain=career"
```

Exact response payload shape:
```json
{
  "items": [
    {
      "id": "uuid",
      "type": "commitment|decision|friction|habit|thread",
      "text": "string",
      "status": "active|needs_review",
      "salience": 1,
      "timeHorizon": "today|this_week|ongoing|null",
      "dueDate": "YYYY-MM-DD|null",
      "lastSeenAt": "ISO-8601|null",
      "domain": "general|health|career|relationships|family|finance|home|learning|spirituality|null",
      "importance": 1,
      "urgency": 1,
      "tags": ["string"],
      "personaId": "string|null"
    }
  ],
  "metadata": {"count": 0, "limit": 5, "sort": "priority_desc", "domainFilter": "career", "personaId": null}
}
```

**GET /user/model**
```bash
curl -s "http://localhost:8000/user/model?tenantId=tenant_a&userId=user_1"
```

**PATCH /user/model**
```bash
curl -s -X PATCH "http://localhost:8000/user/model" \
  -H "Content-Type: application/json" \
  -d '{
    "tenantId":"tenant_a",
    "userId":"user_1",
    "source":"manual_update",
    "patch":{
      "north_star":{
        "work":{
          "vision":"Build products that meaningfully help people",
          "goal":"Ship memory reliability improvements",
          "status":"active",
          "vision_confidence":0.9,
          "vision_source":"user_stated",
          "goal_confidence":0.8,
          "goal_source":"user_stated"
        }
      },
      "current_focus":{"text":"Ship memory reliability","confidence":0.8},
      "key_relationships":[{"name":"Ashley","who":"partner","status":"repairing","confidence":0.8}],
      "preferences":{"tone":"direct and warm","avoid":["therapy voice"],"confidence":0.9}
    }
  }'
```

Response includes:
- `model`: merged human-readable JSON
- `completenessScore`: `{relationships, work, north_star, health, spirituality, general}` each `0-100`
- `metadata.staleness`: per-field stale flags from `updated_at` (`21d` default, `current_focus` stale after `10d`)
- `version`, `createdAt`, `updatedAt`, `lastSource`
- `model.north_star` uses per-domain objects:
  - domains: `relationships|work|health|spirituality|general`
  - fields: `vision`, `goal`, `status` (`active|inactive|unknown`) plus confidence/source metadata

Background updater behavior:
- Runs in background (`USER_MODEL_UPDATER_ENABLED=true` by default).
- Writes `north_star.goal` from recent loops/sessions (low confidence when inferred).
- Writes `north_star.vision` only from explicit user-stated signals (high confidence).

**GET /analysis/daily**
```bash
curl -s "http://localhost:8000/analysis/daily?tenantId=tenant_a&userId=user_1"
```

Response includes:
- `analysisDate`, `themes` (emotional/cognitive), `scores` (`curiosity|warmth|usefulness|forward_motion` on 1-5 scale)
- `steeringNote` (single sentence for tomorrow start behavior)
- `metadata.quality_flag`:
  - `insufficient_data` when fewer than 3 turns were available
  - `needs_review` when confidence is low for 2+ consecutive days
- `exists=false` when no daily analysis is available yet

**POST /ingest**
```bash
curl -s http://localhost:8000/ingest -H 'Content-Type: application/json' -d '{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "personaId": "persona_1",
  "sessionId": "session-demo-001",
  "role": "user",
  "text": "I am stressed at the gym. The blue-widget-glitch is still unresolved.",
  "timestamp": "2026-02-06T10:15:05Z"
}'
```

Docs:
- `docs/SOPHIE_ORCHESTRATOR_INTEGRATION_V1.md`
- `AUDIT_MEMORY_V1.md`
- `DECISIONS.md`
- `CHANGELOG.md`
- `docs/MEMORY_ARCHITECTURE_V1.md`
- `docs/MEMORY_RETRIEVAL_CONTRACT_V1.md`
- `docs/MEMORY_QUALITY_SCORECARD.md`
- `docs/SEMANTIC_RERANK_DIAGNOSTICS.md`
- `docs/DERIVED_MODEL_AND_STEERING_SCHEMA.md`

## Tenant Alias Canonicalization
- Runtime canonicalizes known tenant aliases at request ingress (query + JSON body).
- Current alias mapping includes:
  - `sophie-prod` -> `default`
- Read-path compatibility:
  - `/memory/query` and `/user/model` fan out across canonical + alias scope to avoid historical split loss.
- Physical consolidation:
  - Migration `025_tenant_alias_consolidation.sql` consolidates historical rows from `sophie-prod` into `default`.

Run migrations against local docker Postgres:
```bash
cd /opt/synapse
set -a && source .env && set +a
export DATABASE_URL="postgresql://synapse:${POSTGRES_PASSWORD}@127.0.0.1:5432/synapse"
.venv/bin/python - <<'PY'
import asyncio
from src.db import Database
from src.migrate import run_migrations

async def main():
    db = Database()
    try:
        await run_migrations(db)
        print("migrations_applied_ok")
    finally:
        await db.close()

asyncio.run(main())
PY
```

Memory surface audit (cross-check `default` and `sophie-prod` for one user/fact):
```bash
python3 scripts/audit_memory_surfaces.py \
  --base-url http://localhost:8000 \
  --user-id <user_id> \
  --needle "kidney stones" \
  --tenant default --tenant sophie-prod
```

Memory quality evaluator (fixture-driven):
```bash
python3 scripts/evaluate_memory_quality.py \
  --base-url http://localhost:8000 \
  --cases tests/fixtures/memory_quality_cases.sample.json
```

## Key concepts
- **Session buffer**: Postgres keeps rolling summary + last 12 messages (6 user+assistant turns).
- **Outbox**: evicted turns are queued; retries are backoff‑controlled.
- **Session ingest outbox jobs**: `session_raw_episode` (Graphiti raw episode), then `post_ingest_hook` jobs (`session_summary`, `open_loops`).
- **Graphiti**: semantic memory (episodes/facts/entities + SessionSummary nodes). Receives raw session transcripts from outbox drain.
- **Loops**: procedural memory (commitments/decisions/frictions/habits/threads) extracted by `post_ingest_hook` jobs into Postgres.

## Configuration (important)
Environment flags (see `src/config.py`):
- `IDLE_CLOSE_ENABLED` (default false): close idle sessions in background
- `OUTBOX_DRAIN_ENABLED` (default false): drain outbox in background
- `LOOP_STALENESS_JANITOR_ENABLED` (default true): nightly loop staleness pass
- `LOOP_STALENESS_JANITOR_INTERVAL_SECONDS` (default 86400): janitor cadence
- `DAILY_ANALYSIS_ENABLED` (default true): nightly synthesis from daily transcripts
- `DAILY_ANALYSIS_INTERVAL_SECONDS` (default 86400): analysis cadence
- `DAILY_ANALYSIS_TARGET_OFFSET_DAYS` (default 1): analyze yesterday for tomorrow steering
- `GRAPHITI_PER_TURN` (default false): per‑session episodes only
- `GRAPHITI_LLM_MODEL` (optional): override Graphiti LLM model

Recommended for staging:
```
IDLE_CLOSE_ENABLED=true
OUTBOX_DRAIN_ENABLED=true
```

## Gotchas / things to watch
- **Identity defaults are null** until user states name/home/timezone. Don’t assume name exists.
- **Graphiti recall is not automatic**: call `/memory/query` to retrieve facts/entities.
- **/memory/query defaults to clean recall**: include only `facts`, `factItems`, `entities`, `metadata` unless `includeContext=true`.
- **/session/brief is Graphiti‑native**: facts are filtered for quality (no single-token/vague fragments), and narrativeSummary is derived from Graphiti episode summaries (de‑duplicated from facts).
- **/session/startbrief is structured**: use `handover_text` and `ops_context.top_loops_today`; do not expect legacy `bridgeText/items` fields.
- **SessionSummary node fields**: summary content is stored as top-level node props (`summary_text`, `bridge_text`, `session_id`, `summary_quality_tier`, `summary_source`) and also available via Graphiti node attributes.
- **Outbox won’t drain** unless `/internal/drain` is called or `OUTBOX_DRAIN_ENABLED=true`.
- **Session close** is legacy/admin-safe enqueue-only; canonical finalization is `/session/ingest`.
- **Loop extraction** runs in outbox drain `post_ingest_hook` jobs (best‑effort) and does not affect `/session/ingest` latency.
- **Loop staleness janitor** runs in background (daily by default) and marks old loops `stale`/`needs_review`.
- **Graphiti LLM** uses OpenAI by default (via `OPENAI_API_KEY`) unless overridden by `GRAPHITI_LLM_*` settings.
- **Noise filter**: very short messages may be marked `skipped`.
- **Falkor result shapes vary**: use `src/falkor_utils.py` helpers when parsing `driver.execute_query` output.

## Dev / Test
```bash
docker compose exec synapse pytest -q
```

Contract smoke test for Sophie-facing payloads:
```bash
SYNAPSE_BASE_URL=http://localhost:8000 TENANT_ID=default USER_ID=user_1 scripts/contract_startbrief_memory.sh
```

## Backfill session summaries
Create/refresh Graphiti `SessionSummary` nodes from full `session_transcript.messages` with guaranteed fallback summaries.
```bash
docker compose exec synapse python scripts/backfill_session_summaries.py \
  --tenant-id tenant_a \
  --user-id user_1 \
  --limit 200
```
Use `--dry-run` to preview. Use `--force` to replace existing summaries for the same `session_id`.

Internal API alternative:
```bash
curl -s -X POST "http://localhost:8000/internal/debug/backfill/session_summaries?tenantId=tenant_a&userId=user_1&limit=200&dryRun=true" \
  -H "X-Internal-Token: <INTERNAL_TOKEN>"
```

## Backfill bridge_text
Create/refresh `bridge_text` for SessionSummary nodes.
```bash
docker compose exec synapse python scripts/backfill_bridge_text.py \
  --tenant-id tenant_a \
  --user-id user_1
```
Use `--dry-run` to preview and `--force` to overwrite existing bridge_text.

## Force close open sessions
Close any open sessions still in `session_buffer` (legacy).
```bash
docker compose exec synapse python scripts/force_close_sessions.py \
  --limit 100 \
  --tenant-id tenant_a \
  --user-id user_1
```

## Internal debug endpoints
Require header `X-Internal-Token` = `INTERNAL_TOKEN`.
- `/internal/debug/session?tenantId&userId&sessionId`
- `/internal/debug/user?tenantId&userId`
- `/internal/debug/outbox?tenantId&limit=50` (includes `job_type`, `dedupe_key`, retry fields, and payload summary)
- `/internal/debug/session_ingest_status?tenant_id&user_id&session_id`
- `/internal/debug/loops?tenantId&userId&format=csv`
- `/internal/debug/session?tenantId&userId&sessionId`
- `/internal/debug/nudges?tenantId&userId`
- `/internal/debug/graphiti/episodes?tenantId&userId&limit=5`
- `/internal/debug/graphiti/session_summaries?tenantId&userId&limit=5`
- `/internal/debug/graphiti/session_summaries_clean?tenantId&userId&limit=5`
- `/internal/debug/graphiti/session_summaries_view?tenantId&userId&limit=10`
- `/internal/debug/graphiti/session_summaries_lookup?sessionId&nameContains&database&limit=20`
- `POST /internal/debug/backfill/session_summaries?tenantId&userId&limit&dryRun`
- `POST /internal/debug/graphiti/query`
- `POST /internal/debug/close_session?tenantId&userId&sessionId`
- `POST /internal/debug/close_user_sessions?tenantId&userId&limit=20`
- `POST /internal/debug/emit_raw_episode?tenantId&userId&sessionId`
- `POST /internal/debug/emit_raw_user_sessions?tenantId&userId&limit=20`
- `POST /admin/purgeUser` (requires `X-Admin-Key`)

## License
Private/internal.
