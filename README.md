# Cortex (Synapse Memory API)

A FastAPI memory service with a sliding‑window session buffer and Graphiti‑native semantic memory. Designed to be reliable, tenant‑isolated, and cheap to run.

## What it does (short)
- **/ingest**: writes turns to Postgres (rolling summary + last 12 messages). Never blocks.
- **/brief**: minimal session seed (time + working memory + rolling summary).
- **/session/startbrief**: minimal start bridge (bridgeText + up to 5 durable items). Optional `sessionId`, `personaId`, `timezone`.
- **/session/brief**: Graphiti‑native start brief (structured briefContext + facts/openLoops/commitments + currentFocus).
- **/memory/query**: on‑demand Graphiti memory query (facts/openLoops/commitments + recallSheet).
- **/session/close**: flushes raw transcript to Graphiti, stores a SessionSummary node, and extracts procedural loops (best‑effort).
- **/session/ingest**: send a full session transcript in one call.
- **Outbox**: reliable delivery of evicted turns; raw transcript is sent to Graphiti on session close.

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
1) `/brief` once at session start (optional)
2) `/session/brief` if you want a structured start‑brief (Graphiti)
3) LLM responds
4) `/ingest` user turn
5) `/ingest` assistant turn

## Quick API examples
**POST /brief**
```bash
curl -s http://localhost:8000/brief -H 'Content-Type: application/json' -d '{
  "tenantId": "tenant_a",
  "userId": "user_1",
  "personaId": "persona_1",
  "sessionId": "session-demo-001",
  "now": "2026-02-06T10:15:00Z"
}'
```

**GET /session/brief**
```bash
curl -s "http://localhost:8000/session/brief?tenantId=tenant_a&userId=user_1&now=2026-02-06T10:15:00Z"
```

**GET /session/startbrief**
```bash
curl -s "http://localhost:8000/session/startbrief?tenantId=tenant_a&userId=user_1&now=2026-02-06T10:15:00Z&timezone=America/Los_Angeles"
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

## Key concepts
- **Session buffer**: Postgres keeps rolling summary + last 12 messages (6 user+assistant turns).
- **Outbox**: evicted turns are queued; retries are backoff‑controlled.
- **Graphiti**: semantic memory (episodes/facts/entities + SessionSummary nodes). Receives raw session transcripts on close.
- **Loops**: procedural memory (commitments/decisions/frictions/habits/threads) extracted on session close into Postgres.

## Configuration (important)
Environment flags (see `src/config.py`):
- `IDLE_CLOSE_ENABLED` (default false): close idle sessions in background
- `OUTBOX_DRAIN_ENABLED` (default false): drain outbox in background
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
- **/session/brief is Graphiti‑native**: facts are filtered for quality (no single-token/vague fragments), and narrativeSummary is derived from Graphiti episode summaries (de‑duplicated from facts).
- **/session/startbrief is minimal**: short bridgeText (<=280 chars) + up to 5 durable items (loops first, then unresolved tensions).
- **Outbox won’t drain** unless `/internal/drain` is called or `OUTBOX_DRAIN_ENABLED=true`.
- **Session close** happens via idle close loop (config) or next ingest. Enable idle close for clean session summaries.
- **Loop extraction** runs on session close (best‑effort) and does not affect /ingest latency.
- **Graphiti LLM** uses OpenAI by default (via `OPENAI_API_KEY`) unless overridden by `GRAPHITI_LLM_*` settings.
- **Noise filter**: very short messages may be marked `skipped`.
- **Falkor result shapes vary**: use `src/falkor_utils.py` helpers when parsing `driver.execute_query` output.

## Dev / Test
```bash
docker compose exec synapse pytest -q
```

## Backfill session summaries
Create Graphiti `SessionSummary` nodes for existing sessions.
```bash
docker compose exec synapse python scripts/backfill_session_summaries.py \
  --tenant-id tenant_a \
  --user-id user_1 \
  --limit 200
```
Use `--dry-run` to preview and `--force` to overwrite existing summaries.

## Internal debug endpoints
Require header `X-Internal-Token` = `INTERNAL_TOKEN`.
- `/internal/debug/session?tenantId&userId&sessionId`
- `/internal/debug/user?tenantId&userId`
- `/internal/debug/outbox?tenantId&limit=50`
- `/internal/debug/loops?tenantId&userId&format=csv`
- `/internal/debug/session?tenantId&userId&sessionId`
- `/internal/debug/nudges?tenantId&userId`
- `/internal/debug/graphiti/episodes?tenantId&userId&limit=5`
- `/internal/debug/graphiti/session_summaries?tenantId&userId&limit=5`
- `POST /internal/debug/graphiti/query`
- `POST /internal/debug/close_session?tenantId&userId&sessionId`
- `POST /internal/debug/close_user_sessions?tenantId&userId&limit=20`
- `POST /internal/debug/emit_raw_episode?tenantId&userId&sessionId`
- `POST /internal/debug/emit_raw_user_sessions?tenantId&userId&limit=20`
- `POST /admin/purgeUser` (requires `X-Admin-Key`)

## License
Private/internal.
