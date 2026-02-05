# Cortex (Synapse Memory API)

A FastAPI memory service with a sliding-window session buffer, LLM-first procedural memory (loops), and optional Graphiti semantic memory. Designed to be reliable, tenant‑isolated, and cheap to run.

## What it does (short)
- **/ingest**: writes turns to Postgres (rolling summary + last 6 turns). Never blocks on LLM/Graphiti.
- **/brief**: returns memory context (identity, rolling summary, last turns, loops). Graphiti enrichment is optional.
- **Outbox**: reliable delivery of evicted turns + session summaries to Graphiti.

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
1) `/brief` before each assistant response
2) LLM responds
3) `/ingest` user turn
4) `/ingest` assistant turn

Docs:
- `docs/SOPHIE_ORCHESTRATOR_INTEGRATION_V1.md`
- `AUDIT_MEMORY_V1.md`
- `DECISIONS.md`

## Key concepts
- **Session buffer**: Postgres keeps rolling summary + last 12 messages (6 user+assistant turns).
- **Outbox**: evicted turns are queued for Graphiti; retries are backoff‑controlled.
- **Loops**: procedural memory extracted by LLM (commitments, habits, frictions).
- **Graphiti**: derived semantic memory (episodes/facts/entities), best‑effort. Receives raw session transcripts on close.

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
- **Graphiti recall is not automatic**: call `/brief` with `query` to retrieve facts/entities.
- **Outbox won’t drain** unless `/internal/drain` is called or `OUTBOX_DRAIN_ENABLED=true`.
- **Session close** happens via idle close loop (config) or next ingest. Enable idle close for clean session summaries.
- **Graphiti LLM** uses OpenAI by default (via `OPENAI_API_KEY`) unless overridden by `GRAPHITI_LLM_*` settings.
- **Noise filter**: very short messages may be marked `skipped`.

## Dev / Test
```bash
docker compose exec synapse pytest -q
```

## Internal debug endpoints
Require header `X-Internal-Token` = `INTERNAL_TOKEN`.
- `/internal/debug/session?tenantId&userId&sessionId`
- `/internal/debug/user?tenantId&userId`
- `/internal/debug/outbox?tenantId&limit=50`
- `/internal/debug/loops?tenantId&userId`
- `/internal/debug/nudges?tenantId&userId`
- `POST /internal/debug/close_session?tenantId&userId&sessionId`
- `POST /internal/debug/close_user_sessions?tenantId&userId&limit=20`

## License
Private/internal.
