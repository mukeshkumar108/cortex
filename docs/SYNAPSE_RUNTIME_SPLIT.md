# Synapse Runtime Split

This document describes the current safe runtime split between the HTTP API and background worker processes.

## Roles

### API process

- Compose service: `synapse`
- Container: `synapse-api`
- Serves FastAPI routes on port `8000`
- Runs shared startup initialization:
  - DB pool
  - migrations check
  - Graphiti init
  - session manager init
  - loop manager init
- Does **not** start background loops when:
  - `SYNAPSE_BACKGROUND_LOOPS_ENABLED=false`
- Even if background loops are enabled by mistake, only the safe loops are allowed in `api`:
  - `idle_close`
  - `outbox_drain`

### Worker process

- Compose service: `synapse-worker`
- Container: `synapse-worker`
- Does not publish HTTP ports
- Reuses the same app/module startup path
- Starts background loops when:
  - `SYNAPSE_BACKGROUND_LOOPS_ENABLED=true`
- Refuses startup unless `SYNAPSE_RUNTIME_ROLE=worker`

## Environment flags

- `SYNAPSE_RUNTIME_ROLE=api|worker`
- `SYNAPSE_BACKGROUND_LOOPS_ENABLED=true|false`

The per-loop feature flags still apply. The top-level background loop gate only decides whether this process starts any loop tasks at all.

Current dogfood-safe defaults:

- API:
  - `SYNAPSE_RUNTIME_ROLE=api`
  - `SYNAPSE_BACKGROUND_LOOPS_ENABLED=false`
- Worker:
  - `SYNAPSE_RUNTIME_ROLE=worker`
  - `SYNAPSE_BACKGROUND_LOOPS_ENABLED=true`
  - `OUTBOX_DRAIN_ENABLED=true`
  - `USER_MODEL_UPDATER_ENABLED=true`
  - `USER_MODEL_ENRICHMENT_ENABLED=false`
  - `DAILY_ANALYSIS_ENABLED=false`
  - `DERIVED_PIPELINE_AUDIT_ENABLED=false`
  - `PROACTIVE_SHADOW_CANDIDATES_ENABLED=false`
  - `V2_INVARIANT_CHECKER_ENABLED=false`
  - `LLM_BACKGROUND_LOOPS_STARTUP_DELAY_SECONDS=300`

## Current loop inventory

Started from `src.main.lifespan(...)` through `_start_background_loop_tasks(...)`:

- `idle_close`
- `outbox_drain`
- `user_model_updater`
- `user_model_enrichment`
- `loop_staleness_janitor`
- `derived_silence_detection`
- `derived_memory_audit`
- `proactive_shadow_candidates`
- `daily_analysis`
- `daily_habit_dedupe`
- `v2_invariant_checker`
- `v2_rollout_evaluator`

## Start / restart

API only:

```bash
docker compose up -d synapse
docker compose logs -f synapse
```

Worker only:

```bash
docker compose up -d synapse-worker
docker compose logs -f synapse-worker
```

Both:

```bash
docker compose up -d synapse synapse-worker
```

## Verify

API health:

```bash
curl -fsS http://localhost:8000/health
```

API loop visibility:

```bash
curl -sS http://localhost:8000/internal/debug/background_loops \
  -H "X-Internal-Token: $SYNAPSE_INTERNAL_TOKEN"
```

Expected API result after split:

- `runtime_role: "api"`
- `background_loops_enabled: false`
- `started_loops: []`

Worker logs should show:

- startup role
- whether background loops are enabled
- the final list of started loops
- any skipped loops when the runtime role is `api`
- per-loop run metrics for LLM-heavy loops when enabled

## Deploy safely

1. Confirm current status:

```bash
docker compose ps
docker compose logs --tail=100 synapse
```

2. Create or restart worker:

```bash
docker compose up -d synapse-worker
```

3. Recreate API to pick up `SYNAPSE_BACKGROUND_LOOPS_ENABLED=false`:

```bash
docker compose up -d synapse
```

4. Verify:

```bash
docker compose ps
docker compose logs --tail=100 synapse-worker
curl -fsS http://localhost:8000/health
```

5. Run smoke:

```bash
SYNAPSE_BASE_URL=http://localhost:8000 \
SYNAPSE_INTERNAL_TOKEN=... \
SYNAPSE_TEST_TENANT_ID=... \
SYNAPSE_TEST_USER_ID=... \
SYNAPSE_TEST_SESSION_ID=... \
scripts/smoke_synapse_contract.sh
```

## Rollback

Fast rollback path:

1. Stop worker:

```bash
docker compose stop synapse-worker
```

2. Re-enable loops in the API service by changing:

- `SYNAPSE_BACKGROUND_LOOPS_ENABLED: "false"` -> `"true"`
- optionally set `SYNAPSE_RUNTIME_ROLE: api`

3. Recreate API:

```bash
docker compose up -d synapse
```

## Remaining risks

- Shared startup still initializes DB, Graphiti, and managers in both API and worker processes.
- Loop logic itself is unchanged; this split isolates runtime ownership but does not redesign supervision.
- `run_migrations(db)` still executes during startup in both processes because schema behavior was intentionally left unchanged in this sprint.
