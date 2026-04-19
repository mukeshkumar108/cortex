# Memory Operations Runbook

Purpose: operator-facing runbook for running, validating, and recovering the memory pipelines.

## Prerequisites

- API container: `synapse-api` (`/app`)
- Postgres container: `synapse-postgres`
- Env keys configured for OpenRouter/OpenAI
- Migrations applied through SQL or startup runner

## Standard Pipeline Sequence

For a user:

```bash
cd /app
PYTHONPATH=/app python scripts/run_pass1_memory_triage_batch.py --user-id <USER_ID> --model google/gemma-4-26b-a4b-it
PYTHONPATH=/app python scripts/run_entity_pipeline.py --user-id <USER_ID> --model google/gemma-4-26b-a4b-it
PYTHONPATH=/app python scripts/run_threads_pipeline.py --user-id <USER_ID> --model google/gemma-4-26b-a4b-it
PYTHONPATH=/app python scripts/run_scoring_update.py --user-id <USER_ID>
PYTHONPATH=/app python scripts/run_identity_synthesis.py --user-id <USER_ID> --model google/gemma-4-26b-a4b-it
PYTHONPATH=/app python scripts/run_living_context.py --user-id <USER_ID> --model google/gemma-4-26b-a4b-it
```

Backfill episodic embeddings:

```bash
PYTHONPATH=/app python scripts/run_episodic_embeddings.py --user-id <USER_ID> --limit 500
```

## Health Checks

1. Handover response
```bash
curl -s "http://localhost:8000/session/handover?user_id=<USER_ID>"
```

2. Episodic semantic search
```bash
curl -s -X POST "http://localhost:8000/memory/search" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"<USER_ID>","query":"Jasmine"}'
```

3. Pipeline checkpoints
```sql
SELECT * FROM pipeline_checkpoints WHERE user_id = '<USER_ID>' ORDER BY pipeline_name;
```

## Expected Outcomes

- Pass 1 inserts new `session_classifications` rows only for unclassified sessions.
- Entity/threads re-runs should converge (no runaway merges/fixes).
- Scoring should update all active rows and set thread salience coherently.
- Identity/living context should upsert one row per user.
- `/session/handover` should return coherent living + identity + threads + people + projects.

## Error Surfaces

- Pass scripts write JSONL error files under `tmp/`.
- OpenRouter failures: retry with lower batch or longer timeout.
- Invalid JSON model output: logged per session/action, pipeline continues.
- Missing container path sync: copy changed files into `synapse-api` and restart container.

## Recovery Playbook

1. Soft failure in one pass
- Fix script/prompt issue.
- Re-run that pass only.

2. Data quality drift
- Run audit-enabled pipelines (entity/threads) + scoring.
- Inspect high-salience open threads and top entities.

3. Bad synthesis output (identity/living)
- Re-run synthesis after prompt/schema adjustment.
- Treat prior synthesis as replaceable derived state.

4. Embedding gaps
- Run `run_episodic_embeddings.py`.
- Verify `memory_delta_embedding` coverage with SQL count.

## Rollback Guidance

- Schema migrations are additive and idempotent.
- Derived rows (`identity_profile`, `living_context`, open thread status changes) can be safely regenerated.
- For severe corruption, snapshot + targeted delete of derived rows then re-run from pass outputs.

