# Agent Recovery Playbook

Purpose: fast recovery steps when memory behavior looks wrong.

## Symptom: Hallucinated continuity or wrong person/project

1. Pull fresh handover packet.
2. Run `/memory/search` on disputed topic.
3. Respond with explicit uncertainty and corrected fact.
4. If repeated, trigger rerun:
- pass1 triage (new sessions)
- entity pipeline
- threads pipeline
- scoring update
- identity synthesis
- living context synthesis

## Symptom: Stale unresolved threads dominate responses

1. Run `scripts/run_scoring_update.py`.
2. Verify stale non-persistent threads were auto-snoozed.
3. Confirm persistent goals remain open and salience floor is maintained.

## Symptom: Contradictions disappeared

1. Re-run `scripts/run_living_context.py`.
2. Check `active_contradictions` includes both earlier and recent views.
3. If still missing, inspect recent `session_classifications` evidence.

## Symptom: Weak episodic recall

1. Check embedding coverage:
- count memory-worthy sessions with null `memory_delta_embedding`.
2. Run `scripts/run_episodic_embeddings.py` backfill.
3. Re-test `/memory/search`.

## Symptom: Agent not following user-required behavior

1. Inspect `living_context.sophie_directives`.
2. Confirm directives are grounded in explicit user statements.
3. Re-run living context synthesis if directives are stale/missing.

## SQL Spot Checks

```sql
-- Embedding coverage
SELECT
  COUNT(*) FILTER (WHERE is_memory_worthy) AS memory_worthy,
  COUNT(*) FILTER (WHERE is_memory_worthy AND memory_delta_embedding IS NOT NULL) AS embedded
FROM session_classifications
WHERE user_id = '<USER_ID>';

-- Open thread lifecycle state
SELECT title, thread_type, status, salience_score, follow_up_after
FROM open_threads
WHERE user_id = '<USER_ID>'
ORDER BY salience_score DESC;

-- Living context freshness
SELECT last_synthesized_at, source_session_count, sessions_since_last
FROM living_context
WHERE user_id = '<USER_ID>';
```
