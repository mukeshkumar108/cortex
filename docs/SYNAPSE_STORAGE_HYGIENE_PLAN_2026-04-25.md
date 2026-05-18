# Synapse Storage Hygiene Plan (2026-04-25)

## Why this plan exists

Raw transcript text is generally cheap to store.  
The cost/problem here is **duplication across planes**:

- `session_buffer.messages` (JSON transcript window)
- `session_transcript.messages` (JSON full transcript)
- `turns_v2.content` (row-per-turn duplicate)
- `graphiti_outbox.text/payload` (queue copy of content)
- `episodic_memory_embeddings.unit_text` (windowed transcript copies + vectors)

So growth is driven less by "text exists" and more by "same text exists multiple times + vectors + audit payloads".

## Storage policy (recommended)

### 1) Canonical transcript authority

Keep **one** long-term canonical transcript store:

- Canonical: `session_transcript` (full session JSON by `tenant_id, session_id`)

Treat these as non-canonical/operational:

- `session_buffer`: active-session working set only
- `graphiti_outbox`: delivery queue only
- `turns_v2`: query/index substrate (retained with policy, not forever by default)

### 2) Retention defaults

- `graphiti_outbox`: keep sent rows 14 days, failed rows 30 days
- `startbrief_history`: 30 days
- `pipeline_runs`: 30 days
- `retrieval_shadow_diffs`: 14 days
- `extract_results`: 30 days
- `claims_quarantine`: 90 days
- `derived_quarantine`: 90 days
- `session_buffer`: closed sessions older than 7 days should have `messages=[]` (keep metadata row)

For transcript duplication:

- Keep canonical `session_transcript` long-term
- Keep `turns_v2` only for recent analytics window (for example 90-180 days), unless there is a hard product requirement to keep indefinitely

For embeddings:

- Keep `episodic_memory_embeddings` for recent horizon only (for example 90 days) or top-N recent sessions per user

## Safe rollout sequence

1. Run dry-run counts for each retention rule.
2. Enable only queue/audit table cleanup first (`graphiti_outbox`, `startbrief_history`, `pipeline_runs`, `retrieval_shadow_diffs`).
3. Confirm no regression in `/session/startbrief`, `/memory/query`, `/session/handover`.
4. Enable `session_buffer` closed-message compaction.
5. Enable `turns_v2` and embedding retention windows.
6. Review weekly storage slope and tune intervals.

## Operational guardrails

- Always run retention in transaction batches (timeboxed) to avoid long locks.
- Log deleted row counts per table per run.
- Keep dry-run mode available permanently.
- Do not delete rows younger than retention horizon.

## Current high-growth tables observed on this host

- `derived_assertions`
- `episodic_memory_embeddings`
- `graphiti_outbox`
- `pipeline_runs`
- `session_classifications`
- `startbrief_history`
- `session_transcript`

See `scripts/storage_hygiene.sql` for concrete SQL.

