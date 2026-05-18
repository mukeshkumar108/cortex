-- Synapse storage hygiene SQL playbook
-- Date: 2026-04-25
--
-- Usage example:
--   docker exec -i synapse-postgres psql -U synapse -d synapse -f /dev/stdin < scripts/storage_hygiene.sql
--
-- Important:
-- - Run DRY-RUN section first.
-- - Convert each block to scheduled job only after validating impact.

-- ============================================================================
-- 0) DB footprint snapshot
-- ============================================================================

SELECT
  current_database() AS db_name,
  pg_size_pretty(pg_database_size(current_database())) AS db_size_pretty,
  pg_database_size(current_database()) AS db_size_bytes;

WITH sizes AS (
  SELECT
    c.relname AS table_name,
    pg_total_relation_size(c.oid) AS total_bytes,
    COALESCE(s.n_live_tup, 0) AS est_rows
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
  WHERE c.relkind = 'r'
    AND n.nspname = 'public'
)
SELECT
  table_name,
  pg_size_pretty(total_bytes) AS total_size,
  total_bytes,
  est_rows
FROM sizes
ORDER BY total_bytes DESC
LIMIT 30;

-- ============================================================================
-- 1) DRY-RUN: rows eligible by policy (NO DELETE/UPDATE)
-- ============================================================================

SELECT 'graphiti_outbox_sent_gt_14d' AS policy, count(*) AS rows
FROM graphiti_outbox
WHERE status = 'sent'
  AND COALESCE(sent_at, created_at) < NOW() - INTERVAL '14 days';

SELECT 'graphiti_outbox_failed_gt_30d' AS policy, count(*) AS rows
FROM graphiti_outbox
WHERE status = 'failed'
  AND COALESCE(next_attempt_at, created_at) < NOW() - INTERVAL '30 days';

SELECT 'startbrief_history_gt_30d' AS policy, count(*) AS rows
FROM startbrief_history
WHERE requested_at < NOW() - INTERVAL '30 days';

SELECT 'pipeline_runs_gt_30d' AS policy, count(*) AS rows
FROM pipeline_runs
WHERE created_at < NOW() - INTERVAL '30 days';

SELECT 'retrieval_shadow_diffs_gt_14d' AS policy, count(*) AS rows
FROM retrieval_shadow_diffs
WHERE created_at < NOW() - INTERVAL '14 days';

SELECT 'extract_results_gt_30d' AS policy, count(*) AS rows
FROM extract_results
WHERE created_at < NOW() - INTERVAL '30 days';

SELECT 'claims_quarantine_gt_90d' AS policy, count(*) AS rows
FROM claims_quarantine
WHERE created_at < NOW() - INTERVAL '90 days';

SELECT 'derived_quarantine_gt_90d' AS policy, count(*) AS rows
FROM derived_quarantine
WHERE created_at < NOW() - INTERVAL '90 days';

SELECT 'session_buffer_closed_message_compact_gt_7d' AS policy, count(*) AS rows
FROM session_buffer
WHERE closed_at IS NOT NULL
  AND closed_at < NOW() - INTERVAL '7 days'
  AND jsonb_array_length(COALESCE(messages, '[]'::jsonb)) > 0;

SELECT 'turns_v2_gt_120d' AS policy, count(*) AS rows
FROM turns_v2
WHERE occurred_at < NOW() - INTERVAL '120 days';

SELECT 'episodic_memory_embeddings_gt_90d' AS policy, count(*) AS rows
FROM episodic_memory_embeddings
WHERE reference_time < NOW() - INTERVAL '90 days';

-- ============================================================================
-- 2) APPLY: queue + audit retention
--    (run each block intentionally; keep transaction size bounded)
-- ============================================================================

-- 2.1 graphiti_outbox sent rows > 14d
-- DELETE FROM graphiti_outbox
-- WHERE status = 'sent'
--   AND COALESCE(sent_at, created_at) < NOW() - INTERVAL '14 days';

-- 2.2 graphiti_outbox failed rows > 30d
-- DELETE FROM graphiti_outbox
-- WHERE status = 'failed'
--   AND COALESCE(next_attempt_at, created_at) < NOW() - INTERVAL '30 days';

-- 2.3 startbrief history > 30d
-- DELETE FROM startbrief_history
-- WHERE requested_at < NOW() - INTERVAL '30 days';

-- 2.4 pipeline run traces > 30d
-- DELETE FROM pipeline_runs
-- WHERE created_at < NOW() - INTERVAL '30 days';

-- 2.5 retrieval shadow traces > 14d
-- DELETE FROM retrieval_shadow_diffs
-- WHERE created_at < NOW() - INTERVAL '14 days';

-- ============================================================================
-- 3) APPLY: extraction/quarantine retention
-- ============================================================================

-- 3.1 extract_results > 30d
-- DELETE FROM extract_results
-- WHERE created_at < NOW() - INTERVAL '30 days';

-- 3.2 claims_quarantine > 90d
-- DELETE FROM claims_quarantine
-- WHERE created_at < NOW() - INTERVAL '90 days';

-- 3.3 derived_quarantine > 90d
-- DELETE FROM derived_quarantine
-- WHERE created_at < NOW() - INTERVAL '90 days';

-- ============================================================================
-- 4) APPLY: transcript duplication controls
-- ============================================================================

-- 4.1 compact closed session_buffer message payloads
--     keep row + session_state; remove stale message arrays
-- UPDATE session_buffer
-- SET messages = '[]'::jsonb,
--     updated_at = NOW()
-- WHERE closed_at IS NOT NULL
--   AND closed_at < NOW() - INTERVAL '7 days'
--   AND jsonb_array_length(COALESCE(messages, '[]'::jsonb)) > 0;

-- 4.2 turns_v2 retention window (only if session_transcript is canonical authority)
-- DELETE FROM turns_v2
-- WHERE occurred_at < NOW() - INTERVAL '120 days';

-- ============================================================================
-- 5) APPLY: embedding retention
-- ============================================================================

-- 5.1 time-window retention for episodic embeddings
-- DELETE FROM episodic_memory_embeddings
-- WHERE reference_time < NOW() - INTERVAL '90 days';

-- ============================================================================
-- 6) Post-cleanup vacuum guidance
-- ============================================================================

-- After large cleanup runs, schedule maintenance:
-- VACUUM (ANALYZE) graphiti_outbox;
-- VACUUM (ANALYZE) startbrief_history;
-- VACUUM (ANALYZE) pipeline_runs;
-- VACUUM (ANALYZE) extract_results;
-- VACUUM (ANALYZE) turns_v2;
-- VACUUM (ANALYZE) episodic_memory_embeddings;

