import os
import asyncpg
import pytest

from src.main import app


def _db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    password = os.getenv("POSTGRES_PASSWORD", "password")
    return f"postgresql://synapse:{password}@postgres:5432/synapse"


@pytest.mark.asyncio
async def test_schema_migration():
    async with app.router.lifespan_context(app):
        pass

    conn = await asyncpg.connect(_db_url())
    try:
        identity_cache = await conn.fetchval("SELECT to_regclass('public.identity_cache')")
        assert identity_cache == "identity_cache"

        outbox = await conn.fetchval("SELECT to_regclass('public.graphiti_outbox')")
        assert outbox == "graphiti_outbox"

        cols = await conn.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'session_buffer'
            """
        )
        col_names = {row["column_name"] for row in cols}
        assert "rolling_summary" in col_names
        assert "closed_at" in col_names

        outbox_cols = await conn.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'graphiti_outbox'
            """
        )
        outbox_col_names = {row["column_name"] for row in outbox_cols}
        assert "folded_at" in outbox_col_names
        assert "next_attempt_at" in outbox_col_names
        assert "job_type" in outbox_col_names
        assert "payload" in outbox_col_names
        assert "dedupe_key" in outbox_col_names

        constraint = await conn.fetchval(
            """
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'session_buffer'::regclass
              AND conname = 'session_buffer_messages_len_check'
            """
        )
        assert constraint == "session_buffer_messages_len_check"

        outbox_constraint = await conn.fetchval(
            """
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'graphiti_outbox'::regclass
              AND conname = 'graphiti_outbox_pending_attempts_next_attempt'
            """
        )
        assert outbox_constraint == "graphiti_outbox_pending_attempts_next_attempt"

        dedupe_constraint = await conn.fetchval(
            """
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'graphiti_outbox'::regclass
              AND conname = 'graphiti_outbox_dedupe_key_unique'
            """
        )
        assert dedupe_constraint == "graphiti_outbox_dedupe_key_unique"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_session_pk_composite():
    async with app.router.lifespan_context(app):
        pass

    conn = await asyncpg.connect(_db_url())
    try:
        rows = await conn.fetch(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_name = 'session_buffer'
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            """
        )
        pk_cols = [row["column_name"] for row in rows]
        assert pk_cols == ["tenant_id", "session_id"]
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_t2_v2_additive_schema_objects_exist():
    async with app.router.lifespan_context(app):
        pass

    conn = await asyncpg.connect(_db_url())
    try:
        required_tables = [
            "sessions_v2",
            "turns_v2",
            "entities",
            "entity_aliases",
            "claims",
            "claim_evidence",
            "canonical_mutations",
            "predicate_policy",
            "extract_results",
            "projection_snapshots",
            "projection_latest",
            "claims_quarantine",
            "v2_pipeline_checkpoints",
            "predicate_policy_versions",
            "turn_ingest_idempotency",
        ]
        for table in required_tables:
            exists = await conn.fetchval("SELECT to_regclass($1)", f"public.{table}")
            assert exists == table
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_t2_v2_constraints_and_indexes():
    async with app.router.lifespan_context(app):
        pass

    conn = await asyncpg.connect(_db_url())
    try:
        claims_event_unique = await conn.fetchval(
            """
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'claims'::regclass
              AND conname = 'claims_event_key_unique'
            """
        )
        assert claims_event_unique == "claims_event_key_unique"

        claim_evidence_fk = await conn.fetchval(
            """
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'claim_evidence'::regclass
              AND conname = 'claim_evidence_claim_fk'
            """
        )
        assert claim_evidence_fk == "claim_evidence_claim_fk"

        claims_extract_fk = await conn.fetchval(
            """
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'claims'::regclass
              AND conname = 'claims_extract_result_fk'
            """
        )
        assert claims_extract_fk == "claims_extract_result_fk"

        projection_latest_fk = await conn.fetchval(
            """
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'projection_latest'::regclass
              AND conname = 'projection_latest_mutation_fk'
            """
        )
        assert projection_latest_fk == "projection_latest_mutation_fk"

        turns_unique = await conn.fetchval(
            """
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'turns_v2'::regclass
              AND conname = 'turns_v2_session_turn_unique'
            """
        )
        assert turns_unique == "turns_v2_session_turn_unique"

        turns_session_user_fk = await conn.fetchval(
            """
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'turns_v2'::regclass
              AND conname = 'turns_v2_session_user_fk'
            """
        )
        assert turns_session_user_fk == "turns_v2_session_user_fk"

        entities_merge_fk = await conn.fetchval(
            """
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'entities'::regclass
              AND conname = 'entities_merged_into_fk'
            """
        )
        assert entities_merge_fk == "entities_merged_into_fk"

        policy_version_fk = await conn.fetchval(
            """
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'claims'::regclass
              AND conname = 'claims_policy_version_fk'
            """
        )
        assert policy_version_fk == "claims_policy_version_fk"

        idempotency_pk = await conn.fetchval(
            """
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'turn_ingest_idempotency'::regclass
              AND contype = 'p'
            """
        )
        assert idempotency_pk == "turn_ingest_idempotency_pkey"

        factual_idx = await conn.fetchval(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'claims'
              AND indexname = 'idx_claims_factual_lookup'
            """
        )
        assert factual_idx == "idx_claims_factual_lookup"

        episodic_idx = await conn.fetchval(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'turns_v2'
              AND indexname = 'idx_turns_v2_tenant_user_occurred'
            """
        )
        assert episodic_idx == "idx_turns_v2_tenant_user_occurred"

        projection_idx = await conn.fetchval(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'projection_latest'
              AND indexname = 'idx_projection_latest_lookup'
            """
        )
        assert projection_idx == "idx_projection_latest_lookup"

        idempotency_idx = await conn.fetchval(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
              AND tablename = 'turn_ingest_idempotency'
              AND indexname = 'idx_turn_ingest_idempotency_lookup'
            """
        )
        assert idempotency_idx == "idx_turn_ingest_idempotency_lookup"
    finally:
        await conn.close()
