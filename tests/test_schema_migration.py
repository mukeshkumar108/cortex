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
