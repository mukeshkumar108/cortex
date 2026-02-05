import asyncio
from datetime import datetime
from uuid import uuid4

import asyncpg
import pytest

from src.main import app, graphiti_client
from src.briefing import build_briefing
from src import session as session_module


def _db_url() -> str:
    import os
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    password = os.getenv("POSTGRES_PASSWORD", "password")
    return f"postgresql://synapse:{password}@postgres:5432/synapse"


@pytest.mark.asyncio
async def test_brief_is_minimal_without_query():
    async with app.router.lifespan_context(app):
        result = await build_briefing(
            tenant_id="tenant",
            user_id="user",
            persona_id="persona",
            session_id=None,
            query="Ashley",
            now=datetime.utcnow(),
            graphiti_client=graphiti_client
        )
        assert result.semanticContext == []
        assert result.entities == []
        assert result.activeLoops == []


@pytest.mark.asyncio
async def test_memory_query_uses_graphiti():
    class _FakeGraph:
        async def search_facts(self, **_kwargs):
            return [{"text": "User likes birds", "relevance": 0.9, "source": "graphiti"}]

        async def search_nodes(self, **_kwargs):
            return [{"summary": "Ashley", "type": "person", "uuid": "u1"}]

    fake = _FakeGraph()
    graphiti_client.search_facts = fake.search_facts
    graphiti_client.search_nodes = fake.search_nodes

    from src.main import memory_query
    from src.models import MemoryQueryRequest

    req = MemoryQueryRequest(
        tenantId="t",
        userId="u",
        query="Ashley",
        limit=5,
        referenceTime=None
    )
    resp = await memory_query(req)
    assert len(resp.facts) == 1
    assert len(resp.entities) == 1


@pytest.mark.asyncio
async def test_session_close_sends_raw_transcript_episode():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"

    sent = {}

    async def _fake_add_episode(**kwargs):
        sent.update(kwargs)
        return {"ok": True}

    fake_graphiti = type("G", (), {"add_episode": _fake_add_episode})

    async with app.router.lifespan_context(app):
        await session_module.add_turn(
            tenant_id=tenant,
            session_id=session_id,
            user_id=user,
            role="user",
            text="My name is Mukesh",
            timestamp=datetime.utcnow().isoformat() + "Z"
        )
        await session_module.add_turn(
            tenant_id=tenant,
            session_id=session_id,
            user_id=user,
            role="assistant",
            text="Nice to meet you",
            timestamp=datetime.utcnow().isoformat() + "Z"
        )
        await session_module.close_session(
            tenant_id=tenant,
            session_id=session_id,
            user_id=user,
            graphiti_client=fake_graphiti
        )

    assert "text" in sent
    assert "My name is Mukesh" in sent["text"]
    assert sent.get("role") is None

    conn = await asyncpg.connect(_db_url())
    try:
        row = await conn.fetchrow(
            "SELECT closed_at FROM session_buffer WHERE tenant_id=$1 AND session_id=$2",
            tenant,
            session_id
        )
        assert row is not None
        assert row["closed_at"] is not None
    finally:
        await conn.close()
