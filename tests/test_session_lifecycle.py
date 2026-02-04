import asyncio
import json
import os
import time
from datetime import datetime, timedelta
from uuid import uuid4

import asyncpg
import pytest
from httpx import AsyncClient, ASGITransport

from src.main import app, graphiti_client
from src import loops, session as session_module
from starlette.background import BackgroundTasks
from src.openrouter_client import get_llm_client


def _db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    password = os.getenv("POSTGRES_PASSWORD", "password")
    return f"postgresql://synapse:{password}@postgres:5432/synapse"


_ZERO_VECTOR_1536 = "[" + ",".join(["0"] * 1536) + "]"


@pytest.mark.asyncio
async def test_ingest_without_session_id_autocreates():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    async def _stub_add_episode(**kwargs):
        return {"success": False}

    graphiti_client.add_episode = _stub_add_episode

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            resp = await client.post(
                "/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "personaId": "persona",
                    "role": "user",
                    "text": "hello there friend",
                    "timestamp": now
                }
            )
            data = resp.json()
            assert data["status"] == "ingested"
            assert data["sessionId"]
            session_id = data["sessionId"]

    conn = await asyncpg.connect(_db_url())
    try:
        row = await conn.fetchrow(
            """
            SELECT tenant_id, session_id
            FROM session_buffer
            WHERE tenant_id = $1 AND session_id = $2
            """,
            tenant,
            session_id
        )
        assert row is not None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_ingest_prefers_top_level_session_id():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    async def _stub_add_episode(**kwargs):
        return {"success": False}

    graphiti_client.add_episode = _stub_add_episode

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            resp = await client.post(
                "/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "personaId": "persona",
                    "sessionId": session_id,
                    "role": "user",
                    "text": "hello there friend",
                    "timestamp": now,
                    "metadata": {"sessionId": f"session-{uuid4().hex}"}
                }
            )
            data = resp.json()
            assert data["status"] == "ingested"
            assert data["sessionId"] == session_id

    conn = await asyncpg.connect(_db_url())
    try:
        row = await conn.fetchrow(
            """
            SELECT tenant_id, session_id
            FROM session_buffer
            WHERE tenant_id = $1 AND session_id = $2
            """,
            tenant,
            session_id
        )
        assert row is not None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_ingest_falls_back_to_metadata_session_id():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    async def _stub_add_episode(**kwargs):
        return {"success": False}

    graphiti_client.add_episode = _stub_add_episode

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            resp = await client.post(
                "/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "personaId": "persona",
                    "role": "user",
                    "text": "hello there friend",
                    "timestamp": now,
                    "metadata": {"sessionId": session_id}
                }
            )
            data = resp.json()
            assert data["status"] == "ingested"
            assert data["sessionId"] == session_id

    conn = await asyncpg.connect(_db_url())
    try:
        row = await conn.fetchrow(
            """
            SELECT tenant_id, session_id
            FROM session_buffer
            WHERE tenant_id = $1 AND session_id = $2
            """,
            tenant,
            session_id
        )
        assert row is not None
    finally:
        await conn.close()

@pytest.mark.asyncio
async def test_session_pk_isolation():
    tenant_a = f"tenant-{uuid4().hex}"
    tenant_b = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    async def _stub_add_episode(**kwargs):
        return {"success": False}

    graphiti_client.add_episode = _stub_add_episode

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            await client.post(
                "/ingest",
                json={
                    "tenantId": tenant_a,
                    "userId": user,
                    "personaId": "persona",
                    "role": "user",
                    "text": "hello from tenant a",
                    "timestamp": now,
                    "metadata": {"sessionId": session_id}
                }
            )
            await client.post(
                "/ingest",
                json={
                    "tenantId": tenant_b,
                    "userId": user,
                    "personaId": "persona",
                    "role": "user",
                    "text": "hello from tenant b",
                    "timestamp": now,
                    "metadata": {"sessionId": session_id}
                }
            )

    conn = await asyncpg.connect(_db_url())
    try:
        rows = await conn.fetch(
            """
            SELECT tenant_id, session_id
            FROM session_buffer
            WHERE session_id = $1
            """,
            session_id
        )
        assert len(rows) == 2
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_rolling_summary():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow()

    llm_client = get_llm_client()
    llm_client._call_llm = lambda *args, **kwargs: asyncio.sleep(0, result="summary")

    async def _stub_add_episode(**kwargs):
        return {"success": False}

    graphiti_client.add_episode = _stub_add_episode

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            for i in range(14):
                await client.post(
                    "/ingest",
                    json={
                        "tenantId": tenant,
                        "userId": user,
                        "personaId": "persona",
                        "role": "user",
                        "text": f"turn {i}",
                        "timestamp": (now + timedelta(seconds=i)).isoformat() + "Z",
                        "metadata": {"sessionId": session_id}
                    }
                )

    await session_module.janitor_process(tenant, session_id, user, graphiti_client)
    await asyncio.sleep(0.05)

    conn = await asyncpg.connect(_db_url())
    try:
        row = await conn.fetchrow(
            """
            SELECT rolling_summary, messages
            FROM session_buffer
            WHERE tenant_id = $1 AND session_id = $2
            """,
            tenant,
            session_id
        )
        assert row["rolling_summary"] is not None
        messages = row["messages"]
        if isinstance(messages, str):
            messages = json.loads(messages)
        assert len(messages) == 12
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_session_close_gap():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    old_time = datetime.utcnow() - timedelta(minutes=31)
    now = datetime.utcnow()

    llm_client = get_llm_client()
    llm_client._call_llm = lambda *args, **kwargs: asyncio.sleep(0, result="summary")

    async def _stub_add_episode(**kwargs):
        return {"success": True}

    graphiti_client.add_episode = _stub_add_episode

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            await client.post(
                "/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "personaId": "persona",
                    "role": "user",
                    "text": "old message",
                    "timestamp": old_time.isoformat() + "Z",
                    "metadata": {"sessionId": session_id}
                }
            )
            await client.post(
                "/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "personaId": "persona",
                    "role": "user",
                    "text": "new message",
                    "timestamp": now.isoformat() + "Z",
                    "metadata": {"sessionId": session_id}
                }
            )
    await session_module.close_session(tenant, session_id, user, graphiti_client)
    await asyncio.sleep(0.05)

    conn = await asyncpg.connect(_db_url())
    try:
        row = await conn.fetchrow(
            """
            SELECT closed_at
            FROM session_buffer
            WHERE tenant_id = $1 AND session_id = $2
            """,
            tenant,
            session_id
        )
        assert row["closed_at"] is not None
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_ingest_returns_fast(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    recorded = []

    def _noop_add_task(self, func, *args, **kwargs):
        recorded.append((func, args, kwargs))

    monkeypatch.setattr(BackgroundTasks, "add_task", _noop_add_task, raising=True)

    async def _stub_add_episode(**kwargs):
        return {"success": False}

    graphiti_client.add_episode = _stub_add_episode

    start = time.monotonic()
    async with app.router.lifespan_context(app):
        if loops._manager is not None:
            async def _no_embed(_text):
                return _ZERO_VECTOR_1536
            loops._manager._generate_embedding = _no_embed

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            resp = await client.post(
                "/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "personaId": "persona",
                    "role": "user",
                    "text": "I will do this tomorrow",
                    "timestamp": now,
                    "metadata": {"sessionId": session_id}
                }
            )
            assert resp.status_code == 200
    elapsed = time.monotonic() - start

    assert elapsed < 0.2
    assert recorded
