import asyncio
import json
import os
from datetime import datetime, timedelta
from uuid import uuid4

import asyncpg
import pytest
from httpx import AsyncClient, ASGITransport

from src.main import app, graphiti_client
from src import session as session_module
from src.openrouter_client import get_llm_client


def _db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    password = os.getenv("POSTGRES_PASSWORD", "password")
    return f"postgresql://synapse:{password}@postgres:5432/synapse"


@pytest.mark.asyncio
async def test_sliding_window_never_exceeds_12():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow()

    async with app.router.lifespan_context(app):
        for i in range(20):
            await session_module.add_turn(
                tenant_id=tenant,
                session_id=session_id,
                user_id=user,
                role="user",
                text=f"turn {i}",
                timestamp=(now + timedelta(seconds=i)).isoformat() + "Z"
            )

    conn = await asyncpg.connect(_db_url())
    try:
        messages = await conn.fetchval(
            """
            SELECT messages
            FROM session_buffer
            WHERE tenant_id = $1 AND session_id = $2
            """,
            tenant,
            session_id
        )
        if isinstance(messages, str):
            messages = json.loads(messages)
        assert len(messages) <= 12
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_concurrent_ingest_never_exceeds_12():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow()
    total_turns = 20

    async with app.router.lifespan_context(app):
        start = asyncio.Event()

        async def _worker(i: int) -> None:
            await start.wait()
            await session_module.add_turn(
                tenant_id=tenant,
                session_id=session_id,
                user_id=user,
                role="user",
                text=f"turn {i}",
                timestamp=(now + timedelta(seconds=i)).isoformat() + "Z"
            )

        tasks = [asyncio.create_task(_worker(i)) for i in range(total_turns)]
        start.set()
        await asyncio.gather(*tasks)

    conn = await asyncpg.connect(_db_url())
    try:
        messages = await conn.fetchval(
            """
            SELECT messages
            FROM session_buffer
            WHERE tenant_id = $1 AND session_id = $2
            """,
            tenant,
            session_id
        )
        if isinstance(messages, str):
            messages = json.loads(messages)
        assert len(messages) <= 12

        outbox_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
            """,
            tenant,
            user,
            session_id
        )
        assert outbox_count == total_turns - 12
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_eviction_writes_outbox():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow()

    async with app.router.lifespan_context(app):
        for i in range(13):
            await session_module.add_turn(
                tenant_id=tenant,
                session_id=session_id,
                user_id=user,
                role="user",
                text=f"turn {i}",
                timestamp=(now + timedelta(seconds=i)).isoformat() + "Z"
            )

    conn = await asyncpg.connect(_db_url())
    try:
        row = await conn.fetchrow(
            """
            SELECT tenant_id, user_id, session_id, status
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
            ORDER BY id ASC
            LIMIT 1
            """,
            tenant,
            user,
            session_id
        )
        assert row is not None
        assert row["status"] == "pending"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_janitor_marks_outbox_sent():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow()

    llm_client = get_llm_client()
    llm_client._call_llm = lambda *args, **kwargs: asyncio.sleep(0, result="summary")

    async with app.router.lifespan_context(app):
        for i in range(13):
            await session_module.add_turn(
                tenant_id=tenant,
                session_id=session_id,
                user_id=user,
                role="user",
                text=f"turn {i}",
                timestamp=(now + timedelta(seconds=i)).isoformat() + "Z"
            )

        await session_module.janitor_process(tenant, session_id, user, graphiti_client)

    conn = await asyncpg.connect(_db_url())
    try:
        status = await conn.fetchval(
            """
            SELECT status
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
            ORDER BY id ASC
            LIMIT 1
            """,
            tenant,
            user,
            session_id
        )
        assert status == "sent"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_janitor_accepts_add_episode_without_metadata_param():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow()

    llm_client = get_llm_client()
    llm_client._call_llm = lambda *args, **kwargs: asyncio.sleep(0, result="summary")

    class _StrictGraph:
        async def add_episode(self, **_kwargs):
            raise AssertionError("add_episode should not be called when per-turn is disabled")

    async with app.router.lifespan_context(app):
        for i in range(13):
            await session_module.add_turn(
                tenant_id=tenant,
                session_id=session_id,
                user_id=user,
                role="user",
                text=f"turn {i}",
                timestamp=(now + timedelta(seconds=i)).isoformat() + "Z"
            )

        await session_module.janitor_process(tenant, session_id, user, _StrictGraph())

    conn = await asyncpg.connect(_db_url())
    try:
        status = await conn.fetchval(
            """
            SELECT status
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
            ORDER BY id ASC
            LIMIT 1
            """,
            tenant,
            user,
            session_id
        )
        assert status == "sent"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_janitor_dead_letters_permanent_errors():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow()

    llm_client = get_llm_client()
    llm_client._call_llm = lambda *args, **kwargs: asyncio.sleep(0, result="summary")
    class _PermanentErrorGraph:
        async def add_episode(self, **_kwargs):
            raise TypeError("unexpected keyword argument 'metadata'")

    async with app.router.lifespan_context(app):
        settings = session_module._manager.settings
        original = settings.graphiti_per_turn
        settings.graphiti_per_turn = True
        for i in range(13):
            await session_module.add_turn(
                tenant_id=tenant,
                session_id=session_id,
                user_id=user,
                role="user",
                text=f"turn {i}",
                timestamp=(now + timedelta(seconds=i)).isoformat() + "Z"
            )

        await session_module.janitor_process(tenant, session_id, user, _PermanentErrorGraph())
        settings.graphiti_per_turn = original

    conn = await asyncpg.connect(_db_url())
    try:
        row = await conn.fetchrow(
            """
            SELECT status, next_attempt_at, last_error
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
            ORDER BY id ASC
            LIMIT 1
            """,
            tenant,
            user,
            session_id
        )
        assert row["status"] == "failed"
        assert row["next_attempt_at"] is None
        assert "metadata" in (row["last_error"] or "")
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_janitor_retries_transient_errors_with_backoff():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow()

    llm_client = get_llm_client()
    llm_client._call_llm = lambda *args, **kwargs: asyncio.sleep(0, result="summary")
    class _TransientErrorGraph:
        async def add_episode(self, **_kwargs):
            raise TimeoutError("timed out")

    async with app.router.lifespan_context(app):
        settings = session_module._manager.settings
        original = settings.graphiti_per_turn
        settings.graphiti_per_turn = True
        for i in range(13):
            await session_module.add_turn(
                tenant_id=tenant,
                session_id=session_id,
                user_id=user,
                role="user",
                text=f"turn {i}",
                timestamp=(now + timedelta(seconds=i)).isoformat() + "Z"
            )

        await session_module.janitor_process(tenant, session_id, user, _TransientErrorGraph())
        settings.graphiti_per_turn = original

    conn = await asyncpg.connect(_db_url())
    try:
        row = await conn.fetchrow(
            """
            SELECT status, next_attempt_at, attempts
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
            ORDER BY id ASC
            LIMIT 1
            """,
            tenant,
            user,
            session_id
        )
        assert row["status"] == "pending"
        assert row["next_attempt_at"] is not None
        assert row["attempts"] >= 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_internal_drain_endpoint_drains_outbox():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow()

    llm_client = get_llm_client()
    llm_client._call_llm = lambda *args, **kwargs: asyncio.sleep(0, result="summary")

    async with app.router.lifespan_context(app):
        for i in range(13):
            await session_module.add_turn(
                tenant_id=tenant,
                session_id=session_id,
                user_id=user,
                role="user",
                text=f"turn {i}",
                timestamp=(now + timedelta(seconds=i)).isoformat() + "Z"
            )

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            resp = await client.post(f"/internal/drain?limit=10&tenant_id={tenant}")
            assert resp.status_code == 200
            payload = resp.json()
            assert payload["sent"] >= 1

    conn = await asyncpg.connect(_db_url())
    try:
        pending = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
              AND status = 'pending'
            """,
            tenant,
            user,
            session_id
        )
        assert pending == 0
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_drain_claims_null_next_attempt_at():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow()

    llm_client = get_llm_client()
    llm_client._call_llm = lambda *args, **kwargs: asyncio.sleep(0, result="summary")

    async with app.router.lifespan_context(app):
        for i in range(13):
            await session_module.add_turn(
                tenant_id=tenant,
                session_id=session_id,
                user_id=user,
                role="user",
                text=f"turn {i}",
                timestamp=(now + timedelta(seconds=i)).isoformat() + "Z"
            )

        conn = await asyncpg.connect(_db_url())
        try:
            await conn.execute(
                """
                UPDATE graphiti_outbox
                SET next_attempt_at = NULL
                WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
                """,
                tenant,
                user,
                session_id
            )
        finally:
            await conn.close()

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            resp = await client.post(f"/internal/drain?limit=10&tenant_id={tenant}")
            assert resp.status_code == 200
            payload = resp.json()
            assert payload["claimed"] >= 1


@pytest.mark.asyncio
async def test_drain_budget_returns_quickly():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow()

    async with app.router.lifespan_context(app):
        settings = session_module._manager.settings
        original = settings.graphiti_per_turn
        settings.graphiti_per_turn = True

        class _SlowGraph:
            async def add_episode(self, **_kwargs):
                await asyncio.sleep(5)
                return {"success": True}

        graphiti_client.add_episode = _SlowGraph().add_episode
        for i in range(13):
            await session_module.add_turn(
                tenant_id=tenant,
                session_id=session_id,
                user_id=user,
                role="user",
                text=f"turn {i}",
                timestamp=(now + timedelta(seconds=i)).isoformat() + "Z"
            )

        start = datetime.utcnow()
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            resp = await client.post(
                f"/internal/drain?limit=10&tenant_id={tenant}&budget_seconds=0.2&per_row_timeout_seconds=0.1"
            )
            assert resp.status_code == 200
        elapsed = (datetime.utcnow() - start).total_seconds()
        assert elapsed < 2.0
        settings.graphiti_per_turn = original


@pytest.mark.asyncio
async def test_drain_returns_zero_when_none_eligible():
    tenant = f"tenant-{uuid4().hex}"
    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            resp = await client.post(f"/internal/drain?limit=5&tenant_id={tenant}")
            assert resp.status_code == 200
            payload = resp.json()
            assert payload["claimed"] == 0


@pytest.mark.asyncio
async def test_outbox_retry_on_graphiti_failure():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow()

    llm_client = get_llm_client()
    llm_client._call_llm = lambda *args, **kwargs: asyncio.sleep(0, result="summary")
    attempts = {"count": 0}

    async def _flaky_add_episode(**_kwargs):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("graphiti down")
        return {"success": True}

    graphiti_client.add_episode = _flaky_add_episode

    async with app.router.lifespan_context(app):
        settings = session_module._manager.settings
        original = settings.graphiti_per_turn
        settings.graphiti_per_turn = True
        for i in range(13):
            await session_module.add_turn(
                tenant_id=tenant,
                session_id=session_id,
                user_id=user,
                role="user",
                text=f"turn {i}",
                timestamp=(now + timedelta(seconds=i)).isoformat() + "Z"
            )

        await session_module.janitor_process(tenant, session_id, user, graphiti_client)

        conn = await asyncpg.connect(_db_url())
        try:
            await conn.execute(
                """
                UPDATE graphiti_outbox
                SET next_attempt_at = NOW() - INTERVAL '1 second'
                WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
                """,
                tenant,
                user,
                session_id
            )
        finally:
            await conn.close()

        await session_module.janitor_process(tenant, session_id, user, graphiti_client)
        settings.graphiti_per_turn = original

    conn = await asyncpg.connect(_db_url())
    try:
        row = await conn.fetchrow(
            """
            SELECT status, attempts
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
            ORDER BY id ASC
            LIMIT 1
            """,
            tenant,
            user,
            session_id
        )
        assert row["attempts"] >= 2
        assert row["status"] == "sent"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_outbox_folding_not_repeated_on_retry():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow()

    attempts = {"count": 0}
    fold_calls = {"count": 0}

    async def _flaky_add_episode(**_kwargs):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("graphiti down")
        return {"success": True}

    async with app.router.lifespan_context(app):
        settings = session_module._manager.settings
        original = settings.graphiti_per_turn
        settings.graphiti_per_turn = True
        graphiti_client.add_episode = _flaky_add_episode
        for i in range(13):
            await session_module.add_turn(
                tenant_id=tenant,
                session_id=session_id,
                user_id=user,
                role="user",
                text=f"turn {i}",
                timestamp=(now + timedelta(seconds=i)).isoformat() + "Z"
            )

        manager = session_module._manager

        async def _count_fold(current_summary, new_turn):
            fold_calls["count"] += 1
            return "summary"

        manager._fold_into_summary = _count_fold

        await session_module.janitor_process(tenant, session_id, user, graphiti_client)
        await session_module.janitor_process(tenant, session_id, user, graphiti_client)
        settings.graphiti_per_turn = original

    assert fold_calls["count"] == 1


@pytest.mark.asyncio
async def test_session_close_flushes_remaining_turns():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow()

    calls = {"count": 0}

    async def _count_episode(**_kwargs):
        calls["count"] += 1
        return {"success": True}

    graphiti_client.add_episode = _count_episode

    async with app.router.lifespan_context(app):
        for i in range(3):
            await session_module.add_turn(
                tenant_id=tenant,
                session_id=session_id,
                user_id=user,
                role="user",
                text=f"turn {i}",
                timestamp=(now + timedelta(seconds=i)).isoformat() + "Z"
            )

        await session_module.close_session(tenant, session_id, user, graphiti_client)

    conn = await asyncpg.connect(_db_url())
    try:
        closed_at = await conn.fetchval(
            """
            SELECT closed_at FROM session_buffer
            WHERE tenant_id = $1 AND session_id = $2
            """,
            tenant,
            session_id
        )
        assert closed_at is not None

        count = await conn.fetchval(
            """
            SELECT COUNT(*) FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
            """,
            tenant,
            user,
            session_id
        )
        assert count == 3
        assert calls["count"] == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_tenant_isolation_outbox():
    tenant_a = f"tenant-{uuid4().hex}"
    tenant_b = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow()

    class _FakeGraph:
        def __init__(self):
            self.group_ids = []

        async def add_episode(self, **kwargs):
            self.group_ids.append(kwargs.get("group_id"))

    fake_graph = _FakeGraph()
    graphiti_client.client = fake_graph
    graphiti_client._initialized = True
    graphiti_client.add_episode = graphiti_client.__class__.add_episode.__get__(
        graphiti_client,
        graphiti_client.__class__
    )

    llm_client = get_llm_client()
    llm_client._call_llm = lambda *args, **kwargs: asyncio.sleep(0, result="summary")

    async with app.router.lifespan_context(app):
        for i in range(13):
            await session_module.add_turn(
                tenant_id=tenant_a,
                session_id=session_id,
                user_id=user,
                role="user",
                text=f"turn {i}",
                timestamp=(now + timedelta(seconds=i)).isoformat() + "Z"
            )
        for i in range(13):
            await session_module.add_turn(
                tenant_id=tenant_b,
                session_id=session_id,
                user_id=user,
                role="user",
                text=f"turn {i}",
                timestamp=(now + timedelta(seconds=i)).isoformat() + "Z"
            )

        await session_module.close_session(tenant_a, session_id, user, graphiti_client)
        await session_module.close_session(tenant_b, session_id, user, graphiti_client)

    conn = await asyncpg.connect(_db_url())
    try:
        count_a = await conn.fetchval(
            """
            SELECT COUNT(*) FROM graphiti_outbox
            WHERE tenant_id = $1 AND session_id = $2
            """,
            tenant_a,
            session_id
        )
        count_b = await conn.fetchval(
            """
            SELECT COUNT(*) FROM graphiti_outbox
            WHERE tenant_id = $1 AND session_id = $2
            """,
            tenant_b,
            session_id
        )
        assert count_a == 13
        assert count_b == 13
        assert f"{tenant_a}__{user}" in fake_graph.group_ids
        assert f"{tenant_b}__{user}" in fake_graph.group_ids
    finally:
        await conn.close()
