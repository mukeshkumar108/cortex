from datetime import datetime
from uuid import uuid4
import os
import json

import asyncpg
import pytest
from httpx import AsyncClient, ASGITransport

from src.main import app, graphiti_client
from src import main as main_module
from src import session as session_module


def _db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    password = os.getenv("POSTGRES_PASSWORD", "password")
    return f"postgresql://synapse:{password}@postgres:5432/synapse"


@pytest.mark.asyncio
async def test_session_ingest_enqueues_raw_job_and_transcript():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    called = {"count": 0}

    async def _stub_add_session_episode(**_kwargs):
        called["count"] += 1
        return {"success": True, "episode_uuid": "ep1"}

    graphiti_client.add_session_episode = _stub_add_session_episode

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            resp = await client.post(
                "/session/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "sessionId": session_id,
                    "startedAt": now,
                    "endedAt": now,
                    "messages": [
                        {"role": "user", "text": "My name is Mukesh", "timestamp": now},
                        {"role": "assistant", "text": "Nice to meet you", "timestamp": now}
                    ]
                }
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "ingested"
            assert data["sessionId"] == session_id
            assert data["graphitiAdded"] is False

    assert called["count"] == 0

    conn = await asyncpg.connect(_db_url())
    try:
        row = await conn.fetchrow(
            """
            SELECT job_type, status, dedupe_key, payload
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
              AND job_type = 'session_raw_episode'
            ORDER BY id DESC
            LIMIT 1
            """,
            tenant,
            user,
            session_id
        )
        assert row is not None
        assert row["status"] == "pending"
        assert row["dedupe_key"] == f"session_ingest_raw:{tenant}:{user}:{session_id}"
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        assert isinstance(payload, dict)
        assert payload.get("tenant_id") == tenant
        assert payload.get("user_id") == user
        assert payload.get("session_id") == session_id
        assert payload.get("reference_time")
        transcript = await conn.fetchrow(
            """
            SELECT messages
            FROM session_transcript
            WHERE tenant_id = $1 AND session_id = $2
            """,
            tenant,
            session_id
        )
        assert transcript is not None
        transcript_messages = transcript["messages"]
        if isinstance(transcript_messages, str):
            transcript_messages = json.loads(transcript_messages)
        assert isinstance(transcript_messages, list)
        assert len(transcript_messages) == 2
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_session_ingest_dedupe_key_idempotent():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"
    payload = {
        "tenantId": tenant,
        "userId": user,
        "sessionId": session_id,
        "startedAt": now,
        "endedAt": now,
        "messages": [
            {"role": "user", "text": "hello", "timestamp": now}
        ],
    }

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            first = await client.post("/session/ingest", json=payload)
            second = await client.post("/session/ingest", json=payload)
            assert first.status_code == 200
            assert second.status_code == 200

    conn = await asyncpg.connect(_db_url())
    try:
        count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
              AND dedupe_key = $4
            """,
            tenant,
            user,
            session_id,
            f"session_ingest_raw:{tenant}:{user}:{session_id}"
        )
        assert int(count or 0) == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_session_ingest_dedupe_does_not_reset_sent_job():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    async def _stub_add_session_episode(**_kwargs):
        return {"success": True, "episode_uuid": "ep1"}

    async def _stub_add_session_summary(**_kwargs):
        return {"success": True}

    async def _stub_extract_and_create_loops(**_kwargs):
        return {"new_loops": 0, "completions": 0}

    graphiti_client.add_session_episode = _stub_add_session_episode
    graphiti_client.add_session_summary = _stub_add_session_summary
    session_module.loops.extract_and_create_loops = _stub_extract_and_create_loops

    payload = {
        "tenantId": tenant,
        "userId": user,
        "sessionId": session_id,
        "startedAt": now,
        "endedAt": now,
        "messages": [
            {"role": "user", "text": "hello", "timestamp": now}
        ],
    }

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            first = await client.post("/session/ingest", json=payload)
            assert first.status_code == 200

        await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=20,
            tenant_id=tenant,
            budget_seconds=3.0,
            per_row_timeout_seconds=5.0
        )
        await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=20,
            tenant_id=tenant,
            budget_seconds=3.0,
            per_row_timeout_seconds=5.0
        )

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            second = await client.post("/session/ingest", json=payload)
            assert second.status_code == 200

    conn = await asyncpg.connect(_db_url())
    try:
        row = await conn.fetchrow(
            """
            SELECT status, attempts, sent_at, last_error
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
              AND dedupe_key = $4
            ORDER BY id DESC
            LIMIT 1
            """,
            tenant,
            user,
            session_id,
            f"session_ingest_raw:{tenant}:{user}:{session_id}"
        )
        assert row is not None
        assert row["status"] == "sent"
        assert row["sent_at"] is not None
        assert int(row["attempts"] or 0) >= 1
        assert row["last_error"] in (None, "")
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_session_ingest_graphiti_failure_retries_without_losing_payload():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    async def _stub_add_session_episode(**_kwargs):
        return {"success": False}

    graphiti_client.add_session_episode = _stub_add_session_episode

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            resp = await client.post(
                "/session/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "sessionId": session_id,
                    "startedAt": now,
                    "endedAt": now,
                    "messages": [
                        {"role": "user", "text": "failure case", "timestamp": now}
                    ]
                }
            )
            assert resp.status_code == 200

        counts = await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=20,
            tenant_id=tenant,
            budget_seconds=3.0,
            per_row_timeout_seconds=5.0
        )
        assert counts["claimed"] >= 1
        assert counts["pending"] >= 1

    conn = await asyncpg.connect(_db_url())
    try:
        outbox_row = await conn.fetchrow(
            """
            SELECT status, attempts, next_attempt_at, last_error
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
              AND job_type = 'session_raw_episode'
            ORDER BY id DESC
            LIMIT 1
            """,
            tenant,
            user,
            session_id
        )
        assert outbox_row is not None
        assert outbox_row["status"] in {"pending", "failed"}
        assert int(outbox_row["attempts"] or 0) >= 1
        assert outbox_row["next_attempt_at"] is not None
        transcript = await conn.fetchrow(
            """
            SELECT messages
            FROM session_transcript
            WHERE tenant_id = $1 AND session_id = $2
            """,
            tenant,
            session_id
        )
        assert transcript is not None
        transcript_messages = transcript["messages"]
        if isinstance(transcript_messages, str):
            transcript_messages = json.loads(transcript_messages)
        assert isinstance(transcript_messages, list)
        assert len(transcript_messages) == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_session_ingest_graphiti_permanent_error_dead_letters():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    async def _stub_add_session_episode(**_kwargs):
        return {"success": False, "error": "validation failed: bad payload", "status_code": 400}

    graphiti_client.add_session_episode = _stub_add_session_episode

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            resp = await client.post(
                "/session/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "sessionId": session_id,
                    "startedAt": now,
                    "endedAt": now,
                    "messages": [
                        {"role": "user", "text": "permanent error case", "timestamp": now}
                    ]
                }
            )
            assert resp.status_code == 200

        counts = await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=20,
            tenant_id=tenant,
            budget_seconds=3.0,
            per_row_timeout_seconds=5.0
        )
        assert counts["failed"] >= 1

    conn = await asyncpg.connect(_db_url())
    try:
        outbox_row = await conn.fetchrow(
            """
            SELECT status, attempts, next_attempt_at, last_error
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
              AND job_type = 'session_raw_episode'
            ORDER BY id DESC
            LIMIT 1
            """,
            tenant,
            user,
            session_id
        )
        assert outbox_row is not None
        assert outbox_row["status"] == "failed"
        assert int(outbox_row["attempts"] or 0) >= 1
        assert outbox_row["next_attempt_at"] is None
        assert "validation" in str(outbox_row["last_error"]).lower()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_session_ingest_postproc_jobs_enqueued_and_executed(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    called = {}

    async def _stub_add_session_episode(**_kwargs):
        return {"success": True, "episode_uuid": "ep1"}

    async def _stub_add_session_summary(**kwargs):
        called["summary"] = kwargs
        return {"success": True}

    async def _stub_extract_and_create_loops(**kwargs):
        called["loops"] = kwargs
        return {"new_loops": 1, "completions": 0}

    graphiti_client.add_session_episode = _stub_add_session_episode
    graphiti_client.add_session_summary = _stub_add_session_summary
    monkeypatch.setattr(
        session_module.loops,
        "extract_and_create_loops",
        _stub_extract_and_create_loops,
        raising=True
    )

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            resp = await client.post(
                "/session/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "sessionId": session_id,
                    "startedAt": now,
                    "endedAt": now,
                    "messages": [
                        {"role": "user", "text": "I need to ship this", "timestamp": now},
                        {"role": "assistant", "text": "Let's define one next step", "timestamp": now}
                    ]
                }
            )
            assert resp.status_code == 200

        first = await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=20,
            tenant_id=tenant,
            budget_seconds=3.0,
            per_row_timeout_seconds=5.0
        )
        assert first["sent"] >= 1

        second = await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=20,
            tenant_id=tenant,
            budget_seconds=3.0,
            per_row_timeout_seconds=5.0
        )
        assert second["claimed"] >= 1

    assert called.get("summary", {}).get("session_id") == session_id
    assert called.get("loops", {}).get("session_id") == session_id


@pytest.mark.asyncio
async def test_session_ingest_enqueues_only_summary_and_loops_hooks():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    async def _stub_add_session_episode(**_kwargs):
        return {"success": True, "episode_uuid": "ep1"}

    graphiti_client.add_session_episode = _stub_add_session_episode

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            resp = await client.post(
                "/session/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "sessionId": session_id,
                    "startedAt": now,
                    "endedAt": now,
                    "messages": [
                        {"role": "user", "text": "Hello", "timestamp": now}
                    ]
                }
            )
            assert resp.status_code == 200

        first = await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=20,
            tenant_id=tenant,
            budget_seconds=3.0,
            per_row_timeout_seconds=5.0
        )
        assert first["sent"] >= 1

    conn = await asyncpg.connect(_db_url())
    try:
        rows = await conn.fetch(
            """
            SELECT dedupe_key
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
              AND job_type = 'post_ingest_hook'
            ORDER BY dedupe_key ASC
            """,
            tenant,
            user,
            session_id
        )
        dedupe_keys = [r["dedupe_key"] for r in rows]
        assert dedupe_keys == sorted([
            f"session_hook_loops:{tenant}:{user}:{session_id}",
            f"session_hook_summary:{tenant}:{user}:{session_id}",
        ])
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_session_ingest_commit_freshness_and_debug_status(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    async def _stub_add_session_episode(**_kwargs):
        return {"success": True, "episode_uuid": "ep1"}

    async def _stub_add_session_summary(**_kwargs):
        return {"success": True}

    async def _stub_extract_and_create_loops(**_kwargs):
        return {"new_loops": 1, "completions": 0}

    async def _stub_latest_summary_node(*_args, **_kwargs):
        return None

    async def _stub_recent_session_summary_nodes(*_args, **_kwargs):
        return []

    async def _stub_recent_episode_summaries(*_args, **_kwargs):
        return []

    async def _stub_search_nodes(*_args, **_kwargs):
        return []

    async def _stub_db_fetchone(query, *args, **kwargs):
        # Keep startbrief lightweight for this focused integration test.
        if "FROM daily_analysis" in query:
            return None
        if "FROM user_model" in query:
            return None
        return await original_db_fetchone(query, *args, **kwargs)

    graphiti_client.add_session_episode = _stub_add_session_episode
    graphiti_client.add_session_summary = _stub_add_session_summary
    graphiti_client.get_latest_session_summary_node = _stub_latest_summary_node
    graphiti_client.get_recent_session_summary_nodes = _stub_recent_session_summary_nodes
    graphiti_client.get_recent_episode_summaries = _stub_recent_episode_summaries
    graphiti_client.search_nodes = _stub_search_nodes
    monkeypatch.setattr(
        session_module.loops,
        "extract_and_create_loops",
        _stub_extract_and_create_loops,
        raising=True
    )
    monkeypatch.setattr(main_module, "_require_internal_token", lambda _token: None, raising=True)
    original_db_fetchone = main_module.db.fetchone
    monkeypatch.setattr(main_module.db, "fetchone", _stub_db_fetchone, raising=True)

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            ingest_resp = await client.post(
                "/session/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "sessionId": session_id,
                    "startedAt": now,
                    "endedAt": now,
                    "messages": [
                        {"role": "user", "text": "Need to ship by Friday", "timestamp": now},
                        {"role": "assistant", "text": "Let's pin next action", "timestamp": now}
                    ]
                }
            )
            assert ingest_resp.status_code == 200

            # 1) Ingest returns 200 only after transcript + outbox row are committed.
            conn = await asyncpg.connect(_db_url())
            try:
                transcript_row = await conn.fetchrow(
                    """
                    SELECT updated_at, messages
                    FROM session_transcript
                    WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
                    LIMIT 1
                    """,
                    tenant,
                    user,
                    session_id,
                )
                outbox_row = await conn.fetchrow(
                    """
                    SELECT id, job_type, status, dedupe_key
                    FROM graphiti_outbox
                    WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
                      AND job_type = 'session_raw_episode'
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    tenant,
                    user,
                    session_id,
                )
            finally:
                await conn.close()
            assert transcript_row is not None
            assert outbox_row is not None
            assert outbox_row["status"] == "pending"
            assert outbox_row["dedupe_key"] == f"session_ingest_raw:{tenant}:{user}:{session_id}"

            # 2) Before drain, startbrief freshness reports pending ingest backlog.
            startbrief_resp = await client.get(
                "/session/startbrief",
                params={
                    "tenantId": tenant,
                    "userId": user,
                    "sessionId": session_id,
                    "now": now,
                    "timezone": "UTC",
                }
            )
            assert startbrief_resp.status_code == 200
            startbrief_json = startbrief_resp.json()
            freshness = (((startbrief_json or {}).get("evidence") or {}).get("freshness") or {})
            assert freshness.get("has_pending_session_ingest_jobs") is True
            assert int(freshness.get("pending_raw_episode_jobs") or 0) >= 1

        # Process raw job -> enqueue hooks, then process hooks.
        first = await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=20,
            tenant_id=tenant,
            budget_seconds=3.0,
            per_row_timeout_seconds=5.0
        )
        assert first["sent"] >= 1
        second = await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=20,
            tenant_id=tenant,
            budget_seconds=3.0,
            per_row_timeout_seconds=5.0
        )
        assert second["claimed"] >= 1

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            # 3) Debug endpoint returns transcript presence + raw/hook job rows.
            status_resp = await client.get(
                "/internal/debug/session_ingest_status",
                params={
                    "tenant_id": tenant,
                    "user_id": user,
                    "session_id": session_id,
                },
                headers={"X-Internal-Token": "test-token"},
            )
            assert status_resp.status_code == 200
            status_json = status_resp.json()
            transcript = status_json.get("transcript") or {}
            jobs = status_json.get("jobs") or []
            assert transcript.get("exists") is True
            assert int(transcript.get("message_count") or 0) >= 1
            job_types = {j.get("job_type") for j in jobs}
            assert "session_raw_episode" in job_types
            assert "post_ingest_hook" in job_types


@pytest.mark.asyncio
async def test_session_close_after_ingest_does_not_duplicate_raw_or_hooks(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"
    calls = {"raw_episode": 0}

    async def _stub_add_session_episode(**_kwargs):
        calls["raw_episode"] += 1
        return {"success": True, "episode_uuid": "ep1"}

    async def _stub_add_session_summary(**_kwargs):
        return {"success": True}

    async def _stub_extract_and_create_loops(**_kwargs):
        return {"new_loops": 0, "completions": 0}

    graphiti_client.add_session_episode = _stub_add_session_episode
    graphiti_client.add_session_summary = _stub_add_session_summary
    monkeypatch.setattr(
        session_module.loops,
        "extract_and_create_loops",
        _stub_extract_and_create_loops,
        raising=True
    )

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            ingest_resp = await client.post(
                "/session/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "sessionId": session_id,
                    "startedAt": now,
                    "endedAt": now,
                    "messages": [
                        {"role": "user", "text": "Please remember this", "timestamp": now},
                        {"role": "assistant", "text": "I will", "timestamp": now}
                    ],
                },
            )
            assert ingest_resp.status_code == 200

            close_resp = await client.post(
                "/session/close",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "sessionId": session_id,
                    "personaId": "persona"
                },
            )
            assert close_resp.status_code == 200
            assert close_resp.json().get("closed") is True

        first = await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=20,
            tenant_id=tenant,
            budget_seconds=3.0,
            per_row_timeout_seconds=5.0
        )
        assert first["sent"] >= 1
        second = await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=20,
            tenant_id=tenant,
            budget_seconds=3.0,
            per_row_timeout_seconds=5.0
        )
        assert second["claimed"] >= 1

    assert calls["raw_episode"] == 1

    conn = await asyncpg.connect(_db_url())
    try:
        raw_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
              AND job_type = 'session_raw_episode'
            """,
            tenant,
            user,
            session_id,
        )
        hook_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
              AND job_type = 'post_ingest_hook'
            """,
            tenant,
            user,
            session_id,
        )
    finally:
        await conn.close()

    assert int(raw_count or 0) == 1
    assert int(hook_count or 0) == 2


@pytest.mark.asyncio
async def test_session_close_without_ingest_enqueues_session_ingest():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            first_ingest = await client.post(
                "/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "personaId": "persona",
                    "sessionId": session_id,
                    "role": "user",
                    "text": "I need to remember this action.",
                    "timestamp": now,
                },
            )
            assert first_ingest.status_code == 200

            second_ingest = await client.post(
                "/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "personaId": "persona",
                    "sessionId": session_id,
                    "role": "assistant",
                    "text": "Captured.",
                    "timestamp": now,
                },
            )
            assert second_ingest.status_code == 200

            close_resp = await client.post(
                "/session/close",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "sessionId": session_id,
                    "personaId": "persona"
                },
            )
            assert close_resp.status_code == 200
            assert close_resp.json().get("closed") is True

    conn = await asyncpg.connect(_db_url())
    try:
        raw_row = await conn.fetchrow(
            """
            SELECT dedupe_key, status
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
              AND job_type = 'session_raw_episode'
            ORDER BY id DESC
            LIMIT 1
            """,
            tenant,
            user,
            session_id
        )
        transcript_row = await conn.fetchrow(
            """
            SELECT messages
            FROM session_transcript
            WHERE tenant_id = $1 AND session_id = $2
            """,
            tenant,
            session_id
        )
    finally:
        await conn.close()

    assert raw_row is not None
    assert raw_row["status"] == "pending"
    assert raw_row["dedupe_key"] == f"session_ingest_raw:{tenant}:{user}:{session_id}"
    assert transcript_row is not None


@pytest.mark.asyncio
async def test_session_ingest_infers_habit_daily_log_signals(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"
    habit_id = uuid4()

    async def _stub_add_session_episode(**_kwargs):
        return {"success": True, "episode_uuid": "ep1"}

    async def _stub_add_session_summary(**_kwargs):
        return {"success": True}

    async def _stub_extract_and_create_loops(**_kwargs):
        return {"new_loops": 0, "completions": 0}

    async def _stub_habit_inference_llm(*, prompt, max_tokens=500, temperature=0.7, task="generic"):
        assert "HABITS_JSON" in str(prompt)
        assert "TRANSCRIPT" in str(prompt)
        return json.dumps(
            {
                    "habits": [
                    {"habit_id": str(habit_id), "status": "completed", "nudged": True, "acknowledged": True}
                ]
            }
        )

    graphiti_client.add_session_episode = _stub_add_session_episode
    graphiti_client.add_session_summary = _stub_add_session_summary
    monkeypatch.setattr(
        session_module.loops,
        "extract_and_create_loops",
        _stub_extract_and_create_loops,
        raising=True
    )

    conn = await asyncpg.connect(_db_url())
    try:
        await conn.execute(
            """
            INSERT INTO loops (
                id, tenant_id, user_id, persona_id, type, status, text,
                confidence, salience, time_horizon, hint, metadata, created_at, updated_at, last_seen_at
            )
            VALUES (
                $1, $2, $3, 'persona', 'habit', 'active', 'Walk 10k steps daily',
                0.9, 5, 'ongoing', 'can be split across walks', '{}'::jsonb, NOW(), NOW(), NOW()
            )
            """,
            habit_id,
            tenant,
            user,
        )
    finally:
        await conn.close()

    async with app.router.lifespan_context(app):
        monkeypatch.setattr(
            session_module._manager.llm_client,
            "_call_llm",
            _stub_habit_inference_llm,
            raising=True
        )
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            ingest_resp = await client.post(
                "/session/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "sessionId": session_id,
                    "startedAt": now,
                    "endedAt": now,
                    "messages": [
                        {"role": "user", "text": "I went for a walk and hit my steps.", "timestamp": now},
                        {"role": "assistant", "text": "Great, keep your walk habit going.", "timestamp": now},
                    ],
                },
            )
            assert ingest_resp.status_code == 200

        first = await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=20,
            tenant_id=tenant,
            budget_seconds=3.0,
            per_row_timeout_seconds=5.0
        )
        assert first["sent"] >= 1
        second = await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=20,
            tenant_id=tenant,
            budget_seconds=3.0,
            per_row_timeout_seconds=5.0
        )
        assert second["claimed"] >= 1

    conn = await asyncpg.connect(_db_url())
    try:
        row = await conn.fetchrow(
            """
            SELECT completed, nudged, acknowledged, user_response, inferred_from
            FROM habit_daily_log
            WHERE user_id = $1
              AND habit_id = $2
              AND date = CURRENT_DATE
            """,
            user,
            habit_id,
        )
    finally:
        await conn.close()

    assert row is not None
    assert bool(row["completed"]) is True
    assert bool(row["nudged"]) is True
    assert bool(row["acknowledged"]) is True
    assert row["inferred_from"] == "transcript mention"
