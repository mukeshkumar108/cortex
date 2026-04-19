from datetime import datetime
from uuid import uuid4
import json

import asyncpg
import pytest
from httpx import ASGITransport, AsyncClient

from src.config import get_settings
from src.main import app


def _db_url() -> str:
    return get_settings().get_database_url()


@pytest.mark.asyncio
async def test_t3_dual_write_ingest_idempotent_for_turns_v2(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"
    payload = {
        "tenantId": tenant,
        "userId": user,
        "personaId": "persona",
        "sessionId": session_id,
        "role": "user",
        "text": "same payload should dedupe in turns_v2",
        "timestamp": now,
    }

    settings = get_settings()
    monkeypatch.setattr(settings, "v2_dual_write_enabled", True, raising=False)
    monkeypatch.setattr(settings, "v2_dual_write_fail_open", False, raising=False)

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            first = await client.post("/ingest", json=payload)
            second = await client.post("/ingest", json=payload)
            assert first.status_code == 200
            assert second.status_code == 200
            assert first.json()["status"] == "ingested"
            assert second.json()["status"] == "ingested"

    conn = await asyncpg.connect(_db_url())
    try:
        v2_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM turns_v2
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
            """,
            tenant,
            user,
            session_id,
        )
        assert int(v2_count or 0) == 1

        idem_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM turn_ingest_idempotency
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
            """,
            tenant,
            user,
            session_id,
        )
        assert int(idem_count or 0) == 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_t3b_out_of_order_insert_is_normalized_deterministically(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    t1 = "2026-01-01T10:00:05Z"
    t0 = "2026-01-01T10:00:01Z"

    settings = get_settings()
    monkeypatch.setattr(settings, "v2_dual_write_enabled", True, raising=False)
    monkeypatch.setattr(settings, "v2_dual_write_fail_open", False, raising=False)

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            first = await client.post(
                "/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "personaId": "persona",
                    "sessionId": session_id,
                    "role": "user",
                    "text": "later event",
                    "timestamp": t1,
                },
            )
            second = await client.post(
                "/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "personaId": "persona",
                    "sessionId": session_id,
                    "role": "assistant",
                    "text": "earlier event arriving late",
                    "timestamp": t0,
                },
            )
            assert first.status_code == 200
            assert second.status_code == 200

    conn = await asyncpg.connect(_db_url())
    try:
        rows = await conn.fetch(
            """
            SELECT turn_id, turn_index, occurred_at, metadata
            FROM turns_v2
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
            ORDER BY turn_index ASC
            """,
            tenant,
            user,
            session_id,
        )
        assert len(rows) == 2
        assert int(rows[0]["turn_index"]) == 0
        assert int(rows[1]["turn_index"]) == 1
        assert int(rows[0]["turn_id"]) < int(rows[1]["turn_id"])
        assert rows[0]["occurred_at"] < rows[1]["occurred_at"]
        second_meta = rows[1]["metadata"] or {}
        if isinstance(second_meta, str):
            second_meta = json.loads(second_meta)
        assert second_meta.get("timestamp_normalized_from_out_of_order") is True
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_t3_dual_write_session_ingest_writes_v2_evidence_rows(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    settings = get_settings()
    monkeypatch.setattr(settings, "v2_dual_write_enabled", True, raising=False)
    monkeypatch.setattr(settings, "v2_dual_write_fail_open", False, raising=False)

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post(
                "/session/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "sessionId": session_id,
                    "startedAt": now,
                    "endedAt": now,
                    "messages": [
                        {"role": "user", "text": "hello", "timestamp": now},
                        {"role": "assistant", "text": "hi", "timestamp": now},
                    ],
                },
            )
            assert resp.status_code == 200
            assert resp.json()["status"] == "ingested"

    conn = await asyncpg.connect(_db_url())
    try:
        session_row = await conn.fetchrow(
            """
            SELECT tenant_id, session_id, user_id, status
            FROM sessions_v2
            WHERE tenant_id = $1 AND session_id = $2
            """,
            tenant,
            session_id,
        )
        assert session_row is not None
        assert session_row["user_id"] == user
        assert session_row["status"] == "open"

        rows = await conn.fetch(
            """
            SELECT role, content, turn_index
            FROM turns_v2
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
            ORDER BY turn_index ASC
            """,
            tenant,
            user,
            session_id,
        )
        assert len(rows) == 2
        assert rows[0]["role"] == "user"
        assert rows[0]["content"] == "hello"
        assert rows[1]["role"] == "assistant"
        assert rows[1]["content"] == "hi"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_t3b_session_ingest_rejects_malformed_turn(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    settings = get_settings()
    monkeypatch.setattr(settings, "v2_dual_write_enabled", True, raising=False)
    monkeypatch.setattr(settings, "v2_dual_write_fail_open", False, raising=False)

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post(
                "/session/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "sessionId": session_id,
                    "messages": [
                        {"role": "user", "text": "ok", "timestamp": now},
                        {"role": "assistant", "text": "   ", "timestamp": now},
                    ],
                },
            )
            assert resp.status_code == 400
            detail = resp.json().get("detail") or {}
            assert detail.get("code") == "EVIDENCE_EMPTY_TEXT"

    conn = await asyncpg.connect(_db_url())
    try:
        turns_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM turns_v2
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
            """,
            tenant,
            user,
            session_id,
        )
        assert int(turns_count or 0) == 0
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_t3b_timestamps_are_normalized_to_utc(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    local_ts = "2026-01-01T12:00:00+02:00"

    settings = get_settings()
    monkeypatch.setattr(settings, "v2_dual_write_enabled", True, raising=False)
    monkeypatch.setattr(settings, "v2_dual_write_fail_open", False, raising=False)

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post(
                "/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "personaId": "persona",
                    "sessionId": session_id,
                    "role": "user",
                    "text": "I have timezone normalized evidence",
                    "timestamp": local_ts,
                },
            )
            assert resp.status_code == 200
            assert resp.json()["status"] == "ingested"

    conn = await asyncpg.connect(_db_url())
    try:
        turn = await conn.fetchrow(
            """
            SELECT occurred_at
            FROM turns_v2
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
            ORDER BY turn_index ASC
            LIMIT 1
            """,
            tenant,
            user,
            session_id,
        )
        assert turn is not None
        occurred = turn["occurred_at"]
        assert occurred.tzinfo is not None
        assert occurred.isoformat() == "2026-01-01T10:00:00+00:00"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_t3_dual_write_preserves_legacy_ingest_path(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    settings = get_settings()
    monkeypatch.setattr(settings, "v2_dual_write_enabled", True, raising=False)
    monkeypatch.setattr(settings, "v2_dual_write_fail_open", False, raising=False)

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post(
                "/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "personaId": "persona",
                    "sessionId": session_id,
                    "role": "assistant",
                    "text": "legacy path still active",
                    "timestamp": now,
                },
            )
            assert resp.status_code == 200
            assert resp.json()["status"] == "ingested"
            assert resp.json()["sessionId"] == session_id

    conn = await asyncpg.connect(_db_url())
    try:
        legacy_row = await conn.fetchrow(
            """
            SELECT messages
            FROM session_buffer
            WHERE tenant_id = $1 AND session_id = $2
            """,
            tenant,
            session_id,
        )
        assert legacy_row is not None
        messages = legacy_row["messages"] or []
        if isinstance(messages, str):
            messages = json.loads(messages)
        assert isinstance(messages, list)
        assert len(messages) == 1
        assert messages[0]["text"] == "legacy path still active"

        transcript_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM session_transcript
            WHERE tenant_id = $1 AND session_id = $2
            """,
            tenant,
            session_id,
        )
        assert int(transcript_count or 0) == 0
    finally:
        await conn.close()
