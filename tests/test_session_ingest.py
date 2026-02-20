from datetime import datetime
from uuid import uuid4

import pytest
from httpx import AsyncClient, ASGITransport

from src.main import app, graphiti_client
from src import main as main_module
from src import session as session_module


@pytest.mark.asyncio
async def test_session_ingest_calls_graphiti_once(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    called = {}

    async def _stub_add_session_episode(**kwargs):
        called.update(kwargs)
        return {"ok": True, "episode_uuid": "ep1"}

    async def _stub_summarize_messages(_messages):
        return {"summary_text": "User said hello.", "bridge_text": "User said hello."}

    async def _stub_add_session_summary(**kwargs):
        called["summary"] = kwargs

    async def _stub_extract_and_create_loops(**kwargs):
        called["loops"] = kwargs
        return {"new_loops": 1, "completions": 0}

    graphiti_client.add_session_episode = _stub_add_session_episode
    graphiti_client.add_session_summary = _stub_add_session_summary
    session_module.summarize_session_messages = _stub_summarize_messages
    monkeypatch.setattr(
        main_module.loops,
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
                    "personaId": "persona",
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

    assert "messages" in called
    transcript = graphiti_client._format_message_transcript(called["messages"])
    assert "User: My name is Mukesh" in transcript
    assert "Assistant: Nice to meet you" in transcript
    assert called.get("summary", {}).get("summary_text") == "User said hello."
    assert called.get("loops", {}).get("session_id") == session_id
    assert called.get("loops", {}).get("persona_id") == "default"
    assert called.get("loops", {}).get("provenance", {}).get("session_id") == session_id


@pytest.mark.asyncio
async def test_session_ingest_uses_default_persona_when_missing(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    called = {}

    async def _stub_add_session_episode(**kwargs):
        called.update(kwargs)
        return {"ok": True, "episode_uuid": "ep1"}

    async def _stub_summarize_messages(_messages):
        return {"summary_text": "Quick check-in.", "bridge_text": "Continue from this check-in."}

    async def _stub_add_session_summary(**kwargs):
        called["summary"] = kwargs

    async def _stub_extract_and_create_loops(**kwargs):
        called["loops"] = kwargs
        return {"new_loops": 0, "completions": 0}

    graphiti_client.add_session_episode = _stub_add_session_episode
    graphiti_client.add_session_summary = _stub_add_session_summary
    session_module.summarize_session_messages = _stub_summarize_messages
    monkeypatch.setattr(
        main_module.loops,
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
                        {"role": "user", "text": "Hello", "timestamp": now}
                    ]
                }
            )
            assert resp.status_code == 200

    assert called.get("loops", {}).get("persona_id") == "default"
