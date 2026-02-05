from datetime import datetime
from uuid import uuid4

import pytest
from httpx import AsyncClient, ASGITransport

from src.main import app, graphiti_client


@pytest.mark.asyncio
async def test_session_ingest_calls_graphiti_once():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    called = {}

    async def _stub_add_session_episode(**kwargs):
        called.update(kwargs)
        return {"ok": True}

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
