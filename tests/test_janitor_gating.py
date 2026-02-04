import asyncio
from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from httpx import AsyncClient, ASGITransport

from src.main import app, graphiti_client
from src import session as session_module


@pytest.mark.asyncio
async def test_janitor_gating_user_only_and_cooldown():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow()

    calls = {"count": 0}

    async def _stub_janitor(*_args, **_kwargs):
        calls["count"] += 1

    original_janitor = session_module.janitor_process
    original_add_episode = graphiti_client.add_episode
    session_module.janitor_process = _stub_janitor
    graphiti_client.add_episode = lambda **kwargs: {"success": False}

    try:
        async with app.router.lifespan_context(app):
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test"
            ) as client:
                for i in range(21):
                    role = "user" if i % 2 == 0 else "assistant"
                    ts = (now + timedelta(seconds=i)).isoformat() + "Z"
                    await client.post(
                        "/ingest",
                        json={
                            "tenantId": tenant,
                            "userId": user,
                            "personaId": "persona",
                            "sessionId": session_id,
                            "role": role,
                            "text": f"turn {i}",
                            "timestamp": ts
                        }
                    )
                    await asyncio.sleep(0)
    finally:
        session_module.janitor_process = original_janitor
        graphiti_client.add_episode = original_add_episode

    assert calls["count"] == 1
