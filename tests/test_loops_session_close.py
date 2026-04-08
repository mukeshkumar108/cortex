from datetime import datetime
from uuid import uuid4

import pytest
from httpx import AsyncClient, ASGITransport

from src.main import app, graphiti_client
from src import session as session_module


@pytest.mark.asyncio
async def test_session_close_enqueues_and_drain_executes_loop_hook(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    persona_id = "persona"
    now = datetime.utcnow().isoformat() + "Z"

    async def _stub_add_session_episode(**_kwargs):
        return {"success": True, "episode_uuid": "ep1"}

    graphiti_client.add_session_episode = _stub_add_session_episode

    captured = {}

    async def _stub_extract_and_create_loops(
        *,
        tenant_id,
        user_id,
        persona_id,
        user_text,
        recent_turns,
        source_turn_ts,
        session_id,
        provenance
    ):
        captured["tenant_id"] = tenant_id
        captured["user_id"] = user_id
        captured["persona_id"] = persona_id
        captured["user_text"] = user_text
        captured["recent_turns"] = recent_turns
        captured["source_turn_ts"] = source_turn_ts
        captured["session_id"] = session_id
        captured["provenance"] = provenance
        return {"new_loops": 0, "completions": 0}

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
            await client.post(
                "/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "personaId": persona_id,
                    "sessionId": session_id,
                    "role": "user",
                    "text": "I will finish the portfolio this week.",
                    "timestamp": now
                }
            )
            await client.post(
                "/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "personaId": persona_id,
                    "sessionId": session_id,
                    "role": "assistant",
                    "text": "Sounds good.",
                    "timestamp": now
                }
            )

            resp = await client.post(
                "/session/close",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "sessionId": session_id,
                    "personaId": persona_id
                }
            )
            assert resp.status_code == 200
            assert resp.json().get("closed") is True

        # /session/close itself must not run hooks synchronously.
        assert captured == {}

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

    assert captured["tenant_id"] == tenant
    assert captured["user_id"] == user
    assert captured["persona_id"] == "default"
    assert captured["session_id"] == session_id
    assert len(captured["recent_turns"]) == 2
    assert captured["user_text"]

    provenance = captured["provenance"]
    assert provenance["session_id"] == session_id
    assert provenance["start_ts"]
    assert provenance["end_ts"]

    start_ts = datetime.fromisoformat(provenance["start_ts"].replace("Z", "+00:00"))
    end_ts = datetime.fromisoformat(provenance["end_ts"].replace("Z", "+00:00"))
    assert start_ts <= end_ts
