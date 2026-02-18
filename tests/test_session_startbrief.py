from datetime import datetime

import pytest
from httpx import AsyncClient, ASGITransport

from src.main import app, graphiti_client
from src import loops as loops_module
from src.models import Loop


@pytest.mark.asyncio
async def test_session_startbrief_uses_loops_and_filters_summary(monkeypatch):
    now = datetime.utcnow().isoformat() + "Z"

    async def _stub_recent_summaries(*_args, **_kwargs):
        return [{
            "summary": "I feel anxious about the demo. At the gym. Finish portfolio site this week.",
            "reference_time": now
        }]

    async def _stub_search_nodes(*_args, **_kwargs):
        return [{
            "type": "Tension",
            "summary": "Flaky tests in release pipeline",
            "attributes": {"status": "unresolved", "description": "Flaky tests in release pipeline"}
        }]

    async def _stub_get_top_loops(*_args, **_kwargs):
        return [
            Loop(
                id="00000000-0000-0000-0000-000000000001",
                type="thread",
                status="active",
                text="Finish portfolio site",
                confidence=0.8,
                salience=4,
                timeHorizon="this_week",
                sourceTurnTs=None,
                dueDate=None,
                entityRefs=[],
                tags=[],
                createdAt=now,
                updatedAt=None,
                lastSeenAt=None,
                metadata={}
            )
        ]

    graphiti_client.get_recent_episode_summaries = _stub_recent_summaries
    graphiti_client.search_nodes = _stub_search_nodes
    monkeypatch.setattr(
        loops_module,
        "get_top_loops_for_startbrief",
        _stub_get_top_loops,
        raising=True
    )

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            resp = await client.get(
                "/session/startbrief",
                params={"tenantId": "t", "userId": "u", "now": now}
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["bridgeText"]
            assert len(data["bridgeText"]) <= 280
            assert "gym" not in data["bridgeText"].lower()
            assert "feel" not in data["bridgeText"].lower()
            assert len(data["items"]) >= 1
            assert data["items"][0]["kind"] == "loop"
