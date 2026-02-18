from datetime import datetime, timedelta, timezone

import pytest
from httpx import AsyncClient, ASGITransport

from src.main import app, graphiti_client
from src import session as session_module
from src import loops as loops_module
from src.models import Loop


@pytest.mark.asyncio
async def test_session_startbrief_uses_loops_and_filters_summary(monkeypatch):
    now_dt = datetime(2026, 2, 6, 10, 15, tzinfo=timezone.utc)
    now = now_dt.isoformat().replace("+00:00", "Z")
    session_id = "session-test"

    async def _stub_latest_summary_node(*_args, **_kwargs):
        return {
            "summary": "I feel anxious about the demo. At the gym. Finish portfolio site this week.",
            "attributes": {"bridge_text": "Finish portfolio site this week."},
            "created_at": now
        }

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
                lastSeenAt=now,
                metadata={}
            )
        ]

    async def _stub_last_interaction_time(_tenant_id, _session_id):
        return now_dt - timedelta(hours=8)

    graphiti_client.get_latest_session_summary_node = _stub_latest_summary_node
    graphiti_client.search_nodes = _stub_search_nodes
    monkeypatch.setattr(
        session_module,
        "get_last_interaction_time",
        _stub_last_interaction_time,
        raising=True
    )
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
                params={
                    "tenantId": "t",
                    "userId": "u",
                    "now": now,
                    "sessionId": session_id,
                    "timezone": "America/Los_Angeles"
                }
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["bridgeText"]
            assert len(data["bridgeText"]) <= 280
            assert "gym" not in data["bridgeText"].lower()
            assert "feel" not in data["bridgeText"].lower()
            assert len(data["items"]) >= 1
            assert data["items"][0]["kind"] == "loop"
            assert data["items"][0]["lastSeenAt"] == now
            assert data["timeGapHuman"]
            assert data["timeOfDayLabel"] == "NIGHT"
