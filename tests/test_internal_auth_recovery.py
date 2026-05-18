from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from httpx import ASGITransport, AsyncClient

from src import main as main_module
from src import loops as loops_module
from src.models import (
    Fact,
    Loop,
    MemoryQueryResponse,
)


REAL_REQUIRE_INTERNAL_TOKEN = main_module._require_internal_token
TEST_TOKEN = "test-internal-token"


def _headers(token: str | None) -> dict[str, str]:
    if token is None:
        return {}
    return {"x-internal-token": token}


@pytest.fixture
def _enforce_internal_auth(monkeypatch):
    settings = main_module.get_settings()
    previous = settings.internal_token
    settings.internal_token = TEST_TOKEN
    monkeypatch.setattr(main_module, "_require_internal_token", REAL_REQUIRE_INTERNAL_TOKEN, raising=True)
    try:
        yield
    finally:
        settings.internal_token = previous


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "path", "kwargs"),
    [
        (
            "post",
            "/session/ingest",
            {
                "json": {
                    "tenantId": "default",
                    "userId": "u1",
                    "sessionId": "s1",
                    "messages": [{"role": "user", "text": "hi", "timestamp": "2026-05-18T10:00:00Z"}],
                }
            },
        ),
        (
            "get",
            "/session/startbrief",
            {"params": {"tenantId": "default", "userId": "u1", "now": "2026-05-18T10:00:00Z", "timezone": "UTC"}},
        ),
        (
            "post",
            "/memory/query",
            {"json": {"tenantId": "default", "userId": "u1", "query": "focus", "limit": 3}},
        ),
        (
            "get",
            "/signals/pack",
            {"params": {"tenantId": "default", "userId": "u1"}},
        ),
    ],
)
async def test_internal_auth_rejects_missing_and_wrong_tokens(_enforce_internal_auth, method, path, kwargs):
    async with AsyncClient(transport=ASGITransport(app=main_module.app), base_url="http://test") as client:
        missing = await getattr(client, method)(path, **kwargs)
        wrong = await getattr(client, method)(path, headers=_headers("wrong-token"), **kwargs)

    assert missing.status_code == 401
    assert missing.json() == {"detail": "Unauthorized"}
    assert wrong.status_code == 401
    assert wrong.json() == {"detail": "Unauthorized"}


@pytest.mark.asyncio
async def test_session_ingest_accepts_valid_internal_token(_enforce_internal_auth, monkeypatch):
    calls: list[dict[str, object]] = []

    async def _stub_enqueue_session_ingest(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(main_module.session, "enqueue_session_ingest", _stub_enqueue_session_ingest, raising=True)

    payload = {
        "tenantId": "default",
        "userId": "u1",
        "sessionId": "s1",
        "startedAt": "2026-05-18T10:00:00Z",
        "endedAt": "2026-05-18T10:01:00Z",
        "messages": [{"role": "user", "text": "hello", "timestamp": "2026-05-18T10:00:00Z"}],
    }

    async with AsyncClient(transport=ASGITransport(app=main_module.app), base_url="http://test") as client:
        response = await client.post("/session/ingest", json=payload, headers=_headers(TEST_TOKEN))

    assert response.status_code == 200
    assert response.json() == {"status": "ingested", "sessionId": "s1", "graphitiAdded": False}
    assert len(calls) == 1
    assert calls[0]["tenant_id"] == "default"


@pytest.mark.asyncio
async def test_session_startbrief_accepts_valid_internal_token(_enforce_internal_auth, monkeypatch):
    now_dt = datetime(2026, 5, 18, 10, 0, tzinfo=timezone.utc)
    now_iso = now_dt.isoformat().replace("+00:00", "Z")
    summary = {
        "session_id": "s1",
        "created_at": (now_dt - timedelta(minutes=10)).isoformat().replace("+00:00", "Z"),
        "summary_text": "They narrowed the next concrete step.",
        "bridge_text": "They just narrowed the next step.",
        "attributes": {
            "session_id": "s1",
            "summary_facts": "They narrowed the next concrete step.",
            "tone": "steady",
            "moment": "clear next step",
            "decisions": ["Continue the thread"],
            "unresolved": ["Confirm progress next time"],
            "salience": "high",
            "reference_time": (now_dt - timedelta(minutes=10)).isoformat().replace("+00:00", "Z"),
        },
    }

    async def _stub_latest_summary_node(*_args, **_kwargs):
        return summary

    async def _stub_recent_session_summary_nodes(*_args, **_kwargs):
        return [summary]

    async def _stub_search_nodes(*_args, **_kwargs):
        return []

    async def _stub_get_top_loops(*_args, **_kwargs):
        return [
            Loop(
                id="00000000-0000-0000-0000-000000000001",
                type="thread",
                status="active",
                text="Finish the release follow-up today",
                confidence=0.8,
                salience=5,
                timeHorizon="today",
                sourceTurnTs=None,
                dueDate=None,
                entityRefs=[],
                tags=[],
                createdAt=now_iso,
                updatedAt=None,
                lastSeenAt=now_iso,
                metadata={},
            )
        ]

    async def _stub_db_fetchone(query, *_args, **_kwargs):
        if "FROM session_transcript" in query:
            return {"messages": [{"role": "user", "text": "last", "timestamp": (now_dt - timedelta(minutes=10)).isoformat()}]}
        if "FROM daily_analysis" in query:
            return None
        if "FROM user_model" in query:
            return None
        return None

    async def _stub_log_history(*_args, **_kwargs):
        return None

    async def _stub_build_entity_hints(*_args, **_kwargs):
        return []

    async def _stub_bridge_llm(*_args, **_kwargs):
        return "They had just tightened the same thread and kept momentum."

    monkeypatch.setattr(main_module.graphiti_client, "get_latest_session_summary_node", _stub_latest_summary_node, raising=True)
    monkeypatch.setattr(main_module.graphiti_client, "get_recent_session_summary_nodes", _stub_recent_session_summary_nodes, raising=True)
    monkeypatch.setattr(main_module.graphiti_client, "search_nodes", _stub_search_nodes, raising=True)
    monkeypatch.setattr(loops_module, "get_top_loops_for_startbrief", _stub_get_top_loops, raising=True)
    monkeypatch.setattr("src.main.db.fetchone", _stub_db_fetchone, raising=False)
    monkeypatch.setattr(main_module, "_log_startbrief_history", _stub_log_history, raising=True)
    monkeypatch.setattr(main_module, "_build_startbrief_entity_hints_from_profiles", _stub_build_entity_hints, raising=True)
    monkeypatch.setattr(main_module, "_generate_startbrief_bridge_llm", _stub_bridge_llm, raising=True)

    async with AsyncClient(transport=ASGITransport(app=main_module.app), base_url="http://test") as client:
        response = await client.get(
            "/session/startbrief",
            params={"tenantId": "default", "userId": "u1", "now": now_iso, "sessionId": "s1", "timezone": "UTC"},
            headers=_headers(TEST_TOKEN),
        )

    assert response.status_code == 200
    data = response.json()
    assert data["handover_depth"] in {"continuation", "today", "multi_day", "fresh"}
    assert isinstance(data["resume"], dict)
    assert data["handover_text"]


@pytest.mark.asyncio
async def test_memory_query_accepts_valid_internal_token(_enforce_internal_auth, monkeypatch):
    async def _stub_route(self, *, tenant_id: str, user_id: str):
        return "legacy"

    async def _stub_memory_adapter(request):
        return MemoryQueryResponse(
            facts=["Stay on the release thread."],
            factItems=[Fact(text="Stay on the release thread.", relevance=0.9, source="test")],
            entities=[],
            episodes=[],
            metadata={"responseMode": "recall"},
        )

    monkeypatch.setattr(main_module.RolloutController, "decide_route", _stub_route, raising=True)
    monkeypatch.setattr(main_module, "_memory_query_legacy_adapter", _stub_memory_adapter, raising=True)

    async with AsyncClient(transport=ASGITransport(app=main_module.app), base_url="http://test") as client:
        response = await client.post(
            "/memory/query",
            json={"tenantId": "default", "userId": "u1", "query": "focus", "limit": 3},
            headers=_headers(TEST_TOKEN),
        )

    assert response.status_code == 200
    assert response.json()["facts"] == ["Stay on the release thread."]


@pytest.mark.asyncio
async def test_signals_pack_accepts_valid_internal_token(_enforce_internal_auth, monkeypatch):
    async def _stub_build_signals_pack(**kwargs):
        return {
            "generated_at": "2026-05-18T10:00:00Z",
            "tenant_id": kwargs["tenantId"],
            "user_id": kwargs["userId"],
            "session_id": kwargs["sessionId"],
            "classes": {"identity": [], "trajectory": [], "today": [], "open_loops": [], "state": [], "relationships": [], "habits": []},
            "debug": {"counts": {}, "rejection_reasons": {}},
        }

    monkeypatch.setattr(main_module, "_build_signals_pack", _stub_build_signals_pack, raising=True)

    async with AsyncClient(transport=ASGITransport(app=main_module.app), base_url="http://test") as client:
        response = await client.get(
            "/signals/pack",
            params={"tenantId": "default", "userId": "u1", "sessionId": "s1"},
            headers=_headers(TEST_TOKEN),
        )

    assert response.status_code == 200
    assert response.json()["session_id"] == "s1"
