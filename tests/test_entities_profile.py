from datetime import datetime, timezone

import pytest
from httpx import ASGITransport, AsyncClient

from src.main import app, graphiti_client
from src import loops as loops_module


@pytest.mark.asyncio
async def test_entities_profile_resolves_canonical_graphiti_entity_first(monkeypatch):
    reference_time = datetime(2026, 4, 9, 9, 0, tzinfo=timezone.utc)
    now = reference_time.isoformat().replace("+00:00", "Z")

    async def _stub_resolve_canonical_entity_candidate(*_args, **_kwargs):
        return {
            "entityId": "p1",
            "name": "Ashley",
            "type": "person",
            "role": "girlfriend",
            "importance": 0.91,
            "salience": 0.88,
            "lastSeenAt": now,
            "raw": {
                "attributes": {
                    "aliases": ["Ash"],
                }
            },
        }

    async def _stub_search_facts(**_kwargs):
        return [
            {"text": "Ashley is the user's girlfriend.", "relevance": 0.91, "valid_at": now},
            {"text": "Ashley and the user discussed relationship repair.", "relevance": 0.82, "valid_at": now},
        ]

    async def _stub_get_top_loops_for_startbrief(*_args, **_kwargs):
        class _Loop:
            id = "l1"
            type = "thread"
            text = "Repair relationship with Ashley"
            status = "active"
            salience = 7
        return [_Loop()]

    async def _stub_fetch_user_model_rows_for_scope(*_args, **_kwargs):
        return [{"model": {"key_relationships": [{"name": "Ashley", "who": "girlfriend"}]}}]

    monkeypatch.setattr("src.main._resolve_canonical_entity_candidate", _stub_resolve_canonical_entity_candidate, raising=True)
    graphiti_client.search_facts = _stub_search_facts
    monkeypatch.setattr(loops_module, "get_top_loops_for_startbrief", _stub_get_top_loops_for_startbrief, raising=True)
    monkeypatch.setattr("src.main._fetch_user_model_rows_for_scope", _stub_fetch_user_model_rows_for_scope, raising=True)

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post(
                "/entities/profile",
                json={
                    "tenantId": "default",
                    "userId": "u1",
                    "name": "Ashley",
                    "referenceTime": now,
                },
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["entity"]["canonicalName"] == "Ashley"
            assert data["entity"]["type"] == "person"
            assert data["entity"]["aliases"] == ["Ash"]
            assert data["entity"]["role"] == "girlfriend"
            assert data["openLoops"][0]["text"].lower().startswith("repair relationship with ashley")
            assert "graphiti_nodes" in data["provenance"]["sources"]


@pytest.mark.asyncio
async def test_entities_profile_returns_404_when_graphiti_canonical_entity_missing(monkeypatch):
    async def _stub_resolve_canonical_entity_candidate(*_args, **_kwargs):
        return None

    async def _stub_fetch_user_model_rows_for_scope(*_args, **_kwargs):
        return []

    monkeypatch.setattr("src.main._resolve_canonical_entity_candidate", _stub_resolve_canonical_entity_candidate, raising=True)
    monkeypatch.setattr("src.main._fetch_user_model_rows_for_scope", _stub_fetch_user_model_rows_for_scope, raising=True)

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post(
                "/entities/profile",
                json={
                    "tenantId": "default",
                    "userId": "u1",
                    "name": "Bluum",
                },
            )
            assert resp.status_code == 404
