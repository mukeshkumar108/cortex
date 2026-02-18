import pytest
from httpx import AsyncClient, ASGITransport

from src.main import app
from src import loops as loops_module
from src import config as config_module
import os


@pytest.mark.asyncio
async def test_internal_debug_loops_json(monkeypatch):
    token = "test-token"
    os.environ["INTERNAL_TOKEN"] = token
    config_module.get_settings.cache_clear()

    async def _stub_get_active_loops_debug(tenant_id, user_id):
        return [
            {"id": "loop-1", "text": "Finish portfolio", "status": "active"},
            {"id": "loop-2", "text": "Fix flaky tests", "status": "active"}
        ]

    monkeypatch.setattr(
        loops_module,
        "get_active_loops_debug",
        _stub_get_active_loops_debug,
        raising=True
    )

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            resp = await client.get(
                "/internal/debug/loops",
                params={"tenantId": "t", "userId": "u"},
                headers={"X-Internal-Token": token}
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["count"] == 2
            assert data["rows"][0]["id"] == "loop-1"


@pytest.mark.asyncio
async def test_internal_debug_loops_csv(monkeypatch):
    token = "test-token"
    os.environ["INTERNAL_TOKEN"] = token
    config_module.get_settings.cache_clear()

    async def _stub_get_active_loops_debug(tenant_id, user_id):
        return [
            {"id": "loop-1", "text": "Finish portfolio", "status": "active"}
        ]

    monkeypatch.setattr(
        loops_module,
        "get_active_loops_debug",
        _stub_get_active_loops_debug,
        raising=True
    )

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            resp = await client.get(
                "/internal/debug/loops",
                params={"tenantId": "t", "userId": "u", "format": "csv"},
                headers={"X-Internal-Token": token}
            )
            assert resp.status_code == 200
            assert resp.headers["content-type"].startswith("text/csv")
            text = resp.text
            assert "id" in text
            assert "loop-1" in text
