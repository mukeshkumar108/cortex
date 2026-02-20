from datetime import datetime

import pytest
from httpx import AsyncClient, ASGITransport

from src.main import app


@pytest.mark.asyncio
async def test_get_daily_analysis_empty(monkeypatch):
    async def _stub_fetchone(*_args, **_kwargs):
        return None

    monkeypatch.setattr("src.main.db.fetchone", _stub_fetchone, raising=False)

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            resp = await client.get(
                "/analysis/daily",
                params={"tenantId": "t", "userId": "u"}
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["exists"] is False
            assert data["themes"] == []
            assert data["scores"] == {}


@pytest.mark.asyncio
async def test_get_daily_analysis_with_row(monkeypatch):
    async def _stub_fetchone(*_args, **_kwargs):
        return {
            "analysis_date": datetime(2026, 2, 18).date(),
            "themes": ["Emotional pressure and regulation"],
            "scores": {"curiosity": 4, "warmth": 4, "usefulness": 3, "forward_motion": 3},
            "steering_note": "Lead with curiosity then one concrete next step.",
            "confidence": 0.74,
            "metadata": {"analysis_version": "v1"},
            "created_at": datetime(2026, 2, 19, 0, 5, 0),
            "updated_at": datetime(2026, 2, 19, 0, 5, 0),
        }

    monkeypatch.setattr("src.main.db.fetchone", _stub_fetchone, raising=False)

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            resp = await client.get(
                "/analysis/daily",
                params={"tenantId": "t", "userId": "u"}
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["exists"] is True
            assert data["analysisDate"] == "2026-02-18"
            assert data["steeringNote"]
            assert data["scores"]["curiosity"] == 4
