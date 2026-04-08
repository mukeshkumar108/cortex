from datetime import datetime, date
from uuid import uuid4
import os
import json

import asyncpg
import pytest
from httpx import AsyncClient, ASGITransport

from src.main import app, graphiti_client
from src import loops as loops_module
from src.models import Loop


def _db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    password = os.getenv("POSTGRES_PASSWORD", "password")
    return f"postgresql://synapse:{password}@postgres:5432/synapse"


def _loop_item(*, text: str, loop_type: str = "thread", salience: int = 4, time_horizon: str = "ongoing") -> Loop:
    now = datetime.utcnow().isoformat() + "Z"
    return Loop(
        id=uuid4(),
        type=loop_type,
        status="active",
        text=text,
        confidence=0.8,
        salience=salience,
        timeHorizon=time_horizon,
        sourceTurnTs=now,
        dueDate=None,
        entityRefs=[],
        tags=[],
        createdAt=now,
        updatedAt=now,
        lastSeenAt=now,
        metadata={},
    )


@pytest.mark.asyncio
async def test_signals_pack_shape_and_class_caps(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    async def _stub_get_top_loops_for_startbrief(**_kwargs):
        return [
            _loop_item(text="Finish migration rollout", loop_type="commitment", time_horizon="today"),
            _loop_item(text="Resolve bug backlog", loop_type="thread", time_horizon="this_week"),
            _loop_item(text="Prepare roadmap update", loop_type="decision", time_horizon="ongoing"),
            _loop_item(text="Extra loop should be capped", loop_type="thread", time_horizon="ongoing"),
        ]

    async def _stub_recent_session_summary_nodes(**_kwargs):
        return [
            {
                "session_id": session_id,
                "summary_text": "User is pushing to complete reliability fixes and reduce churn.",
                "reference_time": now,
                "attributes": {
                    "tone": "direct",
                    "moment": "shipping sprint",
                },
            },
            {
                "session_id": session_id,
                "summary_text": "User wants clean handoff and fewer regressions this week.",
                "reference_time": now,
                "attributes": {
                    "tone": "focused",
                    "moment": "prioritization",
                },
            },
        ]

    monkeypatch.setattr(
        loops_module,
        "get_top_loops_for_startbrief",
        _stub_get_top_loops_for_startbrief,
        raising=True
    )
    monkeypatch.setattr(
        graphiti_client,
        "get_recent_session_summary_nodes",
        _stub_recent_session_summary_nodes,
        raising=True
    )

    conn = await asyncpg.connect(_db_url())
    try:
        await conn.execute(
            """
            INSERT INTO user_model (tenant_id, user_id, model, version, last_source)
            VALUES ($1, $2, $3::jsonb, 1, 'test')
            ON CONFLICT (tenant_id, user_id)
            DO UPDATE SET model = EXCLUDED.model, updated_at = NOW()
            """,
            tenant,
            user,
            json.dumps({
                "current_focus": {"text": "Ship reliability fixes", "confidence": 0.92},
                "north_star": {
                    "work": {"goal": "Ship stable releases", "goal_confidence": 0.85},
                    "general": {"vision": "Build resilient systems", "vision_confidence": 0.78},
                },
                "key_relationships": [
                    {"name": "Alex", "who": "manager", "status": "weekly sync", "confidence": 0.82},
                    {"name": "Nina", "who": "partner", "status": "trip planning", "confidence": 0.75},
                    {"name": "and", "who": "brother", "status": "active", "confidence": 0.9},
                ],
            }),
        )
        await conn.execute(
            """
            INSERT INTO daily_analysis (
                tenant_id, user_id, analysis_date, themes, scores, steering_note, confidence, source, metadata
            )
            VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7, 'test', '{}'::jsonb)
            ON CONFLICT (tenant_id, user_id, analysis_date)
            DO UPDATE SET themes = EXCLUDED.themes, steering_note = EXCLUDED.steering_note, confidence = EXCLUDED.confidence
            """,
            tenant,
            user,
            date.today(),
            json.dumps(["execution quality", "focus protection", "clear commitments"]),
            json.dumps({"clarity": 0.8}),
            "Keep scope narrow for this sprint.",
            0.8,
        )
    finally:
        await conn.close()

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            resp = await client.get(
                "/signals/pack",
                params={"tenantId": tenant, "userId": user, "sessionId": session_id}
            )
            assert resp.status_code == 200
            data = resp.json()

    assert isinstance(data.get("generated_at"), str)
    assert data.get("session_id") == session_id
    classes = data.get("classes") or {}
    expected_classes = {"identity", "trajectory", "today", "open_loops", "state", "relationships", "habits"}
    assert set(classes.keys()) == expected_classes
    for class_name in expected_classes:
        items = classes.get(class_name) or []
        assert isinstance(items, list)
        if class_name != "habits":
            assert len(items) <= 3
        for signal in items:
            assert set(signal.keys()).issuperset({
                "id", "class", "text", "confidence", "salience", "sensitivity", "recency_ts", "source"
            })
            assert signal["class"] == class_name

    debug = data.get("debug") or {}
    assert "counts" in debug
    assert "rejection_reasons" in debug
    relationship_texts = [s.get("text") for s in (classes.get("relationships") or [])]
    assert "and (brother): active" not in relationship_texts
    assert "Alex (manager): weekly sync" in relationship_texts


@pytest.mark.asyncio
async def test_signals_pack_high_sensitivity_sets_steer_only(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"

    async def _stub_get_top_loops_for_startbrief(**_kwargs):
        return [
            _loop_item(
                text="Discuss relapse risk and addiction triggers this week",
                loop_type="thread",
                time_horizon="today",
            )
        ]

    async def _stub_recent_session_summary_nodes(**_kwargs):
        return []

    monkeypatch.setattr(
        loops_module,
        "get_top_loops_for_startbrief",
        _stub_get_top_loops_for_startbrief,
        raising=True
    )
    monkeypatch.setattr(
        graphiti_client,
        "get_recent_session_summary_nodes",
        _stub_recent_session_summary_nodes,
        raising=True
    )

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            resp = await client.get(
                "/signals/pack",
                params={"tenantId": tenant, "userId": user}
            )
            assert resp.status_code == 200
            data = resp.json()

    open_loop_signals = (data.get("classes") or {}).get("open_loops") or []
    assert open_loop_signals
    high = next((s for s in open_loop_signals if s.get("sensitivity") == "HIGH"), None)
    assert high is not None
    assert high.get("surface_policy") == "steer_only"
    assert "Sensitive topic present" in str(high.get("text"))


@pytest.mark.asyncio
async def test_signals_pack_habits_include_today_log_status(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    persona = "persona-test"
    habit_id = uuid4()
    now = datetime.utcnow().isoformat() + "Z"

    async def _stub_get_top_loops_for_startbrief(**_kwargs):
        return []

    async def _stub_recent_session_summary_nodes(**_kwargs):
        return []

    monkeypatch.setattr(
        loops_module,
        "get_top_loops_for_startbrief",
        _stub_get_top_loops_for_startbrief,
        raising=True
    )
    monkeypatch.setattr(
        graphiti_client,
        "get_recent_session_summary_nodes",
        _stub_recent_session_summary_nodes,
        raising=True
    )

    async with app.router.lifespan_context(app):
        conn = await asyncpg.connect(_db_url())
        try:
            await conn.execute(
                """
                INSERT INTO loops (
                    id, tenant_id, user_id, persona_id, type, status, text,
                    confidence, salience, time_horizon, created_at, updated_at,
                    last_seen_at, hint, metadata
                )
                VALUES (
                    $1, $2, $3, $4, 'habit', 'active', $5,
                    0.93, 5, 'ongoing', NOW(), NOW(), NOW(), $6, '{}'::jsonb
                )
                """,
                habit_id,
                tenant,
                user,
                persona,
                "Walk 10k steps daily",
                "can be split across walks",
            )
            await conn.execute(
                """
                INSERT INTO habit_daily_log (user_id, habit_id, date, completed, nudged)
                VALUES ($1, $2, $3, FALSE, FALSE)
                ON CONFLICT (habit_id, date) DO NOTHING
                """,
                user,
                habit_id,
                datetime.utcnow().date(),
            )
        finally:
            await conn.close()

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test"
        ) as client:
            resp = await client.get(
                "/signals/pack",
                params={"tenantId": tenant, "userId": user, "now": now}
            )
            assert resp.status_code == 200
            data = resp.json()

    habits = ((data.get("classes") or {}).get("habits") or [])
    assert habits
    texts = [str(item.get("text")) for item in habits]
    assert any(
        "[habit] Walk 10k steps daily (ongoing) — can be split across walks | today: not yet | nudged: no | acknowledged: no" == text
        for text in texts
    )
