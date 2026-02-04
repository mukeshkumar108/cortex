import asyncio
import json
import os
from datetime import datetime
from uuid import uuid4

import asyncpg
import pytest

from src.main import app
from src import loops as loops_module
from src.openrouter_client import get_llm_client


def _db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    password = os.getenv("POSTGRES_PASSWORD", "password")
    return f"postgresql://synapse:{password}@postgres:5432/synapse"


_ZERO_VECTOR_1536 = "[" + ",".join(["0"] * 1536) + "]"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "loop_type, due_date",
    [
        ("commitment", "2026-02-10"),
        ("friction", None),
        ("decision", None),
        ("habit", None),
        ("thread", None),
    ],
)
async def test_loop_extraction_types(loop_type, due_date):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    persona = "persona"
    now = datetime.utcnow()

    llm_client = get_llm_client()
    llm_client._call_llm = lambda *args, **kwargs: asyncio.sleep(
        0,
        result=json.dumps({
            "new_loops": [
                {
                    "type": loop_type,
                    "text": f"{loop_type} text",
                    "confidence": 0.8,
                    "salience": 3,
                    "time_horizon": "this_week",
                    "due_date": due_date,
                    "entity_refs": ["Ashley"],
                    "tags": ["work"],
                    "reason": "test"
                }
            ],
            "reinforced_loops": [],
            "completed_loops": [],
            "dropped_loops": []
        })
    )

    async with app.router.lifespan_context(app):
        if loops_module._manager is not None:
            async def _embed(_text):
                return _ZERO_VECTOR_1536
            loops_module._manager._generate_embedding = _embed
        await loops_module.extract_and_create_loops(
            tenant_id=tenant,
            user_id=user,
            persona_id=persona,
            user_text="test context",
            recent_turns=[{"role": "user", "text": "test context"}],
            source_turn_ts=now
        )

    conn = await asyncpg.connect(_db_url())
    try:
        row = await conn.fetchrow(
            """
            SELECT type, status, due_date, confidence, salience, time_horizon
            FROM loops
            WHERE tenant_id = $1 AND user_id = $2
            ORDER BY created_at DESC
            LIMIT 1
            """,
            tenant,
            user
        )
        assert row["type"] == loop_type
        assert row["status"] == "active"
        if loop_type == "commitment":
            assert row["due_date"] is not None
        else:
            assert row["due_date"] is None
        assert row["confidence"] >= 0.6
        assert row["salience"] == 3
        assert row["time_horizon"] == "this_week"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_loop_prompt_contains_rules_and_empty_example():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    persona = "persona"
    now = datetime.utcnow()
    captured = {}

    llm_client = get_llm_client()

    async def _capture(*args, **kwargs):
        captured["prompt"] = kwargs.get("prompt") or (args[0] if args else "")
        return json.dumps({
            "new_loops": [],
            "reinforced_loops": [],
            "completed_loops": [],
            "dropped_loops": []
        })

    async with app.router.lifespan_context(app):
        if loops_module._manager is not None:
            async def _embed(_text):
                return _ZERO_VECTOR_1536
            loops_module._manager._generate_embedding = _embed
        llm_client._call_llm = _capture
        await loops_module.extract_and_create_loops(
            tenant_id=tenant,
            user_id=user,
            persona_id=persona,
            user_text="test context",
            recent_turns=[{"role": "user", "text": "test context"}],
            source_turn_ts=now
        )

    prompt = captured.get("prompt", "")
    assert "\"new_loops\":[],\"reinforced_loops\":[],\"completed_loops\":[],\"dropped_loops\":[]" in prompt
    assert "Only create a new loop if it is" in prompt


@pytest.mark.asyncio
async def test_salience_bump_on_existing_loop():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    persona = "persona"
    now = datetime.utcnow()

    llm_client = get_llm_client()

    async with app.router.lifespan_context(app):
        if loops_module._manager is not None:
            async def _embed(_text):
                return _ZERO_VECTOR_1536
            loops_module._manager._generate_embedding = _embed
        llm_client._call_llm = lambda *args, **kwargs: asyncio.sleep(
            0,
            result=json.dumps({
                "new_loops": [
                    {
                        "type": "thread",
                        "text": "Project Apollo",
                        "confidence": 0.9,
                        "salience": 2,
                        "time_horizon": "ongoing",
                        "due_date": None,
                        "entity_refs": [],
                        "tags": [],
                        "reason": "project thread"
                    }
                ],
                "reinforced_loops": [],
                "completed_loops": [],
                "dropped_loops": []
            })
        )
        await loops_module.extract_and_create_loops(
            tenant_id=tenant,
            user_id=user,
            persona_id=persona,
            user_text="Project Apollo thread",
            recent_turns=[{"role": "user", "text": "Project Apollo thread"}],
            source_turn_ts=now
        )
        conn = await asyncpg.connect(_db_url())
        try:
            loop_id = await conn.fetchval(
                """
                SELECT id FROM loops
                WHERE tenant_id = $1 AND user_id = $2
                ORDER BY created_at DESC
                LIMIT 1
                """,
                tenant,
                user
            )
        finally:
            await conn.close()

        llm_client._call_llm = lambda *args, **kwargs: asyncio.sleep(
            0,
            result=json.dumps({
                "new_loops": [],
                "reinforced_loops": [
                    {"loop_id": str(loop_id), "confidence": 0.8, "reason": "mentioned again"}
                ],
                "completed_loops": [],
                "dropped_loops": []
            })
        )
        await loops_module.extract_and_create_loops(
            tenant_id=tenant,
            user_id=user,
            persona_id=persona,
            user_text="Project Apollo thread again",
            recent_turns=[{"role": "user", "text": "Project Apollo thread again"}],
            source_turn_ts=now
        )

    conn = await asyncpg.connect(_db_url())
    try:
        rows = await conn.fetch(
            """
            SELECT salience FROM loops
            WHERE tenant_id = $1 AND user_id = $2
            """,
            tenant,
            user
        )
        assert len(rows) == 1
        assert rows[0]["salience"] == 3
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_loop_completion_and_drop():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    persona = "persona"
    now = datetime.utcnow()

    async with app.router.lifespan_context(app):
        manager = loops_module._manager
        assert manager is not None
        async def _embed(_text):
            return _ZERO_VECTOR_1536
        async def _sim(*_args, **_kwargs):
            return 0.9
        manager._generate_embedding = _embed
        manager._check_similarity = _sim

        loop_id = await loops_module.create_loop(
            tenant_id=tenant,
            user_id=user,
            persona_id=persona,
            loop_type="commitment",
            text="Send the report",
            confidence=0.9,
            salience=3,
            time_horizon="today",
            source_turn_ts=now,
            due_date=None,
            entity_refs=[],
            tags=[],
            metadata={}
        )

        dropped_id = await loops_module.create_loop(
            tenant_id=tenant,
            user_id=user,
            persona_id=persona,
            loop_type="thread",
            text="Follow up with Ashley",
            confidence=0.9,
            salience=2,
            time_horizon="this_week",
            source_turn_ts=now,
            due_date=None,
            entity_refs=[],
            tags=[],
            metadata={}
        )
        llm_client = get_llm_client()
        llm_client._call_llm = lambda *args, **kwargs: asyncio.sleep(
            0,
            result=json.dumps({
                "new_loops": [],
                "reinforced_loops": [],
                "completed_loops": [
                    {
                        "loop_id": str(loop_id),
                        "confidence": 0.9,
                        "reason": "user confirmed done",
                        "evidence_text": "I finished it",
                        "evidence_type": "explicit"
                    }
                ],
                "dropped_loops": [
                    {"loop_id": str(dropped_id), "confidence": 0.8, "reason": "user canceled"}
                ]
            })
        )
        await loops_module.extract_and_create_loops(
            tenant_id=tenant,
            user_id=user,
            persona_id=persona,
            user_text="I did it, never mind the other",
            recent_turns=[{"role": "user", "text": "I did it, never mind the other"}],
            source_turn_ts=now
        )

    conn = await asyncpg.connect(_db_url())
    try:
        status = await conn.fetchval(
            """
            SELECT status FROM loops
            WHERE id = $1
            """,
            loop_id
        )
        assert status == "completed"
        dropped_status = await conn.fetchval(
            """
            SELECT status FROM loops
            WHERE id = $1
            """,
            dropped_id
        )
        assert dropped_status == "dropped"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_implicit_completion_creates_nudge_candidate():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    persona = "persona"
    now = datetime.utcnow()

    async with app.router.lifespan_context(app):
        manager = loops_module._manager
        assert manager is not None
        async def _embed(_text):
            return _ZERO_VECTOR_1536
        manager._generate_embedding = _embed

        loop_id = await loops_module.create_loop(
            tenant_id=tenant,
            user_id=user,
            persona_id=persona,
            loop_type="commitment",
            text="Send the report",
            confidence=0.9,
            salience=3,
            time_horizon="today",
            source_turn_ts=now,
            due_date=None,
            entity_refs=[],
            tags=[],
            metadata={}
        )

        llm_client = get_llm_client()
        llm_client._call_llm = lambda *args, **kwargs: asyncio.sleep(
            0,
            result=json.dumps({
                "new_loops": [],
                "reinforced_loops": [],
                "completed_loops": [
                    {
                        "loop_id": str(loop_id),
                        "confidence": 0.7,
                        "reason": "seems done",
                        "evidence_text": "I think it's done",
                        "evidence_type": "implicit"
                    }
                ],
                "dropped_loops": []
            })
        )
        await loops_module.extract_and_create_loops(
            tenant_id=tenant,
            user_id=user,
            persona_id=persona,
            user_text="I think I finished it",
            recent_turns=[{"role": "user", "text": "I think I finished it"}],
            source_turn_ts=now
        )

    conn = await asyncpg.connect(_db_url())
    try:
        row = await conn.fetchrow(
            """
            SELECT status, metadata
            FROM loops
            WHERE id = $1
            """,
            loop_id
        )
        assert row["status"] == "active"
        metadata = row["metadata"] or {}
        assert "pending_nudge" in metadata
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_low_confidence_completion_does_nothing():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    persona = "persona"
    now = datetime.utcnow()

    async with app.router.lifespan_context(app):
        manager = loops_module._manager
        assert manager is not None
        async def _embed(_text):
            return _ZERO_VECTOR_1536
        manager._generate_embedding = _embed

        loop_id = await loops_module.create_loop(
            tenant_id=tenant,
            user_id=user,
            persona_id=persona,
            loop_type="commitment",
            text="Send the report",
            confidence=0.9,
            salience=3,
            time_horizon="today",
            source_turn_ts=now,
            due_date=None,
            entity_refs=[],
            tags=[],
            metadata={}
        )

        llm_client = get_llm_client()
        llm_client._call_llm = lambda *args, **kwargs: asyncio.sleep(
            0,
            result=json.dumps({
                "new_loops": [],
                "reinforced_loops": [],
                "completed_loops": [
                    {
                        "loop_id": str(loop_id),
                        "confidence": 0.4,
                        "reason": "weak signal",
                        "evidence_text": "maybe",
                        "evidence_type": "explicit"
                    }
                ],
                "dropped_loops": []
            })
        )
        await loops_module.extract_and_create_loops(
            tenant_id=tenant,
            user_id=user,
            persona_id=persona,
            user_text="maybe done",
            recent_turns=[{"role": "user", "text": "maybe done"}],
            source_turn_ts=now
        )

    conn = await asyncpg.connect(_db_url())
    try:
        row = await conn.fetchrow(
            """
            SELECT status, metadata
            FROM loops
            WHERE id = $1
            """,
            loop_id
        )
        assert row["status"] == "active"
        metadata = row["metadata"] or {}
        assert "pending_nudge" not in metadata
    finally:
        await conn.close()
