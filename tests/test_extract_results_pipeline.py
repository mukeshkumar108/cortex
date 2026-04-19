from datetime import datetime
import os
import json
from uuid import uuid4

import asyncpg
import pytest
from httpx import ASGITransport, AsyncClient

from src.config import get_settings
from src.extraction_results import ExtractionContractError, persist_extract_result
from src.main import app, graphiti_client
from src.predicate_policy import PredicatePolicyService
from src import session as session_module


def _db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    password = os.getenv("POSTGRES_PASSWORD", "password")
    return f"postgresql://synapse:{password}@postgres:5432/synapse"


@pytest.mark.asyncio
async def test_t4_pipeline_writes_extract_results_and_does_not_write_claims(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    settings = get_settings()
    monkeypatch.setattr(settings, "extract_results_enabled", True, raising=False)
    monkeypatch.setattr(settings, "extract_results_model_version", "t4-extractor-v1", raising=False)
    monkeypatch.setattr(settings, "extract_results_prompt_version", "t4-prompt-v1", raising=False)
    monkeypatch.setattr(settings, "extract_results_policy_version", None, raising=False)
    monkeypatch.setattr(settings, "v2_dual_write_enabled", True, raising=False)
    monkeypatch.setattr(settings, "v2_dual_write_fail_open", False, raising=False)

    async def _stub_add_session_episode(**_kwargs):
        return {"success": True, "episode_uuid": f"ep-{uuid4().hex}"}

    async def _stub_add_session_summary(**_kwargs):
        return {"success": True}

    async def _stub_extract_and_create_loops(**_kwargs):
        return {"new_loops": 0, "completions": 0}

    graphiti_client.add_session_episode = _stub_add_session_episode
    graphiti_client.add_session_summary = _stub_add_session_summary
    session_module.loops.extract_and_create_loops = _stub_extract_and_create_loops

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.post(
                "/session/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "sessionId": session_id,
                    "startedAt": now,
                    "endedAt": now,
                    "messages": [
                        {"role": "user", "text": "I am moving to Berlin", "timestamp": now},
                        {"role": "assistant", "text": "Noted, I will remember that", "timestamp": now},
                    ],
                },
            )
            assert resp.status_code == 200

        first = await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=30,
            tenant_id=tenant,
            budget_seconds=3.0,
            per_row_timeout_seconds=5.0,
        )
        assert first["sent"] >= 1
        second = await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=30,
            tenant_id=tenant,
            budget_seconds=3.0,
            per_row_timeout_seconds=5.0,
        )
        assert second["claimed"] >= 1

    conn = await asyncpg.connect(_db_url())
    try:
        row = await conn.fetchrow(
            """
            SELECT
              tenant_id, user_id, session_id,
              model_version, prompt_version, predicate_policy_version,
              status, candidates, error_text, started_at, completed_at
            FROM extract_results
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
            ORDER BY created_at DESC
            LIMIT 1
            """,
            tenant,
            user,
            session_id,
        )
        assert row is not None
        assert row["model_version"] == "t4-extractor-v1"
        assert row["prompt_version"] == "t4-prompt-v1"
        assert row["predicate_policy_version"]
        assert row["status"] == "succeeded"
        candidates = row["candidates"]
        if isinstance(candidates, str):
            candidates = json.loads(candidates)
        assert isinstance(candidates, dict)
        assert candidates.get("schema_version") == "t4.extract_results.v1"
        assert row["started_at"] is not None
        assert row["completed_at"] is not None
        assert row["error_text"] in (None, "")

        claim_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM claims
            WHERE tenant_id = $1 AND user_id = $2
            """,
            tenant,
            user,
        )
        assert int(claim_count or 0) == 0
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_t4_missing_policy_version_fails_closed():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    db = session_module._manager.db if session_module._manager is not None else None
    if db is None:
        async with app.router.lifespan_context(app):
            db = session_module._manager.db

    with pytest.raises(ExtractionContractError) as exc:
        await persist_extract_result(
            db=db,
            tenant_id=tenant,
            user_id=user,
            session_id=session_id,
            messages=[],
            extractor_model_version="t4-extractor-v1",
            prompt_version="t4-prompt-v1",
            policy_version="v2.missing",
            reference_time=datetime.utcnow(),
        )
    assert exc.value.code == "EXTRACT_UNKNOWN_POLICY_VERSION"


@pytest.mark.asyncio
async def test_t4_malformed_candidate_payload_captured_as_structured_failure():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"

    async with app.router.lifespan_context(app):
        db = session_module._manager.db
        policy_version = await PredicatePolicyService(db).get_current_policy_version()
        result = await persist_extract_result(
            db=db,
            tenant_id=tenant,
            user_id=user,
            session_id=session_id,
            messages=[],
            extractor_model_version="t4-extractor-v1",
            prompt_version="t4-prompt-v1",
            policy_version=policy_version,
            reference_time=datetime.utcnow(),
            candidate_payload="not-an-object",
        )
        assert result.status == "failed"
        assert result.deduped is False

    conn = await asyncpg.connect(_db_url())
    try:
        row = await conn.fetchrow(
            """
            SELECT status, error_text, raw_output, candidates
            FROM extract_results
            WHERE tenant_id = $1 AND extract_result_id = $2
            """,
            tenant,
            result.extract_result_id,
        )
        assert row is not None
        assert row["status"] == "failed"
        assert "validation error" in str(row["error_text"] or "")
        raw_output = row["raw_output"]
        candidates = row["candidates"]
        if isinstance(raw_output, str):
            raw_output = json.loads(raw_output)
        if isinstance(candidates, str):
            candidates = json.loads(candidates)
        assert isinstance(raw_output, dict)
        assert isinstance(candidates, dict)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_t4_duplicate_retry_behavior_is_deterministic():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    ts = datetime.utcnow().isoformat() + "Z"
    messages = [{"role": "user", "text": "same extraction payload", "timestamp": ts}]
    reference_time = datetime.utcnow()

    async with app.router.lifespan_context(app):
        db = session_module._manager.db
        policy_version = await PredicatePolicyService(db).get_current_policy_version()
        first = await persist_extract_result(
            db=db,
            tenant_id=tenant,
            user_id=user,
            session_id=session_id,
            messages=messages,
            extractor_model_version="t4-extractor-v1",
            prompt_version="t4-prompt-v1",
            policy_version=policy_version,
            reference_time=reference_time,
        )
        second = await persist_extract_result(
            db=db,
            tenant_id=tenant,
            user_id=user,
            session_id=session_id,
            messages=messages,
            extractor_model_version="t4-extractor-v1",
            prompt_version="t4-prompt-v1",
            policy_version=policy_version,
            reference_time=reference_time,
        )
        assert first.extract_run_id == second.extract_run_id
        assert second.deduped is True

    conn = await asyncpg.connect(_db_url())
    try:
        count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM extract_results
            WHERE tenant_id = $1 AND extract_run_id = $2::uuid
            """,
            tenant,
            first.extract_run_id,
        )
        assert int(count or 0) == 1
    finally:
        await conn.close()
