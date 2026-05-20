from datetime import datetime, timezone
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from src.briefing import build_briefing
from src.config import get_settings
from src.main import app, graphiti_client
from src import loops as loops_module
from src import main as main_module
from src import session as session_module


def _set_graphiti_enabled(monkeypatch, enabled: bool) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "graphiti_enabled", enabled, raising=False)
    monkeypatch.setattr(settings, "background_loops_enabled", False, raising=False)
    monkeypatch.setattr(graphiti_client.settings, "graphiti_enabled", enabled, raising=False)
    monkeypatch.setattr(graphiti_client.settings, "background_loops_enabled", False, raising=False)
    graphiti_client._initialized = False
    graphiti_client.client = None


def _maybe_json(value):
    if isinstance(value, str):
        return json.loads(value)
    return value


async def _enqueue_session_ingest(client: AsyncClient, *, tenant: str, user: str, session_id: str, now: str) -> None:
    resp = await client.post(
        "/session/ingest",
        json={
            "tenantId": tenant,
            "userId": user,
            "sessionId": session_id,
            "startedAt": now,
            "endedAt": now,
            "messages": [
                {"role": "user", "text": "I committed to ship the fix tomorrow and still need to confirm QA.", "timestamp": now},
                {"role": "assistant", "text": "You plan to ship tomorrow and need a QA confirmation.", "timestamp": now},
            ],
        },
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_session_summary_pipeline_writes_postgres_and_keeps_outbox_flow(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    called = {"graphiti_summary": 0, "graphiti_episode": 0, "pass1": 0}

    _set_graphiti_enabled(monkeypatch, False)

    async def _stub_summarize(_messages):
        return {
            "summary_text": "They committed to ship the fix tomorrow and still need QA confirmation.",
            "bridge_text": "Next time, confirm QA and ship the fix.",
            "summary_facts": "They committed to ship the fix tomorrow.",
            "tone": "steady",
            "moment": "They narrowed the final blocker.",
            "decisions": ["Ship the fix tomorrow"],
            "unresolved": ["Confirm QA"],
            "index_text": "Ship the fix tomorrow. Confirm QA.",
            "salience": "high",
            "summary_quality_tier": "test",
            "summary_source": "unit_test",
        }

    async def _stub_extract_and_create_loops(**_kwargs):
        return {"new_loops": 0}

    async def _stub_persist_extract_result(**_kwargs):
        return SimpleNamespace(extract_result_id=1, deduped=True)

    async def _stub_run_pass1_triage(**kwargs):
        called["pass1"] += 1
        await kwargs["db"].execute(
            """
            INSERT INTO session_classifications (
              session_id, user_id, session_date, is_memory_worthy, session_kind,
              one_line_summary, entity_mentions, run_entity_pass, run_threads_pass,
              identity_relevant, emotional_weight, emotional_note, processed_at, model_used
            )
            VALUES ($1, $2, NOW(), true, 'conversation', $3, ARRAY[]::text[], false, false, false, 'steady', 'focused', NOW(), 'test')
            ON CONFLICT (session_id)
            DO UPDATE SET
              one_line_summary = EXCLUDED.one_line_summary,
              processed_at = NOW()
            """,
            kwargs["session_id"],
            kwargs["user_id"],
            "They committed to ship the fix tomorrow.",
        )
        return SimpleNamespace(
            run_actionable_pass=False,
            run_session_changes_pass=False,
            run_entity_pass=False,
            run_threads_pass=False,
            should_run_identity=False,
            should_run_living_context=False,
        )

    async def _unexpected_graphiti_episode(**_kwargs):
        called["graphiti_episode"] += 1
        raise AssertionError("Graphiti episode sink should not run when disabled")

    async def _unexpected_graphiti_summary(**_kwargs):
        called["graphiti_summary"] += 1
        raise AssertionError("Graphiti summary sink should not run when disabled")

    monkeypatch.setattr(session_module, "summarize_session_messages", _stub_summarize, raising=True)
    monkeypatch.setattr(session_module.loops, "extract_and_create_loops", _stub_extract_and_create_loops, raising=False)
    monkeypatch.setattr(main_module, "persist_extract_result", _stub_persist_extract_result, raising=True)
    monkeypatch.setattr(main_module, "run_pass1_triage", _stub_run_pass1_triage, raising=True)
    monkeypatch.setattr(graphiti_client, "add_session_episode", _unexpected_graphiti_episode, raising=True)
    monkeypatch.setattr(graphiti_client, "add_session_summary", _unexpected_graphiti_summary, raising=True)

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            await _enqueue_session_ingest(client, tenant=tenant, user=user, session_id=session_id, now=now)

        await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=20,
            tenant_id=tenant,
            budget_seconds=5.0,
            per_row_timeout_seconds=5.0,
        )
        await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=20,
            tenant_id=tenant,
            budget_seconds=5.0,
            per_row_timeout_seconds=5.0,
        )

        summary_row = await main_module.db.fetchone(
            """
            SELECT summary_text, bridge_text, decisions, unresolved_items, extra_attributes
            FROM session_summaries
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
            """,
            tenant,
            user,
            session_id,
        )
        assert summary_row is not None
        decisions = _maybe_json(summary_row["decisions"])
        unresolved_items = _maybe_json(summary_row["unresolved_items"])
        extra_attributes = _maybe_json(summary_row["extra_attributes"])
        assert summary_row["summary_text"] == "They committed to ship the fix tomorrow and still need QA confirmation."
        assert summary_row["bridge_text"] == "Next time, confirm QA and ship the fix."
        assert decisions == ["Ship the fix tomorrow"]
        assert unresolved_items == ["Confirm QA"]
        assert extra_attributes["summary_quality_tier"] == "test"
        assert extra_attributes["index_text"] == "Ship the fix tomorrow. Confirm QA."

        classification_row = await main_module.db.fetchone(
            """
            SELECT one_line_summary
            FROM session_classifications
            WHERE session_id = $1
            """,
            session_id,
        )
        assert classification_row is not None
        assert "ship the fix tomorrow" in classification_row["one_line_summary"].lower()

        outbox_rows = await main_module.db.fetch(
            """
            SELECT job_type, status
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
            ORDER BY id ASC
            """,
            tenant,
            user,
            session_id,
        )
        assert outbox_rows
        assert all(row["status"] == "sent" for row in outbox_rows)

    assert called["graphiti_summary"] == 0
    assert called["graphiti_episode"] == 0
    assert called["pass1"] == 1


@pytest.mark.asyncio
async def test_summary_hook_postgres_failure_leaves_job_retryable_until_sink_succeeds(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

    _set_graphiti_enabled(monkeypatch, False)
    async def _stub_summarize(_messages):
        return {
            "summary_text": "They are ready to ship.",
            "bridge_text": "Confirm the release next session.",
            "summary_facts": "They are ready to ship.",
            "tone": "steady",
            "moment": "Release is close.",
            "decisions": ["Ship soon"],
            "unresolved": ["Confirm QA"],
            "index_text": "They are ready to ship.",
            "salience": "high",
        }

    async def _stub_extract_and_create_loops(**_kwargs):
        return {"new_loops": 0}

    async def _stub_persist_extract_result(**_kwargs):
        return SimpleNamespace(extract_result_id=1, deduped=True)

    async def _stub_run_pass1_triage(**_kwargs):
        return SimpleNamespace(
            run_actionable_pass=False,
            run_session_changes_pass=False,
            run_entity_pass=False,
            run_threads_pass=False,
            should_run_identity=False,
            should_run_living_context=False,
        )

    monkeypatch.setattr(session_module, "summarize_session_messages", _stub_summarize, raising=True)
    monkeypatch.setattr(session_module.loops, "extract_and_create_loops", _stub_extract_and_create_loops, raising=False)
    monkeypatch.setattr(main_module, "persist_extract_result", _stub_persist_extract_result, raising=True)
    monkeypatch.setattr(main_module, "run_pass1_triage", _stub_run_pass1_triage, raising=True)

    failure_state = {"calls": 0}

    original_upsert = main_module.upsert_session_summary

    async def _wrapped_upsert(*args, **kwargs):
        failure_state["calls"] += 1
        if failure_state["calls"] == 1:
            raise RuntimeError("postgres sink down")
        return await original_upsert(*args, **kwargs)

    monkeypatch.setattr(main_module, "upsert_session_summary", _wrapped_upsert, raising=True)

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            await _enqueue_session_ingest(client, tenant=tenant, user=user, session_id=session_id, now=now)

        await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=20,
            tenant_id=tenant,
            budget_seconds=5.0,
            per_row_timeout_seconds=5.0,
        )
        await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=20,
            tenant_id=tenant,
            budget_seconds=5.0,
            per_row_timeout_seconds=5.0,
        )

        hook_row = await main_module.db.fetchone(
            """
            SELECT status, sent_at, last_error
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
              AND job_type = 'post_ingest_hook'
              AND text = 'post_ingest_hook:session_summary'
            ORDER BY id DESC
            LIMIT 1
            """,
            tenant,
            user,
            session_id,
        )
        assert hook_row is not None
        assert hook_row["status"] == "pending"
        assert hook_row["sent_at"] is None
        assert "postgres sink down" in (hook_row["last_error"] or "")

        summary_count = await main_module.db.fetchval(
            """
            SELECT COUNT(*)
            FROM session_summaries
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
            """,
            tenant,
            user,
            session_id,
        )
        assert int(summary_count or 0) == 0

        await main_module.db.execute(
            """
            UPDATE graphiti_outbox
            SET next_attempt_at = NOW() - INTERVAL '1 second'
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
              AND job_type = 'post_ingest_hook'
              AND text = 'post_ingest_hook:session_summary'
            """,
            tenant,
            user,
            session_id,
        )

        await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=20,
            tenant_id=tenant,
            budget_seconds=5.0,
            per_row_timeout_seconds=5.0,
        )

        hook_row = await main_module.db.fetchone(
            """
            SELECT status, sent_at
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
              AND job_type = 'post_ingest_hook'
              AND text = 'post_ingest_hook:session_summary'
            ORDER BY id DESC
            LIMIT 1
            """,
            tenant,
            user,
            session_id,
        )
        assert hook_row is not None
        assert hook_row["status"] == "sent"
        assert hook_row["sent_at"] is not None


@pytest.mark.asyncio
async def test_summary_hook_llm_failure_leaves_job_retryable(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

    _set_graphiti_enabled(monkeypatch, False)
    async def _failing_summarize(_messages):
        raise RuntimeError("llm failed")

    async def _stub_extract_and_create_loops(**_kwargs):
        return {"new_loops": 0}

    async def _stub_persist_extract_result(**_kwargs):
        return SimpleNamespace(extract_result_id=1, deduped=True)

    async def _stub_run_pass1_triage(**_kwargs):
        return SimpleNamespace(
            run_actionable_pass=False,
            run_session_changes_pass=False,
            run_entity_pass=False,
            run_threads_pass=False,
            should_run_identity=False,
            should_run_living_context=False,
        )

    monkeypatch.setattr(session_module, "summarize_session_messages", _failing_summarize, raising=True)
    monkeypatch.setattr(session_module.loops, "extract_and_create_loops", _stub_extract_and_create_loops, raising=False)
    monkeypatch.setattr(main_module, "persist_extract_result", _stub_persist_extract_result, raising=True)
    monkeypatch.setattr(main_module, "run_pass1_triage", _stub_run_pass1_triage, raising=True)

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            await _enqueue_session_ingest(client, tenant=tenant, user=user, session_id=session_id, now=now)

        await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=20,
            tenant_id=tenant,
            budget_seconds=5.0,
            per_row_timeout_seconds=5.0,
        )
        await session_module.drain_outbox(
            graphiti_client=graphiti_client,
            limit=20,
            tenant_id=tenant,
            budget_seconds=5.0,
            per_row_timeout_seconds=5.0,
        )

        hook_row = await main_module.db.fetchone(
            """
            SELECT status, sent_at, last_error
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
              AND job_type = 'post_ingest_hook'
              AND text = 'post_ingest_hook:session_summary'
            ORDER BY id DESC
            LIMIT 1
            """,
            tenant,
            user,
            session_id,
        )
        assert hook_row is not None
        assert hook_row["status"] == "pending"
        assert hook_row["sent_at"] is None
        assert "llm failed" in (hook_row["last_error"] or "")


@pytest.mark.asyncio
async def test_session_startbrief_and_build_briefing_read_session_summaries(monkeypatch):
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    settings = get_settings()
    now = datetime(2026, 5, 20, 9, 0, tzinfo=timezone.utc)

    _set_graphiti_enabled(monkeypatch, False)
    monkeypatch.setattr(settings, "internal_token", "test-internal", raising=False)
    monkeypatch.setattr(graphiti_client.settings, "internal_token", "test-internal", raising=False)
    async def _unexpected_classification_fallback(**_kwargs):
        raise AssertionError("classification fallback should not run")

    async def _stub_get_top_loops(**_kwargs):
        return []

    async def _stub_entity_hints(**_kwargs):
        return []

    monkeypatch.setattr(main_module, "_load_recent_session_summaries_from_classifications", _unexpected_classification_fallback, raising=True)
    monkeypatch.setattr(loops_module, "get_top_loops_for_startbrief", _stub_get_top_loops, raising=True)
    monkeypatch.setattr(main_module, "_build_startbrief_entity_hints_from_profiles", _stub_entity_hints, raising=True)

    async with app.router.lifespan_context(app):
        await main_module.db.execute(
            """
            INSERT INTO session_summaries (
              tenant_id, user_id, session_id, summary_text, bridge_text, decisions,
              unresolved_items, extra_attributes, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb, $9, $10)
            """,
            tenant,
            user,
            session_id,
            "They narrowed the next concrete step.",
            "Pick up by confirming the next concrete step.",
            ["Confirm the next concrete step"],
            ["Keep momentum"],
            {
                "summary_facts": "They narrowed the next concrete step.",
                "tone": "steady",
                "moment": "Momentum held.",
                "index_text": "They narrowed the next concrete step.",
                "salience": "high",
                "reference_time": now.isoformat().replace("+00:00", "Z"),
            },
            now,
            now,
        )

        response = await main_module.session_startbrief(
            tenantId=tenant,
            userId=user,
            now=now.isoformat().replace("+00:00", "Z"),
            sessionId=session_id,
            personaId=None,
            timezone="UTC",
            x_internal_token="test-internal",
        )
        assert response.evidence["summary_fetch_count"] >= 1
        assert session_id in response.evidence["session_summary_ids_fetched"]
        assert response.evidence["fallback_used"] is False

        briefing = await build_briefing(
            tenant_id=tenant,
            user_id=user,
            persona_id="persona",
            session_id=session_id,
            query=None,
            now=now.replace(tzinfo=None),
            graphiti_client=graphiti_client,
            database=main_module.db,
        )
        assert briefing.episodeBridge == "Pick up by confirming the next concrete step."
        assert briefing.metadata["hasSessionSummaryBridge"] is True


@pytest.mark.asyncio
async def test_graphiti_disabled_skips_falkordb_driver_initialization(monkeypatch):
    _set_graphiti_enabled(monkeypatch, False)

    def _unexpected_driver(*_args, **_kwargs):
        raise AssertionError("FalkorDriver should not be constructed when Graphiti is disabled")

    monkeypatch.setattr("src.graphiti_client.FalkorDriver", _unexpected_driver, raising=True)

    async with app.router.lifespan_context(app):
        assert graphiti_client.client is None
