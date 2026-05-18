from datetime import datetime, timezone
import uuid

import pytest
from httpx import ASGITransport, AsyncClient

from src.config import get_settings
from src.derived_pipeline import run_pass1_5_entities, run_pass2b_session_changes, run_pass2c_entity_candidates
from src.derived_passes.pass2b_session_changes import _normalize_session_changes_payload, run_pass2b_session_changes_llm
from src.main import app, db


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def test_pass2b_normalize_keeps_project_decision_change():
    out = _normalize_session_changes_payload(
        {
            "session_changes": [
                {
                    "kind": "focus_change",
                    "title": "Focus Sophie on single parents",
                    "summary": "We changed product focus to single parents first.",
                    "status": "detected",
                    "confidence": "high",
                }
            ]
        }
    )
    assert len(out) == 1
    assert out[0]["kind"] == "focus_change"


def test_pass2b_normalize_keeps_appointment_date_change_without_overriding_llm_status():
    out = _normalize_session_changes_payload(
        {
            "session_changes": [
                {
                    "kind": "schedule_change",
                    "title": "Mum appointment moved to Tuesday",
                    "summary": "Mum's appointment moved to Tuesday.",
                    "status": "detected",
                    "confidence": "high",
                }
            ]
        }
    )
    assert len(out) == 1
    assert out[0]["status"] == "detected"


def test_pass2b_normalize_dedupes_identical_changes():
    out = _normalize_session_changes_payload(
        {
            "session_changes": [
                {
                    "kind": "focus_change",
                    "title": "Product focus changed",
                    "summary": "We changed product focus.",
                    "status": "detected",
                },
                {
                    "kind": "focus_change",
                    "title": "Product focus changed",
                    "summary": "We changed product focus.",
                    "status": "detected",
                }
            ]
        }
    )
    assert len(out) == 1


def test_pass2b_normalize_fills_message_hint_from_title_when_missing():
    out = _normalize_session_changes_payload(
        {
            "session_changes": [
                {
                    "kind": "state_change",
                    "title": "Relationship status changed",
                    "summary": "Ashley is coming back on Friday.",
                    "status": "detected",
                }
            ]
        }
    )
    assert len(out) == 1
    assert out[0]["provenance"]["message_hint"] == "Relationship status changed"


def test_pass2b_normalize_coerces_invalid_status_and_confidence():
    out = _normalize_session_changes_payload(
        {
            "session_changes": [
                {
                    "kind": "decision_change",
                    "title": "Project direction changed",
                    "summary": "We are no longer working on the golf caddy.",
                    "status": "garbage",
                    "confidence": "very_high",
                }
            ]
        }
    )
    assert len(out) == 1
    assert out[0]["status"] == "detected"
    assert out[0]["confidence"] == "medium"


def test_pass2b_normalize_preserves_time_text_and_unresolved_basis():
    out = _normalize_session_changes_payload(
        {
            "session_changes": [
                {
                    "kind": "schedule_change",
                    "title": "Launch timing shifted",
                    "summary": "The launch is happening later.",
                    "effective_iso": "2026-05-01T10:00:00Z",
                    "time_text": "later",
                    "provenance": {"date_resolution_basis": "unresolved"},
                    "status": "needs_review",
                }
            ]
        }
    )
    assert len(out) == 1
    assert out[0]["effective_iso"] is None
    assert out[0]["provenance"]["time_text"] == "later"
    assert out[0]["provenance"]["date_resolution_basis"] == "unresolved"


@pytest.mark.asyncio
async def test_run_pass2b_session_changes_llm_includes_temporal_reference_context(monkeypatch):
    captured = {}

    async def _stub_call_json_llm(*, prompt, model, max_tokens=1600, temperature=0.1):
        captured["prompt"] = prompt
        return {"session_changes": []}

    monkeypatch.setattr("src.derived_passes.pass2b_session_changes.call_json_llm", _stub_call_json_llm, raising=True)

    await run_pass2b_session_changes_llm(
        messages=[{"role": "user", "text": "It moved to Friday night.", "timestamp": "2026-04-27T09:00:00Z"}],
        model="test-model",
        reference_time=datetime(2026, 4, 27, 19, 45, tzinfo=timezone.utc),
        timezone_name="UTC",
    )

    prompt = captured["prompt"]
    assert "now_iso: 2026-04-27T19:45:00+00:00" in prompt
    assert "timezone: UTC" in prompt
    assert "local_date: 2026-04-27" in prompt
    assert "local_day: Monday" in prompt
    assert "time_of_day: evening" in prompt


@pytest.mark.asyncio
async def test_run_pass2b_session_changes_persists_rows(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        tenant_id = "default"
        user_id = _unique("session-changes-user")
        session_id = _unique("session-changes-session")
        messages = [
            {"role": "user", "text": "I changed my mind, we are focusing on Sophie for single parents first.", "timestamp": "2026-04-27T09:00:00Z"},
        ]
        await db.execute(
            """
            INSERT INTO session_transcript (tenant_id, session_id, user_id, messages, created_at, updated_at)
            VALUES ($1,$2,$3,$4::jsonb,NOW(),NOW())
            """,
            tenant_id,
            session_id,
            user_id,
            messages,
        )

        async def _stub_llm(*, messages, model, reference_time=None, timezone_name=None):
            return [
                {
                    "kind": "focus_change",
                    "title": "Focus Sophie on single parents",
                    "summary": "User changed product focus to Sophie for single parents first.",
                    "effective_iso": None,
                    "source": "chat",
                    "provenance": {"message_hint": "focusing on Sophie for single parents first"},
                    "confidence": "high",
                    "status": "detected",
                }
            ]

        monkeypatch.setattr("src.derived_pipeline.run_pass2b_session_changes_llm", _stub_llm, raising=True)

        await run_pass2b_session_changes(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            settings=settings,
        )

        row = await db.fetchone(
            """
            SELECT kind, title, summary, status, provenance
            FROM session_changes
            WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
            """,
            tenant_id,
            user_id,
            session_id,
        )
        assert row is not None
        assert row["kind"] == "focus_change"
        assert row["title"] == "Focus Sophie on single parents"
        assert row["status"] == "detected"
        assert "message_hint" in (row["provenance"] or {})


@pytest.mark.asyncio
async def test_run_pass2b_session_changes_supersedes_earlier_related_change(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        tenant_id = "default"
        user_id = _unique("session-changes-supersede-user")
        first_session_id = _unique("session-changes-first")
        second_session_id = _unique("session-changes-second")
        first_messages = [
            {"role": "user", "text": "Mum's appointment moved to Monday.", "timestamp": "2026-04-27T09:00:00Z"},
        ]
        second_messages = [
            {"role": "user", "text": "Correction: mum's appointment is Tuesday, not Monday.", "timestamp": "2026-04-28T09:00:00Z"},
        ]
        await db.execute(
            """
            INSERT INTO session_transcript (tenant_id, session_id, user_id, messages, created_at, updated_at)
            VALUES ($1,$2,$3,$4::jsonb,NOW(),NOW())
            """,
            tenant_id,
            first_session_id,
            user_id,
            first_messages,
        )
        await db.execute(
            """
            INSERT INTO session_transcript (tenant_id, session_id, user_id, messages, created_at, updated_at)
            VALUES ($1,$2,$3,$4::jsonb,NOW(),NOW())
            """,
            tenant_id,
            second_session_id,
            user_id,
            second_messages,
        )

        responses = [
            [
                {
                    "kind": "schedule_change",
                    "title": "Mum appointment moved to Monday",
                    "summary": "Mum's appointment moved to Monday.",
                    "effective_iso": "2026-04-28T09:00:00Z",
                    "source": "chat",
                    "provenance": {"message_hint": "moved to Monday"},
                    "confidence": "high",
                    "status": "detected",
                }
            ],
            [
                {
                    "kind": "schedule_change",
                    "title": "Mum appointment moved to Tuesday",
                    "summary": "Correction: mum's appointment is Tuesday, not Monday.",
                    "effective_iso": "2026-04-29T09:00:00Z",
                    "source": "chat",
                    "provenance": {"message_hint": "Tuesday, not Monday"},
                    "confidence": "high",
                    "status": "needs_review",
                }
            ],
        ]

        async def _stub_llm(*, messages, model, reference_time=None, timezone_name=None):
            return responses.pop(0)

        monkeypatch.setattr("src.derived_pipeline.run_pass2b_session_changes_llm", _stub_llm, raising=True)

        await run_pass2b_session_changes(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=first_session_id,
            messages=first_messages,
            settings=settings,
        )
        await run_pass2b_session_changes(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=second_session_id,
            messages=second_messages,
            settings=settings,
        )

        rows = await db.fetch(
            """
            SELECT title, status
            FROM session_changes
            WHERE tenant_id=$1 AND user_id=$2
            ORDER BY created_at
            """,
            tenant_id,
            user_id,
        )
        statuses = {row["title"]: row["status"] for row in rows}
        assert statuses["Mum appointment moved to Monday"] == "superseded"
        assert statuses["Mum appointment moved to Tuesday"] == "needs_review"


@pytest.mark.asyncio
async def test_run_pass2c_entity_candidates_persists_and_pass15_resolves(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        tenant_id = "default"
        user_id = _unique("entity-candidates-user")
        session_id = _unique("entity-candidates-session")
        messages = [
            {"role": "user", "text": "Riley visited England and Bluum is the main project now.", "timestamp": "2026-04-27T09:00:00Z"},
        ]
        await db.execute(
            """
            INSERT INTO session_transcript (tenant_id, session_id, user_id, messages, created_at, updated_at)
            VALUES ($1,$2,$3,$4::jsonb,NOW(),NOW())
            """,
            tenant_id,
            session_id,
            user_id,
            messages,
        )

        async def _stub_llm(*, messages, model):
            return [
                {
                    "name": "Riley",
                    "candidate_type": "person",
                    "summary": "Important person in the user's life.",
                    "source": "chat",
                    "provenance": {"message_hint": "Riley visited England"},
                    "confidence": "high",
                    "status": "detected",
                }
            ]

        monkeypatch.setattr("src.derived_pipeline.run_pass2c_entity_candidates_llm", _stub_llm, raising=True)

        await run_pass2c_entity_candidates(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            settings=settings,
        )

        row = await db.fetchone(
            """
            SELECT candidate_id, name, candidate_type, status
            FROM entity_candidates
            WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
            """,
            tenant_id,
            user_id,
            session_id,
        )
        assert row is not None
        assert row["name"] == "Riley"
        assert row["candidate_type"] == "person"
        assert row["status"] == "detected"

        settings.derived_pipeline_llm_enabled = False
        await run_pass1_5_entities(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            settings=settings,
        )

        resolved = await db.fetchone(
            """
            SELECT status, metadata
            FROM entity_candidates
            WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
            """,
            tenant_id,
            user_id,
            session_id,
        )
        assert resolved is not None
        assert resolved["status"] == "resolved"
        assert (resolved["metadata"] or {}).get("resolved_by") == "pass1_5_entities"


@pytest.mark.asyncio
async def test_session_changes_endpoint_groups_by_kind(monkeypatch):
    rows = [
        {
            "change_id": 1,
            "session_id": "s1",
            "kind": "focus_change",
            "title": "Focus Sophie on single parents",
            "summary": "summary-1",
            "effective_iso": datetime(2026, 4, 27, 8, 0, tzinfo=timezone.utc),
            "source": "chat",
            "provenance": {"message_hint": "hint"},
            "confidence_score": 0.9,
            "confidence_label": "high",
            "status": "detected",
            "created_at": datetime(2026, 4, 27, 9, 0, tzinfo=timezone.utc),
            "updated_at": datetime(2026, 4, 27, 9, 5, tzinfo=timezone.utc),
        },
        {
            "change_id": 2,
            "session_id": "s2",
            "kind": "schedule_change",
            "title": "Mum appointment moved to Tuesday",
            "summary": "summary-2",
            "effective_iso": None,
            "source": "chat",
            "provenance": {"message_hint": "hint2"},
            "confidence_score": 0.7,
            "confidence_label": "medium",
            "status": "needs_review",
            "created_at": datetime(2026, 4, 27, 9, 0, tzinfo=timezone.utc),
            "updated_at": datetime(2026, 4, 27, 9, 5, tzinfo=timezone.utc),
        },
    ]

    async def _stub_fetch(*_args, **_kwargs):
        return rows

    monkeypatch.setattr("src.main.db.fetch", _stub_fetch, raising=False)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/session-changes", params={"tenantId": "t", "userId": "u"})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["items"]) == 2
        assert len(data["byKind"]["focus_change"]) == 1
        assert len(data["byKind"]["schedule_change"]) == 1


@pytest.mark.asyncio
async def test_entity_candidates_endpoint_groups_by_type(monkeypatch):
    rows = [
        {
            "candidate_id": 1,
            "session_id": "s1",
            "name": "Riley",
            "candidate_type": "person",
            "summary": "summary-1",
            "source": "chat",
            "provenance": {"message_hint": "hint"},
            "confidence_score": 0.9,
            "confidence_label": "high",
            "status": "detected",
            "created_at": datetime(2026, 4, 27, 9, 0, tzinfo=timezone.utc),
            "updated_at": datetime(2026, 4, 27, 9, 5, tzinfo=timezone.utc),
        },
        {
            "candidate_id": 2,
            "session_id": "s2",
            "name": "Bluum",
            "candidate_type": "project",
            "summary": "summary-2",
            "source": "chat",
            "provenance": {"message_hint": "hint2"},
            "confidence_score": 0.8,
            "confidence_label": "medium",
            "status": "needs_review",
            "created_at": datetime(2026, 4, 27, 9, 0, tzinfo=timezone.utc),
            "updated_at": datetime(2026, 4, 27, 9, 5, tzinfo=timezone.utc),
        },
    ]

    async def _stub_fetch(*_args, **_kwargs):
        return rows

    monkeypatch.setattr("src.main.db.fetch", _stub_fetch, raising=False)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/entity-candidates", params={"tenantId": "t", "userId": "u"})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["items"]) == 2
        assert len(data["byType"]["person"]) == 1
        assert len(data["byType"]["project"]) == 1
