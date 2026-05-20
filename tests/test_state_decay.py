from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pytest
from httpx import ASGITransport, AsyncClient

from src import main as main_module
from src.state_decay import build_state_decay_report


NOW = datetime(2026, 5, 20, 9, 0, tzinfo=timezone.utc)


class FakeDB:
    def __init__(self):
        self.timeline_event_rows: List[Dict[str, Any]] = []
        self.session_change_rows: List[Dict[str, Any]] = []
        self.living_context_row: Dict[str, Any] | None = None
        self.handover_rows: List[Dict[str, Any]] = []

    async def fetch(self, query: str, *args):
        if "FROM timeline_events" in query:
            return self.timeline_event_rows
        if "FROM session_changes" in query:
            return self.session_change_rows
        if "FROM session_handover_packets" in query:
            return self.handover_rows
        return []

    async def fetchone(self, query: str, *args):
        if "FROM living_context" in query:
            return self.living_context_row
        if "FROM session_handover_packets" in query:
            rows = list(self.handover_rows)
            rows.sort(key=lambda row: row.get("created_at") or NOW, reverse=True)
            return rows[0] if rows else None
        return None


def _timeline_state_event(*, event_id: str, age: timedelta, status: str = "active", summary: str = "User felt stressed.", confidence: float = 0.8) -> Dict[str, Any]:
    occurred_at = NOW - age
    return {
        "event_id": event_id,
        "tenant_id": "default",
        "user_id": "u1",
        "timeline_type": "interaction",
        "event_type": "state_signal",
        "domain": "state",
        "title": "State signal",
        "summary": summary,
        "occurred_at": occurred_at,
        "observed_at": occurred_at,
        "valid_from": occurred_at,
        "valid_until": None,
        "expires_at": None,
        "status": status,
        "confidence": confidence,
        "salience": 0.7,
        "actor": "system",
        "subject": "state:stress",
        "object_refs": [{"targetType": "state", "targetId": "state:stress"}],
        "source_table": "session_changes",
        "source_id": "901",
        "evidence_refs": [{"session_id": "s1", "turn_id": "t1"}],
        "user_corrected": False,
        "user_visible": True,
        "effect": None,
        "metadata": {"single_session": False},
        "created_at": occurred_at,
        "updated_at": occurred_at,
    }


def _session_change_row(*, age: timedelta, summary: str = "The user sounded overwhelmed.") -> Dict[str, Any]:
    occurred_at = NOW - age
    return {
        "change_id": 901,
        "tenant_id": "default",
        "user_id": "u1",
        "kind": "state_change",
        "title": "State change",
        "summary": summary,
        "effective_iso": occurred_at,
        "provenance": {"source_turn_refs": [{"session_id": "s2", "turn_id": "t2"}]},
        "confidence_score": 0.72,
        "status": "confirmed",
        "created_at": occurred_at,
        "updated_at": occurred_at,
    }


def _handover_row() -> Dict[str, Any]:
    return {
        "packet_id": "packet-1",
        "tenant_id": "default",
        "user_id": "u1",
        "session_id": "s-h1",
        "summary": "Recent session focused on a rough afternoon.",
        "open_questions": [],
        "unresolved_decisions": [],
        "pending_actions": [],
        "recent_state_note": "The user sounded stressed after the meeting.",
        "important_people": [],
        "active_topics": [],
        "do_not_overdo": [],
        "source_turn_refs": [{"session_id": "s-h1", "turn_index": 0}],
        "created_at": NOW - timedelta(hours=3),
        "updated_at": NOW - timedelta(hours=3),
        "expires_at": NOW + timedelta(hours=10),
        "status": "active",
    }


@pytest.mark.asyncio
async def test_state_decay_endpoint_requires_internal_auth(monkeypatch):
    called: Dict[str, Any] = {}

    def _stub_require_internal_token(token: str | None) -> None:
        called["token"] = token
        raise main_module.HTTPException(status_code=401, detail="Unauthorized")

    monkeypatch.setattr(main_module, "_require_internal_token", _stub_require_internal_token, raising=True)
    async with AsyncClient(transport=ASGITransport(app=main_module.app), base_url="http://test") as client:
        response = await client.get("/internal/debug/state-decay", params={"tenantId": "default", "userId": "u1"})
    assert response.status_code == 401
    assert called["token"] is None


@pytest.mark.asyncio
async def test_state_under_24h_may_be_current():
    db = FakeDB()
    db.timeline_event_rows = [_timeline_state_event(event_id="evt-current", age=timedelta(hours=6), status="confirmed")]

    out = await build_state_decay_report(db, tenant_id="default", user_id="u1", as_of=NOW)

    assert len(out["current_state_notes"]) == 1
    assert out["current_state_notes"][0]["freshness"] == "current"


@pytest.mark.asyncio
async def test_single_session_state_is_provisional():
    db = FakeDB()
    db.session_change_rows = [_session_change_row(age=timedelta(hours=3))]

    out = await build_state_decay_report(db, tenant_id="default", user_id="u1", as_of=NOW)

    assert len(out["provisional_state_notes"]) >= 1
    assert any("single_session_state_is_provisional" in note["gaps"] for note in out["provisional_state_notes"])


@pytest.mark.asyncio
async def test_state_over_48h_becomes_stale_warning():
    db = FakeDB()
    db.timeline_event_rows = [_timeline_state_event(event_id="evt-stale", age=timedelta(hours=60))]

    out = await build_state_decay_report(db, tenant_id="default", user_id="u1", as_of=NOW)

    assert len(out["stale_state_warnings"]) == 1
    assert out["stale_state_warnings"][0]["freshness"] == "stale"


@pytest.mark.asyncio
async def test_state_over_two_weeks_is_historical():
    db = FakeDB()
    db.timeline_event_rows = [_timeline_state_event(event_id="evt-historical", age=timedelta(days=16))]

    out = await build_state_decay_report(
        db,
        tenant_id="default",
        user_id="u1",
        include_historical=True,
        as_of=NOW,
    )

    assert len(out["historical_state_notes"]) == 1
    assert out["historical_state_notes"][0]["freshness"] == "historical"


@pytest.mark.asyncio
async def test_stale_state_adds_required_suppression():
    db = FakeDB()
    db.timeline_event_rows = [_timeline_state_event(event_id="evt-stale", age=timedelta(days=3))]

    out = await build_state_decay_report(db, tenant_id="default", user_id="u1", as_of=NOW)

    assert "Do not present stale emotional state as current." in out["suppressions"]


@pytest.mark.asyncio
async def test_handover_and_living_context_are_provisional_modifiers():
    db = FakeDB()
    db.handover_rows = [_handover_row()]
    db.living_context_row = {
        "current_focus": "recovery",
        "emotional_texture": "stressed",
        "primary_tension": "too many commitments",
        "relationship_pulse": None,
        "why_it_matters": None,
        "sophie_directives": None,
        "created_at": NOW - timedelta(hours=5),
        "updated_at": NOW - timedelta(hours=1),
    }

    out = await build_state_decay_report(db, tenant_id="default", user_id="u1", as_of=NOW)

    assert len(out["provisional_state_notes"]) >= 2
    assert any("Fast handover recent state should not become durable identity." == item for item in out["tone_modifiers"])
    assert "living_context_requires_revalidation" in out["gaps"]
