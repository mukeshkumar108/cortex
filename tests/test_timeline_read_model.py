from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest
from httpx import ASGITransport, AsyncClient

from src import main as main_module
from src.timeline_read_model import _derive_freshness, build_timeline_read_model


NOW = datetime(2026, 5, 19, 9, 0, tzinfo=timezone.utc)


class FakeDB:
    def __init__(self):
        self.action_item_rows: List[Dict[str, Any]] = []
        self.action_update_rows: List[Dict[str, Any]] = []
        self.calendar_rows: List[Dict[str, Any]] = []
        self.session_change_rows: List[Dict[str, Any]] = []
        self.handover_rows: List[Dict[str, Any]] = []
        self.outcome_rows: List[Dict[str, Any]] = []
        self.relationship_rows: List[Dict[str, Any]] = []

    async def fetch(self, query: str, *args):
        if "FROM action_items" in query:
            return self.action_item_rows
        if "FROM action_updates" in query:
            return self.action_update_rows
        if "FROM calendar_items" in query:
            return self.calendar_rows
        if "FROM session_changes" in query:
            return self.session_change_rows
        if "FROM session_handover_packets" in query:
            return self.handover_rows
        if "FROM attention_outcomes" in query:
            return self.outcome_rows
        if "FROM memory_relationship_links" in query:
            return self.relationship_rows
        return []


def _action_item_row(*, row_id: str = "act-1", updated_at: datetime = NOW - timedelta(hours=1), due_at: datetime | None = None) -> Dict[str, Any]:
    return {
        "id": row_id,
        "tenant_id": "default",
        "user_id": "u1",
        "kind": "todo",
        "title": "Send Bob the draft",
        "notes": "Need to send before lunch.",
        "status": "pending",
        "due_at": due_at or (NOW + timedelta(hours=2)),
        "remind_at": NOW + timedelta(hours=1),
        "source_ref": {
            "source_object_ids": ["obligation:send-bob-draft"],
            "source_link_ids": ["link-obligation-workstream"],
            "evidence_refs": [{"session_id": "s1", "turn_id": "t1"}],
        },
        "confidence": 0.9,
        "provenance_summary": "User said they need to send the draft.",
        "created_at": NOW - timedelta(days=1),
        "updated_at": updated_at,
        "completed_at": None,
        "dismissed_at": None,
        "cancelled_at": None,
    }


def _expired_action_item_row() -> Dict[str, Any]:
    return {
        **_action_item_row(row_id="act-expired", updated_at=NOW - timedelta(days=10), due_at=NOW - timedelta(days=9)),
        "title": "Stale obligation",
        "status": "done",
        "completed_at": NOW - timedelta(days=8),
    }


def _calendar_item_row() -> Dict[str, Any]:
    return {
        "id": "cal-1",
        "tenant_id": "default",
        "user_id": "u1",
        "title": "Dentist appointment",
        "description": "Cleaning and x-rays.",
        "notes": None,
        "starts_at": NOW + timedelta(hours=26),
        "ends_at": NOW + timedelta(hours=27),
        "status": "confirmed",
        "source_ref": {"source_object_ids": ["event:dentist"], "source_link_ids": ["link-health"]},
        "evidence_refs": [{"session_id": "s2", "turn_id": "t2"}],
        "confidence": 0.82,
        "provenance_summary": "Imported from calendar.",
        "created_at": NOW - timedelta(days=2),
        "updated_at": NOW - timedelta(hours=3),
        "cancelled_at": None,
        "archived_at": None,
    }


def _attention_outcome_row() -> Dict[str, Any]:
    return {
        "outcome_id": "out-1",
        "tenant_id": "default",
        "user_id": "u1",
        "source_table": "follow_up_candidates",
        "source_id": "11",
        "outcome_type": "dismissed",
        "outcome_reason": "Already handled.",
        "occurred_at": NOW - timedelta(hours=4),
        "snoozed_until": None,
        "suppress_until": None,
        "metadata": {"source_object_ids": ["person:ashley"]},
        "created_at": NOW - timedelta(hours=4),
    }


def _handover_row(*, created_at: datetime = NOW - timedelta(hours=2), expires_at: datetime | None = None) -> Dict[str, Any]:
    return {
        "packet_id": "packet-1",
        "tenant_id": "default",
        "user_id": "u1",
        "session_id": "s-h1",
        "summary": "Recent session focused on invoice follow-up.",
        "source_turn_refs": [{"session_id": "s-h1", "turn_index": 0}],
        "created_at": created_at,
        "updated_at": created_at,
        "expires_at": expires_at or (NOW + timedelta(hours=16)),
        "status": "active",
    }


@pytest.mark.asyncio
async def test_internal_timeline_endpoint_requires_internal_auth(monkeypatch):
    called: Dict[str, Any] = {}

    def _stub_require_internal_token(token: str | None) -> None:
        called["token"] = token
        raise main_module.HTTPException(status_code=401, detail="Unauthorized")

    monkeypatch.setattr(main_module, "_require_internal_token", _stub_require_internal_token, raising=True)
    async with AsyncClient(transport=ASGITransport(app=main_module.app), base_url="http://test") as client:
        response = await client.get("/internal/debug/timeline", params={"tenantId": "default", "userId": "u1"})
    assert response.status_code == 401
    assert called["token"] is None


@pytest.mark.asyncio
async def test_timeline_includes_action_calendar_attention_and_handover_events(monkeypatch):
    db = FakeDB()
    db.action_item_rows = [_action_item_row()]
    db.calendar_rows = [_calendar_item_row()]
    db.outcome_rows = [_attention_outcome_row()]
    db.handover_rows = [_handover_row()]
    monkeypatch.setattr(main_module, "db", db, raising=True)
    monkeypatch.setattr(main_module, "get_settings", lambda: SimpleNamespace(internal_token="test-token"), raising=True)

    async with AsyncClient(transport=ASGITransport(app=main_module.app), base_url="http://test") as client:
        response = await client.get(
            "/internal/debug/timeline",
            params={"tenantId": "default", "userId": "u1", "limit": 10},
            headers={"X-Internal-Token": "test-token"},
        )

    assert response.status_code == 200
    items = response.json()["items"]
    by_source = {item["source_table"]: item for item in items}
    assert by_source["action_items"]["event_type"] == "obligation_item"
    assert by_source["action_items"]["domain"] == "obligations"
    assert by_source["calendar_items"]["event_type"] == "calendar_event"
    assert by_source["calendar_items"]["domain"] == "events"
    assert by_source["attention_outcomes"]["event_type"] == "attention_feedback"
    assert by_source["session_handover_packets"]["event_type"] == "handover"


@pytest.mark.asyncio
async def test_expired_items_are_hidden_by_default():
    db = FakeDB()
    db.action_item_rows = [_action_item_row(), _expired_action_item_row()]

    out = await build_timeline_read_model(db, tenant_id="default", user_id="u1", as_of=NOW)

    assert [item["source_id"] for item in out["items"]] == ["act-1"]
    assert out["metadata"]["returnedCount"] == 1


@pytest.mark.asyncio
async def test_include_expired_true_includes_expired_events():
    db = FakeDB()
    db.action_item_rows = [_action_item_row(), _expired_action_item_row()]

    out = await build_timeline_read_model(
        db,
        tenant_id="default",
        user_id="u1",
        include_expired=True,
        as_of=NOW,
    )

    assert {item["source_id"] for item in out["items"]} == {"act-1", "act-expired"}
    expired = next(item for item in out["items"] if item["source_id"] == "act-expired")
    assert expired["freshness"] == "expired"


def test_freshness_classification_current_recent_stale_expired():
    assert _derive_freshness(
        status="active",
        occurred_at=NOW - timedelta(hours=2),
        first_seen_at=NOW - timedelta(days=1),
        last_seen_at=NOW - timedelta(hours=1),
        expires_at=None,
        as_of=NOW,
    ) == "current"
    assert _derive_freshness(
        status="confirmed",
        occurred_at=NOW - timedelta(hours=30),
        first_seen_at=NOW - timedelta(hours=30),
        last_seen_at=NOW - timedelta(hours=30),
        expires_at=None,
        as_of=NOW,
    ) == "recent"
    assert _derive_freshness(
        status="detected",
        occurred_at=NOW - timedelta(days=8),
        first_seen_at=NOW - timedelta(days=8),
        last_seen_at=NOW - timedelta(days=8),
        expires_at=None,
        as_of=NOW,
    ) == "stale"
    assert _derive_freshness(
        status="active",
        occurred_at=NOW - timedelta(days=1),
        first_seen_at=NOW - timedelta(days=1),
        last_seen_at=NOW - timedelta(days=1),
        expires_at=NOW - timedelta(hours=1),
        as_of=NOW,
    ) == "expired"


@pytest.mark.asyncio
async def test_filtering_by_domain_works():
    db = FakeDB()
    db.action_item_rows = [_action_item_row()]
    db.calendar_rows = [_calendar_item_row()]

    out = await build_timeline_read_model(
        db,
        tenant_id="default",
        user_id="u1",
        domain="events",
        as_of=NOW,
    )

    assert len(out["items"]) == 1
    assert out["items"][0]["source_table"] == "calendar_items"
    assert out["items"][0]["domain"] == "events"


@pytest.mark.asyncio
async def test_metadata_counts_are_correct():
    db = FakeDB()
    db.action_item_rows = [_action_item_row()]
    db.calendar_rows = [_calendar_item_row()]
    db.outcome_rows = [_attention_outcome_row()]
    db.handover_rows = [_handover_row()]

    out = await build_timeline_read_model(db, tenant_id="default", user_id="u1", as_of=NOW)
    metadata = out["metadata"]

    assert metadata["returnedCount"] == 4
    assert metadata["bySourceTable"]["action_items"] == 1
    assert metadata["bySourceTable"]["calendar_items"] == 1
    assert metadata["bySourceTable"]["attention_outcomes"] == 1
    assert metadata["bySourceTable"]["session_handover_packets"] == 1
    assert metadata["byDomain"]["obligations"] == 1
    assert metadata["byDomain"]["events"] == 1
    assert metadata["byDomain"]["attention_feedback"] == 1
    assert metadata["byDomain"]["handover"] == 1
    assert metadata["readOnly"] is True
    assert metadata["oldestOccurredAt"] is not None
    assert metadata["newestOccurredAt"] is not None
