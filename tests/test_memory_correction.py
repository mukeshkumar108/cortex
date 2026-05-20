from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pytest
from httpx import ASGITransport, AsyncClient

from src import main as main_module
from src.attention_preview import build_attention_preview
from src.memory_corrections import record_memory_correction


NOW = datetime(2026, 5, 20, 9, 0, tzinfo=timezone.utc)


class FakePoolAcquire:
    def __init__(self, db: "FakeDB"):
        self.db = db

    async def __aenter__(self):
        return self.db

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakePool:
    def __init__(self, db: "FakeDB"):
        self.db = db

    def acquire(self):
        return FakePoolAcquire(self.db)


class FakeDB:
    def __init__(self):
        self.timeline_event_rows: List[Dict[str, Any]] = []
        self.outcome_rows: List[Dict[str, Any]] = []
        self.follow_up_rows: List[Dict[str, Any]] = []
        self.action_item_rows: List[Dict[str, Any]] = []
        self.living_context_row: Dict[str, Any] | None = None
        self.audit_rows: List[Dict[str, Any]] = []

    async def get_pool(self):
        return FakePool(self)

    def transaction(self):
        return FakeTransaction()

    async def fetch(self, query: str, *args):
        if "FROM timeline_events" in query:
            return self.timeline_event_rows
        if "FROM attention_outcomes" in query:
            return self.outcome_rows
        if "FROM follow_up_candidates" in query:
            return self.follow_up_rows
        if "FROM action_items" in query and "status='pending'" in query:
            return self.action_item_rows
        return []

    async def fetchrow(self, query: str, *args):
        if "SELECT * FROM action_items" in query:
            action_id = str(args[2])
            for row in self.action_item_rows:
                if str(row.get("id")) == action_id:
                    return row
            return None
        if "UPDATE action_items" in query:
            action_id = str(args[2])
            for row in self.action_item_rows:
                if str(row.get("id")) == action_id:
                    row["status"] = args[3]
                    row["completed_at"] = args[4]
                    row["cancelled_at"] = args[5]
                    row["dismissed_at"] = args[6]
                    row["updated_at"] = NOW
                    return row
            return None
        if "INSERT INTO action_audit_log" in query:
            self.audit_rows.append({"query": query, "args": args})
            return {"id": "audit-1"}
        return None

    async def fetchone(self, query: str, *args):
        if "INSERT INTO timeline_events" in query:
            row = {
                "event_id": f"evt-{len(self.timeline_event_rows) + 1}",
                "tenant_id": args[0],
                "user_id": args[1],
                "timeline_type": args[2],
                "event_type": args[3],
                "domain": args[4],
                "title": args[5],
                "summary": args[6],
                "occurred_at": args[7] or NOW,
                "observed_at": NOW,
                "valid_from": args[8],
                "valid_until": args[9],
                "expires_at": args[10],
                "status": args[11],
                "confidence": args[12],
                "salience": args[13],
                "actor": args[14],
                "subject": args[15],
                "object_refs": args[16] or [],
                "source_table": args[17],
                "source_id": args[18],
                "evidence_refs": args[19] or [],
                "user_corrected": args[20],
                "user_visible": args[21],
                "effect": args[22],
                "metadata": args[23] or {},
                "created_at": NOW,
                "updated_at": NOW,
            }
            self.timeline_event_rows.append(row)
            return row
        if "INSERT INTO attention_outcomes" in query:
            row = {
                "outcome_id": f"outcome-{len(self.outcome_rows) + 1}",
                "tenant_id": args[0],
                "user_id": args[1],
                "companion_id": args[2],
                "attention_item_id": args[3],
                "source_table": args[4],
                "source_id": args[5],
                "outcome_type": args[6],
                "outcome_reason": args[7],
                "surface_mode": args[8],
                "action_policy": args[9],
                "snoozed_until": args[10],
                "suppress_until": args[11],
                "metadata": args[12] or {},
                "occurred_at": NOW,
                "created_at": NOW,
            }
            self.outcome_rows.append(row)
            return row
        if "FROM living_context" in query:
            return self.living_context_row
        return None

    async def execute(self, query: str, *args):
        if "INSERT INTO action_audit_log" in query:
            self.audit_rows.append({"query": query, "args": args})
        return None


def _follow_up_row() -> Dict[str, Any]:
    return {
        "candidate_id": 11,
        "tenant_id": "default",
        "user_id": "u1",
        "candidate_key": "ashley-takedown",
        "title": "Check on Ashley and the takedown",
        "reason": "Ashley was unwell and the event takedown likely needs a gentle follow-up this morning.",
        "priority_score": 0.91,
        "confidence": 0.86,
        "due_at": NOW,
        "status": "shadow_open",
        "source_turn_refs": [{"session_id": "s1", "turn_id": "t3", "text": "Ashley called in sick"}],
        "metadata": {
            "source_object_ids": ["person:ashley", "workstream:event-takedown", "state:unwell"],
            "source_link_ids": ["link-involves", "link-affects"],
        },
        "created_at": NOW - timedelta(hours=10),
        "updated_at": NOW - timedelta(hours=1),
    }


def _action_item_row() -> Dict[str, Any]:
    return {
        "id": "11111111-1111-1111-1111-111111111111",
        "tenant_id": "default",
        "user_id": "u1",
        "kind": "todo",
        "title": "Submit report",
        "notes": "Need to finish this today.",
        "status": "pending",
        "due_at": NOW + timedelta(hours=1),
        "remind_at": NOW,
        "source_ref": {"source_object_ids": ["obligation:submit-report"], "source_link_ids": ["link-task"]},
        "confidence": 0.88,
        "created_at": NOW - timedelta(days=1),
        "updated_at": NOW - timedelta(hours=1),
        "completed_at": None,
        "dismissed_at": None,
        "cancelled_at": None,
    }


@pytest.mark.asyncio
async def test_memory_correction_endpoint_requires_internal_auth(monkeypatch):
    called: Dict[str, Any] = {}

    def _stub_require_internal_token(token: str | None) -> None:
        called["token"] = token
        raise main_module.HTTPException(status_code=401, detail="Unauthorized")

    monkeypatch.setattr(main_module, "_require_internal_token", _stub_require_internal_token, raising=True)
    async with AsyncClient(transport=ASGITransport(app=main_module.app), base_url="http://test") as client:
        response = await client.post("/internal/debug/memory/correction", json={"tenantId": "default", "userId": "u1", "command": "thats_wrong"})
    assert response.status_code == 401
    assert called["token"] is None


@pytest.mark.asyncio
async def test_thats_wrong_writes_user_corrected_timeline_event():
    db = FakeDB()

    out = await record_memory_correction(
        db,
        tenant_id="default",
        user_id="u1",
        command="thats_wrong",
        target_type="state",
        target_id="state:angry",
        note="That is wrong.",
    )

    assert out["recorded"]["event_type"] == "user_correction"
    assert out["recorded"]["user_corrected"] is True
    assert out["recorded"]["effect"] == "correct"


@pytest.mark.asyncio
async def test_remember_this_writes_confirmation_timeline_event():
    db = FakeDB()

    out = await record_memory_correction(
        db,
        tenant_id="default",
        user_id="u1",
        command="remember_this",
        target_type="profile",
        note="Remember that this matters.",
    )

    assert out["recorded"]["event_type"] == "memory_confirmation"
    assert out["recorded"]["effect"] == "confirm"
    assert out["recorded"]["user_corrected"] is False


@pytest.mark.asyncio
async def test_dont_bring_that_up_again_writes_timeline_event_and_suppresses_matching_attention_item():
    db = FakeDB()
    db.follow_up_rows = [_follow_up_row()]

    correction = await record_memory_correction(
        db,
        tenant_id="default",
        user_id="u1",
        command="dont_bring_that_up_again",
        source_table="follow_up_candidates",
        source_id="11",
        target_type="attention_item",
        target_id="11",
        note="Stop surfacing this.",
    )

    assert correction["recorded"]["effect"] == "suppress_related"
    assert correction["applied_side_effects"][0]["type"] == "attention_outcome"

    visible = await build_attention_preview(db, tenant_id="default", user_id="u1", companion_id="sophie", as_of=NOW)
    hidden = await build_attention_preview(
        db,
        tenant_id="default",
        user_id="u1",
        companion_id="sophie",
        include_suppressed=True,
        as_of=NOW,
    )

    assert visible["items"] == []
    assert hidden["items"][0]["status"] == "suppressed"


@pytest.mark.asyncio
async def test_thats_done_writes_timeline_event_and_marks_matching_action_item_done():
    db = FakeDB()
    db.action_item_rows = [_action_item_row()]

    out = await record_memory_correction(
        db,
        tenant_id="default",
        user_id="u1",
        command="thats_done",
        source_table="action_items",
        source_id="11111111-1111-1111-1111-111111111111",
        target_type="action_item",
        target_id="11111111-1111-1111-1111-111111111111",
        note="Finished it.",
    )

    assert out["recorded"]["event_type"] == "user_mark_done"
    assert any(item["type"] == "action_item_mark_done" for item in out["applied_side_effects"])
    assert db.action_item_rows[0]["status"] == "done"
