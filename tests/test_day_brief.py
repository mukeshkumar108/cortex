from datetime import datetime, timezone
from typing import Any, Dict, List
from uuid import uuid4

import pytest

from src.day_brief import DayBriefError, buildDayBrief


class FakeDB:
    def __init__(self):
        self.calendar_rows: List[Dict[str, Any]] = []
        self.action_rows: List[Dict[str, Any]] = []
        self.queries: List[str] = []

    async def fetch(self, query: str, *args):
        self.queries.append(query)
        if "FROM calendar_items" in query:
            start = args[2]
            end = args[3]
            return [
                row
                for row in self.calendar_rows
                if row.get("tenant_id") == args[0]
                and row.get("user_id") == args[1]
                and row.get("status") == "confirmed"
                and (row.get("ends_at") or row.get("starts_at")) >= start
                and row.get("starts_at") < end
            ]
        if "FROM action_items" in query:
            return [
                row
                for row in self.action_rows
                if row.get("tenant_id") == args[0]
                and row.get("user_id") == args[1]
                and row.get("status") == "pending"
            ]
        if "FROM actionable_candidates" in query:
            return []
        return []


def _calendar_row(title: str, starts_at: datetime, *, user_id: str = "u1", status: str = "confirmed") -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    return {
        "id": uuid4(),
        "tenant_id": "default",
        "user_id": user_id,
        "title": title,
        "description": None,
        "notes": None,
        "starts_at": starts_at,
        "ends_at": None,
        "timezone": "UTC",
        "all_day": False,
        "location": None,
        "participants": [],
        "organizer": None,
        "rsvp_status": "unknown",
        "status": status,
        "source_kind": "manual",
        "source_ref": None,
        "evidence_refs": [],
        "provenance_summary": None,
        "confidence": None,
        "external_provider": None,
        "external_id": None,
        "external_calendar_id": None,
        "external_etag": None,
        "external_updated_at": None,
        "sync_status": "not_synced",
        "recurrence_rule": None,
        "recurrence_parent_id": None,
        "dedupe_key": None,
        "metadata": {},
        "created_at": now,
        "updated_at": now,
        "cancelled_at": None,
        "archived_at": None,
    }


def _action_row(title: str, *, kind: str, due_at=None, remind_at=None, user_id: str = "u1") -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    return {
        "id": uuid4(),
        "tenant_id": "default",
        "user_id": user_id,
        "kind": kind,
        "title": title,
        "notes": None,
        "status": "pending",
        "due_at": due_at,
        "remind_at": remind_at,
        "recurrence_rule": None,
        "source_type": "direct_user",
        "source_ref": None,
        "confidence": None,
        "provenance_summary": None,
        "created_at": now,
        "updated_at": now,
        "completed_at": None,
        "dismissed_at": None,
        "cancelled_at": None,
    }


@pytest.mark.asyncio
async def test_day_brief_combines_calendar_and_actions():
    db = FakeDB()
    db.calendar_rows = [
        _calendar_row("Morning meeting", datetime(2026, 5, 1, 9, 0, tzinfo=timezone.utc)),
        _calendar_row("Cancelled event", datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc), status="cancelled"),
        _calendar_row("Other user event", datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc), user_id="u2"),
    ]
    db.action_rows = [
        _action_row("Submit report", kind="todo", due_at=datetime(2026, 5, 1, 15, 0, tzinfo=timezone.utc)),
        _action_row("Take meds", kind="reminder", remind_at=datetime(2026, 5, 1, 8, 0, tzinfo=timezone.utc)),
        _action_row("Old todo", kind="todo", due_at=datetime(2026, 4, 30, 8, 0, tzinfo=timezone.utc)),
    ]

    out = await buildDayBrief(db, tenant_id="default", user_id="u1", day="2026-05-01", timezone_name="UTC")

    assert out["date"] == "2026-05-01"
    assert out["timezone"] == "UTC"
    assert [item["title"] for item in out["calendarEvents"]] == ["Morning meeting"]
    assert [item["title"] for item in out["dueToday"]] == ["Submit report"]
    assert [item["title"] for item in out["remindersToday"]] == ["Take meds"]
    assert [item["title"] for item in out["overdue"]] == ["Old todo"]
    assert out["counts"] == {"calendarEvents": 1, "dueToday": 1, "remindersToday": 1, "overdue": 1}
    assert out["pressureLevel"] == "medium"
    assert out["sourceFreshness"]["mode"] == "on_demand"


@pytest.mark.asyncio
async def test_day_brief_high_pressure_is_deterministic():
    db = FakeDB()
    db.action_rows = [
        _action_row(f"Old todo {idx}", kind="todo", due_at=datetime(2026, 4, 30, 8, 0, tzinfo=timezone.utc))
        for idx in range(3)
    ]

    out = await buildDayBrief(db, tenant_id="default", user_id="u1", day="2026-05-01", timezone_name="UTC")

    assert out["pressureLevel"] == "high"
    assert out["suggestedFocus"][0] == "clear_overdue"


@pytest.mark.asyncio
async def test_day_brief_rejects_invalid_timezone():
    db = FakeDB()

    with pytest.raises(DayBriefError) as exc:
        await buildDayBrief(db, tenant_id="default", user_id="u1", day="2026-05-01", timezone_name="Nope/Nope")

    assert exc.value.status_code == 400
