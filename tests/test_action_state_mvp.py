from datetime import date, datetime, timezone
from typing import Any, Dict, List
from uuid import uuid4

import pytest

from src.action_state import (
    ActionStateError,
    archiveActionItem,
    createActionItem,
    dismissActionItem,
    listActionItems,
    listDailyAgenda,
    markActionItemDone,
    maybeAutoPromoteCandidate,
    promoteCandidateToActionItem,
    shouldAutoPromoteCandidate,
    updateActionItem,
    updateActionItemStatus,
)


class _Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self):
        self.items: Dict[str, Dict[str, Any]] = {}
        self.candidates: Dict[int, Dict[str, Any]] = {}
        self.audit_rows: List[Dict[str, Any]] = []

    def transaction(self):
        return _Tx()

    async def fetchrow(self, query: str, *args):
        if "INSERT INTO action_items" in query:
            item_id = str(uuid4())
            row = {
                "id": item_id,
                "tenant_id": args[0],
                "user_id": args[1],
                "kind": args[2],
                "title": args[3],
                "notes": args[4],
                "status": args[5] if len(args) > 5 and isinstance(args[5], str) else "pending",
                "due_at": args[6] if len(args) > 6 else None,
                "remind_at": args[7] if len(args) > 7 else None,
                "recurrence_rule": args[8] if len(args) > 8 else None,
                "source_type": args[9] if len(args) > 9 and isinstance(args[9], str) else "candidate_promotion",
                "source_ref": args[10] if len(args) > 10 else None,
                "confidence": args[11] if len(args) > 11 else None,
                "provenance_summary": args[12] if len(args) > 12 else None,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
                "completed_at": None,
                "dismissed_at": None,
                "cancelled_at": None,
            }
            self.items[item_id] = row
            return row

        if "SELECT * FROM action_items" in query:
            item_id = args[2]
            return self.items.get(str(item_id))

        if "UPDATE action_items" in query:
            item_id = str(args[2])
            row = self.items[item_id]
            if len(args) == 7:
                row["status"] = args[3]
                row["completed_at"] = args[4]
                row["cancelled_at"] = args[5]
                row["dismissed_at"] = args[6]
            else:
                row["title"] = args[3]
                row["notes"] = args[4]
                row["kind"] = args[5]
                row["due_at"] = args[6]
                row["remind_at"] = args[7]
                row["recurrence_rule"] = args[8]
                row["status"] = args[9]
                row["completed_at"] = args[10]
                row["cancelled_at"] = args[11]
                row["dismissed_at"] = args[12]
            row["updated_at"] = datetime.now(timezone.utc)
            return row

        if "FROM actionable_candidates" in query and "candidate_id=$3" in query:
            return self.candidates.get(int(args[2]))

        if "UPDATE actionable_candidates" in query:
            cid = int(args[2])
            row = self.candidates[cid]
            row["status"] = "confirmed"
            row["promoted_action_item_id"] = args[3]
            row["updated_at"] = datetime.now(timezone.utc)
            row["confidence"] = row.get("confidence") or row.get("confidence_score")
            row["provenance_summary"] = row.get("provenance_summary") or row.get("summary")
            return row

        return None

    async def fetch(self, query: str, *args):
        if "FROM actionable_candidates" in query:
            tenant_id, user_id, statuses = args[0], args[1], set(args[2] or [])
            return [
                dict(row)
                for row in self.candidates.values()
                if row.get("tenant_id") == tenant_id
                and row.get("user_id") == user_id
                and row.get("status") in statuses
            ]
        return []

    async def execute(self, query: str, *args):
        if "INSERT INTO action_audit_log" in query:
            self.audit_rows.append(
                {
                    "tenant_id": args[0],
                    "user_id": args[1],
                    "object_type": args[2],
                    "object_id": args[3],
                    "action": args[4],
                }
            )
        if "UPDATE actionable_candidates" in query:
            cid = int(args[2])
            row = self.candidates[cid]
            row["status"] = "confirmed"
            row["promoted_action_item_id"] = args[3]
            row["metadata"] = {**(row.get("metadata") or {}), **(args[4] or {})}
            row["updated_at"] = datetime.now(timezone.utc)
            row["confidence"] = row.get("confidence") or row.get("confidence_score")
            row["provenance_summary"] = row.get("provenance_summary") or row.get("summary")
        return "OK"


class FakeAcquire:
    def __init__(self, conn: FakeConn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakePool:
    def __init__(self, conn: FakeConn):
        self.conn = conn

    def acquire(self):
        return FakeAcquire(self.conn)


class FakeDB:
    def __init__(self, conn: FakeConn):
        self.conn = conn
        self.fetch_rows: List[Dict[str, Any]] = []
        self.candidate_rows: List[Dict[str, Any]] = []

    async def get_pool(self):
        return FakePool(self.conn)

    async def fetch(self, query: str, *args):
        if "FROM action_items" in query:
            rows = list(self.fetch_rows)
            if "kind=$" in query:
                expected_kind = str(args[2]).lower() if len(args) >= 3 else None
                if expected_kind:
                    rows = [r for r in rows if str(r.get("kind") or "").lower() == expected_kind]
            if "status=$" in query:
                expected_status = str(args[2]).lower() if len(args) >= 3 else None
                if expected_status:
                    rows = [r for r in rows if str(r.get("status") or "").lower() == expected_status]
            if "title ILIKE $" in query:
                title_query = str(args[-3]).lower() if len(args) >= 3 else ""
                title_query = title_query.strip("%")
                if title_query:
                    rows = [r for r in rows if title_query in str(r.get("title") or "").lower()]
            if "status='pending'" in query:
                rows = [r for r in rows if r.get("status") == "pending"]
            return rows
        if "FROM actionable_candidates" in query:
            return self.candidate_rows
        return []


@pytest.mark.asyncio
async def test_create_action_item_writes_audit():
    conn = FakeConn()
    db = FakeDB(conn)

    out = await createActionItem(
        db,
        {
            "tenantId": "default",
            "userId": "u1",
            "kind": "todo",
            "title": "Send report",
        },
    )

    assert out["status"] == "pending"
    assert len(conn.audit_rows) == 1
    assert conn.audit_rows[0]["object_type"] == "action_item"


@pytest.mark.asyncio
async def test_invalid_transition_rejected():
    conn = FakeConn()
    db = FakeDB(conn)
    created = await createActionItem(db, {"tenantId": "default", "userId": "u1", "kind": "todo", "title": "X"})

    await markActionItemDone(db, tenant_id="default", user_id="u1", action_item_id=created["id"])

    with pytest.raises(ActionStateError) as exc:
        await updateActionItemStatus(
            db,
            tenant_id="default",
            user_id="u1",
            action_item_id=created["id"],
            status="dismissed",
        )
    assert exc.value.status_code == 409


@pytest.mark.asyncio
async def test_mark_done_sets_completed_at():
    conn = FakeConn()
    db = FakeDB(conn)
    created = await createActionItem(db, {"tenantId": "default", "userId": "u1", "kind": "todo", "title": "X"})

    out = await markActionItemDone(db, tenant_id="default", user_id="u1", action_item_id=created["id"])

    assert out["status"] == "done"
    assert out["completedAt"] is not None


@pytest.mark.asyncio
async def test_dismiss_sets_dismissed_at():
    conn = FakeConn()
    db = FakeDB(conn)
    created = await createActionItem(db, {"tenantId": "default", "userId": "u1", "kind": "todo", "title": "X"})

    out = await dismissActionItem(db, tenant_id="default", user_id="u1", action_item_id=created["id"])

    assert out["status"] == "dismissed"
    assert out["dismissedAt"] is not None


@pytest.mark.asyncio
async def test_promote_candidate_creates_item_and_links_candidate():
    conn = FakeConn()
    conn.candidates[7] = {
        "candidate_id": 7,
        "tenant_id": "default",
        "user_id": "u1",
        "candidate_key": "c7",
        "candidate_subtype": "todo",
        "title": "Follow up with Sam",
        "summary": "Need follow-up",
        "status": "detected",
        "confidence_score": 0.8,
        "due_iso": None,
        "proposed_due_at": None,
        "proposed_remind_at": None,
        "provenance_summary": None,
    }
    db = FakeDB(conn)

    out = await promoteCandidateToActionItem(db, tenant_id="default", user_id="u1", candidate_id=7)

    assert out["actionItem"]["kind"] == "todo"
    assert out["candidate"]["status"] == "confirmed"


@pytest.mark.asyncio
async def test_live_confirm_links_existing_background_candidate():
    conn = FakeConn()
    conn.candidates[9] = {
        "candidate_id": 9,
        "tenant_id": "default",
        "user_id": "u1",
        "record_type": "reminder_candidate",
        "candidate_key": "c9",
        "candidate_subtype": "reminder",
        "title": "Call John tomorrow",
        "summary": "User mentioned calling John tomorrow.",
        "status": "needs_review",
        "confidence_score": 0.65,
        "due_iso": datetime(2026, 5, 2, 9, 0, tzinfo=timezone.utc),
        "proposed_due_at": None,
        "proposed_remind_at": None,
        "provenance_summary": None,
        "metadata": {},
        "updated_at": datetime.now(timezone.utc),
    }
    db = FakeDB(conn)

    out = await createActionItem(
        db,
        {
            "tenantId": "default",
            "userId": "u1",
            "kind": "reminder",
            "title": "Call John",
            "remindAt": datetime(2026, 5, 2, 9, 0, tzinfo=timezone.utc),
            "sourceType": "direct_user",
        },
    )

    assert out["kind"] == "reminder"
    assert conn.candidates[9]["status"] == "confirmed"
    assert conn.candidates[9]["promoted_action_item_id"] == out["id"]
    assert conn.candidates[9]["metadata"]["reconciliation_action"] == "confirmed_by_action_item"


@pytest.mark.asyncio
async def test_daily_agenda_groups_due_reminders_and_overdue():
    conn = FakeConn()
    db = FakeDB(conn)
    db.fetch_rows = [
        {
            "id": uuid4(),
            "tenant_id": "default",
            "user_id": "u1",
            "kind": "todo",
            "title": "Due today",
            "notes": None,
            "status": "pending",
            "due_at": datetime(2026, 4, 28, 12, 0, tzinfo=timezone.utc),
            "remind_at": None,
            "recurrence_rule": None,
            "source_type": "direct_user",
            "source_ref": None,
            "confidence": None,
            "provenance_summary": None,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
            "completed_at": None,
            "dismissed_at": None,
            "cancelled_at": None,
        },
        {
            "id": uuid4(),
            "tenant_id": "default",
            "user_id": "u1",
            "kind": "reminder",
            "title": "Reminder",
            "notes": None,
            "status": "pending",
            "due_at": None,
            "remind_at": datetime(2026, 4, 28, 9, 0, tzinfo=timezone.utc),
            "recurrence_rule": None,
            "source_type": "direct_user",
            "source_ref": None,
            "confidence": None,
            "provenance_summary": None,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
            "completed_at": None,
            "dismissed_at": None,
            "cancelled_at": None,
        },
        {
            "id": uuid4(),
            "tenant_id": "default",
            "user_id": "u1",
            "kind": "todo",
            "title": "Overdue",
            "notes": None,
            "status": "pending",
            "due_at": datetime(2026, 4, 27, 9, 0, tzinfo=timezone.utc),
            "remind_at": None,
            "recurrence_rule": None,
            "source_type": "direct_user",
            "source_ref": None,
            "confidence": None,
            "provenance_summary": None,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
            "completed_at": None,
            "dismissed_at": None,
            "cancelled_at": None,
        },
    ]
    db.candidate_rows = [
        {
            "candidate_id": 1,
            "title": "Candidate",
            "status": "needs_review",
            "candidate_subtype": "todo",
            "proposed_due_at": None,
            "proposed_remind_at": None,
            "provenance_summary": None,
            "confidence": 0.8,
            "metadata": {},
            "provenance": {},
        }
    ]

    out = await listDailyAgenda(db, tenant_id="default", user_id="u1", day=date(2026, 4, 28).isoformat(), timezone_name="UTC")

    assert out["counts"]["dueToday"] == 1
    assert out["counts"]["remindersToday"] == 1
    assert out["counts"]["overdue"] == 1
    assert out["counts"]["pendingCandidates"] == 1


@pytest.mark.asyncio
async def test_needs_review_candidate_promotion_allowed():
    conn = FakeConn()
    conn.candidates[9] = {
        "candidate_id": 9,
        "tenant_id": "default",
        "user_id": "u1",
        "candidate_key": "c9",
        "candidate_subtype": "reminder",
        "title": "Ping Alex",
        "summary": "Reminder from chat",
        "status": "needs_review",
        "confidence": 0.9,
        "confidence_score": 0.9,
        "due_iso": None,
        "proposed_due_at": None,
        "proposed_remind_at": None,
        "provenance_summary": "user asked directly",
        "provenance": {"has_direct_user_expression": True},
        "metadata": {"has_direct_user_expression": True},
    }
    db = FakeDB(conn)

    out = await promoteCandidateToActionItem(db, tenant_id="default", user_id="u1", candidate_id=9)
    assert out["candidate"]["status"] == "confirmed"


def test_auto_promote_policy_true_for_high_confidence_low_risk():
    candidate = {
        "candidate_subtype": "todo",
        "confidence": 0.9,
        "metadata": {"has_direct_user_expression": True},
        "provenance": {},
    }
    assert shouldAutoPromoteCandidate(candidate) is True


def test_auto_promote_policy_false_for_calendar_event_or_low_confidence():
    calendar_candidate = {
        "candidate_subtype": "calendar_event",
        "confidence": 0.99,
        "metadata": {"has_direct_user_expression": True},
        "provenance": {},
    }
    low_confidence_candidate = {
        "candidate_subtype": "todo",
        "confidence": 0.6,
        "metadata": {"has_direct_user_expression": True},
        "provenance": {},
    }
    assert shouldAutoPromoteCandidate(calendar_candidate) is False
    assert shouldAutoPromoteCandidate(low_confidence_candidate) is False


@pytest.mark.asyncio
async def test_auto_promote_writes_system_audit_row():
    conn = FakeConn()
    conn.candidates[11] = {
        "candidate_id": 11,
        "tenant_id": "default",
        "user_id": "u1",
        "candidate_key": "c11",
        "candidate_subtype": "todo",
        "title": "Draft follow-up",
        "summary": "User requested this directly",
        "status": "detected",
        "confidence": 0.95,
        "confidence_score": 0.95,
        "due_iso": None,
        "proposed_due_at": None,
        "proposed_remind_at": None,
        "provenance_summary": "direct_user_expression",
        "provenance": {"has_direct_user_expression": True},
        "metadata": {"has_direct_user_expression": True},
    }
    db = FakeDB(conn)

    out = await maybeAutoPromoteCandidate(db, tenant_id="default", user_id="u1", candidate_id=11)
    assert out["autoPromoted"] is True
    assert any(row["action"] == "auto_promote" for row in conn.audit_rows)


@pytest.mark.asyncio
async def test_list_all_pending_tasks():
    conn = FakeConn()
    db = FakeDB(conn)
    db.fetch_rows = [
        {"id": uuid4(), "tenant_id": "default", "user_id": "u1", "kind": "todo", "title": "A", "notes": None, "status": "pending", "due_at": None, "remind_at": None, "recurrence_rule": None, "source_type": "direct_user", "source_ref": None, "confidence": None, "provenance_summary": None, "created_at": datetime.now(timezone.utc), "updated_at": datetime.now(timezone.utc), "completed_at": None, "dismissed_at": None, "cancelled_at": None},
        {"id": uuid4(), "tenant_id": "default", "user_id": "u1", "kind": "todo", "title": "B", "notes": None, "status": "done", "due_at": None, "remind_at": None, "recurrence_rule": None, "source_type": "direct_user", "source_ref": None, "confidence": None, "provenance_summary": None, "created_at": datetime.now(timezone.utc), "updated_at": datetime.now(timezone.utc), "completed_at": datetime.now(timezone.utc), "dismissed_at": None, "cancelled_at": None},
    ]
    out = await listActionItems(db, {"tenantId": "default", "userId": "u1"})
    assert len(out) == 1
    assert out[0]["status"] == "pending"


@pytest.mark.asyncio
async def test_filter_by_kind_todo_reminder():
    conn = FakeConn()
    db = FakeDB(conn)
    db.fetch_rows = [
        {"id": uuid4(), "tenant_id": "default", "user_id": "u1", "kind": "todo", "title": "Todo", "notes": None, "status": "pending", "due_at": None, "remind_at": None, "recurrence_rule": None, "source_type": "direct_user", "source_ref": None, "confidence": None, "provenance_summary": None, "created_at": datetime.now(timezone.utc), "updated_at": datetime.now(timezone.utc), "completed_at": None, "dismissed_at": None, "cancelled_at": None},
        {"id": uuid4(), "tenant_id": "default", "user_id": "u1", "kind": "reminder", "title": "Reminder", "notes": None, "status": "pending", "due_at": None, "remind_at": None, "recurrence_rule": None, "source_type": "direct_user", "source_ref": None, "confidence": None, "provenance_summary": None, "created_at": datetime.now(timezone.utc), "updated_at": datetime.now(timezone.utc), "completed_at": None, "dismissed_at": None, "cancelled_at": None},
    ]
    todos = await listActionItems(db, {"tenantId": "default", "userId": "u1", "kind": "todo"})
    reminders = await listActionItems(db, {"tenantId": "default", "userId": "u1", "kind": "reminder"})
    assert len(todos) == 1
    assert todos[0]["kind"] == "todo"
    assert len(reminders) == 1
    assert reminders[0]["kind"] == "reminder"


@pytest.mark.asyncio
async def test_edit_title_due_at_remind_at_and_audit_old_new():
    conn = FakeConn()
    db = FakeDB(conn)
    created = await createActionItem(db, {"tenantId": "default", "userId": "u1", "kind": "todo", "title": "Old title"})
    out = await updateActionItem(
        db,
        tenant_id="default",
        user_id="u1",
        action_item_id=created["id"],
        patch={
            "title": "New title",
            "dueAt": datetime(2026, 4, 29, 10, 0, tzinfo=timezone.utc),
            "remindAt": datetime(2026, 4, 29, 9, 0, tzinfo=timezone.utc),
        },
        actor="user",
    )
    assert out["title"] == "New title"
    assert out["dueAt"] is not None
    assert out["remindAt"] is not None
    assert conn.audit_rows[-1]["action"] == "update"


@pytest.mark.asyncio
async def test_adding_remind_at_to_todo():
    conn = FakeConn()
    db = FakeDB(conn)
    created = await createActionItem(db, {"tenantId": "default", "userId": "u1", "kind": "todo", "title": "T"})
    out = await updateActionItem(
        db,
        tenant_id="default",
        user_id="u1",
        action_item_id=created["id"],
        patch={"remindAt": datetime(2026, 4, 29, 8, 0, tzinfo=timezone.utc)},
        actor="user",
    )
    assert out["kind"] == "todo"
    assert out["remindAt"] is not None


@pytest.mark.asyncio
async def test_dismiss_instead_of_hard_delete():
    conn = FakeConn()
    db = FakeDB(conn)
    created = await createActionItem(db, {"tenantId": "default", "userId": "u1", "kind": "todo", "title": "Remove me"})
    out = await dismissActionItem(db, tenant_id="default", user_id="u1", action_item_id=created["id"])
    assert out["status"] == "dismissed"
    assert created["id"] in conn.items


@pytest.mark.asyncio
async def test_archive_soft_delete_sets_archived():
    conn = FakeConn()
    db = FakeDB(conn)
    created = await createActionItem(db, {"tenantId": "default", "userId": "u1", "kind": "todo", "title": "Archive me"})
    out = await archiveActionItem(db, tenant_id="default", user_id="u1", action_item_id=created["id"], actor="user")
    assert out["status"] == "archived"
    assert created["id"] in conn.items


@pytest.mark.asyncio
async def test_filter_by_title_search():
    conn = FakeConn()
    db = FakeDB(conn)
    db.fetch_rows = [
        {"id": uuid4(), "tenant_id": "default", "user_id": "u1", "kind": "todo", "title": "Book dentist", "notes": None, "status": "pending", "due_at": None, "remind_at": None, "recurrence_rule": None, "source_type": "direct_user", "source_ref": None, "confidence": None, "provenance_summary": None, "created_at": datetime.now(timezone.utc), "updated_at": datetime.now(timezone.utc), "completed_at": None, "dismissed_at": None, "cancelled_at": None},
        {"id": uuid4(), "tenant_id": "default", "user_id": "u1", "kind": "todo", "title": "Call mom", "notes": None, "status": "pending", "due_at": None, "remind_at": None, "recurrence_rule": None, "source_type": "direct_user", "source_ref": None, "confidence": None, "provenance_summary": None, "created_at": datetime.now(timezone.utc), "updated_at": datetime.now(timezone.utc), "completed_at": None, "dismissed_at": None, "cancelled_at": None},
    ]
    out = await listActionItems(db, {"tenantId": "default", "userId": "u1", "title": "dent"})
    assert len(out) == 1
    assert out[0]["title"] == "Book dentist"
