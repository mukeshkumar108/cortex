from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List
from uuid import uuid4

import pytest

from src.calendar_state import (
    CalendarStateError,
    archiveCalendarItem,
    cancelCalendarItem,
    createCalendarItem,
    listCalendarItems,
    updateCalendarItem,
)


class _Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self):
        self.items: Dict[str, Dict[str, Any]] = {}
        self.audit_rows: List[Dict[str, Any]] = []

    def transaction(self):
        return _Tx()

    async def fetchrow(self, query: str, *args):
        if "INSERT INTO calendar_items" in query:
            item_id = str(uuid4())
            now = datetime.now(timezone.utc)
            row = {
                "id": item_id,
                "tenant_id": args[0],
                "user_id": args[1],
                "title": args[2],
                "description": args[3],
                "notes": args[4],
                "starts_at": args[5],
                "ends_at": args[6],
                "timezone": args[7],
                "all_day": args[8],
                "location": args[9],
                "participants": args[10],
                "organizer": args[11],
                "rsvp_status": args[12],
                "status": args[13],
                "source_kind": args[14],
                "source_ref": args[15],
                "evidence_refs": args[16],
                "provenance_summary": args[17],
                "confidence": args[18],
                "external_provider": args[19],
                "external_id": args[20],
                "external_calendar_id": args[21],
                "external_etag": args[22],
                "external_updated_at": args[23],
                "sync_status": args[24],
                "recurrence_rule": args[25],
                "recurrence_parent_id": args[26],
                "dedupe_key": args[27],
                "metadata": args[28],
                "created_at": now,
                "updated_at": now,
                "cancelled_at": None,
                "archived_at": None,
            }
            self.items[item_id] = row
            return row

        if "SELECT * FROM calendar_items" in query:
            item_id = str(args[2])
            row = self.items.get(item_id)
            if not row:
                return None
            if row["tenant_id"] != args[0] or row["user_id"] != args[1]:
                return None
            return row

        if "UPDATE calendar_items" in query:
            item_id = str(args[2])
            row = self.items[item_id]
            fields = [
                "title",
                "description",
                "notes",
                "starts_at",
                "ends_at",
                "timezone",
                "all_day",
                "location",
                "participants",
                "organizer",
                "rsvp_status",
                "status",
                "source_kind",
                "source_ref",
                "evidence_refs",
                "provenance_summary",
                "confidence",
                "external_provider",
                "external_id",
                "external_calendar_id",
                "external_etag",
                "external_updated_at",
                "sync_status",
                "recurrence_rule",
                "recurrence_parent_id",
                "dedupe_key",
                "metadata",
                "cancelled_at",
                "archived_at",
            ]
            for idx, field in enumerate(fields, start=3):
                row[field] = args[idx]
            row["updated_at"] = datetime.now(timezone.utc)
            return row

        return None

    async def execute(self, query: str, *args):
        if "INSERT INTO calendar_audit_log" in query:
            self.audit_rows.append(
                {
                    "tenant_id": args[0],
                    "user_id": args[1],
                    "object_id": args[2],
                    "action": args[3],
                    "actor": args[6],
                }
            )
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
        self.last_query = ""
        self.last_args = ()

    async def get_pool(self):
        return FakePool(self.conn)

    async def fetch(self, query: str, *args):
        self.last_query = query
        self.last_args = args
        if "FROM calendar_items" not in query:
            return []
        rows = [r for r in self.fetch_rows if r.get("tenant_id") == args[0] and r.get("user_id") == args[1]]
        if "status <> 'cancelled'" in query:
            rows = [r for r in rows if r.get("status") != "cancelled"]
        if "status <> 'archived'" in query:
            rows = [r for r in rows if r.get("status") != "archived"]
        if "status=$" in query:
            expected_status = str(args[2]).lower()
            rows = [r for r in rows if r.get("status") == expected_status]
        return rows


@pytest.mark.asyncio
async def test_create_calendar_item_writes_audit():
    conn = FakeConn()
    db = FakeDB(conn)
    starts_at = datetime(2026, 5, 5, 9, 0, tzinfo=timezone.utc)

    out = await createCalendarItem(
        db,
        {
            "tenantId": "default",
            "userId": "u1",
            "title": "Meeting with Sarah",
            "startsAt": starts_at,
            "endsAt": starts_at + timedelta(hours=1),
            "participants": [{"name": "Sarah"}],
            "sourceKind": "chat",
            "sourceRef": {"sessionId": "s1"},
        },
    )

    assert out["status"] == "confirmed"
    assert out["sourceKind"] == "chat"
    assert out["participants"] == [{"name": "Sarah"}]
    assert len(conn.audit_rows) == 1
    assert conn.audit_rows[0]["action"] == "create"


@pytest.mark.asyncio
async def test_list_calendar_items_hides_cancelled_and_archived_by_default():
    conn = FakeConn()
    db = FakeDB(conn)
    now = datetime.now(timezone.utc)
    db.fetch_rows = [
        {"id": uuid4(), "tenant_id": "default", "user_id": "u1", "title": "A", "starts_at": now, "status": "confirmed"},
        {"id": uuid4(), "tenant_id": "default", "user_id": "u1", "title": "B", "starts_at": now, "status": "cancelled"},
        {"id": uuid4(), "tenant_id": "default", "user_id": "u1", "title": "C", "starts_at": now, "status": "archived"},
        {"id": uuid4(), "tenant_id": "default", "user_id": "u2", "title": "D", "starts_at": now, "status": "confirmed"},
    ]

    out = await listCalendarItems(db, {"tenantId": "default", "userId": "u1"})

    assert [item["title"] for item in out] == ["A"]
    assert "tenant_id=$1" in db.last_query
    assert "user_id=$2" in db.last_query


@pytest.mark.asyncio
async def test_list_calendar_items_applies_range_overlap_filters():
    conn = FakeConn()
    db = FakeDB(conn)
    await listCalendarItems(
        db,
        {
            "tenantId": "default",
            "userId": "u1",
            "date_from": datetime(2026, 5, 1, tzinfo=timezone.utc),
            "date_to": datetime(2026, 5, 2, tzinfo=timezone.utc),
        },
    )

    assert "COALESCE(ends_at, starts_at) >=" in db.last_query
    assert "starts_at <=" in db.last_query


@pytest.mark.asyncio
async def test_patch_calendar_item_updates_fields_and_audits():
    conn = FakeConn()
    db = FakeDB(conn)
    created = await createCalendarItem(
        db,
        {
            "tenantId": "default",
            "userId": "u1",
            "title": "Dinner",
            "startsAt": datetime(2026, 5, 7, 17, 0, tzinfo=timezone.utc),
        },
    )

    out = await updateCalendarItem(
        db,
        tenant_id="default",
        user_id="u1",
        calendar_item_id=created["id"],
        patch={"title": "Dinner with James", "location": "Downtown"},
    )

    assert out["title"] == "Dinner with James"
    assert out["location"] == "Downtown"
    assert conn.audit_rows[-1]["action"] == "update"


@pytest.mark.asyncio
async def test_cancel_and_archive_calendar_item_set_timestamps():
    conn = FakeConn()
    db = FakeDB(conn)
    created = await createCalendarItem(
        db,
        {
            "tenantId": "default",
            "userId": "u1",
            "title": "Call",
            "startsAt": datetime(2026, 5, 8, 10, 0, tzinfo=timezone.utc),
        },
    )

    cancelled = await cancelCalendarItem(db, tenant_id="default", user_id="u1", calendar_item_id=created["id"])
    assert cancelled["status"] == "cancelled"
    assert cancelled["cancelledAt"] is not None

    archived = await archiveCalendarItem(db, tenant_id="default", user_id="u1", calendar_item_id=created["id"])
    assert archived["status"] == "archived"
    assert archived["archivedAt"] is not None


@pytest.mark.asyncio
async def test_cross_user_update_is_not_found():
    conn = FakeConn()
    db = FakeDB(conn)
    created = await createCalendarItem(
        db,
        {
            "tenantId": "default",
            "userId": "u1",
            "title": "Private event",
            "startsAt": datetime(2026, 5, 8, 10, 0, tzinfo=timezone.utc),
        },
    )

    with pytest.raises(CalendarStateError) as exc:
        await updateCalendarItem(
            db,
            tenant_id="default",
            user_id="u2",
            calendar_item_id=created["id"],
            patch={"title": "Nope"},
        )

    assert exc.value.status_code == 404
