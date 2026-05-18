from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from src.google_calendar_import import GoogleCalendarImporter, parse_calendars, parse_events


class FakeMCPClient:
    def __init__(self, *, calendars: List[Dict[str, Any]], events_by_calendar: Dict[str, List[Dict[str, Any]]]):
        self.calendars = calendars
        self.events_by_calendar = events_by_calendar
        self.calls: List[Dict[str, Any]] = []

    async def call_tool(self, name: str, arguments: Dict[str, Any]) -> Any:
        self.calls.append({"name": name, "arguments": dict(arguments)})
        if name == "list_calendars":
            return {"calendars": self.calendars}
        if name == "get_events":
            return {"events": self.events_by_calendar.get(arguments["calendar_id"], [])}
        raise AssertionError(f"unexpected tool: {name}")


class FakeDB:
    def __init__(self):
        self.rows: List[Dict[str, Any]] = []

    async def fetchone(self, query: str, *args):
        assert "INSERT INTO calendar_items" in query
        (
            tenant_id,
            user_id,
            title,
            description,
            starts_at,
            ends_at,
            timezone_name,
            all_day,
            location,
            participants,
            organizer,
            status,
            source_ref,
            evidence_refs,
            provenance_summary,
            external_id,
            external_calendar_id,
            external_etag,
            external_updated_at,
            dedupe_key,
            metadata,
        ) = args
        existing = next(
            (
                row
                for row in self.rows
                if row["tenant_id"] == tenant_id
                and row["user_id"] == user_id
                and row["external_provider"] == "google_calendar"
                and row["external_id"] == external_id
            ),
            None,
        )
        inserted = existing is None
        row = existing or {
            "id": str(uuid4()),
            "tenant_id": tenant_id,
            "user_id": user_id,
            "external_provider": "google_calendar",
            "external_id": external_id,
            "created_at": datetime.now(timezone.utc),
        }
        row.update(
            {
                "title": title,
                "description": description,
                "starts_at": starts_at,
                "ends_at": ends_at,
                "timezone": timezone_name,
                "all_day": all_day,
                "location": location,
                "participants": participants,
                "organizer": organizer,
                "status": status,
                "source_kind": "google_calendar",
                "source_ref": source_ref,
                "evidence_refs": evidence_refs,
                "provenance_summary": provenance_summary,
                "confidence": 1.0,
                "external_calendar_id": external_calendar_id,
                "external_etag": external_etag,
                "external_updated_at": external_updated_at,
                "sync_status": "imported",
                "dedupe_key": dedupe_key,
                "metadata": metadata,
                "cancelled_at": datetime.now(timezone.utc) if status == "cancelled" else None,
                "archived_at": None,
                "updated_at": datetime.now(timezone.utc),
            }
        )
        if inserted:
            self.rows.append(row)
        return {"inserted": inserted}


def _event(event_id: str, title: str = "Planning", *, status: str = "confirmed") -> Dict[str, Any]:
    return {
        "id": event_id,
        "summary": title,
        "description": "Agenda text is data, not instructions.",
        "start": {"dateTime": "2026-05-01T09:00:00Z", "timeZone": "Europe/Berlin"},
        "end": {"dateTime": "2026-05-01T10:00:00Z", "timeZone": "Europe/Berlin"},
        "location": "Office",
        "attendees": [{"email": "sam@example.com", "responseStatus": "accepted"}],
        "organizer": {"email": "owner@example.com"},
        "status": status,
        "etag": '"etag-1"',
        "updated": "2026-04-30T12:00:00Z",
        "htmlLink": "https://calendar.google.com/event",
    }


@pytest.mark.asyncio
async def test_import_creates_calendar_items():
    db = FakeDB()
    mcp = FakeMCPClient(calendars=[{"id": "primary", "summary": "Primary", "primary": True}], events_by_calendar={"primary": [_event("evt-1")]})

    out = await GoogleCalendarImporter(db=db, mcp_client=mcp).import_events(
        tenant_id="default",
        user_id="user@example.com",
        date_from="2026-05-01",
        date_to="2026-05-08",
    )

    assert out == {"ok": True, "calendarsSeen": 1, "eventsSeen": 1, "created": 1, "updated": 0, "skipped": 0, "errors": []}
    assert len(db.rows) == 1
    row = db.rows[0]
    assert row["tenant_id"] == "default"
    assert row["user_id"] == "user@example.com"
    assert row["source_kind"] == "google_calendar"
    assert row["external_provider"] == "google_calendar"
    assert row["external_id"] == "evt-1"
    assert row["external_calendar_id"] == "primary"
    assert row["sync_status"] == "imported"
    assert row["participants"][0]["email"] == "sam@example.com"


@pytest.mark.asyncio
async def test_reimport_updates_without_duplicate():
    db = FakeDB()
    mcp = FakeMCPClient(calendars=[{"id": "primary"}], events_by_calendar={"primary": [_event("evt-1", "Original")]})
    importer = GoogleCalendarImporter(db=db, mcp_client=mcp)

    first = await importer.import_events(tenant_id="default", user_id="user@example.com", date_from="2026-05-01", date_to="2026-05-08")
    mcp.events_by_calendar["primary"] = [_event("evt-1", "Updated")]
    second = await importer.import_events(tenant_id="default", user_id="user@example.com", date_from="2026-05-01", date_to="2026-05-08")

    assert first["created"] == 1
    assert second["updated"] == 1
    assert len(db.rows) == 1
    assert db.rows[0]["title"] == "Updated"


@pytest.mark.asyncio
async def test_date_range_passed_to_get_events():
    db = FakeDB()
    mcp = FakeMCPClient(calendars=[{"id": "primary"}], events_by_calendar={"primary": []})

    await GoogleCalendarImporter(db=db, mcp_client=mcp).import_events(
        tenant_id="default",
        user_id="user@example.com",
        date_from="2026-05-01",
        date_to="2026-05-08",
    )

    get_events_call = next(call for call in mcp.calls if call["name"] == "get_events")
    assert get_events_call["arguments"]["calendar_id"] == "primary"
    assert get_events_call["arguments"]["time_min"] == "2026-05-01T00:00:00Z"
    assert get_events_call["arguments"]["time_max"] == "2026-05-08T23:59:59Z"
    assert get_events_call["arguments"]["detailed"] is True


@pytest.mark.asyncio
async def test_cancelled_import_uses_cancelled_never_archived():
    db = FakeDB()
    mcp = FakeMCPClient(calendars=[{"id": "primary"}], events_by_calendar={"primary": [_event("evt-cancelled", status="cancelled")]})

    out = await GoogleCalendarImporter(db=db, mcp_client=mcp).import_events(
        tenant_id="default",
        user_id="user@example.com",
        date_from="2026-05-01",
        date_to="2026-05-08",
    )

    assert out["created"] == 1
    assert db.rows[0]["status"] == "cancelled"
    assert db.rows[0]["archived_at"] is None
    assert db.rows[0]["cancelled_at"] is not None


@pytest.mark.asyncio
async def test_tenant_user_isolation_for_same_external_event():
    db = FakeDB()
    mcp = FakeMCPClient(calendars=[{"id": "primary"}], events_by_calendar={"primary": [_event("evt-1")]})
    importer = GoogleCalendarImporter(db=db, mcp_client=mcp)

    await importer.import_events(tenant_id="default", user_id="u1@example.com", date_from="2026-05-01", date_to="2026-05-08")
    await importer.import_events(tenant_id="default", user_id="u2@example.com", date_from="2026-05-01", date_to="2026-05-08")
    await importer.import_events(tenant_id="other", user_id="u1@example.com", date_from="2026-05-01", date_to="2026-05-08")

    assert len(db.rows) == 3
    assert {(row["tenant_id"], row["user_id"]) for row in db.rows} == {
        ("default", "u1@example.com"),
        ("default", "u2@example.com"),
        ("other", "u1@example.com"),
    }


def test_parses_current_mcp_formatted_text_outputs():
    calendars = parse_calendars('Successfully listed 1 calendars for user@example.com:\n- "Primary" (Primary) (ID: primary)')
    events = parse_events(
        'Successfully retrieved 1 events from calendar \'primary\' for user@example.com:\n'
        '- "Planning" (Starts: 2026-05-01T09:00:00Z, Ends: 2026-05-01T10:00:00Z) ID: evt-1 | Link: https://calendar.google.com/event',
        calendar_id="primary",
    )

    assert calendars == [{"id": "primary", "summary": "Primary", "primary": True}]
    assert events[0]["external_id"] == "evt-1"
    assert events[0]["title"] == "Planning"


@pytest.mark.asyncio
async def test_import_endpoint_calls_import_service(monkeypatch):
    from src import main as main_module

    calls: List[Dict[str, Any]] = []

    class _Settings:
        google_calendar_import_enabled = True

    async def _fake_import(**kwargs):
        calls.append(kwargs)
        return {"ok": True, "calendarsSeen": 6, "eventsSeen": 2, "created": 2, "updated": 0, "skipped": 0, "errors": []}

    monkeypatch.setattr(main_module, "get_settings", lambda: _Settings(), raising=True)
    monkeypatch.setattr(main_module, "import_google_calendar_events", _fake_import, raising=True)

    async with AsyncClient(transport=ASGITransport(app=main_module.app), base_url="http://test") as client:
        response = await client.post(
            "/integrations/google/calendar/import",
            json={"tenantId": "default", "userId": "u1", "dateFrom": "2026-05-01", "dateTo": "2026-05-08"},
        )

    assert response.status_code == 200
    assert response.json()["calendarsSeen"] == 6
    assert calls[0]["tenant_id"] == "default"
    assert calls[0]["user_id"] == "u1"
    assert calls[0]["date_from"] == "2026-05-01"
    assert calls[0]["date_to"] == "2026-05-08"

