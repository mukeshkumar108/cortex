from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
import re
from typing import Any, Dict, List, Optional, Sequence

from .config import Settings, get_settings
from .db import Database
from .workspace_mcp_client import WorkspaceMCPClient, WorkspaceMCPError


GOOGLE_CALENDAR_PROVIDER = "google_calendar"


class GoogleCalendarImportError(RuntimeError):
    pass


@dataclass
class CalendarImportResult:
    ok: bool = True
    calendarsSeen: int = 0
    eventsSeen: int = 0
    created: int = 0
    updated: int = 0
    skipped: int = 0
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "calendarsSeen": self.calendarsSeen,
            "eventsSeen": self.eventsSeen,
            "created": self.created,
            "updated": self.updated,
            "skipped": self.skipped,
            "errors": self.errors,
        }


def _clean_text(value: Any, *, limit: int = 2000) -> Optional[str]:
    if value is None:
        return None
    text = str(value).replace("\x00", " ").strip()
    if not text:
        return None
    return re.sub(r"\s+", " ", text)[:limit]


def _parse_datetime(value: Any, *, all_day: bool = False) -> Optional[datetime]:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime.combine(value, time.min)
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if len(raw) == 10 and raw.count("-") == 2:
            dt = datetime.fromisoformat(raw)
            all_day = True
        else:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00").replace("z", "+00:00"))
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _date_param_to_rfc3339(value: str, *, end: bool = False) -> str:
    raw = (value or "").strip()
    if len(raw) == 10 and raw.count("-") == 2:
        suffix = "T23:59:59Z" if end else "T00:00:00Z"
        return f"{raw}{suffix}"
    dt = _parse_datetime(raw)
    if dt is None:
        raise GoogleCalendarImportError("Invalid date range")
    return dt.isoformat().replace("+00:00", "Z")


def _extract_result_list(payload: Any, keys: Sequence[str]) -> Optional[List[Dict[str, Any]]]:
    if isinstance(payload, list):
        rows = [dict(x) for x in payload if isinstance(x, dict)]
        return rows
    if isinstance(payload, dict):
        for key in keys:
            value = payload.get(key)
            if isinstance(value, list):
                return [dict(x) for x in value if isinstance(x, dict)]
        result = payload.get("result")
        if isinstance(result, (dict, list)):
            return _extract_result_list(result, keys)
    return None


def parse_calendars(payload: Any) -> List[Dict[str, Any]]:
    structured = _extract_result_list(payload, ("calendars", "items"))
    if structured is not None:
        out = []
        for row in structured:
            cal_id = _clean_text(row.get("id") or row.get("calendar_id") or row.get("calendarId"), limit=512)
            if not cal_id:
                continue
            out.append(
                {
                    "id": cal_id,
                    "summary": _clean_text(row.get("summary") or row.get("name"), limit=500) or cal_id,
                    "primary": bool(row.get("primary")),
                }
            )
        return out

    text = str(payload or "")
    calendars: List[Dict[str, Any]] = []
    for line in text.splitlines():
        match = re.search(r'-\s+"(?P<summary>.*?)"(?P<primary>\s+\(Primary\))?\s+\(ID:\s*(?P<id>.*?)\)\s*$', line.strip())
        if not match:
            continue
        calendars.append(
            {
                "id": match.group("id").strip(),
                "summary": match.group("summary").strip(),
                "primary": bool(match.group("primary")),
            }
        )
    return calendars


def _google_event_time(value: Any) -> tuple[Optional[datetime], bool, str]:
    if isinstance(value, dict):
        raw = value.get("dateTime") or value.get("date")
        all_day = bool(value.get("date")) and not bool(value.get("dateTime"))
        tz = _clean_text(value.get("timeZone"), limit=128) or "UTC"
    else:
        raw = value
        all_day = isinstance(value, str) and len(value.strip()) == 10 and value.strip().count("-") == 2
        tz = "UTC"
    return (_parse_datetime(raw, all_day=all_day), all_day, tz)


def _normalize_event(row: Dict[str, Any], *, calendar_id: str) -> Optional[Dict[str, Any]]:
    event_id = _clean_text(row.get("id") or row.get("event_id") or row.get("eventId"), limit=512)
    title = _clean_text(row.get("summary") or row.get("title"), limit=500)
    starts_at, start_all_day, start_tz = _google_event_time(row.get("start") or row.get("starts_at") or row.get("startsAt"))
    ends_at, end_all_day, end_tz = _google_event_time(row.get("end") or row.get("ends_at") or row.get("endsAt"))
    if not event_id or not starts_at:
        return None
    attendees = row.get("attendees") if isinstance(row.get("attendees"), list) else []
    participants = [
        {
            "email": _clean_text(item.get("email"), limit=320),
            "name": _clean_text(item.get("displayName") or item.get("name"), limit=320),
            "responseStatus": _clean_text(item.get("responseStatus"), limit=64),
            "optional": bool(item.get("optional")),
            "organizer": bool(item.get("organizer")),
        }
        for item in attendees
        if isinstance(item, dict)
    ]
    organizer = row.get("organizer") if isinstance(row.get("organizer"), dict) else None
    status = "cancelled" if str(row.get("status") or "").strip().lower() == "cancelled" else "confirmed"
    return {
        "external_id": event_id,
        "external_calendar_id": _clean_text(row.get("calendar_id") or row.get("calendarId") or calendar_id, limit=512) or calendar_id,
        "title": title or "Untitled event",
        "description": _clean_text(row.get("description"), limit=8000),
        "starts_at": starts_at,
        "ends_at": ends_at,
        "timezone": start_tz or end_tz or "UTC",
        "all_day": start_all_day or end_all_day,
        "location": _clean_text(row.get("location"), limit=1000),
        "participants": participants,
        "organizer": organizer,
        "status": status,
        "external_etag": _clean_text(row.get("etag"), limit=512),
        "external_updated_at": _parse_datetime(row.get("updated")) if row.get("updated") else None,
        "html_link": _clean_text(row.get("htmlLink") or row.get("link"), limit=1200),
    }


def parse_events(payload: Any, *, calendar_id: str) -> List[Dict[str, Any]]:
    structured = _extract_result_list(payload, ("events", "items"))
    if structured is not None:
        return [event for row in structured if (event := _normalize_event(row, calendar_id=calendar_id))]

    text = str(payload or "")
    events: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None
    basic_pattern = re.compile(
        r'^-\s+"(?P<title>.*?)"\s+\(Starts:\s*(?P<start>.*?),\s*Ends:\s*(?P<end>.*?)\)(?:\s+Meeting:\s*(?P<meeting>.*?))?\s+ID:\s*(?P<id>.*?)\s+\|\s+Link:\s*(?P<link>.*)$'
    )
    for line in text.splitlines():
        stripped = line.strip()
        match = basic_pattern.match(stripped)
        if match:
            if current:
                events.append(current)
            starts_at = _parse_datetime(match.group("start"))
            if not starts_at:
                current = None
                continue
            current = {
                "external_id": match.group("id").strip(),
                "external_calendar_id": calendar_id,
                "title": match.group("title").strip() or "Untitled event",
                "description": None,
                "starts_at": starts_at,
                "ends_at": _parse_datetime(match.group("end")),
                "timezone": "UTC",
                "all_day": len(match.group("start").strip()) == 10,
                "location": None,
                "participants": [],
                "organizer": None,
                "status": "confirmed",
                "external_etag": None,
                "external_updated_at": None,
                "html_link": match.group("link").strip(),
            }
            continue
        if current and stripped.startswith("Description:"):
            current["description"] = _clean_text(stripped.split(":", 1)[1], limit=8000)
        elif current and stripped.startswith("Location:"):
            value = _clean_text(stripped.split(":", 1)[1], limit=1000)
            current["location"] = None if value in {"No Location", "None"} else value
        elif current and stripped.startswith("Attendees:"):
            raw = stripped.split(":", 1)[1].strip()
            if raw and raw != "None":
                current["participants"] = [{"email": email.strip()} for email in raw.split(",") if email.strip()]
    if current:
        events.append(current)
    return [event for event in events if event.get("external_id") and event.get("starts_at")]


async def _upsert_calendar_event(db: Database, *, tenant_id: str, user_id: str, event: Dict[str, Any]) -> str:
    row = await db.fetchone(
        """
        INSERT INTO calendar_items (
          tenant_id, user_id, title, description, notes, starts_at, ends_at, timezone,
          all_day, location, participants, organizer, rsvp_status, status, source_kind,
          source_ref, evidence_refs, provenance_summary, confidence, external_provider,
          external_id, external_calendar_id, external_etag, external_updated_at, sync_status,
          recurrence_rule, recurrence_parent_id, dedupe_key, metadata,
          created_at, updated_at, cancelled_at, archived_at
        )
        VALUES (
          $1,$2,$3,$4,NULL,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,'unknown',$12,'google_calendar',
          $13::jsonb,$14::jsonb,$15,1.0,'google_calendar',
          $16,$17,$18,$19,'imported',
          NULL,NULL,$20,$21::jsonb,
          NOW(),NOW(),CASE WHEN $12='cancelled' THEN NOW() ELSE NULL END,NULL
        )
        ON CONFLICT (tenant_id, user_id, external_provider, external_id)
        WHERE external_provider IS NOT NULL AND external_id IS NOT NULL
        DO UPDATE SET
          title=EXCLUDED.title,
          description=EXCLUDED.description,
          starts_at=EXCLUDED.starts_at,
          ends_at=EXCLUDED.ends_at,
          timezone=EXCLUDED.timezone,
          all_day=EXCLUDED.all_day,
          location=EXCLUDED.location,
          participants=EXCLUDED.participants,
          organizer=EXCLUDED.organizer,
          status=EXCLUDED.status,
          source_kind='google_calendar',
          source_ref=EXCLUDED.source_ref,
          evidence_refs=EXCLUDED.evidence_refs,
          provenance_summary=EXCLUDED.provenance_summary,
          confidence=1.0,
          external_calendar_id=EXCLUDED.external_calendar_id,
          external_etag=EXCLUDED.external_etag,
          external_updated_at=EXCLUDED.external_updated_at,
          sync_status='imported',
          dedupe_key=EXCLUDED.dedupe_key,
          metadata=EXCLUDED.metadata,
          cancelled_at=CASE WHEN EXCLUDED.status='cancelled' THEN COALESCE(calendar_items.cancelled_at, NOW()) ELSE NULL END,
          updated_at=NOW()
        RETURNING (xmax = 0) AS inserted
        """,
        tenant_id,
        user_id,
        event["title"],
        event.get("description"),
        event["starts_at"],
        event.get("ends_at"),
        event.get("timezone") or "UTC",
        bool(event.get("all_day")),
        event.get("location"),
        event.get("participants") or [],
        event.get("organizer"),
        event.get("status") or "confirmed",
        {
            "provider": GOOGLE_CALENDAR_PROVIDER,
            "calendarId": event.get("external_calendar_id"),
            "eventId": event.get("external_id"),
            "htmlLink": event.get("html_link"),
        },
        [
            {
                "type": "google_calendar_event",
                "calendarId": event.get("external_calendar_id"),
                "eventId": event.get("external_id"),
            }
        ],
        f"Imported from Google Calendar {event.get('external_calendar_id')}",
        event.get("external_id"),
        event.get("external_calendar_id"),
        event.get("external_etag"),
        event.get("external_updated_at"),
        f"google_calendar:{event.get('external_calendar_id')}:{event.get('external_id')}",
        {
            "importedBy": "google_workspace_mcp",
            "htmlLink": event.get("html_link"),
        },
    )
    return "created" if bool((row or {}).get("inserted")) else "updated"


class GoogleCalendarImporter:
    def __init__(self, *, db: Database, mcp_client: Optional[Any] = None, settings: Optional[Settings] = None) -> None:
        self.db = db
        self.settings = settings or get_settings()
        self.mcp_client = mcp_client or WorkspaceMCPClient(settings=self.settings)

    def _user_google_email(self, user_id: str, google_email: Optional[str] = None) -> str:
        value = (google_email or self.settings.google_workspace_mcp_user_email or "").strip()
        if value:
            return value
        if "@" in (user_id or ""):
            return user_id
        raise GoogleCalendarImportError("Google Workspace user email is required")

    async def import_events(
        self,
        *,
        tenant_id: str,
        user_id: str,
        date_from: str,
        date_to: str,
        google_email: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not tenant_id or not user_id:
            raise GoogleCalendarImportError("tenant_id and user_id are required")
        user_google_email = self._user_google_email(user_id, google_email)
        time_min = _date_param_to_rfc3339(date_from, end=False)
        time_max = _date_param_to_rfc3339(date_to, end=True)
        result = CalendarImportResult()

        calendars_payload = await self.mcp_client.call_tool("list_calendars", {"user_google_email": user_google_email})
        calendars = parse_calendars(calendars_payload)
        result.calendarsSeen = len(calendars)

        for calendar in calendars:
            calendar_id = calendar.get("id")
            if not calendar_id:
                result.skipped += 1
                continue
            try:
                events_payload = await self.mcp_client.call_tool(
                    "get_events",
                    {
                        "user_google_email": user_google_email,
                        "calendar_id": calendar_id,
                        "time_min": time_min,
                        "time_max": time_max,
                        "max_results": 250,
                        "detailed": True,
                    },
                )
                events = parse_events(events_payload, calendar_id=calendar_id)
                result.eventsSeen += len(events)
                for event in events:
                    try:
                        outcome = await _upsert_calendar_event(self.db, tenant_id=tenant_id, user_id=user_id, event=event)
                        if outcome == "created":
                            result.created += 1
                        else:
                            result.updated += 1
                    except Exception as exc:
                        result.errors.append(f"{calendar_id}/{event.get('external_id')}: {exc}")
                        result.skipped += 1
            except Exception as exc:
                result.errors.append(f"{calendar_id}: {exc}")
        result.ok = not result.errors
        return result.to_dict()


async def import_google_calendar_events(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    date_from: str,
    date_to: str,
    google_email: Optional[str] = None,
    mcp_client: Optional[Any] = None,
    settings: Optional[Settings] = None,
) -> Dict[str, Any]:
    importer = GoogleCalendarImporter(db=db, mcp_client=mcp_client, settings=settings)
    return await importer.import_events(
        tenant_id=tenant_id,
        user_id=user_id,
        date_from=date_from,
        date_to=date_to,
        google_email=google_email,
    )
