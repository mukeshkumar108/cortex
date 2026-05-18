from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import HTTPException

from .action_state import listDailyAgenda
from .calendar_state import _row_to_calendar_item
from .db import Database


class DayBriefError(HTTPException):
    def __init__(self, status_code: int, detail: str):
        super().__init__(status_code=status_code, detail=detail)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise DayBriefError(400, "date must be YYYY-MM-DD")


def _load_timezone(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        raise DayBriefError(400, f"Invalid timezone: {timezone_name}")


def _dt_iso(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if isinstance(value, datetime) else None


def _derive_pressure_level(*, event_count: int, due_count: int, reminder_count: int, overdue_count: int) -> str:
    if overdue_count >= 3 or (event_count + due_count + reminder_count) >= 8:
        return "high"
    if overdue_count > 0 or (event_count + due_count + reminder_count) >= 4:
        return "medium"
    return "low"


def _derive_suggested_focus(*, calendar_events: List[Dict[str, Any]], due_today: List[Dict[str, Any]], reminders_today: List[Dict[str, Any]], overdue: List[Dict[str, Any]]) -> List[str]:
    focus: List[str] = []
    if overdue:
        focus.append("clear_overdue")
    if calendar_events:
        focus.append("protect_calendar_blocks")
    if due_today:
        focus.append("finish_due_items")
    if reminders_today:
        focus.append("handle_reminders")
    if not focus:
        focus.append("light_planning")
    return focus


async def buildDayBrief(
    db: Database,
    *,
    tenant_id: str,
    user_id: str,
    day: str,
    timezone_name: str,
) -> Dict[str, Any]:
    if not user_id:
        raise DayBriefError(400, "userId is required")
    tenant_id = tenant_id or "default"
    timezone_name = (timezone_name or "UTC").strip() or "UTC"
    tz = _load_timezone(timezone_name)
    target_date = _parse_date(day)

    start_local = datetime.combine(target_date, time.min).replace(tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)

    calendar_rows = await db.fetch(
        """
        SELECT *
        FROM calendar_items
        WHERE tenant_id=$1
          AND user_id=$2
          AND status='confirmed'
          AND COALESCE(ends_at, starts_at) >= $3
          AND starts_at < $4
        ORDER BY starts_at ASC, created_at DESC
        """,
        tenant_id,
        user_id,
        start_utc,
        end_utc,
    )
    calendar_events = [_row_to_calendar_item(dict(row)) for row in calendar_rows]

    action_agenda = await listDailyAgenda(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        day=target_date.isoformat(),
        timezone_name=timezone_name,
    )
    due_today = list(action_agenda.get("dueToday") or [])
    reminders_today = list(action_agenda.get("remindersToday") or [])
    overdue = list(action_agenda.get("overdue") or [])

    calendar_latest = max(
        [row.get("updated_at") for row in calendar_rows if isinstance(row.get("updated_at"), datetime)],
        default=None,
    )
    generated_at = datetime.now(timezone.utc)
    counts = {
        "calendarEvents": len(calendar_events),
        "dueToday": len(due_today),
        "remindersToday": len(reminders_today),
        "overdue": len(overdue),
    }

    return {
        "date": target_date.isoformat(),
        "timezone": timezone_name,
        "generatedAt": generated_at.isoformat(),
        "calendarEvents": calendar_events,
        "remindersToday": reminders_today,
        "dueToday": due_today,
        "overdue": overdue,
        "counts": counts,
        "suggestedFocus": _derive_suggested_focus(
            calendar_events=calendar_events,
            due_today=due_today,
            reminders_today=reminders_today,
            overdue=overdue,
        ),
        "pressureLevel": _derive_pressure_level(
            event_count=counts["calendarEvents"],
            due_count=counts["dueToday"],
            reminder_count=counts["remindersToday"],
            overdue_count=counts["overdue"],
        ),
        "sourceFreshness": {
            "mode": "on_demand",
            "calendarItemsLatestUpdatedAt": _dt_iso(calendar_latest),
            "actionItemsLatestUpdatedAt": None,
            "generatedAt": generated_at.isoformat(),
            "cached": False,
        },
    }
