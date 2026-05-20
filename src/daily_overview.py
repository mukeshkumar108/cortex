from __future__ import annotations

from collections import Counter
from datetime import date, datetime, time, timedelta, timezone as dt_timezone
import logging
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import HTTPException

from .attention_preview import build_attention_preview
from .fast_handover import get_latest_fast_handover_packet
from .state_decay import build_state_decay_report
from .timeline_read_model import _derive_freshness, _ensure_utc, _normalize_text
from .timeline_read_model import build_timeline_read_model


logger = logging.getLogger(__name__)


DEFAULT_COMPANION_ID = "sophie"
DEFAULT_LIMIT = 25
MAX_LIMIT = 100
RECENTLY_COMPLETED_HOURS = 36
NOISE_TERMS = ("smoke", "demo", "test", "daybrief-refresh")
ACTIVE_ATTENTION_POLICIES = {"suggest_only", "draft_only", "observe_only"}
ACTIVE_TASK_STATUSES = {"pending"}
DONE_TASK_STATUSES = {"done", "completed"}
INACTIVE_TASK_STATUSES = {"cancelled", "dismissed", "archived"}
INACTIVE_EVENT_STATUSES = {"cancelled", "archived"}


class DailyOverviewError(HTTPException):
    def __init__(self, status_code: int, detail: str):
        super().__init__(status_code=status_code, detail=detail)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise DailyOverviewError(400, "date must be YYYY-MM-DD") from exc


def _load_timezone(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise DailyOverviewError(400, f"Invalid timezone: {timezone_name}") from exc


def _iso(value: Any) -> Optional[str]:
    parsed = _ensure_utc(value)
    return parsed.isoformat() if parsed else None


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _is_noise_title(title: Any) -> bool:
    lowered = _normalize_text(title).lower()
    return any(term in lowered for term in NOISE_TERMS)


def _event_day_status(*, starts_at: Optional[datetime], ends_at: Optional[datetime], now_local: datetime) -> str:
    if not starts_at:
        return "unknown"
    starts_local = starts_at.astimezone(now_local.tzinfo)
    ends_local = (ends_at or starts_at).astimezone(now_local.tzinfo)
    if starts_local <= now_local <= ends_local:
        return "current"
    if starts_local > now_local:
        return "upcoming"
    return "past_today"


def _schedule_action_hint(*, day_status: str, starts_at: Optional[datetime], now_local: datetime) -> str:
    if day_status == "current":
        return "in_progress_now"
    if day_status == "past_today":
        return "already_passed_today"
    if starts_at:
        delta = starts_at.astimezone(now_local.tzinfo) - now_local
        if delta <= timedelta(hours=2):
            return "prepare_soon"
    return "upcoming_today"


def _task_action_hint(*, status: str, due_at: Optional[datetime], now_local: datetime, is_recently_completed: bool) -> str:
    normalized_status = _normalize_text(status).lower()
    if is_recently_completed or normalized_status in DONE_TASK_STATUSES:
        return "recently_completed"
    if due_at and due_at.astimezone(now_local.tzinfo) < now_local:
        return "overdue_active"
    return "due_today"


def _task_bucket(*, status: str, due_at: Optional[datetime], completed_at: Optional[datetime], day_start_local: datetime, day_end_local: datetime, now_local: datetime) -> Optional[str]:
    normalized_status = _normalize_text(status).lower()
    if normalized_status in ACTIVE_TASK_STATUSES:
        if due_at:
            due_local = due_at.astimezone(now_local.tzinfo)
            if due_local < day_start_local:
                return "overdue"
            if day_start_local <= due_local < day_end_local:
                return "due_today"
        return None
    if normalized_status in DONE_TASK_STATUSES and completed_at:
        completed_local = completed_at.astimezone(now_local.tzinfo)
        if now_local - completed_local <= timedelta(hours=RECENTLY_COMPLETED_HOURS):
            return "recently_completed"
    return None


def _derive_source_health(
    *,
    schedule_count: int,
    task_count: int,
    attention_count: int,
    handover_present: bool,
    timeline_count: int,
    gaps: List[str],
) -> Dict[str, Any]:
    section_status = {
        "schedule": "ok" if schedule_count > 0 else "empty",
        "tasks": "ok" if task_count > 0 else "empty",
        "attention": "ok" if attention_count > 0 else "empty",
        "handover": "ok" if handover_present else "empty",
        "timeline": "ok" if timeline_count > 0 else "empty",
    }
    return {
        "status": "ok" if not gaps else "partial",
        "sections": section_status,
    }


async def _resolve_timezone_name(db: Any, *, tenant_id: str, user_id: str, timezone_name: Optional[str]) -> str:
    direct = _normalize_text(timezone_name)
    if direct:
        _load_timezone(direct)
        return direct
    row = None
    try:
        row = await db.fetchone(
            """
            SELECT timezone
            FROM identity_cache
            WHERE tenant_id=$1::text
              AND user_id=$2::text
            """,
            tenant_id,
            user_id,
        )
    except Exception:
        row = None
    cached = _normalize_text((row or {}).get("timezone"))
    if cached:
        _load_timezone(cached)
        return cached
    return "UTC"


async def _fetch_schedule_rows(db: Any, *, tenant_id: str, user_id: str, start_utc: datetime, end_utc: datetime) -> List[Dict[str, Any]]:
    rows = await db.fetch(
        """
        SELECT id, tenant_id, user_id, title, starts_at, ends_at, status, confidence,
               source_ref, created_at, updated_at, cancelled_at, archived_at
        FROM calendar_items
        WHERE tenant_id=$1::text
          AND user_id=$2::text
          AND COALESCE(ends_at, starts_at) >= $3::timestamptz
          AND starts_at < $4::timestamptz
        ORDER BY starts_at ASC, created_at DESC
        """,
        tenant_id,
        user_id,
        start_utc,
        end_utc,
    )
    return [dict(row) for row in rows or []]


async def _fetch_task_rows(db: Any, *, tenant_id: str, user_id: str, day_start_utc: datetime, day_end_utc: datetime, include_expired: bool) -> List[Dict[str, Any]]:
    status_filter = ""
    if not include_expired:
        status_filter = "AND status <> 'archived'"
    recent_cutoff_utc = day_start_utc - timedelta(hours=RECENTLY_COMPLETED_HOURS)
    rows = await db.fetch(
        f"""
        SELECT id, tenant_id, user_id, kind, title, notes, status, due_at, remind_at, confidence,
               source_ref, created_at, updated_at, completed_at, dismissed_at, cancelled_at
        FROM action_items
        WHERE tenant_id=$1::text
          AND user_id=$2::text
          {status_filter}
          AND (
            (due_at IS NOT NULL AND due_at < $3::timestamptz)
            OR (remind_at IS NOT NULL AND remind_at < $3::timestamptz)
            OR (completed_at IS NOT NULL AND completed_at >= $4::timestamptz)
            OR updated_at >= $4::timestamptz
          )
        ORDER BY COALESCE(due_at, remind_at, updated_at) ASC, updated_at DESC
        """,
        tenant_id,
        user_id,
        day_end_utc,
        recent_cutoff_utc,
    )
    return [dict(row) for row in rows or []]


def _build_schedule_section(
    rows: List[Dict[str, Any]],
    *,
    include_expired: bool,
    now_utc: datetime,
    now_local: datetime,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    gaps: List[str] = []
    expired_count = 0
    diagnostic_noise_count = 0
    for row in rows:
        status = _normalize_text(row.get("status")).lower() or "unknown"
        starts_at = _ensure_utc(row.get("starts_at"))
        ends_at = _ensure_utc(row.get("ends_at"))
        expires_at = _ensure_utc(row.get("cancelled_at")) or _ensure_utc(row.get("archived_at"))
        freshness = _derive_freshness(
            status=status,
            occurred_at=starts_at,
            first_seen_at=_ensure_utc(row.get("created_at")),
            last_seen_at=_ensure_utc(row.get("updated_at")),
            expires_at=expires_at,
            as_of=now_utc,
            reinforced=False,
        )
        item_gaps: List[str] = []
        if not isinstance((row.get("source_ref") or {}).get("source_object_ids"), list):
            item_gaps.append("source_object_ids_unknown")
        if not row.get("source_ref"):
            item_gaps.append("evidence_refs_unknown")
        is_noise = _is_noise_title(row.get("title"))
        if is_noise:
            diagnostic_noise_count += 1
            item_gaps.append("diagnostic_noise")
        is_expired = freshness == "expired" or status in INACTIVE_EVENT_STATUSES
        if is_expired:
            expired_count += 1
        if (is_expired or is_noise) and not include_expired:
            continue
        day_status = _event_day_status(starts_at=starts_at, ends_at=ends_at, now_local=now_local)
        items.append(
            {
                "title": _normalize_text(row.get("title")) or "Untitled event",
                "starts_at": _iso(starts_at),
                "ends_at": _iso(ends_at),
                "status": status,
                "freshness": freshness,
                "source_id": _normalize_text(row.get("id")),
                "source_table": "calendar_items",
                "action_hint": _schedule_action_hint(day_status=day_status, starts_at=starts_at, now_local=now_local),
                "time_status": day_status,
                "gaps": item_gaps,
                "diagnostic_noise": is_noise,
            }
        )
    if not items:
        gaps.append("no_schedule_items_for_day")
    return items, {
        "gaps": gaps,
        "expiredCount": expired_count,
        "diagnosticNoiseCount": diagnostic_noise_count,
    }


def _build_tasks_section(
    rows: List[Dict[str, Any]],
    *,
    include_expired: bool,
    day_start_local: datetime,
    day_end_local: datetime,
    day_start_utc: datetime,
    day_end_utc: datetime,
    now_utc: datetime,
    now_local: datetime,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    gaps: List[str] = []
    expired_count = 0
    stale_count = 0
    diagnostic_noise_count = 0
    for row in rows:
        status = _normalize_text(row.get("status")).lower() or "unknown"
        due_at = _ensure_utc(row.get("due_at")) or _ensure_utc(row.get("remind_at"))
        completed_at = _ensure_utc(row.get("completed_at"))
        expires_at = completed_at or _ensure_utc(row.get("dismissed_at")) or _ensure_utc(row.get("cancelled_at"))
        freshness = _derive_freshness(
            status=status,
            occurred_at=due_at,
            first_seen_at=_ensure_utc(row.get("created_at")),
            last_seen_at=_ensure_utc(row.get("updated_at")),
            expires_at=expires_at,
            as_of=now_utc,
            reinforced=bool(_ensure_utc(row.get("updated_at")) and _ensure_utc(row.get("created_at")) and _ensure_utc(row.get("updated_at")) > _ensure_utc(row.get("created_at"))),
        )
        bucket = _task_bucket(
            status=status,
            due_at=due_at,
            completed_at=completed_at,
            day_start_local=day_start_local,
            day_end_local=day_end_local,
            now_local=now_local,
        )
        is_noise = _is_noise_title(row.get("title"))
        is_expired = freshness == "expired" or status in INACTIVE_TASK_STATUSES
        is_recently_completed = bucket == "recently_completed"
        if freshness == "stale" and status in ACTIVE_TASK_STATUSES:
            stale_count += 1
        if is_noise:
            diagnostic_noise_count += 1
        if is_expired:
            expired_count += 1
        if not bucket and not (include_expired and (is_expired or is_noise)):
            continue
        if (is_noise or is_expired) and not include_expired and not is_recently_completed:
            continue
        item_gaps: List[str] = []
        if not row.get("source_ref"):
            item_gaps.append("evidence_refs_unknown")
        if freshness == "stale" and status in ACTIVE_TASK_STATUSES:
            item_gaps.append("stale_pending_task")
        if is_noise:
            item_gaps.append("diagnostic_noise")
        if due_at and not (day_start_utc <= due_at < day_end_utc) and bucket == "due_today":
            item_gaps.append("due_at_timezone_edge_case")
        items.append(
            {
                "title": _normalize_text(row.get("title")) or "Untitled task",
                "status": status,
                "due_at": _iso(due_at),
                "occurred_at": _iso(completed_at or _ensure_utc(row.get("updated_at"))),
                "priority": None,
                "confidence": _safe_float(row.get("confidence")),
                "freshness": freshness,
                "source_id": _normalize_text(row.get("id")),
                "source_table": "action_items",
                "action_hint": _task_action_hint(
                    status=status,
                    due_at=due_at,
                    now_local=now_local,
                    is_recently_completed=is_recently_completed,
                ),
                "bucket": bucket or "diagnostic",
                "gaps": item_gaps,
                "diagnostic_noise": is_noise,
            }
        )
    if not items:
        gaps.append("no_active_tasks_for_day")
    return items, {
        "gaps": gaps,
        "expiredCount": expired_count,
        "staleCount": stale_count,
        "diagnosticNoiseCount": diagnostic_noise_count,
    }


def _build_attention_section(payload: Dict[str, Any]) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for row in payload.get("items") or []:
        action_policy = _normalize_text(row.get("action_policy")).lower()
        if action_policy not in ACTIVE_ATTENTION_POLICIES:
            continue
        items.append(
            {
                "title": _normalize_text(row.get("title")),
                "reason": _normalize_text(row.get("reason")),
                "attention_type": _normalize_text(row.get("attention_type")),
                "surface_mode": _normalize_text(row.get("surface_mode")),
                "action_policy": action_policy,
                "priority": row.get("priority"),
                "urgency": row.get("urgency"),
                "sensitivity": _normalize_text(row.get("sensitivity")),
                "status": _normalize_text(row.get("status")),
                "expires_at": row.get("expires_at"),
                "source_table": _normalize_text(row.get("source_table")),
                "source_id": _normalize_text(row.get("source_id")),
                "gaps": list(row.get("gaps") or []),
            }
        )
        if len(items) >= 5:
            break
    meta = payload.get("metadata") or {}
    return items, {
        "suppressedCount": int(meta.get("suppressedCount") or 0) + int(meta.get("outcomeSuppressedCount") or 0),
        "expiredCount": int(meta.get("expiredCount") or 0),
        "gaps": [] if items else ["no_attention_items"],
    }


def _build_handover_section(row: Optional[Dict[str, Any]]) -> tuple[Optional[Dict[str, Any]], List[str]]:
    if not row:
        return None, ["no_active_handover_packet"]
    return {
        "summary": _normalize_text(row.get("summary")) or None,
        "open_questions": list(row.get("open_questions") or []),
        "unresolved_decisions": list(row.get("unresolved_decisions") or []),
        "pending_actions": list(row.get("pending_actions") or []),
        "recent_state_note": _normalize_text(row.get("recent_state_note")) or None,
        "important_people": list(row.get("important_people") or []),
        "active_topics": list(row.get("active_topics") or []),
        "do_not_overdo": list(row.get("do_not_overdo") or []),
        "expires_at": _iso(row.get("expires_at")),
        "gaps": [
            "handover_is_short_lived_and_not_durable_profile_memory",
            "recent_state_note_is_provisional",
        ],
    }, []


def _build_recent_timeline_section(payload: Dict[str, Any]) -> Dict[str, Any]:
    items = list(payload.get("items") or [])
    selected: List[Dict[str, Any]] = []
    for item in items:
        source_table = _normalize_text(item.get("source_table")).lower()
        if source_table in {"attention_outcomes", "action_updates", "session_changes", "calendar_items", "action_items", "timeline_events"}:
            selected.append(
                {
                    "title": _normalize_text(item.get("title")),
                    "event_type": _normalize_text(item.get("event_type")),
                    "domain": _normalize_text(item.get("domain")),
                    "timeline_type": _normalize_text(item.get("timeline_type")),
                    "source_table": source_table,
                    "source_id": _normalize_text(item.get("source_id")),
                    "occurred_at": item.get("occurred_at"),
                    "freshness": _normalize_text(item.get("freshness")),
                    "status": _normalize_text(item.get("status")),
                    "gaps": list(item.get("gaps") or []),
                }
            )
        if len(selected) >= 8:
            break
    timeline_meta = payload.get("metadata") or {}
    by_freshness = timeline_meta.get("byFreshness") or {}
    return {
        "items": selected,
        "staleCount": int(by_freshness.get("stale") or 0),
        "historicalCount": int(by_freshness.get("historical") or 0),
        "expiredCount": int(by_freshness.get("expired") or 0),
        "gaps": [] if selected else ["no_recent_timeline_signals"],
    }


def _build_suggested_focus(
    *,
    schedule_items: List[Dict[str, Any]],
    task_items: List[Dict[str, Any]],
    attention_items: List[Dict[str, Any]],
    handover: Optional[Dict[str, Any]],
    timeline_section: Dict[str, Any],
    state_decay: Dict[str, Any],
) -> Dict[str, Any]:
    active_tasks = [item for item in task_items if item.get("bucket") in {"due_today", "overdue"} and not item.get("diagnostic_noise")]
    overdue_tasks = [item for item in active_tasks if item.get("action_hint") == "overdue_active"]
    upcoming_events = [item for item in schedule_items if item.get("time_status") in {"upcoming", "current"} and not item.get("diagnostic_noise")]
    stale_state_signals = list(state_decay.get("stale_state_warnings") or [])
    if not active_tasks and not upcoming_events and not attention_items:
        reason = "No active items found; avoid inventing a brief."
        return {
            "primary_focus": "no_active_items",
            "secondary_focus": "stay_read_only",
            "suggested_next_action": "do_not_generate_user_facing_brief",
            "avoid_overdoing": "avoid_inventing_context",
            "confidence": 0.95,
            "reason": reason,
        }
    if not active_tasks and not upcoming_events and task_items and all(item.get("diagnostic_noise") for item in task_items):
        reason = "Only stale smoke/demo items found; no user-facing daily overview should be generated."
        return {
            "primary_focus": "diagnostic_noise_only",
            "secondary_focus": "suppress_test_data",
            "suggested_next_action": "ignore_noisy_items",
            "avoid_overdoing": "do_not_treat_demo_rows_as_real_commitments",
            "confidence": 0.98,
            "reason": reason,
        }
    if overdue_tasks and upcoming_events:
        reason = "One overdue active task and one appointment today."
        return {
            "primary_focus": "balance_overdue_and_schedule",
            "secondary_focus": "protect_event_timing",
            "suggested_next_action": "review_overdue_before_next_event",
            "avoid_overdoing": "do_not_nag_about_old_state",
            "confidence": 0.88,
            "reason": reason,
        }
    if handover and ((handover.get("unresolved_decisions") or []) or (handover.get("open_questions") or [])):
        reason = "Recent handover suggests unresolved decision; ask gently before assuming."
        return {
            "primary_focus": "recent_unresolved_decision",
            "secondary_focus": "confirm_recent_context",
            "suggested_next_action": "use_handover_as_provisional_prompt",
            "avoid_overdoing": "do_not_treat_handover_as_durable_truth",
            "confidence": 0.82,
            "reason": reason,
        }
    if stale_state_signals:
        reason = "Recent state evidence is stale; overview should not claim it is current."
        return {
            "primary_focus": "fresh_operational_items_only",
            "secondary_focus": "decay_old_state",
            "suggested_next_action": "prefer_tasks_and_schedule_over_old_state",
            "avoid_overdoing": "do_not_claim_stale_emotions_are_current",
            "confidence": 0.86,
            "reason": reason,
        }
    reason = "The day has concrete schedule, task, or attention items and can support a cautious overview."
    return {
        "primary_focus": "concrete_day_items",
        "secondary_focus": "light_continuity",
        "suggested_next_action": "compose_from_schedule_tasks_attention",
        "avoid_overdoing": "avoid_over_personalising_from_provisional_context",
        "confidence": 0.8,
        "reason": reason,
    }


async def build_daily_overview(
    db: Any,
    *,
    tenant_id: str,
    user_id: str,
    companion_id: str = DEFAULT_COMPANION_ID,
    date_value: Optional[str] = None,
    timezone_name: Optional[str] = None,
    include_expired: bool = False,
    limit: int = DEFAULT_LIMIT,
    as_of: Optional[datetime] = None,
) -> Dict[str, Any]:
    if not _normalize_text(user_id):
        raise DailyOverviewError(400, "userId is required")
    safe_limit = max(1, min(int(limit or DEFAULT_LIMIT), MAX_LIMIT))
    effective_timezone_name = await _resolve_timezone_name(db, tenant_id=tenant_id, user_id=user_id, timezone_name=timezone_name)
    tz = _load_timezone(effective_timezone_name)
    now_utc = as_of or datetime.now(dt_timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=dt_timezone.utc)
    now_local = now_utc.astimezone(tz)
    target_date = _parse_date(date_value) if _normalize_text(date_value) else now_local.date()
    day_start_local = datetime.combine(target_date, time.min).replace(tzinfo=tz)
    day_end_local = day_start_local + timedelta(days=1)
    day_start_utc = day_start_local.astimezone(dt_timezone.utc)
    day_end_utc = day_end_local.astimezone(dt_timezone.utc)

    try:
        schedule_rows = await _fetch_schedule_rows(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            start_utc=day_start_utc,
            end_utc=day_end_utc,
        )
    except Exception:
        logger.exception(
            "daily_overview.schedule_query_failed tenant_id=%s user_id=%s date=%s timezone=%s",
            tenant_id,
            user_id,
            target_date.isoformat(),
            effective_timezone_name,
        )
        raise
    try:
        task_rows = await _fetch_task_rows(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            day_start_utc=day_start_utc,
            day_end_utc=day_end_utc,
            include_expired=include_expired,
        )
    except Exception:
        logger.exception(
            "daily_overview.tasks_query_failed tenant_id=%s user_id=%s date=%s include_expired=%s",
            tenant_id,
            user_id,
            target_date.isoformat(),
            include_expired,
        )
        raise
    try:
        attention_payload = await build_attention_preview(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            companion_id=companion_id,
            limit=min(10, safe_limit),
            include_expired=include_expired,
            include_suppressed=False,
            as_of=now_utc,
        )
    except Exception:
        logger.exception(
            "daily_overview.attention_query_failed tenant_id=%s user_id=%s companion_id=%s",
            tenant_id,
            user_id,
            companion_id,
        )
        raise
    try:
        handover_row = await get_latest_fast_handover_packet(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            include_expired=include_expired,
            as_of=now_utc,
        )
    except Exception:
        logger.exception(
            "daily_overview.handover_query_failed tenant_id=%s user_id=%s include_expired=%s",
            tenant_id,
            user_id,
            include_expired,
        )
        raise
    try:
        timeline_payload = await build_timeline_read_model(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            limit=min(20, safe_limit),
            include_expired=include_expired,
            as_of=now_utc,
        )
    except Exception:
        logger.exception(
            "daily_overview.timeline_query_failed tenant_id=%s user_id=%s include_expired=%s",
            tenant_id,
            user_id,
            include_expired,
        )
        raise
    try:
        state_decay = await build_state_decay_report(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            companion_id=companion_id,
            include_historical=include_expired,
            limit=min(12, safe_limit),
            as_of=now_utc,
        )
    except Exception:
        logger.exception(
            "daily_overview.state_decay_query_failed tenant_id=%s user_id=%s include_expired=%s",
            tenant_id,
            user_id,
            include_expired,
        )
        raise

    schedule_items, schedule_meta = _build_schedule_section(
        schedule_rows,
        include_expired=include_expired,
        now_utc=now_utc,
        now_local=now_local,
    )
    task_items, task_meta = _build_tasks_section(
        task_rows,
        include_expired=include_expired,
        day_start_local=day_start_local,
        day_end_local=day_end_local,
        day_start_utc=day_start_utc,
        day_end_utc=day_end_utc,
        now_utc=now_utc,
        now_local=now_local,
    )
    attention_items, attention_meta = _build_attention_section(attention_payload)
    handover, handover_gaps = _build_handover_section(handover_row)
    timeline_section = _build_recent_timeline_section(timeline_payload)

    schedule_items = schedule_items[:safe_limit]
    task_items = task_items[:safe_limit]
    attention_items = attention_items[: min(5, safe_limit)]

    top_level_gaps: List[str] = []
    for group in (
        schedule_meta.get("gaps") or [],
        task_meta.get("gaps") or [],
        attention_meta.get("gaps") or [],
        handover_gaps,
        timeline_section.get("gaps") or [],
        state_decay.get("gaps") or [],
    ):
        top_level_gaps.extend(group)

    source_tables_used = sorted(
        {
            "calendar_items",
            "action_items",
            "attention_outcomes" if timeline_section.get("items") else "",
            "session_handover_packets" if handover else "",
            "action_updates" if any(item.get("source_table") == "action_updates" for item in timeline_section.get("items") or []) else "",
            "session_changes" if any(item.get("source_table") == "session_changes" for item in timeline_section.get("items") or []) else "",
        }
        - {""}
    )
    suggested_focus = _build_suggested_focus(
        schedule_items=schedule_items,
        task_items=task_items,
        attention_items=attention_items,
        handover=handover,
        timeline_section=timeline_section,
        state_decay=state_decay,
    )
    section_counts = {
        "schedule": len(schedule_items),
        "tasks": len(task_items),
        "worthAttention": len(attention_items),
        "recentTimelineSignals": len(timeline_section.get("items") or []),
        "stateDecayCurrent": len(state_decay.get("current_state_notes") or []),
        "stateDecayStale": len(state_decay.get("stale_state_warnings") or []),
        "handover": 1 if handover else 0,
    }
    metadata = {
        "counts": section_counts,
        "sourceTablesUsed": source_tables_used,
        "staleCount": int(task_meta.get("staleCount") or 0) + int(timeline_section.get("staleCount") or 0) + len(state_decay.get("stale_state_warnings") or []),
        "expiredCount": int(schedule_meta.get("expiredCount") or 0) + int(task_meta.get("expiredCount") or 0) + int(attention_meta.get("expiredCount") or 0) + int(timeline_section.get("expiredCount") or 0),
        "suppressedCount": int(attention_meta.get("suppressedCount") or 0),
        "diagnosticNoiseCount": int(schedule_meta.get("diagnosticNoiseCount") or 0) + int(task_meta.get("diagnosticNoiseCount") or 0),
        "stateDecaySuppressions": list(state_decay.get("suppressions") or []),
        "gaps": top_level_gaps,
        "readOnly": True,
    }
    return {
        "tenantId": tenant_id,
        "userId": user_id,
        "companionId": _normalize_text(companion_id) or DEFAULT_COMPANION_ID,
        "date": target_date.isoformat(),
        "timezone": effective_timezone_name,
        "generatedAt": now_utc.isoformat(),
        "readOnly": True,
        "sourceHealth": _derive_source_health(
            schedule_count=len(schedule_items),
            task_count=len(task_items),
            attention_count=len(attention_items),
            handover_present=bool(handover),
            timeline_count=len(timeline_section.get("items") or []),
            gaps=top_level_gaps,
        ),
        "gaps": top_level_gaps,
        "todaysSchedule": schedule_items,
        "tasksAndObligations": task_items,
        "worthAttention": attention_items,
        "recentContinuity": handover,
        "recentTimelineSignals": timeline_section,
        "stateDecay": state_decay,
        "suggestedFocus": suggested_focus,
        "metadata": metadata,
    }
