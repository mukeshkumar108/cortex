from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import Any, Dict, List, Optional


DEFAULT_LIMIT = 50
MAX_LIMIT = 200
CURRENT_WINDOW_HOURS = 6
RECENT_WINDOW_HOURS = 48
STALE_WINDOW_DAYS = 7
HISTORICAL_WINDOW_DAYS = 30


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_list(value: Any) -> List[Any]:
    return list(value) if isinstance(value, list) else []


def _normalize_dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _ensure_utc(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=dt_timezone.utc)
    text = _normalize_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt_timezone.utc)
    return parsed


def _iso(value: Any) -> Optional[str]:
    parsed = _ensure_utc(value)
    return parsed.isoformat() if parsed else None


def _first_non_null(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return None


def _derive_freshness(
    *,
    status: str,
    occurred_at: Optional[datetime],
    first_seen_at: Optional[datetime],
    last_seen_at: Optional[datetime],
    expires_at: Optional[datetime],
    as_of: datetime,
    reinforced: bool = False,
) -> str:
    if expires_at and expires_at < as_of:
        return "expired"
    reference_time = last_seen_at or occurred_at or first_seen_at
    if not reference_time:
        return "unknown"
    age = as_of - reference_time
    normalized_status = _normalize_text(status).lower()
    active_statuses = {
        "active",
        "confirmed",
        "pending",
        "open",
        "eligible",
        "proposed",
        "applied",
        "detected",
        "confirmed",
        "synced",
        "imported",
    }
    if age <= timedelta(hours=CURRENT_WINDOW_HOURS) and normalized_status in active_statuses:
        return "current"
    if age <= timedelta(hours=RECENT_WINDOW_HOURS):
        return "recent"
    if age >= timedelta(days=HISTORICAL_WINDOW_DAYS):
        return "historical"
    if reinforced:
        return "historical"
    if age >= timedelta(days=STALE_WINDOW_DAYS):
        return "stale"
    return "stale"


def _event_base(
    *,
    source_table: str,
    source_id: str,
    tenant_id: str,
    user_id: str,
    event_type: str,
    domain: str,
    title: str,
    summary: Optional[str],
    occurred_at: Any,
    first_seen_at: Any,
    last_seen_at: Any,
    expires_at: Any,
    status: Optional[str],
    confidence: Any,
    salience: Any,
    evidence_refs: Any,
    related_object_ids: Any,
    related_link_ids: Any,
) -> Dict[str, Any]:
    evidence_list = [item for item in _normalize_list(evidence_refs) if isinstance(item, dict)]
    object_ids = [_normalize_text(item) for item in _normalize_list(related_object_ids) if _normalize_text(item)]
    link_ids = [_normalize_text(item) for item in _normalize_list(related_link_ids) if _normalize_text(item)]
    event = {
        "id": f"{source_table}:{source_id}",
        "tenant_id": tenant_id,
        "user_id": user_id,
        "event_type": event_type,
        "domain": domain,
        "title": _normalize_text(title) or f"{source_table} event",
        "summary": _normalize_text(summary) or None,
        "source_table": source_table,
        "source_id": source_id,
        "occurred_at": _iso(occurred_at),
        "first_seen_at": _iso(first_seen_at),
        "last_seen_at": _iso(last_seen_at),
        "expires_at": _iso(expires_at),
        "status": _normalize_text(status) or "unknown",
        "confidence": float(confidence) if confidence is not None else None,
        "salience": float(salience) if salience is not None else None,
        "evidence_refs": evidence_list,
        "related_object_ids": object_ids,
        "related_link_ids": link_ids,
        "freshness": "unknown",
        "gaps": [],
        "missing_metadata": [],
    }
    if not event["occurred_at"]:
        event["gaps"].append("occurred_at_unknown")
        event["missing_metadata"].append("occurred_at")
    if not event["evidence_refs"]:
        event["gaps"].append("evidence_refs_unknown")
        event["missing_metadata"].append("evidence_refs")
    return event


def _action_item_event(row: Dict[str, Any], *, as_of: datetime) -> Dict[str, Any]:
    domain = "habits" if _normalize_text(row.get("kind")).lower() == "habit" else "obligations"
    occurred_at = _first_non_null(row.get("due_at"), row.get("remind_at"), row.get("created_at"))
    event = _event_base(
        source_table="action_items",
        source_id=_normalize_text(row.get("id")),
        tenant_id=_normalize_text(row.get("tenant_id")),
        user_id=_normalize_text(row.get("user_id")),
        event_type="obligation_item",
        domain=domain,
        title=_normalize_text(row.get("title")),
        summary=_normalize_text(row.get("notes")) or _normalize_text(row.get("provenance_summary")) or None,
        occurred_at=occurred_at,
        first_seen_at=row.get("created_at"),
        last_seen_at=row.get("updated_at"),
        expires_at=_first_non_null(row.get("completed_at"), row.get("dismissed_at"), row.get("cancelled_at")),
        status=row.get("status"),
        confidence=row.get("confidence"),
        salience=None,
        evidence_refs=_normalize_dict(row.get("source_ref")).get("evidence_refs"),
        related_object_ids=_normalize_dict(row.get("source_ref")).get("source_object_ids"),
        related_link_ids=_normalize_dict(row.get("source_ref")).get("source_link_ids"),
    )
    event["freshness"] = _derive_freshness(
        status=event["status"],
        occurred_at=_ensure_utc(occurred_at),
        first_seen_at=_ensure_utc(row.get("created_at")),
        last_seen_at=_ensure_utc(row.get("updated_at")),
        expires_at=_ensure_utc(_first_non_null(row.get("completed_at"), row.get("dismissed_at"), row.get("cancelled_at"))),
        as_of=as_of,
        reinforced=_ensure_utc(row.get("updated_at")) and _ensure_utc(row.get("created_at")) and _ensure_utc(row.get("updated_at")) > _ensure_utc(row.get("created_at")),
    )
    return event


def _action_update_event(row: Dict[str, Any], *, as_of: datetime) -> Dict[str, Any]:
    event = _event_base(
        source_table="action_updates",
        source_id=_normalize_text(row.get("id")),
        tenant_id=_normalize_text(row.get("tenant_id")),
        user_id=_normalize_text(row.get("user_id")),
        event_type="obligation_update",
        domain="obligations",
        title=f"Action update: {_normalize_text(row.get('proposed_change')) or 'change'}",
        summary=_normalize_text(row.get("evidence_summary")) or None,
        occurred_at=row.get("applied_at") or row.get("created_at"),
        first_seen_at=row.get("created_at"),
        last_seen_at=row.get("updated_at"),
        expires_at=_first_non_null(row.get("applied_at"), row.get("rejected_at")),
        status=row.get("status"),
        confidence=row.get("confidence"),
        salience=None,
        evidence_refs=_normalize_dict(row.get("source_ref")).get("evidence_refs"),
        related_object_ids=[_normalize_text(row.get("target_action_item_id"))] if _normalize_text(row.get("target_action_item_id")) else [],
        related_link_ids=[],
    )
    event["freshness"] = _derive_freshness(
        status=event["status"],
        occurred_at=_ensure_utc(row.get("applied_at") or row.get("created_at")),
        first_seen_at=_ensure_utc(row.get("created_at")),
        last_seen_at=_ensure_utc(row.get("updated_at")),
        expires_at=_ensure_utc(_first_non_null(row.get("applied_at"), row.get("rejected_at"))),
        as_of=as_of,
        reinforced=_ensure_utc(row.get("updated_at")) and _ensure_utc(row.get("created_at")) and _ensure_utc(row.get("updated_at")) > _ensure_utc(row.get("created_at")),
    )
    return event


def _calendar_item_event(row: Dict[str, Any], *, as_of: datetime) -> Dict[str, Any]:
    event = _event_base(
        source_table="calendar_items",
        source_id=_normalize_text(row.get("id")),
        tenant_id=_normalize_text(row.get("tenant_id")),
        user_id=_normalize_text(row.get("user_id")),
        event_type="calendar_event",
        domain="events",
        title=_normalize_text(row.get("title")),
        summary=_normalize_text(row.get("description")) or _normalize_text(row.get("notes")) or _normalize_text(row.get("provenance_summary")) or None,
        occurred_at=row.get("starts_at"),
        first_seen_at=row.get("created_at"),
        last_seen_at=row.get("updated_at"),
        expires_at=_first_non_null(row.get("cancelled_at"), row.get("archived_at"), row.get("ends_at"), row.get("starts_at")),
        status=row.get("status"),
        confidence=row.get("confidence"),
        salience=None,
        evidence_refs=row.get("evidence_refs"),
        related_object_ids=_normalize_dict(row.get("source_ref")).get("source_object_ids"),
        related_link_ids=_normalize_dict(row.get("source_ref")).get("source_link_ids"),
    )
    event["freshness"] = _derive_freshness(
        status=event["status"],
        occurred_at=_ensure_utc(row.get("starts_at")),
        first_seen_at=_ensure_utc(row.get("created_at")),
        last_seen_at=_ensure_utc(row.get("updated_at")),
        expires_at=_ensure_utc(_first_non_null(row.get("cancelled_at"), row.get("archived_at"), row.get("ends_at"), row.get("starts_at"))),
        as_of=as_of,
        reinforced=_ensure_utc(row.get("updated_at")) and _ensure_utc(row.get("created_at")) and _ensure_utc(row.get("updated_at")) > _ensure_utc(row.get("created_at")),
    )
    return event


def _session_change_event(row: Dict[str, Any], *, as_of: datetime) -> Dict[str, Any]:
    domain = "workstreams" if _normalize_text(row.get("kind")).lower() in {"decision_change", "focus_change"} else "state"
    provenance = _normalize_dict(row.get("provenance"))
    event = _event_base(
        source_table="session_changes",
        source_id=_normalize_text(row.get("change_id")),
        tenant_id=_normalize_text(row.get("tenant_id")),
        user_id=_normalize_text(row.get("user_id")),
        event_type="session_change",
        domain=domain,
        title=_normalize_text(row.get("title")),
        summary=_normalize_text(row.get("summary")) or None,
        occurred_at=row.get("effective_iso") or row.get("created_at"),
        first_seen_at=row.get("created_at"),
        last_seen_at=row.get("updated_at"),
        expires_at=None,
        status=row.get("status"),
        confidence=row.get("confidence_score"),
        salience=None,
        evidence_refs=provenance.get("source_turn_refs") or provenance.get("evidence_refs"),
        related_object_ids=provenance.get("source_object_ids"),
        related_link_ids=provenance.get("source_link_ids"),
    )
    event["freshness"] = _derive_freshness(
        status=event["status"],
        occurred_at=_ensure_utc(row.get("effective_iso") or row.get("created_at")),
        first_seen_at=_ensure_utc(row.get("created_at")),
        last_seen_at=_ensure_utc(row.get("updated_at")),
        expires_at=None,
        as_of=as_of,
        reinforced=_ensure_utc(row.get("updated_at")) and _ensure_utc(row.get("created_at")) and _ensure_utc(row.get("updated_at")) > _ensure_utc(row.get("created_at")),
    )
    return event


def _handover_event(row: Dict[str, Any], *, as_of: datetime) -> Dict[str, Any]:
    event = _event_base(
        source_table="session_handover_packets",
        source_id=_normalize_text(row.get("packet_id")),
        tenant_id=_normalize_text(row.get("tenant_id")),
        user_id=_normalize_text(row.get("user_id")),
        event_type="handover",
        domain="handover",
        title="Session handover packet",
        summary=_normalize_text(row.get("summary")) or None,
        occurred_at=row.get("created_at"),
        first_seen_at=row.get("created_at"),
        last_seen_at=row.get("updated_at"),
        expires_at=row.get("expires_at"),
        status=row.get("status"),
        confidence=None,
        salience=None,
        evidence_refs=row.get("source_turn_refs"),
        related_object_ids=[],
        related_link_ids=[],
    )
    event["freshness"] = _derive_freshness(
        status=event["status"],
        occurred_at=_ensure_utc(row.get("created_at")),
        first_seen_at=_ensure_utc(row.get("created_at")),
        last_seen_at=_ensure_utc(row.get("updated_at")),
        expires_at=_ensure_utc(row.get("expires_at")),
        as_of=as_of,
        reinforced=False,
    )
    return event


def _attention_outcome_event(row: Dict[str, Any], *, as_of: datetime) -> Dict[str, Any]:
    metadata = _normalize_dict(row.get("metadata"))
    source_table = _normalize_text(row.get("source_table")) or "attention_outcomes"
    source_id = _normalize_text(row.get("source_id")) or _normalize_text(row.get("outcome_id"))
    event = _event_base(
        source_table="attention_outcomes",
        source_id=_normalize_text(row.get("outcome_id")),
        tenant_id=_normalize_text(row.get("tenant_id")),
        user_id=_normalize_text(row.get("user_id")),
        event_type="attention_feedback",
        domain="attention_feedback",
        title=f"Attention outcome: {_normalize_text(row.get('outcome_type')) or 'unknown'}",
        summary=_normalize_text(row.get("outcome_reason")) or None,
        occurred_at=row.get("occurred_at"),
        first_seen_at=row.get("created_at"),
        last_seen_at=row.get("occurred_at"),
        expires_at=_first_non_null(row.get("suppress_until"), row.get("snoozed_until")),
        status=row.get("outcome_type"),
        confidence=None,
        salience=None,
        evidence_refs=metadata.get("evidence_refs"),
        related_object_ids=metadata.get("source_object_ids"),
        related_link_ids=metadata.get("source_link_ids"),
    )
    event["summary"] = event["summary"] or f"Applies to {source_table}:{source_id}"
    event["freshness"] = _derive_freshness(
        status=event["status"],
        occurred_at=_ensure_utc(row.get("occurred_at")),
        first_seen_at=_ensure_utc(row.get("created_at")),
        last_seen_at=_ensure_utc(row.get("occurred_at")),
        expires_at=_ensure_utc(_first_non_null(row.get("suppress_until"), row.get("snoozed_until"))),
        as_of=as_of,
        reinforced=False,
    )
    return event


def _relationship_link_event(row: Dict[str, Any], *, as_of: datetime) -> Dict[str, Any]:
    event = _event_base(
        source_table="memory_relationship_links",
        source_id=_normalize_text(row.get("link_id")),
        tenant_id=_normalize_text(row.get("tenant_id")),
        user_id=_normalize_text(row.get("user_id")),
        event_type="relationship_link",
        domain="relationship",
        title=f"{_normalize_text(row.get('relationship_type')) or 'link'}: {_normalize_text(row.get('source_id'))} -> {_normalize_text(row.get('target_id'))}",
        summary=None,
        occurred_at=_first_non_null(row.get("valid_from"), row.get("created_at")),
        first_seen_at=row.get("created_at"),
        last_seen_at=row.get("updated_at"),
        expires_at=_first_non_null(row.get("expires_at"), row.get("valid_until")),
        status=row.get("status") or "active",
        confidence=row.get("confidence"),
        salience=row.get("strength"),
        evidence_refs=row.get("source_turn_refs"),
        related_object_ids=[_normalize_text(row.get("source_id")), _normalize_text(row.get("target_id"))],
        related_link_ids=[_normalize_text(row.get("link_id"))],
    )
    event["freshness"] = _derive_freshness(
        status=event["status"],
        occurred_at=_ensure_utc(_first_non_null(row.get("valid_from"), row.get("created_at"))),
        first_seen_at=_ensure_utc(row.get("created_at")),
        last_seen_at=_ensure_utc(row.get("updated_at")),
        expires_at=_ensure_utc(_first_non_null(row.get("expires_at"), row.get("valid_until"))),
        as_of=as_of,
        reinforced=_ensure_utc(row.get("updated_at")) and _ensure_utc(row.get("created_at")) and _ensure_utc(row.get("updated_at")) > _ensure_utc(row.get("created_at")),
    )
    return event


async def _fetch_action_items(db: Any, tenant_id: str, user_id: str) -> List[Dict[str, Any]]:
    rows = await db.fetch(
        """
        SELECT id, tenant_id, user_id, kind, title, notes, status, due_at, remind_at,
               source_ref, confidence, provenance_summary, created_at, updated_at,
               completed_at, dismissed_at, cancelled_at
        FROM action_items
        WHERE tenant_id=$1::text
          AND user_id=$2::text
        ORDER BY updated_at DESC, created_at DESC
        """,
        tenant_id,
        user_id,
    )
    return [dict(row) for row in rows or []]


async def _fetch_action_updates(db: Any, tenant_id: str, user_id: str) -> List[Dict[str, Any]]:
    rows = await db.fetch(
        """
        SELECT id, tenant_id, user_id, target_action_item_id, proposed_change, proposed_due_at,
               proposed_remind_at, evidence_summary, confidence, status, source_ref,
               created_at, updated_at, applied_at, rejected_at
        FROM action_updates
        WHERE tenant_id=$1::text
          AND user_id=$2::text
        ORDER BY updated_at DESC, created_at DESC
        """,
        tenant_id,
        user_id,
    )
    return [dict(row) for row in rows or []]


async def _fetch_calendar_items(db: Any, tenant_id: str, user_id: str) -> List[Dict[str, Any]]:
    rows = await db.fetch(
        """
        SELECT id, tenant_id, user_id, title, description, notes, starts_at, ends_at,
               status, source_ref, evidence_refs, confidence, provenance_summary,
               created_at, updated_at, cancelled_at, archived_at
        FROM calendar_items
        WHERE tenant_id=$1::text
          AND user_id=$2::text
        ORDER BY starts_at DESC, updated_at DESC
        """,
        tenant_id,
        user_id,
    )
    return [dict(row) for row in rows or []]


async def _fetch_session_changes(db: Any, tenant_id: str, user_id: str) -> List[Dict[str, Any]]:
    rows = await db.fetch(
        """
        SELECT change_id, tenant_id, user_id, kind, title, summary, effective_iso, provenance,
               confidence_score, status, created_at, updated_at
        FROM session_changes
        WHERE tenant_id=$1::text
          AND user_id=$2::text
        ORDER BY COALESCE(effective_iso, updated_at, created_at) DESC
        """,
        tenant_id,
        user_id,
    )
    return [dict(row) for row in rows or []]


async def _fetch_handover_packets(db: Any, tenant_id: str, user_id: str) -> List[Dict[str, Any]]:
    rows = await db.fetch(
        """
        SELECT packet_id, tenant_id, user_id, session_id, summary, source_turn_refs,
               created_at, updated_at, expires_at, status
        FROM session_handover_packets
        WHERE tenant_id=$1::text
          AND user_id=$2::text
        ORDER BY created_at DESC
        """,
        tenant_id,
        user_id,
    )
    return [dict(row) for row in rows or []]


async def _fetch_attention_outcomes(db: Any, tenant_id: str, user_id: str) -> List[Dict[str, Any]]:
    rows = await db.fetch(
        """
        SELECT outcome_id, tenant_id, user_id, source_table, source_id, outcome_type,
               outcome_reason, occurred_at, snoozed_until, suppress_until, metadata, created_at
        FROM attention_outcomes
        WHERE tenant_id=$1::text
          AND user_id=$2::text
        ORDER BY occurred_at DESC, created_at DESC
        """,
        tenant_id,
        user_id,
    )
    return [dict(row) for row in rows or []]


async def _fetch_relationship_links(db: Any, tenant_id: str, user_id: str) -> List[Dict[str, Any]]:
    rows = await db.fetch(
        """
        SELECT link_id, tenant_id, user_id, source_id, target_id, relationship_type, confidence,
               source_turn_refs, created_at, updated_at, status, strength, valid_from, valid_until, expires_at
        FROM memory_relationship_links
        WHERE tenant_id=$1::text
          AND user_id=$2::text
        ORDER BY updated_at DESC, created_at DESC
        """,
        tenant_id,
        user_id,
    )
    return [dict(row) for row in rows or []]


def _is_expired_event(event: Dict[str, Any], *, as_of: datetime) -> bool:
    expires_at = _ensure_utc(event.get("expires_at"))
    if expires_at and expires_at < as_of:
        return True
    return _normalize_text(event.get("freshness")).lower() == "expired"


def _apply_filters(
    events: List[Dict[str, Any]],
    *,
    since: Optional[datetime],
    include_expired: bool,
    domain: Optional[str],
    source_table: Optional[str],
    as_of: datetime,
) -> List[Dict[str, Any]]:
    normalized_domain = _normalize_text(domain).lower()
    normalized_source_table = _normalize_text(source_table).lower()
    filtered: List[Dict[str, Any]] = []
    for event in events:
        if normalized_domain and _normalize_text(event.get("domain")).lower() != normalized_domain:
            continue
        if normalized_source_table and _normalize_text(event.get("source_table")).lower() != normalized_source_table:
            continue
        if not include_expired and _is_expired_event(event, as_of=as_of):
            continue
        occurred = _ensure_utc(event.get("occurred_at")) or _ensure_utc(event.get("last_seen_at")) or _ensure_utc(event.get("first_seen_at"))
        if since and occurred and occurred < since:
            continue
        filtered.append(event)
    return filtered


def _sort_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        events,
        key=lambda item: (
            _ensure_utc(item.get("occurred_at"))
            or _ensure_utc(item.get("last_seen_at"))
            or _ensure_utc(item.get("first_seen_at"))
            or datetime.min.replace(tzinfo=dt_timezone.utc)
        ),
        reverse=True,
    )


def _build_metadata(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_source = Counter(_normalize_text(item.get("source_table")) for item in events if _normalize_text(item.get("source_table")))
    by_domain = Counter(_normalize_text(item.get("domain")) for item in events if _normalize_text(item.get("domain")))
    by_freshness = Counter(_normalize_text(item.get("freshness")) for item in events if _normalize_text(item.get("freshness")))
    occurred_times = [
        _ensure_utc(item.get("occurred_at"))
        for item in events
        if _ensure_utc(item.get("occurred_at")) is not None
    ]
    oldest = min(occurred_times).isoformat() if occurred_times else None
    newest = max(occurred_times).isoformat() if occurred_times else None
    return {
        "returnedCount": len(events),
        "bySourceTable": dict(by_source),
        "byDomain": dict(by_domain),
        "byFreshness": dict(by_freshness),
        "oldestOccurredAt": oldest,
        "newestOccurredAt": newest,
        "readOnly": True,
    }


async def build_timeline_read_model(
    db: Any,
    *,
    tenant_id: str,
    user_id: str,
    limit: int = DEFAULT_LIMIT,
    since: Optional[datetime] = None,
    include_expired: bool = False,
    domain: Optional[str] = None,
    source_table: Optional[str] = None,
    as_of: Optional[datetime] = None,
) -> Dict[str, Any]:
    safe_limit = max(1, min(int(limit or DEFAULT_LIMIT), MAX_LIMIT))
    effective_as_of = as_of or datetime.now(dt_timezone.utc)
    if effective_as_of.tzinfo is None:
        effective_as_of = effective_as_of.replace(tzinfo=dt_timezone.utc)

    action_items = [_action_item_event(row, as_of=effective_as_of) for row in await _fetch_action_items(db, tenant_id, user_id)]
    action_updates = [_action_update_event(row, as_of=effective_as_of) for row in await _fetch_action_updates(db, tenant_id, user_id)]
    calendar_items = [_calendar_item_event(row, as_of=effective_as_of) for row in await _fetch_calendar_items(db, tenant_id, user_id)]
    session_changes = [_session_change_event(row, as_of=effective_as_of) for row in await _fetch_session_changes(db, tenant_id, user_id)]
    handovers = [_handover_event(row, as_of=effective_as_of) for row in await _fetch_handover_packets(db, tenant_id, user_id)]
    outcomes = [_attention_outcome_event(row, as_of=effective_as_of) for row in await _fetch_attention_outcomes(db, tenant_id, user_id)]
    relationship_links = [_relationship_link_event(row, as_of=effective_as_of) for row in await _fetch_relationship_links(db, tenant_id, user_id)]

    all_events = action_items + action_updates + calendar_items + session_changes + handovers + outcomes + relationship_links
    filtered = _apply_filters(
        all_events,
        since=since,
        include_expired=include_expired,
        domain=domain,
        source_table=source_table,
        as_of=effective_as_of,
    )
    sorted_events = _sort_events(filtered)[:safe_limit]
    return {
        "tenantId": tenant_id,
        "userId": user_id,
        "asOf": effective_as_of.isoformat(),
        "items": sorted_events,
        "metadata": _build_metadata(sorted_events),
    }
