from __future__ import annotations

from datetime import datetime, timedelta, timezone as dt_timezone
from typing import Any, Dict, List, Optional

from .attention_outcomes import fetch_attention_outcomes


DEFAULT_COMPANION_ID = "sophie"
DEFAULT_EXCLUDED_STATUSES = {"expired", "completed", "dismissed", "suppressed", "archived"}


COMPANION_PROFILES: dict[str, dict[str, Any]] = {
    "sophie": {
        "default_surface_mode": "proactive_allowed",
        "default_action_policy": "suggest_only",
        "suppress_emotional_state": False,
        "ops_only": False,
        "allow_escalation_candidate": False,
    },
    "ashley_ops": {
        "default_surface_mode": "proactive_recommended",
        "default_action_policy": "draft_only",
        "suppress_emotional_state": True,
        "ops_only": True,
        "allow_escalation_candidate": False,
    },
    "elderly_care": {
        "default_surface_mode": "proactive_recommended",
        "default_action_policy": "suggest_only",
        "suppress_emotional_state": False,
        "ops_only": False,
        "allow_escalation_candidate": True,
    },
    "chronic_health": {
        "default_surface_mode": "proactive_allowed",
        "default_action_policy": "suggest_only",
        "suppress_emotional_state": False,
        "ops_only": False,
        "allow_escalation_candidate": False,
    },
    "mukesh_builder": {
        "default_surface_mode": "reactive_only",
        "default_action_policy": "draft_only",
        "suppress_emotional_state": True,
        "ops_only": True,
        "allow_escalation_candidate": False,
    },
    "admin_only": {
        "default_surface_mode": "reactive_only",
        "default_action_policy": "observe_only",
        "suppress_emotional_state": True,
        "ops_only": False,
        "allow_escalation_candidate": False,
    },
}


EMOTIONAL_TERMS = (
    "anxious",
    "overwhelmed",
    "upset",
    "sad",
    "lonely",
    "hurt",
    "emotional",
    "tension",
    "fear",
    "stress",
    "stressed",
    "unwell",
    "pain",
)

OPS_TERMS = (
    "invoice",
    "client",
    "calendar",
    "meeting",
    "schedule",
    "event",
    "deadline",
    "follow up",
    "follow-up",
    "ops",
    "project",
    "workstream",
    "todo",
    "task",
)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _iso(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        dt = value
    elif value is None:
        return None
    else:
        return _normalize_text(value) or None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=dt_timezone.utc)
    return dt.isoformat()


def _ensure_utc(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=dt_timezone.utc)
    return None


def _candidate_status_to_attention_status(value: str, *, as_of: datetime, latest_useful_at: Optional[datetime]) -> str:
    status = _normalize_text(value).lower()
    if status in {"dismissed", "shadow_dismissed"}:
        return "dismissed"
    if status in {"acted_on", "confirmed", "shadow_sent"}:
        return "acknowledged"
    if status in {"stale", "expired", "shadow_stale"}:
        return "expired"
    if latest_useful_at and latest_useful_at < as_of:
        return "expired"
    return "eligible"


def _windowed_status(*, status: str, as_of: datetime, earliest_surface_at: Optional[datetime], latest_useful_at: Optional[datetime]) -> str:
    normalized = _normalize_text(status).lower() or "eligible"
    if normalized in {"expired", "completed", "dismissed", "suppressed", "archived", "acknowledged", "surfaced", "snoozed"}:
        return normalized
    if latest_useful_at and latest_useful_at < as_of:
        return "expired"
    if earliest_surface_at and earliest_surface_at > as_of:
        return "pending"
    return "eligible"


def _priority_from_score(score: float) -> int:
    if score >= 0.85:
        return 95
    if score >= 0.65:
        return 80
    if score >= 0.45:
        return 65
    return 50


def _importance_from_text(text: str) -> int:
    lowered = text.lower()
    if any(term in lowered for term in ("urgent", "critical", "medication", "invoice", "deadline", "doctor")):
        return 85
    if any(term in lowered for term in ("follow up", "meeting", "appointment", "task", "check in")):
        return 70
    return 55


def _urgency_from_dates(*, as_of: datetime, target: Optional[datetime]) -> int:
    if not target:
        return 45
    delta = target - as_of
    if delta.total_seconds() <= 0:
        return 90
    if delta <= timedelta(hours=6):
        return 85
    if delta <= timedelta(days=1):
        return 75
    if delta <= timedelta(days=3):
        return 60
    return 45


def _sensitivity_for_text(text: str) -> str:
    lowered = text.lower()
    if any(term in lowered for term in ("caregiver", "wellness check", "missed medication")):
        return "critical"
    if any(term in lowered for term in EMOTIONAL_TERMS):
        return "high"
    return "low"


def _extract_object_ids(payload: Any) -> List[str]:
    if isinstance(payload, list):
        return [_normalize_text(item) for item in payload if _normalize_text(item)]
    if isinstance(payload, dict):
        for key in ("source_object_ids", "object_ids", "entity_ids", "thread_ids"):
            value = payload.get(key)
            if isinstance(value, list):
                return [_normalize_text(item) for item in value if _normalize_text(item)]
    return []


def _extract_link_ids(payload: Any) -> List[str]:
    if isinstance(payload, dict):
        for key in ("source_link_ids", "link_ids"):
            value = payload.get(key)
            if isinstance(value, list):
                return [_normalize_text(item) for item in value if _normalize_text(item)]
    return []


def _extract_evidence_refs(*payloads: Any) -> List[Dict[str, Any]]:
    refs: List[Dict[str, Any]] = []
    for payload in payloads:
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    refs.append(item)
        elif isinstance(payload, dict):
            for key in ("source_turn_refs", "evidence_refs"):
                value = payload.get(key)
                if isinstance(value, list):
                    refs.extend(item for item in value if isinstance(item, dict))
    return refs


def _gaps_for_item(*, source_object_ids: List[str], source_link_ids: List[str], evidence_refs: List[Dict[str, Any]], source_table: str) -> tuple[List[str], List[str]]:
    gaps: List[str] = []
    missing_metadata: List[str] = []
    if not source_object_ids:
        gaps.append("source_object_ids_unknown")
        missing_metadata.append("source_object_ids")
    if not source_link_ids and source_table in {"follow_up_candidates", "open_threads"}:
        gaps.append("source_link_ids_unknown")
        missing_metadata.append("source_link_ids")
    if not evidence_refs:
        gaps.append("evidence_refs_unknown")
        missing_metadata.append("evidence_refs")
    return gaps, missing_metadata


def _apply_living_context_modifier(item: Dict[str, Any], living_context: Dict[str, Any]) -> Dict[str, Any]:
    if not living_context:
        return item
    note = _normalize_text(living_context.get("primary_tension")) or _normalize_text(living_context.get("emotional_texture"))
    if not note:
        return item
    if item["attention_type"] not in {"check_in", "follow_up", "clarification"}:
        return item
    item["reason"] = f'{item["reason"]} Context modifier: {note}.'.strip()
    if item["sensitivity"] == "low" and any(term in note.lower() for term in EMOTIONAL_TERMS):
        item["sensitivity"] = "medium"
    return item


def _is_emotional_item(item: Dict[str, Any]) -> bool:
    haystack = " ".join(
        [
            _normalize_text(item.get("title")),
            _normalize_text(item.get("reason")),
            _normalize_text(item.get("attention_type")),
            " ".join(_normalize_text(ref.get("text")) for ref in item.get("evidence_refs") or [] if isinstance(ref, dict)),
        ]
    ).lower()
    return any(term in haystack for term in EMOTIONAL_TERMS)


def _is_ops_item(item: Dict[str, Any]) -> bool:
    haystack = " ".join([_normalize_text(item.get("title")), _normalize_text(item.get("reason"))]).lower()
    return any(term in haystack for term in OPS_TERMS)


def _filter_for_companion(item: Dict[str, Any], companion_id: str) -> Optional[Dict[str, Any]]:
    profile = COMPANION_PROFILES.get(companion_id, COMPANION_PROFILES[DEFAULT_COMPANION_ID])
    if profile.get("suppress_emotional_state") and _is_emotional_item(item):
        return None
    if profile.get("ops_only") and not _is_ops_item(item):
        return None
    if item.get("attention_type") == "escalation_candidate" and not profile.get("allow_escalation_candidate"):
        item["attention_type"] = "risk_signal"
        item["action_policy"] = "observe_only" if companion_id == "admin_only" else item.get("action_policy") or "suggest_only"
    if companion_id == "admin_only":
        item["surface_mode"] = "reactive_only"
        item["action_policy"] = "observe_only"
    item["companion_id"] = companion_id
    return item


def _status_sort_rank(value: str) -> int:
    status = _normalize_text(value).lower()
    return {
        "eligible": 0,
        "pending": 1,
        "surfaced": 2,
        "acknowledged": 3,
        "snoozed": 4,
        "candidate": 5,
        "expired": 6,
        "completed": 7,
        "dismissed": 8,
        "suppressed": 9,
        "archived": 10,
    }.get(status, 11)


def _recency_sort_value(item: Dict[str, Any]) -> float:
    for key in ("updated_at", "created_at"):
        value = item.get(key)
        if isinstance(value, datetime):
            return value.timestamp()
    return 0.0


def _sort_key(item: Dict[str, Any]) -> tuple[int, int, int, str, float]:
    ideal = item.get("ideal_surface_at") or ""
    return (
        _status_sort_rank(item.get("status")),
        -int(item.get("priority") or 0),
        -int(item.get("urgency") or 0),
        ideal,
        -_recency_sort_value(item),
    )


def _parse_ts(value: Any) -> Optional[datetime]:
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


def _increment_count(counter: Dict[str, int], key: str) -> None:
    normalized = _normalize_text(key) or "unknown"
    counter[normalized] = int(counter.get(normalized) or 0) + 1


def _build_follow_up_item(row: Dict[str, Any], *, tenant_id: str, companion_id: str, as_of: datetime) -> Dict[str, Any]:
    due_at = _ensure_utc(row.get("due_at"))
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    evidence_refs = _extract_evidence_refs(row.get("source_turn_refs"), metadata)
    source_object_ids = _extract_object_ids(metadata)
    source_link_ids = _extract_link_ids(metadata)
    gaps, missing_metadata = _gaps_for_item(
        source_object_ids=source_object_ids,
        source_link_ids=source_link_ids,
        evidence_refs=evidence_refs,
        source_table="follow_up_candidates",
    )
    title = _normalize_text(row.get("title")) or "Follow up"
    reason = _normalize_text(row.get("reason")) or "Existing follow-up candidate is due for review."
    priority_score = _to_float(row.get("priority_score"), 0.6)
    earliest_surface_at = due_at or _ensure_utc(row.get("created_at"))
    latest_useful_at = (due_at + timedelta(days=1)) if due_at else None
    expires_at = (due_at + timedelta(days=2)) if due_at else None
    item = {
        "id": f'follow_up_candidates:{row.get("candidate_id")}',
        "tenant_id": tenant_id,
        "user_id": _normalize_text(row.get("user_id")),
        "companion_id": companion_id,
        "title": title,
        "reason": reason,
        "attention_type": "follow_up",
        "surface_mode": "proactive_allowed",
        "action_policy": "suggest_only",
        "priority": _priority_from_score(priority_score),
        "urgency": _urgency_from_dates(as_of=as_of, target=due_at),
        "importance": _importance_from_text(f"{title} {reason}"),
        "confidence": _to_float(row.get("confidence"), 0.0),
        "sensitivity": _sensitivity_for_text(f"{title} {reason}"),
        "earliest_surface_at": _iso(earliest_surface_at),
        "ideal_surface_at": _iso(due_at or row.get("updated_at")),
        "latest_useful_at": _iso(latest_useful_at),
        "expires_at": _iso(expires_at),
        "status": _windowed_status(
            status=_candidate_status_to_attention_status(_normalize_text(row.get("status")), as_of=as_of, latest_useful_at=latest_useful_at),
            as_of=as_of,
            earliest_surface_at=earliest_surface_at,
            latest_useful_at=latest_useful_at,
        ),
        "source_table": "follow_up_candidates",
        "source_id": _normalize_text(row.get("candidate_id")),
        "source_object_ids": source_object_ids,
        "source_link_ids": source_link_ids,
        "evidence_refs": evidence_refs,
        "gaps": gaps,
        "missing_metadata": missing_metadata,
        "created_at": _ensure_utc(row.get("created_at")),
        "updated_at": _ensure_utc(row.get("updated_at")),
    }
    return item


def _build_clarification_item(row: Dict[str, Any], *, tenant_id: str, companion_id: str, as_of: datetime) -> Dict[str, Any]:
    due_at = _ensure_utc(row.get("due_at"))
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    evidence_refs = _extract_evidence_refs(row.get("source_turn_refs"), metadata)
    source_object_ids = _extract_object_ids(metadata)
    source_link_ids = _extract_link_ids(metadata)
    gaps, missing_metadata = _gaps_for_item(
        source_object_ids=source_object_ids,
        source_link_ids=source_link_ids,
        evidence_refs=evidence_refs,
        source_table="clarification_candidates",
    )
    title = _normalize_text(row.get("title")) or "Clarify candidate"
    reason = _normalize_text(row.get("reason")) or "Low-confidence or contradictory item needs clarification."
    earliest_surface_at = due_at or _ensure_utc(row.get("created_at"))
    latest_useful_at = (due_at + timedelta(days=3)) if due_at else None
    expires_at = (due_at + timedelta(days=7)) if due_at else None
    item = {
        "id": f'clarification_candidates:{row.get("candidate_id")}',
        "tenant_id": tenant_id,
        "user_id": _normalize_text(row.get("user_id")),
        "companion_id": companion_id,
        "title": title,
        "reason": reason,
        "attention_type": "clarification",
        "surface_mode": "confirm_first",
        "action_policy": "never_execute",
        "priority": _priority_from_score(_to_float(row.get("priority_score"), 0.55)),
        "urgency": _urgency_from_dates(as_of=as_of, target=due_at),
        "importance": max(60, _importance_from_text(f"{title} {reason}")),
        "confidence": _to_float(row.get("confidence"), 0.0),
        "sensitivity": _sensitivity_for_text(f"{title} {reason}"),
        "earliest_surface_at": _iso(earliest_surface_at),
        "ideal_surface_at": _iso(due_at or row.get("updated_at")),
        "latest_useful_at": _iso(latest_useful_at),
        "expires_at": _iso(expires_at),
        "status": _windowed_status(
            status=_candidate_status_to_attention_status(_normalize_text(row.get("status")), as_of=as_of, latest_useful_at=latest_useful_at),
            as_of=as_of,
            earliest_surface_at=earliest_surface_at,
            latest_useful_at=latest_useful_at,
        ),
        "source_table": "clarification_candidates",
        "source_id": _normalize_text(row.get("candidate_id")),
        "source_object_ids": source_object_ids,
        "source_link_ids": source_link_ids,
        "evidence_refs": evidence_refs,
        "gaps": gaps,
        "missing_metadata": missing_metadata,
        "created_at": _ensure_utc(row.get("created_at")),
        "updated_at": _ensure_utc(row.get("updated_at")),
    }
    return item


def _build_actionable_item(row: Dict[str, Any], *, tenant_id: str, companion_id: str, as_of: datetime) -> Dict[str, Any]:
    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    provenance = row.get("provenance") if isinstance(row.get("provenance"), dict) else {}
    subtype = _normalize_text(row.get("candidate_subtype")).lower()
    record_type = _normalize_text(row.get("record_type")).lower()
    due_at = _ensure_utc(row.get("proposed_due_at") or row.get("due_iso"))
    relevant_from = _ensure_utc(row.get("relevant_from_iso"))
    relevant_until = _ensure_utc(row.get("relevant_until_iso"))
    if subtype == "follow_up":
        attention_type = "follow_up"
    elif subtype in {"todo", "reminder"} or record_type == "reminder_candidate":
        attention_type = "reminder"
    elif record_type == "commitment":
        attention_type = "accountability"
    elif subtype == "nudge":
        attention_type = "opportunity"
    else:
        attention_type = "clarification" if row.get("status") == "needs_review" else "opportunity"
    action_policy = "suggest_only"
    if subtype == "waiting_on":
        action_policy = "draft_only"
    if attention_type == "clarification":
        action_policy = "never_execute"
    title = _normalize_text(row.get("title")) or "Actionable candidate"
    summary = _normalize_text(row.get("summary"))
    reason = summary or f'Actionable candidate from {record_type or "candidate"} is currently relevant.'
    surface_mode = "confirm_first" if attention_type == "clarification" else "proactive_allowed"
    if action_policy == "draft_only":
        surface_mode = "proactive_recommended"
    source_object_ids = _extract_object_ids(metadata) or _extract_object_ids(provenance)
    source_link_ids = _extract_link_ids(metadata) or _extract_link_ids(provenance)
    evidence_refs = _extract_evidence_refs(provenance, metadata)
    gaps, missing_metadata = _gaps_for_item(
        source_object_ids=source_object_ids,
        source_link_ids=source_link_ids,
        evidence_refs=evidence_refs,
        source_table="actionable_candidates",
    )
    earliest_surface_at = relevant_from or due_at or _ensure_utc(row.get("created_at"))
    latest_useful_at = relevant_until or due_at
    expires_at = relevant_until or (due_at + timedelta(days=1) if due_at else None)
    status = _windowed_status(
        status=_candidate_status_to_attention_status(_normalize_text(row.get("status")), as_of=as_of, latest_useful_at=latest_useful_at),
        as_of=as_of,
        earliest_surface_at=earliest_surface_at,
        latest_useful_at=latest_useful_at,
    )
    priority_base = _to_float(row.get("confidence_score") or row.get("confidence"), 0.5)
    item = {
        "id": f'actionable_candidates:{row.get("candidate_id")}',
        "tenant_id": tenant_id,
        "user_id": _normalize_text(row.get("user_id")),
        "companion_id": companion_id,
        "title": title,
        "reason": reason,
        "attention_type": attention_type,
        "surface_mode": surface_mode,
        "action_policy": action_policy,
        "priority": _priority_from_score(priority_base),
        "urgency": _urgency_from_dates(as_of=as_of, target=due_at or relevant_until),
        "importance": _importance_from_text(f"{title} {summary} {subtype}"),
        "confidence": _to_float(row.get("confidence") or row.get("confidence_score"), 0.0),
        "sensitivity": _sensitivity_for_text(f"{title} {summary}"),
        "earliest_surface_at": _iso(earliest_surface_at),
        "ideal_surface_at": _iso(due_at or relevant_from or row.get("updated_at")),
        "latest_useful_at": _iso(latest_useful_at),
        "expires_at": _iso(expires_at),
        "status": status,
        "source_table": "actionable_candidates",
        "source_id": _normalize_text(row.get("candidate_id")),
        "source_object_ids": source_object_ids,
        "source_link_ids": source_link_ids,
        "evidence_refs": evidence_refs,
        "gaps": gaps,
        "missing_metadata": missing_metadata,
        "created_at": _ensure_utc(row.get("created_at")),
        "updated_at": _ensure_utc(row.get("updated_at")),
    }
    return item


def _build_open_thread_item(row: Dict[str, Any], *, tenant_id: str, companion_id: str, as_of: datetime) -> Dict[str, Any]:
    follow_up_after = _ensure_utc(row.get("follow_up_after"))
    source_object_ids = [_normalize_text(row.get("thread_id"))] if _normalize_text(row.get("thread_id")) else []
    evidence_refs = [{"session_id": session_id} for session_id in (row.get("source_session_ids") or []) if _normalize_text(session_id)]
    gaps, missing_metadata = _gaps_for_item(
        source_object_ids=source_object_ids,
        source_link_ids=[],
        evidence_refs=evidence_refs,
        source_table="open_threads",
    )
    title = _normalize_text(row.get("title")) or "Open thread"
    detail = _normalize_text(row.get("detail"))
    reason = detail or "Open thread has a scheduled follow-up window."
    attention_type = "follow_up" if _normalize_text(row.get("category")).lower() in {"commitment", "project", "relationship"} else "check_in"
    priority_map = {"high": 85, "medium": 65, "low": 45}
    earliest_surface_at = follow_up_after
    latest_useful_at = (follow_up_after + timedelta(days=2)) if follow_up_after else None
    expires_at = (follow_up_after + timedelta(days=4)) if follow_up_after else None
    item = {
        "id": f'open_threads:{_normalize_text(row.get("thread_id"))}',
        "tenant_id": tenant_id,
        "user_id": _normalize_text(row.get("user_id")),
        "companion_id": companion_id,
        "title": title,
        "reason": reason,
        "attention_type": attention_type,
        "surface_mode": "proactive_allowed",
        "action_policy": "suggest_only",
        "priority": priority_map.get(_normalize_text(row.get("priority")).lower(), 55),
        "urgency": _urgency_from_dates(as_of=as_of, target=follow_up_after),
        "importance": _importance_from_text(f"{title} {detail}"),
        "confidence": 0.65,
        "sensitivity": _sensitivity_for_text(f"{title} {detail}"),
        "earliest_surface_at": _iso(earliest_surface_at),
        "ideal_surface_at": _iso(follow_up_after),
        "latest_useful_at": _iso(latest_useful_at),
        "expires_at": _iso(expires_at),
        "status": _windowed_status(
            status=_candidate_status_to_attention_status("open", as_of=as_of, latest_useful_at=latest_useful_at),
            as_of=as_of,
            earliest_surface_at=earliest_surface_at,
            latest_useful_at=latest_useful_at,
        ),
        "source_table": "open_threads",
        "source_id": _normalize_text(row.get("thread_id")),
        "source_object_ids": source_object_ids,
        "source_link_ids": [],
        "evidence_refs": evidence_refs,
        "gaps": gaps,
        "missing_metadata": missing_metadata,
        "created_at": _ensure_utc(row.get("created_at")),
        "updated_at": _ensure_utc(row.get("last_updated_at") or row.get("updated_at")),
    }
    return item


def _build_action_item_item(row: Dict[str, Any], *, tenant_id: str, companion_id: str, as_of: datetime) -> Dict[str, Any]:
    due_at = _ensure_utc(row.get("due_at") or row.get("remind_at"))
    source_ref = row.get("source_ref") if isinstance(row.get("source_ref"), dict) else {}
    source_object_ids = [_normalize_text(row.get("id"))] if _normalize_text(row.get("id")) else []
    source_link_ids = _extract_link_ids(source_ref)
    evidence_refs = _extract_evidence_refs(source_ref)
    gaps, missing_metadata = _gaps_for_item(
        source_object_ids=source_object_ids,
        source_link_ids=source_link_ids,
        evidence_refs=evidence_refs,
        source_table="action_items",
    )
    kind = _normalize_text(row.get("kind")).lower()
    attention_type = "accountability" if kind == "habit" else "reminder"
    earliest_surface_at = _ensure_utc(row.get("remind_at")) or due_at or _ensure_utc(row.get("created_at"))
    latest_useful_at = due_at
    expires_at = (due_at + timedelta(days=7)) if due_at else None
    status = _windowed_status(
        status="expired" if due_at and due_at < as_of - timedelta(days=7) else "eligible",
        as_of=as_of,
        earliest_surface_at=earliest_surface_at,
        latest_useful_at=latest_useful_at,
    )
    title = _normalize_text(row.get("title")) or "Action item"
    notes = _normalize_text(row.get("notes"))
    item = {
        "id": f'action_items:{_normalize_text(row.get("id"))}',
        "tenant_id": tenant_id,
        "user_id": _normalize_text(row.get("user_id")),
        "companion_id": companion_id,
        "title": title,
        "reason": notes or "Pending action item is due or overdue.",
        "attention_type": attention_type,
        "surface_mode": "proactive_allowed",
        "action_policy": "suggest_only",
        "priority": 82 if due_at and due_at <= as_of else 70,
        "urgency": _urgency_from_dates(as_of=as_of, target=due_at),
        "importance": _importance_from_text(f"{title} {notes}"),
        "confidence": _to_float(row.get("confidence"), 0.75),
        "sensitivity": _sensitivity_for_text(f"{title} {notes}"),
        "earliest_surface_at": _iso(earliest_surface_at),
        "ideal_surface_at": _iso(due_at or row.get("remind_at") or row.get("updated_at")),
        "latest_useful_at": _iso(latest_useful_at),
        "expires_at": _iso(expires_at),
        "status": status,
        "source_table": "action_items",
        "source_id": _normalize_text(row.get("id")),
        "source_object_ids": source_object_ids,
        "source_link_ids": source_link_ids,
        "evidence_refs": evidence_refs,
        "gaps": gaps,
        "missing_metadata": missing_metadata,
        "created_at": _ensure_utc(row.get("created_at")),
        "updated_at": _ensure_utc(row.get("updated_at")),
    }
    return item


def _build_calendar_item_item(row: Dict[str, Any], *, tenant_id: str, companion_id: str, as_of: datetime) -> Dict[str, Any]:
    starts_at = _ensure_utc(row.get("starts_at"))
    ends_at = _ensure_utc(row.get("ends_at"))
    evidence_refs = row.get("evidence_refs") if isinstance(row.get("evidence_refs"), list) else []
    source_ref = row.get("source_ref") if isinstance(row.get("source_ref"), dict) else {}
    source_object_ids = [_normalize_text(row.get("id"))] if _normalize_text(row.get("id")) else []
    source_link_ids = _extract_link_ids(source_ref)
    gaps, missing_metadata = _gaps_for_item(
        source_object_ids=source_object_ids,
        source_link_ids=source_link_ids,
        evidence_refs=evidence_refs,
        source_table="calendar_items",
    )
    title = _normalize_text(row.get("title")) or "Calendar item"
    description = _normalize_text(row.get("description")) or _normalize_text(row.get("notes"))
    if starts_at and starts_at >= as_of:
        attention_type = "prep" if starts_at <= as_of + timedelta(days=2) else "reminder"
        reason = description or "Upcoming calendar item is approaching."
        latest_useful_at = starts_at
        expires_at = starts_at
        earliest_surface_at = (starts_at - timedelta(hours=12)) if starts_at >= as_of else starts_at
    else:
        attention_type = "review" if ends_at and ends_at >= as_of - timedelta(days=2) else "follow_up"
        reason = description or "Recent calendar item may need follow-up."
        latest_useful_at = (ends_at or starts_at) + timedelta(days=2) if (ends_at or starts_at) else None
        expires_at = latest_useful_at
        earliest_surface_at = ends_at or starts_at
    action_policy = "suggest_only"
    status = _windowed_status(
        status="eligible",
        as_of=as_of,
        earliest_surface_at=earliest_surface_at,
        latest_useful_at=latest_useful_at,
    )
    item = {
        "id": f'calendar_items:{_normalize_text(row.get("id"))}',
        "tenant_id": tenant_id,
        "user_id": _normalize_text(row.get("user_id")),
        "companion_id": companion_id,
        "title": title,
        "reason": reason,
        "attention_type": attention_type,
        "surface_mode": "proactive_allowed",
        "action_policy": action_policy,
        "priority": 78 if attention_type == "prep" else 62,
        "urgency": _urgency_from_dates(as_of=as_of, target=starts_at or ends_at),
        "importance": _importance_from_text(f"{title} {description}"),
        "confidence": _to_float(row.get("confidence"), 0.7),
        "sensitivity": _sensitivity_for_text(f"{title} {description}"),
        "earliest_surface_at": _iso(earliest_surface_at),
        "ideal_surface_at": _iso((starts_at - timedelta(hours=2)) if starts_at and starts_at >= as_of else ends_at or starts_at),
        "latest_useful_at": _iso(latest_useful_at),
        "expires_at": _iso(expires_at),
        "status": status,
        "source_table": "calendar_items",
        "source_id": _normalize_text(row.get("id")),
        "source_object_ids": source_object_ids,
        "source_link_ids": source_link_ids,
        "evidence_refs": [item for item in evidence_refs if isinstance(item, dict)],
        "gaps": gaps,
        "missing_metadata": missing_metadata,
        "created_at": _ensure_utc(row.get("created_at")),
        "updated_at": _ensure_utc(row.get("updated_at")),
    }
    return item


def _match_outcomes_for_item(item: Dict[str, Any], outcome_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    item_id = _normalize_text(item.get("id"))
    source_table = _normalize_text(item.get("source_table"))
    source_id = _normalize_text(item.get("source_id"))
    matched: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for row in outcome_rows:
        outcome_id = _normalize_text(row.get("outcome_id"))
        if outcome_id and outcome_id in seen:
            continue
        row_attention_item_id = _normalize_text(row.get("attention_item_id"))
        row_source_table = _normalize_text(row.get("source_table"))
        row_source_id = _normalize_text(row.get("source_id"))
        item_match = bool(item_id and row_attention_item_id == item_id)
        source_match = bool(source_table and source_id and row_source_table == source_table and row_source_id == source_id)
        if not item_match and not source_match:
            continue
        if outcome_id:
            seen.add(outcome_id)
        matched.append(row)
    matched.sort(
        key=lambda row: (
            _parse_ts(row.get("occurred_at")) or datetime.min.replace(tzinfo=dt_timezone.utc),
            _parse_ts(row.get("created_at")) or datetime.min.replace(tzinfo=dt_timezone.utc),
        ),
        reverse=True,
    )
    return matched


def _apply_outcome_rules(
    item: Dict[str, Any],
    *,
    now: datetime,
    outcome_rows: List[Dict[str, Any]],
) -> tuple[Dict[str, Any], Dict[str, int]]:
    updated = dict(item)
    diagnostics = {
        "outcomeSuppressedCount": 0,
        "snoozedCount": 0,
        "dismissedCount": 0,
        "completedCount": 0,
        "ignoredSuppressedCount": 0,
    }
    if not outcome_rows:
        return updated, diagnostics

    ignored_count = sum(1 for row in outcome_rows if _normalize_text(row.get("outcome_type")).lower() == "ignored")
    if ignored_count == 1:
        updated["priority"] = max(0, int(updated.get("priority") or 0) - 10)
        updated["urgency"] = max(0, int(updated.get("urgency") or 0) - 5)

    for row in outcome_rows:
        outcome_type = _normalize_text(row.get("outcome_type")).lower()
        snoozed_until = _parse_ts(row.get("snoozed_until"))
        suppress_until = _parse_ts(row.get("suppress_until"))
        suppress_active = suppress_until is None or suppress_until > now

        if outcome_type == "completed":
            updated["status"] = "completed"
            diagnostics["completedCount"] = 1
            diagnostics["outcomeSuppressedCount"] = 1
            return updated, diagnostics
        if outcome_type == "dismissed":
            updated["status"] = "dismissed"
            diagnostics["dismissedCount"] = 1
            diagnostics["outcomeSuppressedCount"] = 1
            return updated, diagnostics
        if outcome_type == "dont_bring_up_again" and suppress_active:
            updated["status"] = "suppressed"
            diagnostics["outcomeSuppressedCount"] = 1
            return updated, diagnostics
        if outcome_type == "stale":
            updated["status"] = "expired"
            return updated, diagnostics
        if outcome_type == "expired":
            updated["status"] = "expired"
            return updated, diagnostics
        if outcome_type == "snoozed" and snoozed_until and snoozed_until > now:
            updated["status"] = "snoozed"
            updated["earliest_surface_at"] = snoozed_until.isoformat()
            updated["ideal_surface_at"] = snoozed_until.isoformat()
            diagnostics["snoozedCount"] = 1
            diagnostics["outcomeSuppressedCount"] = 1
            return updated, diagnostics
        if outcome_type in {"surfaced", "acknowledged", "helpful", "not_helpful", "converted_to_task", "converted_to_draft"}:
            continue

    if ignored_count >= 2:
        updated["status"] = "suppressed"
        diagnostics["ignoredSuppressedCount"] = 1
        diagnostics["outcomeSuppressedCount"] = 1
    return updated, diagnostics


def _should_include_item(*, status: str, include_expired: bool, include_suppressed: bool) -> bool:
    normalized = _normalize_text(status).lower()
    if normalized == "expired":
        return include_expired
    if normalized in {"completed", "dismissed", "suppressed", "archived", "snoozed"}:
        return include_suppressed
    return normalized not in DEFAULT_EXCLUDED_STATUSES


async def build_attention_preview(
    db: Any,
    *,
    tenant_id: str,
    user_id: str,
    companion_id: str = DEFAULT_COMPANION_ID,
    limit: int = 20,
    include_expired: bool = False,
    include_suppressed: bool = False,
    as_of: Optional[datetime] = None,
) -> Dict[str, Any]:
    safe_limit = max(1, min(int(limit or 20), 200))
    now = as_of or datetime.now(dt_timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt_timezone.utc)
    companion_key = _normalize_text(companion_id) or DEFAULT_COMPANION_ID
    if companion_key not in COMPANION_PROFILES:
        companion_key = DEFAULT_COMPANION_ID

    living_context_row = await db.fetchone(
        """
        SELECT current_focus, emotional_texture, primary_tension, relationship_pulse, why_it_matters, sophie_directives
        FROM living_context
        WHERE user_id=$1::text
        """,
        user_id,
    )
    living_context = dict(living_context_row) if living_context_row else {}

    actionable_rows = await db.fetch(
        """
        SELECT candidate_id, tenant_id, user_id, record_type, candidate_subtype, title, summary,
               due_iso, relevant_from_iso, relevant_until_iso, status, confidence_score, confidence,
               provenance, metadata, created_at, updated_at, proposed_due_at
        FROM actionable_candidates
        WHERE tenant_id=$1::text
          AND user_id=$2::text
          AND status IN ('detected','needs_review','confirmed')
        ORDER BY COALESCE(proposed_due_at, due_iso, relevant_until_iso, updated_at) ASC NULLS LAST, updated_at DESC
        LIMIT $3::int
        """,
        tenant_id,
        user_id,
        safe_limit,
    )
    follow_up_rows = await db.fetch(
        """
        SELECT candidate_id, tenant_id, user_id, candidate_key, title, reason, priority_score, confidence,
               due_at, status, source_turn_refs, metadata, created_at, updated_at
        FROM follow_up_candidates
        WHERE tenant_id=$1::text
          AND user_id=$2::text
          AND status='shadow_open'
        ORDER BY priority_score DESC, due_at NULLS LAST, updated_at DESC
        LIMIT $3::int
        """,
        tenant_id,
        user_id,
        safe_limit,
    )
    clarification_rows = await db.fetch(
        """
        SELECT candidate_id, tenant_id, user_id, candidate_key, title, reason, priority_score, confidence,
               due_at, status, source_turn_refs, metadata, created_at, updated_at
        FROM clarification_candidates
        WHERE tenant_id=$1::text
          AND user_id=$2::text
          AND status='shadow_open'
        ORDER BY priority_score DESC, due_at NULLS LAST, updated_at DESC
        LIMIT $3::int
        """,
        tenant_id,
        user_id,
        safe_limit,
    )
    thread_rows = await db.fetch(
        """
        SELECT thread_id, user_id, title, detail, status, priority, category, source_session_ids, follow_up_after
        FROM open_threads
        WHERE user_id=$1::text
          AND status='open'
          AND follow_up_after IS NOT NULL
          AND follow_up_after <= $2::timestamptz
        ORDER BY follow_up_after ASC, last_updated_at DESC NULLS LAST
        LIMIT $3::int
        """,
        user_id,
        now + timedelta(days=1),
        safe_limit,
    )
    action_item_rows = await db.fetch(
        """
        SELECT id, tenant_id, user_id, kind, title, notes, status, due_at, remind_at, source_ref, confidence, created_at, updated_at
        FROM action_items
        WHERE tenant_id=$1::text
          AND user_id=$2::text
          AND status='pending'
          AND (
            (due_at IS NOT NULL AND due_at <= $3::timestamptz)
            OR (remind_at IS NOT NULL AND remind_at <= $3::timestamptz)
          )
        ORDER BY COALESCE(remind_at, due_at, updated_at) ASC, updated_at DESC
        LIMIT $4::int
        """,
        tenant_id,
        user_id,
        now + timedelta(days=1),
        safe_limit,
    )
    calendar_rows = await db.fetch(
        """
        SELECT id, tenant_id, user_id, title, description, notes, starts_at, ends_at, status, source_ref, evidence_refs, confidence
        FROM calendar_items
        WHERE tenant_id=$1::text
          AND user_id=$2::text
          AND status='confirmed'
          AND (
            starts_at BETWEEN $3::timestamptz AND $4::timestamptz
            OR (ends_at IS NOT NULL AND ends_at BETWEEN $5::timestamptz AND $6::timestamptz)
          )
        ORDER BY starts_at ASC, updated_at DESC
        LIMIT $7::int
        """,
        tenant_id,
        user_id,
        now,
        now + timedelta(days=3),
        now - timedelta(days=2),
        now,
        safe_limit,
    )

    items: List[Dict[str, Any]] = []
    for row in actionable_rows or []:
        items.append(_build_actionable_item(dict(row), tenant_id=tenant_id, companion_id=companion_key, as_of=now))
    for row in follow_up_rows or []:
        items.append(_build_follow_up_item(dict(row), tenant_id=tenant_id, companion_id=companion_key, as_of=now))
    for row in clarification_rows or []:
        items.append(_build_clarification_item(dict(row), tenant_id=tenant_id, companion_id=companion_key, as_of=now))
    for row in thread_rows or []:
        items.append(_build_open_thread_item(dict(row), tenant_id=tenant_id, companion_id=companion_key, as_of=now))
    for row in action_item_rows or []:
        items.append(_build_action_item_item(dict(row), tenant_id=tenant_id, companion_id=companion_key, as_of=now))
    for row in calendar_rows or []:
        items.append(_build_calendar_item_item(dict(row), tenant_id=tenant_id, companion_id=companion_key, as_of=now))

    profile_filtered_items: List[Dict[str, Any]] = []
    suppressed_count = 0
    for raw_item in items:
        modified = _apply_living_context_modifier(raw_item, living_context)
        filtered = _filter_for_companion(modified, companion_key)
        if filtered is not None:
            profile_filtered_items.append(filtered)
        else:
            suppressed_count += 1

    outcome_rows = await fetch_attention_outcomes(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        attention_item_ids=[_normalize_text(item.get("id")) for item in profile_filtered_items],
        source_refs=[
            (_normalize_text(item.get("source_table")), _normalize_text(item.get("source_id")))
            for item in profile_filtered_items
        ],
    )

    by_status: Dict[str, int] = {}
    by_source_table: Dict[str, int] = {}
    by_attention_type: Dict[str, int] = {}
    expired_count = 0
    outcome_suppressed_count = 0
    snoozed_count = 0
    dismissed_count = 0
    completed_count = 0
    ignored_suppressed_count = 0
    visible_items: List[Dict[str, Any]] = []
    for item in profile_filtered_items:
        item_outcomes = _match_outcomes_for_item(item, outcome_rows)
        adjusted_item, outcome_diagnostics = _apply_outcome_rules(item, now=now, outcome_rows=item_outcomes)
        _increment_count(by_status, adjusted_item.get("status"))
        _increment_count(by_source_table, item.get("source_table"))
        _increment_count(by_attention_type, item.get("attention_type"))
        outcome_suppressed_count += outcome_diagnostics["outcomeSuppressedCount"]
        snoozed_count += outcome_diagnostics["snoozedCount"]
        dismissed_count += outcome_diagnostics["dismissedCount"]
        completed_count += outcome_diagnostics["completedCount"]
        ignored_suppressed_count += outcome_diagnostics["ignoredSuppressedCount"]
        if adjusted_item.get("status") == "expired":
            expired_count += 1
        if _should_include_item(
            status=adjusted_item.get("status"),
            include_expired=include_expired,
            include_suppressed=include_suppressed,
        ):
            visible_items.append(adjusted_item)

    visible_items.sort(key=_sort_key)
    output_items = visible_items[:safe_limit]

    return {
        "tenantId": tenant_id,
        "userId": user_id,
        "companionId": companion_key,
        "asOf": now.isoformat(),
        "items": output_items,
        "metadata": {
            "totalGenerated": len(items),
            "returnedCount": len(output_items),
            "suppressedCount": suppressed_count,
            "expiredCount": expired_count,
            "outcomeSuppressedCount": outcome_suppressed_count,
            "snoozedCount": snoozed_count,
            "dismissedCount": dismissed_count,
            "completedCount": completed_count,
            "ignoredSuppressedCount": ignored_suppressed_count,
            "byStatus": by_status,
            "bySourceTable": by_source_table,
            "byAttentionType": by_attention_type,
            "sourceTables": [
                "actionable_candidates",
                "follow_up_candidates",
                "clarification_candidates",
                "open_threads",
                "action_items",
                "calendar_items",
            ],
            "livingContextUsedAsModifier": bool(living_context),
            "profileApplied": companion_key,
            "includeExpired": bool(include_expired),
            "includeSuppressed": bool(include_suppressed),
            "outcomeRulesApplied": True,
            "readOnly": True,
        },
    }
