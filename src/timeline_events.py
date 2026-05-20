from __future__ import annotations

from datetime import datetime, timezone as dt_timezone
from typing import Any, Dict, List, Optional


ALLOWED_TIMELINE_TYPES = {"interaction", "user_life", "calendar_event", "relationship", "system"}
ALLOWED_CORRECTION_COMMANDS = {
    "forget_that",
    "dont_bring_that_up_again",
    "thats_wrong",
    "thats_done",
    "this_is_important",
    "remember_this",
}
COMMAND_TO_EFFECT = {
    "forget_that": "forget",
    "dont_bring_that_up_again": "suppress_related",
    "thats_wrong": "correct",
    "thats_done": "mark_done",
    "this_is_important": "confirm",
    "remember_this": "confirm",
}
COMMAND_TO_EVENT_TYPE = {
    "forget_that": "user_forget_request",
    "dont_bring_that_up_again": "user_dont_bring_up_again",
    "thats_wrong": "user_correction",
    "thats_done": "user_mark_done",
    "this_is_important": "memory_confirmation",
    "remember_this": "memory_confirmation",
}
COMMAND_TO_TIMELINE_TYPE = {
    "forget_that": "interaction",
    "dont_bring_that_up_again": "interaction",
    "thats_wrong": "interaction",
    "thats_done": "interaction",
    "this_is_important": "system",
    "remember_this": "system",
}
CORRECTION_EVENT_TYPES = {
    "user_correction",
    "user_forget_request",
    "user_mark_done",
    "user_dont_bring_up_again",
    "memory_confirmation",
}
SUPPRESSING_EFFECTS = {"suppress_related", "mark_done", "correct", "forget"}


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _normalize_list(value: Any) -> List[Any]:
    return list(value) if isinstance(value, list) else []


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


def normalize_timeline_event(row: Dict[str, Any]) -> Dict[str, Any]:
    metadata = _normalize_dict(row.get("metadata"))
    provenance_source_table = _normalize_text(row.get("source_table")) or None
    provenance_source_id = _normalize_text(row.get("source_id")) or None
    object_refs = [item for item in _normalize_list(row.get("object_refs")) if isinstance(item, dict)]
    evidence_refs = [item for item in _normalize_list(row.get("evidence_refs")) if isinstance(item, dict)]
    return {
        "id": f"timeline_events:{_normalize_text(row.get('event_id'))}",
        "event_id": _normalize_text(row.get("event_id")),
        "tenant_id": _normalize_text(row.get("tenant_id")),
        "user_id": _normalize_text(row.get("user_id")),
        "timeline_type": _normalize_text(row.get("timeline_type")) or None,
        "event_type": _normalize_text(row.get("event_type")),
        "domain": _normalize_text(row.get("domain")) or "unknown",
        "title": _normalize_text(row.get("title")) or "Timeline event",
        "summary": _normalize_text(row.get("summary")) or None,
        "source_table": "timeline_events",
        "source_id": _normalize_text(row.get("event_id")),
        "occurred_at": _iso(row.get("occurred_at")),
        "first_seen_at": _iso(row.get("created_at") or row.get("observed_at")),
        "last_seen_at": _iso(row.get("updated_at") or row.get("observed_at")),
        "expires_at": _iso(row.get("expires_at")),
        "status": _normalize_text(row.get("status")) or "active",
        "confidence": float(row.get("confidence")) if row.get("confidence") is not None else None,
        "salience": float(row.get("salience")) if row.get("salience") is not None else None,
        "evidence_refs": evidence_refs,
        "related_object_ids": [
            _normalize_text(item.get("id") or item.get("targetId") or item.get("sourceId"))
            for item in object_refs
            if _normalize_text(item.get("id") or item.get("targetId") or item.get("sourceId"))
        ],
        "related_link_ids": [],
        "freshness": "unknown",
        "gaps": [],
        "missing_metadata": [],
        "actor": _normalize_text(row.get("actor")) or None,
        "subject": _normalize_text(row.get("subject")) or None,
        "object_refs": object_refs,
        "user_corrected": bool(row.get("user_corrected")),
        "user_visible": bool(row.get("user_visible", True)),
        "effect": _normalize_text(row.get("effect")) or None,
        "metadata": {
            **metadata,
            "timeline_source_table": provenance_source_table,
            "timeline_source_id": provenance_source_id,
        },
    }


async def fetch_timeline_event_rows(
    db: Any,
    *,
    tenant_id: str,
    user_id: str,
    timeline_type: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    params: List[Any] = [tenant_id, user_id]
    timeline_filter = ""
    if _normalize_text(timeline_type):
        timeline_filter = "AND timeline_type=$3::text"
        params.append(_normalize_text(timeline_type))
    limit_clause = ""
    if limit is not None:
        limit_clause = f"LIMIT ${len(params) + 1}::int"
        params.append(int(limit))
    rows = await db.fetch(
        f"""
        SELECT event_id, tenant_id, user_id, timeline_type, event_type, domain, title, summary,
               occurred_at, observed_at, valid_from, valid_until, expires_at, status, confidence,
               salience, actor, subject, object_refs, source_table, source_id, evidence_refs,
               user_corrected, user_visible, effect, metadata, created_at, updated_at
        FROM timeline_events
        WHERE tenant_id=$1::text
          AND user_id=$2::text
          {timeline_filter}
        ORDER BY COALESCE(occurred_at, observed_at, created_at) DESC, created_at DESC
        {limit_clause}
        """,
        *params,
    )
    return [dict(row) for row in rows or []]


async def record_timeline_event(
    db: Any,
    *,
    tenant_id: str,
    user_id: str,
    timeline_type: str,
    event_type: str,
    domain: Optional[str] = None,
    title: Optional[str] = None,
    summary: Optional[str] = None,
    occurred_at: Optional[datetime] = None,
    valid_from: Optional[datetime] = None,
    valid_until: Optional[datetime] = None,
    expires_at: Optional[datetime] = None,
    status: str = "active",
    confidence: Optional[float] = None,
    salience: Optional[float] = None,
    actor: Optional[str] = None,
    subject: Optional[str] = None,
    object_refs: Optional[List[Dict[str, Any]]] = None,
    source_table: Optional[str] = None,
    source_id: Optional[str] = None,
    evidence_refs: Optional[List[Dict[str, Any]]] = None,
    user_corrected: bool = False,
    user_visible: bool = True,
    effect: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    normalized_timeline_type = _normalize_text(timeline_type)
    if normalized_timeline_type not in ALLOWED_TIMELINE_TYPES:
        raise ValueError(f"Unsupported timeline_type: {timeline_type}")
    row = await db.fetchone(
        """
        INSERT INTO timeline_events (
          tenant_id,
          user_id,
          timeline_type,
          event_type,
          domain,
          title,
          summary,
          occurred_at,
          valid_from,
          valid_until,
          expires_at,
          status,
          confidence,
          salience,
          actor,
          subject,
          object_refs,
          source_table,
          source_id,
          evidence_refs,
          user_corrected,
          user_visible,
          effect,
          metadata
        )
        VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
          $17, $18, $19, $20, $21, $22, $23, $24
        )
        RETURNING event_id, tenant_id, user_id, timeline_type, event_type, domain, title, summary,
                  occurred_at, observed_at, valid_from, valid_until, expires_at, status, confidence,
                  salience, actor, subject, object_refs, source_table, source_id, evidence_refs,
                  user_corrected, user_visible, effect, metadata, created_at, updated_at
        """,
        tenant_id,
        user_id,
        normalized_timeline_type,
        _normalize_text(event_type),
        _normalize_text(domain) or None,
        _normalize_text(title) or None,
        _normalize_text(summary) or None,
        occurred_at,
        valid_from,
        valid_until,
        expires_at,
        _normalize_text(status) or "active",
        confidence,
        salience,
        _normalize_text(actor) or None,
        _normalize_text(subject) or None,
        [item for item in _normalize_list(object_refs) if isinstance(item, dict)],
        _normalize_text(source_table) or None,
        _normalize_text(source_id) or None,
        [item for item in _normalize_list(evidence_refs) if isinstance(item, dict)],
        bool(user_corrected),
        bool(user_visible),
        _normalize_text(effect) or None,
        _normalize_dict(metadata),
    )
    return normalize_timeline_event(dict(row or {}))


def build_object_refs(
    *,
    target_type: Optional[str],
    target_id: Optional[str],
    source_table: Optional[str],
    source_id: Optional[str],
    metadata: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    refs: List[Dict[str, Any]] = []
    normalized_target_type = _normalize_text(target_type)
    normalized_target_id = _normalize_text(target_id)
    if normalized_target_type or normalized_target_id:
        refs.append(
            {
                "targetType": normalized_target_type or None,
                "targetId": normalized_target_id or None,
            }
        )
    normalized_source_table = _normalize_text(source_table)
    normalized_source_id = _normalize_text(source_id)
    if normalized_source_table or normalized_source_id:
        refs.append(
            {
                "sourceTable": normalized_source_table or None,
                "sourceId": normalized_source_id or None,
            }
        )
    extra_refs = _normalize_list(_normalize_dict(metadata).get("object_refs"))
    refs.extend(item for item in extra_refs if isinstance(item, dict))
    return refs


def timeline_event_matches_target(
    event: Dict[str, Any],
    *,
    source_table: Optional[str],
    source_id: Optional[str],
    target_id: Optional[str],
    object_ids: Optional[List[str]] = None,
) -> bool:
    normalized_source_table = _normalize_text(source_table)
    normalized_source_id = _normalize_text(source_id)
    normalized_target_id = _normalize_text(target_id)
    normalized_object_ids = {_normalize_text(item) for item in (object_ids or []) if _normalize_text(item)}
    if normalized_source_table and normalized_source_id:
        if _normalize_text(event.get("source_table")) == normalized_source_table and _normalize_text(event.get("source_id")) == normalized_source_id:
            return True
    metadata = _normalize_dict(event.get("metadata"))
    candidate_ids = {
        _normalize_text(metadata.get("targetId")),
        _normalize_text(metadata.get("sourceId")),
        _normalize_text(event.get("subject")),
        _normalize_text(event.get("source_id")),
    }
    if normalized_target_id and normalized_target_id in candidate_ids:
        return True
    object_refs = [item for item in _normalize_list(event.get("object_refs")) if isinstance(item, dict)]
    for ref in object_refs:
        if normalized_target_id and _normalize_text(ref.get("targetId")) == normalized_target_id:
            return True
        if normalized_source_table and normalized_source_id:
            if _normalize_text(ref.get("sourceTable")) == normalized_source_table and _normalize_text(ref.get("sourceId")) == normalized_source_id:
                return True
        if normalized_object_ids and _normalize_text(ref.get("targetId")) in normalized_object_ids:
            return True
    related_ids = {_normalize_text(item) for item in _normalize_list(event.get("related_object_ids")) if _normalize_text(item)}
    if normalized_object_ids and related_ids.intersection(normalized_object_ids):
        return True
    return False


def correction_command_to_defaults(command: str) -> Dict[str, str]:
    normalized = _normalize_text(command).lower()
    if normalized not in ALLOWED_CORRECTION_COMMANDS:
        raise ValueError(f"Unsupported command: {command}")
    return {
        "event_type": COMMAND_TO_EVENT_TYPE[normalized],
        "effect": COMMAND_TO_EFFECT[normalized],
        "timeline_type": COMMAND_TO_TIMELINE_TYPE[normalized],
    }
