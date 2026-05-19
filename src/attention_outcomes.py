from __future__ import annotations

from datetime import datetime, timezone as dt_timezone
from typing import Any, Dict, List, Optional


ALLOWED_OUTCOME_TYPES = {
    "surfaced",
    "acknowledged",
    "ignored",
    "dismissed",
    "snoozed",
    "completed",
    "helpful",
    "not_helpful",
    "dont_bring_up_again",
    "stale",
    "converted_to_task",
    "converted_to_draft",
    "expired",
}


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_metadata(value: Any) -> Dict[str, Any]:
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
    if not parsed:
        return None
    return parsed.isoformat()


def normalize_outcome_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "outcomeId": _normalize_text(row.get("outcome_id")),
        "tenantId": _normalize_text(row.get("tenant_id")),
        "userId": _normalize_text(row.get("user_id")),
        "companionId": _normalize_text(row.get("companion_id")) or None,
        "attentionItemId": _normalize_text(row.get("attention_item_id")),
        "sourceTable": _normalize_text(row.get("source_table")) or None,
        "sourceId": _normalize_text(row.get("source_id")) or None,
        "outcomeType": _normalize_text(row.get("outcome_type")),
        "outcomeReason": _normalize_text(row.get("outcome_reason")) or None,
        "surfaceMode": _normalize_text(row.get("surface_mode")) or None,
        "actionPolicy": _normalize_text(row.get("action_policy")) or None,
        "occurredAt": _iso(row.get("occurred_at")),
        "snoozedUntil": _iso(row.get("snoozed_until")),
        "suppressUntil": _iso(row.get("suppress_until")),
        "metadata": _normalize_metadata(row.get("metadata")),
        "createdAt": _iso(row.get("created_at")),
    }


async def record_attention_outcome(
    db: Any,
    *,
    tenant_id: str,
    user_id: str,
    companion_id: Optional[str],
    attention_item_id: str,
    source_table: Optional[str],
    source_id: Optional[str],
    outcome_type: str,
    outcome_reason: Optional[str] = None,
    surface_mode: Optional[str] = None,
    action_policy: Optional[str] = None,
    snoozed_until: Optional[datetime] = None,
    suppress_until: Optional[datetime] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    normalized_outcome_type = _normalize_text(outcome_type).lower()
    if normalized_outcome_type not in ALLOWED_OUTCOME_TYPES:
        raise ValueError(f"Unsupported outcomeType: {outcome_type}")

    row = await db.fetchone(
        """
        INSERT INTO attention_outcomes (
          tenant_id,
          user_id,
          companion_id,
          attention_item_id,
          source_table,
          source_id,
          outcome_type,
          outcome_reason,
          surface_mode,
          action_policy,
          snoozed_until,
          suppress_until,
          metadata
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        RETURNING outcome_id, tenant_id, user_id, companion_id, attention_item_id, source_table, source_id,
                  outcome_type, outcome_reason, surface_mode, action_policy, occurred_at, snoozed_until,
                  suppress_until, metadata, created_at
        """,
        tenant_id,
        user_id,
        _normalize_text(companion_id) or None,
        attention_item_id,
        _normalize_text(source_table) or None,
        _normalize_text(source_id) or None,
        normalized_outcome_type,
        _normalize_text(outcome_reason) or None,
        _normalize_text(surface_mode) or None,
        _normalize_text(action_policy) or None,
        snoozed_until,
        suppress_until,
        _normalize_metadata(metadata),
    )
    return normalize_outcome_row(row or {})


async def fetch_attention_outcomes(
    db: Any,
    *,
    tenant_id: str,
    user_id: str,
    attention_item_ids: List[str],
    source_refs: List[tuple[str, str]],
) -> List[Dict[str, Any]]:
    normalized_item_ids = [item_id for item_id in (_normalize_text(item) for item in attention_item_ids) if item_id]
    normalized_source_refs = [
        {"source_table": table, "source_id": source_id}
        for table, source_id in source_refs
        if _normalize_text(table) and _normalize_text(source_id)
    ]
    if not normalized_item_ids and not normalized_source_refs:
        return []

    rows = await db.fetch(
        """
        SELECT outcome_id, tenant_id, user_id, companion_id, attention_item_id, source_table, source_id,
               outcome_type, outcome_reason, surface_mode, action_policy, occurred_at, snoozed_until,
               suppress_until, metadata, created_at
        FROM attention_outcomes
        WHERE tenant_id=$1
          AND user_id=$2
          AND (
            attention_item_id = ANY($3::text[])
            OR (
              source_table IS NOT NULL
              AND source_id IS NOT NULL
              AND EXISTS (
                SELECT 1
                FROM jsonb_to_recordset($4::jsonb) AS refs(source_table text, source_id text)
                WHERE refs.source_table = attention_outcomes.source_table
                  AND refs.source_id = attention_outcomes.source_id
              )
            )
          )
        ORDER BY occurred_at DESC, created_at DESC
        """,
        tenant_id,
        user_id,
        normalized_item_ids,
        normalized_source_refs,
    )
    return [dict(row) for row in rows or []]
