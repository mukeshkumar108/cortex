from __future__ import annotations

from datetime import datetime, timezone as dt_timezone
import logging
from typing import Any, Dict, List, Optional

from .db import Database

logger = logging.getLogger(__name__)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _normalize_text_list(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    out: List[str] = []
    seen = set()
    for item in value:
        text = _normalize_text(item)
        if not text or text in seen:
            continue
        out.append(text)
        seen.add(text)
    return out


def _normalize_object_list(value: Any) -> List[Dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _normalize_extra_attributes(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _to_iso_utc(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        dt = value
    else:
        raw = _normalize_text(value)
        if not raw:
            return None
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            return raw
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=dt_timezone.utc)
    return dt.astimezone(dt_timezone.utc).isoformat().replace("+00:00", "Z")


def build_summary_extra_attributes(
    *,
    summary_payload: Dict[str, Any],
    reference_time: Any,
    episode_uuid: Optional[str] = None,
) -> Dict[str, Any]:
    extra = {
        "summary_quality_tier": _normalize_text(summary_payload.get("summary_quality_tier")) or None,
        "summary_source": _normalize_text(summary_payload.get("summary_source")) or None,
        "summary_facts": _normalize_text(summary_payload.get("summary_facts")) or None,
        "tone": _normalize_text(summary_payload.get("tone")) or None,
        "moment": _normalize_text(summary_payload.get("moment")) or None,
        "decisions": _normalize_text_list(summary_payload.get("decisions")),
        "unresolved": _normalize_text_list(summary_payload.get("unresolved")),
        "index_text": _normalize_text(summary_payload.get("index_text")) or None,
        "salience": _normalize_text(summary_payload.get("salience")) or "low",
        "reference_time": _to_iso_utc(reference_time),
    }
    if episode_uuid:
        extra["episode_uuid"] = _normalize_text(episode_uuid)
    return {key: value for key, value in extra.items() if value not in (None, "")}


async def upsert_session_summary(
    db: Database,
    *,
    tenant_id: str,
    user_id: str,
    session_id: str,
    summary_text: Optional[str],
    bridge_text: Optional[str],
    key_points: Any = None,
    decisions: Any = None,
    unresolved_items: Any = None,
    pending_actions: Any = None,
    people_mentioned: Any = None,
    events_mentioned: Any = None,
    state_signals: Any = None,
    topics: Any = None,
    do_not_overinfer: Any = None,
    evidence_refs: Any = None,
    extra_attributes: Optional[Dict[str, Any]] = None,
    model: Optional[str] = None,
    status: str = "active",
) -> None:
    attrs = _normalize_extra_attributes(extra_attributes)
    await db.execute(
        """
        INSERT INTO session_summaries (
          tenant_id,
          user_id,
          session_id,
          summary_text,
          bridge_text,
          key_points,
          decisions,
          unresolved_items,
          pending_actions,
          people_mentioned,
          events_mentioned,
          state_signals,
          topics,
          do_not_overinfer,
          evidence_refs,
          extra_attributes,
          model,
          status
        )
        VALUES (
          $1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb, $9::jsonb, $10::jsonb,
          $11::jsonb, $12::jsonb, $13::jsonb, $14::jsonb, $15::jsonb, $16::jsonb, $17, $18
        )
        ON CONFLICT (tenant_id, user_id, session_id)
        DO UPDATE SET
          summary_text = EXCLUDED.summary_text,
          bridge_text = EXCLUDED.bridge_text,
          key_points = EXCLUDED.key_points,
          decisions = EXCLUDED.decisions,
          unresolved_items = EXCLUDED.unresolved_items,
          pending_actions = EXCLUDED.pending_actions,
          people_mentioned = EXCLUDED.people_mentioned,
          events_mentioned = EXCLUDED.events_mentioned,
          state_signals = EXCLUDED.state_signals,
          topics = EXCLUDED.topics,
          do_not_overinfer = EXCLUDED.do_not_overinfer,
          evidence_refs = EXCLUDED.evidence_refs,
          extra_attributes = EXCLUDED.extra_attributes,
          model = EXCLUDED.model,
          status = EXCLUDED.status,
          updated_at = NOW()
        """,
        tenant_id,
        user_id,
        session_id,
        _normalize_text(summary_text) or None,
        _normalize_text(bridge_text) or None,
        _normalize_text_list(key_points),
        _normalize_text_list(decisions),
        _normalize_text_list(unresolved_items),
        _normalize_text_list(pending_actions),
        _normalize_text_list(people_mentioned),
        _normalize_text_list(events_mentioned),
        _normalize_text_list(state_signals),
        _normalize_text_list(topics),
        _normalize_text_list(do_not_overinfer),
        _normalize_object_list(evidence_refs),
        attrs,
        _normalize_text(model) or None,
        _normalize_text(status) or "active",
    )


def normalize_session_summary_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(row, dict):
        return None
    attrs = _normalize_extra_attributes(row.get("extra_attributes"))
    attrs.setdefault("summary_text", _normalize_text(row.get("summary_text")) or None)
    attrs.setdefault("bridge_text", _normalize_text(row.get("bridge_text")) or None)
    attrs.setdefault("session_id", _normalize_text(row.get("session_id")) or None)
    attrs.setdefault("created_at", _to_iso_utc(row.get("created_at")))
    attrs.setdefault("updated_at", _to_iso_utc(row.get("updated_at")))
    attrs.setdefault("reference_time", attrs.get("reference_time") or _to_iso_utc(row.get("updated_at")) or _to_iso_utc(row.get("created_at")))
    attrs.setdefault("decisions", _normalize_text_list(row.get("decisions")))
    attrs.setdefault("unresolved", _normalize_text_list(row.get("unresolved_items")))
    attrs.setdefault("index_text", _normalize_text(attrs.get("index_text")) or _normalize_text(row.get("summary_text")) or None)
    attrs.setdefault("salience", _normalize_text(attrs.get("salience")) or "low")
    summary_text = _normalize_text(row.get("summary_text"))
    bridge_text = _normalize_text(row.get("bridge_text"))
    return {
        "session_id": _normalize_text(row.get("session_id")) or None,
        "created_at": _to_iso_utc(row.get("created_at")),
        "updated_at": _to_iso_utc(row.get("updated_at")),
        "summary_text": summary_text,
        "bridge_text": bridge_text,
        "summary": _normalize_text(attrs.get("index_text")) or summary_text,
        "attributes": attrs,
        "name": f"session_summary_{_normalize_text(row.get('session_id'))}",
        "labels": ["SessionSummary"],
        "type": "SessionSummary",
    }


async def fetch_recent_session_summaries(
    db: Database,
    *,
    tenant_id: str,
    user_id: str,
    limit: int = 12,
) -> List[Dict[str, Any]]:
    rows = await db.fetch(
        """
        SELECT
          summary_id,
          tenant_id,
          user_id,
          session_id,
          summary_text,
          bridge_text,
          key_points,
          decisions,
          unresolved_items,
          pending_actions,
          people_mentioned,
          events_mentioned,
          state_signals,
          topics,
          do_not_overinfer,
          evidence_refs,
          extra_attributes,
          model,
          status,
          created_at,
          updated_at
        FROM session_summaries
        WHERE tenant_id = $1
          AND user_id = $2
          AND status = 'active'
        ORDER BY COALESCE(updated_at, created_at) DESC, created_at DESC
        LIMIT $3
        """,
        tenant_id,
        user_id,
        max(1, min(int(limit or 12), 100)),
    )
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        normalized = normalize_session_summary_row(row)
        if normalized:
            out.append(normalized)
    return out
