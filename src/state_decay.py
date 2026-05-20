from __future__ import annotations

from datetime import datetime, timedelta, timezone as dt_timezone
from typing import Any, Dict, List, Optional

from .fast_handover import get_latest_fast_handover_packet
from .timeline_events import fetch_timeline_event_rows
from .timeline_read_model import _ensure_utc, _normalize_text


DEFAULT_COMPANION_ID = "sophie"
DEFAULT_LIMIT = 25
MAX_LIMIT = 100
STATE_EVENT_TYPES = {"state_signal", "state_decay", "state_suppression"}
SENSITIVE_TERMS = {
    "anxious",
    "overwhelmed",
    "sad",
    "angry",
    "furious",
    "hurt",
    "panic",
    "depressed",
    "unwell",
    "pain",
    "stressed",
}


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


def _note_is_sensitive(text: str) -> bool:
    lowered = text.lower()
    return any(term in lowered for term in SENSITIVE_TERMS)


def _append_unique(items: List[str], value: str) -> None:
    clean = _normalize_text(value)
    if clean and clean not in items:
        items.append(clean)


def _build_state_note(
    *,
    source_table: str,
    source_id: str,
    title: Optional[str],
    summary: Optional[str],
    observed_at: Any,
    occurred_at: Any,
    freshness: str,
    status: str,
    confidence: Optional[float],
    evidence_refs: Any,
    gaps: Optional[List[str]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    text = _normalize_text(summary) or _normalize_text(title) or "State note"
    evidence_list = [item for item in (evidence_refs or []) if isinstance(item, dict)]
    note_gaps = list(gaps or [])
    if not evidence_list:
        note_gaps.append("missing_evidence_refs")
    return {
        "text": text,
        "title": _normalize_text(title) or None,
        "source_table": source_table,
        "source_id": source_id,
        "occurred_at": _iso(occurred_at),
        "observed_at": _iso(observed_at),
        "freshness": freshness,
        "status": _normalize_text(status) or "unknown",
        "confidence": confidence,
        "evidence_refs": evidence_list,
        "gaps": note_gaps,
        "metadata": metadata or {},
    }


def _classify_state_age(
    *,
    age: timedelta,
    reinforced: bool,
    single_session: bool,
    sensitive: bool,
) -> tuple[str, List[str]]:
    gaps: List[str] = []
    if age > timedelta(days=14) and not reinforced:
        return "historical", gaps
    if age > timedelta(hours=48) and not reinforced:
        return "stale", gaps
    if age > timedelta(hours=24):
        return "provisional", gaps
    if single_session:
        gaps.append("single_session_state_is_provisional")
        return "provisional", gaps
    if sensitive:
        gaps.append("sensitive_state_should_not_be_proactive")
    return "current", gaps


async def build_state_decay_report(
    db: Any,
    *,
    tenant_id: str,
    user_id: str,
    companion_id: str = DEFAULT_COMPANION_ID,
    include_historical: bool = False,
    limit: int = DEFAULT_LIMIT,
    as_of: Optional[datetime] = None,
) -> Dict[str, Any]:
    safe_limit = max(1, min(int(limit or DEFAULT_LIMIT), MAX_LIMIT))
    now = as_of or datetime.now(dt_timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt_timezone.utc)

    timeline_event_rows = await fetch_timeline_event_rows(db, tenant_id=tenant_id, user_id=user_id, limit=max(safe_limit * 2, 20))
    session_change_rows = [
        dict(row)
        for row in await db.fetch(
            """
            SELECT change_id, tenant_id, user_id, kind, title, summary, effective_iso, provenance,
                   confidence_score, status, created_at, updated_at
            FROM session_changes
            WHERE tenant_id=$1::text
              AND user_id=$2::text
            ORDER BY COALESCE(effective_iso, updated_at, created_at) DESC
            LIMIT $3::int
            """,
            tenant_id,
            user_id,
            max(safe_limit * 2, 20),
        )
    ]
    living_context_row = await db.fetchone(
        """
        SELECT current_focus, emotional_texture, primary_tension, relationship_pulse, why_it_matters,
               sophie_directives, created_at, updated_at
        FROM living_context
        WHERE user_id=$1::text
        ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
        LIMIT 1
        """,
        user_id,
    )
    handover_row = await get_latest_fast_handover_packet(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        include_expired=True,
        as_of=now,
    )

    current_state_notes: List[Dict[str, Any]] = []
    provisional_state_notes: List[Dict[str, Any]] = []
    stale_state_warnings: List[Dict[str, Any]] = []
    historical_state_notes: List[Dict[str, Any]] = []
    suppressions: List[str] = []
    tone_modifiers: List[str] = []
    unsafe_to_surface: List[Dict[str, Any]] = []
    evidence_refs: List[Dict[str, Any]] = []
    gaps: List[str] = []
    freshness_counts: Dict[str, int] = {"current": 0, "provisional": 0, "stale": 0, "historical": 0}
    confidence_values: List[float] = []

    for row in timeline_event_rows:
        domain = _normalize_text(row.get("domain")).lower()
        event_type = _normalize_text(row.get("event_type")).lower()
        if domain != "state" and event_type not in STATE_EVENT_TYPES and not event_type.startswith("state_"):
            continue
        occurred_at = _ensure_utc(row.get("occurred_at")) or _ensure_utc(row.get("observed_at")) or _ensure_utc(row.get("created_at"))
        if not occurred_at:
            gaps.append("state_event_missing_time")
            continue
        age = now - occurred_at
        metadata = dict(row.get("metadata") or {}) if isinstance(row.get("metadata"), dict) else {}
        single_session = bool(metadata.get("single_session", False))
        sensitive = _note_is_sensitive(f"{row.get('title') or ''} {row.get('summary') or ''}")
        reinforced = bool(_normalize_text(row.get("status")).lower() == "confirmed")
        freshness, note_gaps = _classify_state_age(age=age, reinforced=reinforced, single_session=single_session, sensitive=sensitive)
        if _normalize_text(row.get("effect")).lower() == "suppress_related" or _normalize_text(row.get("status")).lower() == "suppressed":
            freshness = "stale"
            note_gaps.append("explicit_state_suppression")
        note = _build_state_note(
            source_table="timeline_events",
            source_id=_normalize_text(row.get("event_id")),
            title=row.get("title"),
            summary=row.get("summary"),
            observed_at=row.get("observed_at"),
            occurred_at=occurred_at,
            freshness=freshness,
            status=_normalize_text(row.get("status")),
            confidence=_safe_float(row.get("confidence")),
            evidence_refs=row.get("evidence_refs"),
            gaps=note_gaps,
            metadata=metadata,
        )
        freshness_counts[freshness] = freshness_counts.get(freshness, 0) + 1
        if note["confidence"] is not None:
            confidence_values.append(note["confidence"])
        evidence_refs.extend(ref for ref in note["evidence_refs"] if ref not in evidence_refs)
        if freshness == "current":
            current_state_notes.append(note)
        elif freshness == "provisional":
            provisional_state_notes.append(note)
        elif freshness == "stale":
            stale_state_warnings.append(note)
        elif include_historical:
            historical_state_notes.append(note)
        if sensitive:
            _append_unique(tone_modifiers, "Use sensitive state only as tone modifier or reactive context.")
            unsafe_to_surface.append(note)

    for row in session_change_rows:
        kind = _normalize_text(row.get("kind")).lower()
        if "state" not in kind:
            continue
        occurred_at = _ensure_utc(row.get("effective_iso")) or _ensure_utc(row.get("updated_at")) or _ensure_utc(row.get("created_at"))
        if not occurred_at:
            gaps.append("session_change_missing_time")
            continue
        age = now - occurred_at
        freshness, note_gaps = _classify_state_age(age=age, reinforced=False, single_session=True, sensitive=_note_is_sensitive(_normalize_text(row.get("summary"))))
        provenance = row.get("provenance") if isinstance(row.get("provenance"), dict) else {}
        note = _build_state_note(
            source_table="session_changes",
            source_id=_normalize_text(row.get("change_id")),
            title=row.get("title"),
            summary=row.get("summary"),
            observed_at=row.get("updated_at"),
            occurred_at=occurred_at,
            freshness=freshness,
            status=_normalize_text(row.get("status")) or "confirmed",
            confidence=_safe_float(row.get("confidence_score")),
            evidence_refs=provenance.get("source_turn_refs") or provenance.get("evidence_refs"),
            gaps=note_gaps,
        )
        freshness_counts[freshness] = freshness_counts.get(freshness, 0) + 1
        if note["confidence"] is not None:
            confidence_values.append(note["confidence"])
        evidence_refs.extend(ref for ref in note["evidence_refs"] if ref not in evidence_refs)
        if freshness == "current":
            provisional_state_notes.append(note)
        elif freshness == "provisional":
            provisional_state_notes.append(note)
        elif freshness == "stale":
            stale_state_warnings.append(note)
        elif include_historical:
            historical_state_notes.append(note)

    if living_context_row:
        living_context = dict(living_context_row)
        summary_bits = [
            _normalize_text(living_context.get("emotional_texture")),
            _normalize_text(living_context.get("primary_tension")),
        ]
        snapshot_text = ". ".join(bit for bit in summary_bits if bit)
        if snapshot_text:
            note = _build_state_note(
                source_table="living_context",
                source_id="latest",
                title="Living context snapshot",
                summary=snapshot_text,
                observed_at=living_context.get("updated_at") or living_context.get("created_at"),
                occurred_at=living_context.get("updated_at") or living_context.get("created_at"),
                freshness="provisional",
                status="provisional",
                confidence=None,
                evidence_refs=[],
                gaps=["living_context_is_snapshot_not_guaranteed_truth"],
                metadata={"snapshot": True},
            )
            provisional_state_notes.append(note)
            freshness_counts["provisional"] = freshness_counts.get("provisional", 0) + 1
            gaps.append("living_context_requires_revalidation")
            if _note_is_sensitive(snapshot_text):
                _append_unique(tone_modifiers, "Living context emotional texture should stay provisional.")
                unsafe_to_surface.append(note)

    if handover_row and _normalize_text(handover_row.get("recent_state_note")):
        note = _build_state_note(
            source_table="session_handover_packets",
            source_id=_normalize_text(handover_row.get("packet_id")),
            title="Fast handover recent state",
            summary=handover_row.get("recent_state_note"),
            observed_at=handover_row.get("created_at"),
            occurred_at=handover_row.get("created_at"),
            freshness="provisional",
            status="provisional",
            confidence=None,
            evidence_refs=handover_row.get("source_turn_refs"),
            gaps=["handover_recent_state_note_is_short_lived"],
            metadata={"short_lived": True},
        )
        provisional_state_notes.append(note)
        freshness_counts["provisional"] = freshness_counts.get("provisional", 0) + 1
        _append_unique(tone_modifiers, "Fast handover recent state should not become durable identity.")

    if stale_state_warnings:
        _append_unique(suppressions, "Do not present stale emotional state as current.")
    if not evidence_refs:
        gaps.append("no_state_evidence_refs_available")
    if not current_state_notes and not provisional_state_notes and not stale_state_warnings and not historical_state_notes:
        gaps.append("no_state_signals_found")

    current_state_notes = current_state_notes[:safe_limit]
    provisional_state_notes = provisional_state_notes[:safe_limit]
    stale_state_warnings = stale_state_warnings[:safe_limit]
    historical_state_notes = historical_state_notes[:safe_limit]
    unsafe_to_surface = unsafe_to_surface[:safe_limit]
    confidence = round(sum(confidence_values) / len(confidence_values), 3) if confidence_values else None

    return {
        "tenantId": tenant_id,
        "userId": user_id,
        "companionId": _normalize_text(companion_id) or DEFAULT_COMPANION_ID,
        "asOf": now.isoformat(),
        "current_state_notes": current_state_notes,
        "provisional_state_notes": provisional_state_notes,
        "stale_state_warnings": stale_state_warnings,
        "historical_state_notes": historical_state_notes,
        "suppressions": suppressions,
        "tone_modifiers": tone_modifiers,
        "unsafe_to_surface": unsafe_to_surface,
        "evidence_refs": evidence_refs[: safe_limit * 2],
        "gaps": sorted(set(filter(None, gaps))),
        "freshness": freshness_counts,
        "confidence": confidence,
        "readOnly": True,
    }
