from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone as dt_timezone
from typing import Any, Dict, List, Optional


FAST_HANDOVER_TTL_HOURS = 18
STATE_NOTE_FRESHNESS_HOURS = 6
MAX_LIST_ITEMS = 4
STATE_TERMS = (
    "tired",
    "exhausted",
    "overwhelmed",
    "stressed",
    "anxious",
    "unwell",
    "in pain",
    "low energy",
    "fried",
    "drained",
    "confused",
    "sick",
)
PERSON_STOPWORDS = {
    "I",
    "The",
    "And",
    "But",
    "We",
    "It",
    "This",
    "That",
    "Today",
    "Tomorrow",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
}


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _parse_ts(value: Any) -> Optional[datetime]:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=dt_timezone.utc)
    return dt


def _shorten(text: str, limit: int = 180) -> str:
    clean = re.sub(r"\s+", " ", _normalize_text(text)).strip()
    if len(clean) <= limit:
        return clean
    return clean[: max(0, limit - 1)].rstrip() + "…"


def _clean_sentence(value: str) -> str:
    text = _shorten(value, 180)
    return text.rstrip(" .")


def _dedupe_keep_order(items: List[str], *, limit: int = MAX_LIST_ITEMS) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for item in items:
        clean = _clean_sentence(item)
        key = clean.lower()
        if not clean or key in seen:
            continue
        seen.add(key)
        out.append(clean)
        if len(out) >= limit:
            break
    return out


def _split_sentences(text: str) -> List[str]:
    clean = _normalize_text(text)
    if not clean:
        return []
    parts = re.split(r"(?<=[.!?])\s+", clean)
    return [part.strip() for part in parts if part.strip()]


def _extract_open_questions(messages: List[Dict[str, Any]]) -> List[str]:
    found: List[str] = []
    for msg in messages:
        text = _normalize_text(msg.get("text"))
        for sentence in _split_sentences(text):
            if sentence.endswith("?"):
                found.append(sentence)
    return _dedupe_keep_order(found)


def _extract_unresolved_decisions(messages: List[Dict[str, Any]]) -> List[str]:
    patterns = (
        r"\b(?:should i|should we|whether to|decide(?:d)? to|not sure if|unsure whether|i don't know if|choose between)\b",
        r"\b(?:cancel|reschedule|send|ship|follow up|follow-up)\b.*\?",
    )
    found: List[str] = []
    for msg in messages:
        text = _normalize_text(msg.get("text"))
        lowered = text.lower()
        if any(re.search(pattern, lowered) for pattern in patterns):
            found.extend(_split_sentences(text))
    return _dedupe_keep_order(found)


def _extract_pending_actions(messages: List[Dict[str, Any]]) -> List[str]:
    found: List[str] = []
    action_patterns = [
        re.compile(r"\b(?:i(?:\s+\w+){0,2}\s+need to|i should|i have to|i will|remind me to)\s+([^.!?]+)", re.IGNORECASE),
        re.compile(r"\b(?:follow up with|follow up on|follow-up with|follow-up on|call|email|send|draft|prepare|book|schedule|check on|nudge)\s+([^.!?]+)", re.IGNORECASE),
    ]
    for msg in messages:
        text = _normalize_text(msg.get("text"))
        for pattern in action_patterns:
            for match in pattern.finditer(text):
                phrase = match.group(0)
                if phrase:
                    found.append(phrase)
    return _dedupe_keep_order(found)


def _extract_recent_state_note(messages: List[Dict[str, Any]], *, created_at: datetime) -> Optional[str]:
    for msg in reversed(messages):
        if _normalize_text(msg.get("role")).lower() != "user":
            continue
        ts = _parse_ts(msg.get("timestamp")) or created_at
        if created_at - ts > timedelta(hours=STATE_NOTE_FRESHNESS_HOURS):
            continue
        text = _normalize_text(msg.get("text"))
        lowered = text.lower()
        if any(term in lowered for term in STATE_TERMS):
            return _shorten(text, 160)
    return None


def _extract_important_people(messages: List[Dict[str, Any]]) -> List[str]:
    names: List[str] = []
    pattern = re.compile(r"\b(?:Dr\.\s+[A-Z][a-z]+|[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b")
    for msg in messages:
        text = _normalize_text(msg.get("text"))
        for match in pattern.findall(text):
            token = _normalize_text(match)
            if token in PERSON_STOPWORDS:
                continue
            names.append(token)
    return _dedupe_keep_order(names)


def _extract_active_topics(
    messages: List[Dict[str, Any]],
    *,
    pending_actions: List[str],
    unresolved_decisions: List[str],
    important_people: List[str],
) -> List[str]:
    topics: List[str] = []
    topic_patterns = (
        r"(invoice\s*#?\d+)",
        r"(appointment[^.!?]*)",
        r"(meeting[^.!?]*)",
        r"(event takedown)",
        r"(parser bug)",
        r"(release[^.!?]*)",
    )
    for msg in messages:
        text = _normalize_text(msg.get("text"))
        lowered = text.lower()
        for pattern in topic_patterns:
            for match in re.findall(pattern, lowered, flags=re.IGNORECASE):
                topics.append(match)
    topics.extend(pending_actions[:2])
    topics.extend(unresolved_decisions[:2])
    topics.extend(important_people[:2])
    return _dedupe_keep_order(topics)


def _build_summary(
    *,
    active_topics: List[str],
    open_questions: List[str],
    unresolved_decisions: List[str],
    pending_actions: List[str],
    recent_state_note: Optional[str],
) -> str:
    parts: List[str] = []
    if active_topics:
        parts.append(f"Recent session focused on {', '.join(active_topics[:2])}.")
    if pending_actions:
        parts.append(f"Obvious next action: {pending_actions[0]}.")
    if open_questions:
        parts.append(f"Open question: {open_questions[0]}.")
    elif unresolved_decisions:
        parts.append(f"Pending decision: {unresolved_decisions[0]}.")
    if recent_state_note:
        parts.append("Recent state may be relevant, but should be re-checked before assuming it still holds.")
    summary = " ".join(parts).strip()
    return summary or "Recent session ended with light continuity cues but no strong durable handover."


def build_fast_handover_packet(
    *,
    tenant_id: str,
    user_id: str,
    session_id: str,
    messages: List[Dict[str, Any]],
    created_at: Optional[datetime] = None,
    ttl_hours: int = FAST_HANDOVER_TTL_HOURS,
) -> Optional[Dict[str, Any]]:
    normalized_messages = [dict(msg) for msg in (messages or []) if isinstance(msg, dict) and _normalize_text(msg.get("text"))]
    if not normalized_messages:
        return None
    packet_created_at = created_at or max(
        [_parse_ts(msg.get("timestamp")) for msg in normalized_messages if _parse_ts(msg.get("timestamp"))] or [datetime.now(dt_timezone.utc)]
    )
    if packet_created_at.tzinfo is None:
        packet_created_at = packet_created_at.replace(tzinfo=dt_timezone.utc)
    expires_at = packet_created_at + timedelta(hours=max(1, int(ttl_hours or FAST_HANDOVER_TTL_HOURS)))
    open_questions = _extract_open_questions(normalized_messages)
    unresolved_decisions = _extract_unresolved_decisions(normalized_messages)
    pending_actions = _extract_pending_actions(normalized_messages)
    recent_state_note = _extract_recent_state_note(normalized_messages, created_at=packet_created_at)
    important_people = _extract_important_people(normalized_messages)
    active_topics = _extract_active_topics(
        normalized_messages,
        pending_actions=pending_actions,
        unresolved_decisions=unresolved_decisions,
        important_people=important_people,
    )
    caution_notes = [
        "Fast handover only; do not treat this as durable profile or identity.",
        "Confirm any recent state before acting on it.",
    ]
    if not recent_state_note:
        caution_notes = caution_notes[:1]
    source_turn_refs = [
        {
            "session_id": session_id,
            "turn_index": idx,
            "role": _normalize_text(msg.get("role")).lower() or None,
            "timestamp": _normalize_text(msg.get("timestamp")) or None,
            "text": _shorten(_normalize_text(msg.get("text")), 140),
        }
        for idx, msg in enumerate(normalized_messages[-6:])
    ]
    return {
        "tenant_id": tenant_id,
        "user_id": user_id,
        "session_id": session_id,
        "summary": _build_summary(
            active_topics=active_topics,
            open_questions=open_questions,
            unresolved_decisions=unresolved_decisions,
            pending_actions=pending_actions,
            recent_state_note=recent_state_note,
        ),
        "open_questions": open_questions,
        "unresolved_decisions": unresolved_decisions,
        "pending_actions": pending_actions,
        "recent_state_note": recent_state_note,
        "important_people": important_people,
        "active_topics": active_topics,
        "do_not_overdo": caution_notes,
        "created_at": packet_created_at,
        "expires_at": expires_at,
        "source_turn_refs": source_turn_refs,
        "status": "active" if expires_at > datetime.now(dt_timezone.utc) else "expired",
    }


async def persist_fast_handover_packet(
    db: Any,
    *,
    tenant_id: str,
    user_id: str,
    session_id: str,
    messages: List[Dict[str, Any]],
    created_at: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    packet = build_fast_handover_packet(
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=session_id,
        messages=messages,
        created_at=created_at,
    )
    if not packet:
        return None
    await db.execute(
        """
        INSERT INTO session_handover_packets (
            tenant_id, user_id, session_id, summary, open_questions, unresolved_decisions,
            pending_actions, recent_state_note, important_people, active_topics, do_not_overdo,
            created_at, expires_at, source_turn_refs, status
        )
        VALUES (
            $1, $2, $3, $4, $5::jsonb, $6::jsonb,
            $7::jsonb, $8, $9::jsonb, $10::jsonb, $11::jsonb,
            $12, $13, $14::jsonb, $15
        )
        ON CONFLICT (tenant_id, user_id, session_id)
        DO UPDATE SET
            summary = EXCLUDED.summary,
            open_questions = EXCLUDED.open_questions,
            unresolved_decisions = EXCLUDED.unresolved_decisions,
            pending_actions = EXCLUDED.pending_actions,
            recent_state_note = EXCLUDED.recent_state_note,
            important_people = EXCLUDED.important_people,
            active_topics = EXCLUDED.active_topics,
            do_not_overdo = EXCLUDED.do_not_overdo,
            created_at = EXCLUDED.created_at,
            expires_at = EXCLUDED.expires_at,
            source_turn_refs = EXCLUDED.source_turn_refs,
            status = EXCLUDED.status,
            updated_at = NOW()
        """,
        tenant_id,
        user_id,
        session_id,
        packet["summary"],
        packet["open_questions"],
        packet["unresolved_decisions"],
        packet["pending_actions"],
        packet["recent_state_note"],
        packet["important_people"],
        packet["active_topics"],
        packet["do_not_overdo"],
        packet["created_at"],
        packet["expires_at"],
        packet["source_turn_refs"],
        packet["status"],
    )
    return packet


async def get_latest_fast_handover_packet(
    db: Any,
    *,
    tenant_id: str,
    user_id: str,
    session_id: Optional[str] = None,
    include_expired: bool = False,
    as_of: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    now = as_of or datetime.now(dt_timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt_timezone.utc)
    where = ["tenant_id = $1::text", "user_id = $2::text"]
    args: List[Any] = [tenant_id, user_id]
    if _normalize_text(session_id):
        where.append(f"session_id = ${len(args) + 1}::text")
        args.append(session_id)
    if not include_expired:
        where.append(f"expires_at > ${len(args) + 1}::timestamptz")
        args.append(now)
    query = f"""
        SELECT
            tenant_id, user_id, session_id, summary, open_questions, unresolved_decisions,
            pending_actions, recent_state_note, important_people, active_topics, do_not_overdo,
            created_at, expires_at, source_turn_refs,
            CASE WHEN expires_at <= NOW() THEN 'expired' ELSE status END AS status,
            updated_at
        FROM session_handover_packets
        WHERE {' AND '.join(where)}
        ORDER BY created_at DESC, updated_at DESC
        LIMIT 1
        """
    if hasattr(db, "fetchone"):
        row = await db.fetchone(query, *args)
    else:
        row = await db.fetchrow(query, *args)
        row = dict(row) if row else None
    if not row:
        return None
    return dict(row)
