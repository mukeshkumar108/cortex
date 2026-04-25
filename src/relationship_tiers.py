from __future__ import annotations

import re
from typing import Any, Dict, Iterable, Optional

from .canonicalization import normalize_text

TIER_1_CORE = "tier_1_core"
TIER_2_MEANINGFUL = "tier_2_meaningful"
TIER_3_CONTEXTUAL = "tier_3_contextual"
TIER_4_INCIDENTAL = "tier_4_incidental"

CORE_RELATIONSHIPS = {
    "daughter",
    "son",
    "child",
    "girlfriend",
    "boyfriend",
    "partner",
    "spouse",
    "mother",
    "father",
    "parent",
    "sister",
    "brother",
}

MEANINGFUL_RELATIONSHIPS = {
    "close_friend",
    "friend",
    "cofounder",
    "mentor",
    "collaborator",
    "colleague",
}

CONTEXT_PATTERNS = (
    r"\bacting as a bridge\b",
    r"\bcommunication bridge\b",
    r"\bkey point of contact\b",
    r"\bpartner'?s child\b",
    r"\bdaughter of\b",
    r"\bson of\b",
    r"\bmother of the user's\b",
    r"\bfather of the user's\b",
    r"\blittle girl\b",
    r"\bintermediary\b",
)


def _combined_entity_text(row: Dict[str, Any]) -> str:
    parts = [
        normalize_text(row.get("canonical_name"), casefold=False),
        normalize_text(row.get("profile_text"), casefold=False),
        normalize_text(row.get("last_known_status"), casefold=False),
    ]
    key_facts = row.get("key_facts")
    if isinstance(key_facts, list):
        for fact in key_facts:
            if isinstance(fact, dict):
                text = fact.get("fact") or fact.get("text") or fact.get("statement")
            else:
                text = fact
            clean = normalize_text(text, casefold=False)
            if clean:
                parts.append(clean)
    return " ".join(part for part in parts if part)


def classify_relationship_tier(
    relationship_to_user: Any,
    *,
    row: Optional[Dict[str, Any]] = None,
) -> str:
    relationship = normalize_text(relationship_to_user).lower().replace(" ", "_")
    if relationship in CORE_RELATIONSHIPS:
        return TIER_1_CORE
    if relationship in MEANINGFUL_RELATIONSHIPS:
        return TIER_2_MEANINGFUL
    if row:
        combined = _combined_entity_text(row).lower()
        if any(re.search(pattern, combined) for pattern in CONTEXT_PATTERNS):
            return TIER_3_CONTEXTUAL
    if relationship and relationship not in {"other", "assistant"}:
        return TIER_2_MEANINGFUL
    return TIER_4_INCIDENTAL


def relationship_tier_rank(tier: Optional[str]) -> int:
    return {
        TIER_1_CORE: 4,
        TIER_2_MEANINGFUL: 3,
        TIER_3_CONTEXTUAL: 2,
        TIER_4_INCIDENTAL: 1,
    }.get(normalize_text(tier), 0)


def entity_relationship_tier(row: Dict[str, Any]) -> str:
    return classify_relationship_tier(row.get("relationship_to_user"), row=row)


def entity_is_promoted_contextual(row: Dict[str, Any]) -> bool:
    tier = entity_relationship_tier(row)
    if tier != TIER_3_CONTEXTUAL:
        return False
    relationship = normalize_text(row.get("relationship_to_user")).lower().replace(" ", "_")
    if relationship in CORE_RELATIONSHIPS | MEANINGFUL_RELATIONSHIPS:
        return True
    try:
        distinct_sessions = int(row.get("distinct_session_count") or 0)
    except Exception:
        distinct_sessions = 0
    try:
        importance = float(row.get("importance_score") or 0.0)
    except Exception:
        importance = 0.0
    try:
        salience = float(row.get("salience_score") or 0.0)
    except Exception:
        salience = 0.0
    return distinct_sessions >= 6 and (importance >= 0.9 or salience >= 0.92)


def always_on_people_allowed(row: Dict[str, Any]) -> bool:
    tier = entity_relationship_tier(row)
    if tier == TIER_1_CORE:
        return True
    if tier == TIER_2_MEANINGFUL:
        return True
    if tier == TIER_3_CONTEXTUAL:
        return entity_is_promoted_contextual(row)
    return False


def pass4_anchor_allowed(row: Dict[str, Any]) -> bool:
    tier = entity_relationship_tier(row)
    if tier in {TIER_1_CORE, TIER_2_MEANINGFUL}:
        return True
    if tier == TIER_3_CONTEXTUAL:
        return entity_is_promoted_contextual(row)
    return False
