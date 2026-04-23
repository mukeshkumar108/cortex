"""
Async hardening helpers for the Gemma/Postgres derived synthesis pipeline.

This module deliberately keeps the six-pass pipeline as the serving synthesis
layer. Canonical v2 remains separate governance/audit infrastructure.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .canonicalization import normalize_text, stable_short_hash
from .config import Settings, get_settings
from .db import Database
from .openrouter_client import get_llm_client
from .derived_passes.pass1_triage import run_rich_pass1_llm
from .derived_passes.pass15_entities import build_entity_profile, resolve_entity_mentions
from .derived_passes.pass3_threads import audit_thread_registry, extract_thread_actions, VALID_CATEGORIES, VALID_PRIORITIES
from .derived_passes.pass4_identity import normalize_identity_output, synthesize_identity_profile
from .derived_passes.pass5_living_context import normalize_living_context_output, synthesize_living_context
from .derived_passes.synthesis_quality import conservative_rewrite_text, has_synthesis_quality_issue

logger = logging.getLogger(__name__)

PASS1_TRIAGE = "pass1_triage"
PASS1_5_ENTITIES = "pass1_5_entities"
PASS3_THREADS = "pass3_threads"
PASS4_IDENTITY = "pass4_identity"
PASS5_LIVING_CONTEXT = "pass5_living_context"

QUARANTINE_PARSE_FAILURE = "parse_failure"
QUARANTINE_MISSING_EVIDENCE_REFS = "missing_evidence_refs"
QUARANTINE_INVALID_LIFECYCLE_TRANSITION = "invalid_lifecycle_transition"

HIGH_SIGNAL_SURFACES = {
    "memory_delta",
    "identity_signal",
    "thread_signal",
    "thread_update",
    "identity_trait",
    "living_context_statement",
}

EXCLUSIVE_SLOTS = {
    "relationship.status",
    "user.location",
    "preference.durable_correction",
}

SILENCE_ENTITY_THRESHOLD_DAYS = 30
SILENCE_THREAD_THRESHOLD_DAYS = 30

SILENCE_IMPORTANT_RELATIONSHIPS = {
    "partner",
    "girlfriend",
    "boyfriend",
    "spouse",
    "daughter",
    "son",
    "child",
    "father",
    "mother",
    "parent",
    "sister",
    "brother",
    "family",
    "friend",
    "close_friend",
}
SILENCE_GENERIC_SYSTEM_TERMS = {
    "repository",
    "repo",
    "runtime",
    "api",
    "database",
    "postgres",
    "engine",
    "framework",
    "server",
    "schema",
}
SILENCE_USER_CENTRAL_PROJECT_TERMS = {
    "user-central",
    "user central",
    "user_project",
    "owned_project",
    "core project",
    "primary project",
    "primary_work",
    "main project",
    "user's project",
    "user project",
    "building",
    "founded",
    "owned",
}

RESERVED_ASSISTANT_ENTITY_NAMES = {
    "sophie",
    "sophie ai",
    "sophie assistant",
}

SYSTEM_ENTITY_TYPES = {
    "assistant",
    "system",
    "tool",
    "technology",
    "repo",
    "repository",
    "other",
}

RELATIONSHIP_RESOLUTION_TERMS = {
    "stable",
    "steady",
    "good terms",
    "in a good place",
    "reconnected",
    "reconnecting",
    "reconcile",
    "reconciled",
    "repair happened",
    "partner again",
    "together again",
    "dating again",
}

RELATIONSHIP_CONFLICT_TERMS = {
    "unresolved",
    "tension",
    "strained",
    "distance",
    "silent",
    "silence",
    "repair",
    "breakup",
    "broke up",
    "conflict",
    "unclear",
    "not talking",
    "no contact",
}

PROJECT_ENTITY_RELATIONSHIPS = {
    "active_project",
    "user_project",
    "owned_project",
    "core_project",
    "primary_project",
}

STRONG_RELATIONSHIP_RANK = {
    "daughter": 100,
    "son": 100,
    "child": 100,
    "girlfriend": 95,
    "boyfriend": 95,
    "partner": 95,
    "spouse": 95,
    "mother": 90,
    "father": 90,
    "parent": 90,
    "sister": 85,
    "brother": 85,
    "close_friend": 65,
    "friend": 50,
    "colleague": 40,
    "active_project": 35,
    "assistant": 30,
    "other": 10,
}


@dataclass(frozen=True)
class DerivedPassResult:
    run_id: int
    pass_name: str
    output_hash: str
    run_entity_pass: bool = False
    run_threads_pass: bool = False
    should_run_identity: bool = False
    should_run_living_context: bool = False


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
        return [value] if value.strip() else []
    return [value]


def _text_list(value: Any, *, limit: int = 12) -> List[str]:
    out: List[str] = []
    for item in _as_list(value):
        if isinstance(item, dict):
            text = item.get("text") or item.get("name") or item.get("statement") or item.get("delta")
        else:
            text = item
        normalized = normalize_text(text, casefold=False)
        if normalized and normalized not in out:
            out.append(normalized)
        if len(out) >= limit:
            break
    return out


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return normalize_text(value) in {"true", "yes", "1", "y"}


def _messages_text(messages: Sequence[Dict[str, Any]], *, user_only: bool = True) -> str:
    lines: List[str] = []
    for msg in messages:
        role = normalize_text(msg.get("role"))
        if user_only and role != "user":
            continue
        text = normalize_text(msg.get("text") or msg.get("content"), casefold=False)
        if text:
            lines.append(f"{role or 'unknown'}: {text}")
    return "\n".join(lines)


def _source_turn_refs(
    *,
    session_id: str,
    messages: Sequence[Dict[str, Any]],
    text_hint: Optional[str] = None,
    max_refs: int = 8,
) -> List[Dict[str, Any]]:
    hint = normalize_text(text_hint or "")
    refs: List[Dict[str, Any]] = []
    for idx, msg in enumerate(messages):
        role = normalize_text(msg.get("role"))
        if role != "user":
            continue
        text = normalize_text(msg.get("text") or msg.get("content"), casefold=False)
        if not text:
            continue
        if hint:
            hint_tokens = {t for t in re.findall(r"[a-z0-9']{4,}", hint)[:8]}
            text_norm = normalize_text(text)
            if hint_tokens and not any(token in text_norm for token in hint_tokens):
                continue
        refs.append(
            {
                "session_id": session_id,
                "turn_index": idx,
                "role": role,
                "timestamp": msg.get("timestamp"),
                "text": text[:500],
            }
        )
        if len(refs) >= max_refs:
            break
    if not refs and not hint:
        for idx, msg in enumerate(messages):
            role = normalize_text(msg.get("role"))
            text = normalize_text(msg.get("text") or msg.get("content"), casefold=False)
            if role == "user" and text:
                refs.append(
                    {
                        "session_id": session_id,
                        "turn_index": idx,
                        "role": role,
                        "timestamp": msg.get("timestamp"),
                        "text": text[:500],
                    }
                )
                if len(refs) >= max_refs:
                    break
    return refs


def _semantic_category(statement: str, surface: str) -> str:
    text = normalize_text(f"{surface} {statement}")
    if any(word in text for word in ("daughter", "girlfriend", "partner", "mother", "father", "family", "relationship")):
        return "relationship"
    if any(word in text for word in ("health", "hospital", "pain", "kidney", "sleep", "hydration", "walk", "walking")):
        return "health"
    if any(word in text for word in ("goal", "want", "trying to", "building", "project", "persistent")):
        return "persistent_goal"
    if surface.startswith("identity"):
        return "core_identity"
    if "preference" in text or "like" in text or "hate" in text:
        return "preference"
    return "session_observation"


def _memory_layer_and_floor(category: str) -> tuple[str, float]:
    if category in {"relationship", "health", "persistent_goal", "core_identity"}:
        return "LML", 0.65
    if category == "preference":
        return "LML", 0.45
    return "SML", 0.0


def _is_greeting_or_presence_check(text: Any) -> bool:
    value = normalize_text(text)
    if not value:
        return True
    compact = re.sub(r"[^a-z0-9 ]+", "", value).strip()
    if compact in {
        "hi",
        "hello",
        "hey",
        "hey sophie",
        "hi sophie",
        "hello sophie",
        "good morning",
        "good morning sophie",
        "good afternoon",
        "good afternoon sophie",
        "good evening",
        "good evening sophie",
        "are you there",
        "you there",
    }:
        return True
    return bool(re.fullmatch(r"(hey|hi|hello|good morning|good afternoon|good evening)[, ]+(sophie)?", compact))


def _meaningful_low_confidence_question(text: Any) -> bool:
    value = normalize_text(text)
    if not value:
        return False
    if any(term in value for term in ("where does", "where is", "what city", "what town", "what country", "how old", "birthday")):
        return False
    return any(
        term in value
        for term in (
            "relationship",
            "interpretation",
            "unclear",
            "ambiguous",
            "unconfirmed",
            "inferred",
            "intent",
            "contradiction",
            "tension",
            "family role",
            "behavioral inference",
            "emotional tone",
        )
    )


def _low_confidence_reason(statement: str, surface: str = "") -> Optional[str]:
    text = normalize_text(f"{surface} {statement}")
    if not text:
        return None
    uncertain = any(
        marker in text
        for marker in (
            "unclear",
            "not sure",
            "maybe",
            "possibly",
            "appears",
            "seems",
            "might",
            "unknown",
            "open question",
            "ambiguous",
            "unconfirmed",
            "inferred",
        )
    )
    relationship_terms = (
        "relationship",
        "partner",
        "girlfriend",
        "boyfriend",
        "dating",
        "breakup",
        "rupture",
        "close to",
        "tension with",
    )
    career_terms = (
        "career",
        "work",
        "job",
        "company",
        "business",
        "project",
        "client",
        "manager",
        "startup",
        "professional",
    )
    family_terms = (
        "family",
        "daughter",
        "son",
        "sister",
        "brother",
        "mother",
        "father",
        "dad",
        "mum",
        "cousin",
        "aunt",
        "uncle",
        "parent",
        "relative",
        "role",
    )
    behavior_terms = (
        "pattern",
        "avoid",
        "avoiding",
        "deflect",
        "freeze",
        "keeps",
        "kept",
        "tends to",
        "seems to",
        "appears to",
        "might be",
        "could be",
        "infer",
        "inferred",
    )
    tension_terms = ("tension", "stuck", "blocked", "afraid", "scared", "overwhelmed", "conflict", "pressure")
    if any(term in text for term in relationship_terms) and (uncertain or "interpretation" in text):
        return "uncertain_relationship_interpretation"
    if any(term in text for term in career_terms) and any(term in text for term in tension_terms) and (uncertain or "ambiguous" in text):
        return "ambiguous_career_tension"
    if any(term in text for term in family_terms) and (uncertain or "role unclear" in text or "relationship unclear" in text):
        return "unclear_family_role"
    if any(term in text for term in behavior_terms) and (uncertain or "inference" in text or "inferred" in text):
        return "partial_behavioral_inference"
    return None


def _low_confidence_question(reason_code: str, statement: str) -> str:
    if reason_code == "uncertain_relationship_interpretation":
        return f"Relationship interpretation is uncertain: {normalize_text(statement, casefold=False)}"
    if reason_code == "ambiguous_career_tension":
        return f"Career/work tension is ambiguous: {normalize_text(statement, casefold=False)}"
    if reason_code == "unclear_family_role":
        return f"Family role is unclear: {normalize_text(statement, casefold=False)}"
    if reason_code == "partial_behavioral_inference":
        return f"Behavioral inference is partial: {normalize_text(statement, casefold=False)}"
    return normalize_text(statement, casefold=False)


def _entity_is_user_central_project(row: Dict[str, Any]) -> bool:
    text = normalize_text(
        " ".join(
            str(row.get(key) or "")
            for key in ("canonical_name", "target_name", "relationship_to_user", "profile_text", "last_known_status")
        )
    )
    if not text:
        return False
    if any(term in text for term in SILENCE_USER_CENTRAL_PROJECT_TERMS):
        return True
    if any(term in normalize_text(row.get("target_name")) for term in SILENCE_GENERIC_SYSTEM_TERMS):
        return False
    return float(row.get("importance") or 0.0) >= 0.9 and float(row.get("salience") or 0.0) >= 0.85


def _is_reserved_assistant_entity(name: Any) -> bool:
    return normalize_text(name) in RESERVED_ASSISTANT_ENTITY_NAMES


def _relationship_rank(value: Any) -> int:
    return STRONG_RELATIONSHIP_RANK.get(normalize_text(value).replace(" ", "_"), 0)


def _resolution_strength(value: Any, *, default: str = "medium") -> str:
    strength = normalize_text(value)
    if strength in {"weak", "medium", "strong"}:
        return strength
    return default


def _resolution_relevance(value: Any, *, default: str = "medium") -> str:
    relevance = normalize_text(value)
    if relevance in {"low", "medium", "high"}:
        return relevance
    return default


def _confidence_float(value: Any, *, default: float = 0.45) -> float:
    try:
        return max(0.0, min(1.0, float(value)))
    except Exception:
        return default


def _resolution_profile_allowed(
    *,
    canonical: str,
    entity_type: str,
    relationship_to_user: Optional[str],
    status: str,
    evidence_strength: str,
    memory_relevance: str,
    relationship_confidence: float,
    confidence: float,
    existing_profile_text: Optional[str],
    entity_messages: Sequence[Dict[str, Any]],
    source_turn_refs: Sequence[Dict[str, Any]],
) -> bool:
    if _is_reserved_assistant_entity(canonical):
        return False
    if normalize_text(entity_type) in {"assistant", "system", "tool", "technology", "repo", "repository"}:
        return False
    if normalize_text(status) != "active":
        return False
    if evidence_strength == "weak" or memory_relevance == "low":
        return False
    if not source_turn_refs:
        return False
    if not entity_messages:
        return False
    if normalize_text(existing_profile_text):
        return True
    if _relationship_rank(relationship_to_user) >= 85 and relationship_confidence >= 0.55:
        return True
    if normalize_text(entity_type) == "project" and memory_relevance == "high":
        return True
    return confidence >= 0.75 and memory_relevance in {"medium", "high"}


def _has_entity_serving_content(row: Dict[str, Any]) -> bool:
    profile = normalize_text(row.get("profile_text") or row.get("profile_snippet"))
    if profile:
        return True
    key_facts = row.get("key_facts")
    if isinstance(key_facts, list) and any(bool(item) for item in key_facts):
        return True
    open_questions = row.get("open_questions")
    if isinstance(open_questions, list) and any(normalize_text(item) for item in open_questions):
        return True
    if _relationship_rank(row.get("relationship_to_user")) >= 85:
        return True
    return False


def _infer_relationship_from_messages(canonical: str, messages: Sequence[Dict[str, Any]]) -> Optional[str]:
    name = normalize_text(canonical)
    if not name:
        return None
    user_lines: List[str] = []
    for msg in messages or []:
        if normalize_text(msg.get("role")) != "user":
            continue
        text = normalize_text(msg.get("text") or msg.get("content"))
        if name and name in text:
            user_lines.append(text)
    text = "\n".join(user_lines)
    if not text:
        return None
    relationship_terms = [
        ("girlfriend", ("girlfriend", "partner", "my gf")),
        ("boyfriend", ("boyfriend", "my bf")),
        ("daughter", ("daughter", "my daughter", "my child")),
        ("son", ("son", "my son", "my child")),
        ("mother", ("mother", "mum", "mom")),
        ("father", ("father", "dad")),
        ("sister", ("sister",)),
        ("brother", ("brother",)),
    ]
    for label, terms in relationship_terms:
        if any(term in text for term in terms):
            return label
    return None


def _sanitize_entity_fields(
    *,
    canonical: str,
    entity_type: Optional[str],
    relationship_to_user: Optional[str],
    messages: Sequence[Dict[str, Any]],
    existing_relationship: Optional[str] = None,
) -> tuple[str, Optional[str]]:
    canonical_norm = _canonical_name_norm(canonical)
    proposed_type = normalize_text(entity_type) or _entity_type(canonical)
    proposed_relationship = normalize_text(relationship_to_user) or None

    if canonical_norm in RESERVED_ASSISTANT_ENTITY_NAMES:
        return "assistant", "assistant"

    inferred_relationship = _infer_relationship_from_messages(canonical, messages)
    if _relationship_rank(inferred_relationship) > _relationship_rank(proposed_relationship):
        proposed_relationship = inferred_relationship

    if _relationship_rank(existing_relationship) > _relationship_rank(proposed_relationship):
        proposed_relationship = normalize_text(existing_relationship)

    if (proposed_relationship or "").replace(" ", "_") in PROJECT_ENTITY_RELATIONSHIPS:
        proposed_type = "project"

    return proposed_type, proposed_relationship


def _is_entity_allowed_for_living_context(row: Dict[str, Any]) -> bool:
    name = row.get("canonical_name") or row.get("canonical_name_normalized")
    if _is_reserved_assistant_entity(name):
        return False
    entity_type = normalize_text(row.get("type"))
    relationship = normalize_text(row.get("relationship_to_user"))
    if relationship == "assistant":
        return False
    if entity_type in SYSTEM_ENTITY_TYPES:
        return _entity_is_user_central_project(
            {
                "target_name": name,
                "canonical_name": name,
                "relationship_to_user": relationship,
                "profile_text": row.get("profile_text") or row.get("profile_snippet"),
                "last_known_status": row.get("last_known_status"),
                "importance": row.get("importance_score") or 0.0,
                "salience": row.get("salience_score") or 0.0,
            }
        )
    return True


def _priority_rank(value: Any) -> int:
    priority = normalize_text(value)
    if priority == "high":
        return 3
    if priority == "medium":
        return 2
    if priority == "low":
        return 1
    return 0


def _thread_signal_rank(row: Dict[str, Any]) -> tuple[int, float, float, datetime]:
    category = normalize_text(row.get("category"))
    thread_type = normalize_text(row.get("thread_type"))
    relationship_bonus = 1 if category in {"relationship", "health", "assistant_feedback"} else 0
    durable_bonus = 1 if thread_type == "persistent_goal" else 0
    priority = _priority_rank(row.get("priority")) + relationship_bonus + durable_bonus
    salience = float(row.get("salience_score") or 0.0)
    importance = float(row.get("importance_score") or 0.0)
    last_seen = row.get("last_mentioned_at") if isinstance(row.get("last_mentioned_at"), datetime) else datetime.min.replace(tzinfo=timezone.utc)
    return (priority, salience, importance, last_seen)


def _thread_allowed_for_synthesis(row: Dict[str, Any]) -> bool:
    priority = normalize_text(row.get("priority"))
    category = normalize_text(row.get("category"))
    thread_type = normalize_text(row.get("thread_type"))
    salience = float(row.get("salience_score") or 0.0)
    if priority == "high":
        return True
    if thread_type == "persistent_goal":
        return True
    if category in {"relationship", "health", "assistant_feedback"} and salience >= 0.5:
        return True
    if salience > 0.5 and priority == "medium":
        return True
    return False


def _entity_signal_rank(row: Dict[str, Any]) -> tuple[int, float, float, datetime]:
    relationship = normalize_text(row.get("relationship_to_user"))
    role_rank = _relationship_rank(relationship)
    importance = float(row.get("importance_score") or 0.0)
    salience = float(row.get("salience_score") or 0.0)
    last_seen = row.get("last_seen_at") if isinstance(row.get("last_seen_at"), datetime) else datetime.min.replace(tzinfo=timezone.utc)
    profile = normalize_text(row.get("profile_text") or row.get("profile_snippet"))
    profile_bonus = 1 if len(profile) >= 40 else 0
    return (role_rank + profile_bonus, importance, salience, last_seen)


def _entity_allowed_for_synthesis(row: Dict[str, Any]) -> bool:
    if not _is_entity_allowed_for_living_context(row):
        return False
    if not _has_entity_serving_content(row):
        return False
    relationship = normalize_text(row.get("relationship_to_user"))
    profile = normalize_text(row.get("profile_text") or row.get("profile_snippet"))
    if _relationship_rank(relationship) >= 85:
        return True
    if len(profile) < 40:
        return False
    return float(row.get("salience_score") or 0.0) > 0.5 or float(row.get("importance_score") or 0.0) >= 0.65


def _rank_and_prune_threads(rows: Sequence[Dict[str, Any]], *, limit: int = 5) -> List[Dict[str, Any]]:
    candidates = [dict(row) for row in rows if _thread_allowed_for_synthesis(dict(row))]
    candidates.sort(key=_thread_signal_rank, reverse=True)
    return candidates[:limit]


def _rank_and_prune_entities(rows: Sequence[Dict[str, Any]], *, limit: int = 5) -> List[Dict[str, Any]]:
    candidates = [dict(row) for row in rows if _entity_allowed_for_synthesis(dict(row))]
    candidates.sort(key=_entity_signal_rank, reverse=True)
    return candidates[:limit]


def _rank_and_prune_sessions(rows: Sequence[Dict[str, Any]], *, limit: int = 20) -> List[Dict[str, Any]]:
    def score(row: Dict[str, Any]) -> tuple[int, int, datetime]:
        raw = row.get("raw_triage_output") if isinstance(row.get("raw_triage_output"), dict) else {}
        deltas = [x for x in _text_list(raw.get("memory_deltas"), limit=8) if not _is_greeting_or_presence_check(x)]
        threads = _text_list(raw.get("thread_signals"), limit=8)
        identity = _text_list(raw.get("identity_signals"), limit=8)
        tension = 1 if normalize_text(row.get("tension_signal")) else 0
        signal_count = len(deltas) + len(threads) + len(identity) + tension
        role_bonus = 1 if any(_semantic_category(x, "session") in {"relationship", "health", "persistent_goal"} for x in [*deltas, *threads, *identity]) else 0
        ts = row.get("session_date") if isinstance(row.get("session_date"), datetime) else datetime.min.replace(tzinfo=timezone.utc)
        return (role_bonus, signal_count, ts)

    candidates = [dict(row) for row in rows if score(dict(row))[1] > 0]
    candidates.sort(key=score, reverse=True)
    selected = candidates[:limit]
    selected.sort(key=lambda row: row.get("session_date") if isinstance(row.get("session_date"), datetime) else datetime.min.replace(tzinfo=timezone.utc))
    return selected


async def _thread_entity_mismatch(
    *,
    db: Database,
    user_id: str,
    title: Any,
    detail: Any,
    evidence_turn_refs: Any = None,
    related_entities: Any = None,
) -> Optional[Dict[str, Any]]:
    title_text = normalize_text(title).casefold()
    detail_text = normalize_text(detail).casefold()
    if not title_text or not detail_text:
        return None
    entities = await db.fetch(
        """
        SELECT canonical_name, relationship_to_user, type
        FROM entity_profiles
        WHERE user_id=$1
          AND status='active'
          AND type='person'
          AND COALESCE(canonical_name, '') <> ''
          AND replace(lower(COALESCE(relationship_to_user, '')), ' ', '_') NOT IN (
            'assistant','system','active_project','user_project','owned_project','core_project','primary_project'
          )
        """,
        user_id,
    )
    title_entities: set[str] = set()
    detail_entities: set[str] = set()
    related_entity_names = {
        normalize_text(item).casefold()
        for item in (related_entities if isinstance(related_entities, list) else [])
        if normalize_text(item)
    }
    for entity in entities:
        name = normalize_text(entity.get("canonical_name"))
        if len(name) < 3:
            continue
        needle = name.casefold()
        if needle in title_text or needle in related_entity_names:
            title_entities.add(name)
        if needle in detail_text:
            detail_entities.add(name)
    if title_entities and detail_entities and title_entities.isdisjoint(detail_entities):
        evidence_text = " ".join(
            normalize_text(ref.get("text")).casefold()
            for ref in (evidence_turn_refs if isinstance(evidence_turn_refs, list) else [])
            if isinstance(ref, dict)
        )
        explicit_detail_entities = {
            name for name in detail_entities if name.casefold() in evidence_text
        }
        if not explicit_detail_entities:
            return None
        return {
            "reason": "thread_title_detail_entity_mismatch",
            "title_entities": sorted(title_entities),
            "detail_entities": sorted(explicit_detail_entities),
        }
    return None


async def _strip_inferred_thread_detail_entities(
    *,
    db: Database,
    user_id: str,
    title: Any,
    detail: Any,
    evidence_turn_refs: Any = None,
    related_entities: Any = None,
) -> str:
    text = normalize_text(detail)
    title_text = normalize_text(title).casefold()
    if not text:
        return text
    evidence_text = " ".join(
        normalize_text(ref.get("text")).casefold()
        for ref in (evidence_turn_refs if isinstance(evidence_turn_refs, list) else [])
        if isinstance(ref, dict)
    )
    related_entity_names = {
        normalize_text(item).casefold()
        for item in (related_entities if isinstance(related_entities, list) else [])
        if normalize_text(item)
    }
    entities = await db.fetch(
        """
        SELECT canonical_name, relationship_to_user, type
        FROM entity_profiles
        WHERE user_id=$1
          AND status='active'
          AND type='person'
          AND COALESCE(canonical_name, '') <> ''
          AND replace(lower(COALESCE(relationship_to_user, '')), ' ', '_') NOT IN (
            'assistant','system','active_project','user_project','owned_project','core_project','primary_project'
          )
        """,
        user_id,
    )
    title_entities: set[str] = set()
    inferred_detail_entities: set[str] = set()
    for entity in entities:
        name = normalize_text(entity.get("canonical_name"))
        if len(name) < 3:
            continue
        needle = name.casefold()
        in_title_or_related = needle in title_text or needle in related_entity_names
        in_detail = needle in text.casefold()
        if in_title_or_related:
            title_entities.add(name)
        elif in_detail and (needle not in evidence_text or related_entity_names):
            inferred_detail_entities.add(name)
    if not title_entities or not inferred_detail_entities:
        return text
    cleaned = text
    for name in inferred_detail_entities:
        cleaned = re.sub(rf"\bwith\s+{re.escape(name)}\b", "with them", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(rf"\bto\s+{re.escape(name)}\b", "to them", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(rf"\b{re.escape(name)}'s\b", "their", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(rf"\b{re.escape(name)}\b", "them", cleaned, flags=re.IGNORECASE)
    return normalize_text(cleaned)


def _entity_silence_eligible(row: Dict[str, Any]) -> bool:
    name = normalize_text(row.get("target_name"))
    entity_type = normalize_text(row.get("entity_type"))
    relationship = normalize_text(row.get("relationship_to_user")).replace(" ", "_")
    text = normalize_text(
        " ".join(
            str(row.get(key) or "")
            for key in ("target_name", "relationship_to_user", "profile_text", "last_known_status")
        )
    )
    is_generic_system = entity_type in {"tool", "system", "repo", "repository", "technology"} or any(
        term in name for term in SILENCE_GENERIC_SYSTEM_TERMS
    )
    if is_generic_system and not _entity_is_user_central_project(row):
        return False
    if entity_type == "person":
        return True
    if relationship in SILENCE_IMPORTANT_RELATIONSHIPS:
        return True
    if any(term in text for term in ("daughter", "son", "partner", "girlfriend", "father", "mother", "family", "close friend")):
        return True
    if entity_type == "project" and _entity_is_user_central_project(row):
        return True
    return False


def _thread_silence_eligible(row: Dict[str, Any]) -> bool:
    category = normalize_text(row.get("category"))
    thread_type = normalize_text(row.get("thread_type"))
    priority = normalize_text(row.get("priority"))
    text = normalize_text(f"{row.get('target_name') or ''} {row.get('detail') or ''}")
    if category in {"relationship", "family", "health"}:
        return True
    if category == "goal" and (thread_type == "persistent_goal" or priority == "high" or float(row.get("importance") or 0.0) >= 0.75):
        return True
    if category == "project" and (
        any(term in text for term in SILENCE_USER_CENTRAL_PROJECT_TERMS)
        or priority == "high"
        or float(row.get("importance") or 0.0) >= 0.85
    ):
        return True
    return False


def _input_hash(
    *,
    tenant_id: str,
    user_id: str,
    session_id: Optional[str],
    pass_name: str,
    messages: Optional[Sequence[Dict[str, Any]]] = None,
    extra: Optional[Dict[str, Any]] = None,
    settings: Optional[Settings] = None,
) -> str:
    s = settings or get_settings()
    return stable_short_hash(
        {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "session_id": session_id,
            "pass_name": pass_name,
            "messages": messages or [],
            "extra": extra or {},
            "model_version": s.derived_pipeline_model_version,
            "prompt_version": s.derived_pipeline_prompt_version,
            "policy_version": s.derived_pipeline_policy_version,
        },
        length=24,
    )


async def _start_run(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    session_id: Optional[str],
    pass_name: str,
    input_hash: str,
    input_watermark: Optional[str] = None,
    settings: Optional[Settings] = None,
) -> int:
    s = settings or get_settings()
    await db.execute(
        """
        INSERT INTO pipeline_runs (
            tenant_id, user_id, session_id, pass_name,
            model_version, prompt_version, policy_version,
            input_watermark, input_hash, status, attempt_count, started_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'running',1,NOW())
        ON CONFLICT (
            tenant_id, user_id, pass_name, input_hash, model_version, prompt_version, COALESCE(policy_version, '')
        )
        DO UPDATE SET
            status = CASE
                WHEN pipeline_runs.status = 'succeeded' THEN pipeline_runs.status
                ELSE 'running'
            END,
            attempt_count = CASE
                WHEN pipeline_runs.status = 'succeeded' THEN pipeline_runs.attempt_count
                ELSE pipeline_runs.attempt_count + 1
            END,
            started_at = CASE
                WHEN pipeline_runs.status = 'succeeded' THEN pipeline_runs.started_at
                ELSE NOW()
            END,
            error_code = NULL,
            error_message = NULL
        """,
        tenant_id,
        user_id,
        session_id,
        pass_name,
        s.derived_pipeline_model_version,
        s.derived_pipeline_prompt_version,
        s.derived_pipeline_policy_version,
        input_watermark,
        input_hash,
    )
    row = await db.fetchone(
        """
        SELECT run_id
        FROM pipeline_runs
        WHERE tenant_id=$1
          AND user_id=$2
          AND pass_name=$3
          AND input_hash=$4
          AND model_version=$5
          AND prompt_version=$6
          AND COALESCE(policy_version, '')=COALESCE($7, '')
        ORDER BY run_id DESC
        LIMIT 1
        """,
        tenant_id,
        user_id,
        pass_name,
        input_hash,
        s.derived_pipeline_model_version,
        s.derived_pipeline_prompt_version,
        s.derived_pipeline_policy_version,
    )
    if not row:
        raise RuntimeError("failed to create pipeline run")
    return int(row["run_id"])


async def _complete_run(db: Database, run_id: int, output_hash: str) -> None:
    await db.execute(
        """
        UPDATE pipeline_runs
        SET status='succeeded', output_hash=$2, completed_at=NOW()
        WHERE run_id=$1
        """,
        run_id,
        output_hash,
    )


async def _fail_run(db: Database, run_id: int, code: str, message: str) -> None:
    await db.execute(
        """
        UPDATE pipeline_runs
        SET status='failed', error_code=$2, error_message=$3, completed_at=NOW()
        WHERE run_id=$1
        """,
        run_id,
        code[:120],
        message[:2000],
    )


async def _succeeded_run(db: Database, run_id: int) -> Optional[Dict[str, Any]]:
    row = await db.fetchone(
        """
        SELECT status, output_hash
        FROM pipeline_runs
        WHERE run_id=$1
        """,
        run_id,
    )
    if row and row.get("status") == "succeeded" and row.get("output_hash"):
        return row
    return None


async def _quarantine(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    pass_name: str,
    session_id: Optional[str],
    reason_code: str,
    payload: Dict[str, Any],
    run_id: Optional[int] = None,
) -> None:
    await db.execute(
        """
        INSERT INTO derived_quarantine (
            tenant_id, user_id, pass_name, session_id, reason_code, payload, run_id
        )
        VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7)
        """,
        tenant_id,
        user_id,
        pass_name,
        session_id,
        reason_code,
        payload,
        run_id,
    )


async def _write_assertion(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    pass_name: str,
    surface: str,
    statement_text: str,
    run_id: int,
    source_session_ids: Sequence[str],
    source_turn_refs: Sequence[Dict[str, Any]],
    slot_key: Optional[str] = None,
    salience: Optional[float] = None,
    importance: Optional[float] = None,
    confidence_extraction: Optional[float] = None,
    confidence_validity: Optional[float] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[int]:
    statement = normalize_text(statement_text, casefold=False)
    if not statement:
        return None
    if surface in HIGH_SIGNAL_SURFACES and (not source_session_ids or not source_turn_refs):
        await _quarantine(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            pass_name=pass_name,
            session_id=source_session_ids[0] if source_session_ids else None,
            reason_code=QUARANTINE_MISSING_EVIDENCE_REFS,
            payload={"surface": surface, "statement_text": statement, "slot_key": slot_key},
            run_id=run_id,
        )
        return None
    category = _semantic_category(statement, surface)
    memory_layer, retention_floor = _memory_layer_and_floor(category)
    row = await db.fetchone(
        """
        INSERT INTO derived_assertions (
            tenant_id, user_id, pass_name, surface, slot_key, statement_text,
            lifecycle_state, salience, importance, confidence_extraction,
            confidence_validity, source_session_ids, source_turn_refs,
            run_id, metadata, memory_layer, semantic_category, retention_floor,
            distinct_session_count
        )
        VALUES (
            $1,$2,$3,$4,$5,$6,'active',$7,$8,$9,$10,$11::text[],$12::jsonb,
            $13,$14::jsonb,$15,$16,$17,$18
        )
        RETURNING assertion_id
        """,
        tenant_id,
        user_id,
        pass_name,
        surface,
        slot_key,
        statement,
        salience,
        importance,
        confidence_extraction,
        confidence_validity,
        list(source_session_ids),
        list(source_turn_refs),
        run_id,
        metadata or {},
        memory_layer,
        category,
        retention_floor,
        len(set(source_session_ids)),
    )
    return int(row["assertion_id"]) if row else None


def _looks_uncertain(text: str) -> bool:
    return _low_confidence_reason(text) is not None


async def _write_low_confidence_item(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    surface: str,
    statement_text: str,
    source_session_ids: Sequence[str],
    source_turn_refs: Sequence[Dict[str, Any]],
    run_id: Optional[int],
    confidence: Optional[float] = None,
    question_text: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    statement = normalize_text(statement_text, casefold=False)
    if not statement or not source_session_ids:
        return
    if not _meaningful_low_confidence_question(question_text) and not _meaningful_low_confidence_question(statement):
        return
    await db.execute(
        """
        INSERT INTO low_confidence_items (
          tenant_id, user_id, surface, statement_text, question_text,
          confidence, status, source_session_ids, source_turn_refs,
          run_id, metadata, first_seen_at, last_seen_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,'open',$7::text[],$8::jsonb,$9,$10::jsonb,NOW(),NOW())
        """,
        tenant_id,
        user_id,
        surface,
        statement,
        normalize_text(question_text, casefold=False) or None,
        confidence,
        list(source_session_ids),
        list(source_turn_refs),
        run_id,
        metadata or {},
    )


async def _write_memory_event(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    event_type: str,
    title: str,
    description: Optional[str],
    source_session_ids: Sequence[str],
    source_turn_refs: Sequence[Dict[str, Any]],
    run_id: Optional[int],
    event_time: Optional[datetime] = None,
    time_confidence: float = 0.5,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    clean_title = normalize_text(title, casefold=False)
    if not clean_title or not source_session_ids:
        return
    if event_type not in {"past_event", "upcoming_event", "commitment", "anniversary", "deadline"}:
        event_type = "past_event"
    await db.execute(
        """
        INSERT INTO memory_events (
          tenant_id, user_id, event_type, title, description, event_time,
          time_confidence, lifecycle_state, source_session_ids, source_turn_refs,
          run_id, metadata, created_at, updated_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,'active',$8::text[],$9::jsonb,$10,$11::jsonb,NOW(),NOW())
        """,
        tenant_id,
        user_id,
        event_type,
        clean_title,
        normalize_text(description, casefold=False) or None,
        event_time,
        float(time_confidence),
        list(source_session_ids),
        list(source_turn_refs),
        run_id,
        metadata or {},
    )


async def _write_memory_contradiction(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    topic: str,
    earlier_view: str,
    recent_view: str,
    source_session_ids: Sequence[str],
    source_turn_refs: Sequence[Dict[str, Any]],
    run_id: Optional[int],
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    clean_topic = normalize_text(topic, casefold=False)
    earlier = normalize_text(earlier_view, casefold=False)
    recent = normalize_text(recent_view, casefold=False)
    if not clean_topic or not earlier or not recent or not source_session_ids:
        return
    await db.execute(
        """
        INSERT INTO memory_contradictions (
          tenant_id, user_id, topic, earlier_view, recent_view, status,
          source_session_ids, source_turn_refs, run_id, metadata,
          first_seen_at, last_seen_at
        )
        VALUES ($1,$2,$3,$4,$5,'active',$6::text[],$7::jsonb,$8,$9::jsonb,NOW(),NOW())
        """,
        tenant_id,
        user_id,
        clean_topic,
        earlier,
        recent,
        list(source_session_ids),
        list(source_turn_refs),
        run_id,
        metadata or {},
    )


async def _write_relationship_link(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    source_type: str,
    source_id: str,
    target_type: str,
    target_id: str,
    relationship_type: str,
    source_session_ids: Sequence[str],
    source_turn_refs: Sequence[Dict[str, Any]],
    confidence: float = 0.6,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    if not all(normalize_text(v) for v in (source_type, source_id, target_type, target_id, relationship_type)):
        return
    await db.execute(
        """
        INSERT INTO memory_relationship_links (
          tenant_id, user_id, source_type, source_id, target_type, target_id,
          relationship_type, confidence, source_session_ids, source_turn_refs,
          metadata, created_at, updated_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::text[],$10::jsonb,$11::jsonb,NOW(),NOW())
        ON CONFLICT (
          tenant_id, user_id, source_type, source_id, target_type, target_id, relationship_type
        )
        DO UPDATE SET
          confidence=GREATEST(memory_relationship_links.confidence, EXCLUDED.confidence),
          source_session_ids=(
            SELECT ARRAY(
              SELECT DISTINCT x
              FROM unnest(memory_relationship_links.source_session_ids || EXCLUDED.source_session_ids) AS x
              WHERE x IS NOT NULL AND x <> ''
            )
          ),
          source_turn_refs=memory_relationship_links.source_turn_refs || EXCLUDED.source_turn_refs,
          updated_at=NOW()
        """,
        tenant_id,
        user_id,
        normalize_text(source_type),
        normalize_text(source_id),
        normalize_text(target_type),
        normalize_text(target_id),
        normalize_text(relationship_type),
        float(confidence),
        list(source_session_ids),
        list(source_turn_refs),
        metadata or {},
    )


def _event_type_for_statement(statement: str) -> Optional[str]:
    lowered = normalize_text(statement)
    if not lowered:
        return None
    event_markers = (
        "birthday",
        "anniversary",
        "deadline",
        "next week",
        "tomorrow",
        "today",
        "yesterday",
        "ended",
        "broke up",
        "visited",
        "appointment",
        "by the end",
        "committed to",
    )
    if not any(marker in lowered for marker in event_markers):
        return None
    if "birthday" in lowered or "anniversary" in lowered:
        return "anniversary"
    if "deadline" in lowered or "by the end" in lowered:
        return "deadline"
    if "committed to" in lowered or "appointment" in lowered or "tomorrow" in lowered or "next week" in lowered:
        return "commitment"
    if "ended" in lowered or "broke up" in lowered or "visited" in lowered or "yesterday" in lowered:
        return "past_event"
    return "upcoming_event"


async def _update_checkpoint(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    pipeline_name: str,
    run_id: int,
    input_watermark: Optional[str],
    output_hash: str,
    increment_identity: int = 0,
    increment_context: int = 0,
    reset_identity: bool = False,
    reset_context: bool = False,
) -> None:
    await db.execute(
        """
        INSERT INTO pipeline_checkpoints (
            user_id, pipeline_name, tenant_id, last_processed,
            last_input_watermark, last_success_run_id, last_output_hash,
            identity_signal_count_since_last, context_delta_count_since_last, updated_at
        )
        VALUES (
            $2,$3,$1,NOW(),$5,$4,$6,
            CASE WHEN $9 THEN 0 ELSE $7 END,
            CASE WHEN $10 THEN 0 ELSE $8 END,
            NOW()
        )
        ON CONFLICT (user_id, pipeline_name)
        DO UPDATE SET
            tenant_id=EXCLUDED.tenant_id,
            last_processed=NOW(),
            last_input_watermark=EXCLUDED.last_input_watermark,
            last_success_run_id=EXCLUDED.last_success_run_id,
            last_output_hash=EXCLUDED.last_output_hash,
            identity_signal_count_since_last=CASE
                WHEN $9 THEN 0
                ELSE COALESCE(pipeline_checkpoints.identity_signal_count_since_last, 0) + $7
            END,
            context_delta_count_since_last=CASE
                WHEN $10 THEN 0
                ELSE COALESCE(pipeline_checkpoints.context_delta_count_since_last, 0) + $8
            END,
            updated_at=NOW()
        """,
        tenant_id,
        user_id,
        pipeline_name,
        run_id,
        input_watermark,
        output_hash,
        int(increment_identity),
        int(increment_context),
        bool(reset_identity),
        bool(reset_context),
    )


async def _checkpoint(db: Database, user_id: str, pipeline_name: str) -> Optional[Dict[str, Any]]:
    return await db.fetchone(
        """
        SELECT *
        FROM pipeline_checkpoints
        WHERE user_id=$1 AND pipeline_name=$2
        """,
        user_id,
        pipeline_name,
    )


async def _source_turn_refs_from_session(
    *,
    db: Database,
    session_id: str,
    text_hint: Optional[str] = None,
    max_refs: int = 8,
) -> List[Dict[str, Any]]:
    row = await db.fetchone(
        """
        SELECT messages
        FROM session_transcript
        WHERE session_id=$1
        LIMIT 1
        """,
        session_id,
    )
    messages = (row or {}).get("messages")
    if not isinstance(messages, list):
        messages = []
    refs = _source_turn_refs(session_id=session_id, messages=messages, text_hint=text_hint, max_refs=max_refs)
    if refs:
        return refs
    return [{"session_id": session_id, "source": "session_classifications"}]


def _extract_json_object(raw: str) -> Optional[Dict[str, Any]]:
    text = (raw or "").strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        pass
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            parsed = json.loads(text[start : end + 1])
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None
    return None


def _heuristic_pass1(messages: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    user_text = _messages_text(messages, user_only=True)
    text_norm = normalize_text(user_text)
    sentences = [
        normalize_text(s, casefold=False)
        for s in re.split(r"(?<=[.!?])\s+|\n+", user_text)
        if normalize_text(s)
    ]
    signal_terms = (
        "remember",
        "don't forget",
        "do not forget",
        "need to",
        "i want",
        "i am",
        "i'm",
        "my ",
        "ashley",
        "jasmine",
        "health",
        "pain",
        "hospital",
        "relationship",
        "sophie",
        "synapse",
    )
    memory_deltas = [s for s in sentences if any(term in normalize_text(s) for term in signal_terms)][:4]
    entity_mentions: List[str] = []
    for match in re.findall(r"\b[A-Z][a-zA-Z]{2,}(?:\s+[A-Z][a-zA-Z]{2,}){0,2}\b", user_text):
        name = normalize_text(match, casefold=False)
        if name in {"I", "The", "This", "That", "Pass", "Model", "JSON"}:
            continue
        if name not in entity_mentions:
            entity_mentions.append(name)
    identity_signals = [
        s for s in sentences
        if any(term in normalize_text(s) for term in ("i am", "i'm", "i want", "i believe", "i feel", "my fear", "my goal"))
    ][:4]
    thread_signals = [
        s for s in sentences
        if any(term in normalize_text(s) for term in ("need to", "follow up", "remind", "worried", "waiting", "unresolved", "goal"))
    ][:4]
    context_relevant = bool(memory_deltas or thread_signals or "tension" in text_norm or "stressed" in text_norm)
    emotional_weight = "medium" if any(t in text_norm for t in ("stressed", "upset", "worried", "scared", "angry")) else "low"
    return {
        "is_memory_worthy": bool(memory_deltas or entity_mentions or identity_signals or thread_signals),
        "session_kind": "mixed" if memory_deltas else "transient",
        "memory_deltas": memory_deltas,
        "entity_mentions": entity_mentions[:12],
        "thread_signals": thread_signals,
        "identity_signals": identity_signals,
        "emotional_weight": emotional_weight,
        "emotional_note": None,
        "tension_signal": None,
        "context_relevant": context_relevant,
        "run_entity_pass": bool(entity_mentions),
        "run_threads_pass": bool(thread_signals),
        "identity_relevant": bool(identity_signals),
    }


async def _call_pass1_llm(messages: Sequence[Dict[str, Any]], settings: Settings) -> Optional[Dict[str, Any]]:
    if not bool(settings.derived_pipeline_llm_enabled):
        return None
    return await run_rich_pass1_llm(
        messages=list(messages),
        model=settings.derived_pipeline_model_version,
    )


def _normalize_pass1_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    memory_deltas = _text_list(payload.get("memory_deltas"), limit=6)
    entity_mentions = _text_list(payload.get("entity_mentions"), limit=20)
    thread_signals = _text_list(payload.get("thread_signals"), limit=8)
    identity_signals = _text_list(payload.get("identity_signals"), limit=8)
    context_relevant = _to_bool(payload.get("context_relevant")) or bool(memory_deltas or thread_signals or payload.get("tension_signal"))
    return {
        "is_memory_worthy": _to_bool(payload.get("is_memory_worthy")) or bool(memory_deltas or entity_mentions or thread_signals or identity_signals),
        "session_kind": normalize_text(payload.get("session_kind")) or "transient",
        "memory_deltas": memory_deltas,
        "entity_mentions": entity_mentions,
        "thread_signals": thread_signals,
        "identity_signals": identity_signals,
        "emotional_weight": normalize_text(payload.get("emotional_weight")) or "none",
        "emotional_note": normalize_text(payload.get("emotional_note"), casefold=False) or None,
        "tension_signal": normalize_text(payload.get("tension_signal"), casefold=False) or None,
        "context_relevant": context_relevant,
        "run_entity_pass": _to_bool(payload.get("run_entity_pass")) or bool(entity_mentions),
        "run_threads_pass": _to_bool(payload.get("run_threads_pass")) or bool(thread_signals),
        "identity_relevant": _to_bool(payload.get("identity_relevant")) or bool(identity_signals),
    }


async def run_pass1_triage(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    session_id: str,
    messages: Sequence[Dict[str, Any]],
    reference_time: Optional[datetime] = None,
    settings: Optional[Settings] = None,
) -> DerivedPassResult:
    s = settings or get_settings()
    input_hash = _input_hash(
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=session_id,
        pass_name=PASS1_TRIAGE,
        messages=messages,
        settings=s,
    )
    run_id = await _start_run(
        db=db,
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=session_id,
        pass_name=PASS1_TRIAGE,
        input_hash=input_hash,
        input_watermark=session_id,
        settings=s,
    )
    existing = await _succeeded_run(db, run_id)
    if existing:
        classification = await db.fetchone(
            """
            SELECT run_entity_pass, run_threads_pass
            FROM session_classifications
            WHERE session_id=$1 AND user_id=$2
            """,
            session_id,
            user_id,
        ) or {}
        return DerivedPassResult(
            run_id=run_id,
            pass_name=PASS1_TRIAGE,
            output_hash=str(existing["output_hash"]),
            run_entity_pass=bool(classification.get("run_entity_pass")),
            run_threads_pass=bool(classification.get("run_threads_pass")),
            should_run_identity=await should_run_pass4_identity(db=db, user_id=user_id, settings=s),
            should_run_living_context=await should_run_pass5_living_context(db=db, user_id=user_id, settings=s),
        )
    try:
        parsed = await _call_pass1_llm(messages, s)
        source = "llm"
        if not parsed:
            if bool(s.derived_pipeline_llm_enabled):
                await _quarantine(
                    db=db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    pass_name=PASS1_TRIAGE,
                    session_id=session_id,
                    reason_code=QUARANTINE_PARSE_FAILURE,
                    payload={
                        "message": "Pass 1 LLM output was missing or not valid JSON; deterministic fallback used.",
                        "fallback": "heuristic",
                    },
                    run_id=run_id,
                )
            parsed = _heuristic_pass1(messages)
            source = "heuristic_fallback"
        payload = _normalize_pass1_payload(parsed)
        payload["source"] = source
        session_date = reference_time or _utcnow()
        output_hash = stable_short_hash(payload, length=24)
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy,
                session_kind, one_line_summary, entity_mentions, run_entity_pass,
                run_threads_pass, identity_relevant, emotional_weight,
                emotional_note, tension_signal, processed_at, model_used,
                raw_triage_output, context_relevant
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7::text[],$8,$9,$10,$11,$12,$13,NOW(),$14,$15::jsonb,$16)
            ON CONFLICT (session_id)
            DO UPDATE SET
                user_id=EXCLUDED.user_id,
                session_date=EXCLUDED.session_date,
                is_memory_worthy=EXCLUDED.is_memory_worthy,
                session_kind=EXCLUDED.session_kind,
                one_line_summary=EXCLUDED.one_line_summary,
                entity_mentions=EXCLUDED.entity_mentions,
                run_entity_pass=EXCLUDED.run_entity_pass,
                run_threads_pass=EXCLUDED.run_threads_pass,
                identity_relevant=EXCLUDED.identity_relevant,
                emotional_weight=EXCLUDED.emotional_weight,
                emotional_note=EXCLUDED.emotional_note,
                tension_signal=EXCLUDED.tension_signal,
                processed_at=NOW(),
                model_used=EXCLUDED.model_used,
                raw_triage_output=EXCLUDED.raw_triage_output,
                context_relevant=EXCLUDED.context_relevant
            """,
            session_id,
            user_id,
            session_date,
            payload["is_memory_worthy"],
            payload["session_kind"],
            payload["memory_deltas"][0] if payload["memory_deltas"] else None,
            payload["entity_mentions"],
            payload["run_entity_pass"],
            payload["run_threads_pass"],
            payload["identity_relevant"],
            payload["emotional_weight"],
            payload["emotional_note"],
            payload["tension_signal"],
            s.derived_pipeline_model_version if source == "llm" else f"{s.derived_pipeline_model_version}:fallback",
            payload,
            payload["context_relevant"],
        )
        assertions: List[int] = []
        for statement in payload["memory_deltas"]:
            refs = _source_turn_refs(session_id=session_id, messages=messages, text_hint=statement)
            assertion_id = await _write_assertion(
                db=db,
                tenant_id=tenant_id,
                user_id=user_id,
                pass_name=PASS1_TRIAGE,
                surface="memory_delta",
                statement_text=statement,
                run_id=run_id,
                source_session_ids=[session_id],
                source_turn_refs=refs,
                salience=0.55,
                importance=0.55,
                confidence_extraction=0.72 if source == "llm" else 0.45,
                confidence_validity=0.65 if source == "llm" else 0.45,
            )
            if assertion_id:
                assertions.append(assertion_id)
            event_type = _event_type_for_statement(statement)
            if event_type:
                await _write_memory_event(
                    db=db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    event_type=event_type,
                    title=statement[:180],
                    description=statement,
                    source_session_ids=[session_id],
                    source_turn_refs=refs,
                    run_id=run_id,
                    metadata={"source": "pass1.memory_delta"},
                )
            reason_code = _low_confidence_reason(statement, "memory_delta")
            if reason_code:
                await _write_low_confidence_item(
                    db=db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    surface="memory_delta",
                    statement_text=statement,
                    question_text=_low_confidence_question(reason_code, statement),
                    confidence=0.45 if source == "llm" else 0.35,
                    source_session_ids=[session_id],
                    source_turn_refs=refs,
                    run_id=run_id,
                    metadata={"source": "pass1.memory_delta", "reason_code": reason_code},
                )
        for statement in payload["identity_signals"]:
            refs = _source_turn_refs(session_id=session_id, messages=messages, text_hint=statement)
            await _write_assertion(
                db=db,
                tenant_id=tenant_id,
                user_id=user_id,
                pass_name=PASS1_TRIAGE,
                surface="identity_signal",
                statement_text=statement,
                slot_key=None,
                run_id=run_id,
                source_session_ids=[session_id],
                source_turn_refs=refs,
                salience=0.7,
                importance=0.75,
                confidence_extraction=0.72 if source == "llm" else 0.45,
                confidence_validity=0.62 if source == "llm" else 0.42,
            )
            reason_code = _low_confidence_reason(statement, "identity_signal")
            if reason_code:
                await _write_low_confidence_item(
                    db=db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    surface="identity_signal",
                    statement_text=statement,
                    question_text=_low_confidence_question(reason_code, statement),
                    confidence=0.45 if source == "llm" else 0.35,
                    source_session_ids=[session_id],
                    source_turn_refs=refs,
                    run_id=run_id,
                    metadata={"source": "pass1.identity_signal", "reason_code": reason_code},
                )
        for statement in payload["thread_signals"]:
            refs = _source_turn_refs(session_id=session_id, messages=messages, text_hint=statement)
            await _write_assertion(
                db=db,
                tenant_id=tenant_id,
                user_id=user_id,
                pass_name=PASS1_TRIAGE,
                surface="thread_signal",
                statement_text=statement,
                run_id=run_id,
                source_session_ids=[session_id],
                source_turn_refs=refs,
                salience=0.65,
                importance=0.65,
                confidence_extraction=0.72 if source == "llm" else 0.45,
                confidence_validity=0.58 if source == "llm" else 0.4,
            )
            reason_code = _low_confidence_reason(statement, "thread_signal")
            if reason_code:
                await _write_low_confidence_item(
                    db=db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    surface="thread_signal",
                    statement_text=statement,
                    question_text=_low_confidence_question(reason_code, statement),
                    confidence=0.45 if source == "llm" else 0.35,
                    source_session_ids=[session_id],
                    source_turn_refs=refs,
                    run_id=run_id,
                    metadata={"source": "pass1.thread_signal", "reason_code": reason_code},
                )
        if payload.get("tension_signal"):
            refs = _source_turn_refs(session_id=session_id, messages=messages, text_hint=payload["tension_signal"])
            await _write_assertion(
                db=db,
                tenant_id=tenant_id,
                user_id=user_id,
                pass_name=PASS1_TRIAGE,
                surface="living_context_statement",
                statement_text=payload["tension_signal"],
                run_id=run_id,
                source_session_ids=[session_id],
                source_turn_refs=refs,
                salience=0.7,
                importance=0.65,
                confidence_extraction=0.72 if source == "llm" else 0.45,
                confidence_validity=0.58 if source == "llm" else 0.4,
                metadata={"kind": "tension_signal"},
            )
            reason_code = _low_confidence_reason(payload["tension_signal"], "tension_signal")
            if reason_code:
                await _write_low_confidence_item(
                    db=db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    surface="tension_signal",
                    statement_text=payload["tension_signal"],
                    question_text=_low_confidence_question(reason_code, payload["tension_signal"]),
                    confidence=0.45 if source == "llm" else 0.35,
                    source_session_ids=[session_id],
                    source_turn_refs=refs,
                    run_id=run_id,
                    metadata={"source": "pass1.tension_signal", "reason_code": reason_code},
                )

        await _complete_run(db, run_id, output_hash)
        await _update_checkpoint(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            pipeline_name=PASS1_TRIAGE,
            run_id=run_id,
            input_watermark=session_id,
            output_hash=output_hash,
            increment_identity=1 if payload["identity_relevant"] else 0,
            increment_context=1 if payload["context_relevant"] else 0,
        )
        should_identity = await should_run_pass4_identity(db=db, user_id=user_id, settings=s)
        should_context = await should_run_pass5_living_context(db=db, user_id=user_id, settings=s)
        return DerivedPassResult(
            run_id=run_id,
            pass_name=PASS1_TRIAGE,
            output_hash=output_hash,
            run_entity_pass=payload["run_entity_pass"],
            run_threads_pass=payload["run_threads_pass"],
            should_run_identity=should_identity,
            should_run_living_context=should_context,
        )
    except Exception as exc:
        await _fail_run(db, run_id, "pass1_failed", str(exc))
        raise


async def should_run_pass4_identity(*, db: Database, user_id: str, settings: Optional[Settings] = None) -> bool:
    s = settings or get_settings()
    pass1 = await _checkpoint(db, user_id, PASS1_TRIAGE)
    if pass1 and int(pass1.get("identity_signal_count_since_last") or 0) >= int(s.derived_pipeline_identity_signal_threshold):
        return True
    previous = await _checkpoint(db, user_id, PASS4_IDENTITY)
    last = previous.get("last_processed") if previous else None
    if isinstance(last, datetime):
        return (_utcnow() - last.astimezone(timezone.utc)) >= timedelta(days=int(s.derived_pipeline_identity_ceiling_days))
    return bool(pass1 and int(pass1.get("identity_signal_count_since_last") or 0) > 0)


async def should_run_pass5_living_context(*, db: Database, user_id: str, settings: Optional[Settings] = None) -> bool:
    s = settings or get_settings()
    pass1 = await _checkpoint(db, user_id, PASS1_TRIAGE)
    if pass1 and int(pass1.get("context_delta_count_since_last") or 0) >= int(s.derived_pipeline_context_delta_threshold):
        return True
    previous = await _checkpoint(db, user_id, PASS5_LIVING_CONTEXT)
    last = previous.get("last_processed") if previous else None
    if isinstance(last, datetime):
        return (_utcnow() - last.astimezone(timezone.utc)) >= timedelta(days=int(s.derived_pipeline_context_ceiling_days))
    return bool(pass1 and int(pass1.get("context_delta_count_since_last") or 0) > 0)


def _canonical_name(name: str) -> str:
    return normalize_text(name, casefold=False).strip()


def _canonical_name_norm(name: str) -> str:
    return normalize_text(name)


def _entity_type(name: str) -> str:
    lowered = normalize_text(name)
    if lowered in {"sophie", "synapse", "bluum"}:
        return "project" if lowered != "sophie" else "other"
    return "person"


def _valid_thread_transition(current_status: str, current_lifecycle: str, action: str) -> bool:
    action_norm = normalize_text(action).upper()
    status = normalize_text(current_status)
    lifecycle = normalize_text(current_lifecycle)
    if lifecycle in {"resolved", "superseded"} and action_norm in {"UPDATE", "SNOOZE"}:
        return False
    if status == "resolved" and action_norm in {"UPDATE", "SNOOZE"}:
        return False
    if action_norm == "RESOLVE" and lifecycle in {"resolved", "superseded"}:
        return False
    return True


def _thread_quality_fields_present(action: Dict[str, Any]) -> bool:
    return any(
        key in action
        for key in (
            "unresolvedness",
            "follow_up_value",
            "evidence_strength",
            "why_this_matters_later",
        )
    )


def _follow_up_value(value: Any, *, default: str = "medium") -> str:
    follow_up = normalize_text(value)
    if follow_up in {"low", "medium", "high"}:
        return follow_up
    return default


def _unresolvedness(value: Any, *, default: str = "open") -> str:
    state = normalize_text(value)
    if state in {"open", "resolved", "unclear"}:
        return state
    return default


def _thread_action_allowed(action: Dict[str, Any], *, kind: str, category: str, priority: str) -> bool:
    if not _thread_quality_fields_present(action):
        return True
    evidence_strength = _resolution_strength(action.get("evidence_strength"))
    follow_up_value = _follow_up_value(action.get("follow_up_value"))
    unresolvedness = _unresolvedness(action.get("unresolvedness"))
    why_later = normalize_text(action.get("why_this_matters_later"))
    if kind == "CREATE":
        if unresolvedness == "resolved":
            return False
        if unresolvedness == "unclear" and priority != "high":
            return False
        if evidence_strength == "weak" and priority != "high":
            return False
        if follow_up_value == "low" and priority != "high":
            return False
        if category == "commitment" and follow_up_value == "low":
            return False
        if not why_later and priority != "high":
            return False
    if kind == "UPDATE":
        if evidence_strength == "weak" and follow_up_value == "low":
            return False
    if kind in {"RESOLVE", "SNOOZE"}:
        if evidence_strength == "weak":
            return False
    return True


def _thread_topic_key(*, title: Any, detail: Any, category: Any, related_entities: Any = None) -> str:
    text = " ".join(
        [
            normalize_text(title),
            normalize_text(detail),
            " ".join(_text_list(related_entities, limit=8)),
        ]
    ).casefold()
    cat = normalize_text(category)
    if any(term in text for term in ("kidney stone", "kidney stones", "hydration", "electrolyte", "lemon water")):
        return "health:kidney_hydration"
    if "gym" in text and any(term in text for term in ("routine", "workout", "training", "exercise")):
        return "goal:gym_routine"
    if any(term in text for term in ("sophie", "assistant")) and any(
        term in text for term in ("memory", "reliability", "hallucinat", "gaslight", "repeat")
    ):
        return "assistant_feedback:sophie_reliability"
    for entity in _text_list(related_entities, limit=8):
        entity_norm = _canonical_name_norm(entity)
        if entity_norm and any(term in text for term in ("reconnect", "contact", "message", "instagram", "check-in", "check in")):
            return f"relationship:{entity_norm}:contact"
    words = [
        word
        for word in re.findall(r"[a-z0-9]+", text)
        if len(word) >= 4
        and word
        not in {
            "user",
            "thread",
            "with",
            "from",
            "this",
            "that",
            "their",
            "about",
            "still",
            "needs",
            "wants",
            "goal",
            "plan",
        }
    ]
    return f"{cat or 'other'}:" + " ".join(sorted(set(words))[:8])


def _text_has_any_term(value: Any, terms: set[str]) -> bool:
    text = normalize_text(value).casefold()
    return bool(text and any(term in text for term in terms))


def _is_relationship_resolution_action(action: Dict[str, Any], *, category: str) -> bool:
    if category != "relationship":
        return False
    if _unresolvedness(action.get("unresolvedness"), default="open") == "resolved":
        return True
    if _resolution_strength(action.get("evidence_strength")) == "weak":
        return False
    text = " ".join(
        [
            normalize_text(action.get("title")),
            normalize_text(action.get("detail")),
            normalize_text(action.get("why_this_matters_later")),
        ]
    )
    return _text_has_any_term(text, RELATIONSHIP_RESOLUTION_TERMS)


def _is_relationship_conflict_thread(row: Dict[str, Any]) -> bool:
    if normalize_text(row.get("category")) != "relationship":
        return False
    text = " ".join(
        [
            normalize_text(row.get("title")),
            normalize_text(row.get("detail")),
            normalize_text(row.get("resolution_note")),
        ]
    )
    return _text_has_any_term(text, RELATIONSHIP_CONFLICT_TERMS)


def _thread_related_entity_set(row: Dict[str, Any]) -> set[str]:
    entities = {
        _canonical_name_norm(item)
        for item in _text_list(row.get("related_entities"), limit=8)
        if _canonical_name_norm(item)
    }
    return {item for item in entities if item}


def _profile_supports_conservative_rewrite(row: Dict[str, Any]) -> bool:
    try:
        confidence = float(row.get("confidence") or 0.0)
    except Exception:
        confidence = 0.0
    try:
        salience = float(row.get("salience_score") or 0.0)
    except Exception:
        salience = 0.0
    try:
        importance = float(row.get("importance_score") or 0.0)
    except Exception:
        importance = 0.0
    mention_count = int(row.get("mention_count") or 0)
    distinct_sessions = int(row.get("distinct_session_count") or 0)
    return (
        confidence >= 0.85
        or salience >= 0.8
        or importance >= 0.8
        or mention_count >= 3
        or distinct_sessions >= 2
    )


def _is_static_zombie_thread(row: Dict[str, Any]) -> bool:
    if normalize_text(row.get("status")) != "open":
        return False
    if normalize_text(row.get("thread_type")) == "persistent_goal":
        return False
    if normalize_text(row.get("lifecycle_state")) not in {"", "active"}:
        return False
    last_seen = row.get("last_mentioned_at") or row.get("last_updated_at") or row.get("first_seen_at")
    if not isinstance(last_seen, datetime):
        return False
    if (_utcnow() - last_seen.astimezone(timezone.utc)) < timedelta(days=45):
        return False
    follow_up_after = row.get("follow_up_after")
    if isinstance(follow_up_after, datetime) and follow_up_after.astimezone(timezone.utc) >= _utcnow():
        return False
    return True


async def _find_existing_thread_for_topic(
    *,
    db: Database,
    user_id: str,
    title: Any,
    detail: Any,
    category: Any,
    related_entities: Any = None,
    include_resolved: bool = False,
) -> Optional[Dict[str, Any]]:
    incoming_key = _thread_topic_key(
        title=title,
        detail=detail,
        category=category,
        related_entities=related_entities,
    )
    if not incoming_key:
        return None
    rows = await db.fetch(
        """
        SELECT thread_id, title, detail, category, status, lifecycle_state, related_entities,
               source_session_ids
        FROM open_threads
        WHERE user_id=$1
          AND (
            status IN ('open','snoozed')
            OR ($2 AND status='resolved')
          )
        ORDER BY
          CASE status WHEN 'open' THEN 0 ELSE 1 END,
          last_mentioned_at DESC NULLS LAST,
          last_updated_at DESC NULLS LAST
        LIMIT 80
        """,
        user_id,
        include_resolved,
    )
    for row in rows:
        existing_key = _thread_topic_key(
            title=row.get("title"),
            detail=row.get("detail"),
            category=row.get("category"),
            related_entities=row.get("related_entities"),
        )
        if existing_key == incoming_key:
            return dict(row)
    return None


async def run_pass1_5_entities(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    session_id: str,
    messages: Sequence[Dict[str, Any]],
    settings: Optional[Settings] = None,
) -> DerivedPassResult:
    s = settings or get_settings()
    classification = await db.fetchone(
        "SELECT entity_mentions, raw_triage_output FROM session_classifications WHERE session_id=$1 AND user_id=$2",
        session_id,
        user_id,
    )
    mentions = _text_list((classification or {}).get("entity_mentions"), limit=20)
    input_hash = _input_hash(
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=session_id,
        pass_name=PASS1_5_ENTITIES,
        messages=messages,
        extra={"entity_mentions": mentions},
        settings=s,
    )
    run_id = await _start_run(
        db=db,
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=session_id,
        pass_name=PASS1_5_ENTITIES,
        input_hash=input_hash,
        input_watermark=session_id,
        settings=s,
    )
    existing = await _succeeded_run(db, run_id)
    if existing:
        return DerivedPassResult(run_id=run_id, pass_name=PASS1_5_ENTITIES, output_hash=str(existing["output_hash"]))
    try:
        resolved_items: Optional[List[Dict[str, Any]]] = None
        if bool(s.derived_pipeline_llm_enabled) and mentions:
            existing_entities = await db.fetch(
                """
                SELECT entity_id, canonical_name, canonical_name_normalized, type,
                       aliases, relationship_to_user, status, confidence
                FROM entity_profiles
                WHERE user_id=$1
                ORDER BY last_seen_at DESC NULLS LAST, created_at DESC
                LIMIT 80
                """,
                user_id,
            )
            mention_payload = [
                {
                    "mention": name,
                    "session_ids": [session_id],
                    "context_snippets": [ref.get("text") for ref in _source_turn_refs(session_id=session_id, messages=messages, text_hint=name)[:3]],
                }
                for name in mentions
            ]
            resolved_items = await resolve_entity_mentions(
                existing_entities=existing_entities,
                mentions=mention_payload,
                model=s.derived_pipeline_model_version,
            )
        if resolved_items:
            names_to_write: List[Dict[str, Any]] = []
            existing_by_id = {
                _canonical_name_norm(row.get("entity_id")): row
                for row in await db.fetch("SELECT * FROM entity_profiles WHERE user_id=$1", user_id)
            }
            for item in resolved_items:
                decision = normalize_text(item.get("decision")).upper()
                mention = _canonical_name(item.get("mention"))
                if decision == "SKIP":
                    continue
                if decision == "MATCH":
                    matched_id = normalize_text(item.get("matched_entity_id"))
                    target = existing_by_id.get(matched_id)
                    if target:
                        await db.execute(
                            """
                            UPDATE entity_profiles
                            SET aliases = (
                                  SELECT ARRAY(
                                    SELECT DISTINCT x
                                    FROM unnest(COALESCE(aliases, '{}') || $2::text[]) AS x
                                    WHERE x IS NOT NULL AND x <> ''
                                  )
                                ),
                                mention_count = COALESCE(mention_count, 0) + 1,
                                last_seen_at = NOW(),
                                last_updated_at = NOW(),
                                source_session_ids = (
                                  SELECT ARRAY(
                                    SELECT DISTINCT x
                                    FROM unnest(COALESCE(source_session_ids, '{}') || $3::text[]) AS x
                                    WHERE x IS NOT NULL AND x <> ''
                                  )
                                ),
                                confidence = LEAST(COALESCE(confidence, 0.4) + 0.05, 1.0)
                            WHERE user_id=$1 AND entity_id=$4
                            """,
                            user_id,
                            [mention] if mention else [],
                            [session_id],
                            matched_id,
                        )
                        names_to_write.append({"name": target.get("canonical_name") or mention, "matched": True})
                    continue
                if decision in {"NEW", "TENTATIVE"}:
                    confidence = _confidence_float(item.get("confidence"), default=0.45)
                    evidence_strength = _resolution_strength(
                        item.get("evidence_strength"),
                        default="medium" if confidence >= 0.75 else "weak",
                    )
                    memory_relevance = _resolution_relevance(item.get("memory_relevance"))
                    status = normalize_text(item.get("status")) or ("tentative" if decision == "TENTATIVE" else "active")
                    if decision == "TENTATIVE" or evidence_strength == "weak" or memory_relevance == "low":
                        status = "tentative"
                        confidence = min(confidence, 0.55)
                    names_to_write.append(
                        {
                            "name": _canonical_name(item.get("canonical_name")) or mention,
                            "type": normalize_text(item.get("type")) or None,
                            "status": status,
                            "relationship_to_user": normalize_text(item.get("relationship_to_user")) or None,
                            "confidence": confidence,
                            "aliases": _text_list(item.get("aliases")) or ([mention] if mention else []),
                            "decision": decision,
                            "evidence_strength": evidence_strength,
                            "memory_relevance": memory_relevance,
                            "relationship_confidence": _confidence_float(item.get("relationship_confidence"), default=confidence),
                            "why_this_matters": normalize_text(item.get("why_this_matters") or item.get("reason"), casefold=False),
                        }
                    )
            write_items = names_to_write
        else:
            write_items = [{"name": name} for name in mentions]

        for item in write_items:
            name = item.get("name") if isinstance(item, dict) else item
            canonical = _canonical_name(name)
            canonical_norm = _canonical_name_norm(canonical)
            if not canonical_norm:
                continue
            source_refs = _source_turn_refs(session_id=session_id, messages=messages, text_hint=canonical)
            if not source_refs:
                continue
            existing_entity = await db.fetchone(
                """
                SELECT type, relationship_to_user
                FROM entity_profiles
                WHERE user_id=$1 AND canonical_name_normalized=$2
                """,
                user_id,
                canonical_norm,
            )
            category = _semantic_category(canonical, "entity_mention")
            memory_layer, _floor = _memory_layer_and_floor(category)
            entity_status = normalize_text((item or {}).get("status") if isinstance(item, dict) else None) or "tentative"
            evidence_strength = _resolution_strength((item or {}).get("evidence_strength") if isinstance(item, dict) else None)
            memory_relevance = _resolution_relevance((item or {}).get("memory_relevance") if isinstance(item, dict) else None)
            relationship_confidence = _confidence_float(
                (item or {}).get("relationship_confidence") if isinstance(item, dict) else None,
                default=_confidence_float((item or {}).get("confidence") if isinstance(item, dict) else None, default=0.45),
            )
            entity_type, relationship_to_user = _sanitize_entity_fields(
                canonical=canonical,
                entity_type=normalize_text((item or {}).get("type") if isinstance(item, dict) else None) or None,
                relationship_to_user=normalize_text((item or {}).get("relationship_to_user") if isinstance(item, dict) else None) or None,
                messages=messages,
                existing_relationship=(existing_entity or {}).get("relationship_to_user"),
            )
            confidence_value = (item or {}).get("confidence") if isinstance(item, dict) else None
            if not isinstance(confidence_value, (int, float)):
                confidence_value = 0.45
            aliases_value = _text_list((item or {}).get("aliases") if isinstance(item, dict) else None) or [canonical]
            await db.execute(
                """
                INSERT INTO entity_profiles (
                    user_id, canonical_name, canonical_name_normalized, type,
                    aliases, status, relationship_to_user, confidence, mention_count, first_seen_at,
                    last_seen_at, last_updated_at, last_processed_session_date,
                    source_session_ids, memory_layer
                )
                VALUES (
                    $1,$2,$3,$4,$5::text[],$6,$7,$8,1,NOW(),NOW(),NOW(),NOW(),$9::text[],$10
                )
                ON CONFLICT (user_id, canonical_name_normalized)
                DO UPDATE SET
                    aliases = (
                        SELECT ARRAY(
                            SELECT DISTINCT x
                            FROM unnest(COALESCE(entity_profiles.aliases, '{}') || EXCLUDED.aliases) AS x
                            WHERE x IS NOT NULL AND x <> ''
                        )
                    ),
                    mention_count = COALESCE(entity_profiles.mention_count, 0) + 1,
                    last_seen_at = NOW(),
                    last_updated_at = NOW(),
                    last_processed_session_date = NOW(),
                    source_session_ids = (
                        SELECT ARRAY(
                            SELECT DISTINCT x
                            FROM unnest(COALESCE(entity_profiles.source_session_ids, '{}') || EXCLUDED.source_session_ids) AS x
                            WHERE x IS NOT NULL AND x <> ''
                        )
                    ),
                    status = CASE
                        WHEN EXCLUDED.status = 'active' THEN 'active'
                        WHEN COALESCE(entity_profiles.mention_count, 0) + 1 >= 3 THEN 'active'
                        ELSE entity_profiles.status
                    END,
                    memory_layer = COALESCE(entity_profiles.memory_layer, EXCLUDED.memory_layer)
                """,
                user_id,
                canonical,
                canonical_norm,
                entity_type,
                aliases_value,
                entity_status,
                relationship_to_user,
                float(confidence_value),
                [session_id],
                memory_layer,
            )
            await db.execute(
                """
                UPDATE entity_profiles
                SET type=$3,
                    relationship_to_user=$4,
                    distinct_session_count=GREATEST(
                        COALESCE(distinct_session_count, 0),
                        COALESCE(cardinality(source_session_ids), 0)
                    ),
                    last_updated_at=NOW()
                WHERE user_id=$1 AND canonical_name_normalized=$2
                """,
                user_id,
                canonical_norm,
                entity_type,
                relationship_to_user,
            )
            if bool(s.derived_pipeline_llm_enabled):
                current_profile = await db.fetchone(
                    """
                    SELECT entity_id, profile_text, mention_count
                    FROM entity_profiles
                    WHERE user_id=$1 AND canonical_name_normalized=$2
                    """,
                    user_id,
                    canonical_norm,
                )
                entity_messages = [
                    msg
                    for msg in list(messages)
                    if normalize_text(msg.get("role")) == "user"
                    and canonical_norm in normalize_text(msg.get("text") or msg.get("content"))
                ]
                should_profile = _resolution_profile_allowed(
                    canonical=canonical,
                    entity_type=entity_type,
                    relationship_to_user=relationship_to_user,
                    status=entity_status,
                    evidence_strength=evidence_strength,
                    memory_relevance=memory_relevance,
                    relationship_confidence=relationship_confidence,
                    confidence=float(confidence_value),
                    existing_profile_text=(current_profile or {}).get("profile_text"),
                    entity_messages=entity_messages,
                    source_turn_refs=source_refs,
                )
                if should_profile:
                    profile_payload = await build_entity_profile(
                        canonical_name=canonical,
                        entity_type=entity_type,
                        relationship_to_user=relationship_to_user or "other",
                        messages=entity_messages,
                        existing_profile_text=(current_profile or {}).get("profile_text"),
                        model=s.derived_pipeline_model_version,
                    )
                    if profile_payload:
                        await db.execute(
                            """
                            UPDATE entity_profiles
                            SET profile_text=COALESCE($3, profile_text),
                                key_facts=$4::jsonb,
                                open_questions=$5::jsonb,
                                last_known_status=COALESCE($6, last_known_status),
                                last_updated_at=NOW()
                            WHERE user_id=$1 AND canonical_name_normalized=$2
                            """,
                            user_id,
                            canonical_norm,
                            profile_payload.get("profile_text"),
                            profile_payload.get("key_facts") or [],
                            profile_payload.get("open_questions") or [],
                            profile_payload.get("last_known_status"),
                        )
            await _write_assertion(
                db=db,
                tenant_id=tenant_id,
                user_id=user_id,
                pass_name=PASS1_5_ENTITIES,
                surface="entity_mention",
                statement_text=f"{canonical} was mentioned in the session.",
                run_id=run_id,
                source_session_ids=[session_id],
                source_turn_refs=source_refs,
                salience=0.45,
                importance=0.45,
                confidence_extraction=0.7,
                confidence_validity=0.55,
                metadata={"entity_name": canonical},
            )
            await _write_relationship_link(
                db=db,
                tenant_id=tenant_id,
                user_id=user_id,
                source_type="entity",
                source_id=canonical_norm,
                target_type="session",
                target_id=session_id,
                relationship_type="mentioned_in",
                source_session_ids=[session_id],
                source_turn_refs=_source_turn_refs(session_id=session_id, messages=messages, text_hint=canonical),
                confidence=0.7,
                metadata={"entity_name": canonical},
            )
            if bool(s.derived_pipeline_llm_enabled):
                profile_row = await db.fetchone(
                    """
                    SELECT open_questions
                    FROM entity_profiles
                    WHERE user_id=$1 AND canonical_name_normalized=$2
                    """,
                    user_id,
                    canonical_norm,
                )
                for question in _text_list((profile_row or {}).get("open_questions"), limit=3):
                    await _write_low_confidence_item(
                        db=db,
                        tenant_id=tenant_id,
                        user_id=user_id,
                        surface="entity_profile",
                        statement_text=f"Open question about {canonical}: {question}",
                        question_text=question,
                        confidence=0.35,
                        source_session_ids=[session_id],
                        source_turn_refs=_source_turn_refs(session_id=session_id, messages=messages, text_hint=canonical),
                        run_id=run_id,
                        metadata={"entity_name": canonical},
                    )
        output_hash = stable_short_hash({"mentions": mentions}, length=24)
        await _complete_run(db, run_id, output_hash)
        await _update_checkpoint(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            pipeline_name=PASS1_5_ENTITIES,
            run_id=run_id,
            input_watermark=session_id,
            output_hash=output_hash,
        )
        return DerivedPassResult(run_id=run_id, pass_name=PASS1_5_ENTITIES, output_hash=output_hash)
    except Exception as exc:
        await _fail_run(db, run_id, "pass1_5_failed", str(exc))
        raise


async def run_pass3_threads(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    session_id: str,
    messages: Sequence[Dict[str, Any]],
    settings: Optional[Settings] = None,
) -> DerivedPassResult:
    s = settings or get_settings()
    classification = await db.fetchone(
        "SELECT raw_triage_output FROM session_classifications WHERE session_id=$1 AND user_id=$2",
        session_id,
        user_id,
    )
    raw = (classification or {}).get("raw_triage_output") if classification else {}
    thread_signals = _text_list((raw or {}).get("thread_signals"), limit=8) if isinstance(raw, dict) else []
    input_hash = _input_hash(
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=session_id,
        pass_name=PASS3_THREADS,
        messages=messages,
        extra={"thread_signals": thread_signals},
        settings=s,
    )
    run_id = await _start_run(
        db=db,
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=session_id,
        pass_name=PASS3_THREADS,
        input_hash=input_hash,
        input_watermark=session_id,
        settings=s,
    )
    existing = await _succeeded_run(db, run_id)
    if existing:
        return DerivedPassResult(run_id=run_id, pass_name=PASS3_THREADS, output_hash=str(existing["output_hash"]))
    try:
        classification_meta = raw if isinstance(raw, dict) else {}
        actions: Optional[List[Dict[str, Any]]] = None
        if bool(s.derived_pipeline_llm_enabled):
            existing_threads = await db.fetch(
                """
                SELECT thread_id, title, detail, status, priority, category,
                       lifecycle_state, follow_up_after, source_session_ids
                FROM open_threads
                WHERE user_id=$1 AND status IN ('open','snoozed')
                ORDER BY last_updated_at DESC NULLS LAST, created_at DESC
                LIMIT 50
                """,
                user_id,
            )
            actions = await extract_thread_actions(
                messages=list(messages),
                session_date=_utcnow(),
                emotional_weight=normalize_text(classification_meta.get("emotional_weight")) or "none",
                emotional_note=normalize_text(classification_meta.get("emotional_note")) or None,
                thread_signals=thread_signals,
                existing_threads=existing_threads,
                model=s.derived_pipeline_model_version,
            )
        if not actions:
            actions = [
                {
                    "action": "CREATE",
                    "title": signal[:120],
                    "detail": signal,
                    "category": _semantic_category(signal, "thread_update"),
                    "priority": "medium",
                    "source_session_id": session_id,
                }
                for signal in thread_signals
            ]

        async def _resolve_prior_relationship_threads(
            *,
            action: Dict[str, Any],
            keep_thread_id: Optional[str] = None,
        ) -> int:
            related_entities = _thread_related_entity_set(action)
            title_text = normalize_text(action.get("title"))
            detail_text = normalize_text(action.get("detail"))
            rows = await db.fetch(
                """
                SELECT thread_id, title, detail, category, status, lifecycle_state,
                       related_entities, priority
                FROM open_threads
                WHERE user_id=$1
                  AND status='open'
                  AND category='relationship'
                ORDER BY last_updated_at DESC NULLS LAST, created_at DESC
                LIMIT 30
                """,
                user_id,
            )
            resolved_ids: List[str] = []
            for row in rows:
                thread_id = normalize_text(row.get("thread_id"))
                if not thread_id or thread_id == normalize_text(keep_thread_id):
                    continue
                candidate_entities = _thread_related_entity_set(dict(row))
                shares_entity = bool(related_entities and candidate_entities and related_entities & candidate_entities)
                same_topic = _thread_topic_key(
                    title=row.get("title"),
                    detail=row.get("detail"),
                    category=row.get("category"),
                    related_entities=row.get("related_entities"),
                ) == _thread_topic_key(
                    title=title_text,
                    detail=detail_text,
                    category="relationship",
                    related_entities=list(related_entities),
                )
                if not shares_entity and not same_topic:
                    continue
                if not _is_relationship_conflict_thread(dict(row)):
                    continue
                resolved_ids.append(thread_id)
            if not resolved_ids:
                return 0
            await db.execute(
                """
                UPDATE open_threads
                SET status='resolved',
                    lifecycle_state=CASE WHEN $3::text IS NULL THEN 'resolved' ELSE 'superseded' END,
                    superseded_by_thread_id=$3,
                    resolution_note=COALESCE(NULLIF($4,''), 'Resolved by newer relationship state.'),
                    resolved_at=NOW(),
                    last_updated_at=NOW()
                WHERE user_id=$1 AND thread_id = ANY($2::text[])
                """,
                user_id,
                resolved_ids,
                normalize_text(keep_thread_id) or None,
                normalize_text(action.get("resolution_note") or action.get("why_this_matters_later") or action.get("detail")),
            )
            return len(resolved_ids)

        for action in actions:
            kind = normalize_text(action.get("action")).upper()
            if kind in {"NO_ACTION", ""}:
                continue
            if kind in {"UPDATE", "RESOLVE", "SNOOZE"}:
                thread_id_existing = normalize_text(action.get("thread_id"))
                action_category = normalize_text(action.get("category")) or "other"
                action_priority = normalize_text(action.get("priority")) or "medium"
                if action_priority not in VALID_PRIORITIES:
                    action_priority = "medium"
                if not _thread_action_allowed(
                    action,
                    kind=kind,
                    category=action_category,
                    priority=action_priority,
                ):
                    continue
                current = await db.fetchone(
                    """
                    SELECT status, lifecycle_state
                    FROM open_threads
                    WHERE user_id=$1 AND thread_id=$2
                    """,
                    user_id,
                    thread_id_existing,
                )
                if not current or not _valid_thread_transition(
                    current.get("status"),
                    current.get("lifecycle_state"),
                    kind,
                ):
                    await _quarantine(
                        db=db,
                        tenant_id=tenant_id,
                        user_id=user_id,
                        pass_name=PASS3_THREADS,
                        session_id=session_id,
                        reason_code=QUARANTINE_INVALID_LIFECYCLE_TRANSITION,
                        payload={"action": action, "current": current},
                        run_id=run_id,
                    )
                    continue
                if kind == "RESOLVE":
                    await db.execute(
                        """
                        UPDATE open_threads
                        SET status='resolved',
                            lifecycle_state='resolved',
                            resolved_at=NOW(),
                            resolution_note=$3,
                            last_updated_at=NOW()
                        WHERE user_id=$1 AND thread_id=$2
                        """,
                        user_id,
                        thread_id_existing,
                        normalize_text(action.get("resolution_note")) or "Resolved from latest context.",
                    )
                    if _is_relationship_resolution_action(action, category=action_category):
                        await _resolve_prior_relationship_threads(action=action, keep_thread_id=thread_id_existing)
                    continue
                if kind == "SNOOZE":
                    await db.execute(
                        """
                        UPDATE open_threads
                        SET status='snoozed',
                            lifecycle_state='snoozed',
                            last_updated_at=NOW()
                        WHERE user_id=$1 AND thread_id=$2
                        """,
                        user_id,
                        thread_id_existing,
                    )
                    continue
                if kind == "UPDATE":
                    signal = normalize_text(action.get("detail")) or normalize_text(action.get("title"))
                    evidence_refs = _source_turn_refs(session_id=session_id, messages=messages, text_hint=signal)
                    if not evidence_refs:
                        await _quarantine(
                            db=db,
                            tenant_id=tenant_id,
                            user_id=user_id,
                            pass_name=PASS3_THREADS,
                            session_id=session_id,
                            reason_code=QUARANTINE_MISSING_EVIDENCE_REFS,
                            payload={"thread_update": signal, "thread_id": thread_id_existing},
                            run_id=run_id,
                        )
                        continue
                    await db.execute(
                        """
                        UPDATE open_threads
                        SET detail=COALESCE($3, detail),
                            status='open',
                            lifecycle_state='active',
                            source_session_ids=(
                                SELECT ARRAY(
                                  SELECT DISTINCT x
                                  FROM unnest(COALESCE(source_session_ids, '{}') || $4::text[]) AS x
                                  WHERE x IS NOT NULL AND x <> ''
                                )
                            ),
                            evidence_turn_refs=COALESCE(evidence_turn_refs, '[]'::jsonb) || $5::jsonb,
                            last_updated_at=NOW(),
                            last_mentioned_at=NOW()
                        WHERE user_id=$1 AND thread_id=$2
                        """,
                        user_id,
                        thread_id_existing,
                        signal or None,
                        [session_id],
                        evidence_refs,
                    )
                    await db.execute(
                        """
                        UPDATE open_threads
                        SET distinct_session_count=GREATEST(
                            COALESCE(distinct_session_count, 0),
                            COALESCE(cardinality(source_session_ids), 0)
                        )
                        WHERE user_id=$1 AND thread_id=$2
                        """,
                        user_id,
                        thread_id_existing,
                    )
                    await _write_assertion(
                        db=db,
                        tenant_id=tenant_id,
                        user_id=user_id,
                        pass_name=PASS3_THREADS,
                        surface="thread_update",
                        statement_text=signal or f"Thread {thread_id_existing} was updated.",
                        run_id=run_id,
                        source_session_ids=[session_id],
                        source_turn_refs=evidence_refs,
                        salience=0.68,
                        importance=0.65,
                        confidence_extraction=0.7,
                        confidence_validity=0.58,
                        metadata={"thread_id": thread_id_existing, "reactivated": current.get("status") == "snoozed"},
                    )
                    continue

            signal = normalize_text(action.get("detail")) or normalize_text(action.get("title"))
            title = normalize_text(action.get("title"))[:120] or signal[:120]
            if kind != "CREATE":
                continue
            category = _semantic_category(signal, "thread_update")
            if normalize_text(action.get("category")) in VALID_CATEGORIES:
                category = normalize_text(action.get("category"))
            priority = normalize_text(action.get("priority")) or "medium"
            if priority not in VALID_PRIORITIES:
                priority = "medium"
            related_entities = _text_list(action.get("related_entities"), limit=6)
            if _is_relationship_resolution_action(action, category=category):
                resolved_count = await _resolve_prior_relationship_threads(action=action)
                if resolved_count > 0:
                    continue
            if not _thread_action_allowed(action, kind=kind or "CREATE", category=category, priority=priority):
                continue
            memory_layer, retention_floor = _memory_layer_and_floor(category)
            evidence_refs = _source_turn_refs(session_id=session_id, messages=messages, text_hint=signal)
            if not evidence_refs:
                await _quarantine(
                    db=db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    pass_name=PASS3_THREADS,
                    session_id=session_id,
                    reason_code=QUARANTINE_MISSING_EVIDENCE_REFS,
                    payload={"thread_signal": signal},
                    run_id=run_id,
                )
                continue
            mismatch = await _thread_entity_mismatch(
                db=db,
                user_id=user_id,
                title=title,
                detail=signal,
                evidence_turn_refs=evidence_refs,
                related_entities=related_entities,
            )
            if mismatch:
                await _quarantine(
                    db=db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    pass_name=PASS3_THREADS,
                    session_id=session_id,
                    reason_code=QUARANTINE_PARSE_FAILURE,
                    payload={"thread_action": action, **mismatch},
                    run_id=run_id,
                )
                continue
            signal = await _strip_inferred_thread_detail_entities(
                db=db,
                user_id=user_id,
                title=title,
                detail=signal,
                evidence_turn_refs=evidence_refs,
                related_entities=related_entities,
            )
            existing_topic_thread = await _find_existing_thread_for_topic(
                db=db,
                user_id=user_id,
                title=title,
                detail=signal,
                category=category,
                related_entities=related_entities,
            )
            if existing_topic_thread:
                await db.execute(
                    """
                    UPDATE open_threads
                    SET detail=COALESCE($3, detail),
                        status='open',
                        lifecycle_state='active',
                        source_session_ids=(
                            SELECT ARRAY(
                              SELECT DISTINCT x
                              FROM unnest(COALESCE(source_session_ids, '{}') || $4::text[]) AS x
                              WHERE x IS NOT NULL AND x <> ''
                            )
                        ),
                        related_entities=(
                            SELECT ARRAY(
                              SELECT DISTINCT x
                              FROM unnest(COALESCE(related_entities, '{}') || $5::text[]) AS x
                              WHERE x IS NOT NULL AND x <> ''
                            )
                        ),
                        evidence_turn_refs=COALESCE(evidence_turn_refs, '[]'::jsonb) || $6::jsonb,
                        last_updated_at=NOW(),
                        last_mentioned_at=NOW()
                    WHERE user_id=$1 AND thread_id=$2
                    """,
                    user_id,
                    existing_topic_thread["thread_id"],
                    signal or None,
                    [session_id],
                    related_entities,
                    evidence_refs,
                )
                await db.execute(
                    """
                    UPDATE open_threads
                    SET distinct_session_count=GREATEST(
                        COALESCE(distinct_session_count, 0),
                        COALESCE(cardinality(source_session_ids), 0)
                    )
                    WHERE user_id=$1 AND thread_id=$2
                    """,
                    user_id,
                    existing_topic_thread["thread_id"],
                )
                await _write_assertion(
                    db=db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    pass_name=PASS3_THREADS,
                    surface="thread_update",
                    statement_text=signal,
                    run_id=run_id,
                    source_session_ids=[session_id],
                    source_turn_refs=evidence_refs,
                    salience=0.65,
                    importance=0.65,
                    confidence_extraction=0.7,
                    confidence_validity=0.58,
                    metadata={"thread_id": existing_topic_thread["thread_id"], "deduped_create": True},
                )
                continue
            resolved_topic_thread = await _find_existing_thread_for_topic(
                db=db,
                user_id=user_id,
                title=title,
                detail=signal,
                category=category,
                related_entities=related_entities,
                include_resolved=True,
            )
            if resolved_topic_thread and normalize_text(resolved_topic_thread.get("status")) == "resolved":
                await db.execute(
                    """
                    UPDATE open_threads
                    SET title=COALESCE(NULLIF($3,''), title),
                        detail=COALESCE($4, detail),
                        status='open',
                        lifecycle_state='active',
                        superseded_by_thread_id=NULL,
                        resolution_note=NULL,
                        resolved_at=NULL,
                        source_session_ids=(
                            SELECT ARRAY(
                              SELECT DISTINCT x
                              FROM unnest(COALESCE(source_session_ids, '{}') || $5::text[]) AS x
                              WHERE x IS NOT NULL AND x <> ''
                            )
                        ),
                        related_entities=(
                            SELECT ARRAY(
                              SELECT DISTINCT x
                              FROM unnest(COALESCE(related_entities, '{}') || $6::text[]) AS x
                              WHERE x IS NOT NULL AND x <> ''
                            )
                        ),
                        evidence_turn_refs=COALESCE(evidence_turn_refs, '[]'::jsonb) || $7::jsonb,
                        last_updated_at=NOW(),
                        last_mentioned_at=NOW()
                    WHERE user_id=$1 AND thread_id=$2
                    """,
                    user_id,
                    resolved_topic_thread["thread_id"],
                    title,
                    signal or None,
                    [session_id],
                    related_entities,
                    evidence_refs,
                )
                await db.execute(
                    """
                    UPDATE open_threads
                    SET distinct_session_count=GREATEST(
                        COALESCE(distinct_session_count, 0),
                        COALESCE(cardinality(source_session_ids), 0)
                    )
                    WHERE user_id=$1 AND thread_id=$2
                    """,
                    user_id,
                    resolved_topic_thread["thread_id"],
                )
                await _write_assertion(
                    db=db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    pass_name=PASS3_THREADS,
                    surface="thread_update",
                    statement_text=signal,
                    run_id=run_id,
                    source_session_ids=[session_id],
                    source_turn_refs=evidence_refs,
                    salience=0.68,
                    importance=0.68,
                    confidence_extraction=0.72,
                    confidence_validity=0.6,
                    metadata={"thread_id": resolved_topic_thread["thread_id"], "reactivated": True},
                )
                continue
            thread_id = stable_short_hash({"user_id": user_id, "thread": normalize_text(title)}, length=20)
            await db.execute(
                """
                INSERT INTO open_threads (
                    thread_id, user_id, title, detail, status, priority, category,
                    related_entities, source_session_ids, first_seen_at, last_updated_at, last_mentioned_at,
                    lifecycle_state, evidence_turn_refs, confidence_extraction,
                    confidence_validity, memory_layer, semantic_category, retention_floor
                )
                VALUES (
                    $1,$2,$3,$4,'open',$12,$5,$6::text[],$7::text[],NOW(),NOW(),NOW(),
                    'active',$8::jsonb,0.7,0.55,$9,$10,$11
                )
                ON CONFLICT (thread_id)
                DO UPDATE SET
                    detail=COALESCE(open_threads.detail, EXCLUDED.detail),
                    status=CASE WHEN open_threads.status='resolved' THEN open_threads.status ELSE 'open' END,
                    source_session_ids=(
                        SELECT ARRAY(
                            SELECT DISTINCT x
                            FROM unnest(COALESCE(open_threads.source_session_ids, '{}') || EXCLUDED.source_session_ids) AS x
                            WHERE x IS NOT NULL AND x <> ''
                        )
                    ),
                    related_entities=(
                        SELECT ARRAY(
                            SELECT DISTINCT x
                            FROM unnest(COALESCE(open_threads.related_entities, '{}') || EXCLUDED.related_entities) AS x
                            WHERE x IS NOT NULL AND x <> ''
                        )
                    ),
                    last_updated_at=NOW(),
                    last_mentioned_at=NOW(),
                    lifecycle_state=CASE
                        WHEN open_threads.lifecycle_state='resolved' THEN open_threads.lifecycle_state
                        ELSE 'active'
                    END,
                    evidence_turn_refs=COALESCE(open_threads.evidence_turn_refs, '[]'::jsonb) || EXCLUDED.evidence_turn_refs,
                    confidence_extraction=GREATEST(COALESCE(open_threads.confidence_extraction, 0), EXCLUDED.confidence_extraction),
                    confidence_validity=GREATEST(COALESCE(open_threads.confidence_validity, 0), EXCLUDED.confidence_validity),
                    memory_layer=COALESCE(open_threads.memory_layer, EXCLUDED.memory_layer),
                    semantic_category=COALESCE(open_threads.semantic_category, EXCLUDED.semantic_category),
                    retention_floor=GREATEST(COALESCE(open_threads.retention_floor, 0), EXCLUDED.retention_floor)
                """,
                thread_id,
                user_id,
                title,
                signal,
                category,
                related_entities,
                [session_id],
                evidence_refs,
                memory_layer,
                category,
                retention_floor,
                priority,
            )
            await db.execute(
                """
                UPDATE open_threads
                SET distinct_session_count=GREATEST(
                    COALESCE(distinct_session_count, 0),
                    COALESCE(cardinality(source_session_ids), 0)
                )
                WHERE user_id=$1 AND thread_id=$2
                """,
                user_id,
                thread_id,
            )
            await _write_assertion(
                db=db,
                tenant_id=tenant_id,
                user_id=user_id,
                pass_name=PASS3_THREADS,
                surface="thread_update",
                statement_text=signal,
                run_id=run_id,
                source_session_ids=[session_id],
                source_turn_refs=evidence_refs,
                salience=0.65,
                importance=0.65,
                confidence_extraction=0.7,
                confidence_validity=0.55,
                metadata={"thread_id": thread_id},
            )
            for related in related_entities:
                await _write_relationship_link(
                    db=db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    source_type="thread",
                    source_id=thread_id,
                    target_type="entity",
                    target_id=_canonical_name_norm(related),
                    relationship_type="involves",
                    source_session_ids=[session_id],
                    source_turn_refs=evidence_refs,
                    confidence=0.6,
                    metadata={"entity_name": related},
                )
        output_hash = stable_short_hash({"thread_signals": thread_signals}, length=24)
        await _complete_run(db, run_id, output_hash)
        await _update_checkpoint(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            pipeline_name=PASS3_THREADS,
            run_id=run_id,
            input_watermark=session_id,
            output_hash=output_hash,
        )
        return DerivedPassResult(run_id=run_id, pass_name=PASS3_THREADS, output_hash=output_hash)
    except Exception as exc:
        await _fail_run(db, run_id, "pass3_failed", str(exc))
        raise


async def _recent_classifications(
    db: Database,
    *,
    user_id: str,
    identity_only: bool = False,
    context_only: bool = False,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    clauses = ["user_id=$1"]
    if identity_only:
        clauses.append("identity_relevant IS TRUE")
    if context_only:
        clauses.append("context_relevant IS TRUE")
    where = " AND ".join(clauses)
    return await db.fetch(
        f"""
        SELECT session_id, session_date, raw_triage_output, tension_signal
        FROM session_classifications
        WHERE {where}
        ORDER BY session_date DESC NULLS LAST, processed_at DESC NULLS LAST
        LIMIT {int(limit)}
        """,
        user_id,
    )


async def _living_context_session_window(db: Database, *, user_id: str) -> List[Dict[str, Any]]:
    async def fetch_window(days: int) -> List[Dict[str, Any]]:
        return await db.fetch(
            """
            SELECT session_id, session_date, raw_triage_output, tension_signal
            FROM session_classifications
            WHERE user_id=$1
              AND is_memory_worthy IS TRUE
              AND session_date >= NOW() - ($2::text || ' days')::interval
            ORDER BY context_relevant DESC NULLS LAST,
                     session_date DESC NULLS LAST,
                     processed_at DESC NULLS LAST
            LIMIT 30
            """,
            user_id,
            str(days),
        )

    rows = await fetch_window(30)
    if len(rows) < 3:
        rows = await fetch_window(60)
    return rows[:30]


def _packet_evidence_strength(source_session_ids: Any, distinct_session_count: Any = None) -> str:
    try:
        count = int(distinct_session_count or 0)
    except Exception:
        count = 0
    if count <= 0 and isinstance(source_session_ids, list):
        count = len(set(str(x) for x in source_session_ids if x))
    if count >= 3:
        return "strong"
    if count >= 1:
        return "medium"
    return "weak"


def _packet_importance(row: Dict[str, Any]) -> float:
    values = []
    for key in ("importance_score", "salience_score", "confidence", "confidence_validity", "confidence_extraction"):
        try:
            values.append(float(row.get(key) or 0.0))
        except Exception:
            pass
    if _relationship_rank(row.get("relationship_to_user")) >= 85:
        values.append(0.95)
    if normalize_text(row.get("priority")) == "high":
        values.append(0.9)
    return max(values or [0.0])


def _with_packet_reason(row: Dict[str, Any], *, why: str) -> Dict[str, Any]:
    out = dict(row)
    evidence_strength = _packet_evidence_strength(out.get("source_session_ids"), out.get("distinct_session_count"))
    out["why_included"] = why
    out["evidence_strength"] = evidence_strength
    out["importance_score"] = max(float(out.get("importance_score") or 0.0), _packet_importance(out))
    out["evidence_refs"] = out.get("evidence_turn_refs") or out.get("source_turn_refs") or out.get("source_session_ids") or []
    return out


async def build_pass4_identity_packet(
    *,
    db: Database,
    user_id: str,
    rows: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    ranked_rows = _rank_and_prune_sessions([dict(row) for row in rows], limit=30)
    persistent_goals_raw = await db.fetch(
        """
        SELECT thread_id, title, detail, source_session_ids, first_seen_at,
               last_mentioned_at, distinct_session_count, salience_score,
               importance_score, evidence_turn_refs
        FROM open_threads
        WHERE user_id=$1
          AND thread_type='persistent_goal'
          AND COALESCE(cardinality(source_session_ids), 0) > 0
        ORDER BY distinct_session_count DESC NULLS LAST,
                 importance_score DESC NULLS LAST,
                 last_mentioned_at DESC NULLS LAST
        LIMIT 20
        """,
        user_id,
    )
    durable_anchors_raw = await db.fetch(
        """
        SELECT canonical_name, canonical_name_normalized, type, relationship_to_user,
               profile_text, key_facts, open_questions, last_known_status,
               source_session_ids, first_seen_at, last_seen_at, distinct_session_count,
               salience_score, importance_score
        FROM entity_profiles
        WHERE user_id=$1
          AND status='active'
          AND relationship_to_user IN ('daughter','son','child','girlfriend','boyfriend','partner','spouse','mother','father','parent')
          AND COALESCE(cardinality(source_session_ids), 0) > 0
        ORDER BY importance_score DESC NULLS LAST, last_seen_at DESC NULLS LAST
        LIMIT 12
        """,
        user_id,
    )
    persistent_goals = [
        _with_packet_reason(dict(row), why="persistent_goal_with_evidence")
        for row in persistent_goals_raw
        if _packet_evidence_strength(row.get("source_session_ids"), row.get("distinct_session_count")) != "weak"
    ]
    durable_anchors = [
        _with_packet_reason(dict(row), why="durable_relationship_anchor")
        for row in durable_anchors_raw
        if _has_entity_serving_content(dict(row))
    ]
    return {
        "recent_identity_sessions": ranked_rows,
        "persistent_goals": persistent_goals,
        "durable_anchors": durable_anchors,
        "dropped": [],
    }


async def build_pass5_living_packet(
    *,
    db: Database,
    user_id: str,
    rows: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    ranked_rows = _rank_and_prune_sessions([dict(row) for row in rows], limit=20)
    open_thread_rows = await db.fetch(
        """
        SELECT thread_id, title, detail, priority, category, thread_type,
               related_entities,
               source_session_ids, evidence_turn_refs, last_mentioned_at,
               follow_up_after, salience_score, importance_score,
               distinct_session_count
        FROM open_threads
        WHERE user_id=$1
          AND status='open'
          AND COALESCE(cardinality(source_session_ids), 0) > 0
        ORDER BY salience_score DESC NULLS LAST, last_mentioned_at DESC NULLS LAST
        LIMIT 40
        """,
        user_id,
    )
    entity_rows = await db.fetch(
        """
        SELECT canonical_name, canonical_name_normalized,
               CASE
                 WHEN replace(lower(COALESCE(relationship_to_user, '')), ' ', '_') IN (
                   'active_project','user_project','owned_project','core_project','primary_project'
                 ) THEN 'project'
                 ELSE type
               END AS type,
               relationship_to_user,
               LEFT(profile_text, 200) AS profile_text,
               key_facts, open_questions, last_known_status, salience_score,
               importance_score, last_seen_at, source_session_ids,
               distinct_session_count
        FROM entity_profiles
        WHERE user_id=$1
          AND status='active'
          AND COALESCE(cardinality(source_session_ids), 0) > 0
          AND (
            last_seen_at >= NOW() - interval '30 days'
            OR relationship_to_user IN ('daughter','son','child','girlfriend','boyfriend','partner','spouse','mother','father','parent')
          )
        ORDER BY salience_score DESC NULLS LAST, last_seen_at DESC NULLS LAST
        LIMIT 60
        """,
        user_id,
    )
    contradictions = await db.fetch(
        """
        SELECT topic, earlier_view, recent_view, source_session_ids, source_turn_refs,
               first_seen_at, last_seen_at
        FROM memory_contradictions
        WHERE user_id=$1 AND status='active'
        ORDER BY last_seen_at DESC
        LIMIT 5
        """,
        user_id,
    )
    low_confidence = await db.fetch(
        """
        SELECT surface, statement_text, question_text, confidence, source_session_ids,
               source_turn_refs, first_seen_at, last_seen_at
        FROM low_confidence_items
        WHERE user_id=$1 AND status='open'
        ORDER BY confidence ASC NULLS FIRST, last_seen_at DESC
        LIMIT 8
        """,
        user_id,
    )
    threads: List[Dict[str, Any]] = []
    dropped: List[Dict[str, Any]] = []
    for row in _rank_and_prune_threads([dict(row) for row in open_thread_rows], limit=12):
        mismatch = await _thread_entity_mismatch(
            db=db,
            user_id=user_id,
            title=row.get("title"),
            detail=row.get("detail"),
            evidence_turn_refs=row.get("evidence_turn_refs"),
            related_entities=row.get("related_entities"),
        )
        if mismatch:
            dropped.append({
                "surface": "open_thread",
                "thread_id": row.get("thread_id"),
                "title": row.get("title"),
                **mismatch,
            })
            continue
        row["detail"] = await _strip_inferred_thread_detail_entities(
            db=db,
            user_id=user_id,
            title=row.get("title"),
            detail=row.get("detail"),
            evidence_turn_refs=row.get("evidence_turn_refs"),
            related_entities=row.get("related_entities"),
        )
        threads.append(_with_packet_reason(row, why="high_signal_active_thread"))
        if len(threads) >= 5:
            break
    entities = [
        _with_packet_reason(row, why="active_or_durable_entity")
        for row in _rank_and_prune_entities([dict(row) for row in entity_rows], limit=5)
    ]
    contradiction_items = [
        _with_packet_reason(dict(row), why="active_contradiction")
        for row in contradictions
        if row.get("source_session_ids")
    ]
    low_confidence_items: List[Dict[str, Any]] = []
    seen_low_confidence: set[str] = set()
    for row in low_confidence:
        question = row.get("question_text") or row.get("statement_text")
        key = normalize_text(question).casefold()
        if not key or key in seen_low_confidence:
            continue
        if not _meaningful_low_confidence_question(question) or not row.get("source_session_ids"):
            continue
        low_confidence_items.append(_with_packet_reason(dict(row), why="behaviorally_relevant_uncertainty"))
        seen_low_confidence.add(key)
        if len(low_confidence_items) >= 3:
            break
    return {
        "recent_sessions": ranked_rows,
        "active_threads": threads,
        "key_entities": entities,
        "contradictions": contradiction_items,
        "transitions": [],
        "low_confidence": low_confidence_items,
        "dropped": dropped,
    }


async def run_pass4_identity(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    settings: Optional[Settings] = None,
) -> DerivedPassResult:
    s = settings or get_settings()
    rows = await _recent_classifications(db, user_id=user_id, identity_only=True, limit=80)
    rows = [
        row
        for row in rows
        if _text_list(((row.get("raw_triage_output") if isinstance(row.get("raw_triage_output"), dict) else {}) or {}).get("identity_signals"), limit=8)
    ]
    identity_packet = await build_pass4_identity_packet(db=db, user_id=user_id, rows=rows)
    rows = identity_packet["recent_identity_sessions"]
    input_hash = _input_hash(
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=None,
        pass_name=PASS4_IDENTITY,
        extra={"sessions": [r.get("session_id") for r in rows]},
        settings=s,
    )
    run_id = await _start_run(
        db=db,
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=None,
        pass_name=PASS4_IDENTITY,
        input_hash=input_hash,
        input_watermark=rows[0].get("session_id") if rows else None,
        settings=s,
    )
    existing = await _succeeded_run(db, run_id)
    if existing:
        return DerivedPassResult(run_id=run_id, pass_name=PASS4_IDENTITY, output_hash=str(existing["output_hash"]))
    try:
        assertions: List[Dict[str, Any]] = []
        for row in rows:
            raw = row.get("raw_triage_output") if isinstance(row.get("raw_triage_output"), dict) else {}
            for signal in _text_list(raw.get("identity_signals"), limit=8):
                refs = await _source_turn_refs_from_session(db=db, session_id=row["session_id"], text_hint=signal)
                assertion_id = await _write_assertion(
                    db=db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    pass_name=PASS4_IDENTITY,
                    surface="identity_trait",
                    statement_text=signal,
                    run_id=run_id,
                    source_session_ids=[row["session_id"]],
                    source_turn_refs=refs,
                    salience=0.75,
                    importance=0.8,
                    confidence_extraction=0.72,
                    confidence_validity=0.6,
                )
                if assertion_id:
                    assertions.append(
                        {
                            "assertion_id": assertion_id,
                            "statement_text": signal,
                            "source_session_ids": [row["session_id"]],
                        }
                    )
                reason_code = _low_confidence_reason(signal, "identity_trait")
                if reason_code:
                    await _write_low_confidence_item(
                        db=db,
                        tenant_id=tenant_id,
                        user_id=user_id,
                        surface="identity_trait",
                        statement_text=signal,
                        question_text=_low_confidence_question(reason_code, signal),
                        confidence=0.42,
                        source_session_ids=[row["session_id"]],
                        source_turn_refs=refs,
                        run_id=run_id,
                        metadata={"source": "pass4.identity_signal", "reason_code": reason_code},
                    )
        rich_output: Optional[Dict[str, Any]] = None
        if bool(s.derived_pipeline_llm_enabled) and rows:
            existing_profile = await db.fetchone(
                "SELECT * FROM identity_profile WHERE user_id=$1",
                user_id,
            )
            persistent_goals = [
                *identity_packet.get("persistent_goals", []),
                *identity_packet.get("durable_anchors", []),
            ]
            parsed = await synthesize_identity_profile(
                existing_profile=existing_profile,
                session_rows=list(reversed(rows)),
                persistent_goals=persistent_goals,
                model=s.derived_pipeline_model_version,
            )
            if parsed:
                rich_output = normalize_identity_output(parsed)
        output_hash = stable_short_hash({"assertions": assertions, "rich": rich_output or {}}, length=24)
        if rich_output:
            await db.execute(
                """
                INSERT INTO identity_profile (
                  user_id, who_they_are, core_values, recurring_patterns,
                  family_history, faith_and_beliefs, what_they_want,
                  recurring_fears, what_they_avoid, how_they_relate,
                  persistent_goals, current_chapter, assertions,
                  last_synthesized_at, source_session_count, synthesis_model,
                  created_at, updated_at
                ) VALUES (
                  $1,$2,$3::jsonb,$4::jsonb,$5,$6,$7,
                  $8::jsonb,$9,$10,$11::jsonb,$12,$13::jsonb,
                  NOW(),$14,$15,NOW(),NOW()
                )
                ON CONFLICT (user_id) DO UPDATE SET
                  who_they_are=EXCLUDED.who_they_are,
                  core_values=EXCLUDED.core_values,
                  recurring_patterns=EXCLUDED.recurring_patterns,
                  family_history=EXCLUDED.family_history,
                  faith_and_beliefs=EXCLUDED.faith_and_beliefs,
                  what_they_want=EXCLUDED.what_they_want,
                  recurring_fears=EXCLUDED.recurring_fears,
                  what_they_avoid=EXCLUDED.what_they_avoid,
                  how_they_relate=EXCLUDED.how_they_relate,
                  persistent_goals=EXCLUDED.persistent_goals,
                  current_chapter=EXCLUDED.current_chapter,
                  assertions=EXCLUDED.assertions,
                  last_synthesized_at=NOW(),
                  source_session_count=EXCLUDED.source_session_count,
                  synthesis_model=EXCLUDED.synthesis_model,
                  updated_at=NOW()
                """,
                user_id,
                rich_output["who_they_are"],
                rich_output["core_values"],
                rich_output["recurring_patterns"],
                rich_output["family_history"],
                rich_output["faith_and_beliefs"],
                rich_output["what_they_want"],
                rich_output["recurring_fears"],
                rich_output["what_they_avoid"],
                rich_output["how_they_relate"],
                rich_output["persistent_goals"],
                rich_output["current_chapter"],
                assertions,
                len(rows),
                s.derived_pipeline_model_version,
            )
        else:
            await db.execute(
                """
                INSERT INTO identity_profile (
                    user_id, assertions, last_synthesized_at, source_session_count,
                    synthesis_model, created_at, updated_at
                )
                VALUES ($1,$2::jsonb,NOW(),$3,$4,NOW(),NOW())
                ON CONFLICT (user_id)
                DO UPDATE SET
                    assertions=$2::jsonb,
                    last_synthesized_at=NOW(),
                    source_session_count=$3,
                    synthesis_model=$4,
                    updated_at=NOW()
                """,
                user_id,
                assertions,
                len(rows),
                s.derived_pipeline_model_version,
            )
        await _complete_run(db, run_id, output_hash)
        await _update_checkpoint(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            pipeline_name=PASS4_IDENTITY,
            run_id=run_id,
            input_watermark=rows[0].get("session_id") if rows else None,
            output_hash=output_hash,
            reset_identity=True,
        )
        return DerivedPassResult(run_id=run_id, pass_name=PASS4_IDENTITY, output_hash=output_hash)
    except Exception as exc:
        await _fail_run(db, run_id, "pass4_failed", str(exc))
        raise


async def run_pass5_living_context(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    settings: Optional[Settings] = None,
) -> DerivedPassResult:
    s = settings or get_settings()
    rows = _rank_and_prune_sessions(await _living_context_session_window(db, user_id=user_id), limit=20)
    living_packet = await build_pass5_living_packet(db=db, user_id=user_id, rows=rows)
    rows = living_packet["recent_sessions"]
    input_hash = _input_hash(
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=None,
        pass_name=PASS5_LIVING_CONTEXT,
        extra={"sessions": [r.get("session_id") for r in rows]},
        settings=s,
    )
    run_id = await _start_run(
        db=db,
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=None,
        pass_name=PASS5_LIVING_CONTEXT,
        input_hash=input_hash,
        input_watermark=rows[0].get("session_id") if rows else None,
        settings=s,
    )
    existing = await _succeeded_run(db, run_id)
    if existing:
        return DerivedPassResult(run_id=run_id, pass_name=PASS5_LIVING_CONTEXT, output_hash=str(existing["output_hash"]))
    try:
        assertions: List[Dict[str, Any]] = []
        for row in rows:
            raw = row.get("raw_triage_output") if isinstance(row.get("raw_triage_output"), dict) else {}
            statements = [s for s in _text_list(raw.get("memory_deltas"), limit=4) if not _is_greeting_or_presence_check(s)]
            statements.extend(_text_list(raw.get("thread_signals"), limit=4))
            tension = normalize_text(row.get("tension_signal"), casefold=False)
            if tension:
                statements.append(tension)
            for statement in statements[:10]:
                refs = await _source_turn_refs_from_session(db=db, session_id=row["session_id"], text_hint=statement)
                assertion_id = await _write_assertion(
                    db=db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    pass_name=PASS5_LIVING_CONTEXT,
                    surface="living_context_statement",
                    statement_text=statement,
                    run_id=run_id,
                    source_session_ids=[row["session_id"]],
                    source_turn_refs=refs,
                    salience=0.7,
                    importance=0.65,
                    confidence_extraction=0.72,
                    confidence_validity=0.58,
                )
                if assertion_id:
                    assertions.append(
                        {
                            "assertion_id": assertion_id,
                            "statement_text": statement,
                            "source_session_ids": [row["session_id"]],
                        }
                    )
                reason_code = _low_confidence_reason(statement, "living_context_statement")
                if reason_code:
                    await _write_low_confidence_item(
                        db=db,
                        tenant_id=tenant_id,
                        user_id=user_id,
                        surface="living_context_statement",
                        statement_text=statement,
                        question_text=_low_confidence_question(reason_code, statement),
                        confidence=0.42,
                        source_session_ids=[row["session_id"]],
                        source_turn_refs=refs,
                        run_id=run_id,
                        metadata={"source": "pass5.context_statement", "reason_code": reason_code},
                    )
        rich_output: Optional[Dict[str, Any]] = None
        if bool(s.derived_pipeline_llm_enabled) and rows:
            existing_context = await db.fetchone("SELECT * FROM living_context WHERE user_id=$1", user_id)
            identity_grounding = await db.fetchone("SELECT * FROM identity_profile WHERE user_id=$1", user_id)
            open_threads = living_packet["active_threads"]
            active_entities = living_packet["key_entities"]
            parsed = await synthesize_living_context(
                existing_context=existing_context,
                identity_grounding=identity_grounding,
                recent_sessions=list(reversed(rows)),
                open_threads=open_threads,
                active_entities=active_entities,
                model=s.derived_pipeline_model_version,
            )
            if parsed:
                rich_output = normalize_living_context_output(parsed)
        output_hash = stable_short_hash({"assertions": assertions, "rich": rich_output or {}}, length=24)
        if rich_output:
            source_session_ids = [r["session_id"] for r in rows if r.get("session_id")]
            source_turn_refs = []
            for sid in source_session_ids[:5]:
                source_turn_refs.extend(await _source_turn_refs_from_session(db=db, session_id=sid, max_refs=2))
            for contradiction in rich_output.get("active_contradictions") or []:
                if not isinstance(contradiction, dict):
                    continue
                await _write_memory_contradiction(
                    db=db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    topic=contradiction.get("topic") or "current tension",
                    earlier_view=contradiction.get("earlier_view") or contradiction.get("earlier") or "",
                    recent_view=contradiction.get("recent_view") or contradiction.get("recent") or contradiction.get("note") or "",
                    source_session_ids=source_session_ids,
                    source_turn_refs=source_turn_refs,
                    run_id=run_id,
                    metadata={"source": "pass5.active_contradictions", "payload": contradiction},
                )
            for field_name in ("current_focus", "primary_tension", "emotional_texture", "relationship_pulse"):
                value = rich_output.get(field_name)
                reason_code = _low_confidence_reason(value or "", f"living_context.{field_name}")
                if reason_code:
                    await _write_low_confidence_item(
                        db=db,
                        tenant_id=tenant_id,
                        user_id=user_id,
                        surface=f"living_context.{field_name}",
                        statement_text=value,
                        question_text=_low_confidence_question(reason_code, value),
                        confidence=0.42,
                        source_session_ids=source_session_ids,
                        source_turn_refs=source_turn_refs,
                        run_id=run_id,
                        metadata={"source": "pass5.rich_output", "field": field_name, "reason_code": reason_code},
                    )
            await db.execute(
                """
                INSERT INTO living_context (
                  user_id, current_focus, recent_narrative,
                  relationship_pulse, emotional_texture,
                  primary_tension, what_theyre_avoiding,
                  unspoken_goal, why_it_matters,
                  active_contradictions, sophie_directives,
                  assertions, last_synthesized_at, sessions_since_last,
                  source_session_count, synthesis_model, created_at, updated_at
                ) VALUES (
                  $1,$2,$3,$4,$5,$6,$7,$8,$9,
                  $10::jsonb,$11::jsonb,$12::jsonb,NOW(),0,$13,$14,NOW(),NOW()
                )
                ON CONFLICT (user_id) DO UPDATE SET
                  current_focus=EXCLUDED.current_focus,
                  recent_narrative=EXCLUDED.recent_narrative,
                  relationship_pulse=EXCLUDED.relationship_pulse,
                  emotional_texture=EXCLUDED.emotional_texture,
                  primary_tension=EXCLUDED.primary_tension,
                  what_theyre_avoiding=EXCLUDED.what_theyre_avoiding,
                  unspoken_goal=EXCLUDED.unspoken_goal,
                  why_it_matters=EXCLUDED.why_it_matters,
                  active_contradictions=EXCLUDED.active_contradictions,
                  sophie_directives=EXCLUDED.sophie_directives,
                  assertions=EXCLUDED.assertions,
                  last_synthesized_at=NOW(),
                  sessions_since_last=0,
                  source_session_count=EXCLUDED.source_session_count,
                  synthesis_model=EXCLUDED.synthesis_model,
                  updated_at=NOW()
                """,
                user_id,
                rich_output["current_focus"],
                rich_output["recent_narrative"],
                rich_output["relationship_pulse"],
                rich_output["emotional_texture"],
                rich_output["primary_tension"],
                rich_output["what_theyre_avoiding"],
                rich_output["unspoken_goal"],
                rich_output["why_it_matters"],
                rich_output["active_contradictions"],
                rich_output["sophie_directives"],
                assertions,
                len(rows),
                s.derived_pipeline_model_version,
            )
        else:
            await db.execute(
                """
                INSERT INTO living_context (
                    user_id, assertions, last_synthesized_at, sessions_since_last,
                    source_session_count, synthesis_model, created_at, updated_at
                )
                VALUES ($1,$2::jsonb,NOW(),0,$3,$4,NOW(),NOW())
                ON CONFLICT (user_id)
                DO UPDATE SET
                    assertions=$2::jsonb,
                    last_synthesized_at=NOW(),
                    sessions_since_last=0,
                    source_session_count=$3,
                    synthesis_model=$4,
                    updated_at=NOW()
                """,
                user_id,
                assertions,
                len(rows),
                s.derived_pipeline_model_version,
            )
        await _complete_run(db, run_id, output_hash)
        await _update_checkpoint(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            pipeline_name=PASS5_LIVING_CONTEXT,
            run_id=run_id,
            input_watermark=rows[0].get("session_id") if rows else None,
            output_hash=output_hash,
            reset_context=True,
        )
        return DerivedPassResult(run_id=run_id, pass_name=PASS5_LIVING_CONTEXT, output_hash=output_hash)
    except Exception as exc:
        await _fail_run(db, run_id, "pass5_failed", str(exc))
        raise


async def bump_memory_access(
    *,
    db: Database,
    user_id: str,
    tenant_id: str = "default",
    surfaces: Optional[Iterable[str]] = None,
) -> None:
    surface_list = list(surfaces or [])
    if surface_list:
        await db.execute(
            """
            UPDATE derived_assertions
            SET last_accessed_at=NOW(), access_count=access_count + 1, updated_at=NOW()
            WHERE tenant_id=$1 AND user_id=$2 AND surface = ANY($3::text[])
              AND lifecycle_state='active'
            """,
            tenant_id,
            user_id,
            surface_list,
        )
    await db.execute(
        """
        UPDATE open_threads
        SET last_accessed_at=NOW(), access_count=access_count + 1
        WHERE user_id=$1 AND status='open'
        """,
        user_id,
    )
    await db.execute(
        """
        UPDATE entity_profiles
        SET last_accessed_at=NOW(), access_count=access_count + 1
        WHERE user_id=$1 AND status IN ('active','tentative')
        """,
        user_id,
    )


async def run_silence_detection(*, db: Database, tenant_id: str = "default") -> int:
    """Daily meaning-memory silence detector.

    Writes one active flag per high-salience entity/thread that has gone quiet.
    It does not notify users and does not mutate source memory state.
    """
    inserted = 0
    entity_rows = await db.fetch(
        """
        SELECT user_id,
               canonical_name_normalized AS target_id,
               canonical_name AS target_name,
               type AS entity_type,
               relationship_to_user,
               profile_text,
               last_known_status,
               last_seen_at,
               COALESCE(salience_score, 0) AS salience,
               COALESCE(importance_score, 0) AS importance
        FROM entity_profiles
        WHERE status='active'
          AND last_seen_at IS NOT NULL
          AND COALESCE(salience_score, 0) >= 0.65
          AND last_seen_at < NOW() - ($1::text || ' days')::interval
        LIMIT 500
        """,
        str(SILENCE_ENTITY_THRESHOLD_DAYS),
    )
    for row in entity_rows:
        if not _entity_silence_eligible(row):
            continue
        result = await db.execute(
            """
            INSERT INTO memory_silence_flags (
              tenant_id, user_id, target_type, target_id, target_name,
              last_seen_at, silence_days, status, source, metadata, created_at, updated_at
            )
            VALUES (
              $1,$2,'entity',$3,$4,$5,
              GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (NOW() - $5)) / 86400))::int,
              'active','daily_silence_detection',$6::jsonb,NOW(),NOW()
            )
            ON CONFLICT (tenant_id, user_id, target_type, target_id, status)
            DO UPDATE SET
              last_seen_at=EXCLUDED.last_seen_at,
              silence_days=EXCLUDED.silence_days,
              updated_at=NOW()
            """,
            tenant_id,
            row["user_id"],
            row["target_id"],
            row["target_name"],
            row["last_seen_at"],
            {
                "salience": row.get("salience"),
                "importance": row.get("importance"),
                "entity_type": row.get("entity_type"),
                "relationship_to_user": row.get("relationship_to_user"),
            },
        )
        if result.startswith("INSERT"):
            inserted += 1
    thread_rows = await db.fetch(
        """
        SELECT user_id,
               thread_id AS target_id,
               title AS target_name,
               detail,
               category,
               priority,
               thread_type,
               last_mentioned_at AS last_seen_at,
               COALESCE(salience_score, 0) AS salience,
               COALESCE(importance_score, 0) AS importance
        FROM open_threads
        WHERE status='open'
          AND last_mentioned_at IS NOT NULL
          AND COALESCE(salience_score, 0) >= 0.65
          AND last_mentioned_at < NOW() - ($1::text || ' days')::interval
        LIMIT 500
        """,
        str(SILENCE_THREAD_THRESHOLD_DAYS),
    )
    for row in thread_rows:
        if not _thread_silence_eligible(row):
            continue
        result = await db.execute(
            """
            INSERT INTO memory_silence_flags (
              tenant_id, user_id, target_type, target_id, target_name,
              last_seen_at, silence_days, status, source, metadata, created_at, updated_at
            )
            VALUES (
              $1,$2,'thread',$3,$4,$5,
              GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (NOW() - $5)) / 86400))::int,
              'active','daily_silence_detection',$6::jsonb,NOW(),NOW()
            )
            ON CONFLICT (tenant_id, user_id, target_type, target_id, status)
            DO UPDATE SET
              last_seen_at=EXCLUDED.last_seen_at,
              silence_days=EXCLUDED.silence_days,
              updated_at=NOW()
            """,
            tenant_id,
            row["user_id"],
            row["target_id"],
            row["target_name"],
            row["last_seen_at"],
            {
                "salience": row.get("salience"),
                "importance": row.get("importance"),
                "category": row.get("category"),
                "thread_type": row.get("thread_type"),
            },
        )
        if result.startswith("INSERT"):
            inserted += 1
    return inserted


async def run_thread_audit(
    *,
    db: Database,
    tenant_id: str = "default",
    user_id: Optional[str] = None,
    settings: Optional[Settings] = None,
) -> Dict[str, int]:
    s = settings or get_settings()
    users: List[str]
    if user_id:
        users = [user_id]
    else:
        rows = await db.fetch(
            """
            SELECT DISTINCT user_id
            FROM open_threads
            WHERE status IN ('open','snoozed')
            LIMIT 200
            """
        )
        users = [row["user_id"] for row in rows if row.get("user_id")]
    summary = {"merged": 0, "resolved": 0, "snoozed": 0, "category_fixed": 0, "flagged": 0}
    for uid in users:
        rows = await db.fetch(
            """
            SELECT thread_id, title, detail, status, priority, category, thread_type,
                   lifecycle_state, source_session_ids, evidence_turn_refs,
                   first_seen_at, last_updated_at, last_mentioned_at,
                   follow_up_after, salience_score, importance_score
            FROM open_threads
            WHERE user_id=$1 AND status IN ('open','snoozed')
            ORDER BY last_updated_at DESC NULLS LAST, created_at DESC
            LIMIT 80
            """,
            uid,
        )
        if not rows:
            continue
        for row in rows:
            if not _is_static_zombie_thread(dict(row)):
                continue
            priority = normalize_text(row.get("priority"))
            try:
                salience = float(row.get("salience_score") or 0.0)
            except Exception:
                salience = 0.0
            try:
                importance = float(row.get("importance_score") or 0.0)
            except Exception:
                importance = 0.0
            if priority == "high" or max(salience, importance) >= 0.75:
                summary["flagged"] += 1
                continue
            await db.execute(
                """
                UPDATE open_threads
                SET status='snoozed',
                    lifecycle_state='snoozed',
                    last_updated_at=NOW()
                WHERE user_id=$1 AND thread_id=$2
                """,
                uid,
                row["thread_id"],
            )
            summary["snoozed"] += 1
        actions: List[Dict[str, Any]] = []
        if bool(s.derived_pipeline_llm_enabled):
            actions = await audit_thread_registry(
                open_threads=[dict(row) for row in rows],
                model=s.derived_pipeline_model_version,
            ) or []
        seen_ids = {normalize_text(row.get("thread_id")) for row in rows}
        for action in actions:
            kind = normalize_text(action.get("action")).upper()
            confidence = _confidence_float(action.get("confidence"), default=0.0)
            if kind == "FLAG_REVIEW" or confidence < 0.85:
                summary["flagged"] += 1
                continue
            if kind == "MERGE":
                keep_id = normalize_text(action.get("keep_thread_id"))
                absorb_ids = [normalize_text(x) for x in _text_list(action.get("absorb_thread_ids"), limit=10)]
                absorb_ids = [x for x in absorb_ids if x and x in seen_ids and x != keep_id]
                if keep_id not in seen_ids or not absorb_ids:
                    summary["flagged"] += 1
                    continue
                await db.execute(
                    """
                    UPDATE open_threads
                    SET title=COALESCE(NULLIF($3,''), title),
                        detail=COALESCE(NULLIF($4,''), detail),
                        category=COALESCE(NULLIF($5,''), category),
                        last_updated_at=NOW()
                    WHERE user_id=$1 AND thread_id=$2
                    """,
                    uid,
                    keep_id,
                    normalize_text(action.get("merged_title"))[:160],
                    normalize_text(action.get("merged_detail")),
                    normalize_text(action.get("merged_category")) if normalize_text(action.get("merged_category")) in VALID_CATEGORIES else None,
                )
                await db.execute(
                    """
                    UPDATE open_threads
                    SET status='resolved',
                        lifecycle_state='superseded',
                        superseded_by_thread_id=$3,
                        resolution_note=COALESCE(NULLIF($4,''), 'Merged into duplicate thread.'),
                        resolved_at=NOW(),
                        last_updated_at=NOW()
                    WHERE user_id=$1 AND thread_id = ANY($2::text[])
                    """,
                    uid,
                    absorb_ids,
                    keep_id,
                    normalize_text(action.get("reason")),
                )
                summary["merged"] += len(absorb_ids)
                continue
            thread_id = normalize_text(action.get("thread_id"))
            if thread_id not in seen_ids:
                summary["flagged"] += 1
                continue
            if kind == "CATEGORY_FIX":
                new_category = normalize_text(action.get("new_category"))
                if new_category not in VALID_CATEGORIES:
                    summary["flagged"] += 1
                    continue
                await db.execute(
                    """
                    UPDATE open_threads
                    SET category=$3, last_updated_at=NOW()
                    WHERE user_id=$1 AND thread_id=$2
                    """,
                    uid,
                    thread_id,
                    new_category,
                )
                summary["category_fixed"] += 1
            elif kind == "RESOLVE" and confidence >= 0.9:
                await db.execute(
                    """
                    UPDATE open_threads
                    SET status='resolved',
                        lifecycle_state='resolved',
                        resolution_note=COALESCE(NULLIF($3,''), 'Resolved by thread audit.'),
                        resolved_at=NOW(),
                        last_updated_at=NOW()
                    WHERE user_id=$1 AND thread_id=$2
                    """,
                    uid,
                    thread_id,
                    normalize_text(action.get("resolution_note") or action.get("reason")),
                )
                summary["resolved"] += 1
            elif kind == "SNOOZE" and confidence >= 0.9:
                await db.execute(
                    """
                    UPDATE open_threads
                    SET status='snoozed',
                        lifecycle_state='snoozed',
                        last_updated_at=NOW()
                    WHERE user_id=$1 AND thread_id=$2
                    """,
                    uid,
                    thread_id,
                )
                summary["snoozed"] += 1
            else:
                summary["flagged"] += 1
    return summary


async def run_entity_audit(
    *,
    db: Database,
    tenant_id: str = "default",
    user_id: Optional[str] = None,
) -> Dict[str, int]:
    del tenant_id  # entity_profiles is currently user-scoped.
    user_filter = "AND user_id=$1" if user_id else ""
    args: List[Any] = [user_id] if user_id else []
    rows = await db.fetch(
        f"""
        SELECT user_id, canonical_name_normalized, canonical_name, type, relationship_to_user,
               profile_text, key_facts, open_questions, last_known_status,
               confidence, mention_count, distinct_session_count,
               salience_score, importance_score
        FROM entity_profiles
        WHERE 1=1 {user_filter}
        LIMIT 1000
        """,
        *args,
    )
    summary = {"cleared": 0, "demoted": 0, "corrected": 0, "sanitized_profiles": 0, "flagged": 0}
    for row in rows:
        canonical = normalize_text(row.get("canonical_name"))
        canonical_norm = normalize_text(row.get("canonical_name_normalized"))
        entity_type = normalize_text(row.get("type"))
        relationship = normalize_text(row.get("relationship_to_user"))
        relationship_key = relationship.replace(" ", "_")
        profile_text = normalize_text(row.get("profile_text"), casefold=False)
        status_text = normalize_text(row.get("last_known_status"), casefold=False)
        if _is_reserved_assistant_entity(canonical) and (entity_type != "assistant" or relationship != "assistant" or normalize_text(row.get("profile_text"))):
            await db.execute(
                """
                UPDATE entity_profiles
                SET type='assistant',
                    relationship_to_user='assistant',
                    profile_text=NULL,
                    key_facts='[]'::jsonb,
                    open_questions='[]'::jsonb,
                    last_known_status=NULL,
                    last_updated_at=NOW()
                WHERE user_id=$1 AND canonical_name_normalized=$2
                """,
                row["user_id"],
                canonical_norm,
            )
            summary["cleared"] += 1
            continue
        if relationship_key in PROJECT_ENTITY_RELATIONSHIPS and entity_type == "person":
            await db.execute(
                """
                UPDATE entity_profiles
                SET type='project',
                    last_updated_at=NOW()
                WHERE user_id=$1 AND canonical_name_normalized=$2
                """,
                row["user_id"],
                canonical_norm,
            )
            summary["corrected"] += 1
            continue
        contaminated_profile = has_synthesis_quality_issue(profile_text) or has_synthesis_quality_issue(status_text)
        if contaminated_profile:
            if _profile_supports_conservative_rewrite(row):
                await db.execute(
                    """
                    UPDATE entity_profiles
                    SET profile_text=$3,
                        last_known_status=$4,
                        last_updated_at=NOW()
                    WHERE user_id=$1 AND canonical_name_normalized=$2
                    """,
                    row["user_id"],
                    canonical_norm,
                    conservative_rewrite_text(profile_text, fallback=None),
                    conservative_rewrite_text(status_text, fallback=None),
                )
                summary["sanitized_profiles"] += 1
            else:
                summary["flagged"] += 1
            continue
        if not _has_entity_serving_content(dict(row)):
            await db.execute(
                """
                UPDATE entity_profiles
                SET status='tentative',
                    last_updated_at=NOW()
                WHERE user_id=$1 AND canonical_name_normalized=$2 AND status='active'
                """,
                row["user_id"],
                canonical_norm,
            )
            summary["demoted"] += 1
    return summary


async def run_conservative_memory_audits(
    *,
    db: Database,
    tenant_id: str = "default",
    settings: Optional[Settings] = None,
) -> Dict[str, Dict[str, int]]:
    thread_summary = await run_thread_audit(db=db, tenant_id=tenant_id, settings=settings)
    entity_summary = await run_entity_audit(db=db, tenant_id=tenant_id)
    return {"threads": thread_summary, "entities": entity_summary}


async def run_reinforcement_decay(*, db: Database, tenant_id: str = "default") -> int:
    result = await db.execute(
        """
        UPDATE derived_assertions
        SET salience = GREATEST(
                COALESCE(retention_floor, 0.0),
                COALESCE(salience, 0.5) *
                CASE WHEN memory_layer='LML' THEN 0.98 ELSE 0.90 END
            ),
            updated_at=NOW()
        WHERE tenant_id=$1
          AND lifecycle_state='active'
          AND COALESCE(last_accessed_at, updated_at, created_at) < NOW() - INTERVAL '7 days'
        """,
        tenant_id,
    )
    try:
        return int(result.split()[-1])
    except Exception:
        return 0


async def run_staleness_review(*, db: Database, tenant_id: str = "default") -> int:
    result = await db.execute(
        """
        UPDATE derived_assertions
        SET staleness_status='review_needed',
            staleness_review_at=NOW(),
            updated_at=NOW()
        WHERE tenant_id=$1
          AND lifecycle_state='active'
          AND COALESCE(salience, 0) >= 0.75
          AND staleness_status = 'fresh'
          AND COALESCE(last_accessed_at, updated_at, created_at) < NOW() - INTERVAL '90 days'
        """,
        tenant_id,
    )
    try:
        return int(result.split()[-1])
    except Exception:
        return 0


async def run_explicit_consolidation(*, db: Database, tenant_id: str = "default") -> int:
    rows = await db.fetch(
        """
        SELECT user_id,
               surface,
               lower(statement_text) AS normalized_statement,
               array_agg(assertion_id ORDER BY assertion_id) AS assertion_ids,
               array_agg(DISTINCT unnest_session) AS session_ids,
               jsonb_agg(source_turn_refs) AS turn_refs,
               COUNT(*) AS n,
               MAX(salience) AS salience,
               MAX(importance) AS importance,
               MAX(confidence_validity) AS confidence_validity
        FROM (
            SELECT da.*, unnest(da.source_session_ids) AS unnest_session
            FROM derived_assertions da
            WHERE da.tenant_id=$1
              AND da.lifecycle_state='active'
              AND da.surface IN ('thread_signal','identity_signal','memory_delta','living_context_statement')
        ) x
        GROUP BY user_id, surface, lower(statement_text)
        HAVING COUNT(*) >= 3
        LIMIT 100
        """,
        tenant_id,
    )
    inserted = 0
    for row in rows:
        statement = row.get("normalized_statement")
        if not statement:
            continue
        await db.execute(
            """
            INSERT INTO consolidated_insights (
                tenant_id, user_id, insight_type, statement_text, lifecycle_state,
                source_assertion_ids, source_session_ids, source_turn_refs,
                salience, importance, confidence_validity, promoted_to
            )
            VALUES ($1,$2,$3,$4,'active',$5::bigint[],$6::text[],$7::jsonb,$8,$9,$10,$11::text[])
            """,
            tenant_id,
            row["user_id"],
            row["surface"],
            statement,
            row.get("assertion_ids") or [],
            row.get("session_ids") or [],
            row.get("turn_refs") or [],
            row.get("salience"),
            row.get("importance"),
            row.get("confidence_validity"),
            ["identity_profile" if row["surface"] == "identity_signal" else "living_context"],
        )
        inserted += 1
    return inserted
