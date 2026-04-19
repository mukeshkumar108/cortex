from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
import re


ONTOLOGY_NODE_TYPES: List[str] = [
    "person",
    "project",
    "goal",
    "loop",
    "preference",
    "event",
]

ONTOLOGY_DOMAIN_FACETS: List[str] = [
    "relationships",
    "work",
    "health",
    "logistics",
    "lifestyle",
    "identity",
]

ONTOLOGY_EDGE_TYPES: List[str] = [
    "related_to_user_as",
    "working_on",
    "pursuing",
    "about",
    "prefers",
    "involved_in",
    "cares_about",
    "evidence_for",
]

ONTOLOGY_NODE_DISCIPLINE: Dict[str, str] = {
    "goal": "Desired outcome or direction. Never use for an active unresolved follow-up thread.",
    "loop": "Active unresolved thread requiring continuation or attention. Never use for long-horizon aspiration.",
    "preference": "Stable behaviorally relevant preference. Never use for one-off requests or inferred personality traits.",
    "event": "Meaningful autobiographical occurrence or interaction. Never mirror every transcript turn or trivial fragment.",
}

ONTOLOGY_PRECEDENCE_RULES: List[str] = [
    "canonical Person/Project identity beats weaker duplicate or variant copies",
    "Loop beats Goal when the concept is an active unresolved follow-up thread",
    "Goal beats Loop when the concept is a desired outcome or longer-horizon direction",
    "Preference requires durable support and never overrides stable preference state from one-off context",
    "Event supports canonical nodes through evidence_for and should not replace them",
    "when candidates overlap, prefer one clean canonical representation over multiple ambiguous nodes",
]

FACT_STORAGE_RULES: List[str] = [
    "facts are grounded claims attached to nodes or edges, not standalone node types",
    "relationship and role claims belong on edges when possible",
    "time-bounded claims should carry valid_at/invalid_at metadata when known",
    "fact trust should be represented with confidence and provenance metadata, not extra abstraction nodes",
]

CANONICAL_NODE_METADATA_FIELDS: Dict[str, List[str]] = {
    "all_nodes": [
        "domains",
        "confidence",
        "importance",
        "salience",
        "first_seen_at",
        "last_seen_at",
    ],
    "identity_nodes": [
        "aliases",
    ],
    "goal": [
        "status",
        "time_horizon",
    ],
    "loop": [
        "status",
        "time_horizon",
    ],
    "preference": [
        "polarity",
    ],
    "event": [
        "occurred_at",
        "significance",
    ],
}

CANONICAL_EDGE_METADATA_FIELDS: Dict[str, List[str]] = {
    "all_edges": [
        "confidence",
        "importance",
    ],
    "related_to_user_as": [
        "role",
    ],
    "temporal_claims": [
        "valid_at",
        "invalid_at",
    ],
    "evidence_for": [
        "provenance",
        "session_id",
        "episode_uuid",
        "timestamp",
        "message_index",
        "evidence_summary",
    ],
}

CANONICAL_PROVENANCE_FIELDS: List[str] = [
    "session_id",
    "episode_uuid",
    "timestamp",
    "message_index",
    "evidence_summary",
]

EVENT_SIGNIFICANCE_LEVELS: List[str] = [
    "low",
    "medium",
    "high",
]

INTERNAL_GRAPH_LABELS: Set[str] = {
    "sessionsummary",
    "tension",
    "observation",
    "environment",
    "mentalstate",
    "userfocus",
    "episodic",
    "community",
}

EPHEMERAL_ENTITY_PATTERNS: Sequence[re.Pattern[str]] = (
    re.compile(r"^session_summary_", flags=re.IGNORECASE),
    re.compile(r"\b(quick update|major update|daily analysis|system memory problem|code base)\b", flags=re.IGNORECASE),
    re.compile(r"^[0-9a-f-]{16,}$", flags=re.IGNORECASE),
)

GENERIC_NOUN_PHRASES: Set[str] = {
    "update",
    "major update",
    "quick update",
    "file",
    "thing",
    "stuff",
    "issue",
    "problem",
    "task",
    "todo",
    "conversation",
    "chat",
    "message",
    "system memory problem",
    "code base",
}

TRANSCRIPT_FRAGMENT_RE = re.compile(r"^(user|assistant|system)\s*:", flags=re.IGNORECASE)

QUERY_TYPE_HINTS: Dict[str, Set[str]] = {
    "person": {"person", "people", "relationship", "relationships", "family", "friend", "friends", "girlfriend", "boyfriend", "daughter", "son"},
    "project": {"project", "projects", "product", "products", "startup", "company", "companies", "work"},
    "goal": {"goal", "goals", "aim", "aims", "priority", "priorities", "plan", "plans"},
    "loop": {"loop", "loops", "open loop", "open loops", "todo", "todos", "task", "tasks", "follow up", "follow-up"},
    "preference": {"preference", "preferences", "prefer", "likes", "dislikes"},
    "event": {"event", "events", "episode", "episodes", "happened", "recently"},
}

CANONICAL_NAME_ALIASES: Dict[str, str] = {
    "the user": "User",
    "user": "User",
    "me": "User",
    "myself": "User",
    "ashleys daughter": "Yoshi",
}

KNOWN_ENTITY_CANONICAL_NAMES: Dict[str, str] = {
    "ashley": "Ashley",
    "jasmine": "Jasmine",
    "sophie": "Sophie",
    "bluum": "Bluum",
    "yoshi": "Yoshi",
}

KNOWN_ENTITY_TYPES: Dict[str, str] = {
    "ashley": "person",
    "jasmine": "person",
    "yoshi": "person",
    "user": "person",
    "bluum": "project",
    "sophie": "project",
}

NODE_TYPE_PRECEDENCE: Dict[str, int] = {
    "person": 100,
    "project": 95,
    "loop": 80,
    "goal": 75,
    "preference": 65,
    "event": 50,
    "other": 0,
}

CANONICAL_SUPPORT_ATTRIBUTE_KEYS: Set[str] = {
    "person_id",
    "project_id",
    "goal_id",
    "loop_id",
    "preference_id",
    "entity_id",
    "entity_ids",
    "entity_refs",
    "canonical_refs",
    "related_entities",
    "about",
    "about_entity",
    "about_entity_id",
    "subject_id",
    "object_id",
}


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    return re.sub(r"\s+", " ", value).strip()


def canonicalize_entity_name(name: Any) -> str:
    clean = normalize_text(name)
    if not clean:
        return ""
    lower = clean.lower()
    if lower in CANONICAL_NAME_ALIASES:
        return CANONICAL_NAME_ALIASES[lower]
    if lower in KNOWN_ENTITY_CANONICAL_NAMES:
        return KNOWN_ENTITY_CANONICAL_NAMES[lower]
    return clean


def infer_ontology_type(raw: Any, labels: Optional[Sequence[Any]] = None, name: Any = None) -> str:
    joined = " ".join(
        [
            normalize_text(raw).lower(),
            " ".join(normalize_text(x).lower() for x in (labels or []) if normalize_text(x)),
        ]
    ).strip()
    canonical_name = canonicalize_entity_name(name).lower()

    if canonical_name in KNOWN_ENTITY_TYPES:
        return KNOWN_ENTITY_TYPES[canonical_name]
    if any(token in joined for token in ("person", "people", "human", "user")):
        return "person"
    if any(token in joined for token in ("project", "product", "startup", "tool", "app", "company", "organization", "business", "org")):
        return "project"
    if "goal" in joined:
        return "goal"
    if "loop" in joined:
        return "loop"
    if "preference" in joined:
        return "preference"
    if "event" in joined:
        return "event"
    return "other"


def _word_count(text: str) -> int:
    return len(re.findall(r"[A-Za-z0-9]+", text or ""))


def _generic_name(text: str) -> bool:
    lower = normalize_text(text).lower()
    return lower in GENERIC_NOUN_PHRASES


def has_durable_support(attributes: Optional[Dict[str, Any]]) -> bool:
    attrs = attributes if isinstance(attributes, dict) else {}
    return bool(
        attrs.get("provenance")
        or attrs.get("session_id")
        or attrs.get("updated_at")
        or attrs.get("last_seen_at")
        or attrs.get("importance")
        or attrs.get("role")
    )


def has_canonical_node_support(attributes: Optional[Dict[str, Any]]) -> bool:
    attrs = attributes if isinstance(attributes, dict) else {}
    for key in CANONICAL_SUPPORT_ATTRIBUTE_KEYS:
        value = attrs.get(key)
        if isinstance(value, list) and value:
            return True
        if isinstance(value, dict) and value:
            return True
        if value not in (None, "", [], {}):
            return True
    return False


def _loop_vs_goal_precedence(node_type: str, attrs: Dict[str, Any]) -> int:
    status = normalize_text(attrs.get("status")).lower()
    time_horizon = normalize_text(attrs.get("time_horizon") or attrs.get("timeHorizon")).lower()
    if node_type == "loop":
        if status in {"open", "blocked", "waiting", "unresolved", "pending"}:
            return NODE_TYPE_PRECEDENCE["loop"] + 8
        return NODE_TYPE_PRECEDENCE["loop"]
    if node_type == "goal":
        if time_horizon in {"ongoing", "monthly", "quarterly", "yearly", "long_term", "long-term"}:
            return NODE_TYPE_PRECEDENCE["goal"] + 8
        if status in {"active", "planned", "in_progress", "in-progress"}:
            return NODE_TYPE_PRECEDENCE["goal"] + 4
        return NODE_TYPE_PRECEDENCE["goal"]
    return NODE_TYPE_PRECEDENCE.get(node_type, 0)


def type_discipline_score(node_type: str, name: Any, attributes: Optional[Dict[str, Any]] = None) -> float:
    clean = canonicalize_entity_name(name)
    lower = clean.lower()
    attrs = attributes if isinstance(attributes, dict) else {}
    score = 0.6

    if clean in KNOWN_ENTITY_CANONICAL_NAMES.values() or lower == "user":
        score += 0.25
    if attrs.get("domains"):
        score += 0.05
    if attrs.get("role") or attrs.get("importance"):
        score += 0.05
    if attrs.get("provenance") or attrs.get("session_id"):
        score += 0.05
    if has_canonical_node_support(attrs):
        score += 0.1
    if has_durable_support(attrs):
        score += 0.05
    if node_type == "preference" and attrs.get("polarity"):
        score += 0.1
    if node_type == "event" and (attrs.get("occurred_at") or attrs.get("provenance") or attrs.get("session_id")):
        score += 0.1

    return score


def validate_ontology_node(
    *,
    name: Any,
    raw_type: Any,
    labels: Optional[Sequence[Any]] = None,
    attributes: Optional[Dict[str, Any]] = None,
    allowed_types: Optional[Sequence[str]] = None,
    include_internal: bool = False,
) -> Tuple[bool, str, str, Optional[str], float]:
    canonical_name = canonicalize_entity_name(name)
    if not canonical_name:
        return False, "", "other", "empty_name", 0.0

    inferred = infer_ontology_type(raw_type, labels=labels, name=canonical_name)
    attrs = attributes if isinstance(attributes, dict) else {}
    lower = canonical_name.lower()

    if not include_internal and is_internal_graph_node(labels=labels, raw_type=raw_type, name=canonical_name):
        return False, canonical_name, inferred, "internal_graph_node", 0.0
    if inferred not in set(ONTOLOGY_NODE_TYPES):
        return False, canonical_name, inferred, "outside_ontology", 0.0
    if allowed_types:
        normalized_allowed = {normalize_text(item).lower() for item in allowed_types if normalize_text(item)}
        if inferred not in normalized_allowed:
            return False, canonical_name, inferred, "disallowed_type", 0.0
    if _generic_name(canonical_name):
        return False, canonical_name, inferred, "generic_noun_phrase", 0.0
    if TRANSCRIPT_FRAGMENT_RE.search(canonical_name):
        return False, canonical_name, inferred, "transcript_fragment", 0.0

    if inferred == "goal":
        if attrs.get("status") and normalize_text(attrs.get("status")).lower() in {"open", "blocked", "waiting", "unresolved", "pending"}:
            return False, canonical_name, inferred, "goal_has_loop_status", 0.0

    if inferred == "loop":
        if attrs.get("status") and normalize_text(attrs.get("status")).lower() in {"completed", "done", "resolved"}:
            return False, canonical_name, inferred, "resolved_loop", 0.0

    if inferred == "preference":
        if not (attrs.get("polarity") and has_durable_support(attrs)):
            return False, canonical_name, inferred, "preference_without_support", 0.0
        if _word_count(canonical_name) < 2 and lower not in KNOWN_ENTITY_TYPES:
            return False, canonical_name, inferred, "low_signal_preference", 0.0

    if inferred == "event":
        if _word_count(canonical_name) < 3:
            return False, canonical_name, inferred, "low_signal_event", 0.0
        if not (attrs.get("occurred_at") or attrs.get("session_id") or attrs.get("provenance")):
            return False, canonical_name, inferred, "event_without_evidence", 0.0
        if not has_canonical_node_support(attrs):
            return False, canonical_name, inferred, "event_without_canonical_support", 0.0
        if attrs.get("message_count") is not None:
            try:
                if int(attrs.get("message_count") or 0) <= 1:
                    return False, canonical_name, inferred, "single_message_event", 0.0
            except Exception:
                pass
        if _word_count(canonical_name) < 5 and not (attrs.get("summary") or attrs.get("details")):
            return False, canonical_name, inferred, "thin_event_without_evidence", 0.0

    score = type_discipline_score(inferred, canonical_name, attrs)
    return True, canonical_name, inferred, None, score


def choose_preferred_node(existing: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
    existing_type = normalize_text(existing.get("type")).lower() or "other"
    candidate_type = normalize_text(candidate.get("type")).lower() or "other"
    existing_attrs = existing.get("attributes") if isinstance(existing.get("attributes"), dict) else {}
    candidate_attrs = candidate.get("attributes") if isinstance(candidate.get("attributes"), dict) else {}

    existing_precedence = _loop_vs_goal_precedence(existing_type, existing_attrs)
    candidate_precedence = _loop_vs_goal_precedence(candidate_type, candidate_attrs)

    if existing_type in {"person", "project"} and candidate_type not in {"person", "project"}:
        return existing
    if candidate_type in {"person", "project"} and existing_type not in {"person", "project"}:
        return candidate

    if candidate_precedence > existing_precedence:
        return candidate
    if existing_precedence > candidate_precedence:
        return existing

    existing_score = float(existing.get("discipline_score") or 0.0)
    candidate_score = float(candidate.get("discipline_score") or 0.0)
    if candidate_score > existing_score:
        return candidate
    if existing_score > candidate_score:
        return existing

    existing_ts = normalize_text(existing.get("updated_at") or existing.get("reference_time") or existing.get("created_at"))
    candidate_ts = normalize_text(candidate.get("updated_at") or candidate.get("reference_time") or candidate.get("created_at"))
    if candidate_ts >= existing_ts:
        return candidate
    return existing


def is_internal_graph_node(labels: Optional[Sequence[Any]] = None, raw_type: Any = None, name: Any = None) -> bool:
    normalized_labels = {
        normalize_text(label).lower()
        for label in (labels or [])
        if normalize_text(label)
    }
    if normalized_labels & INTERNAL_GRAPH_LABELS:
        return True
    raw_type_text = normalize_text(raw_type).lower()
    if raw_type_text in INTERNAL_GRAPH_LABELS:
        return True
    lower_name = canonicalize_entity_name(name).lower()
    return any(pattern.search(lower_name) for pattern in EPHEMERAL_ENTITY_PATTERNS)


def is_allowed_runtime_node(
    *,
    name: Any,
    raw_type: Any,
    labels: Optional[Sequence[Any]] = None,
    allowed_types: Optional[Sequence[str]] = None,
    include_internal: bool = False,
) -> bool:
    allowed, _canonical_name, _inferred, _reason, _score = validate_ontology_node(
        name=name,
        raw_type=raw_type,
        labels=labels,
        allowed_types=allowed_types,
        include_internal=include_internal,
    )
    return allowed


def infer_query_node_types(query: Any) -> List[str]:
    lower = normalize_text(query).lower()
    if not lower:
        return []
    matches: List[str] = []
    for ontology_type, hints in QUERY_TYPE_HINTS.items():
        if any(hint in lower for hint in hints):
            matches.append(ontology_type)
    deduped: List[str] = []
    for item in matches:
        if item not in deduped:
            deduped.append(item)
    return deduped
