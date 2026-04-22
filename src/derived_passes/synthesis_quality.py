from __future__ import annotations

import re
from typing import Any, Dict, List

from .common import as_list, clean_text

BANNED_SYNTHESIS_PHRASES = [
    "rollercoaster",
    "deep complexity",
    "the vibe is",
    "trying to prove",
    "to prove his",
    "to prove her",
    "to prove their",
    "prove his capability",
    "prove her capability",
    "prove their capability",
    "weight of",
    "driven by",
    "defined by",
    "navigating life through",
    "someone who seeks",
    "someone who can",
    "beneath it all",
    "underneath",
    "navigates life defined by",
    "what's underneath",
    "guilt and shame",
    "cares about deeply",
    "precious memories",
    "heavy, quiet grief",
    "high-stakes precision",
    "emotional terrain",
    "existential",
    "reclaiming his agency",
    "reclaiming her agency",
    "reclaiming their agency",
]

_BANNED_RE = re.compile(
    "|".join(re.escape(phrase) for phrase in BANNED_SYNTHESIS_PHRASES),
    flags=re.IGNORECASE,
)

_PERSONALITY_VERDICT_RE = re.compile(
    r"\b(the user|mukesh)\s+is\s+(someone who|a person who|the kind of person|defined by|driven by)\b",
    flags=re.IGNORECASE,
)

_MOTIVE_RE = re.compile(
    r"\b(trying to prove|to prove (his|her|their|himself|herself|themselves)|driven by|beneath it all|underneath|secretly|unspoken desire|hidden motive)\b",
    flags=re.IGNORECASE,
)

_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def synthesis_quality_flags(value: Any) -> List[str]:
    text = clean_text(value)
    if not text:
        return []
    flags: List[str] = []
    lower = text.lower()
    for phrase in BANNED_SYNTHESIS_PHRASES:
        if phrase in lower:
            flags.append(f"banned_phrase:{phrase}")
    if _PERSONALITY_VERDICT_RE.search(text):
        flags.append("personality_verdict")
    if _MOTIVE_RE.search(text):
        flags.append("motive_inference")
    return flags


def has_synthesis_quality_issue(value: Any) -> bool:
    return bool(synthesis_quality_flags(value))


def conservative_rewrite_text(value: Any, *, fallback: str | None = None) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    if not has_synthesis_quality_issue(text):
        return text

    kept = [
        sentence.strip()
        for sentence in _SENTENCE_SPLIT_RE.split(text)
        if sentence.strip() and not has_synthesis_quality_issue(sentence)
    ]
    rewritten = " ".join(kept).strip()
    if rewritten:
        return rewritten
    return fallback


def sanitize_dict_strings(row: Dict[str, Any], *, fallback_by_key: Dict[str, str | None] | None = None) -> Dict[str, Any]:
    cleaned = dict(row)
    fallbacks = fallback_by_key or {}
    for key, value in list(cleaned.items()):
        if isinstance(value, str):
            cleaned[key] = conservative_rewrite_text(value, fallback=fallbacks.get(key))
    return cleaned


def sanitize_list_of_dicts(rows: Any, *, fallback_by_key: Dict[str, str | None] | None = None) -> List[Any]:
    out: List[Any] = []
    for item in as_list(rows):
        if isinstance(item, dict):
            out.append(sanitize_dict_strings(item, fallback_by_key=fallback_by_key))
        elif isinstance(item, str):
            cleaned = conservative_rewrite_text(item)
            if cleaned:
                out.append(cleaned)
        else:
            out.append(item)
    return out


def directive_is_explicit(row: Dict[str, Any]) -> bool:
    directive = clean_text(row.get("directive"))
    if not directive:
        return False
    reason = clean_text(row.get("reason")).lower()
    if not reason:
        return False
    if has_synthesis_quality_issue(directive) or has_synthesis_quality_issue(reason):
        return False
    explicit_markers = (
        "explicit",
        "user said",
        "user asked",
        "user requested",
        "user told",
        "user stated",
        "user expressed",
        "user identified",
        "forbade",
        "hard constraint",
        "low_confidence_queue",
    )
    inferred_markers = (
        "inferred",
        "implicit",
        "suggests",
        "appears",
        "based on context",
        "care style",
    )
    if any(marker in reason for marker in inferred_markers):
        return False
    return any(marker in reason for marker in explicit_markers)
