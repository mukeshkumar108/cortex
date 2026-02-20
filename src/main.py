from fastapi import FastAPI, HTTPException, BackgroundTasks, Header, Response
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
from datetime import datetime, timedelta, date, timezone as dt_timezone
import logging
import re
import json
from copy import deepcopy
from typing import Optional, Dict, Any, List, Tuple

from .models import (
    IngestRequest,
    BriefRequest,
    IngestResponse,
    BriefResponse,
    MemoryQueryRequest,
    MemoryQueryResponse,
    MemoryLoopsResponse,
    MemoryLoopItem,
    Fact,
    Entity,
    SessionStartBriefResponse,
    SessionStartBriefItem,
    SessionCloseRequest,
    SessionIngestRequest,
    SessionIngestResponse,
    SessionBriefResponse,
    PurgeUserRequest,
    UserModelPatchRequest,
    UserModelResponse,
    DailyAnalysisResponse,
)
from .utils import extract_location
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import io
import csv
from .config import get_settings
from .falkor_utils import extract_count, extract_node_dicts, pick_first_node
from .db import Database
from .graphiti_client import GraphitiClient
from . import session
from . import loops
from .ingestion import ingest as process_ingest
from .briefing import build_briefing
from .migrate import run_migrations
from .openrouter_client import get_llm_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global instances
db = Database()
graphiti_client = GraphitiClient()


INTERPRETIVE_TERMS = (
    "feels",
    "feeling",
    "struggling",
    "isolating",
    "grounding",
    "tension",
    "vibe",
    "emotional",
)
ENERGY_HINT_TERMS = (
    "tired",
    "exhausted",
    "drained",
    "sleepy",
    "energized",
    "wired",
    "low energy",
    "high energy",
)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    return re.sub(r"\s+", " ", value).strip()


def _strip_list_prefix(value: str) -> str:
    # Remove common bullet/numbering prefixes so claim splitting doesn't emit "-" or "1."
    return re.sub(r"^\s*(?:[-*•]+|\d+[.)])\s+", "", value).strip()


_KNOWN_SHEET_HEADINGS = {
    "FACTS:",
    "OPEN_LOOPS:",
    "COMMITMENTS:",
    "CONTEXT_ANCHORS:",
    "USER_STATED_STATE:",
    "CURRENT_FOCUS:",
}
_HEADING_LIKE_RE = re.compile(r"^[A-Z][A-Z0-9_ ]{2,40}:$")


def _is_heading_like(value: str) -> bool:
    clean = _normalize_text(value)
    if not clean:
        return False
    if clean in _KNOWN_SHEET_HEADINGS:
        return True
    # Generic ALL_CAPS heading lines like "FACTS:" or "ACTION ITEMS:"
    return bool(_HEADING_LIKE_RE.match(clean))


_WORD_TOKEN_RE = re.compile(r"[A-Za-z0-9]+(?:['’][A-Za-z]+)?")
_GENERIC_LEADING_TOKENS = {"a", "an", "the", "my", "our", "your", "this", "that", "these", "those"}
_VAGUE_NOUN_TOKENS = {
    # Keep this list short and stable; we only want to suppress obvious low-signal fragments.
    "user",
    "presentation",
    "project",
    "projects",
    "thing",
    "stuff",
}
_COPULA_TOKENS = {"is", "am", "are", "was", "were", "be", "been", "being"}


def _word_tokens(value: str) -> List[str]:
    return _WORD_TOKEN_RE.findall(value or "")


def _has_proper_possessive(value: str) -> bool:
    # "Ashley's presentation" / "Ashley’s presentation"
    clean = _normalize_text(value)
    return bool(re.search(r"\b[A-Z][a-z]+['’]s\b", clean))


def _allow_fact_text(value: Optional[str]) -> bool:
    """
    Minimal fact-quality filter:
    - Require >= 2 word tokens, OR allow "ProperNoun's <descriptor>".
    - Suppress obvious single-token/vague fragments.
    """
    clean = _strip_list_prefix(_normalize_text(value))
    if not clean:
        return False
    if _is_heading_like(clean):
        return False

    tokens = _word_tokens(clean)
    if len(tokens) < 2:
        # Single tokens like "User" or "presentation" are too low-signal.
        return False

    if len(tokens) == 2:
        # Accept proper-noun possessive constructions even if the descriptor is generic.
        if _has_proper_possessive(clean):
            return True

        lowered = [t.lower() for t in tokens if t]
        if any(t in _COPULA_TOKENS for t in lowered):
            # "User is" / "I am" style fragments are not useful standalone.
            return False

        meaningful = [t for t in lowered if t not in _GENERIC_LEADING_TOKENS]
        if len(meaningful) == 1 and meaningful[0] in _VAGUE_NOUN_TOKENS:
            # "the presentation", "my project"
            return False
        if meaningful and all(t in _VAGUE_NOUN_TOKENS for t in meaningful):
            # "user presentation"
            return False

    # Multi-token facts generally have enough surface area to be useful.
    return True


def _shorten_line(value: str, limit: int) -> str:
    clean = _normalize_text(value)
    if len(clean) <= limit:
        return clean
    return clean[: max(0, limit - 3)].rstrip() + "..."


def _select_facts(candidates: List[str], limit: int) -> List[str]:
    filtered: List[str] = []
    for raw in candidates:
        claim = _strip_list_prefix(_normalize_text(raw)).strip(" .")
        if not claim:
            continue
        if _is_heading_like(claim):
            continue
        if not _allow_claim(claim) or _is_explicit_user_state_claim(claim):
            continue
        if not _allow_fact_text(claim):
            continue
        filtered.append(_shorten_line(claim, 160))
    return _dedupe_keep_order(filtered, limit=limit)


def _split_claims(text: Optional[str]) -> List[str]:
    raw = (text or "").strip()
    if not raw:
        return []
    # Important: split before collapsing whitespace so we don't merge "\nFACTS:\n- X" into a single claim.
    parts = re.split(r"[;\n]+|(?<=[.!?])\s+", raw)
    out: List[str] = []
    for part in parts:
        value = _strip_list_prefix(_normalize_text(part)).strip(" .")
        if not value:
            continue
        if _is_heading_like(value):
            continue
        out.append(value)
    return out


def _extract_explicit_user_state(text: Optional[str]) -> Optional[str]:
    clean = _normalize_text(text)
    if not clean:
        return None
    match = re.search(
        r"\b(i feel|i'm feeling|i am feeling|i felt|i am|i'm)\s+([a-z][^.;,!?\n]{0,60})",
        clean,
        flags=re.IGNORECASE
    )
    if not match:
        return None
    candidate = _normalize_text(match.group(0))
    if _is_focus_phrase(candidate):
        return None
    return candidate


def _contains_interpretive_language(text: Optional[str]) -> bool:
    lower = _normalize_text(text).lower()
    if not lower:
        return False
    return any(term in lower for term in INTERPRETIVE_TERMS)


def _is_explicit_user_state_claim(text: Optional[str]) -> bool:
    return _extract_explicit_user_state(text) is not None


def _is_focus_phrase(text: Optional[str]) -> bool:
    lower = _normalize_text(text).lower()
    if not lower:
        return False
    focus_terms = (
        "focus",
        "focused",
        "priority",
        "priorities",
        "trying to",
        "need to",
        "working on",
        "right now",
        "today i need",
    )
    return any(term in lower for term in focus_terms)


def _allow_claim(text: Optional[str]) -> bool:
    clean = _normalize_text(text)
    if not clean:
        return False
    if _is_heading_like(clean):
        return False
    if _contains_interpretive_language(clean) and not _extract_explicit_user_state(clean):
        return False
    return True


def _dedupe_keep_order(items: List[str], limit: int) -> List[str]:
    seen = set()
    out: List[str] = []
    for raw in items:
        item = _normalize_text(raw)
        if not item:
            continue
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
        if len(out) >= limit:
            break
    return out


def _time_of_day_label(dt: datetime) -> str:
    hour = dt.hour
    if 0 <= hour < 5:
        return "NIGHT"
    if 5 <= hour < 12:
        return "MORNING"
    if 12 <= hour < 17:
        return "AFTERNOON"
    return "EVENING"


def _extract_energy_hint_from_texts(texts: List[str]) -> Optional[str]:
    for text in texts:
        state = _extract_explicit_user_state(text)
        if not state:
            continue
        lower = state.lower()
        for term in ENERGY_HINT_TERMS:
            if term in lower:
                return term.upper().replace(" ", "_")
    return None


def _looks_like_environment(value: Optional[str]) -> bool:
    if not value:
        return False
    if extract_location(value):
        return True
    lower = _normalize_text(value).lower()
    return any(term in lower for term in ("raining", "weather", "noisy", "quiet", "outside", "indoors"))


def _resolve_timezone(tz: Optional[str]) -> Optional[ZoneInfo]:
    if not tz:
        return None
    try:
        return ZoneInfo(tz)
    except ZoneInfoNotFoundError:
        return None


def _default_user_model() -> Dict[str, Any]:
    def _default_north_star_domain() -> Dict[str, Any]:
        return {
            "vision": None,
            "goal": None,
            "status": "unknown",
            "vision_confidence": None,
            "vision_source": None,
            "goal_confidence": None,
            "goal_source": None,
            "updated_at": None,
        }

    def _default_north_star() -> Dict[str, Any]:
        return {
            "relationships": _default_north_star_domain(),
            "work": _default_north_star_domain(),
            "health": _default_north_star_domain(),
            "spirituality": _default_north_star_domain(),
            "general": _default_north_star_domain(),
        }

    return {
        "north_star": _default_north_star(),
        "current_focus": None,
        "key_relationships": [],
        "work_context": None,
        "patterns": [],
        "preferences": {},
        "health": None,
        "spirituality": None,
    }


def _is_populated(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        clean = value.strip()
        if not clean:
            return False
        if clean.lower() in {"unknown", "none", "n/a", "na"}:
            return False
        return True
    if isinstance(value, list):
        return any(_is_populated(item) for item in value)
    if isinstance(value, dict):
        content_keys = [k for k in value.keys() if k not in {"confidence", "source", "updated_at"}]
        if not content_keys:
            return False
        return any(_is_populated(value.get(k)) for k in content_keys)
    return True


def _normalize_confidence(raw: Any, default: float = 0.6) -> float:
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return default
    if v > 1.0:
        v = v / 100.0
    return max(0.0, min(1.0, v))


def _extract_confidence(value: Any, default: float = 0.6) -> float:
    if isinstance(value, dict):
        if "confidence" in value:
            return _normalize_confidence(value.get("confidence"), default=default)
        confidences = [_extract_confidence(v, default=default) for v in value.values() if _is_populated(v)]
        return sum(confidences) / len(confidences) if confidences else default
    if isinstance(value, list):
        confidences = [_extract_confidence(v, default=default) for v in value if _is_populated(v)]
        return sum(confidences) / len(confidences) if confidences else default
    return default


def _normalize_status(value: Any) -> str:
    raw = _normalize_text(str(value or "")).lower()
    if raw in {"active", "inactive", "unknown"}:
        return raw
    return "unknown"


def _normalize_north_star(value: Any) -> Dict[str, Any]:
    baseline = _default_user_model()["north_star"]
    if not isinstance(value, dict):
        return baseline

    # Backward compatibility for legacy shape: {"text": "...", "confidence": ...}
    if "text" in value:
        goal = _normalize_text(value.get("text"))
        if goal:
            baseline["general"]["goal"] = goal
            baseline["general"]["status"] = "active"
            baseline["general"]["goal_confidence"] = _normalize_confidence(value.get("confidence"), default=0.7)
            baseline["general"]["goal_source"] = value.get("source") or "legacy"
            baseline["general"]["updated_at"] = value.get("updated_at") or datetime.utcnow().isoformat()
        return baseline

    for domain, default_entry in baseline.items():
        incoming = value.get(domain)
        if incoming is None:
            continue
        if isinstance(incoming, str):
            text = _normalize_text(incoming)
            if text:
                default_entry["goal"] = text
                default_entry["status"] = "active"
                default_entry["goal_source"] = "legacy"
                default_entry["goal_confidence"] = 0.7
            continue
        if not isinstance(incoming, dict):
            continue
        entry = deepcopy(default_entry)
        entry["vision"] = _normalize_text(incoming.get("vision")) or None
        entry["goal"] = _normalize_text(incoming.get("goal")) or None
        entry["status"] = _normalize_status(incoming.get("status"))
        entry["vision_confidence"] = (
            _normalize_confidence(incoming.get("vision_confidence"), default=0.7)
            if incoming.get("vision_confidence") is not None else None
        )
        entry["goal_confidence"] = (
            _normalize_confidence(incoming.get("goal_confidence"), default=0.6)
            if incoming.get("goal_confidence") is not None else None
        )
        entry["vision_source"] = incoming.get("vision_source")
        entry["goal_source"] = incoming.get("goal_source")
        entry["updated_at"] = incoming.get("updated_at")
        baseline[domain] = entry
    return baseline


def _field_completeness(value: Any) -> float:
    if not _is_populated(value):
        return 0.0
    # 70% population, 30% confidence contribution.
    confidence = _extract_confidence(value, default=0.6)
    return 70.0 + (30.0 * confidence)


def _north_star_completeness(value: Any) -> float:
    north_star = _normalize_north_star(value)
    scores: List[float] = []
    for entry in north_star.values():
        if not isinstance(entry, dict):
            continue
        vision = _normalize_text(entry.get("vision"))
        goal = _normalize_text(entry.get("goal"))
        if not vision and not goal:
            continue
        vision_conf = _normalize_confidence(entry.get("vision_confidence"), default=0.7) if vision else 0.0
        goal_conf = _normalize_confidence(entry.get("goal_confidence"), default=0.6) if goal else 0.0
        signals = []
        if vision:
            signals.append(70.0 + (30.0 * vision_conf))
        if goal:
            signals.append(70.0 + (30.0 * goal_conf))
        if signals:
            scores.append(sum(signals) / len(signals))
    if not scores:
        return 0.0
    return sum(scores) / len(scores)


def _compute_domain_completeness(model: Dict[str, Any]) -> Dict[str, int]:
    domains = {
        "relationships": ["key_relationships"],
        "work": ["current_focus", "work_context"],
        "north_star": ["north_star"],
        "health": ["health"],
        "spirituality": ["spirituality"],
        "general": ["patterns", "preferences"],
    }
    out: Dict[str, int] = {}
    for domain, keys in domains.items():
        if domain == "north_star":
            scores = [_north_star_completeness(model.get("north_star"))]
        else:
            scores = [_field_completeness(model.get(k)) for k in keys]
        value = int(round(sum(scores) / len(scores))) if scores else 0
        out[domain] = max(0, min(100, value))
    return out


def _parse_iso_ts(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value).strip()
        if not raw:
            return None
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(dt_timezone.utc).replace(tzinfo=None)
    return dt


def _build_user_model_staleness_metadata(
    model: Dict[str, Any],
    now: Optional[datetime] = None
) -> Dict[str, Any]:
    now_dt = now or datetime.utcnow()
    if now_dt.tzinfo is not None:
        now_dt = now_dt.astimezone(dt_timezone.utc).replace(tzinfo=None)

    default_days = 21
    current_focus_days = 10
    fields: Dict[str, Dict[str, Any]] = {}

    def _record(path: str, value: Any, threshold_days: int) -> None:
        if not isinstance(value, dict):
            return
        updated_raw = value.get("updated_at")
        updated_dt = _parse_iso_ts(updated_raw)
        if not updated_dt:
            return
        age_days = max(0, int((now_dt - updated_dt).total_seconds() // 86400))
        fields[path] = {
            "updatedAt": updated_dt.isoformat() + "Z",
            "ageDays": age_days,
            "thresholdDays": threshold_days,
            "stale": age_days > threshold_days
        }

    _record("current_focus", model.get("current_focus"), current_focus_days)
    _record("work_context", model.get("work_context"), default_days)
    _record("health", model.get("health"), default_days)
    _record("spirituality", model.get("spirituality"), default_days)
    _record("preferences", model.get("preferences"), default_days)

    north_star = model.get("north_star")
    if isinstance(north_star, dict):
        for domain, entry in north_star.items():
            _record(f"north_star.{domain}", entry, default_days)

    relationships = model.get("key_relationships")
    if isinstance(relationships, list):
        for idx, row in enumerate(relationships):
            _record(f"key_relationships[{idx}]", row, default_days)

    patterns = model.get("patterns")
    if isinstance(patterns, list):
        for idx, row in enumerate(patterns):
            _record(f"patterns[{idx}]", row, default_days)

    stale_paths = [path for path, meta in fields.items() if meta.get("stale")]
    return {
        "staleness": {
            "fields": fields,
            "stalePaths": stale_paths,
            "hasStaleFields": bool(stale_paths),
            "thresholdDays": {
                "default": default_days,
                "current_focus": current_focus_days
            }
        }
    }


def _deep_merge_patch(base: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    merged = deepcopy(base)
    for key, patch_value in (patch or {}).items():
        if patch_value is None:
            merged.pop(key, None)
            continue
        current_value = merged.get(key)
        if isinstance(current_value, dict) and isinstance(patch_value, dict):
            merged[key] = _deep_merge_patch(current_value, patch_value)
        else:
            merged[key] = patch_value
    return merged


def _normalize_user_model(value: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    baseline = _default_user_model()
    if not isinstance(value, dict):
        return baseline
    normalized = baseline.copy()
    normalized.update(value)
    normalized["north_star"] = _normalize_north_star(normalized.get("north_star"))
    return normalized


def _extract_first_match(texts: List[str], patterns: List[re.Pattern]) -> Optional[str]:
    for text in texts:
        for pattern in patterns:
            match = pattern.search(text)
            if not match:
                continue
            if match.lastindex:
                candidate = _normalize_text(match.group(match.lastindex))
            else:
                candidate = _normalize_text(match.group(0))
            if candidate:
                return candidate[:200]
    return None


def _extract_relationships_from_texts(texts: List[str], confidence: float) -> List[Dict[str, Any]]:
    relationship_re = re.compile(
        r"\bmy\s+(partner|wife|husband|boyfriend|girlfriend|friend|mother|mom|father|dad|sister|brother)\s+([A-Z][a-zA-Z]+)\b",
        flags=re.IGNORECASE
    )
    out: List[Dict[str, Any]] = []
    seen = set()
    for text in texts:
        for who, name in relationship_re.findall(text):
            key = f"{name.lower()}|{who.lower()}"
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "name": name,
                "who": who.lower(),
                "status": "active",
                "confidence": confidence,
                "source": "user_stated",
                "updated_at": datetime.utcnow().isoformat()
            })
    return out


def _merge_relationships(existing: List[Dict[str, Any]], incoming: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for rel in existing or []:
        if not isinstance(rel, dict):
            continue
        key = f"{str(rel.get('name', '')).lower()}|{str(rel.get('who', '')).lower()}"
        if key.strip("|"):
            index[key] = rel
    for rel in incoming or []:
        if not isinstance(rel, dict):
            continue
        key = f"{str(rel.get('name', '')).lower()}|{str(rel.get('who', '')).lower()}"
        if not key.strip("|"):
            continue
        prev = index.get(key)
        prev_conf = _extract_confidence(prev, default=0.0) if prev else 0.0
        new_conf = _extract_confidence(rel, default=0.0)
        if not prev or new_conf >= prev_conf:
            index[key] = rel
    return list(index.values())


def _merge_patterns(existing: List[Dict[str, Any]], incoming: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for row in existing or []:
        if not isinstance(row, dict):
            continue
        text = _normalize_text(row.get("text"))
        if text:
            index[text.lower()] = row
    for row in incoming or []:
        if not isinstance(row, dict):
            continue
        text = _normalize_text(row.get("text"))
        if not text:
            continue
        key = text.lower()
        prev = index.get(key)
        prev_conf = _extract_confidence(prev, default=0.0) if prev else 0.0
        new_conf = _extract_confidence(row, default=0.0)
        if not prev or new_conf >= prev_conf:
            index[key] = row
    return list(index.values())


def _merge_preference_notes(existing_notes: Any, incoming_notes: Any) -> List[str]:
    out: List[str] = []
    seen = set()
    for source in (existing_notes, incoming_notes):
        if not isinstance(source, list):
            continue
        for item in source:
            note = _normalize_text(item)
            if not note:
                continue
            key = note.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(note[:200])
    return out[:4]


def _infer_domain_from_text(text: str) -> str:
    lower = _normalize_text(text).lower()
    if re.search(r"\b(partner|relationship|friend|family|wife|husband|girlfriend|boyfriend|mother|father|mom|dad|sister|brother)\b", lower):
        return "relationships"
    if re.search(r"\b(work|project|career|business|ship|build|launch|product|startup|company)\b", lower):
        return "work"
    if re.search(r"\b(walk|sleep|health|exercise|gym|diet|bed|steps|run|workout)\b", lower):
        return "health"
    if re.search(r"\b(pray|meditate|spiritual|faith|church|quran|bible|dua)\b", lower):
        return "spirituality"
    return "general"


def _map_loop_domain_to_north_star(loop_domain: Optional[str], text: str) -> str:
    domain = _normalize_text(loop_domain).lower() if loop_domain else ""
    if domain in {"relationships", "family"}:
        return "relationships"
    if domain in {"work", "career"}:
        return "work"
    if domain == "health":
        return "health"
    if domain == "spirituality":
        return "spirituality"
    if domain in {"learning", "finance", "home", "general", ""}:
        return _infer_domain_from_text(text)
    return "general"


def _is_strategic_goal_candidate(loop_item: Any, text: str) -> bool:
    if not text:
        return False
    words = [w for w in re.split(r"\s+", text) if w]
    if len(words) < 3:
        return False

    lower = text.lower()
    tactical_patterns = (
        r"\bwake at\b",
        r"\bleave by\b",
        r"\b\d{1,2}\s?(?:am|pm)\b",
        r"\bafter coffee\b",
        r"\bminutes?\b",
        r"\btoday\b",
    )
    if any(re.search(p, lower) for p in tactical_patterns):
        return False

    loop_type = (getattr(loop_item, "type", "") or "").lower()
    if loop_type not in {"thread", "habit", "decision", "commitment"}:
        return False

    time_horizon = (getattr(loop_item, "timeHorizon", "") or "").lower()
    if time_horizon == "today" and loop_type == "commitment":
        return False

    # Avoid promoting obvious housekeeping chores into north-star goals.
    if re.search(r"\b(tidy|clean|kitchen|worktop|dishes|laundry)\b", lower):
        return False

    return True


def _merge_north_star(current: Any, incoming: Any) -> Dict[str, Any]:
    current_ns = _normalize_north_star(current)
    incoming_ns = _normalize_north_star(incoming)
    merged = deepcopy(current_ns)

    for domain, incoming_entry in incoming_ns.items():
        if not isinstance(incoming_entry, dict):
            continue
        existing_entry = merged.get(domain) if isinstance(merged.get(domain), dict) else {}

        incoming_vision = _normalize_text(incoming_entry.get("vision"))
        incoming_goal = _normalize_text(incoming_entry.get("goal"))
        incoming_status = _normalize_status(incoming_entry.get("status"))
        incoming_vision_conf = _normalize_confidence(incoming_entry.get("vision_confidence"), default=0.7)
        incoming_goal_conf = _normalize_confidence(incoming_entry.get("goal_confidence"), default=0.6)
        incoming_vision_source = incoming_entry.get("vision_source")
        incoming_goal_source = incoming_entry.get("goal_source")

        existing_vision_conf = _normalize_confidence(existing_entry.get("vision_confidence"), default=0.0)
        existing_goal_conf = _normalize_confidence(existing_entry.get("goal_confidence"), default=0.0)
        existing_vision_source = existing_entry.get("vision_source")
        existing_goal_source = existing_entry.get("goal_source")

        # Vision: protect high-confidence user_stated values.
        if incoming_vision:
            if not (
                existing_vision_source == "user_stated"
                and existing_vision_conf > incoming_vision_conf
                and incoming_vision_source != "user_stated"
            ):
                existing_entry["vision"] = incoming_vision
                existing_entry["vision_confidence"] = incoming_vision_conf
                existing_entry["vision_source"] = incoming_vision_source

        # Goal: allow inferred updates but do not downgrade stronger user-stated goals.
        if incoming_goal:
            if not (
                existing_goal_source == "user_stated"
                and existing_goal_conf > incoming_goal_conf
                and incoming_goal_source != "user_stated"
            ):
                existing_entry["goal"] = incoming_goal
                existing_entry["goal_confidence"] = incoming_goal_conf
                existing_entry["goal_source"] = incoming_goal_source

        # Update status only when incoming contains real signal.
        if incoming_vision or incoming_goal:
            existing_entry["status"] = "active" if incoming_status == "unknown" else incoming_status
        elif incoming_status in {"active", "inactive"}:
            existing_entry["status"] = incoming_status

        if incoming_entry.get("updated_at"):
            existing_entry["updated_at"] = incoming_entry.get("updated_at")

        merged[domain] = _normalize_north_star({domain: existing_entry}).get(domain)

    return _normalize_north_star(merged)


def _apply_user_model_proposal(
    current_model: Dict[str, Any],
    proposal: Dict[str, Any]
) -> Dict[str, Any]:
    merged = deepcopy(current_model)

    for field in ("north_star", "current_focus", "work_context", "health", "spirituality", "preferences"):
        incoming = proposal.get(field)
        if incoming is None:
            continue
        existing = merged.get(field)
        if field == "north_star":
            merged[field] = _merge_north_star(existing, incoming)
            continue
        if field == "preferences" and isinstance(existing, dict) and isinstance(incoming, dict):
            # Preserve stable structured preferences and merge notes instead of replacing whole object.
            merged_preferences = deepcopy(existing)
            merged_preferences.update({k: v for k, v in incoming.items() if k != "notes"})
            merged_preferences["notes"] = _merge_preference_notes(
                existing.get("notes"),
                incoming.get("notes")
            )
            merged[field] = merged_preferences
            continue
        incoming_conf = _extract_confidence(incoming, default=0.0)
        existing_conf = _extract_confidence(existing, default=0.0)
        existing_source = (existing or {}).get("source") if isinstance(existing, dict) else None
        # Never let low-confidence inferred updates overwrite stronger user-stated values.
        if existing_source == "user_stated" and existing_conf > incoming_conf:
            continue
        merged[field] = incoming

    if isinstance(proposal.get("key_relationships"), list):
        merged["key_relationships"] = _merge_relationships(
            merged.get("key_relationships") if isinstance(merged.get("key_relationships"), list) else [],
            proposal.get("key_relationships") or []
        )

    if isinstance(proposal.get("patterns"), list):
        merged["patterns"] = _merge_patterns(
            merged.get("patterns") if isinstance(merged.get("patterns"), list) else [],
            proposal.get("patterns") or []
        )

    return _normalize_user_model(merged)


def _explicit_environment_in_text(text: Optional[str]) -> bool:
    """Allow environment only if explicitly stated in most recent user text."""
    if not text:
        return False
    lower = _normalize_text(text).lower()
    patterns = (
        r"\bi'm at\b",
        r"\bi am at\b",
        r"\bi'm in\b",
        r"\bi am in\b",
        r"\bi'm outside\b",
        r"\bi am outside\b",
        r"\bon my walk\b",
        r"\bin the park\b",
    )
    if extract_location(lower):
        return True
    return any(re.search(p, lower) for p in patterns)


def _extract_commitments(texts: List[str], limit: int = 3) -> List[str]:
    commitment_patterns = (
        r"\bi will\b",
        r"\bi'll\b",
        r"\bi plan to\b",
        r"\bi am going to\b",
        r"\bscheduled\b",
        r"\bdeadline\b",
    )
    candidates: List[str] = []
    for text in texts:
        for claim in _split_claims(text):
            lower = claim.lower()
            if any(re.search(pattern, lower) for pattern in commitment_patterns):
                if _allow_claim(claim) and not _is_explicit_user_state_claim(claim):
                    candidates.append(claim)
    return _dedupe_keep_order(candidates, limit=limit)

def _select_current_focus(nodes: List[Dict[str, Any]], now: Optional[datetime] = None) -> Optional[str]:
    now = now or datetime.utcnow()
    if now.tzinfo is not None:
        now = now.replace(tzinfo=None)
    candidates: List[Tuple[datetime, str]] = []
    for node in nodes:
        attrs = node.get("attributes") if isinstance(node, dict) else None
        node_type = (node.get("type") or "").lower() if isinstance(node, dict) else ""
        if node_type != "userfocus" and not (isinstance(attrs, dict) and "focus" in attrs):
            continue
        focus_text = None
        if isinstance(attrs, dict):
            focus_text = attrs.get("focus")
        if not focus_text:
            focus_text = node.get("summary")
        focus_text = _normalize_text(focus_text)
        if not focus_text:
            continue
        if not _allow_claim(focus_text) or _is_explicit_user_state_claim(focus_text):
            continue
        ts = node.get("updated_at") or node.get("reference_time") or node.get("created_at")
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except Exception:
                ts = None
        if isinstance(ts, datetime) and ts.tzinfo is not None:
            ts = ts.replace(tzinfo=None)
        if not isinstance(ts, datetime):
            ts = now
        candidates.append((ts, focus_text[:80]))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    selected_ts, selected_focus = candidates[0]
    if (now - selected_ts).days > 7:
        return None
    return selected_focus


def _build_structured_sheet(
    facts: List[str],
    open_loops: List[str],
    commitments: List[str],
    anchors: Dict[str, Any],
    user_stated_state: Optional[str],
    current_focus: Optional[str],
    max_chars: int = 720
) -> str:
    def _short(value: str, limit: int) -> str:
        clean = _normalize_text(value)
        if len(clean) <= limit:
            return clean
        return clean[: max(0, limit - 3)].rstrip() + "..."

    fact_items = [_short(v, 72) for v in facts[:4]]
    loop_items = [_short(v, 72) for v in open_loops[:3]]
    commitment_items = [_short(v, 72) for v in commitments[:3]]
    state_item = _short(user_stated_state, 90) if user_stated_state else None
    focus_item = _short(current_focus, 80) if current_focus else None

    def _render() -> str:
        lines: List[str] = []
        lines.append("FACTS:")
        for fact in fact_items:
            lines.append(f"- {fact}")
        lines.append("OPEN_LOOPS:")
        for loop in loop_items:
            lines.append(f"- {loop}")
        lines.append("COMMITMENTS:")
        for item in commitment_items:
            lines.append(f"- {item}")
        lines.append("CONTEXT_ANCHORS:")
        for key in ("timeOfDayLabel", "timeGapDescription", "lastInteraction", "sessionId"):
            value = anchors.get(key)
            if value is not None and value != "":
                lines.append(f"- {key}: {_short(str(value), 80)}")
        if state_item:
            lines.append("USER_STATED_STATE:")
            lines.append(f"- {state_item}")
        if focus_item:
            lines.append("CURRENT_FOCUS:")
            lines.append(f"- {focus_item}")
        return "\n".join(lines).strip()

    output = _render()
    while len(output) > max_chars:
        if commitment_items:
            commitment_items.pop()
        elif loop_items:
            loop_items.pop()
        elif fact_items:
            fact_items.pop()
        elif state_item:
            state_item = None
        elif focus_item:
            focus_item = None
        else:
            break
        output = _render()
    return output


async def _get_latest_session_id(tenant_id: str, user_id: str) -> Optional[str]:
    try:
        row = await db.fetchone(
            """
            SELECT session_id
            FROM session_buffer
            WHERE tenant_id = $1 AND user_id = $2
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            tenant_id,
            user_id
        )
        if not row:
            return None
        return row.get("session_id")
    except Exception:
        return None


async def _get_recent_user_texts(
    tenant_id: str,
    user_id: str,
    limit_sessions: int = 6
) -> List[str]:
    rows = await db.fetch(
        """
        SELECT messages
        FROM session_transcript
        WHERE tenant_id = $1 AND user_id = $2
        ORDER BY updated_at DESC
        LIMIT $3
        """,
        tenant_id,
        user_id,
        limit_sessions
    )
    texts: List[str] = []
    for row in rows or []:
        messages = row.get("messages")
        if isinstance(messages, str):
            try:
                messages = json.loads(messages)
            except Exception:
                messages = []
        if not isinstance(messages, list):
            continue
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            if (msg.get("role") or "").lower() != "user":
                continue
            text = _normalize_text(msg.get("text"))
            if text:
                texts.append(text)
    return texts[-120:]


def _propose_user_model_patch(
    user_texts: List[str],
    active_loops: List[Any],
    high_conf: float,
    low_conf: float
) -> Dict[str, Any]:
    now_iso = datetime.utcnow().isoformat()
    proposal: Dict[str, Any] = {}

    north_star_vision_patterns = [
        re.compile(r"\bmy\s+north\s+star\s+is\s+([^.!?\n]+)", re.IGNORECASE),
        re.compile(r"\bi\s+want\s+to\s+be(?:come)?\s+([^.!?\n]+)", re.IGNORECASE),
        re.compile(r"\bi'm\s+trying\s+to\s+be(?:come)?\s+([^.!?\n]+)", re.IGNORECASE),
        re.compile(r"\bthe\s+kind\s+of\s+person\s+i\s+want\s+to\s+be\s+([^.!?\n]+)", re.IGNORECASE),
    ]
    north_star_goal_patterns = [
        re.compile(r"\bmy\s+(?:big\s+)?goal\s+is\s+([^.!?\n]+)", re.IGNORECASE),
        re.compile(r"\bthis\s+year\s+i\s+want\s+to\s+([^.!?\n]+)", re.IGNORECASE),
        re.compile(r"\bmy\s+goal\s+for\s+this\s+month\s+is\s+([^.!?\n]+)", re.IGNORECASE),
    ]
    focus_patterns = [
        re.compile(r"\b(?:right now|currently)\s+i(?:'m| am)\s+(?:focused on|working on|trying to)\s+([^.!?\n]+)", re.IGNORECASE),
        re.compile(r"\bmy\s+focus\s+is\s+([^.!?\n]+)", re.IGNORECASE),
        re.compile(r"\bthis\s+week\s+i(?:'m| am)\s+focused\s+on\s+([^.!?\n]+)", re.IGNORECASE),
    ]
    work_patterns = [
        re.compile(r"\bi(?:'m| am)\s+building\s+([^.!?\n]+)", re.IGNORECASE),
        re.compile(r"\bi(?:'m| am)\s+working\s+on\s+([^.!?\n]+)", re.IGNORECASE),
        re.compile(r"\bwe(?:'re| are)\s+building\s+([^.!?\n]+)", re.IGNORECASE),
    ]

    north_star_update: Dict[str, Any] = {}
    vision = _extract_first_match(user_texts, north_star_vision_patterns)
    if vision:
        domain = _infer_domain_from_text(vision)
        north_star_update[domain] = {
            "vision": vision,
            "status": "active",
            "vision_confidence": high_conf,
            "vision_source": "user_stated",
            "updated_at": now_iso
        }
    explicit_goal = _extract_first_match(user_texts, north_star_goal_patterns)
    if explicit_goal:
        domain = _infer_domain_from_text(explicit_goal)
        entry = north_star_update.get(domain, {})
        entry.update({
            "goal": explicit_goal,
            "status": "active",
            "goal_confidence": high_conf,
            "goal_source": "user_stated",
            "updated_at": now_iso
        })
        north_star_update[domain] = entry

    current_focus = _extract_first_match(user_texts, focus_patterns)
    if current_focus:
        proposal["current_focus"] = {
            "text": current_focus,
            "confidence": high_conf,
            "source": "user_stated",
            "updated_at": now_iso
        }

    work_context = _extract_first_match(user_texts, work_patterns)
    if work_context:
        proposal["work_context"] = {
            "text": work_context,
            "confidence": high_conf,
            "source": "user_stated",
            "updated_at": now_iso
        }

    relationships = _extract_relationships_from_texts(user_texts, confidence=high_conf)
    if relationships:
        proposal["key_relationships"] = relationships

    preference_patterns = (
        r"\b(?:please\s+)?(?:speak|talk)\s+to\s+me\b",
        r"\bi\s+prefer\b",
        r"\bi\s+like\s+when\s+you\b",
        r"\bit\s+helps\s+when\s+you\b",
        r"\bplease\s+(?:don't|do not)\b",
        r"\bavoid\s+(?:saying|using|the)\b",
    )
    profanity_markers = ("fucking", "fuck", "shit", "bitch", "wtf")
    preference_lines = []
    for t in user_texts:
        lower = t.lower()
        if len(t) > 220:
            continue
        if "movie time" in lower:
            continue
        if any(bad in lower for bad in profanity_markers):
            continue
        if any(re.search(p, lower) for p in preference_patterns):
            preference_lines.append(t)
    if preference_lines:
        proposal["preferences"] = {
            "notes": preference_lines[-2:],
            "confidence": high_conf,
            "source": "user_stated",
            "updated_at": now_iso
        }

    explicit_pattern_lines = [
        t for t in user_texts
        if re.search(r"\b(i always|i keep|i tend to|i usually)\b", t, re.IGNORECASE)
    ]
    inferred_patterns: List[Dict[str, Any]] = []
    if explicit_pattern_lines:
        inferred_patterns.append({
            "text": explicit_pattern_lines[-1][:160],
            "confidence": high_conf,
            "source": "user_stated",
            "updated_at": now_iso
        })

    loop_threads = [l for l in active_loops if getattr(l, "type", None) == "thread"]
    if len(active_loops) >= 6:
        inferred_patterns.append({
            "text": "Juggles multiple active commitments and threads",
            "confidence": low_conf,
            "source": "inferred",
            "updated_at": now_iso
        })
    if inferred_patterns:
        proposal["patterns"] = inferred_patterns

    if "current_focus" not in proposal and active_loops:
        top = active_loops[0]
        top_text = _normalize_text(getattr(top, "text", None))
        if top_text:
            proposal["current_focus"] = {
                "text": top_text,
                "confidence": low_conf,
                "source": "inferred",
                "updated_at": now_iso
            }

    if "work_context" not in proposal and loop_threads:
        top_threads = [_normalize_text(getattr(l, "text", "")) for l in loop_threads[:2]]
        top_threads = [t for t in top_threads if t]
        if top_threads:
            proposal["work_context"] = {
                "text": "; ".join(top_threads),
                "confidence": low_conf,
                "source": "inferred",
                "updated_at": now_iso
            }

    health_loops = [l for l in active_loops if _normalize_text((getattr(l, "metadata", {}) or {}).get("domain")).lower() == "health"]
    if health_loops:
        proposal["health"] = {
            "text": _normalize_text(getattr(health_loops[0], "text", "")),
            "confidence": low_conf,
            "source": "inferred",
            "updated_at": now_iso
        }

    spiritual_loops = [l for l in active_loops if _normalize_text((getattr(l, "metadata", {}) or {}).get("domain")).lower() == "spirituality"]
    if spiritual_loops:
        proposal["spirituality"] = {
            "text": _normalize_text(getattr(spiritual_loops[0], "text", "")),
            "confidence": low_conf,
            "source": "inferred",
            "updated_at": now_iso
        }

    # North star goals are allowed from loops/sessions. Prefer per-domain goals from top loops.
    for loop_item in active_loops[:8]:
        text = _normalize_text(getattr(loop_item, "text", ""))
        if not text or not _is_strategic_goal_candidate(loop_item, text):
            continue
        metadata = getattr(loop_item, "metadata", {}) or {}
        loop_domain = _normalize_text(metadata.get("domain")).lower() if metadata.get("domain") else ""
        domain = _map_loop_domain_to_north_star(loop_domain, text)
        if domain not in {"relationships", "work", "health", "spirituality", "general"}:
            domain = "general"
        entry = north_star_update.get(domain, {})
        # Do not overwrite an explicit high-confidence goal with inferred loop goal.
        if _is_populated(entry.get("goal")) and _normalize_confidence(entry.get("goal_confidence"), 0.0) >= high_conf:
            continue
        entry.update({
            "goal": text[:160],
            "status": "active",
            "goal_confidence": low_conf,
            "goal_source": "inferred",
            "updated_at": now_iso
        })
        north_star_update[domain] = entry

    if north_star_update:
        proposal["north_star"] = north_star_update

    return proposal


async def _upsert_user_model(
    tenant_id: str,
    user_id: str,
    model: Dict[str, Any],
    source: str
) -> Optional[Dict[str, Any]]:
    return await db.fetchone(
        """
        INSERT INTO user_model (tenant_id, user_id, model, version, last_source, created_at, updated_at)
        VALUES ($1, $2, $3::jsonb, 1, $4, NOW(), NOW())
        ON CONFLICT (tenant_id, user_id)
        DO UPDATE SET
            model = $3::jsonb,
            version = user_model.version + 1,
            last_source = $4,
            updated_at = NOW()
        RETURNING model, version, updated_at
        """,
        tenant_id,
        user_id,
        model,
        source
    )


async def _run_user_model_updater_once(
    lookback_hours: int,
    max_users: int,
    low_conf: float,
    high_conf: float
) -> int:
    users = await db.fetch(
        """
        WITH recent_sessions AS (
            SELECT tenant_id, user_id, MAX(updated_at) AS ts
            FROM session_transcript
            WHERE updated_at >= NOW() - ($1::int * INTERVAL '1 hour')
            GROUP BY tenant_id, user_id
        ),
        recent_loops AS (
            SELECT tenant_id, user_id, MAX(updated_at) AS ts
            FROM loops
            WHERE updated_at >= NOW() - ($1::int * INTERVAL '1 hour')
            GROUP BY tenant_id, user_id
        ),
        users AS (
            SELECT tenant_id, user_id, MAX(ts) AS ts
            FROM (
                SELECT * FROM recent_sessions
                UNION ALL
                SELECT * FROM recent_loops
            ) x
            GROUP BY tenant_id, user_id
        )
        SELECT tenant_id, user_id
        FROM users
        ORDER BY ts DESC
        LIMIT $2
        """,
        lookback_hours,
        max_users
    )
    if not users:
        return 0

    updates = 0
    for row in users:
        tenant_id = row.get("tenant_id")
        user_id = row.get("user_id")
        if not tenant_id or not user_id:
            continue
        try:
            texts = await _get_recent_user_texts(tenant_id, user_id, limit_sessions=6)
            active_loops = await loops.get_top_loops_for_startbrief(
                tenant_id=tenant_id,
                user_id=user_id,
                limit=12,
                persona_id=None
            )
            proposal = _propose_user_model_patch(
                user_texts=texts,
                active_loops=active_loops or [],
                high_conf=high_conf,
                low_conf=low_conf
            )
            if not proposal:
                continue

            existing = await db.fetchone(
                """
                SELECT model
                FROM user_model
                WHERE tenant_id = $1 AND user_id = $2
                """,
                tenant_id,
                user_id
            )
            current = _normalize_user_model(existing.get("model") if existing else None)
            merged = _apply_user_model_proposal(current, proposal)
            if merged == current:
                continue

            await _upsert_user_model(
                tenant_id=tenant_id,
                user_id=user_id,
                model=merged,
                source="auto_updater"
            )
            updates += 1
        except Exception as e:
            logger.error(f"user model updater failed for {tenant_id}:{user_id}: {e}")
            continue
    return updates


async def user_model_updater_loop(
    interval_seconds: int,
    lookback_hours: int,
    max_users: int,
    low_conf: float,
    high_conf: float
) -> None:
    while True:
        try:
            updates = await _run_user_model_updater_once(
                lookback_hours=lookback_hours,
                max_users=max_users,
                low_conf=low_conf,
                high_conf=high_conf
            )
            if updates:
                logger.info("user model updater applied updates=%s", updates)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"user model updater loop error: {e}")
        await asyncio.sleep(max(30, interval_seconds))


async def loop_staleness_janitor_loop(
    interval_seconds: int
) -> None:
    while True:
        try:
            result = await loops.apply_global_staleness_policy()
            stale_today = int((result or {}).get("stale_today") or 0)
            stale_week = int((result or {}).get("stale_this_week") or 0)
            needs_review = int((result or {}).get("needs_review_ongoing") or 0)
            total = stale_today + stale_week + needs_review
            if total > 0:
                logger.info(
                    "loop staleness janitor updated stale_today=%s stale_this_week=%s needs_review_ongoing=%s",
                    stale_today,
                    stale_week,
                    needs_review
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"loop staleness janitor loop error: {e}")
        await asyncio.sleep(max(300, interval_seconds))


def _safe_parse_json_object(raw: Any) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        return None
    text = raw.strip().replace("\r", "")
    if not text:
        return None
    if text.startswith("```"):
        text = text.strip("`").strip()
    if not text.startswith("{"):
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            text = text[start:end + 1]
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        repaired = re.sub(r",(\s*[}\]])", r"\1", text)
        try:
            parsed = json.loads(repaired)
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None


def _normalize_score_1_to_5(value: Any, default: int = 3) -> int:
    try:
        n = int(round(float(value)))
    except Exception:
        n = default
    return max(1, min(5, n))


def _fallback_daily_analysis(turns: List[Dict[str, str]]) -> Dict[str, Any]:
    user_text = " ".join(
        _normalize_text(t.get("text"))
        for t in turns
        if (t.get("role") or "").lower() == "user"
    ).lower()
    assistant_turns = [
        _normalize_text(t.get("text")).lower()
        for t in turns
        if (t.get("role") or "").lower() == "assistant"
    ]

    themes: List[str] = []
    if re.search(r"\b(stress|anxious|overwhelm|frustrat|irritat|pressure)\b", user_text):
        themes.append("Emotional strain is narrowing attention and decision quality")
    if re.search(r"\b(plan|planning|tomorrow|next|later|should)\b", user_text):
        themes.append("Planning is being used to defer immediate action")
    if re.search(r"\b(stuck|can't|cannot|didn't|did not|avoid|avoiding|procrast)\b", user_text):
        themes.append("Avoidance is appearing as delay and self-protective deflection")
    if re.search(r"\b(low energy|tired|exhausted|aching|cold|bed)\b", user_text):
        themes.append("Low activation energy is driving intention-action gaps")
    if not themes:
        themes = ["Reflective processing without clear behavioral commitment"]

    question_count = sum(text.count("?") for text in assistant_turns)
    curiosity = 3 + (1 if question_count >= 2 else 0) - (1 if question_count == 0 else 0)
    warmth_terms = ("glad", "hear you", "with you", "in your corner", "care", "appreciate")
    warmth_hits = sum(1 for text in assistant_turns if any(term in text for term in warmth_terms))
    warmth = 3 + (1 if warmth_hits >= 1 else 0)
    helpful_terms = ("try", "next", "plan", "step", "could", "let's", "here's")
    usefulness_hits = sum(1 for text in assistant_turns if any(term in text for term in helpful_terms))
    usefulness = 2 + min(2, usefulness_hits)
    forward_terms = ("next", "tomorrow", "this week", "follow up", "plan", "commit")
    forward_hits = sum(1 for text in assistant_turns if any(term in text for term in forward_terms))
    forward_motion = 2 + min(2, forward_hits)

    scores = {
        "curiosity": _normalize_score_1_to_5(curiosity),
        "warmth": _normalize_score_1_to_5(warmth),
        "usefulness": _normalize_score_1_to_5(usefulness),
        "forward_motion": _normalize_score_1_to_5(forward_motion),
    }
    steering = (
        "User repeated planning language more than action evidence today, so start with presence and ask for one"
        " verifiable action before offering encouragement."
    )
    return {
        "themes": themes[:4],
        "scores": scores,
        "steering_note": steering,
        "confidence": 0.45,
        "source": "fallback"
    }


def _extract_transcript_keywords(turns: List[Dict[str, str]], limit: int = 12) -> List[str]:
    text = " ".join(_normalize_text(t.get("text")) for t in turns if isinstance(t, dict))
    if not text:
        return []
    stop = {
        "about", "after", "again", "also", "always", "because", "before", "being", "could", "every",
        "going", "have", "just", "like", "maybe", "more", "really", "should", "still", "that", "then",
        "there", "these", "they", "this", "today", "tomorrow", "want", "with", "would", "your", "from",
        "into", "when", "what", "where", "which", "while", "been", "were", "them", "will", "could",
    }
    counts: Dict[str, int] = {}
    for tok in re.findall(r"[a-zA-Z][a-zA-Z0-9'-]{3,}", text.lower()):
        if tok in stop:
            continue
        counts[tok] = counts.get(tok, 0) + 1
    ranked = sorted(counts.items(), key=lambda kv: (kv[1], len(kv[0])), reverse=True)
    return [k for k, _ in ranked[:limit]]


def _parse_optional_dt(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    raw = _normalize_text(value)
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def should_use_bridge(
    last_session_end: Optional[datetime],
    now: datetime,
    ttl_minutes: Optional[int] = None
) -> bool:
    if not last_session_end or not now:
        return False
    if last_session_end.tzinfo is None:
        last_session_end = last_session_end.replace(tzinfo=dt_timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt_timezone.utc)
    ttl = int(ttl_minutes if ttl_minutes is not None else get_settings().session_bridge_ttl_minutes)
    if ttl <= 0:
        return False
    delta = now - last_session_end
    if delta.total_seconds() < 0:
        return False
    return delta <= timedelta(minutes=ttl)


def _theme_looks_task_like(theme: str) -> bool:
    lower = _normalize_text(theme).lower()
    if not lower:
        return True
    task_patterns = (
        r"\b(plan|planning|schedule|tidy|clean|walk|run|watch|movie|email|call|meeting|bedtime|wake)\b",
        r"\b(morning|afternoon|evening|tonight|tomorrow)\b",
        r"\b(to do|todo|checklist|task)\b",
    )
    psych_markers = (
        r"\b(pattern|avoid|resistance|activation|deflection|emotion|cognitive|belief|self|intention)\b",
    )
    if any(re.search(p, lower) for p in psych_markers):
        return False
    return any(re.search(p, lower) for p in task_patterns)


def _steering_is_too_generic(steering_note: str, turns: List[Dict[str, str]]) -> bool:
    lower = _normalize_text(steering_note).lower()
    if not lower:
        return True
    generic_patterns = (
        r"\b(keep going|you got this|stay positive|enjoy|continue planning|be consistent)\b",
        r"\b(lead with presence|stay warm and curious)\b$",
    )
    if any(re.search(p, lower) for p in generic_patterns):
        return True
    keywords = _extract_transcript_keywords(turns, limit=14)
    keyword_hits = sum(1 for kw in keywords if kw in lower)
    evidence_markers = (
        r"\b(repeated|three times|twice|didn't|did not|kept|again|by evening|today|across)\b",
    )
    has_evidence_language = any(re.search(p, lower) for p in evidence_markers)
    return keyword_hits < 1 or not has_evidence_language


def _extract_summary_evidence_terms(
    session_summaries: List[Dict[str, Any]],
    themes: Optional[List[str]] = None,
    limit: int = 20
) -> List[str]:
    counts: Dict[str, int] = {}
    blobs: List[str] = []
    for row in session_summaries or []:
        blobs.append(_normalize_text(row.get("summary_facts")))
        blobs.append(_normalize_text(row.get("moment")))
        for key in ("decisions", "unresolved"):
            values = row.get(key) or []
            if isinstance(values, list):
                for item in values:
                    blobs.append(_normalize_text(item))
    for t in themes or []:
        blobs.append(_normalize_text(t))
    text = " ".join(b for b in blobs if b)
    for tok in re.findall(r"[a-zA-Z][a-zA-Z0-9'-]{3,}", text.lower()):
        if tok in {"user", "assistant", "session", "today", "tomorrow", "with", "that", "this"}:
            continue
        counts[tok] = counts.get(tok, 0) + 1
    ranked = sorted(counts.items(), key=lambda kv: (kv[1], len(kv[0])), reverse=True)
    return [k for k, _ in ranked[:limit]]


def _steering_references_evidence(steering_note: str, evidence_terms: List[str]) -> bool:
    lower = _normalize_text(steering_note).lower()
    if not lower:
        return False
    for term in evidence_terms or []:
        if term and term in lower:
            return True
    return False


def _daily_analysis_rejection_reasons(
    normalized: Dict[str, Any],
    turns: List[Dict[str, str]],
    evidence_terms: Optional[List[str]] = None
) -> List[str]:
    reasons: List[str] = []
    themes = normalized.get("themes") or []
    if not themes:
        reasons.append("Themes are empty.")
    task_like = [t for t in themes if _theme_looks_task_like(str(t))]
    if task_like:
        reasons.append(f"Themes are task/event-like instead of psychological patterns: {task_like[:2]}")
    steering = _normalize_text(normalized.get("steering_note"))
    if _steering_is_too_generic(steering, turns):
        reasons.append("Steering note is generic or not grounded in transcript evidence.")
    if evidence_terms is not None and not _steering_references_evidence(steering, evidence_terms):
        reasons.append("Steering note must reference a theme/decision/unresolved/stated plan from evidence.")
    return reasons


def _normalize_daily_analysis_payload(payload: Dict[str, Any], turns: List[Dict[str, str]]) -> Dict[str, Any]:
    fallback = _fallback_daily_analysis(turns)
    themes_raw = payload.get("themes") if isinstance(payload, dict) else None
    themes: List[str] = []
    if isinstance(themes_raw, list):
        for item in themes_raw:
            text = _normalize_text(item)
            if text and text.lower() not in {t.lower() for t in themes}:
                themes.append(text[:120])
            if len(themes) >= 4:
                break
    if not themes:
        themes = fallback["themes"]

    scores_raw = payload.get("scores") if isinstance(payload, dict) else None
    scores = {
        "curiosity": _normalize_score_1_to_5((scores_raw or {}).get("curiosity"), default=fallback["scores"]["curiosity"]),
        "warmth": _normalize_score_1_to_5((scores_raw or {}).get("warmth"), default=fallback["scores"]["warmth"]),
        "usefulness": _normalize_score_1_to_5((scores_raw or {}).get("usefulness"), default=fallback["scores"]["usefulness"]),
        "forward_motion": _normalize_score_1_to_5((scores_raw or {}).get("forward_motion"), default=fallback["scores"]["forward_motion"]),
    }

    steering_note = _normalize_text(payload.get("steering_note") if isinstance(payload, dict) else None)
    if not steering_note:
        steering_note = fallback["steering_note"]
    steering_note = steering_note[:220]

    confidence = _normalize_confidence(
        payload.get("confidence") if isinstance(payload, dict) else None,
        default=fallback["confidence"]
    )
    source = "llm" if steering_note and isinstance(payload, dict) and payload.get("steering_note") else fallback["source"]
    return {
        "themes": themes,
        "scores": scores,
        "steering_note": steering_note,
        "confidence": confidence,
        "source": source
    }


async def _get_daily_analysis_users(
    day_start: datetime,
    day_end: datetime,
    max_users: int
) -> List[Dict[str, Any]]:
    return await db.fetch(
        """
        SELECT tenant_id, user_id, MAX(updated_at) AS last_seen
        FROM session_transcript
        WHERE updated_at >= $1
          AND updated_at < $2
        GROUP BY tenant_id, user_id
        ORDER BY last_seen DESC
        LIMIT $3
        """,
        day_start,
        day_end,
        max_users
    )


async def _get_user_daily_turns(
    tenant_id: str,
    user_id: str,
    day_start: datetime,
    day_end: datetime,
    max_turns: int
) -> Tuple[List[Dict[str, str]], List[str]]:
    rows = await db.fetch(
        """
        SELECT session_id, messages, updated_at
        FROM session_transcript
        WHERE tenant_id = $1
          AND user_id = $2
          AND updated_at >= $3
          AND updated_at < $4
        ORDER BY updated_at ASC
        """,
        tenant_id,
        user_id,
        day_start,
        day_end
    )
    turns: List[Dict[str, str]] = []
    session_ids: List[str] = []
    for row in rows:
        sid = _normalize_text(row.get("session_id"))
        if sid:
            session_ids.append(sid)
        messages = row.get("messages") or []
        if not isinstance(messages, list):
            continue
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            role = _normalize_text(msg.get("role")).lower()
            if role not in {"user", "assistant"}:
                continue
            text = _normalize_text(msg.get("text"))
            if not text:
                continue
            turns.append({"role": role, "text": text[:280]})
    if len(turns) > max_turns:
        turns = turns[-max_turns:]
    return turns, session_ids


def _coerce_summary_salience(value: Any) -> str:
    raw = _normalize_text(value).lower()
    if raw in {"high", "medium", "low"}:
        return raw
    return "low"


def _salience_rank(value: str) -> int:
    return {"high": 0, "medium": 1, "low": 2}.get(_coerce_summary_salience(value), 2)


def _trim_daily_summary_inputs(
    summaries: List[Dict[str, Any]],
    max_sessions: int,
    char_budget: int,
) -> List[Dict[str, Any]]:
    if not summaries:
        return []

    def _created_ts(row: Dict[str, Any]) -> float:
        dt = _parse_optional_dt(row.get("created_at"))
        if not dt:
            return 0.0
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=dt_timezone.utc)
        return float(dt.timestamp())

    selected_pool = sorted(
        summaries,
        key=lambda s: (
            _salience_rank(s.get("salience") or "low"),
            -_created_ts(s),
        )
    )[: max(1, max_sessions)]

    ordered = sorted(
        selected_pool,
        key=lambda s: (
            _salience_rank(s.get("salience") or "low"),
            _created_ts(s),
        )
    )
    out: List[Dict[str, Any]] = []
    remaining = max(300, int(char_budget))
    for item in ordered:
        payload = dict(item)
        text = _normalize_text(payload.get("index_text") or payload.get("summary_facts") or "")
        if not text:
            continue
        # Leave room for wrapper tokens/labels.
        wrapper = 100
        allowed = max(0, remaining - wrapper)
        if allowed <= 0:
            if _coerce_summary_salience(payload.get("salience")) == "low":
                continue
            allowed = min(180, remaining)
        if len(text) > allowed:
            text = text[:allowed].rstrip(" ,;")
        if not text:
            continue
        payload["index_text"] = text
        out.append(payload)
        remaining -= len(text) + wrapper
        if remaining <= 0:
            break
    return out


async def _get_user_daily_session_summaries(
    tenant_id: str,
    user_id: str,
    day_start: datetime,
    day_end: datetime,
    max_sessions: int,
    prompt_char_budget: int,
) -> List[Dict[str, Any]]:
    nodes = await graphiti_client.get_recent_session_summary_nodes(
        tenant_id=tenant_id,
        user_id=user_id,
        limit=max(max_sessions * 4, 40),
    )
    rows: List[Dict[str, Any]] = []
    for node in nodes or []:
        attrs = node.get("attributes") if isinstance(node, dict) else {}
        created = _parse_optional_dt(
            node.get("created_at")
            or (attrs.get("created_at") if isinstance(attrs, dict) else None)
            or (attrs.get("reference_time") if isinstance(attrs, dict) else None)
        )
        if not created:
            continue
        if created.tzinfo is not None:
            created_cmp = created.astimezone(dt_timezone.utc).replace(tzinfo=None)
        else:
            created_cmp = created
        if created_cmp < day_start or created_cmp >= day_end:
            continue
        summary_facts = _normalize_text(
            (attrs.get("summary_facts") if isinstance(attrs, dict) else None)
            or node.get("summary_text")
            or node.get("summary")
        )
        tone = _normalize_text((attrs.get("tone") if isinstance(attrs, dict) else None))
        moment = _normalize_text((attrs.get("moment") if isinstance(attrs, dict) else None))
        decisions = attrs.get("decisions") if isinstance(attrs, dict) else []
        unresolved = attrs.get("unresolved") if isinstance(attrs, dict) else []
        if not isinstance(decisions, list):
            decisions = []
        if not isinstance(unresolved, list):
            unresolved = []
        index_text = _normalize_text((attrs.get("index_text") if isinstance(attrs, dict) else None))
        if not index_text:
            index_text = " ".join(
                p for p in [
                    summary_facts,
                    moment,
                    ("Decisions: " + "; ".join(_normalize_text(x) for x in decisions if _normalize_text(x))) if decisions else "",
                    ("Open loops: " + "; ".join(_normalize_text(x) for x in unresolved if _normalize_text(x))) if unresolved else "",
                ] if p
            ).strip()
        rows.append({
            "session_id": _normalize_text(node.get("session_id") or (attrs.get("session_id") if isinstance(attrs, dict) else None)),
            "created_at": created.isoformat(),
            "salience": _coerce_summary_salience((attrs.get("salience") if isinstance(attrs, dict) else None)),
            "summary_facts": summary_facts,
            "tone": tone,
            "moment": moment,
            "decisions": [_normalize_text(x) for x in decisions if _normalize_text(x)],
            "unresolved": [_normalize_text(x) for x in unresolved if _normalize_text(x)],
            "index_text": index_text,
        })
    return _trim_daily_summary_inputs(rows, max_sessions=max_sessions, char_budget=prompt_char_budget)


async def _generate_daily_analysis(
    turns: List[Dict[str, str]],
    session_summaries: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    use_summaries = bool(session_summaries)
    if not turns and not use_summaries:
        return _fallback_daily_analysis(turns)
    llm = get_llm_client()
    transcript = "\n".join(f"{t['role']}: {t['text']}" for t in turns)
    summary_rows = session_summaries or []

    if use_summaries:
        summary_payload = []
        for row in summary_rows:
            summary_payload.append(
                {
                    "session_id": row.get("session_id"),
                    "created_at": row.get("created_at"),
                    "salience": row.get("salience") or "low",
                    "summary_facts": row.get("summary_facts") or "",
                    "tone": row.get("tone") or "",
                    "moment": row.get("moment") or "",
                    "decisions": row.get("decisions") or [],
                    "unresolved": row.get("unresolved") or [],
                    "index_text": row.get("index_text") or "",
                }
            )
        prompt = (
            "You are analyzing one user's day using session summaries (not raw transcripts).\n"
            "Return strict JSON with keys: themes, scores, steering_note, confidence.\n"
            "Evidence weighting rules:\n"
            "- Treat high-salience sessions as strongest evidence.\n"
            "- Medium salience supports patterns.\n"
            "- Low salience may be ignored unless it repeats a pattern.\n"
            "Output rules:\n"
            "- themes: 2-4 short phrases about dominant psychological/emotional patterns (thought, emotion, behavior).\n"
            "- Reject any theme that could appear on a to-do list. Themes must describe what's under the surface.\n"
            "- scores: curiosity, warmth, usefulness, forward_motion each integer 1-5.\n"
            "- steering_note: exactly one sentence for tomorrow; concrete, grounded in evidence from summaries.\n"
            "- steering_note must reference at least one theme, decision, unresolved item, or stated plan from evidence.\n"
            "- Anti-drift rule: if steering_note could apply to any user on any day, reject and regenerate.\n"
            "- confidence: 0-1.\n"
            "JSON schema example:\n"
            "{\"themes\":[\"...\"],\"scores\":{\"curiosity\":3,\"warmth\":4,\"usefulness\":3,\"forward_motion\":3},\"steering_note\":\"...\",\"confidence\":0.72}\n"
            f"SESSION_SUMMARIES_JSON:\n{json.dumps(summary_payload, ensure_ascii=True)}\n"
        )
        rejection_turns: List[Dict[str, str]] = [
            {"role": "user", "text": _normalize_text(row.get("index_text") or row.get("summary_facts") or "")}
            for row in summary_rows
            if _normalize_text(row.get("index_text") or row.get("summary_facts") or "")
        ]
    else:
        prompt = (
            "You are analyzing one user's day of chat transcripts.\n"
            "Return strict JSON with keys: themes, scores, steering_note, confidence.\n"
            "Rules:\n"
            "- themes: 2-4 short phrases about dominant psychological/emotional patterns (thought, emotion, behavior).\n"
            "- Reject any theme that could appear on a to-do list. Themes must describe what's under the surface.\n"
            "- scores: curiosity, warmth, usefulness, forward_motion each integer 1-5.\n"
            "- steering_note: exactly one sentence for tomorrow; concrete, grounded in today's evidence.\n"
            "- steering_note must reference at least one theme, decision, unresolved item, or stated plan from evidence.\n"
            "- Anti-drift rule: if steering_note could apply to any user on any day, reject and regenerate.\n"
            "- steering_note must cite specific observed behavior from today's transcript.\n"
            "- confidence: 0-1.\n"
            "JSON schema example:\n"
            "{\"themes\":[\"...\"],\"scores\":{\"curiosity\":3,\"warmth\":4,\"usefulness\":3,\"forward_motion\":3},\"steering_note\":\"...\",\"confidence\":0.72}\n"
            f"TRANSCRIPT:\n{transcript}\n"
        )
        rejection_turns = turns

    for attempt in range(2):
        raw = await llm._call_llm(
            prompt=prompt,
            max_tokens=500,
            temperature=0.15,
            task="daily_analysis"
        )
        parsed = _safe_parse_json_object(raw) if raw else None
        if not parsed:
            continue
        normalized = _normalize_daily_analysis_payload(parsed, rejection_turns)
        evidence_terms = _extract_summary_evidence_terms(summary_rows, normalized.get("themes")) if use_summaries else _extract_transcript_keywords(rejection_turns, limit=16)
        reasons = _daily_analysis_rejection_reasons(normalized, rejection_turns, evidence_terms=evidence_terms)
        if not reasons:
            return normalized
        prompt = (
            "Previous output was rejected. Regenerate a better JSON response.\n"
            f"Rejection reasons: {'; '.join(reasons)}\n"
            "Hard constraints:\n"
            "- themes must be psychological/emotional patterns only.\n"
            "- no task/event wording.\n"
            "- steering_note must reference specific behavior seen today.\n"
            "- steering_note must reference at least one theme, decision, unresolved item, or stated plan from evidence.\n"
            + (
                f"SESSION_SUMMARIES_JSON:\n{json.dumps(summary_payload, ensure_ascii=True)}\n"
                if use_summaries else
                f"TRANSCRIPT:\n{transcript}\n"
            )
        )
    return _fallback_daily_analysis(turns)


async def _upsert_daily_analysis(
    tenant_id: str,
    user_id: str,
    analysis_date: date,
    analysis: Dict[str, Any],
    metadata: Optional[Dict[str, Any]] = None
) -> None:
    await db.execute(
        """
        INSERT INTO daily_analysis (
            tenant_id, user_id, analysis_date, themes, scores, steering_note,
            confidence, source, metadata, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7, $8, $9::jsonb, NOW(), NOW())
        ON CONFLICT (tenant_id, user_id, analysis_date)
        DO UPDATE SET
            themes = EXCLUDED.themes,
            scores = EXCLUDED.scores,
            steering_note = EXCLUDED.steering_note,
            confidence = EXCLUDED.confidence,
            source = EXCLUDED.source,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
        """,
        tenant_id,
        user_id,
        analysis_date,
        analysis.get("themes") or [],
        analysis.get("scores") or {},
        _normalize_text(analysis.get("steering_note")) or None,
        float(analysis.get("confidence") or 0.0),
        _normalize_text(analysis.get("source")) or "llm",
        metadata or {}
    )


async def _compute_daily_analysis_quality_flag(
    tenant_id: str,
    user_id: str,
    analysis_date: date,
    confidence: float,
    turn_count: int
) -> Optional[str]:
    if turn_count < 3:
        return "insufficient_data"

    threshold = float(getattr(get_settings(), "daily_analysis_low_confidence_threshold", 0.6))
    current_low = float(confidence) < threshold
    if not current_low:
        return None

    prev_row = await db.fetchone(
        """
        SELECT confidence
        FROM daily_analysis
        WHERE tenant_id = $1
          AND user_id = $2
          AND analysis_date = $3
        LIMIT 1
        """,
        tenant_id,
        user_id,
        analysis_date - timedelta(days=1)
    )
    if not prev_row:
        return None
    prev_conf = prev_row.get("confidence")
    try:
        prev_low = float(prev_conf) < threshold
    except Exception:
        prev_low = False
    return "needs_review" if prev_low else None


async def _run_daily_analysis_once(
    target_date: date,
    max_users: int,
    max_turns: int
) -> int:
    day_start = datetime.combine(target_date, datetime.min.time())
    day_end = day_start + timedelta(days=1)
    users = await _get_daily_analysis_users(day_start, day_end, max_users=max_users)
    if not users:
        return 0
    updates = 0
    for row in users:
        tenant_id = row.get("tenant_id")
        user_id = row.get("user_id")
        if not tenant_id or not user_id:
            continue
        try:
            turns, session_ids = await _get_user_daily_turns(
                tenant_id=tenant_id,
                user_id=user_id,
                day_start=day_start,
                day_end=day_end,
                max_turns=max_turns
            )
            settings = get_settings()
            session_summaries = await _get_user_daily_session_summaries(
                tenant_id=tenant_id,
                user_id=user_id,
                day_start=day_start,
                day_end=day_end,
                max_sessions=int(settings.daily_analysis_max_sessions),
                prompt_char_budget=int(settings.daily_analysis_prompt_char_budget),
            )
            input_mode = "session_summaries" if session_summaries else "fallback_raw_turns"
            used_turn_tail = not bool(session_summaries)
            if not session_summaries and not turns:
                continue
            analysis = await _generate_daily_analysis(
                turns=turns,
                session_summaries=session_summaries if session_summaries else None,
            )
            if used_turn_tail and analysis.get("source") == "llm":
                analysis["source"] = "fallback_raw_turns"
            salience_counts = {"high": 0, "medium": 0, "low": 0}
            summary_ids: List[str] = []
            for row_summary in session_summaries:
                sal = _coerce_summary_salience(row_summary.get("salience"))
                salience_counts[sal] += 1
                sid = _normalize_text(row_summary.get("session_id"))
                if sid:
                    summary_ids.append(sid)
            unique_session_ids = _dedupe_keep_order(summary_ids or session_ids, limit=20)
            evidence_turn_count = len(session_summaries) if session_summaries else len(turns)
            quality_flag = await _compute_daily_analysis_quality_flag(
                tenant_id=tenant_id,
                user_id=user_id,
                analysis_date=target_date,
                confidence=float(analysis.get("confidence") or 0.0),
                turn_count=evidence_turn_count
            )
            metadata = {
                "sessions": unique_session_ids,
                "turn_count": len(turns),
                "session_count": len(session_summaries),
                "session_ids_used": unique_session_ids,
                "salience_counts": salience_counts,
                "used_turn_tail": used_turn_tail,
                "input_mode": input_mode,
                "analysis_version": "v2"
            }
            if quality_flag:
                metadata["quality_flag"] = quality_flag
            await _upsert_daily_analysis(
                tenant_id=tenant_id,
                user_id=user_id,
                analysis_date=target_date,
                analysis=analysis,
                metadata=metadata
            )
            updates += 1
        except Exception as e:
            logger.error(f"daily analysis failed for {tenant_id}:{user_id}: {e}")
    return updates


async def _run_daily_analysis_for_user(
    tenant_id: str,
    user_id: str,
    target_date: date,
    max_turns: int
) -> bool:
    day_start = datetime.combine(target_date, datetime.min.time())
    day_end = day_start + timedelta(days=1)
    turns, session_ids = await _get_user_daily_turns(
        tenant_id=tenant_id,
        user_id=user_id,
        day_start=day_start,
        day_end=day_end,
        max_turns=max_turns
    )
    settings = get_settings()
    session_summaries = await _get_user_daily_session_summaries(
        tenant_id=tenant_id,
        user_id=user_id,
        day_start=day_start,
        day_end=day_end,
        max_sessions=int(settings.daily_analysis_max_sessions),
        prompt_char_budget=int(settings.daily_analysis_prompt_char_budget),
    )
    input_mode = "session_summaries" if session_summaries else "fallback_raw_turns"
    used_turn_tail = not bool(session_summaries)
    if not session_summaries and not turns:
        return False
    analysis = await _generate_daily_analysis(
        turns=turns,
        session_summaries=session_summaries if session_summaries else None,
    )
    if used_turn_tail and analysis.get("source") == "llm":
        analysis["source"] = "fallback_raw_turns"
    salience_counts = {"high": 0, "medium": 0, "low": 0}
    summary_ids: List[str] = []
    for row_summary in session_summaries:
        sal = _coerce_summary_salience(row_summary.get("salience"))
        salience_counts[sal] += 1
        sid = _normalize_text(row_summary.get("session_id"))
        if sid:
            summary_ids.append(sid)
    unique_session_ids = _dedupe_keep_order(summary_ids or session_ids, limit=20)
    evidence_turn_count = len(session_summaries) if session_summaries else len(turns)
    quality_flag = await _compute_daily_analysis_quality_flag(
        tenant_id=tenant_id,
        user_id=user_id,
        analysis_date=target_date,
        confidence=float(analysis.get("confidence") or 0.0),
        turn_count=evidence_turn_count
    )
    metadata = {
        "sessions": unique_session_ids,
        "turn_count": len(turns),
        "session_count": len(session_summaries),
        "session_ids_used": unique_session_ids,
        "salience_counts": salience_counts,
        "used_turn_tail": used_turn_tail,
        "input_mode": input_mode,
        "analysis_version": "v2"
    }
    if quality_flag:
        metadata["quality_flag"] = quality_flag
    await _upsert_daily_analysis(
        tenant_id=tenant_id,
        user_id=user_id,
        analysis_date=target_date,
        analysis=analysis,
        metadata=metadata
    )
    return True


async def daily_analysis_loop(
    interval_seconds: int,
    target_offset_days: int,
    max_users: int,
    max_turns: int
) -> None:
    while True:
        try:
            offset = max(0, int(target_offset_days))
            target_date = (datetime.utcnow() - timedelta(days=offset)).date()
            updates = await _run_daily_analysis_once(
                target_date=target_date,
                max_users=max_users,
                max_turns=max_turns
            )
            if updates:
                logger.info("daily analysis updated users=%s target_date=%s", updates, target_date.isoformat())
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"daily analysis loop error: {e}")
        await asyncio.sleep(max(3600, interval_seconds))


async def _get_latest_steering_note(
    tenant_id: str,
    user_id: str,
    reference_date: date
) -> Optional[str]:
    row = await db.fetchone(
        """
        SELECT steering_note
        FROM daily_analysis
        WHERE tenant_id = $1
          AND user_id = $2
          AND analysis_date < $3
        ORDER BY analysis_date DESC
        LIMIT 1
        """,
        tenant_id,
        user_id,
        reference_date
    )
    note = _normalize_text((row or {}).get("steering_note") if isinstance(row, dict) else None)
    return note or None


async def _get_yesterday_analysis_context(
    tenant_id: str,
    user_id: str,
    reference_date: date
) -> Dict[str, Any]:
    yesterday_date = reference_date - timedelta(days=1)
    row = await db.fetchone(
        """
        SELECT analysis_date, themes, steering_note
        FROM daily_analysis
        WHERE tenant_id = $1
          AND user_id = $2
          AND analysis_date = $3
        LIMIT 1
        """,
        tenant_id,
        user_id,
        yesterday_date
    )
    themes: List[str] = []
    steering_note: Optional[str] = None
    if row:
        raw_themes = row.get("themes") or []
        if isinstance(raw_themes, list):
            for item in raw_themes:
                text = _normalize_text(item)
                if text and text.lower() not in {t.lower() for t in themes}:
                    themes.append(text)
                if len(themes) >= 4:
                    break
        steering_note = _normalize_text(row.get("steering_note")) or None
    return {
        "date": yesterday_date.isoformat(),
        "themes": themes,
        "steering_note": steering_note
    }


def _extract_high_confidence_user_model_hints(model: Dict[str, Any], threshold: float) -> List[str]:
    hints: List[str] = []

    current_focus = model.get("current_focus")
    if isinstance(current_focus, dict):
        conf = _normalize_confidence(current_focus.get("confidence"), default=0.0)
        focus_text = _normalize_text(current_focus.get("text"))
        if focus_text and conf >= threshold:
            hints.append(f"Current focus: {focus_text}")

    work_context = model.get("work_context")
    if isinstance(work_context, dict):
        conf = _normalize_confidence(work_context.get("confidence"), default=0.0)
        text = _normalize_text(work_context.get("text"))
        if text and conf >= threshold:
            hints.append(f"Work context: {text}")

    north_star = model.get("north_star")
    if isinstance(north_star, dict):
        for domain in ("relationships", "work", "health", "spirituality", "general"):
            entry = north_star.get(domain)
            if not isinstance(entry, dict):
                continue
            vision = _normalize_text(entry.get("vision"))
            goal = _normalize_text(entry.get("goal"))
            v_conf = _normalize_confidence(entry.get("vision_confidence"), default=0.0)
            g_conf = _normalize_confidence(entry.get("goal_confidence"), default=0.0)
            if vision and v_conf >= threshold:
                hints.append(f"{domain} vision: {vision}")
            if goal and g_conf >= threshold:
                hints.append(f"{domain} goal: {goal}")

    relationships = model.get("key_relationships")
    if isinstance(relationships, list):
        for rel in relationships:
            if not isinstance(rel, dict):
                continue
            conf = _normalize_confidence(rel.get("confidence"), default=0.0)
            if conf < threshold:
                continue
            name = _normalize_text(rel.get("name"))
            who = _normalize_text(rel.get("who"))
            status = _normalize_text(rel.get("status"))
            if name and who and status:
                hints.append(f"Relationship: {name} ({who}), currently {status}")
            elif name and who:
                hints.append(f"Relationship: {name} ({who})")
            if len(hints) >= 8:
                break

    patterns = model.get("patterns")
    if isinstance(patterns, list):
        for row in patterns:
            if not isinstance(row, dict):
                continue
            conf = _normalize_confidence(row.get("confidence"), default=0.0)
            text = _normalize_text(row.get("text"))
            if text and conf >= threshold:
                hints.append(f"Pattern: {text}")
            if len(hints) >= 8:
                break

    return _dedupe_keep_order(hints, limit=8)


def _clean_startbrief_bridge_output(raw: Optional[str]) -> Optional[str]:
    text = _normalize_text(raw)
    if not text:
        return None
    text = re.sub(r"\s*[\"“”]+\s*", " ", text).strip()
    text = re.sub(r"\s+", " ", text).strip()
    parts = re.split(r"(?<=[.!?])\s+", text)
    cleaned: List[str] = []
    for part in parts:
        sentence = _normalize_text(part)
        if not sentence:
            continue
        sentence = re.sub(r"^[A-Za-z][A-Za-z ]{1,28}:\s*", "", sentence)
        cleaned.append(sentence)
    if not cleaned:
        return None
    if len(cleaned) < 3:
        return None
    return " ".join(cleaned[:5]).strip()


_BRIDGE_DISALLOWED_REGEXES = (
    r"\bshould\b",
    r"\bneed to\b",
    r"\bit['’]s time to\b",
    r"\bit is time to\b",
    r"\bencourage\b",
    r"\bplease\b",
    r"\bprioritize\b",
    r"\byou can\b",
    r"\bvital\b",
    r"\bpriority\b",
    r"\bthemes showed\b",
    r"\btendency to\b",
    r"\bfocusing on\b",
    r"\bmaintaining\b",
    r"\bconsistent\b",
    r"\bdiscipline\b",
    r"\b20\d{2}\b",
    r"\bon a (monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b",
    r"\b(january|february|march|april|may|june|july|august|september|october|november|december)\b",
    r"\bhalf past\b",
    r"\bquarter past\b",
    r"\bquarter to\b",
    r"\?",
)


def _bridge_has_disallowed_language(text: Optional[str]) -> bool:
    value = _normalize_text(text).lower()
    if not value:
        return False
    return any(re.search(pattern, value) for pattern in _BRIDGE_DISALLOWED_REGEXES)


def _strip_disallowed_bridge_sentences(text: Optional[str]) -> Optional[str]:
    raw = _normalize_text(text)
    if not raw:
        return None
    parts = re.split(r"(?<=[.!?])\s+", raw)
    kept: List[str] = []
    for sentence in parts:
        clean = _normalize_text(sentence)
        if not clean:
            continue
        if _bridge_has_disallowed_language(clean):
            continue
        kept.append(clean)
    if len(kept) < 2:
        return None
    return " ".join(kept[:5]).strip()


def _natural_time_phrase(time_of_day: str, time_gap_human: Optional[str]) -> str:
    label = (time_of_day or "").upper()
    if label == "MORNING":
        tod = "morning"
    elif label == "AFTERNOON":
        tod = "mid-afternoon"
    elif label == "EVENING":
        tod = "evening"
    else:
        tod = "late night"
    if time_gap_human:
        return f"It's {tod}, and it's been about {time_gap_human}."
    return f"It's {tod}."


def _naturalize_bridge_time_language(
    text: Optional[str],
    time_of_day: str,
    time_gap_human: Optional[str]
) -> Optional[str]:
    raw = _normalize_text(text)
    if not raw:
        return None
    sentences = [s for s in re.split(r"(?<=[.!?])\s+", raw) if _normalize_text(s)]
    formal_time_re = re.compile(
        r"\b20\d{2}\b|"
        r"\bon a (monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b|"
        r"\b(january|february|march|april|may|june|july|august|september|october|november|december)\b|"
        r"\bhalf past\b|\bquarter past\b|\bquarter to\b",
        flags=re.IGNORECASE
    )
    kept: List[str] = []
    for sentence in sentences:
        clean = _normalize_text(sentence)
        if not clean:
            continue
        if formal_time_re.search(clean):
            continue
        kept.append(clean)

    has_natural_time = any(
        re.search(r"\bit'?s (mid-afternoon|morning|evening|late night)\b", s, flags=re.IGNORECASE)
        or re.search(r"\bbeen about\b.*\bsince last spoke\b", s, flags=re.IGNORECASE)
        for s in kept
    )
    if not has_natural_time:
        kept.insert(0, _natural_time_phrase(time_of_day, time_gap_human))

    if len(kept) < 2:
        return None
    return " ".join(kept[:5]).strip()


async def _generate_startbrief_bridge_llm(
    narrative_ingredients: Dict[str, Any],
    depth_label: str,
    rewrite_only: bool = False
) -> Optional[str]:
    llm_client = get_llm_client()
    word_cap = {
        "continuation": 35,
        "today": 80,
        "yesterday": 95,
        "multi_day": 120,
    }.get(depth_label, 95)
    now_local = _normalize_text(narrative_ingredients.get("now_local"))
    gap_human = _normalize_text(narrative_ingredients.get("gap_human"))
    session_frequency = _normalize_text(narrative_ingredients.get("session_frequency"))
    last_thread = _normalize_text(narrative_ingredients.get("last_thread"))
    user_tone = _normalize_text(narrative_ingredients.get("user_tone"))
    open_threads = [_normalize_text(x) for x in (narrative_ingredients.get("open_threads") or []) if _normalize_text(x)]
    if rewrite_only:
        prompt = (
            "Rewrite the ORIENTATION BRIEF to satisfy all constraints exactly.\n"
            "Do not add facts. Do not remove real facts. Rephrase only.\n\n"
            "Constraints:\n"
            "- Address the assistant as \"you\" and refer to the human as \"the user\".\n"
            "- Include NOW_LOCAL once, GAP_HUMAN once, and SESSION_FREQUENCY once.\n"
            "- Include LAST_THREAD and USER_TONE in neutral phrasing.\n"
            "- Include OPEN_THREADS (1-2 max).\n"
            "- No advice or imperatives: avoid should/could/try/maybe/a good idea.\n"
            "- No filler verbs: reported/stated/noted/mentioned/said.\n"
            "- No years (20xx), no full calendar dates, no clock time format HH:MM.\n"
            "- One paragraph only.\n"
            f"- Word cap for DEPTH={depth_label}: {word_cap} words.\n\n"
            "Inputs:\n"
            f"DEPTH: {depth_label}\n"
            f"NOW_LOCAL: {now_local}\n"
            f"GAP_HUMAN: {gap_human}\n"
            f"SESSION_FREQUENCY: {session_frequency}\n"
            f"LAST_THREAD: {last_thread}\n"
            f"USER_TONE: {user_tone}\n"
            f"OPEN_THREADS: {json.dumps(open_threads, ensure_ascii=True)}\n\n"
            "Return ONLY the rewritten paragraph."
        )
    else:
        prompt = (
            "Write a short ORIENTATION BRIEF addressed to the assistant (\"you\") about the user (\"the user\").\n"
            "This text will be prepended as context for a stateless LLM call. It is not shown to the user.\n\n"
            "Include these elements, in order:\n"
            "1) Current local time + time-of-day\n"
            "2) Time gap since last session\n"
            "3) Conversation frequency\n"
            "4) Most recent thread and observed user tone\n"
            "5) Remaining open threads (1-2 max)\n\n"
            "Constraints:\n"
            "- Use \"you\" only for the assistant; use \"the user\" for the human.\n"
            "- Neutral, factual tone. No conversational fluff.\n"
            "- No advice or imperatives. Avoid should/could/try/maybe/a good idea.\n"
            "- No filler verbs: reported/stated/noted/mentioned/said.\n"
            "- No years (20xx). Avoid full calendar dates and HH:MM clock times.\n"
            "- One paragraph only.\n"
            f"- Word cap for DEPTH={depth_label}: {word_cap}.\n\n"
            "Inputs:\n"
            f"DEPTH: {depth_label}\n"
            f"NOW_LOCAL: {now_local}\n"
            f"GAP_HUMAN: {gap_human}\n"
            f"SESSION_FREQUENCY: {session_frequency}\n"
            f"LAST_THREAD: {last_thread}\n"
            f"USER_TONE: {user_tone}\n"
            f"OPEN_THREADS: {json.dumps(open_threads, ensure_ascii=True)}\n\n"
            "Return ONLY the paragraph."
        )
    response = await llm_client._call_llm(
        prompt=prompt,
        max_tokens=220,
        temperature=0.1,
        task="startbrief_bridge"
    )
    return _normalize_text(response) or None


_STARTBRIEF_ADVICE_PATTERNS = (
    r"\bshould\b",
    r"\bcould\b",
    r"\btry\b",
    r"\bmaybe\b",
    r"\ba good idea\b",
    r"\bit would be good\b",
)

_STARTBRIEF_HALLUCINATION_BLACKLIST = (
    "coffee still ready",
    "weather",
    "rain",
    "sunny",
)

_STARTBRIEF_BUREAUCRATIC_PATTERNS = (
    r"\bAt \d{1,2}:\d{2}\b",
    r"\bon (January|February|March|April|May|June|July|August|September|October|November|December)\b.*\b20\d{2}\b",
    r"\b20\d{2}\b",
)

_STARTBRIEF_BUREAUCRATIC_PHRASES = (
    "were reported",
    "ready to proceed",
    "next tasks",
    "No other emotionally significant",
    "no other emotionally significant",
    "worked out just fine",
    "big events",
)

_LOW_VALUE_LOOP_PATTERNS = (
    r"\bcoffee\b",
    r"\bweather\b",
    r"\bstroll\b",
    r"\bmovie time\b",
    r"\bgo out after\b",
)


def _handover_word_cap(depth_label: str) -> int:
    return {
        "continuation": 35,
        "today": 80,
        "yesterday": 95,
        "multi_day": 120,
    }.get(depth_label, 95)


def _handover_mentions_advice(text: str) -> bool:
    lower = _normalize_text(text).lower()
    return any(re.search(p, lower) for p in _STARTBRIEF_ADVICE_PATTERNS)


def _handover_mentions_blacklist(text: str) -> bool:
    lower = _normalize_text(text).lower()
    return any(term in lower for term in _STARTBRIEF_HALLUCINATION_BLACKLIST)


def _validate_handover_text(
    text: str,
    depth_label: str,
    required_context: Optional[Dict[str, Any]] = None
) -> List[str]:
    reasons: List[str] = []
    clean = _normalize_text(text)
    if not clean:
        reasons.append("empty")
        return reasons
    if _handover_mentions_advice(clean):
        reasons.append("advice_language")
    if _handover_mentions_blacklist(clean):
        reasons.append("hallucinated_continuity")
    if any(re.search(p, clean) for p in _STARTBRIEF_BUREAUCRATIC_PATTERNS):
        reasons.append("bureaucratic_timestamp_or_date")
    clean_lower = clean.lower()
    if any(phrase.lower() in clean_lower for phrase in _STARTBRIEF_BUREAUCRATIC_PHRASES):
        reasons.append("bureaucratic_phrase")
    if re.search(r"\b(reported|stated|noted|mentioned|said)\b", clean_lower):
        reasons.append("filler_verb")
    if ".with" in clean_lower:
        reasons.append("malformed_spacing")
    if re.search(r"\b(?:user\s+)?tone:\b|\bopen threads:\b|\btone:\b", clean_lower):
        reasons.append("label_style_fragment")
    if depth_label != "continuation" and "the user" not in clean_lower:
        reasons.append("missing_user_subject")
    if re.search(r"\byou (seemed|felt|were|was)\b", clean_lower):
        reasons.append("wrong_you_reference")
    if re.search(r"\b(steering note|themes?|user model|hints?)\b", clean_lower):
        reasons.append("ops_context_leak")
    if len(re.findall(r"\b(gap|since last spoke|hours since|minutes since)\b", clean.lower())) > 1:
        reasons.append("repeated_time_gap")
    if required_context:
        now_local = _normalize_text(required_context.get("now_local")).lower()
        gap_human = _normalize_text(required_context.get("gap_human")).lower()
        session_frequency = _normalize_text(required_context.get("session_frequency")).lower()
        if now_local and now_local not in clean_lower:
            reasons.append("missing_now_local")
        if gap_human and gap_human not in clean_lower:
            reasons.append("missing_gap_human")
        if session_frequency and session_frequency not in clean_lower:
            reasons.append("missing_session_frequency")
    if len(re.findall(r"\b\w+\b", clean)) > _handover_word_cap(depth_label):
        reasons.append("over_word_cap")
    if depth_label == "continuation":
        sentence_count = len([s for s in re.split(r"[.!?]+", clean) if _normalize_text(s)])
        if sentence_count > 2:
            reasons.append("continuation_too_long")
    return reasons


def _truncate_at_word_boundary(value: str, limit: int) -> str:
    clean = _normalize_text(value)
    if len(clean) <= limit:
        return clean
    cutoff = max(0, limit - 3)
    candidate = clean[:cutoff]
    last_space = candidate.rfind(" ")
    if last_space > 0:
        candidate = candidate[:last_space]
    candidate = candidate.rstrip(" ,.;:")
    return candidate + "..."


def _truncate_to_word_cap(value: str, word_cap: int) -> str:
    clean = _normalize_text(value)
    words = re.findall(r"\S+", clean)
    if len(words) <= word_cap:
        return clean
    return _normalize_text(" ".join(words[:word_cap]))


def _ensure_sentence_spacing(value: str) -> str:
    clean = _normalize_text(value)
    if not clean:
        return ""
    return re.sub(r"([.!?])([A-Za-z])", r"\1 \2", clean)


def _humanize_thread_text(value: str) -> str:
    t = _normalize_text(value)
    if not t:
        return ""
    lower = t.lower()
    transforms = (
        (r"^complete\s+", "finishing "),
        (r"^finish\s+", "finishing "),
        (r"^clear\s+", "clearing "),
        (r"^set\s+", "setting "),
        (r"^go to\b", "getting to"),
        (r"^stop\s+", "stopping "),
    )
    for pattern, repl in transforms:
        if re.match(pattern, lower):
            return re.sub(pattern, repl, lower, count=1)
    return lower


def _natural_open_threads_clause(open_threads: List[str]) -> str:
    items = [_humanize_thread_text(x) for x in open_threads if _normalize_text(x)]
    items = [x for x in items if x][:2]
    if not items:
        return ""
    if len(items) == 1:
        return f"What's still open is {items[0]}."
    return f"What's still open is {items[0]} and {items[1]}."


def _fallback_handover_text(ingredients: Dict[str, Any], depth_label: str) -> str:
    last_thread = _ensure_sentence_spacing(_normalize_text(ingredients.get("last_thread")))
    user_tone = _normalize_text(ingredients.get("user_tone"))
    open_threads = [_normalize_text(x) for x in (ingredients.get("open_threads") or []) if _normalize_text(x)]
    now_local = _normalize_text(ingredients.get("now_local")) or "the current local time context is available"
    gap_human = _normalize_text(ingredients.get("gap_human")) or "an unknown interval"
    session_frequency = _normalize_text(ingredients.get("session_frequency")) or "session frequency is limited"

    tone_clause = ""
    if user_tone and user_tone.lower() not in {"neutral", "steady", "calm", "ok", "fine"}:
        tone_clause = f"The user came in {user_tone}."
    threads_clause = _natural_open_threads_clause(open_threads)
    if depth_label == "continuation":
        base = (
            f"It is {now_local}. You last spoke with the user {gap_human} ago. {session_frequency}. "
            f"The latest thread is {last_thread or 'ongoing'}. {threads_clause}"
        )
    elif depth_label == "today":
        base = (
            f"It is {now_local}. You last spoke with the user {gap_human} ago. {session_frequency}. "
            f"The latest thread is {last_thread or 'ongoing'}. {tone_clause} {threads_clause}"
        )
    elif depth_label == "yesterday":
        base = (
            f"It is {now_local}. You last spoke with the user {gap_human} ago. {session_frequency}. "
            f"The latest thread is {last_thread or 'ongoing'}. {tone_clause} {threads_clause}"
        )
    else:
        base = (
            f"It is {now_local}. You last spoke with the user {gap_human} ago. {session_frequency}. "
            f"The latest thread across recent sessions is {last_thread or 'ongoing'}. {tone_clause} {threads_clause}"
        )

    text = _truncate_at_word_boundary(base, {"continuation": 260, "today": 420, "yesterday": 520, "multi_day": 620}.get(depth_label, 520))
    return _truncate_to_word_cap(text, _handover_word_cap(depth_label))


def _sanitize_handover_tone(text: str) -> str:
    clean = _normalize_text(text)
    if not clean:
        return ""
    # Remove explicit timestamps/dates and rigid bureaucratic wording.
    clean = re.sub(r"\bAt \d{1,2}:\d{2}\b(?:\s+in\s+the\s+(?:morning|afternoon|evening|night))?,?\s*", "", clean, flags=re.IGNORECASE)
    clean = re.sub(
        r"\bon\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+20\d{2}\b,?\s*",
        "earlier today, ",
        clean,
        flags=re.IGNORECASE,
    )
    clean = re.sub(r"\b20\d{2}\b", "", clean)
    clean = re.sub(r"\b(reported|stated|noted)\b", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\bready to proceed\b", "ready to pick this up", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\bnext tasks?\b", "next step", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\bwith user tone\b", "with the user sounding", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\b(?:user\s+)?tone:\s*neutral\b\.?", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\b(?:user\s+)?tone:\s*([^.]+)", r"The user sounded \1", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\bopen threads:\s*", "The open threads are ", clean, flags=re.IGNORECASE)
    clean = re.sub(r"\bopen threads are\s+([A-Z])", lambda m: f"The open threads are {m.group(1).lower()}", clean, flags=re.IGNORECASE)
    # Drop stiff negative boilerplate sentence entirely.
    clean = re.sub(r"[^.]*no other emotionally significant[^.]*\.\s*", "", clean, flags=re.IGNORECASE)
    clean = re.sub(r"([.!?])([A-Za-z])", r"\1 \2", clean)
    clean = re.sub(r"\bThe\s+The\b", "The", clean)
    clean = re.sub(r"\.\.+", ".", clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    # Clean punctuation artifacts from removals.
    clean = re.sub(r"\s+,", ",", clean)
    clean = re.sub(r",\s*,", ", ", clean)
    clean = re.sub(r"\s+\.", ".", clean)
    return clean.strip(" ,")


def _is_low_value_loop(loop_text: str, salience: Optional[int]) -> bool:
    text = _normalize_text(loop_text).lower()
    if not text:
        return True
    if any(re.search(p, text) for p in _LOW_VALUE_LOOP_PATTERNS):
        # Allow only very explicit high-salience overrides.
        return int(salience or 0) < 8
    if int(salience or 0) >= 5:
        return False
    return False


_STARTBRIEF_PLACEHOLDER_VALUES = {
    "summary_text",
    "summary",
    "session_id",
    "index_text",
    "name",
    "created_at",
    "reference_time",
    "uuid",
    "group_id",
    "attributes",
}


def _is_placeholder_value(value: Any) -> bool:
    text = _normalize_text(value).strip().strip("\"'").lower()
    return bool(text) and text in _STARTBRIEF_PLACEHOLDER_VALUES


def _clean_startbrief_value(value: Any, stats: Optional[Dict[str, Any]] = None) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    if _is_placeholder_value(text):
        if isinstance(stats, dict):
            stats["placeholders_blocked"] = int(stats.get("placeholders_blocked") or 0) + 1
        return ""
    return text


def _first_clean_startbrief_value(*values: Any, stats: Optional[Dict[str, Any]] = None) -> str:
    for value in values:
        clean = _clean_startbrief_value(value, stats=stats)
        if clean:
            return clean
    return ""


def _unwrap_node_like(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    if isinstance(raw.get("properties"), dict):
        return raw["properties"]
    if isinstance(raw.get("props"), dict):
        return raw["props"]
    return raw


def _extract_startbrief_summary_identifier(raw: Any) -> str:
    node = _unwrap_node_like(raw)
    if not node:
        return ""
    attrs = _unwrap_node_like(node.get("attributes")) if isinstance(node.get("attributes"), dict) else {}
    if not isinstance(attrs, dict):
        attrs = {}
    return _first_clean_startbrief_value(
        node.get("session_id"),
        attrs.get("session_id"),
        node.get("uuid"),
        attrs.get("uuid"),
        node.get("name"),
        stats=None,
    )


def _normalize_startbrief_session_summary_node(raw: Any, stats: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    node = _unwrap_node_like(raw)
    if not node:
        return None
    attrs = _unwrap_node_like(node.get("attributes")) if isinstance(node.get("attributes"), dict) else {}
    if not isinstance(attrs, dict):
        attrs = {}

    session_id = _first_clean_startbrief_value(node.get("session_id"), attrs.get("session_id"), stats=stats)
    created_at = _first_clean_startbrief_value(
        node.get("created_at"),
        attrs.get("created_at"),
        node.get("reference_time"),
        attrs.get("reference_time"),
        stats=stats,
    )
    summary_facts = _clean_startbrief_value(attrs.get("summary_facts"), stats=stats)
    summary_text = _first_clean_startbrief_value(node.get("summary_text"), attrs.get("summary_text"), stats=stats)
    summary = _first_clean_startbrief_value(node.get("summary"), attrs.get("summary"), stats=stats)
    index_text = _first_clean_startbrief_value(node.get("index_text"), attrs.get("index_text"), stats=stats)
    latest_thread_text = summary_facts or summary_text or summary or index_text
    if _is_placeholder_value(latest_thread_text):
        logger.warning("startbrief summary placeholder stripped session_id=%s latest_thread_text=%s", session_id or "unknown", latest_thread_text)
        if isinstance(stats, dict):
            stats["placeholders_blocked"] = int(stats.get("placeholders_blocked") or 0) + 1
        latest_thread_text = ""

    unresolved = attrs.get("unresolved") if isinstance(attrs.get("unresolved"), list) else []
    decisions = attrs.get("decisions") if isinstance(attrs.get("decisions"), list) else []

    return {
        "session_id": session_id,
        "created_at": created_at,
        "reference_time": _first_clean_startbrief_value(node.get("reference_time"), attrs.get("reference_time"), stats=stats),
        "summary_facts": latest_thread_text,
        "summary_text": summary_text,
        "summary": summary,
        "index_text": index_text,
        "latest_thread_text": latest_thread_text,
        "tone": _clean_startbrief_value(attrs.get("tone"), stats=stats),
        "moment": _clean_startbrief_value(attrs.get("moment"), stats=stats),
        "unresolved": unresolved,
        "decisions": decisions,
        "salience": _clean_startbrief_value(attrs.get("salience"), stats=stats) or "low",
        "bridge_text": _first_clean_startbrief_value(node.get("bridge_text"), attrs.get("bridge_text"), stats=stats),
        "attributes": attrs,
    }


def _is_durable_commitment_text(text: str) -> bool:
    t = _normalize_text(text).lower()
    if not t:
        return False
    durable_patterns = (
        r"\b(walk|run|gym|sleep|wake|bed|morning routine|habit)\b",
        r"\b(portfolio|release|checkpoint|ship|deploy|work|project)\b",
    )
    return any(re.search(p, t) for p in durable_patterns)


def _loop_priority(loop: Dict[str, Any]) -> Tuple[int, int]:
    loop_type = _normalize_text(loop.get("type")).lower()
    type_rank = {"goal": 0, "thread": 1, "habit": 2, "commitment": 3}.get(loop_type, 4)
    salience = int(loop.get("salience") or 0)
    return (type_rank, -salience)


def _ordinal(n: int) -> str:
    if 10 <= (n % 100) <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def _format_now_local(reference_now: datetime, time_of_day_label: str) -> str:
    weekday = reference_now.strftime("%A")
    hour = reference_now.hour
    if hour == 12:
        return f"{weekday} around noon"
    if hour >= 13:
        return f"{weekday} this afternoon"
    tod = {
        "MORNING": "this morning",
        "AFTERNOON": "this afternoon",
        "EVENING": "this evening",
        "NIGHT": "tonight",
    }.get((time_of_day_label or "").upper(), "today")
    return f"{weekday} {tod}"


def _format_gap_human(gap_minutes: Optional[int]) -> str:
    if gap_minutes is None:
        return "an unknown interval"
    if gap_minutes >= 90:
        rounded_hours = max(1, int(round(gap_minutes / 60.0)))
        unit = "hour" if rounded_hours == 1 else "hours"
        return f"about {rounded_hours} {unit}"
    if gap_minutes < 60:
        unit = "minute" if gap_minutes == 1 else "minutes"
        return f"{gap_minutes} {unit}"
    hours = gap_minutes // 60
    minutes = gap_minutes % 60
    hour_unit = "hour" if hours == 1 else "hours"
    if minutes == 0:
        return f"{hours} {hour_unit}"
    minute_unit = "minute" if minutes == 1 else "minutes"
    return f"{hours} {hour_unit} {minutes} {minute_unit}"


def _session_frequency_phrase(sessions_today_count: int) -> str:
    if sessions_today_count <= 0:
        return "This is the 1st conversation today"
    if sessions_today_count >= 3:
        return "You have spoken multiple times today"
    return f"This is the {_ordinal(sessions_today_count + 1)} conversation today"


def select_startbrief_ingredients(
    reference_now: datetime,
    last_session_end: Optional[datetime],
    sessions_today_count: int,
    time_of_day_label: str,
    recent_session_summaries: List[Dict[str, Any]],
    yesterday_daily_analysis: Dict[str, Any],
    top_active_loops: List[Dict[str, Any]],
    user_model_hints: List[str],
) -> Dict[str, Any]:
    gap_minutes: Optional[int] = None
    if last_session_end:
        if last_session_end.tzinfo is None:
            last_session_end = last_session_end.replace(tzinfo=reference_now.tzinfo or dt_timezone.utc)
        gap_minutes = int(max(0, (reference_now - last_session_end).total_seconds() // 60))

    if gap_minutes is not None and gap_minutes <= 30:
        depth_label = "continuation"
    elif gap_minutes is not None and gap_minutes <= 360:
        depth_label = "today"
    elif gap_minutes is not None and gap_minutes <= 1440:
        depth_label = "yesterday"
    else:
        depth_label = "multi_day"

    now_utc = reference_now.astimezone(dt_timezone.utc) if reference_now.tzinfo else reference_now.replace(tzinfo=dt_timezone.utc)
    in_24h: List[Dict[str, Any]] = []
    for s in recent_session_summaries or []:
        created = _parse_optional_dt(s.get("created_at"))
        if not created:
            continue
        created_utc = created.astimezone(dt_timezone.utc) if created.tzinfo else created.replace(tzinfo=dt_timezone.utc)
        if (now_utc - created_utc) <= timedelta(hours=24):
            in_24h.append(s)
    if not in_24h:
        in_24h = recent_session_summaries[:]

    def _salience_rank_local(v: Any) -> int:
        return {"high": 0, "medium": 1, "low": 2}.get(_normalize_text(v).lower(), 2)

    highest = sorted(
        in_24h,
        key=lambda s: (_salience_rank_local(s.get("salience")), -int((_parse_optional_dt(s.get("created_at")) or datetime.min.replace(tzinfo=dt_timezone.utc)).timestamp()))
    )
    selected: List[Dict[str, Any]] = []
    if highest:
        selected.append(highest[0])
    most_recent = sorted(
        in_24h,
        key=lambda s: -int((_parse_optional_dt(s.get("created_at")) or datetime.min.replace(tzinfo=dt_timezone.utc)).timestamp())
    )
    if most_recent:
        recent = most_recent[0]
        if (not selected or recent.get("session_id") != selected[0].get("session_id")) and _normalize_text(recent.get("salience")).lower() in {"high", "medium"}:
            selected.append(recent)
    selected = selected[:2]

    filtered_loops: List[Dict[str, Any]] = []
    for loop in top_active_loops or []:
        text = _normalize_text(loop.get("text"))
        if not text:
            continue
        if _is_low_value_loop(text, loop.get("salience")):
            continue
        loop_type = _normalize_text(loop.get("type")).lower()
        if loop_type == "commitment" and not _is_durable_commitment_text(text):
            continue
        horizon = _normalize_text(loop.get("timeHorizon")).lower()
        salience_value = int(loop.get("salience") or 0)
        if horizon == "today" or salience_value >= 5:
            filtered_loops.append(loop)
    if not filtered_loops:
        filtered_loops = [
            l for l in (top_active_loops or [])
            if _normalize_text(l.get("text")) and not _is_low_value_loop(_normalize_text(l.get("text")), l.get("salience"))
        ][:2]

    filtered_loops = sorted(filtered_loops, key=_loop_priority)[:2]

    last_thread_parts: List[str] = []
    tone_parts: List[str] = []
    unresolved_items: List[str] = []
    for s in selected:
        facts = _normalize_text(
            s.get("latest_thread_text")
            or s.get("summary_facts")
            or s.get("summary_text")
            or s.get("summary")
            or s.get("index_text")
        )
        if _is_placeholder_value(facts):
            logger.warning("startbrief latest_thread placeholder stripped session_id=%s value=%s", _normalize_text(s.get("session_id")), facts)
            facts = ""
        tone = _normalize_text(s.get("tone"))
        moment = _normalize_text(s.get("moment"))
        unresolved = s.get("unresolved")
        if not unresolved and isinstance(s.get("attributes"), dict):
            unresolved = s.get("attributes", {}).get("unresolved")
        line = " ".join([p for p in [facts, tone, moment] if p]).strip()
        if line:
            last_thread_parts.append(_shorten_line(line, 220))
        if tone:
            tone_parts.append(tone)
        if isinstance(unresolved, list):
            for item in unresolved:
                clean = _normalize_text(item)
                if clean:
                    unresolved_items.append(clean)
        elif isinstance(unresolved, str):
            clean = _normalize_text(unresolved)
            if clean:
                unresolved_items.append(clean)

    continuation_hint = None
    if depth_label == "continuation":
        for s in selected:
            bridge = _normalize_text(s.get("bridge_text"))
            if bridge:
                continuation_hint = bridge
                break

    open_threads = _dedupe_keep_order(unresolved_items + [_normalize_text(l.get("text")) for l in filtered_loops], limit=2)
    gap_human = _format_gap_human(gap_minutes)
    session_frequency = _session_frequency_phrase(sessions_today_count)
    now_local = _format_now_local(reference_now, time_of_day_label)

    return {
        "depth_label": depth_label,
        "gap_minutes": gap_minutes,
        "sessions_today_count": sessions_today_count,
        "selected_summaries": selected,
        "last_thread": " ".join(last_thread_parts[:2]).strip(),
        "user_tone": _normalize_text(" ".join(_dedupe_keep_order(tone_parts, limit=2))),
        "open_threads": open_threads,
        "open_loops": [_normalize_text(l.get("text")) for l in filtered_loops[:2] if _normalize_text(l.get("text"))],
        "continuation_hint": continuation_hint,
        "now_state": _normalize_text(filtered_loops[0].get("text")) if filtered_loops else "",
        "now_local": now_local,
        "gap_human": gap_human,
        "session_frequency": session_frequency,
        "yesterday_themes": [_normalize_text(x) for x in (yesterday_daily_analysis.get("themes") or []) if _normalize_text(x)][:2],
        "steering_note": _normalize_text(yesterday_daily_analysis.get("steering_note")) or None,
        "user_model_hints": [_normalize_text(x) for x in user_model_hints if _normalize_text(x)][:6],
    }


def compose_ops_context(
    ingredients: Dict[str, Any],
    top_active_loops: List[Dict[str, Any]],
) -> Dict[str, Any]:
    top_loops_today: List[Dict[str, Any]] = []
    filtered = []
    for loop in top_active_loops:
        text = _normalize_text(loop.get("text"))
        if not text:
            continue
        if _is_low_value_loop(text, loop.get("salience")):
            continue
        loop_type = _normalize_text(loop.get("type")).lower()
        if loop_type == "commitment" and not _is_durable_commitment_text(text):
            continue
        filtered.append(loop)
    filtered = sorted(filtered, key=_loop_priority)[:2]
    for loop in filtered:
        top_loops_today.append(
            {
                "text": _normalize_text(loop.get("text")),
                "type": _normalize_text(loop.get("type")) or None,
                "time_horizon": _normalize_text(loop.get("timeHorizon")) or None,
                "salience": int(loop.get("salience") or 0) if loop.get("salience") is not None else None,
            }
        )
    return {
        "top_loops_today": top_loops_today,
        "waiting_on": [],
        "user_model_hints": ingredients.get("user_model_hints", [])[:6],
        "yesterday_themes": ingredients.get("yesterday_themes", [])[:2],
        "steering_note": ingredients.get("steering_note"),
    }


async def _log_startbrief_history(
    tenant_id: str,
    user_id: str,
    session_id: Optional[str],
    requested_at: datetime,
    time_of_day_label: Optional[str],
    time_gap_human: Optional[str],
    bridge_text: Optional[str],
    items: List[Dict[str, Any]],
    context: Optional[Dict[str, Any]] = None
) -> None:
    await db.execute(
        """
        INSERT INTO startbrief_history (
            tenant_id, user_id, session_id, requested_at,
            time_of_day_label, time_gap_human, bridge_text, items, context
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb)
        """,
        tenant_id,
        user_id,
        session_id,
        requested_at,
        time_of_day_label,
        time_gap_human,
        bridge_text,
        items or [],
        context or {}
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown"""
    # Startup
    logger.info("Starting Synapse Memory API")
    try:
        # Initialize database pool
        if db.pool is not None:
            try:
                await db.close()
            except Exception:
                db.pool = None
        await db.get_pool()
        logger.info("Database connection pool initialized")

        # Run migrations
        await run_migrations(db)
        logger.info("Migrations completed")

        # Initialize Graphiti
        await graphiti_client.initialize()
        logger.info("Graphiti client initialized")

        # Initialize managers
        session.init_session_manager(db)
        logger.info("Session manager initialized")
        loops.init_loop_manager(db)
        logger.info("Loop manager initialized")

        settings = get_settings()
        if settings.idle_close_enabled:
            app.state.idle_close_task = asyncio.create_task(
                session.idle_close_loop(
                    graphiti_client=graphiti_client,
                    interval_seconds=settings.idle_close_interval_seconds,
                    idle_minutes=settings.idle_close_threshold_minutes,
                    batch_size=settings.idle_close_batch_size
                )
            )
            logger.info("Idle close loop started")
        if settings.outbox_drain_enabled:
            app.state.outbox_drain_task = asyncio.create_task(
                session.drain_loop(
                    graphiti_client=graphiti_client,
                    interval_seconds=settings.outbox_drain_interval_seconds,
                    limit=settings.outbox_drain_limit,
                    budget_seconds=settings.outbox_drain_budget_seconds,
                    per_row_timeout_seconds=settings.outbox_drain_per_row_timeout_seconds
                )
            )
            logger.info("Outbox drain loop started")
        if settings.user_model_updater_enabled:
            app.state.user_model_updater_task = asyncio.create_task(
                user_model_updater_loop(
                    interval_seconds=settings.user_model_updater_interval_seconds,
                    lookback_hours=settings.user_model_updater_lookback_hours,
                    max_users=settings.user_model_updater_max_users,
                    low_conf=settings.user_model_low_confidence,
                    high_conf=settings.user_model_high_confidence
                )
            )
            logger.info("User model updater loop started")
        if settings.loop_staleness_janitor_enabled:
            app.state.loop_staleness_janitor_task = asyncio.create_task(
                loop_staleness_janitor_loop(
                    interval_seconds=settings.loop_staleness_janitor_interval_seconds
                )
            )
            logger.info("Loop staleness janitor loop started")
        if settings.daily_analysis_enabled:
            app.state.daily_analysis_task = asyncio.create_task(
                daily_analysis_loop(
                    interval_seconds=settings.daily_analysis_interval_seconds,
                    target_offset_days=settings.daily_analysis_target_offset_days,
                    max_users=settings.daily_analysis_max_users,
                    max_turns=settings.daily_analysis_max_turns
                )
            )
            logger.info("Daily analysis loop started")

    except Exception as e:
        logger.error(f"Failed to initialize services: {e}")
        raise

    yield

    # Shutdown
    logger.info("Shutting down Synapse Memory API")
    if getattr(app.state, "idle_close_task", None):
        app.state.idle_close_task.cancel()
        try:
            await app.state.idle_close_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
    if getattr(app.state, "outbox_drain_task", None):
        app.state.outbox_drain_task.cancel()
        try:
            await app.state.outbox_drain_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
    if getattr(app.state, "user_model_updater_task", None):
        app.state.user_model_updater_task.cancel()
        try:
            await app.state.user_model_updater_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
    if getattr(app.state, "loop_staleness_janitor_task", None):
        app.state.loop_staleness_janitor_task.cancel()
        try:
            await app.state.loop_staleness_janitor_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
    if getattr(app.state, "daily_analysis_task", None):
        app.state.daily_analysis_task.cancel()
        try:
            await app.state.daily_analysis_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
    await db.close()
    logger.info("Database connection pool closed")


# Create FastAPI app with lifespan
app = FastAPI(
    title="Synapse Memory API",
    version="1.0.0",
    lifespan=lifespan
)


def _require_internal_token(token: str | None) -> None:
    settings = get_settings()
    if not settings.internal_token or not token or token != settings.internal_token:
        raise HTTPException(status_code=401, detail="Unauthorized")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "synapse",
        "version": "1.0.0"
    }


@app.post("/ingest", response_model=IngestResponse)
async def ingest(request: IngestRequest, background_tasks: BackgroundTasks):
    """
    Ingest a conversation turn into the memory system.

    This endpoint:
    - Adds messages to session buffer
    - Triggers background janitor (folding + outbox)
    """
    try:
        logger.info(f"Ingesting message from {request.tenantId}:{request.userId}")
        response = await process_ingest(request, graphiti_client, background_tasks)
        return response

    except Exception as e:
        logger.error(f"Ingest endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/brief", response_model=BriefResponse)
async def brief(request: BriefRequest):
    """
    Generate a minimal briefing for session start.

    This endpoint assembles:
    - Temporal authority (current time/day)
    - Working memory (recent messages)
    - Rolling summary (if available)

    Semantic memory is queried on-demand via /memory/query.
    """
    try:
        logger.info(f"Building briefing for {request.tenantId}:{request.userId}")

        # Parse timestamp
        now = datetime.fromisoformat(request.now.replace('Z', '+00:00'))

        # Build briefing
        response = await build_briefing(
            tenant_id=request.tenantId,
            user_id=request.userId,
            persona_id=request.personaId,
            session_id=request.sessionId,
            query=request.query,
            now=now,
            graphiti_client=graphiti_client
        )

        return response

    except Exception as e:
        logger.error(f"Brief endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post(
    "/memory/query",
    response_model=MemoryQueryResponse,
    response_model_exclude_none=True
)
async def memory_query(request: MemoryQueryRequest):
    """
    Query Graphiti for semantic memory snippets on-demand.
    """
    try:
        reference_time = None
        if request.referenceTime:
            value = request.referenceTime
            if value.endswith("Z"):
                value = value.replace("Z", "+00:00")
            reference_time = datetime.fromisoformat(value)

        facts = await graphiti_client.search_facts(
            tenant_id=request.tenantId,
            user_id=request.userId,
            query=request.query,
            limit=request.limit or 10,
            reference_time=reference_time
        )
        entities = await graphiti_client.search_nodes(
            tenant_id=request.tenantId,
            user_id=request.userId,
            query=request.query,
            limit=min(request.limit or 10, 10),
            reference_time=reference_time
        )
        fact_texts_raw = [_normalize_text(f.get("text")) for f in facts if f.get("text")]

        fact_texts = _dedupe_keep_order(
            [
                t for t in fact_texts_raw
                if _allow_claim(t) and _allow_fact_text(t) and not _is_explicit_user_state_claim(t)
            ],
            limit=4
        )
        fact_models = [
            Fact(text=text, relevance=f.get("relevance"), source=f.get("source", "graphiti"))
            for text in fact_texts
            for f in facts
            if _normalize_text(f.get("text")) == text
        ][:4]

        entity_models = []
        for e in entities:
            summary = _normalize_text(e.get("summary"))
            if not summary:
                continue
            entity_models.append(Entity(summary=summary, type=e.get("type"), uuid=e.get("uuid")))

        response = MemoryQueryResponse(
            facts=fact_texts,
            factItems=fact_models,
            entities=entity_models,
            metadata={
                "query": request.query,
                "responseMode": "context" if request.includeContext else "recall",
                "facts": len(fact_models),
                "entities": len(entity_models),
                "limit": request.limit or 10
            }
        )
        if not request.includeContext:
            return response

        focus_nodes = await graphiti_client.search_nodes(
            tenant_id=request.tenantId,
            user_id=request.userId,
            query="current focus priority focused on right now today i need",
            limit=3,
            reference_time=reference_time
        )

        user_stated_state = None
        for text in fact_texts_raw:
            state = _extract_explicit_user_state(text)
            if state:
                user_stated_state = state
                break

        open_loop_items: List[str] = []
        for entity in entities:
            attrs = entity.get("attributes") if isinstance(entity, dict) else None
            entity_type = (entity.get("type") or "").lower() if isinstance(entity, dict) else ""
            if entity_type == "tension" or (isinstance(attrs, dict) and "status" in attrs):
                status = (attrs.get("status") if isinstance(attrs, dict) else None) or "unresolved"
                if isinstance(status, str) and status.lower() != "unresolved":
                    continue
                description = (attrs.get("description") if isinstance(attrs, dict) else None) or entity.get("summary")
                if _allow_claim(description) and not _is_explicit_user_state_claim(description):
                    open_loop_items.append(_normalize_text(description))
        open_loop_items = _dedupe_keep_order(open_loop_items, limit=3)

        commitment_items = _extract_commitments(
            texts=fact_texts + [e.summary for e in entity_models],
            limit=3
        )
        latest_session_id = await _get_latest_session_id(request.tenantId, request.userId)
        last_interaction = None
        try:
            eps = await graphiti_client.get_recent_episode_summaries(
                tenant_id=request.tenantId,
                user_id=request.userId,
                limit=1
            )
            if eps:
                last_interaction = eps[0].get("reference_time")
        except Exception:
            last_interaction = None

        anchors = {
            "timeOfDayLabel": _time_of_day_label(reference_time or datetime.utcnow()),
            "timeGapDescription": None,
            "lastInteraction": last_interaction,
            "sessionId": latest_session_id
        }
        current_focus = _select_current_focus(focus_nodes or [], now=reference_time)
        recall_sheet = _build_structured_sheet(
            facts=fact_texts,
            open_loops=open_loop_items,
            commitments=commitment_items,
            anchors=anchors,
            user_stated_state=user_stated_state,
            current_focus=current_focus,
            max_chars=720
        )

        response.openLoops = open_loop_items
        response.commitments = commitment_items
        response.contextAnchors = anchors
        response.userStatedState = user_stated_state
        response.currentFocus = current_focus
        response.recallSheet = recall_sheet
        response.supplementalContext = recall_sheet
        response.metadata["openLoops"] = len(open_loop_items)
        return response
    except Exception as e:
        logger.error(f"Memory query failed: {e}")
        raise HTTPException(status_code=500, detail="Memory query failed")


@app.get(
    "/memory/loops",
    response_model=MemoryLoopsResponse,
    response_model_exclude_none=True
)
async def memory_loops(
    tenantId: str,
    userId: str,
    limit: int = 10,
    personaId: Optional[str] = None,
    domain: Optional[str] = None
):
    """
    Return prioritized active procedural loops for the user.
    """
    try:
        safe_limit = max(1, min(limit, 50))
        requested_domain = _normalize_text(domain).lower() if domain else None

        # Loops are user-scoped memory; personaId is accepted for backward compatibility,
        # but ignored for retrieval so continuity is stable across persona variants.
        ranked_loops = await loops.get_top_loops_for_startbrief(
            tenant_id=tenantId,
            user_id=userId,
            limit=safe_limit * 2 if requested_domain else safe_limit,
            persona_id=None
        )

        items: List[MemoryLoopItem] = []
        for loop in ranked_loops:
            metadata = loop.metadata if isinstance(loop.metadata, dict) else {}
            loop_domain = _normalize_text(metadata.get("domain")).lower() if metadata.get("domain") else None
            if requested_domain and loop_domain != requested_domain:
                continue
            items.append(
                MemoryLoopItem(
                    id=str(loop.id),
                    type=loop.type,
                    text=loop.text,
                    status=loop.status,
                    salience=loop.salience,
                    timeHorizon=loop.timeHorizon,
                    dueDate=loop.dueDate,
                    lastSeenAt=loop.lastSeenAt,
                    domain=loop_domain,
                    importance=metadata.get("importance"),
                    urgency=metadata.get("urgency"),
                    tags=loop.tags or [],
                    personaId=metadata.get("persona_id")
                )
            )
            if len(items) >= safe_limit:
                break

        return MemoryLoopsResponse(
            items=items,
            metadata={
                "count": len(items),
                "limit": safe_limit,
                "sort": "priority_desc",
                "domainFilter": requested_domain,
                "personaId": None,
                "scope": "user"
            }
        )
    except Exception as e:
        logger.error(f"Memory loops failed: {e}")
        raise HTTPException(status_code=500, detail="Memory loops failed")


@app.get("/user/model", response_model=UserModelResponse)
async def get_user_model(
    tenantId: str,
    userId: str
):
    """
    Return persistent structured user model and domain completeness scores.
    """
    try:
        row = await db.fetchone(
            """
            SELECT model, version, created_at, updated_at, last_source
            FROM user_model
            WHERE tenant_id = $1 AND user_id = $2
            """,
            tenantId,
            userId
        )
        exists = bool(row)
        model = _normalize_user_model(row.get("model") if row else None)
        metadata = _build_user_model_staleness_metadata(model)
        return UserModelResponse(
            tenantId=tenantId,
            userId=userId,
            model=model,
            completenessScore=_compute_domain_completeness(model),
            metadata=metadata,
            version=int(row.get("version") or 0) if row else 0,
            exists=exists,
            createdAt=row.get("created_at").isoformat() if row and row.get("created_at") else None,
            updatedAt=row.get("updated_at").isoformat() if row and row.get("updated_at") else None,
            lastSource=row.get("last_source") if row else None
        )
    except Exception as e:
        logger.error(f"Get user model failed: {e}")
        raise HTTPException(status_code=500, detail="Get user model failed")


@app.patch("/user/model", response_model=UserModelResponse)
async def patch_user_model(request: UserModelPatchRequest):
    """
    Merge-patch persistent structured user model.

    Patch semantics:
    - object fields are merged recursively
    - null deletes a key
    """
    try:
        if not isinstance(request.patch, dict):
            raise HTTPException(status_code=400, detail="Patch must be a JSON object")
        if not request.patch:
            raise HTTPException(status_code=400, detail="Patch cannot be empty")

        existing = await db.fetchone(
            """
            SELECT model
            FROM user_model
            WHERE tenant_id = $1 AND user_id = $2
            """,
            request.tenantId,
            request.userId
        )

        current_model = _normalize_user_model(existing.get("model") if existing else None)
        merged_model = _normalize_user_model(_deep_merge_patch(current_model, request.patch))

        row = await db.fetchone(
            """
            INSERT INTO user_model (tenant_id, user_id, model, version, last_source, created_at, updated_at)
            VALUES ($1, $2, $3::jsonb, 1, $4, NOW(), NOW())
            ON CONFLICT (tenant_id, user_id)
            DO UPDATE SET
                model = $3::jsonb,
                version = user_model.version + 1,
                last_source = COALESCE($4, user_model.last_source),
                updated_at = NOW()
            RETURNING model, version, created_at, updated_at, last_source
            """,
            request.tenantId,
            request.userId,
            merged_model,
            request.source
        )

        normalized = _normalize_user_model(row.get("model") if row else merged_model)
        metadata = _build_user_model_staleness_metadata(normalized)
        return UserModelResponse(
            tenantId=request.tenantId,
            userId=request.userId,
            model=normalized,
            completenessScore=_compute_domain_completeness(normalized),
            metadata=metadata,
            version=int(row.get("version") or 1) if row else 1,
            exists=True,
            createdAt=row.get("created_at").isoformat() if row and row.get("created_at") else None,
            updatedAt=row.get("updated_at").isoformat() if row and row.get("updated_at") else None,
            lastSource=row.get("last_source") if row else request.source
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Patch user model failed: {e}")
        raise HTTPException(status_code=500, detail="Patch user model failed")


@app.get("/analysis/daily", response_model=DailyAnalysisResponse)
async def get_daily_analysis(
    tenantId: str,
    userId: str,
    date: Optional[str] = None
):
    """
    Return daily analysis for a user (specific date or latest available).
    """
    try:
        target_date: Optional[date] = None
        if date:
            try:
                target_date = datetime.fromisoformat(date).date()
            except Exception:
                try:
                    target_date = datetime.strptime(date, "%Y-%m-%d").date()
                except Exception:
                    raise HTTPException(status_code=400, detail="Invalid date; use YYYY-MM-DD")

        if target_date:
            row = await db.fetchone(
                """
                SELECT analysis_date, themes, scores, steering_note, confidence, metadata, created_at, updated_at
                FROM daily_analysis
                WHERE tenant_id = $1
                  AND user_id = $2
                  AND analysis_date = $3
                LIMIT 1
                """,
                tenantId,
                userId,
                target_date
            )
        else:
            row = await db.fetchone(
                """
                SELECT analysis_date, themes, scores, steering_note, confidence, metadata, created_at, updated_at
                FROM daily_analysis
                WHERE tenant_id = $1
                  AND user_id = $2
                ORDER BY analysis_date DESC
                LIMIT 1
                """,
                tenantId,
                userId
            )

        if not row:
            return DailyAnalysisResponse(
                tenantId=tenantId,
                userId=userId,
                exists=False,
                metadata={"requestedDate": target_date.isoformat() if target_date else None}
            )

        return DailyAnalysisResponse(
            tenantId=tenantId,
            userId=userId,
            analysisDate=row.get("analysis_date").isoformat() if row.get("analysis_date") else None,
            themes=row.get("themes") or [],
            scores=row.get("scores") or {},
            steeringNote=_normalize_text(row.get("steering_note")) or None,
            confidence=float(row.get("confidence")) if row.get("confidence") is not None else None,
            exists=True,
            createdAt=row.get("created_at").isoformat() if row.get("created_at") else None,
            updatedAt=row.get("updated_at").isoformat() if row.get("updated_at") else None,
            metadata=row.get("metadata") or {}
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get daily analysis failed: {e}")
        raise HTTPException(status_code=500, detail="Get daily analysis failed")


@app.get("/session/brief", response_model=SessionBriefResponse)
async def session_brief(
    tenantId: str,
    userId: str,
    now: Optional[str] = None
):
    """
    Generate a session start-brief from Graphiti narrative entities.
    """
    try:
        reference_now = datetime.utcnow()
        if now:
            reference_now = datetime.fromisoformat(now.replace("Z", "+00:00"))

        episodes = await graphiti_client.get_recent_episode_summaries(
            tenant_id=tenantId,
            user_id=userId,
            limit=10
        )

        # No transcript fallback; narrative summary must come from Graphiti

        time_gap_description = None
        if episodes:
            last_time = episodes[0].get("reference_time")
            if isinstance(last_time, str):
                try:
                    last_time = datetime.fromisoformat(last_time.replace("Z", "+00:00"))
                except Exception:
                    last_time = None
            if last_time:
                delta = reference_now - last_time
                hours = int(delta.total_seconds() // 3600)
                minutes = int((delta.total_seconds() % 3600) // 60)
                if hours > 0:
                    time_gap_description = f"{hours} hours since last spoke"
                else:
                    time_gap_description = f"{minutes} minutes since last spoke"

        from graphiti_core.search.search_filters import SearchFilters, DateFilter, ComparisonOperator
        current_filter = SearchFilters(
            valid_at=[[DateFilter(date=reference_now, comparison_operator=ComparisonOperator.less_than_equal)]],
            invalid_at=[[DateFilter(date=None, comparison_operator=ComparisonOperator.is_null)]]
        )

        tensions = await graphiti_client.search_nodes(
            tenant_id=tenantId,
            user_id=userId,
            query="current problems tasks unresolved blockers open loops",
            limit=10,
            reference_time=reference_now,
            search_filter=current_filter
        )
        active_loops = []
        for t in tensions:
            attrs = t.get("attributes") if isinstance(t, dict) else None
            t_type = (t.get("type") or "").lower() if isinstance(t, dict) else ""
            is_tension = t_type == "tension"
            if isinstance(attrs, dict) and ("description" in attrs or "status" in attrs):
                is_tension = True
            if not is_tension:
                continue
            status = None
            description = None
            if isinstance(attrs, dict):
                status = attrs.get("status")
                description = attrs.get("description")
            if status and isinstance(status, str) and status.lower() != "unresolved":
                continue
            active_loops.append({
                "description": _normalize_text(description or t.get("summary")),
                "status": status or "unresolved"
            })

        key_entities = await graphiti_client.search_nodes(
            tenant_id=tenantId,
            user_id=userId,
            query="named people places projects tools organizations priorities",
            limit=6,
            reference_time=reference_now,
            search_filter=current_filter
        )
        commitment_entities = await graphiti_client.search_nodes(
            tenant_id=tenantId,
            user_id=userId,
            query="commitment schedule deadline todo follow up plan",
            limit=6,
            reference_time=reference_now,
            search_filter=current_filter
        )
        focus_nodes = await graphiti_client.search_nodes(
            tenant_id=tenantId,
            user_id=userId,
            query="current focus priority focused on right now today i need",
            limit=3,
            reference_time=reference_now,
            search_filter=current_filter
        )

        facts_from_episodes = []
        for ep in episodes[:4]:
            for claim in _split_claims(ep.get("summary")):
                if _allow_claim(claim) and not _is_explicit_user_state_claim(claim):
                    facts_from_episodes.append(claim)

        facts_from_entities = []
        for entity in key_entities:
            summary = _normalize_text(entity.get("summary"))
            entity_type = (entity.get("type") or "").lower()
            if not summary:
                continue
            if entity_type in {"mentalstate"}:
                continue
            if _allow_claim(summary) and not _is_explicit_user_state_claim(summary):
                facts_from_entities.append(summary)

        facts = _select_facts(facts_from_episodes + facts_from_entities, limit=4)
        open_loop_descriptions = _dedupe_keep_order(
            [
                l.get("description")
                for l in active_loops
                if l.get("description")
                and _allow_claim(l.get("description"))
                and not _is_explicit_user_state_claim(l.get("description"))
            ],
            limit=3
        )
        commitment_candidates = [
            _normalize_text(e.get("summary"))
            for e in commitment_entities
            if _normalize_text(e.get("summary"))
        ] + facts_from_episodes
        commitments = _extract_commitments(commitment_candidates, limit=3)

        user_stated_state = None
        for ep in episodes[:4]:
            state = _extract_explicit_user_state(ep.get("summary"))
            if state:
                user_stated_state = state
                break

        time_of_day_label = _time_of_day_label(reference_now)
        energy_hint = _extract_energy_hint_from_texts([ep.get("summary") for ep in episodes[:4]])
        last_interaction = episodes[0].get("reference_time") if episodes else None
        latest_session_id = await _get_latest_session_id(tenantId, userId)

        context_anchors: Dict[str, Any] = {
            "timeOfDayLabel": time_of_day_label,
            "timeGapDescription": time_gap_description,
            "sessionId": latest_session_id,
            "lastInteraction": last_interaction
        }
        current_focus = _select_current_focus(focus_nodes or [], now=reference_now)

        brief_context = _build_structured_sheet(
            facts=facts,
            open_loops=open_loop_descriptions,
            commitments=commitments,
            anchors=context_anchors,
            user_stated_state=user_stated_state,
            current_focus=current_focus,
            max_chars=720
        )

        fact_keys = {f.lower() for f in facts}
        narrative_candidates: List[str] = []
        for ep in episodes[:6]:
            narrative_candidates.extend(_split_claims(ep.get("summary")))

        narrative_lines = _dedupe_keep_order(
            [
                _shorten_line(line, 180)
                for line in narrative_candidates
                if _allow_claim(line) and _allow_fact_text(line) and not _is_explicit_user_state_claim(line)
            ],
            limit=5
        )
        narrative_lines = [line for line in narrative_lines if line.lower() not in fact_keys][:3]
        if not narrative_lines and episodes:
            # Fallback: keep a single narrative anchor without duplicating a fact line verbatim.
            for claim in _split_claims(episodes[0].get("summary")):
                if not _allow_claim(claim) or _is_explicit_user_state_claim(claim):
                    continue
                if not _allow_fact_text(claim):
                    continue
                candidate = claim if claim.lower() not in fact_keys else f"Previously: {claim}"
                if _allow_claim(candidate) and _allow_fact_text(candidate):
                    narrative_lines = [_shorten_line(candidate, 180)]
                    break

        narrative_summary = [{"summary": line, "reference_time": last_interaction} for line in narrative_lines]

        current_vibe: Dict[str, Any] = {
            "timeOfDayLabel": time_of_day_label
        }
        if energy_hint:
            current_vibe["energyHint"] = energy_hint

        return SessionBriefResponse(
            timeGapDescription=time_gap_description,
            timeOfDayLabel=time_of_day_label,
            energyHint=energy_hint,
            facts=facts,
            openLoops=open_loop_descriptions,
            commitments=commitments,
            contextAnchors=context_anchors,
            userStatedState=user_stated_state,
            currentFocus=current_focus,
            temporalVibe=time_of_day_label,
            briefContext=brief_context,
            narrativeSummary=narrative_summary,
            activeLoops=active_loops,
            currentVibe=current_vibe
        )
    except Exception as e:
        logger.error(f"Session brief failed: {e}")
        raise HTTPException(status_code=500, detail="Session brief failed")


@app.get("/session/startbrief", response_model=SessionStartBriefResponse)
async def session_startbrief(
    tenantId: str,
    userId: str,
    now: Optional[str] = None,
    sessionId: Optional[str] = None,
    personaId: Optional[str] = None,
    timezone: Optional[str] = None
):
    """
    Minimal start-brief: short bridgeText + durable items.
    """
    try:
        logger.info(
            "startbrief input: tenant=%s user=%s session=%s persona=%s now=%s timezone=%s",
            tenantId,
            userId,
            sessionId,
            personaId,
            now,
            timezone
        )

        reference_now = datetime.utcnow()
        if now:
            reference_now = datetime.fromisoformat(now.replace("Z", "+00:00"))

        tzinfo = _resolve_timezone(timezone)
        if reference_now.tzinfo is None:
            reference_now = reference_now.replace(tzinfo=tzinfo or dt_timezone.utc)
        if tzinfo:
            reference_now = reference_now.astimezone(tzinfo)

        settings = get_settings()
        time_of_day_label = _time_of_day_label(reference_now.replace(tzinfo=None))
        reference_now_utc = reference_now.astimezone(dt_timezone.utc)
        time_gap_human = None
        last_activity_time: Optional[datetime] = None
        recent_session_summaries: List[Dict[str, Any]] = []
        user_model_hints: List[str] = []
        resume_bridge_text: Optional[str] = None
        resume_use_bridge = False
        summary_norm_stats: Dict[str, Any] = {
            "nodes_seen": 0,
            "nodes_normalized_nonempty": 0,
            "placeholders_blocked": 0,
            "evidence_ids_used_count": 0,
            "evidence_ids_fetched_count": 0,
            "fallback_used": False,
            "fallback_success": False,
        }
        fetched_summary_ids: List[str] = []

        # Prefer session/message timestamps for time gap.
        session_id = sessionId or await _get_latest_session_id(tenantId, userId)
        last_user_text = None
        # Prefer transcript last message timestamp (session/ingest path)
        try:
            row = await db.fetchone(
                """
                SELECT messages
                FROM session_transcript
                WHERE tenant_id = $1 AND user_id = $2
                  AND updated_at <= $3
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                tenantId,
                userId,
                reference_now_utc
            )
            messages = row.get("messages") if row else None
            if isinstance(messages, list) and messages:
                last_msg = messages[-1]
                ts = last_msg.get("timestamp")
                if isinstance(ts, str):
                    try:
                        last_activity_time = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    except Exception:
                        last_activity_time = None
                last_user_text = next(
                    (m.get("text") for m in reversed(messages) if m.get("role") == "user" and m.get("text")),
                    None
                )
        except Exception:
            last_activity_time = None

        # Fallback to session buffer if available
        if session_id and not last_activity_time:
            try:
                last_activity_time = await session.get_last_interaction_time(tenantId, session_id)
            except Exception:
                last_activity_time = None
            if last_user_text is None:
                try:
                    working_memory = await session.get_working_memory(tenantId, session_id)
                    for msg in reversed(working_memory):
                        if msg.role == "user" and msg.text:
                            last_user_text = msg.text
                            break
                except Exception:
                    last_user_text = None

        try:
            logger.info("startbrief graphiti: get_latest_session_summary_node")
            summary_node = await graphiti_client.get_latest_session_summary_node(
                tenant_id=tenantId,
                user_id=userId
            )
            recent_nodes = await graphiti_client.get_recent_session_summary_nodes(
                tenant_id=tenantId,
                user_id=userId,
                limit=10
            )
            for node in recent_nodes:
                summary_norm_stats["nodes_seen"] = int(summary_norm_stats.get("nodes_seen") or 0) + 1
                fetched_id = _extract_startbrief_summary_identifier(node)
                if fetched_id and fetched_id not in fetched_summary_ids:
                    fetched_summary_ids.append(fetched_id)
                normalized = _normalize_startbrief_session_summary_node(node, stats=summary_norm_stats)
                if not normalized:
                    continue
                created_at = _normalize_text(normalized.get("created_at"))
                created_dt = _parse_optional_dt(created_at)
                if created_dt and created_dt.astimezone(dt_timezone.utc) > reference_now_utc:
                    continue
                summary_facts = _normalize_text(normalized.get("latest_thread_text"))
                tone = _normalize_text(normalized.get("tone"))
                moment = _normalize_text(normalized.get("moment"))
                if not summary_facts and not moment:
                    continue
                summary_norm_stats["nodes_normalized_nonempty"] = int(summary_norm_stats.get("nodes_normalized_nonempty") or 0) + 1
                recent_session_summaries.append({
                    "session_id": _normalize_text(normalized.get("session_id")),
                    "created_at": created_at,
                    "summary_facts": summary_facts,
                    "tone": tone,
                    "moment": moment,
                    "unresolved": normalized.get("unresolved") if isinstance(normalized.get("unresolved"), list) else [],
                    "decisions": normalized.get("decisions") if isinstance(normalized.get("decisions"), list) else [],
                    "salience": _normalize_text(normalized.get("salience")) or "low",
                    "summary_text": _normalize_text(normalized.get("summary_text")),
                    "bridge_text": _normalize_text(normalized.get("bridge_text")),
                    "reference_time": _normalize_text(normalized.get("reference_time") or created_at),
                })

            needs_properties_fallback = (
                int(summary_norm_stats.get("nodes_seen") or 0) > 0
                and int(summary_norm_stats.get("nodes_normalized_nonempty") or 0) == 0
            )
            if needs_properties_fallback and fetched_summary_ids:
                summary_norm_stats["fallback_used"] = True
                try:
                    fallback_nodes = await graphiti_client.get_session_summary_nodes_by_ids(
                        tenant_id=tenantId,
                        user_id=userId,
                        ids=fetched_summary_ids[:5],
                        limit=5
                    )
                    for node in fallback_nodes or []:
                        normalized = _normalize_startbrief_session_summary_node(node, stats=summary_norm_stats)
                        if not normalized:
                            continue
                        created_at = _normalize_text(normalized.get("created_at"))
                        created_dt = _parse_optional_dt(created_at)
                        if created_dt and created_dt.astimezone(dt_timezone.utc) > reference_now_utc:
                            continue
                        summary_facts = _normalize_text(normalized.get("latest_thread_text"))
                        tone = _normalize_text(normalized.get("tone"))
                        moment = _normalize_text(normalized.get("moment"))
                        if not summary_facts and not moment:
                            continue
                        summary_norm_stats["nodes_normalized_nonempty"] = int(summary_norm_stats.get("nodes_normalized_nonempty") or 0) + 1
                        recent_session_summaries.append({
                            "session_id": _normalize_text(normalized.get("session_id")),
                            "created_at": created_at,
                            "summary_facts": summary_facts,
                            "tone": tone,
                            "moment": moment,
                            "unresolved": normalized.get("unresolved") if isinstance(normalized.get("unresolved"), list) else [],
                            "decisions": normalized.get("decisions") if isinstance(normalized.get("decisions"), list) else [],
                            "salience": _normalize_text(normalized.get("salience")) or "low",
                            "summary_text": _normalize_text(normalized.get("summary_text")),
                            "bridge_text": _normalize_text(normalized.get("bridge_text")),
                            "reference_time": _normalize_text(normalized.get("reference_time") or created_at),
                        })
                    summary_norm_stats["fallback_success"] = bool(recent_session_summaries)
                except Exception as e:
                    logger.warning("startbrief properties fallback failed: %s", e)
                    summary_norm_stats["fallback_success"] = False
            if summary_node:
                normalized_latest = _normalize_startbrief_session_summary_node(summary_node)
                attrs = normalized_latest.get("attributes") if normalized_latest else {}
                summary_end = _parse_optional_dt(
                    (normalized_latest.get("reference_time") if normalized_latest else None)
                    or (attrs.get("reference_time") if isinstance(attrs, dict) else None)
                    or (normalized_latest.get("created_at") if normalized_latest else None)
                )
                if summary_end and summary_end.astimezone(dt_timezone.utc) > reference_now_utc:
                    summary_end = None
                bridge_candidate = _normalize_text(
                    (normalized_latest.get("bridge_text") if normalized_latest else None) or (
                        attrs.get("bridge_text") if isinstance(attrs, dict) else None
                    )
                )
                resume_use_bridge = should_use_bridge(summary_end, reference_now)
                resume_bridge_text = bridge_candidate if (resume_use_bridge and bridge_candidate) else None
                if not last_activity_time:
                    last_time = _parse_optional_dt(normalized_latest.get("created_at") if normalized_latest else None)
                    if isinstance(last_time, datetime) and last_time.astimezone(dt_timezone.utc) <= reference_now_utc:
                        last_activity_time = last_time
        except Exception:
            resume_bridge_text = None

        # Fallback to legacy episode summaries if no SessionSummary nodes are found
        if not recent_session_summaries:
            try:
                logger.info("startbrief graphiti: get_recent_episode_summaries (fallback)")
                episodes = await graphiti_client.get_recent_episode_summaries(
                    tenant_id=tenantId,
                    user_id=userId,
                    limit=3
                )
                if episodes:
                    for ep in episodes:
                        summary_value = _normalize_text(ep.get("summary"))
                        if not summary_value:
                            continue
                        recent_session_summaries.append({
                            "session_id": _normalize_text(session_id),
                            "created_at": _normalize_text(ep.get("reference_time")),
                            "summary_facts": summary_value,
                            "tone": "",
                            "moment": "",
                            "unresolved": [],
                            "decisions": [],
                            "salience": "low",
                            "summary_text": summary_value,
                            "bridge_text": "",
                            "reference_time": _normalize_text(ep.get("reference_time")),
                        })
                    if not last_activity_time:
                        last_time = episodes[0].get("reference_time")
                        if isinstance(last_time, str):
                            try:
                                last_time = datetime.fromisoformat(last_time.replace("Z", "+00:00"))
                            except Exception:
                                last_time = None
                        if isinstance(last_time, datetime):
                            last_activity_time = last_time
            except Exception:
                pass

        # Diagnostics: environment nodes (not used in output)
        try:
            logger.info("startbrief graphiti: search_nodes (environment diagnostics)")
            env_nodes = await graphiti_client.search_nodes(
                tenant_id=tenantId,
                user_id=userId,
                query="environment location place context",
                limit=3,
                reference_time=reference_now
            )
            env_info = []
            for node in env_nodes or []:
                node_type = (node.get("type") or "").lower() if isinstance(node, dict) else ""
                if node_type != "environment":
                    continue
                env_info.append({
                    "summary": node.get("summary"),
                    "uuid": node.get("uuid"),
                    "created_at": node.get("created_at"),
                    "updated_at": node.get("updated_at"),
                    "reference_time": node.get("reference_time")
                })
            logger.info("startbrief env_nodes=%s", env_info)
        except Exception as e:
            logger.info("startbrief env_nodes lookup failed: %s", e)

        if last_activity_time:
            if last_activity_time.tzinfo is None:
                last_activity_time = last_activity_time.replace(tzinfo=tzinfo or dt_timezone.utc)
            if tzinfo:
                last_activity_time = last_activity_time.astimezone(tzinfo)
            delta = reference_now - last_activity_time
            hours = int(delta.total_seconds() // 3600)
            minutes = int((delta.total_seconds() % 3600) // 60)
            if hours > 0:
                time_gap_human = f"{hours} hours since last spoke"
            else:
                time_gap_human = f"{minutes} minutes since last spoke"

        logger.info(
            "startbrief timegap: last_activity=%s time_gap_human=%s time_of_day=%s",
            last_activity_time.isoformat() if isinstance(last_activity_time, datetime) else None,
            time_gap_human,
            time_of_day_label
        )
        logger.info("startbrief graphiti: search_facts not used")

        logger.info("startbrief loops: get_top_loops_for_startbrief")
        # Loops are user-scoped memory; ignore personaId for retrieval.
        top_loops = await loops.get_top_loops_for_startbrief(
            tenant_id=tenantId,
            user_id=userId,
            limit=5,
            persona_id=None
        )

        loop_items: List[Dict[str, Any]] = []
        for loop_item in top_loops:
            loop_items.append({
                "kind": "loop",
                "type": loop_item.type,
                "text": loop_item.text,
                "timeHorizon": loop_item.timeHorizon,
                "dueDate": loop_item.dueDate,
                "salience": loop_item.salience,
                "lastSeenAt": loop_item.lastSeenAt
            })
            if len(loop_items) >= 5:
                break

        if len(loop_items) < 5:
            logger.info("startbrief graphiti: search_nodes (tensions)")
            tensions = await graphiti_client.search_nodes(
                tenant_id=tenantId,
                user_id=userId,
                query="current problems tasks unresolved blockers open loops",
                limit=5,
                reference_time=reference_now
            )
            for t in tensions or []:
                attrs = t.get("attributes") if isinstance(t, dict) else None
                t_type = (t.get("type") or "").lower() if isinstance(t, dict) else ""
                is_tension = t_type == "tension"
                if isinstance(attrs, dict) and ("description" in attrs or "status" in attrs):
                    is_tension = True
                if not is_tension:
                    continue
                status = (attrs.get("status") if isinstance(attrs, dict) else None) or "unresolved"
                if isinstance(status, str) and status.lower() != "unresolved":
                    continue
                description = (attrs.get("description") if isinstance(attrs, dict) else None) or t.get("summary")
                description = _normalize_text(description)
                if not description:
                    continue
                if not _allow_claim(description) or _is_explicit_user_state_claim(description):
                    continue
                if _looks_like_environment(description):
                    continue
                if any(_normalize_text(i.get("text")).lower() == description.lower() for i in loop_items):
                    continue
                loop_items.append({
                    "kind": "tension",
                    "text": _shorten_line(description, 120),
                    "type": None,
                    "timeHorizon": None,
                    "dueDate": None,
                    "salience": None,
                    "lastSeenAt": None,
                })
                if len(loop_items) >= 5:
                    break

        yesterday_analysis = {"date": None, "themes": [], "steering_note": None}
        try:
            yesterday_analysis = await _get_yesterday_analysis_context(
                tenant_id=tenantId,
                user_id=userId,
                reference_date=reference_now.date()
            )
        except Exception as e:
            logger.error(f"startbrief yesterday analysis lookup failed: {e}")

        try:
            user_model_row = await db.fetchone(
                """
                SELECT model
                FROM user_model
                WHERE tenant_id = $1 AND user_id = $2
                LIMIT 1
                """,
                tenantId,
                userId
            )
            if user_model_row:
                user_model = _normalize_user_model(user_model_row.get("model"))
                user_model_hints = _extract_high_confidence_user_model_hints(
                    user_model,
                    threshold=float(settings.user_model_high_confidence)
                )
        except Exception as e:
            logger.error(f"startbrief user model hint lookup failed: {e}")

        sessions_today_count = 0
        local_day = reference_now.date()
        try:
            local_midnight = reference_now.replace(hour=0, minute=0, second=0, microsecond=0)
            day_start_utc = local_midnight.astimezone(dt_timezone.utc)
            sessions_today_row = await db.fetchone(
                """
                SELECT count(*) AS sessions_today
                FROM session_transcript
                WHERE tenant_id = $1
                  AND user_id = $2
                  AND updated_at >= $3
                  AND updated_at <= $4
                """,
                tenantId,
                userId,
                day_start_utc,
                reference_now_utc
            )
            sessions_today_count = int((sessions_today_row or {}).get("sessions_today") or 0)
        except Exception:
            sessions_today_count = 0

        if sessions_today_count <= 0:
            for s in recent_session_summaries:
                created = _parse_optional_dt(s.get("created_at"))
                if not created:
                    continue
                created_local = created.astimezone(reference_now.tzinfo) if (created.tzinfo and reference_now.tzinfo) else created
                if created_local.date() == local_day:
                    sessions_today_count += 1

        ingredients = select_startbrief_ingredients(
            reference_now=reference_now,
            last_session_end=last_activity_time,
            sessions_today_count=sessions_today_count,
            time_of_day_label=time_of_day_label,
            recent_session_summaries=recent_session_summaries,
            yesterday_daily_analysis=yesterday_analysis,
            top_active_loops=loop_items,
            user_model_hints=user_model_hints,
        )

        # Optional bounded fallback: if nodes were fetched but produced no usable thread text,
        # re-fetch by ids via properties(n) and normalize with the same parser.
        if (
            int(summary_norm_stats.get("nodes_seen") or 0) > 0
            and not _normalize_text(ingredients.get("last_thread"))
            and fetched_summary_ids
            and not summary_norm_stats.get("fallback_used")
        ):
            summary_norm_stats["fallback_used"] = True
            try:
                fallback_nodes = await graphiti_client.get_session_summary_nodes_by_ids(
                    tenant_id=tenantId,
                    user_id=userId,
                    ids=fetched_summary_ids[:5],
                    limit=5
                )
                for node in fallback_nodes or []:
                    normalized = _normalize_startbrief_session_summary_node(node, stats=summary_norm_stats)
                    if not normalized:
                        continue
                    created_at = _normalize_text(normalized.get("created_at"))
                    created_dt = _parse_optional_dt(created_at)
                    if created_dt and created_dt.astimezone(dt_timezone.utc) > reference_now_utc:
                        continue
                    summary_facts = _normalize_text(normalized.get("latest_thread_text"))
                    tone = _normalize_text(normalized.get("tone"))
                    moment = _normalize_text(normalized.get("moment"))
                    if not summary_facts and not moment:
                        continue
                    summary_norm_stats["nodes_normalized_nonempty"] = int(summary_norm_stats.get("nodes_normalized_nonempty") or 0) + 1
                    recent_session_summaries.append({
                        "session_id": _normalize_text(normalized.get("session_id")),
                        "created_at": created_at,
                        "summary_facts": summary_facts,
                        "tone": tone,
                        "moment": moment,
                        "unresolved": normalized.get("unresolved") if isinstance(normalized.get("unresolved"), list) else [],
                        "decisions": normalized.get("decisions") if isinstance(normalized.get("decisions"), list) else [],
                        "salience": _normalize_text(normalized.get("salience")) or "low",
                        "summary_text": _normalize_text(normalized.get("summary_text")),
                        "bridge_text": _normalize_text(normalized.get("bridge_text")),
                        "reference_time": _normalize_text(normalized.get("reference_time") or created_at),
                    })
                if recent_session_summaries:
                    summary_norm_stats["fallback_success"] = True
                    ingredients = select_startbrief_ingredients(
                        reference_now=reference_now,
                        last_session_end=last_activity_time,
                        sessions_today_count=sessions_today_count,
                        time_of_day_label=time_of_day_label,
                        recent_session_summaries=recent_session_summaries,
                        yesterday_daily_analysis=yesterday_analysis,
                        top_active_loops=loop_items,
                        user_model_hints=user_model_hints,
                    )
            except Exception as e:
                logger.warning("startbrief properties fallback failed: %s", e)
                summary_norm_stats["fallback_success"] = False

        local_time = reference_now.strftime("%H:%M")
        first_session_today = sessions_today_count == 0
        narrative_ingredients = {
            "now_local": ingredients.get("now_local"),
            "gap_human": ingredients.get("gap_human"),
            "session_frequency": ingredients.get("session_frequency"),
            "last_thread": ingredients.get("last_thread"),
            "user_tone": ingredients.get("user_tone"),
            "open_threads": ingredients.get("open_threads", [])[:2],
            "continuation_hint": resume_bridge_text if ingredients.get("depth_label") == "continuation" else None,
        }
        depth_label = ingredients.get("depth_label") or "yesterday"
        handover_text = None
        try:
            handover_text = await _generate_startbrief_bridge_llm(
                narrative_ingredients=narrative_ingredients,
                depth_label=depth_label
            )
            handover_text = _ensure_sentence_spacing(handover_text or "")
        except Exception as e:
            logger.error(f"startbrief bridge llm generation failed: {e}")
            handover_text = None

        invalid_reasons = _validate_handover_text(handover_text or "", depth_label, narrative_ingredients)
        if invalid_reasons:
            try:
                rewritten = await _generate_startbrief_bridge_llm(
                    narrative_ingredients={**narrative_ingredients, "existing_text": handover_text or "", "invalid_reasons": invalid_reasons},
                    depth_label=depth_label,
                    rewrite_only=True,
                )
                rewritten = _ensure_sentence_spacing(rewritten or "")
                if not _validate_handover_text(rewritten or "", depth_label, narrative_ingredients):
                    handover_text = rewritten
            except Exception:
                pass

        if _validate_handover_text(handover_text or "", depth_label, narrative_ingredients):
            handover_text = _fallback_handover_text(ingredients, depth_label)

        handover_text = _sanitize_handover_tone(handover_text or "")
        if _validate_handover_text(handover_text or "", depth_label, narrative_ingredients):
            handover_text = _sanitize_handover_tone(_fallback_handover_text(ingredients, depth_label))

        ops_context = compose_ops_context(ingredients, loop_items)
        used_summary_ids = [
            _normalize_text(s.get("session_id"))
            for s in ingredients.get("selected_summaries", [])
            if _normalize_text(s.get("session_id")) and not _is_placeholder_value(s.get("session_id"))
        ]
        evidence_ids = used_summary_ids or fetched_summary_ids or []
        summary_content_quality = "ok"
        if int(summary_norm_stats.get("nodes_seen") or 0) <= 0:
            summary_content_quality = "none_fetched"
        elif not _normalize_text(ingredients.get("last_thread")):
            summary_content_quality = "empty_after_normalization"
        summary_norm_stats["evidence_ids_used_count"] = len(used_summary_ids)
        summary_norm_stats["evidence_ids_fetched_count"] = len(fetched_summary_ids)
        logger.info(
            "startbrief_summary_norm nodes_seen=%s nodes_normalized_nonempty=%s placeholders_blocked=%s evidence_ids_used_count=%s evidence_ids_fetched_count=%s fallback_used=%s fallback_success=%s",
            summary_norm_stats.get("nodes_seen"),
            summary_norm_stats.get("nodes_normalized_nonempty"),
            summary_norm_stats.get("placeholders_blocked"),
            summary_norm_stats.get("evidence_ids_used_count"),
            summary_norm_stats.get("evidence_ids_fetched_count"),
            summary_norm_stats.get("fallback_used"),
            summary_norm_stats.get("fallback_success"),
        )
        daily_analysis_date_used = _normalize_text(yesterday_analysis.get("date")) or None

        response = SessionStartBriefResponse(
            handover_text=_normalize_text(handover_text) or _sanitize_handover_tone(_fallback_handover_text(ingredients, depth_label)),
            handover_depth=depth_label,
            time_context={
                "local_time": local_time,
                "time_of_day": time_of_day_label,
                "gap_minutes": ingredients.get("gap_minutes"),
                "sessions_today": sessions_today_count,
                "first_session_today": first_session_today,
            },
            resume={
                "use_bridge": bool(resume_use_bridge and resume_bridge_text),
                "bridge_text": resume_bridge_text if (resume_use_bridge and resume_bridge_text) else None,
            },
            ops_context=ops_context,
            evidence={
                "session_summary_ids_used": evidence_ids[:2],
                "session_summary_ids_fetched": fetched_summary_ids[:5],
                "summary_fetch_count": len(fetched_summary_ids),
                "summary_used_count": len(used_summary_ids),
                "summary_content_quality": summary_content_quality,
                "fallback_used": bool(summary_norm_stats.get("fallback_used")),
                "fallback_success": bool(summary_norm_stats.get("fallback_success")),
                "daily_analysis_date_used": daily_analysis_date_used,
            },
        )
        try:
            await _log_startbrief_history(
                tenant_id=tenantId,
                user_id=userId,
                session_id=session_id,
                requested_at=reference_now,
                time_of_day_label=time_of_day_label,
                time_gap_human=time_gap_human,
                bridge_text=response.handover_text,
                items=loop_items[:5],
                context={
                    "handover_depth": depth_label,
                    "evidence": response.evidence,
                    "ops_context": response.ops_context,
                    "narrative_ingredients": narrative_ingredients,
                }
            )
        except Exception as e:
            logger.error("startbrief history log failed: %s", e)
        return response
    except Exception as e:
        logger.error(f"Session startbrief failed: {e}")
        raise HTTPException(status_code=500, detail="Session startbrief failed")


@app.post("/admin/purgeUser")
async def purge_user(
    request: PurgeUserRequest,
    x_admin_key: Optional[str] = Header(None)
):
    """
    Admin-only: purge all memory for a tenant/user from Postgres + Graphiti.
    """
    settings = get_settings()
    if not settings.admin_api_key or x_admin_key != settings.admin_api_key:
        raise HTTPException(status_code=403, detail="Forbidden")

    tenant_id = request.tenantId
    user_id = request.userId

    tables = [
        "session_buffer",
        "session_transcript",
        "graphiti_outbox",
        "loops",
        "identity_cache",
        "user_identity"
    ]
    deleted: Dict[str, int] = {}
    for table in tables:
        try:
            count = await db.fetchval(
                f"WITH deleted AS (DELETE FROM {table} WHERE tenant_id = $1 AND user_id = $2 RETURNING 1) SELECT count(*) FROM deleted",
                tenant_id,
                user_id
            )
            deleted[table] = int(count or 0)
        except Exception:
            deleted[table] = 0

    graphiti_result = await graphiti_client.purge_user_graph(tenant_id, user_id)

    return {
        "tenantId": tenant_id,
        "userId": user_id,
        "postgres": deleted,
        "graphiti": graphiti_result
    }


@app.post("/session/close")
async def close_session(request: SessionCloseRequest):
    """
    Public session close endpoint.

    Orchestrator should call this after inactivity to flush raw transcript to Graphiti.
    """
    try:
        # Determine session to close
        session_id = request.sessionId
        if not session_id:
            row = await db.fetchone(
                """
                SELECT session_id
                FROM session_buffer
                WHERE tenant_id = $1 AND user_id = $2 AND closed_at IS NULL
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                request.tenantId,
                request.userId
            )
            session_id = row.get("session_id") if row else None

        if not session_id:
            return {"closed": False, "reason": "no_open_session"}

        ok = await session.close_session(
            tenant_id=request.tenantId,
            session_id=session_id,
            user_id=request.userId,
            graphiti_client=graphiti_client,
            persona_id=request.personaId
        )
        return {"closed": bool(ok), "sessionId": session_id}
    except Exception as e:
        logger.error(f"Session close failed: {e}")
        raise HTTPException(status_code=500, detail="Session close failed")


@app.post("/session/ingest", response_model=SessionIngestResponse)
async def ingest_session(request: SessionIngestRequest):
    """
    Session-only ingestion: send full transcript to Graphiti as one episode.
    """
    try:
        # Determine timestamps
        started_at = request.startedAt
        ended_at = request.endedAt
        if not started_at and request.messages:
            started_at = request.messages[0].timestamp
        if not ended_at and request.messages:
            ended_at = request.messages[-1].timestamp

        episode_name = f"session_raw_{request.sessionId}"
        reference_time = datetime.fromisoformat(ended_at.replace("Z", "+00:00")) if ended_at else datetime.utcnow()
        messages_payload = [m.model_dump() for m in request.messages]

        def _parse_msg_ts(value: Any) -> Optional[datetime]:
            if not value:
                return None
            try:
                return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            except Exception:
                return None

        episode_result = await graphiti_client.add_session_episode(
            tenant_id=request.tenantId,
            user_id=request.userId,
            messages=messages_payload,
            reference_time=reference_time,
            episode_name=episode_name,
            metadata={
                "session_id": request.sessionId,
                "started_at": started_at,
                "ended_at": ended_at,
                "episode_type": "session_raw"
            }
        )
        episode_uuid = None
        if isinstance(episode_result, dict):
            episode_uuid = episode_result.get("episode_uuid")

        # Create SessionSummary + bridge from full transcript
        try:
            summary_payload = await session.summarize_session_messages(messages_payload)
            summary_text = summary_payload.get("summary_text")
            bridge_text = summary_payload.get("bridge_text")
            index_text = summary_payload.get("index_text")
            salience = summary_payload.get("salience") or "low"
            logger.info(
                "Session ingest recap result tenant=%s user=%s session=%s summary_len=%s bridge_len=%s",
                request.tenantId,
                request.userId,
                request.sessionId,
                len(summary_text or ""),
                len(bridge_text or "")
            )
            if summary_text:
                await graphiti_client.add_session_summary(
                    tenant_id=request.tenantId,
                    user_id=request.userId,
                    session_id=request.sessionId,
                    summary_text=summary_text,
                    bridge_text=bridge_text,
                    reference_time=reference_time,
                    episode_uuid=episode_uuid,
                    extra_attributes={
                        "summary_quality_tier": summary_payload.get("summary_quality_tier"),
                        "summary_source": summary_payload.get("summary_source"),
                        "summary_facts": summary_payload.get("summary_facts"),
                        "tone": summary_payload.get("tone"),
                        "moment": summary_payload.get("moment"),
                        "decisions": summary_payload.get("decisions") or [],
                        "unresolved": summary_payload.get("unresolved") or [],
                        "index_text": index_text or "",
                        "salience": salience,
                    },
                    replace_existing_session=True
                )
        except Exception as e:
            logger.error(f"Session ingest summary failed: {e}")

        # Extract loops from full transcript on session ingest (best-effort)
        try:
            if messages_payload:
                # Loops are user-scoped; store under canonical default persona key.
                effective_persona_id = "default"
                last_user_text = next(
                    (
                        m.get("text")
                        for m in reversed(messages_payload)
                        if (m.get("role") or "").lower() == "user" and m.get("text")
                    ),
                    None
                )
                if not last_user_text:
                    last_user_text = messages_payload[-1].get("text") if messages_payload[-1].get("text") else ""

                ts_values = [_parse_msg_ts(m.get("timestamp")) for m in messages_payload]
                ts_values = [t for t in ts_values if t]
                start_ts = min(ts_values) if ts_values else None
                end_ts = max(ts_values) if ts_values else reference_time

                provenance = {
                    "session_id": request.sessionId,
                    "start_ts": start_ts.isoformat() if start_ts else None,
                    "end_ts": end_ts.isoformat() if end_ts else None
                }

                if last_user_text:
                    loop_result = await loops.extract_and_create_loops(
                        tenant_id=request.tenantId,
                        user_id=request.userId,
                        persona_id=effective_persona_id,
                        user_text=last_user_text,
                        recent_turns=messages_payload,
                        source_turn_ts=end_ts or datetime.utcnow(),
                        session_id=request.sessionId,
                        provenance=provenance
                    )
                    logger.info(
                        "Session ingest loop extraction tenant=%s user=%s session=%s new_loops=%s completions=%s",
                        request.tenantId,
                        request.userId,
                        request.sessionId,
                        (loop_result or {}).get("new_loops"),
                        (loop_result or {}).get("completions")
                    )
        except Exception as e:
            logger.error(f"Session ingest loop extraction failed: {e}")

        # Optional: store transcript for debug/audit
        await db.execute(
            """
            INSERT INTO session_transcript (tenant_id, session_id, user_id, messages, updated_at)
            VALUES ($1, $2, $3, $4::jsonb, NOW())
            ON CONFLICT (tenant_id, session_id)
            DO UPDATE SET messages = $4::jsonb, updated_at = NOW(), user_id = EXCLUDED.user_id
            """,
            request.tenantId,
            request.sessionId,
            request.userId,
            messages_payload
        )

        return SessionIngestResponse(
            status="ingested",
            sessionId=request.sessionId,
            graphitiAdded=True
        )
    except Exception as e:
        logger.error(f"Session ingest failed: {e}")
        raise HTTPException(status_code=500, detail="Session ingest failed")


@app.post("/internal/drain")
async def drain(
    limit: int = 200,
    tenant_id: str | None = None,
    budget_seconds: float = 2.0,
    per_row_timeout_seconds: float = 8.0
):
    """Internal outbox drain endpoint."""
    try:
        counts = await session.drain_outbox(
            graphiti_client=graphiti_client,
            limit=limit,
            tenant_id=tenant_id,
            budget_seconds=budget_seconds,
            per_row_timeout_seconds=per_row_timeout_seconds
        )
        return counts
    except Exception as e:
        logger.error(f"Drain endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/internal/debug/session")
async def debug_session(
    tenantId: str,
    userId: str,
    sessionId: str,
    x_internal_token: str | None = Header(default=None)
):
    _require_internal_token(x_internal_token)
    try:
        session_row = await db.fetchone(
            """
            SELECT tenant_id, session_id, user_id, messages, rolling_summary,
                   session_state, created_at, updated_at, closed_at
            FROM session_buffer
            WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3
            """,
            tenantId,
            sessionId,
            userId
        )
        transcript = await db.fetchone(
            """
            SELECT messages, created_at, updated_at
            FROM session_transcript
            WHERE tenant_id = $1 AND session_id = $2
            """,
            tenantId,
            sessionId
        )
        return {
            "session": session_row,
            "transcript": transcript
        }
    except Exception as e:
        logger.error(f"Debug session endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/internal/debug/user")
async def debug_user(
    tenantId: str,
    userId: str,
    x_internal_token: str | None = Header(default=None)
):
    _require_internal_token(x_internal_token)
    try:
        latest_session = await db.fetchone(
            """
            SELECT session_id, updated_at
            FROM session_buffer
            WHERE tenant_id = $1 AND user_id = $2
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            tenantId,
            userId
        )
        session_id = latest_session.get("session_id") if latest_session else None
        last_interaction = None
        if session_id:
            last_interaction = await session.get_last_interaction_time(tenantId, session_id)
        entities = []
        try:
            entities = await graphiti_client.search_nodes(
                tenantId,
                userId,
                query="top entities",
                limit=5,
                reference_time=datetime.utcnow()
            )
        except Exception:
            entities = []
        return {
            "latestSessionId": session_id,
            "lastInteractionTime": last_interaction.isoformat() if last_interaction else None,
            "topEntities": entities
        }
    except Exception as e:
        logger.error(f"Debug user endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/internal/debug/outbox")
async def debug_outbox(
    tenantId: str | None = None,
    limit: int = 50,
    x_internal_token: str | None = Header(default=None)
):
    _require_internal_token(x_internal_token)
    try:
        if tenantId:
            rows = await db.fetch(
                """
                SELECT id, tenant_id, user_id, session_id, status, attempts,
                       next_attempt_at, last_error, created_at, sent_at
                FROM graphiti_outbox
                WHERE tenant_id = $1 AND status IN ('pending', 'failed')
                ORDER BY id DESC
                LIMIT $2
                """,
                tenantId,
                limit
            )
        else:
            rows = await db.fetch(
                """
                SELECT id, tenant_id, user_id, session_id, status, attempts,
                       next_attempt_at, last_error, created_at, sent_at
                FROM graphiti_outbox
                WHERE status IN ('pending', 'failed')
                ORDER BY id DESC
                LIMIT $1
                """,
                limit
            )
        return {"rows": rows}
    except Exception as e:
        logger.error(f"Debug outbox endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/internal/debug/loops")
async def debug_loops(
    tenantId: str,
    userId: str,
    format: str | None = None,
    x_internal_token: str | None = Header(default=None)
):
    _require_internal_token(x_internal_token)
    try:
        rows = await loops.get_active_loops_debug(tenantId, userId)
        if (format or "").lower() == "csv":
            output = io.StringIO()
            fieldnames = sorted({key for row in rows for key in row.keys()})
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
            return Response(content=output.getvalue(), media_type="text/csv")
        return {"count": len(rows), "rows": rows}
    except Exception as e:
        logger.error(f"Debug loops endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/internal/debug/close_session")
async def debug_close_session(
    tenantId: str,
    userId: str,
    sessionId: str,
    personaId: str | None = None,
    x_internal_token: str | None = Header(default=None)
):
    """Force-close a single session immediately."""
    _require_internal_token(x_internal_token)
    try:
        ok = await session.close_session(
            tenant_id=tenantId,
            session_id=sessionId,
            user_id=userId,
            graphiti_client=graphiti_client,
            persona_id=personaId
        )
        return {"closed": bool(ok), "sessionId": sessionId}
    except Exception as e:
        logger.error(f"Debug close_session error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/internal/debug/close_user_sessions")
async def debug_close_user_sessions(
    tenantId: str,
    userId: str,
    limit: int = 20,
    personaId: str | None = None,
    x_internal_token: str | None = Header(default=None)
):
    """Force-close all open sessions for a user (bounded by limit)."""
    _require_internal_token(x_internal_token)
    try:
        rows = await db.fetch(
            """
            SELECT session_id
            FROM session_buffer
            WHERE tenant_id = $1 AND user_id = $2 AND closed_at IS NULL
            ORDER BY updated_at DESC
            LIMIT $3
            """,
            tenantId,
            userId,
            limit
        )
        closed = []
        for row in rows:
            session_id = row["session_id"]
            ok = await session.close_session(
                tenant_id=tenantId,
                session_id=session_id,
                user_id=userId,
                graphiti_client=graphiti_client,
                persona_id=personaId
            )
            if ok:
                closed.append(session_id)
        return {"closedCount": len(closed), "closedSessions": closed}
    except Exception as e:
        logger.error(f"Debug close_user_sessions error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/internal/debug/emit_raw_episode")
async def debug_emit_raw_episode(
    tenantId: str,
    userId: str,
    sessionId: str,
    x_internal_token: str | None = Header(default=None)
):
    """Emit raw transcript episode to Graphiti for a session."""
    _require_internal_token(x_internal_token)
    try:
        await session.send_raw_transcript_episode(
            tenant_id=tenantId,
            session_id=sessionId,
            user_id=userId,
            graphiti_client=graphiti_client
        )
        return {"emitted": True, "sessionId": sessionId}
    except Exception as e:
        logger.error(f"Debug emit_raw_episode error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/internal/debug/emit_raw_user_sessions")
async def debug_emit_raw_user_sessions(
    tenantId: str,
    userId: str,
    limit: int = 20,
    x_internal_token: str | None = Header(default=None)
):
    """Emit raw transcript episodes for recent sessions for a user."""
    _require_internal_token(x_internal_token)
    try:
        rows = await db.fetch(
            """
            SELECT session_id
            FROM session_buffer
            WHERE tenant_id = $1 AND user_id = $2
            ORDER BY updated_at DESC
            LIMIT $3
            """,
            tenantId,
            userId,
            limit
        )
        emitted = []
        for row in rows:
            session_id = row["session_id"]
            await session.send_raw_transcript_episode(
                tenant_id=tenantId,
                session_id=session_id,
                user_id=userId,
                graphiti_client=graphiti_client
            )
            emitted.append(session_id)
        return {"emittedCount": len(emitted), "sessions": emitted}
    except Exception as e:
        logger.error(f"Debug emit_raw_user_sessions error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/internal/debug/graphiti/episodes")
async def debug_graphiti_episodes(
    tenantId: str,
    userId: str,
    limit: int = 5,
    x_internal_token: str | None = Header(default=None)
):
    _require_internal_token(x_internal_token)
    try:
        episodes = await graphiti_client.get_recent_episodes(
            tenant_id=tenantId,
            user_id=userId,
            since=None,
            limit=limit
        )
        results = []
        for episode in episodes or []:
            if isinstance(episode, dict):
                results.append({
                    "name": episode.get("name") or episode.get("episode_name"),
                    "summary": episode.get("summary") or episode.get("episode_summary"),
                    "reference_time": episode.get("reference_time") or episode.get("created_at"),
                    "content": episode.get("episode_body") or episode.get("content") or episode.get("text"),
                    "uuid": episode.get("uuid")
                })
            else:
                results.append({
                    "name": getattr(episode, "name", None),
                    "summary": getattr(episode, "summary", None) or getattr(episode, "episode_summary", None),
                    "reference_time": getattr(episode, "reference_time", None) or getattr(episode, "created_at", None),
                    "content": getattr(episode, "episode_body", None) or getattr(episode, "content", None),
                    "uuid": str(getattr(episode, "uuid", None)) if getattr(episode, "uuid", None) else None
                })
        return {"count": len(results), "episodes": results}
    except Exception as e:
        logger.error(f"Debug graphiti episodes failed: {e}")
        raise HTTPException(status_code=500, detail="Debug graphiti episodes failed")


@app.get("/internal/debug/startbrief/history")
async def debug_startbrief_history(
    tenantId: str,
    userId: str,
    limit: int = 20,
    x_internal_token: str | None = Header(default=None)
):
    _require_internal_token(x_internal_token)
    try:
        safe_limit = max(1, min(int(limit or 20), 200))
        rows = await db.fetch(
            """
            SELECT id, session_id, requested_at, time_of_day_label, time_gap_human,
                   bridge_text, items, context
            FROM startbrief_history
            WHERE tenant_id = $1
              AND user_id = $2
            ORDER BY requested_at DESC
            LIMIT $3
            """,
            tenantId,
            userId,
            safe_limit
        )
        return {
            "count": len(rows),
            "items": [
                {
                    "id": row.get("id"),
                    "session_id": row.get("session_id"),
                    "requested_at": row.get("requested_at").isoformat() if row.get("requested_at") else None,
                    "time_of_day_label": row.get("time_of_day_label"),
                    "time_gap_human": row.get("time_gap_human"),
                    "bridge_text": row.get("bridge_text"),
                    "items": row.get("items") or [],
                    "context": row.get("context") or {}
                }
                for row in rows
            ]
        }
    except Exception as e:
        logger.error(f"Debug startbrief history failed: {e}")
        raise HTTPException(status_code=500, detail="Debug startbrief history failed")


@app.post("/internal/debug/graphiti/query")
async def debug_graphiti_query(
    request: MemoryQueryRequest,
    x_internal_token: str | None = Header(default=None)
):
    _require_internal_token(x_internal_token)
    try:
        reference_time = None
        if request.referenceTime:
            value = request.referenceTime
            if value.endswith("Z"):
                value = value.replace("Z", "+00:00")
            reference_time = datetime.fromisoformat(value)

        facts = await graphiti_client.search_facts(
            tenant_id=request.tenantId,
            user_id=request.userId,
            query=request.query,
            limit=request.limit or 10,
            reference_time=reference_time
        )
        entities = await graphiti_client.search_nodes(
            tenant_id=request.tenantId,
            user_id=request.userId,
            query=request.query,
            limit=min(request.limit or 10, 10),
            reference_time=reference_time
        )
        return {"facts": facts, "entities": entities}
    except Exception as e:
        logger.error(f"Debug graphiti query failed: {e}")
        raise HTTPException(status_code=500, detail="Debug graphiti query failed")


@app.get("/internal/debug/graphiti/session_summary_write_metrics")
async def debug_graphiti_session_summary_write_metrics(
    x_internal_token: str | None = Header(default=None)
):
    _require_internal_token(x_internal_token)
    return graphiti_client.get_session_summary_write_metrics()


@app.get("/internal/debug/graphiti/session_summaries")
async def debug_graphiti_session_summaries(
    tenantId: str,
    userId: str,
    limit: int = 5,
    all: bool = False,
    x_internal_token: str | None = Header(default=None)
):
    _require_internal_token(x_internal_token)
    try:
        if not graphiti_client._initialized:
            await graphiti_client.initialize()
        if not graphiti_client.client:
            return {"count": 0, "summaries": [], "reason": "graphiti_unavailable"}

        driver = getattr(graphiti_client.client, "driver", None)
        if not driver:
            return {"count": 0, "summaries": [], "reason": "driver_unavailable"}

        composite_user_id = graphiti_client._make_composite_user_id(tenantId, userId)
        scoped_driver = driver
        if not all:
            clone = getattr(driver, "clone", None)
            if callable(clone):
                try:
                    scoped_driver = clone(database=composite_user_id)
                except Exception:
                    scoped_driver = driver
        count_query = """
            MATCH (n:SessionSummary {group_id: $group_id})
            RETURN count(n) AS count
        """
        count_params = {"group_id": composite_user_id}
        if all:
            count_query = "MATCH (n:SessionSummary) RETURN count(n) AS count"
            count_params = {}
        count_rows = await scoped_driver.execute_query(count_query, **count_params)
        graph_count = extract_count(count_rows or [])

        if all:
            rows = await scoped_driver.execute_query(
                """
                MATCH (n:SessionSummary)
                RETURN n.name AS name,
                       n.summary AS summary,
                       n.attributes AS attributes,
                       n.created_at AS created_at,
                       n.uuid AS uuid,
                       n.group_id AS group_id
                ORDER BY n.created_at DESC
                LIMIT $limit
                """,
                limit=limit
            )
        else:
            rows = await scoped_driver.execute_query(
                """
                MATCH (n:SessionSummary {group_id: $group_id})
                RETURN n.name AS name,
                       n.summary AS summary,
                       n.attributes AS attributes,
                       n.created_at AS created_at,
                       n.uuid AS uuid,
                       n.group_id AS group_id
                ORDER BY n.created_at DESC
                LIMIT $limit
                """,
                group_id=composite_user_id,
                limit=limit
            )
        summaries = []
        for row in rows or []:
            nodes = extract_node_dicts(row, required_keys=("name",))
            if nodes:
                for node in nodes:
                    summary_value = node.get("summary") or node.get("summary_text")
                    attributes = node.get("attributes") or {}
                    summaries.append({
                        "name": node.get("name"),
                        "summary": summary_value,
                        "attributes": attributes,
                        "created_at": node.get("created_at"),
                        "uuid": node.get("uuid"),
                        "group_id": node.get("group_id")
                    })
                continue
            if isinstance(row, (list, tuple)):
                name = row[0] if len(row) > 0 else None
                summary = row[1] if len(row) > 1 else None
                attributes = row[2] if len(row) > 2 else {}
                created_at = row[3] if len(row) > 3 else None
                uuid = row[4] if len(row) > 4 else None
                group_id = row[5] if len(row) > 5 else None
                if name is None and summary is None and uuid is None:
                    continue
                if not summary and isinstance(attributes, dict):
                    summary = attributes.get("summary_text")
                summaries.append({
                    "name": name,
                    "summary": summary,
                    "attributes": attributes or {},
                    "created_at": created_at,
                    "uuid": uuid,
                    "group_id": group_id
                })
        response = {"count": len(summaries), "summaries": summaries, "graph_count": graph_count}
        if not summaries:
            response["raw_rows"] = rows
        return response
    except Exception as e:
        logger.error(f"Debug graphiti session_summaries failed: {e}")
        raise HTTPException(status_code=500, detail="Debug graphiti session_summaries failed")


@app.get("/internal/debug/graphiti/session_summaries_clean")
async def debug_graphiti_session_summaries_clean(
    tenantId: str,
    userId: str,
    limit: int = 5,
    x_internal_token: str | None = Header(default=None)
):
    """Clean session summaries list (flat fields, de-duplicated)."""
    _require_internal_token(x_internal_token)
    try:
        if not graphiti_client._initialized:
            await graphiti_client.initialize()
        if not graphiti_client.client:
            return {"count": 0, "summaries": [], "reason": "graphiti_unavailable"}

        driver = getattr(graphiti_client.client, "driver", None)
        if not driver:
            return {"count": 0, "summaries": [], "reason": "driver_unavailable"}

        composite_user_id = graphiti_client._make_composite_user_id(tenantId, userId)
        scoped_driver = driver
        clone = getattr(driver, "clone", None)
        if callable(clone):
            try:
                scoped_driver = clone(database=composite_user_id)
            except Exception:
                scoped_driver = driver
        rows = await scoped_driver.execute_query(
            """
            MATCH (n:SessionSummary {group_id: $group_id})
            RETURN properties(n) AS props
            ORDER BY n.created_at DESC
            LIMIT $limit
            """,
            group_id=composite_user_id,
            limit=limit
        )

        seen = set()
        summaries = []
        for row in rows or []:
            for node in extract_node_dicts(row, required_keys=("uuid",)):
                uuid = node.get("uuid")
                if not uuid or uuid in seen:
                    continue
                summary_value = node.get("summary") or node.get("summary_text")
                if not summary_value:
                    attributes = node.get("attributes") or {}
                    if isinstance(attributes, dict):
                        summary_value = attributes.get("summary_text")
                seen.add(uuid)
                summaries.append({
                    "name": node.get("name"),
                    "summary": summary_value,
                    "attributes": node.get("attributes") or {},
                    "created_at": node.get("created_at"),
                    "uuid": node.get("uuid"),
                    "group_id": node.get("group_id")
                })
        response = {"count": len(summaries), "summaries": summaries}
        if not summaries:
            response["raw_rows"] = rows
        return response
    except Exception as e:
        logger.error(f"Debug graphiti session_summaries_clean failed: {e}")
        raise HTTPException(status_code=500, detail="Debug graphiti session_summaries_clean failed")


@app.get("/internal/debug/graphiti/session_summaries_view")
async def debug_graphiti_session_summaries_view(
    tenantId: str,
    userId: str,
    limit: int = 10,
    x_internal_token: str | None = Header(default=None)
):
    """Human-readable view of recent session summaries + bridge_text."""
    _require_internal_token(x_internal_token)
    try:
        if not graphiti_client._initialized:
            await graphiti_client.initialize()
        if not graphiti_client.client:
            return {"count": 0, "summaries": [], "reason": "graphiti_unavailable"}

        driver = getattr(graphiti_client.client, "driver", None)
        if not driver:
            return {"count": 0, "summaries": [], "reason": "driver_unavailable"}

        composite_user_id = graphiti_client._make_composite_user_id(tenantId, userId)
        scoped_driver = driver
        clone = getattr(driver, "clone", None)
        if callable(clone):
            try:
                scoped_driver = clone(database=composite_user_id)
            except Exception:
                scoped_driver = driver
        rows = await scoped_driver.execute_query(
            """
            MATCH (n:SessionSummary {group_id: $group_id})
            RETURN properties(n) AS props
            ORDER BY n.created_at DESC
            LIMIT $limit
            """,
            group_id=composite_user_id,
            limit=limit
        )

        seen = set()
        summaries = []
        for row in rows or []:
            for node in extract_node_dicts(row, required_keys=("uuid",)):
                uuid = node.get("uuid")
                if not uuid or uuid in seen:
                    continue
                seen.add(uuid)
                summary_value = node.get("summary") or node.get("summary_text")
                bridge_text = node.get("bridge_text")
                session_id = node.get("session_id")
                attributes = node.get("attributes") or {}
                if isinstance(attributes, dict):
                    summary_value = summary_value or attributes.get("summary_text")
                    bridge_text = bridge_text or attributes.get("bridge_text")
                    session_id = session_id or attributes.get("session_id")
                if not summary_value or summary_value == "summary":
                    continue
                summaries.append({
                    "created_at": node.get("created_at"),
                    "summary": summary_value,
                    "bridge_text": bridge_text,
                    "session_id": session_id
                })
        return {"count": len(summaries), "summaries": summaries}
    except Exception as e:
        logger.error(f"Debug graphiti session_summaries_view failed: {e}")
        raise HTTPException(status_code=500, detail="Debug graphiti session_summaries_view failed")


@app.post("/internal/debug/backfill/session_summaries")
async def debug_backfill_session_summaries(
    tenantId: Optional[str] = None,
    userId: Optional[str] = None,
    limit: int = 100,
    dryRun: bool = True,
    x_internal_token: str | None = Header(default=None)
):
    """Regenerate SessionSummary nodes from full session_transcript messages."""
    _require_internal_token(x_internal_token)
    try:
        safe_limit = max(1, min(limit, 2000))
        rows = await db.fetch(
            """
            SELECT tenant_id, user_id, session_id, messages, updated_at
            FROM session_transcript
            WHERE ($1::text IS NULL OR tenant_id = $1)
              AND ($2::text IS NULL OR user_id = $2)
            ORDER BY updated_at DESC
            LIMIT $3
            """,
            tenantId,
            userId,
            safe_limit
        )

        processed = 0
        updated = 0
        preview: List[Dict[str, Any]] = []
        for row in rows:
            tenant_id = row.get("tenant_id")
            user_id = row.get("user_id")
            session_id = row.get("session_id")
            messages = row.get("messages")
            if isinstance(messages, str):
                try:
                    messages = json.loads(messages)
                except Exception:
                    messages = []
            if not isinstance(messages, list):
                messages = []

            reference_time = datetime.utcnow()
            for msg in reversed(messages):
                ts = msg.get("timestamp") if isinstance(msg, dict) else None
                if not ts:
                    continue
                try:
                    reference_time = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                    break
                except Exception:
                    continue

            recap = await session.summarize_session_messages(messages)
            summary_text = (recap.get("summary_text") or "").strip()
            bridge_text = (recap.get("bridge_text") or "").strip()
            quality_tier = recap.get("summary_quality_tier")
            source = recap.get("summary_source")
            summary_facts = recap.get("summary_facts")
            tone = recap.get("tone")
            moment = recap.get("moment")
            decisions = recap.get("decisions") or []
            unresolved = recap.get("unresolved") or []
            index_text = recap.get("index_text") or ""
            salience = recap.get("salience") or "low"

            processed += 1
            if not dryRun:
                await graphiti_client.add_session_summary(
                    tenant_id=tenant_id,
                    user_id=user_id,
                    session_id=session_id,
                    summary_text=summary_text,
                    bridge_text=bridge_text,
                    reference_time=reference_time,
                    episode_uuid=None,
                    extra_attributes={
                        "summary_quality_tier": quality_tier,
                        "summary_source": source,
                        "summary_facts": summary_facts,
                        "tone": tone,
                        "moment": moment,
                        "decisions": decisions,
                        "unresolved": unresolved,
                        "index_text": index_text,
                        "salience": salience,
                        "backfilled_at": datetime.utcnow().isoformat(),
                        "backfill_version": "session_summary_v2"
                    },
                    replace_existing_session=True
                )
                updated += 1

            if len(preview) < 20:
                preview.append({
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "session_id": session_id,
                    "summary_quality_tier": quality_tier,
                    "summary_facts": summary_facts,
                    "tone": tone,
                    "moment": moment,
                    "decisions": decisions,
                    "unresolved": unresolved,
                    "index_text": index_text,
                    "salience": salience,
                    "summary_text": summary_text,
                    "bridge_text": bridge_text
                })

        return {
            "dryRun": dryRun,
            "processed": processed,
            "updated": updated,
            "limit": safe_limit,
            "preview": preview
        }
    except Exception as e:
        logger.error(f"Backfill session summaries failed: {e}")
        raise HTTPException(status_code=500, detail="Backfill session summaries failed")


@app.post("/internal/debug/backfill/loops")
async def debug_backfill_loops(
    tenantId: Optional[str] = None,
    userId: Optional[str] = None,
    personaId: Optional[str] = None,
    limit: int = 20,
    dryRun: bool = True,
    force: bool = False,
    x_internal_token: str | None = Header(default=None)
):
    """Backfill loop extraction from recent session transcripts (session-level)."""
    _require_internal_token(x_internal_token)
    try:
        safe_limit = max(1, min(limit, 500))
        rows = await db.fetch(
            """
            SELECT tenant_id, user_id, session_id, messages, updated_at
            FROM session_transcript
            WHERE ($1::text IS NULL OR tenant_id = $1)
              AND ($2::text IS NULL OR user_id = $2)
            ORDER BY updated_at DESC
            LIMIT $3
            """,
            tenantId,
            userId,
            safe_limit
        )

        processed = 0
        skipped = 0
        extracted = 0
        preview: List[Dict[str, Any]] = []

        for row in rows:
            tenant_id = row.get("tenant_id")
            user_id = row.get("user_id")
            session_id = row.get("session_id")
            messages = row.get("messages")
            if isinstance(messages, str):
                try:
                    messages = json.loads(messages)
                except Exception:
                    messages = []
            if not isinstance(messages, list) or not messages:
                skipped += 1
                continue

            effective_persona_id = personaId or "default"

            # Avoid reprocessing the same session unless forced.
            if not force:
                existing = await db.fetchone(
                    """
                    SELECT id
                    FROM loops
                    WHERE tenant_id = $1
                      AND user_id = $2
                      AND metadata->'provenance'->>'session_id' = $3
                    LIMIT 1
                    """,
                    tenant_id,
                    user_id,
                    session_id
                )
                if existing:
                    skipped += 1
                    continue

            last_user_text = next(
                (
                    m.get("text")
                    for m in reversed(messages)
                    if isinstance(m, dict)
                    and (m.get("role") or "").lower() == "user"
                    and m.get("text")
                ),
                None
            )
            if not last_user_text:
                last_user_text = messages[-1].get("text") if isinstance(messages[-1], dict) else None
            if not last_user_text:
                skipped += 1
                continue

            def _parse_msg_ts(value: Any) -> Optional[datetime]:
                if not value:
                    return None
                try:
                    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                except Exception:
                    return None

            ts_values = []
            for msg in messages:
                if not isinstance(msg, dict):
                    continue
                parsed = _parse_msg_ts(msg.get("timestamp"))
                if parsed:
                    ts_values.append(parsed)

            start_ts = min(ts_values) if ts_values else None
            end_ts = max(ts_values) if ts_values else row.get("updated_at") or datetime.utcnow()

            provenance = {
                "session_id": session_id,
                "start_ts": start_ts.isoformat() if start_ts else None,
                "end_ts": end_ts.isoformat() if isinstance(end_ts, datetime) else None
            }

            processed += 1
            if len(preview) < 20:
                preview.append({
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "persona_id": effective_persona_id,
                    "session_id": session_id,
                    "message_count": len(messages),
                    "last_user_text": (last_user_text or "")[:140],
                    "provenance": provenance
                })

            if dryRun:
                continue

            result = await loops.extract_and_create_loops(
                tenant_id=tenant_id,
                user_id=user_id,
                persona_id=effective_persona_id,
                user_text=last_user_text,
                recent_turns=messages,
                source_turn_ts=end_ts if isinstance(end_ts, datetime) else datetime.utcnow(),
                session_id=session_id,
                provenance=provenance
            )
            extracted += int((result or {}).get("new_loops") or 0)

        return {
            "dryRun": dryRun,
            "force": force,
            "processed": processed,
            "skipped": skipped,
            "new_loops": extracted,
            "limit": safe_limit,
            "preview": preview
        }
    except Exception as e:
        logger.error(f"Backfill loops failed: {e}")
        raise HTTPException(status_code=500, detail="Backfill loops failed")


@app.get("/internal/debug/graphiti/session_summaries_lookup")
async def debug_graphiti_session_summaries_lookup(
    sessionId: Optional[str] = None,
    nameContains: Optional[str] = None,
    database: Optional[str] = None,
    limit: int = 20,
    x_internal_token: str | None = Header(default=None)
):
    """Lookup SessionSummary nodes across groups by session_id and/or name fragment."""
    _require_internal_token(x_internal_token)
    try:
        if not graphiti_client._initialized:
            await graphiti_client.initialize()
        if not graphiti_client.client:
            return {"count": 0, "summaries": [], "reason": "graphiti_unavailable"}

        driver = getattr(graphiti_client.client, "driver", None)
        if not driver:
            return {"count": 0, "summaries": [], "reason": "driver_unavailable"}

        scoped_driver = driver
        if database:
            clone = getattr(driver, "clone", None)
            if callable(clone):
                try:
                    scoped_driver = clone(database=database)
                except Exception:
                    scoped_driver = driver

        rows = await scoped_driver.execute_query(
            """
            MATCH (n:SessionSummary)
            WHERE ($session_id IS NULL OR n.session_id = $session_id)
              AND ($name_contains IS NULL OR n.name CONTAINS $name_contains)
            RETURN properties(n) AS props
            ORDER BY n.created_at DESC
            LIMIT $limit
            """,
            session_id=sessionId,
            name_contains=nameContains,
            limit=limit
        )

        seen = set()
        summaries = []
        for row in rows or []:
            for node in extract_node_dicts(row, required_keys=("uuid",)):
                uuid = node.get("uuid")
                if not uuid or uuid in seen:
                    continue
                summary_value = node.get("summary") or node.get("summary_text")
                bridge_text = node.get("bridge_text")
                if not summary_value or not bridge_text:
                    attributes = node.get("attributes") or {}
                    if isinstance(attributes, dict):
                        summary_value = summary_value or attributes.get("summary_text")
                        bridge_text = bridge_text or attributes.get("bridge_text")
                seen.add(uuid)
                summaries.append({
                    "name": node.get("name"),
                    "summary": summary_value,
                    "bridge_text": bridge_text,
                    "session_id": node.get("session_id"),
                    "group_id": node.get("group_id"),
                    "uuid": uuid,
                    "created_at": node.get("created_at")
                })
        response = {"count": len(summaries), "summaries": summaries}
        if database:
            response["database"] = database
        return response
    except Exception as e:
        logger.error(f"Debug graphiti session_summaries_lookup failed: {e}")
        raise HTTPException(status_code=500, detail="Debug graphiti session_summaries_lookup failed")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
