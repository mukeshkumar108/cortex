from fastapi import FastAPI, HTTPException, BackgroundTasks, Header, Response, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
from datetime import datetime, timedelta, date, timezone as dt_timezone
import logging
import re
import json
import uuid
from copy import deepcopy
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import parse_qsl, urlencode

from .models import (
    IngestRequest,
    BriefRequest,
    IngestResponse,
    BriefResponse,
    MemoryQueryRequest,
    MemoryQueryResponse,
    MemoryQueryV2Request,
    MemoryQueryV2Response,
    MemoryQueryV2Item,
    MemoryLoopsResponse,
    MemoryLoopItem,
    Fact,
    Entity,
    EpisodeRecallItem,
    SessionStartBriefResponse,
    SessionStartBriefItem,
    SessionStartBriefEntityProfile,
    SessionStartBriefEntityHint,
    SessionCloseRequest,
    SessionIngestRequest,
    SessionIngestResponse,
    SessionBriefResponse,
    EntityProfileRequest,
    EntityProfileResponse,
    PurgeUserRequest,
    UserModelPatchRequest,
    UserModelResponse,
    DailyAnalysisResponse,
    HabitDailyLogUpsertRequest,
    HabitDailyLogUpsertResponse,
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
from .session import EvidenceContractError
from .briefing import build_briefing
from .migrate import run_migrations
from .openrouter_client import get_llm_client
from .memory_taxonomy import (
    classify_memory_semantic_fallback,
    classify_memory_candidates_semantic,
    summarize_domain_distribution,
    summarize_label_distribution,
)
from .episodic_memory import (
    search_episode_embedding_candidates,
    get_user_episodic_embedding_stats,
    embed_texts,
)
from .memory_ontology import (
    ONTOLOGY_DOMAIN_FACETS,
    ONTOLOGY_EDGE_TYPES,
    ONTOLOGY_NODE_TYPES,
    canonicalize_entity_name,
    infer_ontology_type,
    infer_query_node_types,
    is_allowed_runtime_node,
)
from .canonicalization import normalize_text as canonicalize_text, stable_short_hash
from .extraction_results import persist_extract_result, ExtractionContractError

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
    return canonicalize_text(value, casefold=False)


def _format_vector(vector: List[float]) -> str:
    return "[" + ",".join(str(float(x)) for x in vector) + "]"


SOPHIE_CLUSTER_NAMES = {
    "sophie",
    "sophie repository",
    "synapse",
    "relational continuity engine",
}


def _cluster_projects(projects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    sophie_cluster: List[Dict[str, Any]] = []
    other_projects: List[Dict[str, Any]] = []

    for p in projects or []:
        name = _normalize_text(p.get("canonical_name")).lower()
        aliases = p.get("aliases") if isinstance(p.get("aliases"), list) else []
        alias_norm = {_normalize_text(a).lower() for a in aliases if _normalize_text(a)}
        in_sophie_cluster = (
            name in SOPHIE_CLUSTER_NAMES
            or any(a in SOPHIE_CLUSTER_NAMES for a in alias_norm)
            or ("sophie" in name)
            or ("synapse" in name)
        )
        if in_sophie_cluster:
            sophie_cluster.append(p)
        else:
            other_projects.append(p)

    result: List[Dict[str, Any]] = []
    if sophie_cluster:
        primary = max(
            sophie_cluster,
            key=lambda x: float(x.get("salience_score") or 0.0),
        )
        result.append(
            {
                "cluster_name": "Sophie / Synapse",
                "components": [_normalize_text(p.get("canonical_name")) for p in sophie_cluster if _normalize_text(p.get("canonical_name"))],
                "primary_profile": _normalize_text(primary.get("profile_text")) or None,
                "current_status": _normalize_text(primary.get("last_known_status")) or None,
                "salience": max(float(p.get("salience_score") or 0.0) for p in sophie_cluster),
            }
        )

    for p in other_projects:
        result.append(
            {
                "cluster_name": _normalize_text(p.get("canonical_name")),
                "components": [_normalize_text(p.get("canonical_name"))],
                "primary_profile": _normalize_text(p.get("profile_text")) or None,
                "current_status": _normalize_text(p.get("last_known_status")) or None,
                "salience": float(p.get("salience_score") or 0.0),
            }
        )

    return sorted(result, key=lambda x: float(x.get("salience") or 0.0), reverse=True)


async def _episodic_search(
    *,
    user_id: str,
    query: str,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    text = _normalize_text(query)
    if not _normalize_text(user_id) or not text:
        return []
    model = _normalize_text(get_settings().memory_semantic_embedding_model) or "text-embedding-3-small"
    vectors = await embed_texts([text], model=model)
    if not vectors or not isinstance(vectors, list) or not vectors or not isinstance(vectors[0], list):
        return []
    query_vec = _format_vector(vectors[0])
    rows = await db.fetch(
        """
        SELECT
          session_id,
          session_date,
          session_kind,
          emotional_weight,
          raw_triage_output->'memory_deltas' AS deltas,
          1 - (memory_delta_embedding <=> $1::vector) AS similarity
        FROM session_classifications
        WHERE user_id = $2
          AND is_memory_worthy = true
          AND memory_delta_embedding IS NOT NULL
        ORDER BY memory_delta_embedding <=> $1::vector
        LIMIT $3
        """,
        query_vec,
        user_id,
        max(1, min(int(limit or 5), 20)),
    )
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        out.append(
            {
                "session_id": _normalize_text(row.get("session_id")),
                "session_date": row.get("session_date").isoformat() if isinstance(row.get("session_date"), datetime) else _normalize_text(row.get("session_date")),
                "session_kind": _normalize_text(row.get("session_kind")),
                "emotional_weight": _normalize_text(row.get("emotional_weight")),
                "memory_deltas": row.get("deltas") if isinstance(row.get("deltas"), list) else [],
                "similarity": float(row.get("similarity") or 0.0),
            }
        )
    return out


async def _build_handover_packet(user_id: str) -> Dict[str, Any]:
    def _payload_value(payload: Any) -> str:
        if isinstance(payload, dict):
            for key in ("value", "text", "status", "summary", "name", "title"):
                value = _normalize_text(payload.get(key))
                if value:
                    return value
            compact = _normalize_text(json.dumps(payload, sort_keys=True, ensure_ascii=True))
            return compact
        if isinstance(payload, list):
            parts = [_normalize_text(x) for x in payload if _normalize_text(x)]
            return ", ".join(parts[:3])
        return _normalize_text(payload)

    def _claim_sentence(row: Dict[str, Any]) -> str:
        predicate = _normalize_text(row.get("predicate")).lower()
        subject = _normalize_text(row.get("subject_name")) or _normalize_text(row.get("subject_text")) or "The user"
        value = _payload_value(row.get("object_payload"))
        tail = predicate.replace("_", ".").split(".")[-1] if predicate else "update"
        tail = tail.replace("_", " ").strip() or "update"
        if value:
            return f"{subject} {tail}: {value}"
        return f"{subject} {tail}."

    def _claim_matches(predicate: str, terms: List[str]) -> bool:
        clean = _normalize_text(predicate).lower()
        return bool(clean) and any(term in clean for term in terms)

    async def _load_canonical_inputs(target_user_id: str) -> Dict[str, Any]:
        claim_rows = await db.fetch(
            """
            SELECT
              c.tenant_id,
              c.claim_event_key,
              c.predicate,
              c.subject_entity_id::text AS subject_entity_id,
              c.subject_text,
              c.object_payload,
              c.truth_confidence,
              c.updated_at,
              c.created_at,
              e.canonical_name AS subject_name,
              e.entity_type,
              COUNT(ce.claim_evidence_id)::int AS evidence_count
            FROM claims c
            LEFT JOIN entities e
              ON e.tenant_id = c.tenant_id
             AND e.entity_id = c.subject_entity_id
            LEFT JOIN claim_evidence ce
              ON ce.tenant_id = c.tenant_id
             AND ce.claim_id = c.claim_id
            WHERE c.user_id = $1
              AND c.lifecycle_status = 'active'
            GROUP BY
              c.tenant_id,
              c.claim_event_key,
              c.predicate,
              c.subject_entity_id,
              c.subject_text,
              c.object_payload,
              c.truth_confidence,
              c.updated_at,
              c.created_at,
              e.canonical_name,
              e.entity_type
            ORDER BY c.updated_at DESC NULLS LAST, c.claim_event_key ASC
            LIMIT 300
            """,
            target_user_id,
        )
        entity_rows = await db.fetch(
            """
            SELECT
              tenant_id,
              entity_id::text AS entity_id,
              canonical_name,
              entity_type,
              status,
              created_at,
              updated_at
            FROM entities
            WHERE user_id = $1
              AND status = 'active'
            ORDER BY updated_at DESC NULLS LAST, canonical_name_normalized ASC
            LIMIT 200
            """,
            target_user_id,
        )
        watermark_rows = await db.fetch(
            """
            SELECT
              w.tenant_id,
              w.last_sequence,
              w.updated_at
            FROM canonical_tenant_watermarks w
            WHERE EXISTS (
                SELECT 1
                FROM claims c
                WHERE c.tenant_id = w.tenant_id
                  AND c.user_id = $1
            )
            OR EXISTS (
                SELECT 1
                FROM entities e
                WHERE e.tenant_id = w.tenant_id
                  AND e.user_id = $1
            )
            ORDER BY w.tenant_id ASC
            """,
            target_user_id,
        )
        return {
            "claims": claim_rows or [],
            "entities": entity_rows or [],
            "watermarks": watermark_rows or [],
        }

    def _canonical_living_fallback(claims_rows: List[Dict[str, Any]]) -> Dict[str, Optional[str]]:
        rows = [r for r in (claims_rows or []) if int(r.get("evidence_count") or 0) > 0]
        if not rows:
            return {
                "current_focus": None,
                "primary_tension": None,
                "unspoken_goal": None,
                "emotional_texture": None,
                "relationship_pulse": None,
            }

        def _pick(terms: List[str]) -> Optional[str]:
            for row in rows:
                if not _claim_matches(str(row.get("predicate") or ""), terms):
                    continue
                text = _claim_sentence(row)
                if text:
                    return _shorten_line(text, 220)
            return None

        focus = _pick(["focus", "goal", "plan", "priority", "project", "work"])
        tension = _pick(["tension", "conflict", "stress", "worry", "problem", "blocker"])
        unspoken_goal = _pick(["want", "intent", "desire", "goal"])
        emotional = _pick(["emotion", "mood", "feeling", "affect", "state"])
        relationship = _pick(["relationship", "partner", "family", "friend"])
        return {
            "current_focus": focus,
            "primary_tension": tension,
            "unspoken_goal": unspoken_goal,
            "emotional_texture": emotional,
            "relationship_pulse": relationship,
        }

    def _canonical_identity_fallback(claims_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        rows = [r for r in (claims_rows or []) if int(r.get("evidence_count") or 0) > 0]
        if not rows:
            return {
                "current_chapter": None,
                "core_values": [],
                "persistent_goals": [],
                "what_they_want": None,
            }
        chapter = None
        core_values: List[str] = []
        goals: List[str] = []
        what_they_want = None
        seen_values = set()
        seen_goals = set()
        for row in rows:
            predicate = _normalize_text(row.get("predicate")).lower()
            sentence = _shorten_line(_claim_sentence(row), 220)
            if not sentence:
                continue
            if chapter is None and _claim_matches(predicate, ["chapter", "season", "phase", "identity"]):
                chapter = sentence
            if _claim_matches(predicate, ["value", "belief", "principle"]):
                key = sentence.lower()
                if key not in seen_values and len(core_values) < 3:
                    seen_values.add(key)
                    core_values.append(sentence)
            if _claim_matches(predicate, ["goal", "objective", "commitment"]):
                key = sentence.lower()
                if key not in seen_goals and len(goals) < 6:
                    seen_goals.add(key)
                    goals.append(sentence)
            if what_they_want is None and _claim_matches(predicate, ["want", "intent", "desire"]):
                what_they_want = sentence
        if chapter is None and goals:
            chapter = goals[0]
        return {
            "current_chapter": chapter,
            "core_values": core_values,
            "persistent_goals": goals,
            "what_they_want": what_they_want,
        }

    def _canonical_threads_fallback(claims_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        rows = [r for r in (claims_rows or []) if int(r.get("evidence_count") or 0) > 0]
        out: List[Dict[str, Any]] = []
        seen = set()
        for row in rows:
            predicate = _normalize_text(row.get("predicate")).lower()
            if not _claim_matches(predicate, ["goal", "task", "deadline", "commit", "habit", "plan", "project", "relationship"]):
                continue
            title = _shorten_line(_claim_sentence(row), 180)
            if not title:
                continue
            key = title.lower()
            if key in seen:
                continue
            seen.add(key)
            conf = _normalize_confidence(row.get("truth_confidence"), default=0.62)
            out.append(
                {
                    "title": title,
                    "detail": _normalize_text(_payload_value(row.get("object_payload"))) or None,
                    "category": predicate.split(".")[0] if "." in predicate else "other",
                    "priority": "high" if conf >= 0.85 else ("medium" if conf >= 0.6 else "low"),
                    "thread_type": "persistent_goal" if "goal" in predicate else "situational",
                    "follow_up_after": row.get("updated_at").isoformat() if isinstance(row.get("updated_at"), datetime) else None,
                    "salience": float(conf),
                }
            )
            if len(out) >= 5:
                break
        return out

    def _canonical_people_projects_fallback(
        entities_rows: List[Dict[str, Any]],
        claims_rows: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        claims_by_subject: Dict[str, List[Dict[str, Any]]] = {}
        for row in claims_rows or []:
            subject_id = _normalize_text(row.get("subject_entity_id"))
            if not subject_id:
                continue
            claims_by_subject.setdefault(subject_id, []).append(row)
        people: List[Dict[str, Any]] = []
        projects: List[Dict[str, Any]] = []
        for entity in entities_rows or []:
            entity_type = _normalize_text(entity.get("entity_type")).lower()
            name = _normalize_text(entity.get("canonical_name"))
            entity_id = _normalize_text(entity.get("entity_id"))
            if not name or entity_type not in {"person", "project"}:
                continue
            snippets = [
                _shorten_line(_claim_sentence(row), 180)
                for row in (claims_by_subject.get(entity_id) or [])[:3]
                if _shorten_line(_claim_sentence(row), 180)
            ]
            profile_text = ". ".join(snippets[:2]) if snippets else f"{name} remains in active canonical context."
            status = snippets[0] if snippets else None
            salience = max(
                [_normalize_confidence(r.get("truth_confidence"), default=0.6) for r in (claims_by_subject.get(entity_id) or [])] or [0.6]
            )
            row_payload = {
                "canonical_name": name,
                "relationship_to_user": None,
                "profile_text": profile_text,
                "last_known_status": status,
                "key_facts": snippets[:3],
                "open_questions": [],
                "salience_score": float(salience),
                "aliases": [],
            }
            if entity_type == "person":
                people.append(row_payload)
            else:
                projects.append(row_payload)
        people = sorted(
            people,
            key=lambda row: (float(row.get("salience_score") or 0.0), _normalize_text(row.get("canonical_name")).lower()),
            reverse=True,
        )[:6]
        projects = sorted(
            projects,
            key=lambda row: (float(row.get("salience_score") or 0.0), _normalize_text(row.get("canonical_name")).lower()),
            reverse=True,
        )
        return people, projects

    living = await db.fetchone(
        """
        SELECT current_focus, recent_narrative,
               relationship_pulse, emotional_texture,
               primary_tension, what_theyre_avoiding,
               unspoken_goal, why_it_matters,
               active_contradictions, sophie_directives
        FROM living_context
        WHERE user_id = $1
        """,
        user_id,
    )
    threads = await db.fetch(
        """
        SELECT title, detail, category, priority,
               thread_type, salience_score,
               follow_up_after, related_entities
        FROM open_threads
        WHERE user_id = $1
          AND status = 'open'
        ORDER BY
          CASE thread_type
            WHEN 'persistent_goal' THEN 0
            ELSE 1
          END,
          salience_score DESC
        LIMIT 5
        """,
        user_id,
    )
    people = await db.fetch(
        """
        SELECT canonical_name, relationship_to_user,
               profile_text, last_known_status,
               key_facts, open_questions,
               salience_score
        FROM entity_profiles
        WHERE user_id = $1
          AND type = 'person'
          AND status = 'active'
          AND salience_score >= 0.4
        ORDER BY salience_score DESC
        LIMIT 6
        """,
        user_id,
    )
    projects = await db.fetch(
        """
        SELECT canonical_name, relationship_to_user,
               profile_text, last_known_status,
               salience_score, aliases
        FROM entity_profiles
        WHERE user_id = $1
          AND type = 'project'
          AND status = 'active'
        ORDER BY salience_score DESC
        """,
        user_id,
    )
    identity = await db.fetchone(
        """
        SELECT current_chapter, core_values,
               persistent_goals, recurring_fears,
               what_they_want
        FROM identity_profile
        WHERE user_id = $1
        """,
        user_id,
    )

    living = living or {}
    identity = identity or {}
    canonical_inputs = await _load_canonical_inputs(user_id)
    canonical_living = _canonical_living_fallback(canonical_inputs.get("claims") or [])
    canonical_identity = _canonical_identity_fallback(canonical_inputs.get("claims") or [])
    canonical_threads = _canonical_threads_fallback(canonical_inputs.get("claims") or [])
    canonical_people, canonical_projects = _canonical_people_projects_fallback(
        canonical_inputs.get("entities") or [],
        canonical_inputs.get("claims") or [],
    )

    living_focus = (
        _normalize_text(living.get("primary_tension"))
        or _normalize_text(living.get("current_focus"))
        or _normalize_text(canonical_living.get("primary_tension"))
        or _normalize_text(canonical_living.get("current_focus"))
    )
    episodic_recall = await _episodic_search(user_id=user_id, query=living_focus, limit=3) if living_focus else []
    derived_threads = [
        {
            "title": _normalize_text(t.get("title")),
            "detail": _normalize_text(t.get("detail")),
            "category": _normalize_text(t.get("category")),
            "priority": _normalize_text(t.get("priority")),
            "thread_type": _normalize_text(t.get("thread_type")),
            "follow_up_after": t.get("follow_up_after").isoformat() if isinstance(t.get("follow_up_after"), datetime) else _normalize_text(t.get("follow_up_after")),
            "salience": float(t.get("salience_score") or 0.0),
        }
        for t in threads
    ]
    effective_threads = derived_threads if derived_threads else canonical_threads
    effective_people = [
        {
            "name": _normalize_text(p.get("canonical_name")),
            "relationship": _normalize_text(p.get("relationship_to_user")),
            "profile": _normalize_text(p.get("profile_text")),
            "current_status": _normalize_text(p.get("last_known_status")),
            "key_facts": p.get("key_facts") if isinstance(p.get("key_facts"), list) else [],
            "open_questions": p.get("open_questions") if isinstance(p.get("open_questions"), list) else [],
            "salience": float(p.get("salience_score") or 0.0),
        }
        for p in (people or canonical_people)
    ]
    effective_projects = _cluster_projects(projects if projects else canonical_projects)
    source_provenance = {
        "projection_version": "t9a.handover_reanchor.v1",
        "canonical_claims_considered": len(canonical_inputs.get("claims") or []),
        "canonical_entities_considered": len(canonical_inputs.get("entities") or []),
        "canonical_watermarks": [
            {
                "tenant_id": _normalize_text(r.get("tenant_id")),
                "last_sequence": int(r.get("last_sequence") or 0),
                "updated_at": r.get("updated_at").isoformat() if isinstance(r.get("updated_at"), datetime) else _normalize_text(r.get("updated_at")) or None,
            }
            for r in (canonical_inputs.get("watermarks") or [])
        ],
        "derived_inputs_present": {
            "living_context": bool(living),
            "open_threads": bool(derived_threads),
            "people_profiles": bool(people),
            "project_profiles": bool(projects),
            "identity_profile": bool(identity),
        },
    }

    return {
        "generated_at": datetime.now(dt_timezone.utc).isoformat(),
        "living_context": {
            "current_focus": _normalize_text(living.get("current_focus")) or _normalize_text(canonical_living.get("current_focus")) or None,
            "primary_tension": _normalize_text(living.get("primary_tension")) or _normalize_text(canonical_living.get("primary_tension")) or None,
            "unspoken_goal": _normalize_text(living.get("unspoken_goal")) or _normalize_text(canonical_living.get("unspoken_goal")) or None,
            "emotional_texture": _normalize_text(living.get("emotional_texture")) or _normalize_text(canonical_living.get("emotional_texture")) or None,
            "relationship_pulse": _normalize_text(living.get("relationship_pulse")) or _normalize_text(canonical_living.get("relationship_pulse")) or None,
        },
        "sophie_directives": living.get("sophie_directives") if isinstance(living.get("sophie_directives"), list) else [],
        "active_contradictions": living.get("active_contradictions") if isinstance(living.get("active_contradictions"), list) else [],
        "open_threads": effective_threads,
        "people": effective_people,
        "projects": effective_projects,
        "episodic_recall": episodic_recall,
        "identity": {
            "current_chapter": _normalize_text(identity.get("current_chapter")) or _normalize_text(canonical_identity.get("current_chapter")) or None,
            "core_values": (
                (identity.get("core_values") if isinstance(identity.get("core_values"), list) else [])
                or (canonical_identity.get("core_values") if isinstance(canonical_identity.get("core_values"), list) else [])
            )[:3],
            "persistent_goals": (
                identity.get("persistent_goals") if isinstance(identity.get("persistent_goals"), list) else []
            ) or (canonical_identity.get("persistent_goals") if isinstance(canonical_identity.get("persistent_goals"), list) else []),
            "what_they_want": _normalize_text(identity.get("what_they_want")) or _normalize_text(canonical_identity.get("what_they_want")) or None,
        },
        "provenance": source_provenance,
    }


TENANT_ALIASES: Dict[str, str] = {
    "sophie-prod": "default",
}

TENANT_ALIAS_REVERSE: Dict[str, List[str]] = {}
for _alias, _canonical in TENANT_ALIASES.items():
    TENANT_ALIAS_REVERSE.setdefault(_canonical, []).append(_alias)


def _canonical_tenant_id(value: Any) -> Any:
    clean = _normalize_text(value)
    if not clean:
        return value
    canonical = TENANT_ALIASES.get(clean.lower())
    return canonical or value


def _tenant_scope_candidates(value: Any) -> List[str]:
    """
    Return canonical tenant first, then known equivalent aliases.
    This supports read-path fallback when historical rows were written
    before alias normalization was consistently applied.
    """
    clean = _normalize_text(value)
    if not clean:
        return []
    canonical = _normalize_text(_canonical_tenant_id(clean)) or clean
    candidates: List[str] = []
    for item in [canonical, clean, *(TENANT_ALIAS_REVERSE.get(canonical, []))]:
        normalized = _normalize_text(item)
        if normalized and normalized not in candidates:
            candidates.append(normalized)
    return candidates


def _normalize_tenant_aliases_in_payload(payload: Any) -> Any:
    if isinstance(payload, dict):
        normalized: Dict[str, Any] = {}
        for key, value in payload.items():
            if key in {"tenantId", "tenant_id"}:
                normalized[key] = _canonical_tenant_id(value)
            else:
                normalized[key] = _normalize_tenant_aliases_in_payload(value)
        return normalized
    if isinstance(payload, list):
        return [_normalize_tenant_aliases_in_payload(item) for item in payload]
    return payload


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
        "preferred_name": None,
        "age": None,
        "sex": None,
        "location": None,
        "current_focus": None,
        "key_relationships": [],
        "work_context": None,
        "patterns": [],
        "recent_signals": [],
        "daily_anchors": {},
        "preferences": {},
        "health": None,
        "spirituality": None,
        "narrative": None,
        "narrative_stable": None,
        "narrative_current": None,
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
    _record("daily_anchors", model.get("daily_anchors"), default_days)

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

    recent_signals = model.get("recent_signals")
    if isinstance(recent_signals, list):
        for idx, row in enumerate(recent_signals):
            _record(f"recent_signals[{idx}]", row, default_days)

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
    normalized = deepcopy(baseline)
    normalized.update(value)
    normalized["north_star"] = _normalize_north_star(normalized.get("north_star"))
    return _sanitize_user_model_hygiene(normalized)


def _hydrate_user_model_narratives(model: Dict[str, Any], row: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    hydrated = _normalize_user_model(model if isinstance(model, dict) else None)
    stable = _normalize_text(hydrated.get("narrative_stable")) or _normalize_text((row or {}).get("narrative_stable")) or None
    current = _normalize_text(hydrated.get("narrative_current")) or _normalize_text((row or {}).get("narrative_current")) or None
    hydrated["narrative_stable"] = stable
    hydrated["narrative_current"] = current
    hydrated["narrative"] = _normalize_text(" ".join([x for x in [stable, current] if x])) or _normalize_text(hydrated.get("narrative")) or None
    return hydrated


def _is_high_trust_model_source(source: Any) -> bool:
    return _normalize_text(source).lower() in {"user_stated", "manual_patch"}


def _extract_pattern_evidence_rows(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    evidence = row.get("evidence")
    if isinstance(evidence, list):
        out.extend([e for e in evidence if isinstance(e, dict)])
    elif isinstance(evidence, dict):
        out.append(evidence)
    evidences = row.get("evidences")
    if isinstance(evidences, list):
        out.extend([e for e in evidences if isinstance(e, dict)])
    cleaned: List[Dict[str, Any]] = []
    seen = set()
    for item in out:
        session_id = _normalize_text(item.get("session_id"))
        quote = _normalize_text(item.get("quote"))
        msg_index = item.get("msg_index")
        timestamp = _normalize_text(item.get("timestamp"))
        key = (session_id.lower(), quote.lower(), msg_index, timestamp.lower())
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(item)
    return cleaned


def _pattern_meets_hygiene_rules(row: Dict[str, Any], text: str) -> bool:
    if _looks_like_single_action_pattern(text):
        return False
    source = row.get("source")
    if _is_high_trust_model_source(source):
        return True
    if bool(row.get("habitual_explicit")):
        return True
    evidences = _extract_pattern_evidence_rows(row)
    sessions = {_normalize_text(e.get("session_id")).lower() for e in evidences if _normalize_text(e.get("session_id"))}
    return len(sessions) >= 2


def _sanitize_patterns_for_hygiene(patterns: Any) -> List[Dict[str, Any]]:
    if not isinstance(patterns, list):
        return []
    index: Dict[str, Dict[str, Any]] = {}
    for row in patterns:
        if not isinstance(row, dict):
            continue
        text = _normalize_text(row.get("text"))
        if not text:
            continue
        candidate = deepcopy(row)
        candidate["text"] = text[:160]
        if not _pattern_meets_hygiene_rules(candidate, text):
            continue
        key = text.lower()
        prev = index.get(key)
        if not prev:
            index[key] = candidate
            continue
        prev_conf = _extract_confidence(prev, default=0.0)
        next_conf = _extract_confidence(candidate, default=0.0)
        if _should_replace_by_source(
            existing_source=prev.get("source"),
            existing_conf=prev_conf,
            incoming_source=candidate.get("source"),
            incoming_conf=next_conf,
        ):
            index[key] = candidate
    return list(index.values())


_RELATIONSHIP_SIGNAL_TEXT_RE = re.compile(
    r"^(?P<name>[^()\n:][^()\n:]{0,79}) \((?P<who>[a-z][a-z0-9 _-]{1,39})\): (?P<status>[^\n:][^\n]{0,79})$",
    flags=re.IGNORECASE,
)
_INVALID_RELATIONSHIP_ENTITIES = {"and", "or", "but", "the", "a", "an", "of", "in", "to"}
_INVALID_RELATIONSHIP_NAMES = {
    "was", "is", "are", "be", "been", "being", "the", "a", "an", "this", "that",
    "he", "she", "they", "it", "someone", "person", "relationship",
}


def _build_relationship_signal_text(name: Any, who: Any, status: Any) -> Optional[str]:
    normalized_name = _normalize_text(name)
    normalized_who = _normalize_text(who).lower()
    normalized_status = _normalize_text(status).lower()
    if not normalized_name or not normalized_who or not normalized_status:
        return None
    if normalized_name.lower() in _INVALID_RELATIONSHIP_ENTITIES:
        return None
    if normalized_name.lower() in _INVALID_RELATIONSHIP_NAMES:
        return None
    if normalized_name != canonicalize_entity_name(normalized_name) and canonicalize_entity_name(normalized_name).lower() in _INVALID_RELATIONSHIP_NAMES:
        return None
    if not re.search(r"[A-Za-z]", normalized_name):
        return None
    if normalized_name.islower() and normalized_name.lower() not in {"ashley", "jasmine", "sophie", "bluum", "yoshi", "mukesh"}:
        return None
    text = f"{normalized_name} ({normalized_who}): {normalized_status}"
    match = _RELATIONSHIP_SIGNAL_TEXT_RE.match(text)
    if not match:
        return None
    if _normalize_text(match.group("name")).lower() in _INVALID_RELATIONSHIP_ENTITIES:
        return None
    return text


def _sanitize_relationships_for_hygiene(relationships: Any) -> List[Dict[str, Any]]:
    if not isinstance(relationships, list):
        return []
    cleaned: List[Dict[str, Any]] = []
    for row in relationships:
        if not isinstance(row, dict):
            continue
        if not _build_relationship_signal_text(row.get("name"), row.get("who"), row.get("status")):
            continue
        candidate = deepcopy(row)
        candidate["name"] = _normalize_text(row.get("name"))[:80]
        candidate["who"] = _normalize_text(row.get("who")).lower()[:40]
        candidate["status"] = _normalize_text(row.get("status")).lower()[:40]
        cleaned.append(candidate)
    return _merge_relationships([], cleaned)


def _extract_anchor_evidence_from_preferences(preferences: Dict[str, Any], note_index: int) -> Optional[Dict[str, Any]]:
    evidence_payload = preferences.get("evidence")
    if isinstance(evidence_payload, list):
        if 0 <= note_index < len(evidence_payload):
            candidate = evidence_payload[note_index]
            if isinstance(candidate, dict):
                return candidate
        for candidate in evidence_payload:
            if isinstance(candidate, dict):
                return candidate
    elif isinstance(evidence_payload, dict):
        return evidence_payload
    return None


def _migrate_preferences_notes_to_daily_anchors(model: Dict[str, Any]) -> Dict[str, Any]:
    preferences = model.get("preferences")
    if not isinstance(preferences, dict):
        return model
    notes = preferences.get("notes")
    if not isinstance(notes, list):
        return model

    now_iso = datetime.utcnow().isoformat()
    source = _normalize_text(preferences.get("source")) or "inferred"
    confidence = _normalize_confidence(preferences.get("confidence"), default=0.7)
    incoming_anchors: Dict[str, Any] = {}
    kept_notes: List[str] = []

    for idx, item in enumerate(notes):
        note = _normalize_text(item)
        if not note:
            continue
        if not _looks_like_daily_anchor_signal(note):
            kept_notes.append(note[:200])
            continue
        value = _extract_steps_anchor_value(note)
        if value is None:
            kept_notes.append(note[:200])
            continue
        key = "steps_goal" if ("goal" in note.lower() or value >= 8000) else "minimum_steps"
        evidence = _extract_anchor_evidence_from_preferences(preferences, idx)
        incoming_anchors[key] = {
            "value": int(value),
            "confidence": confidence,
            "source": source,
            "updated_at": _normalize_text(preferences.get("updated_at")) or now_iso,
            "evidence": evidence,
        }

    if incoming_anchors:
        model["daily_anchors"] = _merge_daily_anchors(model.get("daily_anchors"), incoming_anchors)

    updated_preferences = deepcopy(preferences)
    if kept_notes:
        updated_preferences["notes"] = kept_notes[:4]
    else:
        updated_preferences.pop("notes", None)
    model["preferences"] = updated_preferences
    return model


def _sanitize_user_model_hygiene(model: Dict[str, Any]) -> Dict[str, Any]:
    cleaned = deepcopy(model) if isinstance(model, dict) else _default_user_model()
    cleaned["key_relationships"] = _sanitize_relationships_for_hygiene(cleaned.get("key_relationships"))
    cleaned["patterns"] = _sanitize_patterns_for_hygiene(cleaned.get("patterns"))
    cleaned["recent_signals"] = _merge_recent_signals(
        cleaned.get("recent_signals") if isinstance(cleaned.get("recent_signals"), list) else [],
        [],
    )
    cleaned = _migrate_preferences_notes_to_daily_anchors(cleaned)
    return cleaned


_HYGIENE_DIFF_METADATA_KEYS = {
    "updated_at",
    "source",
    "confidence",
    "evidence",
    "expires_at",
    "goal_confidence",
    "vision_confidence",
    "goal_source",
    "vision_source",
    "habitual_explicit",
}


def _canonicalize_user_model_for_hygiene_diff(value: Any) -> Any:
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key in sorted(value.keys()):
            if key in _HYGIENE_DIFF_METADATA_KEYS:
                continue
            canonical = _canonicalize_user_model_for_hygiene_diff(value.get(key))
            if canonical in (None, "", [], {}):
                continue
            out[key] = canonical
        return out
    if isinstance(value, list):
        canonical_items = [_canonicalize_user_model_for_hygiene_diff(item) for item in value]
        canonical_items = [item for item in canonical_items if item not in (None, "", [], {})]
        canonical_items.sort(key=lambda x: json.dumps(x, sort_keys=True, ensure_ascii=True))
        return canonical_items
    if isinstance(value, str):
        return _normalize_text(value)
    return value


def _has_meaningful_user_model_diff(before: Dict[str, Any], after: Dict[str, Any]) -> bool:
    return _canonicalize_user_model_for_hygiene_diff(before) != _canonicalize_user_model_for_hygiene_diff(after)


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
            normalized_name = _normalize_text(name)
            if len(normalized_name) < 2:
                continue
            if not _build_relationship_signal_text(normalized_name, who, "active"):
                continue
            key = f"{name.lower()}|{who.lower()}"
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "name": normalized_name,
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
        if not _build_relationship_signal_text(rel.get("name"), rel.get("who"), rel.get("status")):
            continue
        key = f"{str(rel.get('name', '')).lower()}|{str(rel.get('who', '')).lower()}"
        if key.strip("|"):
            index[key] = rel
    for rel in incoming or []:
        if not isinstance(rel, dict):
            continue
        if not _build_relationship_signal_text(rel.get("name"), rel.get("who"), rel.get("status")):
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


def _parse_utc_ts(value: Any) -> Optional[datetime]:
    raw = _normalize_text(value)
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=dt_timezone.utc)
    return dt.astimezone(dt_timezone.utc)


def _fact_temporal_relevance(
    fact_row: Dict[str, Any],
    now_utc: datetime
) -> Tuple[bool, str, int]:
    valid_at = _parse_utc_ts(fact_row.get("valid_at"))
    invalid_at = _parse_utc_ts(fact_row.get("invalid_at"))

    # Explicitly expired facts should not be returned.
    if invalid_at is not None and invalid_at <= now_utc:
        return False, "stale", 3

    # Time-bound facts within recent window are highest priority.
    if valid_at is not None:
        age = now_utc - valid_at
        if age > timedelta(days=7) and invalid_at is None:
            return True, "stale", 2
        return True, "recent", 0

    # Untimed facts are treated as persistent memory.
    return True, "persistent", 1


_QUERY_STOPWORDS = {
    "the", "a", "an", "and", "or", "for", "to", "of", "in", "on", "at", "by",
    "with", "from", "is", "are", "was", "were", "be", "been", "being", "do",
    "did", "does", "why", "what", "when", "where", "who", "how", "i", "me",
    "my", "you", "your", "we", "our", "it", "this", "that", "these", "those",
    "remember",
}

_NARROW_ENTITY_IDENTITY_RE = re.compile(
    r"^\s*(?:who|what)\s+(?:is|are|'s)\s+(.+?)\s*\??\s*$",
    flags=re.IGNORECASE,
)


def _query_terms(query: str) -> List[str]:
    terms = re.findall(r"[A-Za-z0-9]+", _normalize_text(query).lower())
    return [term for term in terms if len(term) >= 3 and term not in _QUERY_STOPWORDS]


def _extract_narrow_entity_identity_target(query: str) -> Optional[str]:
    text = _normalize_text(query)
    if not text:
        return None
    lower = text.lower()
    if any(
        token in lower
        for token in (
            "remember",
            "lately",
            "recently",
            "other day",
            "continue",
            "thread",
        )
    ):
        return None
    match = _NARROW_ENTITY_IDENTITY_RE.match(text)
    if not match:
        return None
    target = canonicalize_entity_name(match.group(1))
    target = re.sub(r"\s+[?.!]+$", "", target).strip()
    if not target:
        return None
    if len(_query_terms(target)) > 4:
        return None
    return target


def _query_overlap_score(text: str, query_terms: List[str]) -> float:
    normalized = _normalize_text(text).lower()
    if not normalized:
        return 0.0
    if not query_terms:
        return 0.0
    hits = sum(1 for term in query_terms if term in normalized)
    if hits <= 0:
        return 0.0
    # Keep this stable and bounded for downstream ranking.
    return min(0.99, 0.35 + (hits * 0.2))


def _extract_user_model_recall_candidates(model: Dict[str, Any]) -> List[str]:
    rows: List[str] = []
    if not isinstance(model, dict):
        return rows

    def _append(value: Any) -> None:
        text = _normalize_text(value)
        if text:
            rows.append(text)

    for key in ("narrative_current", "narrative_stable", "narrative"):
        _append(model.get(key))

    current_focus = model.get("current_focus") if isinstance(model.get("current_focus"), dict) else None
    if current_focus:
        _append(current_focus.get("text"))

    for key in ("work_context", "health", "spirituality"):
        field = model.get(key) if isinstance(model.get(key), dict) else None
        if field:
            _append(field.get("text"))

    recent_signals = model.get("recent_signals")
    if isinstance(recent_signals, list):
        for row in recent_signals[:10]:
            if isinstance(row, dict):
                _append(row.get("text"))

    relationships = model.get("key_relationships")
    if isinstance(relationships, list):
        for rel in relationships[:10]:
            if not isinstance(rel, dict):
                continue
            signal = _build_relationship_signal_text(rel.get("name"), rel.get("who"), rel.get("status"))
            if signal:
                rows.append(signal)

    return _dedupe_keep_order(rows, limit=16)


def _resolve_tenant_scope(value: Any) -> Tuple[str, List[str]]:
    requested = _normalize_text(value)
    canonical = _normalize_text(_canonical_tenant_id(requested)) or requested
    candidates = _tenant_scope_candidates(canonical)
    if not candidates and canonical:
        candidates = [canonical]
    return canonical or requested, candidates


def _is_session_summary_node(node: Any) -> bool:
    if not isinstance(node, dict):
        return False
    labels = node.get("labels")
    normalized_labels = {
        _normalize_text(label).lower()
        for label in (labels or [])
        if _normalize_text(label)
    } if isinstance(labels, list) else set()
    if "sessionsummary" in normalized_labels:
        return True
    node_type = _normalize_text(node.get("type")).lower()
    return node_type == "sessionsummary"


def _extract_session_summary_candidate_texts(node: Dict[str, Any]) -> List[str]:
    attrs = node.get("attributes") if isinstance(node.get("attributes"), dict) else {}
    candidates: List[str] = []
    for raw in (
        attrs.get("summary_facts"),
        attrs.get("latest_thread_text"),
        attrs.get("summary_text"),
        attrs.get("index_text"),
        node.get("summary"),
        node.get("index_text"),
    ):
        for claim in _split_claims(_normalize_text(raw)):
            if _allow_claim(claim) and _allow_fact_text(claim) and not _is_explicit_user_state_claim(claim):
                candidates.append(claim)
    return _dedupe_keep_order(candidates, limit=8)


def _resolve_memory_intent(value: Optional[str]) -> str:
    intent = _normalize_text(value).lower()
    if intent in {"exact", "episodic", "hybrid"}:
        return intent
    return "exact"


def _resolve_memory_lane_v2(value: Optional[str]) -> str:
    lane = _normalize_text(value).lower()
    if lane in {"factual", "episodic", "continuity", "hybrid"}:
        return lane
    return "hybrid"


def _extract_session_id_from_episode_name(name: str) -> Optional[str]:
    clean = _normalize_text(name)
    if not clean:
        return None
    match = re.match(r"^session_raw_(.+?)(?:_\d{9,})?$", clean)
    if match:
        value = _normalize_text(match.group(1))
        return value or None
    return None


def _normalize_episode_reference_time(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        dt = value
    else:
        raw = _normalize_text(value)
        if not raw:
            return None
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=dt_timezone.utc)
    return dt.astimezone(dt_timezone.utc).isoformat().replace("+00:00", "Z")


def _episode_text_overlap_score(text: str, query_terms: List[str]) -> float:
    clean = _normalize_text(text).lower()
    if not clean or not query_terms:
        return 0.0
    terms = []
    seen = set()
    for term in query_terms:
        t = _normalize_text(term).lower()
        if t and t not in seen:
            terms.append(t)
            seen.add(t)
    if not terms:
        return 0.0
    hits = sum(1 for term in terms if term in clean)
    if hits <= 0:
        return 0.0
    return min(1.0, hits / float(len(terms)))


def _extract_episode_evidence_lines(
    text: Optional[str],
    query_terms: List[str],
    *,
    limit: int = 2,
) -> List[str]:
    raw = text if isinstance(text, str) else ""
    lines = [_normalize_text(line) for line in raw.splitlines() if _normalize_text(line)]
    if not lines:
        return []

    selected: List[str] = []
    lowered_terms = [_normalize_text(term).lower() for term in query_terms if _normalize_text(term)]
    for line in lines:
        lower = line.lower()
        if lowered_terms and not any(term in lower for term in lowered_terms):
            continue
        if line not in selected:
            selected.append(_shorten_line(line, 180))
        if len(selected) >= limit:
            return selected

    if selected:
        return selected[:limit]

    # Fallback: keep one user line and one assistant line for grounding.
    for prefix in ("User:", "Assistant:"):
        for line in lines:
            if line.startswith(prefix):
                selected.append(_shorten_line(line, 180))
                break
        if len(selected) >= limit:
            break
    return selected[:limit]


def _extract_transcript_evidence_lines(
    messages: List[Dict[str, Any]],
    query_terms: List[str],
    *,
    limit: int = 2,
) -> List[str]:
    if not isinstance(messages, list) or not messages:
        return []
    lines: List[str] = []
    for row in messages:
        if not isinstance(row, dict):
            continue
        role = _normalize_text(row.get("role")).lower()
        text = _normalize_text(row.get("text"))
        if role not in {"user", "assistant"} or not text:
            continue
        lines.append(f"{'User' if role == 'user' else 'Assistant'}: {text}")
    return _extract_episode_evidence_lines("\n".join(lines), query_terms, limit=limit)


def _entity_profile_to_node_type(value: Any) -> str:
    raw = _normalize_text(value).lower()
    if raw in {"person", "project", "goal", "loop", "preference", "event"}:
        return raw
    if raw == "place":
        return "other"
    return "other"


def _allowed_node_type(node_type: str, allowed_types: Optional[List[str]]) -> bool:
    if not allowed_types:
        return True
    allowed = {_normalize_text(x).lower() for x in (allowed_types or []) if _normalize_text(x)}
    if not allowed:
        return True
    return _normalize_text(node_type).lower() in allowed


def _score_text_match(
    *,
    text: str,
    query_terms: List[str],
    reference_ts: Optional[datetime] = None,
    now_utc: Optional[datetime] = None,
    base_score: float = 0.0,
) -> float:
    overlap = _query_overlap_score(text, query_terms)
    score = float(base_score) + min(0.8, overlap)
    if isinstance(reference_ts, datetime):
        now = now_utc or datetime.utcnow().replace(tzinfo=dt_timezone.utc)
        ts = reference_ts if reference_ts.tzinfo else reference_ts.replace(tzinfo=dt_timezone.utc)
        age_days = max(0.0, (now - ts.astimezone(dt_timezone.utc)).total_seconds() / 86400.0)
        recency = 1.0 / (1.0 + (age_days / 21.0))
        score += 0.2 * recency
    return max(0.0, min(1.5, score))


def _format_session_transcript_content(messages: Any, *, max_lines: int = 24, max_chars: int = 2400) -> str:
    if isinstance(messages, str):
        try:
            messages = json.loads(messages)
        except Exception:
            messages = []
    if not isinstance(messages, list):
        return ""
    lines: List[str] = []
    for row in messages:
        if not isinstance(row, dict):
            continue
        role = _normalize_text(row.get("role")).lower()
        if role not in {"user", "assistant"}:
            continue
        text = _normalize_text(row.get("text"))
        if not text:
            continue
        prefix = "User" if role == "user" else "Assistant"
        lines.append(f"{prefix}: {text}")
        if len(lines) >= max_lines:
            break
    content = "\n".join(lines).strip()
    return _shorten_line(content, max_chars) if content else ""


def _retrieval_row_metadata(
    *,
    source_type: str,
    derived: bool,
    evidence_backed: bool,
    data_classification: str,
) -> Dict[str, Any]:
    return {
        "source_type": _normalize_text(source_type),
        "derived": bool(derived),
        "evidence_backed": bool(evidence_backed),
        "data_classification": _normalize_text(data_classification),
    }


def _is_canonical_factual_row(row: Any) -> bool:
    if not isinstance(row, dict):
        return False
    return (
        _normalize_text(row.get("source_type")).lower() == "canonical factual"
        and bool(row.get("evidence_backed"))
        and not bool(row.get("derived"))
    )


def _payload_text(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("value", "text", "status", "summary", "title", "name"):
            candidate = _normalize_text(value.get(key))
            if candidate:
                return candidate
        compact = _normalize_text(json.dumps(value, sort_keys=True, ensure_ascii=True))
        return compact
    if isinstance(value, list):
        items = [_normalize_text(x) for x in value if _normalize_text(x)]
        return ", ".join(items[:4])
    return _normalize_text(value)


def _canonical_claim_line(row: Dict[str, Any]) -> str:
    subject = _normalize_text(row.get("subject_name")) or _normalize_text(row.get("subject_text")) or "The user"
    predicate = _normalize_text(row.get("predicate")).lower()
    tail = predicate.replace("_", ".").split(".")[-1] if predicate else "update"
    tail = tail.replace("_", " ").strip() or "update"
    value = _payload_text(row.get("object_payload"))
    if value:
        return f"{subject} {tail}: {value}"
    return f"{subject} {tail}."


def _canonical_claim_terms_match(predicate: Any, terms: List[str]) -> bool:
    clean = _normalize_text(predicate).lower()
    return bool(clean) and any(term in clean for term in terms)


async def _fetch_canonical_signal_rows(
    *,
    tenant_id: str,
    user_id: str,
    claim_limit: int = 160,
    entity_limit: int = 120,
) -> Dict[str, Any]:
    tenant_scope = _tenant_scope_candidates(tenant_id) or [_normalize_text(tenant_id)]
    claim_rows = await db.fetch(
        """
        SELECT
          c.tenant_id,
          c.claim_event_key,
          c.predicate,
          c.subject_entity_id::text AS subject_entity_id,
          c.subject_text,
          c.object_payload,
          c.truth_confidence,
          c.updated_at,
          c.created_at,
          e.canonical_name AS subject_name,
          e.entity_type,
          COUNT(ce.claim_evidence_id)::int AS evidence_count
        FROM claims c
        LEFT JOIN entities e
          ON e.tenant_id = c.tenant_id
         AND e.entity_id = c.subject_entity_id
        LEFT JOIN claim_evidence ce
          ON ce.tenant_id = c.tenant_id
         AND ce.claim_id = c.claim_id
        WHERE c.tenant_id = ANY($1::text[])
          AND c.user_id = $2
          AND c.lifecycle_status = 'active'
        GROUP BY
          c.tenant_id,
          c.claim_event_key,
          c.predicate,
          c.subject_entity_id,
          c.subject_text,
          c.object_payload,
          c.truth_confidence,
          c.updated_at,
          c.created_at,
          e.canonical_name,
          e.entity_type
        ORDER BY c.updated_at DESC NULLS LAST, c.claim_event_key ASC
        LIMIT $3
        """,
        tenant_scope,
        user_id,
        max(1, min(int(claim_limit or 160), 500)),
    )
    entity_rows = await db.fetch(
        """
        SELECT
          tenant_id,
          entity_id::text AS entity_id,
          canonical_name,
          canonical_name_normalized,
          entity_type,
          status,
          created_at,
          updated_at
        FROM entities
        WHERE tenant_id = ANY($1::text[])
          AND user_id = $2
          AND status = 'active'
        ORDER BY updated_at DESC NULLS LAST, canonical_name_normalized ASC
        LIMIT $3
        """,
        tenant_scope,
        user_id,
        max(1, min(int(entity_limit or 120), 300)),
    )
    watermark_rows = await db.fetch(
        """
        SELECT tenant_id, last_sequence, updated_at
        FROM canonical_tenant_watermarks
        WHERE tenant_id = ANY($1::text[])
        ORDER BY tenant_id ASC
        """,
        tenant_scope,
    )
    return {
        "tenant_scope": tenant_scope,
        "claims": claim_rows or [],
        "entities": entity_rows or [],
        "watermarks": watermark_rows or [],
    }


def _canonical_provenance_payload(
    *,
    projection_version: str,
    signal_rows: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "projection_version": projection_version,
        "canonical_claims_considered": len(signal_rows.get("claims") or []),
        "canonical_entities_considered": len(signal_rows.get("entities") or []),
        "canonical_watermarks": [
            {
                "tenant_id": _normalize_text(r.get("tenant_id")),
                "last_sequence": int(r.get("last_sequence") or 0),
                "updated_at": r.get("updated_at").isoformat() if isinstance(r.get("updated_at"), datetime) else _normalize_text(r.get("updated_at")) or None,
            }
            for r in (signal_rows.get("watermarks") or [])
        ],
    }


async def _pg_get_entity_role_grounding(
    *,
    tenant_id: str,
    user_id: str,
    name: Optional[str],
    entity_id: Optional[str],
) -> Dict[str, Any]:
    # GRAPHITI_REPLACED: get_entity_role_grounding (entity relationship role lookup)
    # TEMPORARY_DEGRADED_REPLACEMENT: entity role grounding is inferred from profile aggregates, not claim-store evidence.
    return await _pg_get_entity_role_hint(
        tenant_id=tenant_id,
        user_id=user_id,
        name=name,
        entity_id=entity_id,
    )


async def _pg_get_entity_role_hint(
    *,
    tenant_id: str,
    user_id: str,
    name: Optional[str],
    entity_id: Optional[str],
) -> Dict[str, Any]:
    tenant_scope = _tenant_scope_candidates(tenant_id) or [_normalize_text(tenant_id)]
    normalized_name = canonicalize_entity_name(name)
    normalized_id = _normalize_text(entity_id)
    row = None
    if normalized_id:
        row = await db.fetchone(
            """
            SELECT canonical_name, type, relationship_to_user, confidence
            FROM entity_profiles
            WHERE user_id = $1
              AND entity_id = $2
            LIMIT 1
            """,
            user_id,
            normalized_id,
        )
    if row is None and normalized_name:
        row = await db.fetchone(
            """
            SELECT canonical_name, type, relationship_to_user, confidence
            FROM entity_profiles
            WHERE user_id = $1
              AND lower(canonical_name) = lower($2)
            ORDER BY mention_count DESC NULLS LAST
            LIMIT 1
            """,
            user_id,
            normalized_name,
        )
    if not isinstance(row, dict):
        canonical_row = None
        canonical_id = None
        if normalized_id:
            try:
                canonical_id = str(uuid.UUID(normalized_id))
            except Exception:
                canonical_id = None
        if canonical_id:
            canonical_row = await db.fetchone(
                """
                SELECT entity_id::text AS entity_id, canonical_name, entity_type
                FROM entities
                WHERE tenant_id = ANY($1::text[])
                  AND user_id = $2
                  AND status = 'active'
                  AND entity_id = $3::uuid
                LIMIT 1
                """,
                tenant_scope,
                user_id,
                canonical_id,
            )
        if canonical_row is None and normalized_name:
            canonical_row = await db.fetchone(
                """
                SELECT entity_id::text AS entity_id, canonical_name, entity_type
                FROM entities
                WHERE tenant_id = ANY($1::text[])
                  AND user_id = $2
                  AND status = 'active'
                  AND lower(canonical_name) = lower($3)
                ORDER BY updated_at DESC NULLS LAST
                LIMIT 1
                """,
                tenant_scope,
                user_id,
                normalized_name,
            )
        if not isinstance(canonical_row, dict):
            return {}
        claim_row = await db.fetchone(
            """
            SELECT predicate, object_payload, truth_confidence
            FROM claims
            WHERE tenant_id = ANY($1::text[])
              AND user_id = $2
              AND lifecycle_status = 'active'
              AND subject_entity_id = $3::uuid
            ORDER BY truth_confidence DESC NULLS LAST, updated_at DESC NULLS LAST
            LIMIT 1
            """,
            tenant_scope,
            user_id,
            canonical_row.get("entity_id"),
        )
        role = _payload_text(claim_row.get("object_payload")) if isinstance(claim_row, dict) else None
        entity_type = _normalize_text(canonical_row.get("entity_type")).lower()
        edge_name = "WORKING_ON" if entity_type == "project" else "RELATED_TO_USER_AS"
        return {
            "entity_name": _normalize_text(canonical_row.get("canonical_name")) or normalized_name,
            "role": _normalize_text(role) or None,
            "relationship": _normalize_text(role) or None,
            "edge_name": edge_name,
            "confidence": _normalize_confidence((claim_row or {}).get("truth_confidence"), default=0.58),
            "source": "canonical_entities_claims",
            **_retrieval_row_metadata(
                source_type="canonical signal",
                derived=False,
                evidence_backed=True,
                data_classification="canonical signal",
            ),
        }
    role = _normalize_text(row.get("relationship_to_user")) or None
    entity_type = _normalize_text(row.get("type")).lower()
    edge_name = "WORKING_ON" if entity_type == "project" else "RELATED_TO_USER_AS"
    return {
        "entity_name": _normalize_text(row.get("canonical_name")) or normalized_name,
        "role": role,
        "relationship": role,
        "edge_name": edge_name,
        "confidence": _normalize_confidence(row.get("confidence"), default=0.65),
        "source": "entity_profiles",
        **_retrieval_row_metadata(
            source_type="derived continuity/projection",
            derived=True,
            evidence_backed=False,
            data_classification="derived continuity/projection",
        ),
    }


async def _pg_get_entity_facts_exact(
    *,
    tenant_id: str,
    user_id: str,
    name: Optional[str],
    entity_id: Optional[str],
    limit: int,
    include_derived: bool = False,
) -> List[Dict[str, Any]]:
    # GRAPHITI_REPLACED: get_entity_facts_exact (exact facts tied to one entity)
    # T1 containment: fail-closed for factual retrieval until v2 claim-store facts exist.
    # Continuity rows remain available only for explicit derived/debug flows.
    if not include_derived:
        return []
    return await _pg_get_entity_continuity_facts(
        tenant_id=tenant_id,
        user_id=user_id,
        name=name,
        entity_id=entity_id,
        limit=limit,
    )


async def _pg_get_entity_continuity_facts(
    *,
    tenant_id: str,
    user_id: str,
    name: Optional[str],
    entity_id: Optional[str],
    limit: int,
) -> List[Dict[str, Any]]:
    tenant_scope = _tenant_scope_candidates(tenant_id) or [_normalize_text(tenant_id)]
    safe_limit = max(1, min(int(limit or 8), 40))
    normalized_name = canonicalize_entity_name(name)
    normalized_id = _normalize_text(entity_id)
    entity_row = None
    if normalized_id:
        entity_row = await db.fetchone(
            """
            SELECT entity_id, canonical_name, type, relationship_to_user, key_facts,
                   last_known_status, confidence, last_seen_at
            FROM entity_profiles
            WHERE user_id = $1
              AND entity_id = $2
            LIMIT 1
            """,
            user_id,
            normalized_id,
        )
    if entity_row is None and normalized_name:
        entity_row = await db.fetchone(
            """
            SELECT entity_id, canonical_name, type, relationship_to_user, key_facts,
                   last_known_status, confidence, last_seen_at
            FROM entity_profiles
            WHERE user_id = $1
              AND lower(canonical_name) = lower($2)
            ORDER BY mention_count DESC NULLS LAST
            LIMIT 1
            """,
            user_id,
            normalized_name,
        )
    canonical_entity_row = None
    canonical_entity_id = None
    if normalized_id:
        try:
            canonical_entity_id = str(uuid.UUID(normalized_id))
        except Exception:
            canonical_entity_id = None
    if canonical_entity_id:
        canonical_entity_row = await db.fetchone(
            """
            SELECT entity_id::text AS entity_id, canonical_name, entity_type, updated_at
            FROM entities
            WHERE tenant_id = ANY($1::text[])
              AND user_id = $2
              AND status = 'active'
              AND entity_id = $3::uuid
            LIMIT 1
            """,
            tenant_scope,
            user_id,
            canonical_entity_id,
        )
    if canonical_entity_row is None and normalized_name:
        canonical_entity_row = await db.fetchone(
            """
            SELECT entity_id::text AS entity_id, canonical_name, entity_type, updated_at
            FROM entities
            WHERE tenant_id = ANY($1::text[])
              AND user_id = $2
              AND status = 'active'
              AND lower(canonical_name) = lower($3)
            ORDER BY updated_at DESC NULLS LAST
            LIMIT 1
            """,
            tenant_scope,
            user_id,
            normalized_name,
        )
    if not isinstance(entity_row, dict) and not isinstance(canonical_entity_row, dict):
        return []
    canonical_name = canonicalize_entity_name((entity_row or {}).get("canonical_name")) or canonicalize_entity_name((canonical_entity_row or {}).get("canonical_name")) or normalized_name
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()

    def _append_fact(
        text: str,
        relevance: float,
        source: str,
        valid_at: Any = None,
        *,
        source_type: str = "derived continuity/projection",
        derived: bool = True,
        evidence_backed: bool = False,
        data_classification: str = "derived continuity/projection",
    ) -> None:
        claim = _normalize_text(text)
        if not claim:
            return
        key = claim.lower()
        if key in seen:
            return
        seen.add(key)
        out.append(
            {
                "text": claim,
                "relevance": max(0.0, min(1.0, float(relevance))),
                "source": source,
                "valid_at": valid_at,
                "invalid_at": None,
                **_retrieval_row_metadata(
                    source_type=source_type,
                    derived=derived,
                    evidence_backed=evidence_backed,
                    data_classification=data_classification,
                ),
            }
        )

    relationship = _normalize_text(entity_row.get("relationship_to_user"))
    if relationship:
        _append_fact(
            f"{canonical_name} is the user's {relationship}.",
            _normalize_confidence(entity_row.get("confidence"), default=0.85),
            "postgres_entity_profile",
            entity_row.get("last_seen_at"),
        )
    status = _normalize_text(entity_row.get("last_known_status"))
    if status:
        _append_fact(
            f"{canonical_name}: {status}",
            _normalize_confidence(entity_row.get("confidence"), default=0.7),
            "postgres_entity_profile",
            entity_row.get("last_seen_at"),
        )

    key_facts = entity_row.get("key_facts")
    if isinstance(key_facts, list):
        for fact in key_facts:
            if isinstance(fact, dict):
                text = _normalize_text(fact.get("fact") or fact.get("text"))
                conf = _normalize_confidence(fact.get("confidence"), default=0.72)
                ts = fact.get("last_confirmed") or fact.get("last_seen_at")
            else:
                text = _normalize_text(fact)
                conf = 0.68
                ts = entity_row.get("last_seen_at")
            if text:
                _append_fact(text, conf, "postgres_entity_profile_fact", ts)
            if len(out) >= safe_limit:
                return out[:safe_limit]

    thread_rows = await db.fetch(
        """
        SELECT title, detail, category, priority, status, last_mentioned_at
        FROM open_threads
        WHERE user_id = $1
          AND $2 = ANY(COALESCE(related_entities, ARRAY[]::text[]))
        ORDER BY last_mentioned_at DESC NULLS LAST, created_at DESC
        LIMIT 12
        """,
        user_id,
        canonical_name,
    )
    for row in thread_rows or []:
        title = _normalize_text(row.get("title"))
        detail = _normalize_text(row.get("detail"))
        category = _normalize_text(row.get("category")).lower()
        status = _normalize_text(row.get("status")).lower()
        label = f"[{category}] " if category else ""
        text = f"{label}{title}" if title else detail
        if detail and detail.lower() not in text.lower():
            text = f"{text}. {detail}"
        if status:
            text = f"{text} (thread: {status})"
        _append_fact(text, 0.62, "postgres_open_threads", row.get("last_mentioned_at"))
        if len(out) >= safe_limit:
            return out[:safe_limit]

    session_rows = await db.fetch(
        """
        SELECT session_date, raw_triage_output->'memory_deltas' AS deltas
        FROM session_classifications
        WHERE user_id = $1
          AND EXISTS (
            SELECT 1
            FROM unnest(COALESCE(entity_mentions, ARRAY[]::text[])) AS m
            WHERE lower(m) = lower($2)
          )
        ORDER BY session_date DESC
        LIMIT 16
        """,
        user_id,
        canonical_name,
    )
    for row in session_rows or []:
        deltas = row.get("deltas")
        if not isinstance(deltas, list):
            continue
        for delta in deltas:
            text = _normalize_text(delta)
            if not text:
                continue
            if canonical_name.lower() not in text.lower():
                continue
            _append_fact(
                text,
                0.58,
                "postgres_session_classifications",
                row.get("session_date"),
                data_classification="mixed/unsafe",
            )
            if len(out) >= safe_limit:
                return out[:safe_limit]
    canonical_subject_id = _normalize_text((canonical_entity_row or {}).get("entity_id"))
    if canonical_subject_id:
        canonical_claim_rows = await db.fetch(
            """
            SELECT
              c.predicate,
              c.object_payload,
              c.truth_confidence,
              c.updated_at,
              c.subject_text,
              COUNT(ce.claim_evidence_id)::int AS evidence_count
            FROM claims c
            LEFT JOIN claim_evidence ce
              ON ce.tenant_id = c.tenant_id
             AND ce.claim_id = c.claim_id
            WHERE c.tenant_id = ANY($1::text[])
              AND c.user_id = $2
              AND c.lifecycle_status = 'active'
              AND c.subject_entity_id = $3::uuid
            GROUP BY c.predicate, c.object_payload, c.truth_confidence, c.updated_at, c.subject_text
            ORDER BY c.updated_at DESC NULLS LAST
            LIMIT 40
            """,
            tenant_scope,
            user_id,
            canonical_subject_id,
        )
        for row in canonical_claim_rows or []:
            if int(row.get("evidence_count") or 0) <= 0:
                continue
            _append_fact(
                _canonical_claim_line(
                    {
                        "predicate": row.get("predicate"),
                        "subject_name": canonical_name,
                        "subject_text": row.get("subject_text"),
                        "object_payload": row.get("object_payload"),
                    }
                ),
                _normalize_confidence(row.get("truth_confidence"), default=0.72),
                "canonical_claims",
                row.get("updated_at"),
                source_type="canonical factual",
                derived=False,
                evidence_backed=True,
                data_classification="canonical factual",
            )
            if len(out) >= safe_limit:
                break

    out.sort(
        key=lambda row: (
            float(row.get("relevance") or 0.0),
            _normalize_text(row.get("valid_at")),
        ),
        reverse=True,
    )
    return out[:safe_limit]


async def _pg_search_nodes(
    *,
    tenant_id: str,
    user_id: str,
    query: str,
    limit: int,
    reference_time: Optional[datetime] = None,
    allowed_types: Optional[List[str]] = None,
    search_filter: Any = None,
) -> List[Dict[str, Any]]:
    # GRAPHITI_REPLACED: search_nodes (semantic node retrieval)
    # TEMPORARY_DEGRADED_REPLACEMENT: node retrieval is synthesized from continuity tables, not canonical claim-store graph nodes.
    del search_filter  # Postgres degraded-continuity filter is currently unused.
    safe_limit = max(1, min(int(limit or 10), 50))
    query_terms = _query_terms(query)
    now_utc = (
        reference_time.astimezone(dt_timezone.utc)
        if isinstance(reference_time, datetime) and reference_time.tzinfo
        else datetime.utcnow().replace(tzinfo=dt_timezone.utc)
    )
    tenant_scope = _tenant_scope_candidates(tenant_id) or [_normalize_text(tenant_id)]
    candidates: List[Dict[str, Any]] = []

    entity_rows = await db.fetch(
        """
        SELECT entity_id, canonical_name, type, relationship_to_user, profile_text,
               last_known_status, confidence, mention_count, first_seen_at, last_seen_at,
               created_at, last_updated_at, aliases, status
        FROM entity_profiles
        WHERE user_id = $1
          AND status IN ('active', 'tentative')
        ORDER BY mention_count DESC NULLS LAST, last_seen_at DESC NULLS LAST, created_at DESC
        LIMIT 200
        """,
        user_id,
    )
    for row in entity_rows or []:
        node_type = _entity_profile_to_node_type(row.get("type"))
        if not _allowed_node_type(node_type, allowed_types):
            continue
        name = canonicalize_entity_name(row.get("canonical_name"))
        if not name:
            continue
        profile_text = _normalize_text(row.get("profile_text"))
        status_text = _normalize_text(row.get("last_known_status"))
        summary = profile_text or status_text or name
        search_blob = " ".join([name, profile_text, status_text])
        score = _score_text_match(
            text=search_blob,
            query_terms=query_terms,
            reference_ts=row.get("last_seen_at") or row.get("last_updated_at") or row.get("created_at"),
            now_utc=now_utc,
            base_score=min(0.25, float((row.get("mention_count") or 0)) * 0.02),
        )
        if query_terms and score < 0.08:
            continue
        attrs = {
            "relationship_to_user": _normalize_text(row.get("relationship_to_user")) or None,
            "last_known_status": status_text or None,
            "mention_count": int(row.get("mention_count") or 0),
            "confidence": _normalize_confidence(row.get("confidence"), default=0.6),
            "aliases": row.get("aliases") if isinstance(row.get("aliases"), list) else [],
            "source": "entity_profiles",
            **_retrieval_row_metadata(
                source_type="derived continuity/projection",
                derived=True,
                evidence_backed=False,
                data_classification="derived continuity/projection",
            ),
        }
        candidates.append(
            {
                "uuid": _normalize_text(row.get("entity_id")) or None,
                "name": name,
                "summary": summary,
                "canonical_name": name,
                "type": node_type,
                "group_id": f"pg:{user_id}",
                "created_at": row.get("created_at"),
                "updated_at": row.get("last_updated_at") or row.get("last_seen_at"),
                "attributes": attrs,
                "labels": ["Entity", node_type.title()],
                "discipline_score": round(score, 4),
                "_score": score,
                **_retrieval_row_metadata(
                    source_type="derived continuity/projection",
                    derived=True,
                    evidence_backed=False,
                    data_classification="derived continuity/projection",
                ),
            }
        )

    thread_rows = await db.fetch(
        """
        SELECT thread_id, title, detail, category, priority, status, thread_type,
               salience_score, importance_score, times_mentioned, follow_up_after,
               last_mentioned_at, created_at, last_updated_at, related_entities
        FROM open_threads
        WHERE user_id = $1
          AND status IN ('open', 'snoozed')
        ORDER BY salience_score DESC NULLS LAST, last_mentioned_at DESC NULLS LAST
        LIMIT 200
        """,
        user_id,
    )
    for row in thread_rows or []:
        thread_type = _normalize_text(row.get("thread_type")).lower()
        node_type = "goal" if thread_type == "persistent_goal" else "loop"
        if not _allowed_node_type(node_type, allowed_types):
            continue
        title = _normalize_text(row.get("title"))
        detail = _normalize_text(row.get("detail"))
        summary = title or detail
        if not summary:
            continue
        search_blob = " ".join([summary, detail, _normalize_text(row.get("category")), _normalize_text(row.get("priority"))])
        score = _score_text_match(
            text=search_blob,
            query_terms=query_terms,
            reference_ts=row.get("last_mentioned_at") or row.get("last_updated_at") or row.get("created_at"),
            now_utc=now_utc,
            base_score=min(0.3, float(row.get("salience_score") or 0.0) * 0.25),
        )
        if query_terms and score < 0.08:
            continue
        attrs = {
            "status": _normalize_text(row.get("status")) or None,
            "category": _normalize_text(row.get("category")) or None,
            "priority": _normalize_text(row.get("priority")) or None,
            "thread_type": thread_type or None,
            "times_mentioned": int(row.get("times_mentioned") or 1),
            "follow_up_after": row.get("follow_up_after").isoformat() if isinstance(row.get("follow_up_after"), datetime) else _normalize_text(row.get("follow_up_after")) or None,
            "related_entities": row.get("related_entities") if isinstance(row.get("related_entities"), list) else [],
            "source": "open_threads",
            "salience": float(row.get("salience_score") or 0.0),
            "importance": float(row.get("importance_score") or 0.0),
            **_retrieval_row_metadata(
                source_type="derived continuity/projection",
                derived=True,
                evidence_backed=False,
                data_classification="derived continuity/projection",
            ),
        }
        candidates.append(
            {
                "uuid": _normalize_text(row.get("thread_id")) or None,
                "name": summary[:120],
                "summary": summary,
                "canonical_name": summary[:120],
                "type": node_type,
                "group_id": f"pg:{user_id}",
                "created_at": row.get("created_at"),
                "updated_at": row.get("last_updated_at") or row.get("last_mentioned_at"),
                "attributes": attrs,
                "labels": ["Entity", node_type.title()],
                "discipline_score": round(score, 4),
                "_score": score,
                **_retrieval_row_metadata(
                    source_type="derived continuity/projection",
                    derived=True,
                    evidence_backed=False,
                    data_classification="derived continuity/projection",
                ),
            }
        )

    session_rows = await db.fetch(
        """
        SELECT session_id, session_date, one_line_summary, session_kind, emotional_weight,
               tension_signal, raw_triage_output->'memory_deltas' AS deltas,
               raw_triage_output->'thread_signals' AS thread_signals,
               entity_mentions, processed_at
        FROM session_classifications
        WHERE user_id = $1
          AND is_memory_worthy = true
        ORDER BY session_date DESC NULLS LAST
        LIMIT 220
        """,
        user_id,
    )
    for row in session_rows or []:
        node_type = "event"
        if not _allowed_node_type(node_type, allowed_types):
            continue
        deltas = row.get("deltas")
        if not isinstance(deltas, list):
            deltas = []
        summary = _normalize_text(row.get("one_line_summary")) or _normalize_text(deltas[0] if deltas else None) or _normalize_text(row.get("tension_signal"))
        if not summary:
            continue
        search_blob = " ".join(
            [
                summary,
                " ".join([_normalize_text(x) for x in deltas[:4] if _normalize_text(x)]),
                _normalize_text(row.get("session_kind")),
                _normalize_text(row.get("emotional_weight")),
                _normalize_text(row.get("tension_signal")),
            ]
        )
        score = _score_text_match(
            text=search_blob,
            query_terms=query_terms,
            reference_ts=row.get("session_date") or row.get("processed_at"),
            now_utc=now_utc,
            base_score=0.15,
        )
        if query_terms and score < 0.07:
            continue
        attrs = {
            "session_id": _normalize_text(row.get("session_id")) or None,
            "session_kind": _normalize_text(row.get("session_kind")) or None,
            "emotional_weight": _normalize_text(row.get("emotional_weight")) or None,
            "memory_deltas": deltas,
            "thread_signals": row.get("thread_signals") if isinstance(row.get("thread_signals"), list) else [],
            "entity_mentions": row.get("entity_mentions") if isinstance(row.get("entity_mentions"), list) else [],
            "tension_signal": _normalize_text(row.get("tension_signal")) or None,
            "source": "session_classifications",
            **_retrieval_row_metadata(
                source_type="derived continuity/projection",
                derived=True,
                evidence_backed=False,
                data_classification="mixed/unsafe",
            ),
        }
        session_id = _normalize_text(row.get("session_id"))
        candidates.append(
            {
                "uuid": f"session_classification:{session_id}" if session_id else None,
                "name": summary[:120],
                "summary": summary,
                "canonical_name": summary[:120],
                "type": node_type,
                "group_id": f"pg:{user_id}",
                "created_at": row.get("session_date"),
                "updated_at": row.get("processed_at") or row.get("session_date"),
                "attributes": attrs,
                "labels": ["Entity", node_type.title()],
                "discipline_score": round(score, 4),
                "_score": score,
                **_retrieval_row_metadata(
                    source_type="derived continuity/projection",
                    derived=True,
                    evidence_backed=False,
                    data_classification="mixed/unsafe",
                ),
            }
        )

    seen_entity_names = {
        canonicalize_entity_name(row.get("canonical_name") or row.get("name") or "").lower()
        for row in candidates
        if canonicalize_entity_name(row.get("canonical_name") or row.get("name") or "")
    }
    canonical_entity_rows = await db.fetch(
        """
        SELECT tenant_id, entity_id::text AS entity_id, canonical_name, entity_type, created_at, updated_at
        FROM entities
        WHERE tenant_id = ANY($1::text[])
          AND user_id = $2
          AND status = 'active'
        ORDER BY updated_at DESC NULLS LAST, canonical_name_normalized ASC
        LIMIT 220
        """,
        tenant_scope,
        user_id,
    )
    for row in canonical_entity_rows or []:
        raw_type = _normalize_text(row.get("entity_type"))
        node_type = infer_ontology_type(raw_type)
        if not _allowed_node_type(node_type, allowed_types):
            continue
        name = canonicalize_entity_name(row.get("canonical_name"))
        if not name:
            continue
        key = name.lower()
        if key in seen_entity_names:
            continue
        score = _score_text_match(
            text=name,
            query_terms=query_terms,
            reference_ts=row.get("updated_at") or row.get("created_at"),
            now_utc=now_utc,
            base_score=0.11,
        )
        if query_terms and score < 0.08:
            continue
        candidates.append(
            {
                "uuid": _normalize_text(row.get("entity_id")) or None,
                "name": name,
                "summary": f"{name} is active in canonical memory.",
                "canonical_name": name,
                "type": node_type,
                "group_id": f"pg:{user_id}",
                "created_at": row.get("created_at"),
                "updated_at": row.get("updated_at"),
                "attributes": {
                    "source": "canonical_entities",
                    "entity_type": raw_type or None,
                    **_retrieval_row_metadata(
                        source_type="canonical signal",
                        derived=False,
                        evidence_backed=True,
                        data_classification="canonical signal",
                    ),
                },
                "labels": ["Entity", node_type.title()],
                "discipline_score": round(score, 4),
                "_score": score,
                **_retrieval_row_metadata(
                    source_type="canonical signal",
                    derived=False,
                    evidence_backed=True,
                    data_classification="canonical signal",
                ),
            }
        )

    canonical_claim_rows = await db.fetch(
        """
        SELECT
          c.claim_event_key,
          c.predicate,
          c.object_payload,
          c.truth_confidence,
          c.updated_at,
          c.subject_text,
          e.canonical_name AS subject_name,
          COUNT(ce.claim_evidence_id)::int AS evidence_count
        FROM claims c
        LEFT JOIN entities e
          ON e.tenant_id = c.tenant_id
         AND e.entity_id = c.subject_entity_id
        LEFT JOIN claim_evidence ce
          ON ce.tenant_id = c.tenant_id
         AND ce.claim_id = c.claim_id
        WHERE c.tenant_id = ANY($1::text[])
          AND c.user_id = $2
          AND c.lifecycle_status = 'active'
        GROUP BY
          c.claim_event_key, c.predicate, c.object_payload, c.truth_confidence, c.updated_at, c.subject_text, e.canonical_name
        ORDER BY c.updated_at DESC NULLS LAST, c.claim_event_key ASC
        LIMIT 260
        """,
        tenant_scope,
        user_id,
    )
    seen_claim_events = {
        _normalize_text(row.get("uuid")).lower()
        for row in candidates
        if _normalize_text(row.get("uuid")).lower().startswith("canonical_claim:")
    }
    for row in canonical_claim_rows or []:
        if int(row.get("evidence_count") or 0) <= 0:
            continue
        event_key = _normalize_text(row.get("claim_event_key"))
        if not event_key or event_key.lower() in seen_claim_events:
            continue
        text = _canonical_claim_line(
            {
                "predicate": row.get("predicate"),
                "subject_name": row.get("subject_name"),
                "subject_text": row.get("subject_text"),
                "object_payload": row.get("object_payload"),
            }
        )
        score = _score_text_match(
            text=text,
            query_terms=query_terms,
            reference_ts=row.get("updated_at"),
            now_utc=now_utc,
            base_score=0.14 + (0.08 * _normalize_confidence(row.get("truth_confidence"), default=0.5)),
        )
        if query_terms and score < 0.08:
            continue
        predicate = _normalize_text(row.get("predicate")).lower()
        node_type = "goal" if _canonical_claim_terms_match(predicate, ["goal", "task", "commitment", "plan"]) else "event"
        if not _allowed_node_type(node_type, allowed_types):
            continue
        candidates.append(
            {
                "uuid": f"canonical_claim:{event_key}",
                "name": text[:120],
                "summary": text,
                "canonical_name": text[:120],
                "type": node_type,
                "group_id": f"pg:{user_id}",
                "created_at": row.get("updated_at"),
                "updated_at": row.get("updated_at"),
                "attributes": {
                    "predicate": predicate or None,
                    "claim_event_key": event_key,
                    "source": "canonical_claims",
                    **_retrieval_row_metadata(
                        source_type="canonical factual",
                        derived=False,
                        evidence_backed=True,
                        data_classification="canonical factual",
                    ),
                },
                "labels": ["Entity", node_type.title()],
                "discipline_score": round(score, 4),
                "_score": score,
                **_retrieval_row_metadata(
                    source_type="canonical factual",
                    derived=False,
                    evidence_backed=True,
                    data_classification="canonical factual",
                ),
            }
        )

    candidates.sort(
        key=lambda row: (
            float(row.get("_score") or 0.0),
            _normalize_text(row.get("updated_at")),
            _normalize_text(row.get("created_at")),
        ),
        reverse=True,
    )
    trimmed = candidates[:safe_limit]
    for row in trimmed:
        row.pop("_score", None)
    return trimmed


async def _pg_search_facts(
    *,
    tenant_id: str,
    user_id: str,
    query: str,
    limit: int,
    reference_time: Optional[datetime] = None,
    include_derived: bool = False,
) -> List[Dict[str, Any]]:
    # GRAPHITI_REPLACED: search_facts (semantic fact retrieval)
    # T1 containment: fail-closed for factual retrieval until v2 claim-store facts exist.
    # Continuity rows remain available only for explicit derived/debug flows.
    if not include_derived:
        return []
    return await _pg_search_continuity_facts(
        tenant_id=tenant_id,
        user_id=user_id,
        query=query,
        limit=limit,
        reference_time=reference_time,
    )


async def _pg_search_continuity_facts(
    *,
    tenant_id: str,
    user_id: str,
    query: str,
    limit: int,
    reference_time: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    tenant_scope = _tenant_scope_candidates(tenant_id) or [_normalize_text(tenant_id)]
    safe_limit = max(1, min(int(limit or 10), 50))
    query_terms = _query_terms(query)
    now_utc = (
        reference_time.astimezone(dt_timezone.utc)
        if isinstance(reference_time, datetime) and reference_time.tzinfo
        else datetime.utcnow().replace(tzinfo=dt_timezone.utc)
    )
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()

    def _append_fact(
        text: str,
        source: str,
        valid_at: Any,
        base_score: float = 0.2,
        *,
        source_type: str = "derived continuity/projection",
        derived: bool = True,
        evidence_backed: bool = False,
        data_classification: str = "derived continuity/projection",
    ) -> None:
        claim = _normalize_text(text)
        if not claim:
            return
        if not _allow_claim(claim) or not _allow_fact_text(claim):
            return
        key = claim.lower()
        if key in seen:
            return
        score = _score_text_match(
            text=claim,
            query_terms=query_terms,
            reference_ts=valid_at if isinstance(valid_at, datetime) else None,
            now_utc=now_utc,
            base_score=base_score,
        )
        if query_terms and score < 0.08:
            return
        seen.add(key)
        out.append(
            {
                "text": claim,
                "relevance": max(0.0, min(1.0, score)),
                "source": source,
                "valid_at": valid_at,
                "invalid_at": None,
                **_retrieval_row_metadata(
                    source_type=source_type,
                    derived=derived,
                    evidence_backed=evidence_backed,
                    data_classification=data_classification,
                ),
            }
        )

    entity_rows = await db.fetch(
        """
        SELECT canonical_name, type, relationship_to_user, key_facts, profile_text,
               last_known_status, confidence, last_seen_at, mention_count
        FROM entity_profiles
        WHERE user_id = $1
          AND status IN ('active', 'tentative')
        ORDER BY mention_count DESC NULLS LAST, last_seen_at DESC NULLS LAST
        LIMIT 200
        """,
        user_id,
    )
    for row in entity_rows or []:
        name = canonicalize_entity_name(row.get("canonical_name"))
        if not name:
            continue
        relationship = _normalize_text(row.get("relationship_to_user"))
        if relationship:
            _append_fact(
                f"{name} is the user's {relationship}.",
                "postgres_entity_profile",
                row.get("last_seen_at"),
                base_score=0.35,
            )
        status = _normalize_text(row.get("last_known_status"))
        if status:
            _append_fact(
                f"{name}: {status}",
                "postgres_entity_profile",
                row.get("last_seen_at"),
                base_score=0.3,
            )
        key_facts = row.get("key_facts")
        if isinstance(key_facts, list):
            for fact in key_facts[:8]:
                if isinstance(fact, dict):
                    text = _normalize_text(fact.get("fact") or fact.get("text"))
                    conf = _normalize_confidence(fact.get("confidence"), default=0.65)
                    valid_at = fact.get("last_confirmed") or row.get("last_seen_at")
                else:
                    text = _normalize_text(fact)
                    conf = 0.6
                    valid_at = row.get("last_seen_at")
                if text:
                    _append_fact(text, "postgres_entity_profile_fact", valid_at, base_score=conf * 0.5)

    thread_rows = await db.fetch(
        """
        SELECT title, detail, category, priority, status, last_mentioned_at, follow_up_after, thread_type
        FROM open_threads
        WHERE user_id = $1
          AND status IN ('open', 'snoozed')
        ORDER BY salience_score DESC NULLS LAST, last_mentioned_at DESC NULLS LAST
        LIMIT 180
        """,
        user_id,
    )
    for row in thread_rows or []:
        title = _normalize_text(row.get("title"))
        detail = _normalize_text(row.get("detail"))
        category = _normalize_text(row.get("category")).lower()
        priority = _normalize_text(row.get("priority")).lower()
        status = _normalize_text(row.get("status")).lower()
        thread_type = _normalize_text(row.get("thread_type")).lower()
        label = f"[{category}] " if category else ""
        text = f"{label}{title}" if title else detail
        if detail and detail.lower() not in text.lower():
            text = f"{text}. {detail}"
        if status:
            text = f"{text} (thread: {status})"
        if thread_type == "persistent_goal":
            text = f"Persistent goal: {text}"
        base = 0.28
        if priority == "high":
            base += 0.12
        elif priority == "medium":
            base += 0.06
        _append_fact(text, "postgres_open_threads", row.get("last_mentioned_at") or row.get("follow_up_after"), base_score=base)

    session_rows = await db.fetch(
        """
        SELECT session_date, one_line_summary, emotional_weight, emotional_note, session_kind,
               tension_signal, raw_triage_output->'memory_deltas' AS deltas
        FROM session_classifications
        WHERE user_id = $1
          AND is_memory_worthy = true
          AND ($2::timestamptz IS NULL OR session_date <= $2::timestamptz)
        ORDER BY session_date DESC
        LIMIT 220
        """,
        user_id,
        reference_time,
    )
    for row in session_rows or []:
        deltas = row.get("deltas")
        if isinstance(deltas, list):
            for delta in deltas[:6]:
                text = _normalize_text(delta)
                if text:
                    _append_fact(
                        text,
                        "postgres_session_classifications",
                        row.get("session_date"),
                        base_score=0.26,
                        data_classification="mixed/unsafe",
                    )
        one_line = _normalize_text(row.get("one_line_summary"))
        if one_line:
            _append_fact(
                one_line,
                "postgres_session_classifications",
                row.get("session_date"),
                base_score=0.2,
                data_classification="mixed/unsafe",
            )
        tension = _normalize_text(row.get("tension_signal"))
        if tension:
            _append_fact(
                tension,
                "postgres_session_classifications",
                row.get("session_date"),
                base_score=0.22,
                data_classification="mixed/unsafe",
            )
        emotional_note = _normalize_text(row.get("emotional_note"))
        if emotional_note and _normalize_text(row.get("emotional_weight")).lower() in {"medium", "high"}:
            _append_fact(
                emotional_note,
                "postgres_session_classifications",
                row.get("session_date"),
                base_score=0.18,
                data_classification="mixed/unsafe",
            )

    canonical_claim_rows = await db.fetch(
        """
        SELECT
          c.predicate,
          c.object_payload,
          c.truth_confidence,
          c.updated_at,
          c.subject_text,
          e.canonical_name AS subject_name,
          COUNT(ce.claim_evidence_id)::int AS evidence_count
        FROM claims c
        LEFT JOIN entities e
          ON e.tenant_id = c.tenant_id
         AND e.entity_id = c.subject_entity_id
        LEFT JOIN claim_evidence ce
          ON ce.tenant_id = c.tenant_id
         AND ce.claim_id = c.claim_id
        WHERE c.tenant_id = ANY($1::text[])
          AND c.user_id = $2
          AND c.lifecycle_status = 'active'
          AND ($3::timestamptz IS NULL OR c.updated_at <= $3::timestamptz)
        GROUP BY c.predicate, c.object_payload, c.truth_confidence, c.updated_at, c.subject_text, e.canonical_name
        ORDER BY c.updated_at DESC NULLS LAST
        LIMIT 240
        """,
        tenant_scope,
        user_id,
        reference_time,
    )
    for row in canonical_claim_rows or []:
        if int(row.get("evidence_count") or 0) <= 0:
            continue
        _append_fact(
            _canonical_claim_line(
                {
                    "predicate": row.get("predicate"),
                    "subject_name": row.get("subject_name"),
                    "subject_text": row.get("subject_text"),
                    "object_payload": row.get("object_payload"),
                }
            ),
            "canonical_claims",
            row.get("updated_at"),
            base_score=0.32 + (0.16 * _normalize_confidence(row.get("truth_confidence"), default=0.5)),
            source_type="canonical factual",
            derived=False,
            evidence_backed=True,
            data_classification="canonical factual",
        )

    out.sort(
        key=lambda row: (
            float(row.get("relevance") or 0.0),
            _normalize_text(row.get("valid_at")),
        ),
        reverse=True,
    )
    return out[:safe_limit]


async def _pg_get_recent_episodes(
    *,
    tenant_id: str,
    user_id: str,
    since: Optional[datetime],
    limit: int,
) -> List[Dict[str, Any]]:
    # GRAPHITI_REPLACED: get_recent_episodes (episodic retrieval list for ranking)
    # TEMPORARY_DEGRADED_REPLACEMENT: summary fields may be triage-derived; transcript body is evidence-backed.
    safe_limit = max(1, min(int(limit or 20), 200))
    rows = await db.fetch(
        """
        SELECT
          st.session_id,
          st.updated_at,
          st.messages,
          sc.session_date,
          sc.one_line_summary,
          sc.raw_triage_output->'memory_deltas' AS deltas
        FROM session_transcript st
        LEFT JOIN session_classifications sc
          ON sc.session_id = st.session_id
         AND sc.user_id = st.user_id
        WHERE st.tenant_id = $1
          AND st.user_id = $2
          AND ($3::timestamptz IS NULL OR COALESCE(sc.session_date, st.updated_at) >= $3::timestamptz)
        ORDER BY COALESCE(sc.session_date, st.updated_at) DESC, st.updated_at DESC
        LIMIT $4
        """,
        tenant_id,
        user_id,
        since,
        safe_limit,
    )
    episodes: List[Dict[str, Any]] = []
    for row in rows or []:
        session_id = _normalize_text(row.get("session_id"))
        deltas = row.get("deltas")
        if not isinstance(deltas, list):
            deltas = []
        summary = _normalize_text(row.get("one_line_summary")) or _normalize_text(deltas[0] if deltas else None)
        content = _format_session_transcript_content(row.get("messages"))
        if not summary:
            if deltas:
                summary = _shorten_line(" ".join([_normalize_text(x) for x in deltas[:2] if _normalize_text(x)]), 220)
            if not summary:
                summary = _shorten_line(content, 220)
        ref = row.get("session_date") or row.get("updated_at")
        ref_iso = ref.isoformat() if isinstance(ref, datetime) else _normalize_text(ref) or None
        episodes.append(
            {
                "name": f"session_raw_{session_id}" if session_id else "session_raw_unknown",
                "summary": summary,
                "reference_time": ref_iso,
                "episode_body": content,
                "uuid": f"pg_episode:{session_id}" if session_id else None,
                "source": "session_transcript+session_classifications",
                **_retrieval_row_metadata(
                    source_type="episodic",
                    derived=(not bool(content)),
                    evidence_backed=bool(content),
                    data_classification=("episodic" if bool(content) else "mixed/unsafe"),
                ),
            }
        )
    return episodes


async def _build_episodic_recall_items(
    *,
    tenant_scope: List[str],
    canonical_tenant: str,
    user_id: str,
    query: str,
    query_terms: List[str],
    reference_time: Optional[datetime],
    entity_hints: List[str],
    limit: int,
) -> Tuple[List[EpisodeRecallItem], Dict[str, Any]]:
    if not tenant_scope:
        return [], {"weakRecall": True, "reason": "empty_tenant_scope"}
    settings = get_settings()
    safe_limit = max(1, min(int(limit or 10), 20))
    candidate_limit = max(24, min(safe_limit * 8, 120))
    since = None
    if isinstance(reference_time, datetime):
        since = reference_time - timedelta(days=180)

    embedding_hits: Dict[str, Dict[str, Any]] = {}
    embedding_model_used = _normalize_text(settings.episodic_embedding_model) or "text-embedding-3-small"
    embedding_rows_count = 0
    if bool(settings.episodic_embedding_enabled):
        try:
            embedding_rows = await search_episode_embedding_candidates(
                db=db,
                tenant_scope=tenant_scope,
                user_id=user_id,
                query=query,
                model=embedding_model_used,
                limit=max(candidate_limit, 40),
                reference_time=reference_time,
            )
            embedding_rows_count = len(embedding_rows or [])
            for row in embedding_rows:
                session_id = _normalize_text(row.get("session_id"))
                episode_uuid = _normalize_text(row.get("episode_uuid"))
                similarity = float(row.get("embedding_similarity") or 0.0)
                if similarity <= 0:
                    continue
                for key in (
                    f"session:{session_id}" if session_id else "",
                    f"episode:{episode_uuid}" if episode_uuid else "",
                ):
                    if not key:
                        continue
                    existing = embedding_hits.get(key)
                    if existing is None or similarity > float(existing.get("embedding_similarity") or 0.0):
                        embedding_hits[key] = {
                            "embedding_similarity": similarity,
                            "unit_text": _normalize_text(row.get("unit_text")),
                            "source_tenant": _normalize_text(row.get("tenant_id")),
                            "embedding_model": _normalize_text(row.get("embedding_model")) or embedding_model_used,
                            "user_char_ratio": float(((row.get("metadata") or {}).get("user_char_ratio") or 0.0)),
                            "user_turn_count": int(((row.get("metadata") or {}).get("user_turn_count") or 0)),
                        }
        except Exception as e:
            logger.warning("episodic recall embedding candidate lookup failed tenant_scope=%s user=%s err=%s", tenant_scope, user_id, e)

    # GRAPHITI_REPLACED: get_recent_episodes (episodic candidates for recall ranking)
    # TEMPORARY_DEGRADED_REPLACEMENT: episodic summaries include derived triage context; evidence lines are transcript-backed.
    episode_tasks = [
        _pg_get_recent_episodes(
            tenant_id=tenant_id,
            user_id=user_id,
            since=since,
            limit=candidate_limit,
        )
        for tenant_id in tenant_scope
    ]
    episode_results = await asyncio.gather(*episode_tasks, return_exceptions=True)

    query_text = _normalize_text(query).lower()
    continuation_query = any(
        phrase in query_text
        for phrase in (
            "remember",
            "other day",
            "that conversation",
            "we were exploring",
            "continue that idea",
            "continue that",
        )
    )
    relationship_query = any(
        token in query_text
        for token in (
            "ashley",
            "jasmine",
            "yoshi",
            "girlfriend",
            "daughter",
            "wife",
            "husband",
            "mother",
            "father",
            "mom",
            "dad",
            "sister",
            "brother",
            "friend",
            "relationship",
            "lately",
        )
    )
    reflective_query = any(
        token in query_text
        for token in (
            "spiritual",
            "spirituality",
            "god",
            "faith",
            "meaning",
            "purpose",
            "life",
            "moat",
            "soul",
            "inner",
            "reflect",
        )
    )
    query_profile = "default"
    if relationship_query:
        query_profile = "relationship"
    elif reflective_query:
        query_profile = "reflective"
    elif continuation_query:
        query_profile = "continuation"

    profile_weights: Dict[str, Dict[str, float]] = {
        "relationship": {
            "embeddingSimilarity": 0.44,
            "lexicalOverlap": 0.22,
            "recency": 0.18,
            "linkedEntityOverlap": 0.16,
            "userTurnDensity": 0.02,
            "continuationIntentBonus": 0.03,
        },
        "reflective": {
            "embeddingSimilarity": 0.58,
            "lexicalOverlap": 0.12,
            "recency": 0.10,
            "linkedEntityOverlap": 0.12,
            "userTurnDensity": 0.08,
            "continuationIntentBonus": 0.06,
        },
        "continuation": {
            "embeddingSimilarity": 0.54,
            "lexicalOverlap": 0.15,
            "recency": 0.11,
            "linkedEntityOverlap": 0.14,
            "userTurnDensity": 0.06,
            "continuationIntentBonus": 0.08,
        },
        "default": {
            "embeddingSimilarity": 0.50,
            "lexicalOverlap": 0.20,
            "recency": 0.12,
            "linkedEntityOverlap": 0.18,
            "userTurnDensity": 0.04,
            "continuationIntentBonus": 0.05,
        },
    }
    profile_thresholds: Dict[str, Dict[str, float]] = {
        "relationship": {
            "minScore": 0.30,
            "minEmbedding": 0.24,
            "minLexical": 0.12,
            "minAnchor": 0.12,
            "minUserDensity": 0.35,
            "weakTopScore": 0.37,
            "weakEmbedding": 0.27,
            "weakLexical": 0.14,
            "weakAnchor": 0.14,
            "weakUserDensity": 0.35,
        },
        "reflective": {
            "minScore": 0.26,
            "minEmbedding": 0.22,
            "minLexical": 0.06,
            "minAnchor": 0.06,
            "minUserDensity": 0.50,
            "weakTopScore": 0.33,
            "weakEmbedding": 0.24,
            "weakLexical": 0.08,
            "weakAnchor": 0.08,
            "weakUserDensity": 0.45,
        },
        "continuation": {
            "minScore": 0.27,
            "minEmbedding": 0.22,
            "minLexical": 0.07,
            "minAnchor": 0.07,
            "minUserDensity": 0.45,
            "weakTopScore": 0.35,
            "weakEmbedding": 0.25,
            "weakLexical": 0.08,
            "weakAnchor": 0.08,
            "weakUserDensity": 0.45,
        },
        "default": {
            "minScore": 0.28,
            "minEmbedding": 0.24,
            "minLexical": 0.12,
            "minAnchor": 0.10,
            "minUserDensity": 0.40,
            "weakTopScore": 0.37,
            "weakEmbedding": 0.27,
            "weakLexical": 0.12,
            "weakAnchor": 0.12,
            "weakUserDensity": 0.40,
        },
    }
    active_weights = profile_weights.get(query_profile, profile_weights["default"])
    active_thresholds = profile_thresholds.get(query_profile, profile_thresholds["default"])
    embedding_weight = float(active_weights.get("embeddingSimilarity") or 0.0)
    lexical_weight = float(active_weights.get("lexicalOverlap") or 0.0)
    recency_weight = float(active_weights.get("recency") or 0.0)
    anchor_weight = float(active_weights.get("linkedEntityOverlap") or 0.0)
    user_density_weight = float(active_weights.get("userTurnDensity") or 0.0)
    continuation_bonus = float(active_weights.get("continuationIntentBonus") or 0.0)
    anchor_terms = [_normalize_text(x).lower() for x in (entity_hints or []) if _normalize_text(x)]

    now_utc = (
        reference_time.astimezone(dt_timezone.utc)
        if isinstance(reference_time, datetime) and reference_time.tzinfo is not None
        else (
            reference_time.replace(tzinfo=dt_timezone.utc)
            if isinstance(reference_time, datetime)
            else datetime.utcnow().replace(tzinfo=dt_timezone.utc)
        )
    )

    ranked_rows: List[Dict[str, Any]] = []
    seen_keys = set()
    candidates_seen = 0

    for tenant_id, episodes in zip(tenant_scope, episode_results):
        if isinstance(episodes, Exception):
            logger.warning("episodic recall: get_recent_episodes failed tenant=%s user=%s err=%s", tenant_id, user_id, episodes)
            continue
        for raw in episodes or []:
            if isinstance(raw, dict):
                name = _normalize_text(raw.get("name") or raw.get("episode_name"))
                summary = _normalize_text(raw.get("summary") or raw.get("episode_summary"))
                content = _normalize_text(raw.get("episode_body") or raw.get("content") or raw.get("text"))
                uuid = _normalize_text(raw.get("uuid"))
                reference_value = raw.get("reference_time") or raw.get("created_at")
            else:
                name = _normalize_text(getattr(raw, "name", None))
                summary = _normalize_text(getattr(raw, "summary", None) or getattr(raw, "episode_summary", None))
                content = _normalize_text(getattr(raw, "episode_body", None) or getattr(raw, "content", None) or getattr(raw, "text", None))
                uuid = _normalize_text(getattr(raw, "uuid", None))
                reference_value = getattr(raw, "reference_time", None) or getattr(raw, "created_at", None)

            if name.startswith("session_summary_"):
                continue
            if not (summary or content):
                continue
            candidates_seen += 1

            session_id = _extract_session_id_from_episode_name(name)
            ref_iso = _normalize_episode_reference_time(reference_value)
            search_text = f"{summary}\n{content}".strip()
            lexical_score = _episode_text_overlap_score(search_text, query_terms)
            embedding_match = (
                embedding_hits.get(f"session:{session_id}") if session_id else None
            ) or (
                embedding_hits.get(f"episode:{uuid}") if uuid else None
            )
            embedding_score = float((embedding_match or {}).get("embedding_similarity") or 0.0)
            user_density_score = float((embedding_match or {}).get("user_char_ratio") or 0.5)
            user_density_score = min(1.0, max(0.0, user_density_score))
            anchor_hits = 0
            if anchor_terms and search_text:
                lowered = search_text.lower()
                anchor_hits = sum(1 for term in anchor_terms if term and term in lowered)
            anchor_score = min(1.0, (anchor_hits / float(max(1, len(anchor_terms))))) if anchor_terms else 0.0

            recency_score = 0.0
            ref_dt = _parse_utc_ts(ref_iso) if ref_iso else None
            if ref_dt is not None:
                age_days = max(0.0, (now_utc - ref_dt).total_seconds() / 86400.0)
                recency_score = 1.0 / (1.0 + (age_days / 14.0))
            score = (
                (embedding_weight * embedding_score)
                + (lexical_weight * lexical_score)
                + (recency_weight * recency_score)
                + (anchor_weight * anchor_score)
                + (user_density_weight * user_density_score)
            )
            if continuation_query and lexical_score < 0.20:
                continuation_score = (0.56 * embedding_score) + (0.18 * recency_score) + (0.16 * anchor_score) + (0.10 * user_density_score) + 0.04
                if embedding_score >= 0.20 or lexical_score >= 0.08 or anchor_score >= 0.08:
                    score = max(score, continuation_score)
            if continuation_query and embedding_score >= 0.46 and (lexical_score >= 0.08 or user_density_score >= 0.55):
                score += continuation_bonus

            evidence = _extract_episode_evidence_lines(search_text, query_terms, limit=2)
            embedding_evidence = _normalize_text((embedding_match or {}).get("unit_text"))
            if embedding_evidence and embedding_evidence not in evidence and len(evidence) < 2:
                evidence.append(_shorten_line(embedding_evidence, 180))
            linked_entities: List[str] = []
            if anchor_terms and search_text:
                lowered = search_text.lower()
                for hint in entity_hints:
                    term = _normalize_text(hint).lower()
                    if term and term in lowered and hint not in linked_entities:
                        linked_entities.append(hint)
                    if len(linked_entities) >= 4:
                        break

            dedupe_key = uuid or f"{name}|{ref_iso}|{tenant_id}"
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)

            ranked_rows.append(
                {
                    "episode_id": uuid or None,
                    "session_id": session_id,
                    "reference_time": ref_iso,
                    "score": score,
                    "summary": summary or _shorten_line(search_text, 220),
                    "content": content,
                    "evidence": evidence,
                    "linked_entities": linked_entities,
                    "source_tenant": _normalize_text((embedding_match or {}).get("source_tenant")) or tenant_id,
                    "embedding_similarity": embedding_score,
                    "lexical_overlap": lexical_score,
                    "recency_score": recency_score,
                    "anchor_score": anchor_score,
                    "user_density_score": user_density_score,
                }
            )

    ranked_rows.sort(
        key=lambda row: (
            float(row.get("score") or 0.0),
            _normalize_text(row.get("reference_time")),
        ),
        reverse=True,
    )
    gated_rows: List[Dict[str, Any]] = []
    for row in ranked_rows:
        emb = float(row.get("embedding_similarity") or 0.0)
        lex = float(row.get("lexical_overlap") or 0.0)
        anc = float(row.get("anchor_score") or 0.0)
        dens = float(row.get("user_density_score") or 0.0)
        total = float(row.get("score") or 0.0)
        # Guardrail against low-signal episodic hallucination.
        if (
            total < float(active_thresholds.get("minScore") or 0.28)
            and emb < float(active_thresholds.get("minEmbedding") or 0.24)
            and lex < float(active_thresholds.get("minLexical") or 0.12)
            and anc < float(active_thresholds.get("minAnchor") or 0.10)
            and dens < float(active_thresholds.get("minUserDensity") or 0.40)
        ):
            continue
        gated_rows.append(row)
    ranked_rows = gated_rows

    # Session diversity pass: enforce diversity early, then allow repeats for strong same-session continuity.
    diverse_rows: List[Dict[str, Any]] = []
    used_sessions: set[str] = set()
    diverse_target = min(safe_limit, 4)
    for row in ranked_rows:
        sid = _normalize_text(row.get("session_id"))
        if sid and sid in used_sessions and len(diverse_rows) < diverse_target:
            continue
        diverse_rows.append(row)
        if sid:
            used_sessions.add(sid)
        if len(diverse_rows) >= safe_limit:
            break
    ranked_rows = diverse_rows[:safe_limit]

    # If top candidates have weak/no evidence, use transcript tail by session id as backup grounding.
    for row in ranked_rows[: min(3, len(ranked_rows))]:
        if row.get("evidence"):
            continue
        session_id = _normalize_text(row.get("session_id"))
        if not session_id:
            continue
        transcript_row = await db.fetchone(
            """
            SELECT tenant_id, messages
            FROM session_transcript
            WHERE tenant_id = ANY($1::text[]) AND user_id = $2 AND session_id = $3
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            tenant_scope,
            user_id,
            session_id,
        )
        messages = transcript_row.get("messages") if isinstance(transcript_row, dict) else None
        if isinstance(messages, str):
            try:
                messages = json.loads(messages)
            except Exception:
                messages = []
        evidence = _extract_transcript_evidence_lines(messages if isinstance(messages, list) else [], query_terms, limit=2)
        if evidence:
            row["evidence"] = evidence
            if isinstance(transcript_row, dict):
                row["source_tenant"] = _normalize_text(transcript_row.get("tenant_id")) or row.get("source_tenant")

    items = [
        EpisodeRecallItem(
            episodeId=row.get("episode_id"),
            sessionId=row.get("session_id"),
            referenceTime=row.get("reference_time"),
            score=round(float(row.get("score") or 0.0), 4),
            summary=_normalize_text(row.get("summary")),
            evidence=[_normalize_text(x) for x in (row.get("evidence") or []) if _normalize_text(x)],
            linkedEntities=[_normalize_text(x) for x in (row.get("linked_entities") or []) if _normalize_text(x)],
            sourceTenant=_normalize_text(row.get("source_tenant")) or canonical_tenant,
        )
        for row in ranked_rows
    ]
    top_score = float((ranked_rows[0].get("score") if ranked_rows else 0.0) or 0.0)
    top_embedding = float((ranked_rows[0].get("embedding_similarity") if ranked_rows else 0.0) or 0.0)
    top_lexical = float((ranked_rows[0].get("lexical_overlap") if ranked_rows else 0.0) or 0.0)
    top_anchor = float((ranked_rows[0].get("anchor_score") if ranked_rows else 0.0) or 0.0)
    top_user_density = float((ranked_rows[0].get("user_density_score") if ranked_rows else 0.0) or 0.0)
    weak_recall = (
        (not ranked_rows)
        or (top_score < float(active_thresholds.get("weakTopScore") or 0.37))
        or (
            top_embedding < float(active_thresholds.get("weakEmbedding") or 0.27)
            and top_lexical < float(active_thresholds.get("weakLexical") or 0.12)
            and top_anchor < float(active_thresholds.get("weakAnchor") or 0.12)
            and top_user_density < float(active_thresholds.get("weakUserDensity") or 0.40)
        )
    )
    weak_reason = "no_candidates"
    if ranked_rows:
        if top_score < float(active_thresholds.get("weakTopScore") or 0.37):
            weak_reason = "low_top_score"
        elif (
            top_embedding < float(active_thresholds.get("weakEmbedding") or 0.27)
            and top_lexical < float(active_thresholds.get("weakLexical") or 0.12)
            and top_anchor < float(active_thresholds.get("weakAnchor") or 0.12)
            and top_user_density < float(active_thresholds.get("weakUserDensity") or 0.40)
        ):
            weak_reason = "weak_match_signals"
        else:
            weak_reason = ""
    return items, {
        "weakRecall": weak_recall,
        "weakRecallReason": weak_reason,
        "topScore": round(top_score, 4) if ranked_rows else 0.0,
        "topEmbeddingSimilarity": round(top_embedding, 4) if ranked_rows else 0.0,
        "topLexicalOverlap": round(top_lexical, 4) if ranked_rows else 0.0,
        "topAnchorScore": round(top_anchor, 4) if ranked_rows else 0.0,
        "topUserDensityScore": round(top_user_density, 4) if ranked_rows else 0.0,
        "candidatesSeen": int(candidates_seen),
        "candidatesRanked": int(len(ranked_rows)),
        "diverseSessions": int(len({_normalize_text(r.get('session_id')) for r in ranked_rows if _normalize_text(r.get('session_id'))})),
        "embeddingRowsMatched": int(embedding_rows_count),
        "embeddingSessionHits": int(len(embedding_hits)),
        "queryProfile": query_profile,
        "effectiveWeights": active_weights,
        "thresholds": active_thresholds,
    }


async def _fetch_user_model_rows_for_scope(
    tenant_scope: List[str],
    user_id: str
) -> List[Dict[str, Any]]:
    tenants = [_normalize_text(t) for t in (tenant_scope or []) if _normalize_text(t)]
    if not tenants or not user_id:
        return []
    rows = await db.fetch(
        """
        SELECT tenant_id, model, version, created_at, updated_at, last_source, narrative_stable, narrative_current
        FROM user_model
        WHERE tenant_id = ANY($1::text[]) AND user_id = $2
        """,
        tenants,
        user_id,
    )
    if not rows:
        return []
    order_map = {tenant: idx for idx, tenant in enumerate(tenants)}

    def _sort_key(row: Dict[str, Any]) -> Tuple[float, int]:
        tenant = _normalize_text(row.get("tenant_id"))
        rank = order_map.get(tenant, 999)
        updated_at = row.get("updated_at")
        updated_ts = updated_at.timestamp() if isinstance(updated_at, datetime) else 0.0
        return (-updated_ts, rank)

    return sorted(rows, key=_sort_key)


def _extract_valid_relationship_entities(model: Dict[str, Any], limit: int = 10) -> List[Dict[str, str]]:
    relationships = model.get("key_relationships")
    if not isinstance(relationships, list):
        return []
    entities: List[Dict[str, str]] = []
    seen = set()
    for rel in relationships:
        if not isinstance(rel, dict):
            continue
        if not _build_relationship_signal_text(rel.get("name"), rel.get("who"), rel.get("status")):
            continue
        name = _normalize_text(rel.get("name"))
        who = _normalize_text(rel.get("who")).lower()
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        entities.append({"name": name, "who": who})
        if len(entities) >= limit:
            break
    return entities


def _build_entity_profile_text(name: str, facts: List[str], relationship_type: Optional[str] = None) -> str:
    normalized_name = _normalize_text(name) or "This person"
    relationship = _normalize_text(relationship_type).lower()
    clean_facts = [_normalize_text(f) for f in (facts or []) if _normalize_text(f)]
    lead = (
        f"{normalized_name} is the user's {relationship} and remains a key relationship in the memory context."
        if relationship
        else f"{normalized_name} is a key person in the user's relationship context."
    )
    if not clean_facts:
        return f"{lead} The emotional significance is present but current facts are sparse, and no clear recent event or open thread is available yet."

    emotion_markers = (
        "hurt", "sad", "anger", "angry", "guilt", "love", "loss", "struggling", "upset",
        "tension", "disappointment", "emotional", "burdened", "neglect"
    )
    recent_markers = (
        "today", "yesterday", "last night", "recent", "currently", "this week", "this month",
        "2026", "2025", "february", "january", "march", "april", "may", "june", "july",
        "august", "september", "october", "november", "december", "last year"
    )
    open_thread_markers = (
        "struggling", "unresolved", "blocked", "breakup", "neglect", "hospital",
        "argument", "upset", "hurt", "suffers", "loss"
    )

    emotional_fact = next((f for f in clean_facts if any(k in f.lower() for k in emotion_markers)), clean_facts[0])
    recent_fact = next((f for f in clean_facts if any(k in f.lower() for k in recent_markers)), None)
    open_thread_fact = next((f for f in clean_facts if any(k in f.lower() for k in open_thread_markers)), None)

    sentence_two = f"Emotionally, this matters because {_shorten_line(emotional_fact, 180)}"
    sentence_three_parts: List[str] = []
    if recent_fact:
        sentence_three_parts.append(f"Recent event: {_shorten_line(recent_fact, 170)}")
    if open_thread_fact and open_thread_fact.lower() != (recent_fact or "").lower():
        sentence_three_parts.append(f"Open thread: {_shorten_line(open_thread_fact, 170)}")

    if sentence_three_parts:
        sentence_three = " ".join(sentence_three_parts)
        return _ensure_sentence_spacing(f"{lead} {sentence_two} {sentence_three}")
    return _ensure_sentence_spacing(f"{lead} {sentence_two}")


def _entity_type_bucket(raw: Any) -> str:
    return infer_ontology_type(raw)


_ENTITY_HINT_REJECT_TOKENS = {
    "was", "is", "are", "be", "user", "assistant", "summary", "session", "update",
}
_ENTITY_HINT_PROPER_NOUN_STOPWORDS = {
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
    "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December",
    "The", "This", "That",
}
_RELATIONSHIP_ROLE_MENTION_RE = re.compile(
    r"\b(partner|wife|husband|girlfriend|boyfriend|daughter|son|mother|father|mom|dad|sister|brother|friend)\s+([A-Z][a-z]+)\b",
    flags=re.IGNORECASE,
)
_RELATIONSHIP_ROLE_ALIASES = {
    "mom": "mother",
    "dad": "father",
}


def _allow_entity_hint_name(name: str, entity_type: str) -> bool:
    clean = canonicalize_entity_name(name)
    if not clean:
        return False
    lower = clean.lower()
    if lower in _ENTITY_HINT_REJECT_TOKENS:
        return False
    if lower.startswith("session_summary_") or re.search(r"\bcm[a-z0-9]{8,}\b", lower):
        return False
    if re.fullmatch(r"[0-9a-f-]{16,}", lower):
        return False
    if "__" in clean or clean.count("_") >= 2:
        return False
    if entity_type != "person":
        if lower in {"bluum", "sophie"}:
            return True
        if len(re.findall(r"[A-Za-z0-9]+", clean)) < 2 and len(clean) < 5:
            return False
        if re.search(r"\b(quick update|major update|code base)\b", lower):
            return False
    return True


def _extract_hints_from_summary_texts(
    summaries: List[Dict[str, Any]],
    existing_names: set[str],
    relationship_names: set[str],
    relationship_roles: Optional[Dict[str, str]] = None,
    min_mentions: int = 2,
    limit: int = 4,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    role_index = relationship_roles if isinstance(relationship_roles, dict) else {}
    candidate_mentions: Dict[str, int] = {}
    candidate_last_seen: Dict[str, Optional[str]] = {}
    candidate_role: Dict[str, Optional[str]] = {}
    candidate_type: Dict[str, str] = {}

    for row in summaries or []:
        summary_text = _normalize_text(row.get("summary_facts") or row.get("summary_text") or row.get("moment"))
        if not summary_text:
            continue
        names = re.findall(r"\b[A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})?\b", summary_text)
        for candidate in names:
            clean = _normalize_text(candidate)
            if not clean or clean in _ENTITY_HINT_PROPER_NOUN_STOPWORDS:
                continue
            lower = clean.lower()
            if lower in existing_names:
                continue
            role = _normalize_text(role_index.get(lower)).lower() or None
            if lower in relationship_names or role:
                entity_type = "person"
            elif extract_location(clean):
                entity_type = "place"
            else:
                entity_type = "other"
            if not _allow_entity_hint_name(clean, entity_type):
                continue
            candidate_mentions[lower] = int(candidate_mentions.get(lower) or 0) + 1
            candidate_last_seen[lower] = _normalize_text(row.get("created_at")) or candidate_last_seen.get(lower)
            candidate_role[lower] = role or candidate_role.get(lower)
            candidate_type[lower] = entity_type

    for lower, mentions in sorted(candidate_mentions.items(), key=lambda x: x[1], reverse=True):
        role = _normalize_text(candidate_role.get(lower)).lower() or None
        is_relationship = lower in relationship_names or bool(role)
        if mentions < max(1, int(min_mentions)) and not is_relationship:
            continue
        if lower in existing_names:
            continue
        existing_names.add(lower)
        pretty_name = next((c for c in re.split(r"\s+", lower) if c), lower)
        if " " in lower:
            pretty_name = " ".join([part.capitalize() for part in lower.split(" ") if part])
        else:
            pretty_name = lower.capitalize()
        out.append(
            {
                "entityId": None,
                "name": pretty_name,
                "type": candidate_type.get(lower) or ("person" if is_relationship else "other"),
                "role": role,
                "importance": 0.74 if is_relationship else 0.64,
                "salience": 0.74 if is_relationship else 0.6,
                "lastSeenAt": candidate_last_seen.get(lower),
                "source": "summary_recurrence",
                "confidence": 0.7 if is_relationship else 0.58,
                "updatedAt": candidate_last_seen.get(lower),
            }
        )
        if len(out) >= limit:
            return out
    return out


def _normalize_relationship_role(value: Any) -> str:
    role = _normalize_text(value).lower()
    if not role:
        return ""
    return _RELATIONSHIP_ROLE_ALIASES.get(role, role)


def _extract_relationship_roles_from_texts(
    texts: List[str],
) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for raw in texts or []:
        text = _normalize_text(raw)
        if not text:
            continue
        for role, name in _RELATIONSHIP_ROLE_MENTION_RE.findall(text):
            clean_name = _normalize_text(name)
            clean_role = _normalize_relationship_role(role)
            if not clean_name or not clean_role:
                continue
            out.setdefault(clean_name.lower(), clean_role)
    return out


def _extract_relationship_roles_from_summaries(
    summaries: List[Dict[str, Any]],
) -> Dict[str, str]:
    texts: List[str] = []
    for row in summaries or []:
        texts.append(_normalize_text(row.get("summary_facts") or row.get("summary_text") or row.get("moment")))
    return _extract_relationship_roles_from_texts(texts)


def _coerce_salience_float(raw: Any, default: float = 0.5) -> float:
    text = _normalize_text(raw).lower()
    if text in {"high", "h"}:
        return 1.0
    if text in {"medium", "med", "m"}:
        return 0.7
    if text in {"low", "l"}:
        return 0.4
    try:
        v = float(raw)
    except Exception:
        return default
    if v > 1.0:
        v = v / 5.0 if v <= 5.0 else v / 10.0
    return max(0.0, min(1.0, v))


def _coerce_importance_float(raw: Any, default: float = 0.5) -> float:
    text = _normalize_text(raw).lower()
    if text in {"high", "h"}:
        return 1.0
    if text in {"medium", "med", "m"}:
        return 0.7
    if text in {"low", "l"}:
        return 0.4
    try:
        return max(0.0, min(1.0, float(raw)))
    except Exception:
        return default


def _parse_entity_node_ts(node: Dict[str, Any]) -> Optional[datetime]:
    attrs = node.get("attributes") if isinstance(node.get("attributes"), dict) else {}
    for key in ("last_seen_at", "updated_at", "reference_time", "created_at", "first_seen_at"):
        value = attrs.get(key) if key in attrs else node.get(key)
        dt = _parse_optional_dt(value)
        if dt:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=dt_timezone.utc)
            return dt.astimezone(dt_timezone.utc)
    return None


def _iso_or_none(value: Optional[datetime]) -> Optional[str]:
    if not value:
        return None
    dt = value if value.tzinfo else value.replace(tzinfo=dt_timezone.utc)
    return dt.astimezone(dt_timezone.utc).isoformat()


def _entity_role_from_user_model(user_model: Dict[str, Any], entity_name: str) -> Optional[str]:
    if not entity_name or not isinstance(user_model, dict):
        return None
    relationships = user_model.get("key_relationships")
    if not isinstance(relationships, list):
        return None
    target = entity_name.lower()
    for rel in relationships:
        if not isinstance(rel, dict):
            continue
        name = _normalize_text(rel.get("name")).lower()
        if not name or name != target:
            continue
        who = _normalize_text(rel.get("who")).lower()
        return who or None
    return None


def _entity_importance_score(
    entity_name: str,
    entity_type: str,
    role: Optional[str],
    loop_texts: List[str],
    candidate_texts: List[str],
) -> float:
    terms = _query_terms(entity_name)
    recurrence_hits = 0
    if terms:
        for text in candidate_texts:
            if _query_overlap_score(text, terms) >= 0.55:
                recurrence_hits += 1
    loop_hits = 0
    for loop_text in loop_texts:
        if terms and _query_overlap_score(loop_text, terms) >= 0.45:
            loop_hits += 1
    base = 0.3
    if entity_type in {"person", "project"}:
        base += 0.2
    if role:
        base += 0.2
    base += min(0.2, recurrence_hits * 0.08)
    base += min(0.2, loop_hits * 0.1)
    return max(0.0, min(1.0, base))


async def _build_entity_candidates(
    tenant_id: str,
    user_id: str,
    reference_time: datetime,
    user_model: Optional[Dict[str, Any]] = None,
    context_texts: Optional[List[str]] = None,
    max_hints: int = 8,
    graphiti_only: bool = False,
) -> List[Dict[str, Any]]:
    loop_rows = await loops.get_top_loops_for_startbrief(
        tenant_id=tenant_id,
        user_id=user_id,
        limit=12,
        persona_id=None,
    )
    loop_texts: List[str] = [
        _normalize_text(getattr(row, "text", ""))
        for row in (loop_rows or [])
        if _normalize_text(getattr(row, "text", ""))
    ]
    relationship_entities = _extract_valid_relationship_entities(user_model or {}, limit=10)
    seed_names = [_normalize_text(x.get("name")) for x in relationship_entities if _normalize_text(x.get("name"))]
    context_rows = [_normalize_text(x) for x in (context_texts or []) if _normalize_text(x)]
    relationship_role_index: Dict[str, str] = {}
    for rel in relationship_entities:
        rel_name = _normalize_text(rel.get("name")).lower()
        rel_role = _normalize_relationship_role(rel.get("who"))
        if rel_name and rel_role:
            relationship_role_index.setdefault(rel_name, rel_role)
    for rel_name, rel_role in _extract_relationship_roles_from_texts(context_rows).items():
        if rel_name and rel_role:
            relationship_role_index.setdefault(rel_name, rel_role)

    context_terms = _query_terms(" ".join(context_rows))
    exact_seed_nodes: List[Dict[str, Any]] = []
    for seed_name in seed_names[:8]:
        exact_seed_nodes.extend(
            await graphiti_client.get_canonical_entity_nodes(
                tenant_id=tenant_id,
                user_id=user_id,
                name=seed_name,
                allowed_types=["person", "project", "goal", "loop", "preference", "event"],
                limit=1,
            )
        )
    ranked_nodes = await graphiti_client.get_ranked_canonical_nodes(
        tenant_id=tenant_id,
        user_id=user_id,
        allowed_types=["person", "project", "goal", "loop", "preference", "event"],
        limit=20,
    )
    queries = [
        "important people in the user's life",
        "active projects and products the user is working on",
        "important goals and loops in the user's life",
    ]

        # GRAPHITI_REPLACED: search_nodes (entity candidate retrieval for start surfaces)
        # TEMPORARY_DEGRADED_REPLACEMENT: start-surface entities are continuity hints from Postgres aggregates.
    search_tasks = [
        _pg_search_nodes(
            tenant_id=tenant_id,
            user_id=user_id,
            query=q,
            limit=10,
            reference_time=reference_time,
            allowed_types=["person", "project", "goal", "loop", "preference", "event"],
        )
        for q in queries[:3]
    ]
    search_results = await asyncio.gather(*search_tasks, return_exceptions=True)
    seen = set()
    candidates: List[Dict[str, Any]] = []
    text_pool: List[str] = []
    role_grounding_cache: Dict[str, Dict[str, Any]] = {}
    node_results: List[List[Dict[str, Any]]] = [exact_seed_nodes, ranked_nodes]
    for result in search_results:
        if isinstance(result, Exception):
            continue
        node_results.append(result or [])
    for result in node_results:
        for node in (result or []):
            if not isinstance(node, dict):
                continue
            name = canonicalize_entity_name(node.get("canonical_name") or node.get("name") or node.get("summary"))
            if not name:
                continue
            key = canonicalize_entity_name(name).lower()
            if key in seen:
                continue
            seen.add(key)
            role_grounding = role_grounding_cache.get(key)
            if role_grounding is None:
                # GRAPHITI_REPLACED: get_entity_role_grounding (entity relationship grounding)
                # TEMPORARY_DEGRADED_REPLACEMENT: role grounding is inferred from profile aggregates.
                role_grounding = await _pg_get_entity_role_grounding(
                    tenant_id=tenant_id,
                    user_id=user_id,
                    name=name,
                    entity_id=_normalize_text(node.get("uuid")) or None,
                ) or {}
                role_grounding_cache[key] = role_grounding
            role = (
                _normalize_text(role_grounding.get("role"))
                or _normalize_text(role_grounding.get("relationship"))
                or relationship_role_index.get(name.lower())
                or _entity_role_from_user_model(user_model or {}, name)
            )
            role = role or None
            labels = node.get("labels") if isinstance(node.get("labels"), list) else []
            raw_type = _normalize_text(node.get("type"))
            entity_type = infer_ontology_type(raw_type, labels=labels, name=name)
            if role and entity_type != "project":
                entity_type = "person"
            if entity_type == "project" and _normalize_text(role_grounding.get("edge_name")).upper() == "WORKING_ON":
                role = role or "active_project"
            if not _allow_entity_hint_name(name, entity_type):
                continue
            if not is_allowed_runtime_node(
                name=name,
                raw_type=entity_type,
                labels=labels,
                allowed_types=["person", "project", "goal", "loop", "preference", "event"],
                include_internal=False,
            ):
                continue
            if context_terms and _query_overlap_score(name, context_terms) < 0.2 and not role and entity_type not in {"person", "project"}:
                continue
            ts = _parse_entity_node_ts(node)
            attrs = node.get("attributes") if isinstance(node.get("attributes"), dict) else {}
            salience = _coerce_salience_float(attrs.get("salience"))
            recency = 0.3
            if ts:
                age_hours = max(0.0, (reference_time.astimezone(dt_timezone.utc) - ts).total_seconds() / 3600.0)
                if age_hours <= 24:
                    recency = 1.0
                elif age_hours <= 72:
                    recency = 0.75
                elif age_hours <= 168:
                    recency = 0.55
            text_pool.append(name)
            importance = _entity_importance_score(
                entity_name=name,
                entity_type=entity_type,
                role=role,
                loop_texts=loop_texts,
                candidate_texts=text_pool,
            )
            importance = max(importance, _coerce_importance_float(attrs.get("importance"), default=0.0))
            confidence = _normalize_confidence(attrs.get("confidence"), default=(0.85 if _normalize_text(node.get("uuid")) else 0.6))
            type_boost = 0.15 if entity_type in {"person", "project"} else 0.0
            score = (0.3 * recency) + (0.15 * salience) + (0.3 * importance) + (0.1 * confidence) + type_boost
            candidates.append(
                {
                    "entityId": _normalize_text(node.get("uuid")) or None,
                    "name": name,
                    "type": entity_type,
                    "role": role,
                    "importance": round(float(importance), 4),
                    "salience": round(float(salience), 4),
                    "lastSeenAt": _iso_or_none(ts),
                    "source": "graphiti_node",
                    "confidence": round(float(confidence), 4),
                    "updatedAt": _iso_or_none(ts),
                    "score": round(float(score), 4),
                    "raw": node,
                    "relationshipEdge": _normalize_text(role_grounding.get("edge_name")) or None,
                }
            )

    if not graphiti_only:
        for rel in relationship_entities:
            name = canonicalize_entity_name(rel.get("name"))
            role = _normalize_relationship_role(rel.get("who"))
            if not name:
                continue
            if not _allow_entity_hint_name(name, "person"):
                continue
            key = name.lower()
            if any(_normalize_text(c.get("name")).lower() == key for c in candidates):
                continue
            importance = _entity_importance_score(
                entity_name=name,
                entity_type="person",
                role=role,
                loop_texts=loop_texts,
                candidate_texts=text_pool,
            )
            salience = 0.7
            score = (0.35 * 0.6) + (0.2 * salience) + (0.35 * importance) + (0.1 * 0.65)
            candidates.append(
                {
                    "entityId": None,
                    "name": name,
                    "type": "person",
                    "role": role or None,
                    "importance": round(float(importance), 4),
                    "salience": round(float(salience), 4),
                    "lastSeenAt": None,
                    "source": "user_model_relationship",
                    "confidence": 0.68,
                    "updatedAt": None,
                    "score": round(float(score), 4),
                    "raw": {},
                }
            )

    candidates.sort(
        key=lambda x: (
            float(x.get("importance") or 0.0),
            float(x.get("salience") or 0.0),
            float(x.get("score") or 0.0),
            _normalize_text(x.get("lastSeenAt")),
            _normalize_text(x.get("name")).lower(),
        ),
        reverse=True,
    )
    return candidates[: max(1, min(int(max_hints or 8), 10))]


def _importance_label(value: Any) -> str:
    try:
        score = float(value or 0.0)
    except Exception:
        return "low"
    if score >= 0.8:
        return "high"
    if score >= 0.55:
        return "medium"
    return "low"


def _trusted_identity_basics(user_model: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    model = user_model if isinstance(user_model, dict) else {}
    basics: Dict[str, Any] = {}
    for key in ("name", "preferred_name", "timezone", "home"):
        value = _normalize_text(model.get(key))
        if value:
            basics[key] = value
    relationships = []
    for rel in _extract_valid_relationship_entities(model, limit=8):
        name = canonicalize_entity_name(rel.get("name"))
        role = _normalize_relationship_role(rel.get("who"))
        if name and role:
            relationships.append({"name": name, "role": role})
    if relationships:
        basics["key_relationships"] = relationships[:5]
    return basics


def _build_recent_high_signal_changes(
    recent_session_summaries: List[Dict[str, Any]],
    loop_items: List[Dict[str, Any]],
    max_items: int = 3,
) -> List[str]:
    changes: List[str] = []
    for summary in recent_session_summaries[:2]:
        text = _normalize_text(summary.get("summary_facts") or summary.get("summary_text") or summary.get("moment"))
        if text and _allow_fact_text(text):
            changes.append(_shorten_line(text, 180))
    for loop in loop_items[:2]:
        text = _normalize_text(loop.get("text"))
        if text:
            reason = _normalize_text(loop.get("reason"))
            if reason:
                changes.append(_shorten_line(f"{text}. {reason}", 180))
            else:
                changes.append(_shorten_line(text, 180))
    return _dedupe_keep_order(changes, limit=max_items)


def _compose_structured_handover_text(
    *,
    depth_label: str,
    time_of_day_label: str,
    time_gap_human: Optional[str],
    identity_basics: Dict[str, Any],
    entity_hints: List[SessionStartBriefEntityHint],
    top_loops: List[Dict[str, Any]],
    recent_changes: List[str],
) -> str:
    intro = _natural_time_phrase(time_of_day_label, time_gap_human)
    if depth_label == "continuation":
        intro += " You last spoke with the user in the current thread."
    elif depth_label == "today":
        intro += " You last spoke with the user earlier today."
    elif depth_label == "yesterday":
        intro += " You last spoke with the user yesterday."
    else:
        intro += " You have not spoken with the user recently."

    parts: List[str] = [intro]
    preferred_name = _normalize_text(identity_basics.get("preferred_name") or identity_basics.get("name"))
    if preferred_name:
        parts.append(f"The user's name is {preferred_name}.")
    if recent_changes:
        parts.append(f"Recent change: {recent_changes[0]}")
    if top_loops:
        top_loop_text = _normalize_text(top_loops[0].get("text"))
        if top_loop_text:
            parts.append(f"Top active loop: {top_loop_text}.")
    if entity_hints:
        top_entities = ", ".join([hint.name for hint in entity_hints[:3] if _normalize_text(hint.name)])
        if top_entities:
            parts.append(f"Key entities in view: {top_entities}.")
    return _compress_handover_text(" ".join([p for p in parts if p]).strip(), depth_label) or " ".join([p for p in parts if p]).strip()


async def _resolve_canonical_entity_candidate(
    tenant_id: str,
    user_id: str,
    reference_time: datetime,
    entity_id: Optional[str],
    name: Optional[str],
    user_model: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    normalized_id = _normalize_text(entity_id)
    normalized_name = canonicalize_entity_name(name)
    exact_candidates = await graphiti_client.get_canonical_entity_nodes(
        tenant_id=tenant_id,
        user_id=user_id,
        name=normalized_name or None,
        entity_id=normalized_id or None,
        limit=5,
        allowed_types=["person", "project", "goal", "loop", "preference", "event"],
    )
    if exact_candidates:
        for node in exact_candidates:
            name_value = canonicalize_entity_name(node.get("summary"))
            if normalized_id and _normalize_text(node.get("uuid")) == normalized_id:
                return {
                    "entityId": _normalize_text(node.get("uuid")) or None,
                    "name": name_value,
                    "type": _normalize_text(node.get("type")) or "other",
                    "role": _entity_role_from_user_model(user_model or {}, name_value),
                    "importance": 0.8,
                    "salience": 0.8,
                    "lastSeenAt": _iso_or_none(_parse_entity_node_ts(node)),
                    "source": "graphiti_exact_node",
                    "confidence": 0.95,
                    "updatedAt": _iso_or_none(_parse_entity_node_ts(node)),
                    "score": 0.95,
                    "raw": node,
                }
            if normalized_name and name_value.lower() == normalized_name.lower():
                return {
                    "entityId": _normalize_text(node.get("uuid")) or None,
                    "name": name_value,
                    "type": _normalize_text(node.get("type")) or "other",
                    "role": _entity_role_from_user_model(user_model or {}, name_value),
                    "importance": 0.8,
                    "salience": 0.8,
                    "lastSeenAt": _iso_or_none(_parse_entity_node_ts(node)),
                    "source": "graphiti_exact_node",
                    "confidence": 0.95,
                    "updatedAt": _iso_or_none(_parse_entity_node_ts(node)),
                    "score": 0.95,
                    "raw": node,
                }

    search_queries = [normalized_name] if normalized_name else []
    if not search_queries:
        search_queries = ["important person project goal loop preference event"]

    direct_candidates: List[Dict[str, Any]] = []
    for query in search_queries[:2]:
        # GRAPHITI_REPLACED: search_nodes (direct entity lookup candidates)
        # TEMPORARY_DEGRADED_REPLACEMENT: direct entity lookup uses continuity nodes (derived) until claim-store retrieval is restored.
        rows = await _pg_search_nodes(
            tenant_id=tenant_id,
            user_id=user_id,
            query=query,
            limit=10,
            reference_time=reference_time,
            allowed_types=["person", "project", "goal", "loop", "preference", "event"],
        )
        direct_candidates.extend([row for row in (rows or []) if isinstance(row, dict)])

    candidates = await _build_entity_candidates(
        tenant_id=tenant_id,
        user_id=user_id,
        reference_time=reference_time,
        user_model=user_model,
        context_texts=[normalized_name] if normalized_name else None,
        max_hints=10,
    )

    if direct_candidates:
        for node in direct_candidates:
            name_value = canonicalize_entity_name(node.get("summary"))
            if normalized_id and _normalize_text(node.get("uuid")) == normalized_id:
                return {
                    "entityId": _normalize_text(node.get("uuid")) or None,
                    "name": name_value,
                    "type": _normalize_text(node.get("type")) or "other",
                    "role": _entity_role_from_user_model(user_model or {}, name_value),
                    "importance": 0.8,
                    "salience": 0.8,
                    "lastSeenAt": _iso_or_none(_parse_entity_node_ts(node)),
                    "source": "graphiti_node",
                    "confidence": 0.9,
                    "updatedAt": _iso_or_none(_parse_entity_node_ts(node)),
                    "score": 0.9,
                    "raw": node,
                }
            if normalized_name and name_value.lower() == normalized_name.lower():
                return {
                    "entityId": _normalize_text(node.get("uuid")) or None,
                    "name": name_value,
                    "type": _normalize_text(node.get("type")) or "other",
                    "role": _entity_role_from_user_model(user_model or {}, name_value),
                    "importance": 0.8,
                    "salience": 0.8,
                    "lastSeenAt": _iso_or_none(_parse_entity_node_ts(node)),
                    "source": "graphiti_node",
                    "confidence": 0.9,
                    "updatedAt": _iso_or_none(_parse_entity_node_ts(node)),
                    "score": 0.9,
                    "raw": node,
                }

    return _resolve_entity_candidate(candidates, entity_id=entity_id, name=normalized_name)


async def _build_narrow_identity_fact_candidates(
    *,
    tenant_id: str,
    user_id: str,
    selected_entity: Dict[str, Any],
    user_model: Optional[Dict[str, Any]],
    limit: int,
) -> List[Dict[str, Any]]:
    canonical_name = canonicalize_entity_name(selected_entity.get("name"))
    entity_id = _normalize_text(selected_entity.get("entityId")) or None
    raw_node = selected_entity.get("raw") if isinstance(selected_entity.get("raw"), dict) else {}
    raw_attrs = raw_node.get("attributes") if isinstance(raw_node.get("attributes"), dict) else {}

    # GRAPHITI_REPLACED: get_entity_role_grounding (entity relationship grounding)
    # TEMPORARY_DEGRADED_REPLACEMENT: role hints are derived and not claim-store-backed.
    role_grounding = await _pg_get_entity_role_grounding(
        tenant_id=tenant_id,
        user_id=user_id,
        name=canonical_name or None,
        entity_id=entity_id,
    ) or {}
    # GRAPHITI_REPLACED: get_entity_facts_exact (exact facts for selected entity)
    # TEMPORARY_DEGRADED_REPLACEMENT: strict factual mode filters out derived continuity rows.
    direct_rows = await _pg_get_entity_facts_exact(
        tenant_id=tenant_id,
        user_id=user_id,
        name=canonical_name or None,
        entity_id=entity_id,
        limit=max(limit * 3, 8),
        include_derived=False,
    )

    target_type = _normalize_text(selected_entity.get("type")).lower()
    role = (
        _normalize_text(role_grounding.get("role"))
        or _normalize_text(role_grounding.get("relationship"))
        or _normalize_text(selected_entity.get("role"))
        or _entity_role_from_user_model(user_model or {}, canonical_name)
    )

    seeded_rows: List[Dict[str, Any]] = []
    if canonical_name and role and target_type == "person":
        seeded_rows.append(
            {
                "text": f"{canonical_name} is the user's {role}.",
                "relevance": 1.0,
                "source": "graphiti_relationship",
                "relevance_tier": "persistent",
                "tenant_id": tenant_id,
            }
        )
    elif canonical_name and _normalize_text(role_grounding.get("edge_name")).upper() == "WORKING_ON":
        seeded_rows.append(
            {
                "text": f"{canonical_name} is an active project the user is working on.",
                "relevance": 1.0,
                "source": "graphiti_relationship",
                "relevance_tier": "persistent",
                "tenant_id": tenant_id,
            }
        )

    profile_summary = _normalize_text(raw_attrs.get("profile_summary"))
    raw_summary = _normalize_text(raw_node.get("summary") or raw_node.get("name"))
    summary_text = profile_summary or raw_summary
    if summary_text and canonical_name and canonical_name.lower() in summary_text.lower() and _allow_fact_text(summary_text):
        seeded_rows.append(
            {
                "text": _shorten_line(summary_text, 220),
                "relevance": 0.92,
                "source": "graphiti_node",
                "relevance_tier": "persistent",
                "tenant_id": tenant_id,
            }
        )

    entity_terms = _query_terms(canonical_name)
    out: List[Dict[str, Any]] = []
    seen = set()

    def _push(row: Dict[str, Any]) -> None:
        text = _normalize_text(row.get("text"))
        if not text:
            return
        key = text.lower()
        if key in seen:
            return
        seen.add(key)
        out.append(row)

    for row in seeded_rows:
        # TEMPORARY_DEGRADED_REPLACEMENT: seeded rows are synthetic continuity hints; excluded from strict factual output.
        if _is_canonical_factual_row(row):
            _push(row)

    for row in direct_rows or []:
        if not isinstance(row, dict):
            continue
        if not _is_canonical_factual_row(row):
            continue
        text = _normalize_text(row.get("text"))
        if not text:
            continue
        if not (_allow_claim(text) and _allow_fact_text(text)):
            continue
        if _is_explicit_user_state_claim(text):
            continue
        if canonical_name.lower() not in text.lower() and (entity_terms and _query_overlap_score(text, entity_terms) < 0.35):
            continue
        _push(
            {
                "text": text,
                "relevance": row.get("relevance") or 1.0,
                "source": _normalize_text(row.get("source")) or "graphiti_exact",
                "relevance_tier": "persistent",
                "tenant_id": tenant_id,
                "source_type": row.get("source_type"),
                "derived": row.get("derived"),
                "evidence_backed": row.get("evidence_backed"),
                "data_classification": row.get("data_classification"),
            }
        )
        if len(out) >= max(1, limit):
            break

    return out[: max(1, limit)]


def _resolve_entity_candidate(
    candidates: List[Dict[str, Any]],
    entity_id: Optional[str],
    name: Optional[str],
) -> Optional[Dict[str, Any]]:
    normalized_id = _normalize_text(entity_id)
    normalized_name = _normalize_text(name)
    if normalized_id:
        for row in candidates:
            if _normalize_text(row.get("entityId")) == normalized_id:
                return row
    if normalized_name:
        target = normalized_name.lower()
        for row in candidates:
            if _normalize_text(row.get("name")).lower() == target:
                return row
        for row in candidates:
            if target in _normalize_text(row.get("name")).lower():
                return row
    return candidates[0] if candidates else None


def _days_since_iso(iso_value: Optional[str], reference_time: datetime) -> Optional[int]:
    dt = _parse_optional_dt(iso_value)
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=dt_timezone.utc)
    ref = reference_time if reference_time.tzinfo else reference_time.replace(tzinfo=dt_timezone.utc)
    return max(0, int((ref.astimezone(dt_timezone.utc) - dt.astimezone(dt_timezone.utc)).total_seconds() // 86400))


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


def _merge_recent_signals(existing: List[Dict[str, Any]], incoming: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    now_dt = datetime.utcnow().replace(tzinfo=dt_timezone.utc)
    index: Dict[str, Dict[str, Any]] = {}

    def _is_expired(row: Dict[str, Any]) -> bool:
        expires = _parse_optional_dt(row.get("expires_at"))
        if not expires:
            return False
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=dt_timezone.utc)
        return expires < now_dt

    for row in (existing or []) + (incoming or []):
        if not isinstance(row, dict):
            continue
        text = _normalize_text(row.get("text"))
        if not text:
            continue
        if _is_expired(row):
            continue
        key = text.lower()
        prev = index.get(key)
        if not prev:
            index[key] = row
            continue
        prev_conf = _extract_confidence(prev, default=0.0)
        next_conf = _extract_confidence(row, default=0.0)
        prev_source = prev.get("source")
        next_source = row.get("source")
        if _should_replace_by_source(
            existing_source=prev_source,
            existing_conf=prev_conf,
            incoming_source=next_source,
            incoming_conf=next_conf,
        ):
            index[key] = row

    values = list(index.values())
    values.sort(
        key=lambda r: (
            _parse_optional_dt(r.get("expires_at")) or datetime.max.replace(tzinfo=dt_timezone.utc),
            _extract_confidence(r, default=0.0),
        ),
        reverse=True,
    )
    return values[:12]


def _merge_daily_anchors(existing: Any, incoming: Any) -> Dict[str, Any]:
    existing_obj = existing if isinstance(existing, dict) else {}
    incoming_obj = incoming if isinstance(incoming, dict) else {}
    merged: Dict[str, Any] = deepcopy(existing_obj)
    for key in ("steps_goal", "minimum_steps"):
        incoming_entry = incoming_obj.get(key)
        if not isinstance(incoming_entry, dict):
            continue
        existing_entry = merged.get(key)
        if isinstance(existing_entry, dict):
            existing_conf = _extract_confidence(existing_entry, default=0.0)
            incoming_conf = _extract_confidence(incoming_entry, default=0.0)
            if not _should_replace_by_source(
                existing_source=existing_entry.get("source"),
                existing_conf=existing_conf,
                incoming_source=incoming_entry.get("source"),
                incoming_conf=incoming_conf,
            ):
                continue
        merged[key] = incoming_entry
    return merged


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


def _source_rank(source: Any) -> int:
    value = _normalize_text(source).lower()
    ranks = {
        "inferred": 10,
        "auto_updater": 10,
        "legacy": 10,
        "llm_session_enricher": 20,
        "manual_patch": 30,
        "user_stated": 30,
    }
    return ranks.get(value, 10)


def _should_replace_by_source(
    existing_source: Any,
    existing_conf: float,
    incoming_source: Any,
    incoming_conf: float
) -> bool:
    existing_rank = _source_rank(existing_source)
    incoming_rank = _source_rank(incoming_source)
    if incoming_rank > existing_rank:
        return True
    if incoming_rank < existing_rank:
        return False
    return incoming_conf >= existing_conf


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

        # Vision: preserve source tiers (user_stated/manual_patch > llm_session_enricher > inferred).
        if incoming_vision:
            if _should_replace_by_source(
                existing_source=existing_vision_source,
                existing_conf=existing_vision_conf,
                incoming_source=incoming_vision_source,
                incoming_conf=incoming_vision_conf,
            ):
                existing_entry["vision"] = incoming_vision
                existing_entry["vision_confidence"] = incoming_vision_conf
                existing_entry["vision_source"] = incoming_vision_source

        # Goal: preserve source tiers (user_stated/manual_patch > llm_session_enricher > inferred).
        if incoming_goal:
            if _should_replace_by_source(
                existing_source=existing_goal_source,
                existing_conf=existing_goal_conf,
                incoming_source=incoming_goal_source,
                incoming_conf=incoming_goal_conf,
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

    for field in (
        "north_star",
        "current_focus",
        "work_context",
        "health",
        "spirituality",
        "preferences",
        "daily_anchors",
        "narrative",
        "narrative_stable",
        "narrative_current",
    ):
        incoming = proposal.get(field)
        if incoming is None:
            continue
        existing = merged.get(field)
        if field == "north_star":
            merged[field] = _merge_north_star(existing, incoming)
            continue
        if field == "daily_anchors":
            merged[field] = _merge_daily_anchors(existing, incoming)
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
        incoming_source = incoming.get("source") if isinstance(incoming, dict) else None
        if not _should_replace_by_source(
            existing_source=existing_source,
            existing_conf=existing_conf,
            incoming_source=incoming_source,
            incoming_conf=incoming_conf,
        ):
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

    if isinstance(proposal.get("recent_signals"), list):
        merged["recent_signals"] = _merge_recent_signals(
            merged.get("recent_signals") if isinstance(merged.get("recent_signals"), list) else [],
            proposal.get("recent_signals") or [],
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
    narrative_stable = _normalize_text(model.get("narrative_stable")) or None
    narrative_current = _normalize_text(model.get("narrative_current")) or None
    return await db.fetchone(
        """
        INSERT INTO user_model (
            tenant_id, user_id, model, version, last_source, created_at, updated_at,
            narrative_stable, narrative_current
        )
        VALUES ($1, $2, $3::jsonb, 1, $4, NOW(), NOW(), $5, $6)
        ON CONFLICT (tenant_id, user_id)
        DO UPDATE SET
            model = $3::jsonb,
            version = user_model.version + 1,
            last_source = $4,
            narrative_stable = COALESCE($5, user_model.narrative_stable),
            narrative_current = COALESCE($6, user_model.narrative_current),
            updated_at = NOW()
        RETURNING model, version, updated_at
        """,
        tenant_id,
        user_id,
        model,
        source,
        narrative_stable,
        narrative_current,
    )


async def _acquire_user_model_write_claim(
    tenant_id: str,
    user_id: str,
    claim_owner: str,
    ttl_seconds: int = 180
) -> bool:
    row = await db.fetchone(
        """
        INSERT INTO user_model_write_claims (
            tenant_id, user_id, claim_owner, claimed_at, expires_at
        )
        VALUES ($1, $2, $3, NOW(), NOW() + ($4::int * INTERVAL '1 second'))
        ON CONFLICT (tenant_id, user_id)
        DO UPDATE SET
            claim_owner = EXCLUDED.claim_owner,
            claimed_at = NOW(),
            expires_at = EXCLUDED.expires_at
        WHERE user_model_write_claims.expires_at <= NOW()
           OR user_model_write_claims.claim_owner = EXCLUDED.claim_owner
        RETURNING claim_owner
        """,
        tenant_id,
        user_id,
        claim_owner,
        max(30, int(ttl_seconds)),
    )
    return bool(row and row.get("claim_owner") == claim_owner)


async def _release_user_model_write_claim(tenant_id: str, user_id: str, claim_owner: str) -> None:
    await db.execute(
        """
        DELETE FROM user_model_write_claims
        WHERE tenant_id = $1 AND user_id = $2 AND claim_owner = $3
        """,
        tenant_id,
        user_id,
        claim_owner,
    )


@asynccontextmanager
async def _user_model_write_claim(
    tenant_id: str,
    user_id: str,
    ttl_seconds: int = 180
):
    claim_owner = uuid.uuid4().hex
    acquired = False
    try:
        acquired = await _acquire_user_model_write_claim(
            tenant_id=tenant_id,
            user_id=user_id,
            claim_owner=claim_owner,
            ttl_seconds=ttl_seconds,
        )
        yield acquired
    finally:
        if acquired:
            try:
                await _release_user_model_write_claim(
                    tenant_id=tenant_id,
                    user_id=user_id,
                    claim_owner=claim_owner,
                )
            except Exception:
                pass


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
            async with _user_model_write_claim(tenant_id=tenant_id, user_id=user_id) as claimed:
                if not claimed:
                    continue
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
                    SELECT model, narrative_stable, narrative_current
                    FROM user_model
                    WHERE tenant_id = $1 AND user_id = $2
                    """,
                    tenant_id,
                    user_id
                )
                current = _hydrate_user_model_narratives(
                    existing.get("model") if existing else None,
                    row=existing if isinstance(existing, dict) else None,
                )
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


def _coerce_enrichment_confidence(value: Any, default: float = 0.0) -> float:
    return _normalize_confidence(value, default=default)


def _extract_session_summary_evidence_rows(
    nodes: List[Dict[str, Any]],
    window_start: datetime,
    window_end: datetime
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for raw in nodes or []:
        normalized = _normalize_startbrief_session_summary_node(raw)
        if not normalized:
            continue
        created_at = _normalize_text(normalized.get("created_at"))
        created_dt = _parse_optional_dt(created_at)
        if created_dt and created_dt.tzinfo is None:
            created_dt = created_dt.replace(tzinfo=dt_timezone.utc)
        if created_dt and not (window_start <= created_dt <= window_end):
            continue
        evidence = {
            "session_id": _normalize_text(normalized.get("session_id")),
            "created_at": created_at,
            "summary_facts": _normalize_text(normalized.get("summary_facts")),
            "tone": _normalize_text(normalized.get("tone")),
            "moment": _normalize_text(normalized.get("moment")),
            "decisions": normalized.get("decisions") if isinstance(normalized.get("decisions"), list) else [],
            "unresolved": normalized.get("unresolved") if isinstance(normalized.get("unresolved"), list) else [],
            "bridge_text": _normalize_text(normalized.get("bridge_text")),
            "salience": _normalize_text(normalized.get("salience")) or "low",
        }
        if any(_normalize_text(evidence.get(k)) for k in ("summary_facts", "tone", "moment", "bridge_text")):
            rows.append(evidence)
    return rows


async def _get_transcript_fallback_sessions(
    tenant_id: str,
    user_id: str,
    window_start: datetime,
    window_end: datetime,
    limit: int = 10
) -> List[Dict[str, Any]]:
    rows = await db.fetch(
        """
        SELECT session_id, messages, updated_at
        FROM session_transcript
        WHERE tenant_id = $1
          AND user_id = $2
          AND updated_at >= $3
          AND updated_at <= $4
        ORDER BY updated_at DESC
        LIMIT $5
        """,
        tenant_id,
        user_id,
        window_start,
        window_end,
        limit
    )
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        messages = row.get("messages")
        if isinstance(messages, str):
            try:
                messages = json.loads(messages)
            except Exception:
                messages = []
        if not isinstance(messages, list):
            continue
        out.append(
            {
                "session_id": _normalize_text(row.get("session_id")),
                "updated_at": row.get("updated_at").isoformat() if row.get("updated_at") else None,
                "messages": [
                    {
                        "role": _normalize_text(m.get("role")).lower(),
                        "text": _normalize_text(m.get("text")),
                        "timestamp": _normalize_text(m.get("timestamp")) or None,
                    }
                    for m in messages
                    if isinstance(m, dict) and _normalize_text(m.get("text"))
                ][:40],
            }
        )
    return out


async def _load_enrichment_transcript_inputs(
    tenant_id: str,
    user_id: str,
    window_start: datetime,
    window_end: datetime,
    session_index: List[Dict[str, Any]],
    max_sessions: int = 16,
    max_user_turns: int = 180,
    max_chars: int = 12000
) -> List[Dict[str, Any]]:
    rows = await db.fetch(
        """
        SELECT session_id, messages, updated_at
        FROM session_transcript
        WHERE tenant_id = $1
          AND user_id = $2
          AND updated_at >= $3
          AND updated_at <= $4
        ORDER BY updated_at DESC
        LIMIT $5
        """,
        tenant_id,
        user_id,
        window_start,
        window_end,
        max(max_sessions * 8, 80),
    )
    summary_rank: Dict[str, int] = {}
    for idx, item in enumerate(session_index or []):
        sid = _normalize_text(item.get("session_id"))
        if sid and sid not in summary_rank:
            summary_rank[sid] = idx

    normalized_rows: List[Dict[str, Any]] = []
    for row in rows or []:
        session_id = _normalize_text(row.get("session_id"))
        messages = row.get("messages")
        if isinstance(messages, str):
            try:
                messages = json.loads(messages)
            except Exception:
                messages = []
        if not isinstance(messages, list):
            continue
        user_turns: List[Dict[str, Any]] = []
        for msg_index, msg in enumerate(messages):
            if not isinstance(msg, dict):
                continue
            if _normalize_text(msg.get("role")).lower() != "user":
                continue
            text = _normalize_text(msg.get("text"))
            if not text:
                continue
            user_turns.append(
                {
                    "msg_index": msg_index,
                    "timestamp": _normalize_text(msg.get("timestamp")) or None,
                    "text": text,
                }
            )
        if not user_turns:
            continue
        rank = summary_rank.get(session_id, 10_000)
        updated_at = row.get("updated_at")
        updated_at_iso = updated_at.isoformat() if updated_at else None
        normalized_rows.append(
            {
                "session_id": session_id,
                "updated_at": updated_at_iso,
                "summary_rank": rank,
                "user_turns": user_turns,
            }
        )

    normalized_rows.sort(
        key=lambda r: (
            int(r.get("summary_rank") if r.get("summary_rank") is not None else 10_000),
            _normalize_text(r.get("updated_at")),
        ),
        reverse=False,
    )

    selected: List[Dict[str, Any]] = []
    total_turns = 0
    total_chars = 0
    for row in normalized_rows:
        if len(selected) >= max_sessions:
            break
        capped_turns: List[Dict[str, Any]] = []
        for turn in row.get("user_turns") or []:
            text = _normalize_text(turn.get("text"))
            if not text:
                continue
            projected_turns = total_turns + 1
            projected_chars = total_chars + len(text)
            if projected_turns > max_user_turns or projected_chars > max_chars:
                break
            capped_turns.append(
                {
                    "msg_index": int(turn.get("msg_index") or 0),
                    "timestamp": _normalize_text(turn.get("timestamp")) or None,
                    "text": text[:400],
                }
            )
            total_turns = projected_turns
            total_chars = projected_chars
        if not capped_turns:
            continue
        selected.append(
            {
                "session_id": _normalize_text(row.get("session_id")),
                "updated_at": _normalize_text(row.get("updated_at")) or None,
                "user_turns": capped_turns,
            }
        )
        if total_turns >= max_user_turns or total_chars >= max_chars:
            break
    return selected


def _normalize_enrichment_evidence(payload: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return None
    session_id = _normalize_text(payload.get("session_id"))
    quote = _normalize_text(payload.get("quote"))
    timestamp = _normalize_text(payload.get("timestamp"))
    msg_index_raw = payload.get("msg_index")
    msg_index: Optional[int] = None
    try:
        if msg_index_raw is not None:
            msg_index = int(msg_index_raw)
    except Exception:
        msg_index = None
    if not session_id or not quote:
        return None
    if msg_index is None and not timestamp:
        return None
    evidence: Dict[str, Any] = {
        "session_id": session_id,
        "quote": quote[:240],
    }
    if msg_index is not None and msg_index >= 0:
        evidence["msg_index"] = msg_index
    if timestamp:
        evidence["timestamp"] = timestamp
    return evidence


def _normalize_enrichment_evidence_list(payload: Any) -> List[Dict[str, Any]]:
    if not isinstance(payload, list):
        return []
    out: List[Dict[str, Any]] = []
    seen = set()
    for item in payload:
        normalized = _normalize_enrichment_evidence(item)
        if not normalized:
            continue
        key = (
            normalized.get("session_id"),
            normalized.get("msg_index"),
            normalized.get("timestamp"),
            normalized.get("quote"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(normalized)
    return out


def _looks_like_single_action_pattern(text: str) -> bool:
    lower = _normalize_text(text).lower()
    if not lower:
        return True
    one_off_patterns = (
        r"^\b(went|go|going|did|made|tidied|cleaned|walked|ran|finished|completed)\b",
        r"\b(today|yesterday|this morning|this evening)\b",
    )
    return any(re.search(p, lower) for p in one_off_patterns)


def _looks_like_daily_anchor_signal(text: str) -> bool:
    lower = _normalize_text(text).lower()
    if not lower:
        return False
    return bool(re.search(r"\b\d{1,3}(?:,\d{3})?\s*(?:k|steps?)\b", lower)) or "steps goal" in lower


def _extract_steps_anchor_value(text: str) -> Optional[int]:
    lower = _normalize_text(text).lower().replace(",", "")
    if not lower:
        return None
    m = re.search(r"\b(\d{1,3})\s*k\b", lower)
    if m:
        return int(m.group(1)) * 1000
    m = re.search(r"\b(\d{3,6})\s*steps?\b", lower)
    if m:
        return int(m.group(1))
    return None


def _coerce_small_human_details(
    payload: Any,
    min_confidence: float,
    source: str,
    now_iso: str
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in payload or []:
        if not isinstance(row, dict):
            continue
        text = _normalize_text(row.get("text"))
        conf = _coerce_enrichment_confidence(row.get("confidence"), default=0.0)
        evidence = _normalize_enrichment_evidence(row.get("evidence"))
        if not text or conf < min_confidence or not evidence:
            continue
        if _looks_like_daily_anchor_signal(text):
            continue
        out.append(
            {
                "text": text[:200],
                "confidence": conf,
                "source": source,
                "updated_at": now_iso,
                "evidence": evidence,
            }
        )
    return out


def _coerce_enrichment_relationships(
    payload: Any,
    min_confidence: float,
    source: str,
    now_iso: str
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in payload or []:
        if not isinstance(row, dict):
            continue
        name = _normalize_text(row.get("name"))
        who = _normalize_text(row.get("who")).lower()
        status = _normalize_text(row.get("status")).lower() or "active"
        conf = _coerce_enrichment_confidence(row.get("confidence"), default=0.0)
        evidence = _normalize_enrichment_evidence(row.get("evidence"))
        if not name or not who or conf < min_confidence or not evidence:
            continue
        if not _build_relationship_signal_text(name, who, status):
            continue
        out.append(
            {
                "name": name[:80],
                "who": who[:40],
                "status": status[:40],
                "confidence": conf,
                "source": source,
                "updated_at": now_iso,
                "evidence": evidence,
            }
        )
    return out


def _coerce_enrichment_patterns(
    payload: Any,
    min_confidence: float,
    source: str,
    now_iso: str
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in payload or []:
        if not isinstance(row, dict):
            continue
        text = _normalize_text(row.get("text"))
        conf = _coerce_enrichment_confidence(row.get("confidence"), default=0.0)
        habitual_explicit = bool(row.get("habitual_explicit"))
        evidences = _normalize_enrichment_evidence_list(row.get("evidences"))
        if not evidences:
            single = _normalize_enrichment_evidence(row.get("evidence"))
            if single:
                evidences = [single]
        evidence_sessions = {e.get("session_id") for e in evidences if _normalize_text(e.get("session_id"))}
        if not text or conf < min_confidence:
            continue
        if _looks_like_single_action_pattern(text):
            continue
        if not habitual_explicit and len(evidence_sessions) < 2:
            continue
        out.append(
            {
                "text": text[:160],
                "confidence": conf,
                "source": source,
                "updated_at": now_iso,
                "evidence": evidences[:3],
                "habitual_explicit": habitual_explicit,
            }
        )
    return out


def _coerce_enrichment_scalar_field(
    payload: Any,
    min_confidence: float,
    source: str,
    now_iso: str
) -> Optional[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return None
    text = _normalize_text(payload.get("text"))
    conf = _coerce_enrichment_confidence(payload.get("confidence"), default=0.0)
    evidence = _normalize_enrichment_evidence(payload.get("evidence"))
    if not text or conf < min_confidence or not evidence:
        return None
    return {
        "text": text[:180],
        "confidence": conf,
        "source": source,
        "updated_at": now_iso,
        "evidence": evidence,
    }


def _coerce_enrichment_north_star(
    payload: Any,
    min_confidence: float,
    source: str,
    now_iso: str
) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not isinstance(payload, dict):
        return out
    for domain in ("relationships", "work", "health", "spirituality", "general"):
        entry = payload.get(domain)
        if not isinstance(entry, dict):
            continue
        vision = _normalize_text(entry.get("vision"))
        goal = _normalize_text(entry.get("goal"))
        vision_conf = _coerce_enrichment_confidence(entry.get("vision_confidence"), default=0.0)
        goal_conf = _coerce_enrichment_confidence(entry.get("goal_confidence"), default=0.0)
        normalized_entry: Dict[str, Any] = {"status": "active", "updated_at": now_iso}
        vision_evidence = _normalize_enrichment_evidence(entry.get("vision_evidence"))
        goal_evidence = _normalize_enrichment_evidence(entry.get("goal_evidence"))
        if vision and vision_conf >= min_confidence and vision_evidence:
            normalized_entry.update(
                {
                    "vision": vision[:180],
                    "vision_confidence": vision_conf,
                    "vision_source": source,
                    "vision_evidence": vision_evidence,
                }
            )
        if goal and goal_conf >= min_confidence and (goal_evidence or "vision" in normalized_entry):
            if goal_evidence:
                normalized_entry["goal_evidence"] = goal_evidence
            normalized_entry.update(
                {
                    "goal": goal[:180],
                    "goal_confidence": goal_conf,
                    "goal_source": source,
                }
            )
        if any(k in normalized_entry for k in ("vision", "goal")):
            out[domain] = normalized_entry
    return out


def _coerce_recent_signals(
    payload: Any,
    min_confidence: float,
    source: str,
    now_iso: str
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    now_dt = _parse_optional_dt(now_iso) or datetime.utcnow().replace(tzinfo=dt_timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=dt_timezone.utc)
    for row in payload or []:
        if not isinstance(row, dict):
            continue
        text = _normalize_text(row.get("text"))
        conf = _coerce_enrichment_confidence(row.get("confidence"), default=0.0)
        evidence = _normalize_enrichment_evidence(row.get("evidence"))
        if not text or conf < min_confidence or not evidence:
            continue
        expires_at = _parse_optional_dt(row.get("expires_at"))
        if not expires_at:
            expires_at = now_dt + timedelta(days=7)
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=dt_timezone.utc)
        if expires_at < now_dt:
            continue
        out.append(
            {
                "text": text[:200],
                "confidence": conf,
                "source": source,
                "updated_at": now_iso,
                "expires_at": expires_at.isoformat(),
                "evidence": evidence,
            }
        )
    return out


def _coerce_daily_anchors(
    payload: Any,
    min_confidence: float,
    source: str,
    now_iso: str,
    small_human_details: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    payload_obj = payload if isinstance(payload, dict) else {}

    def _apply_anchor(anchor_key: str, value: Any, confidence: Any, evidence_payload: Any) -> None:
        try:
            numeric = int(value)
        except Exception:
            return
        if numeric <= 0 or numeric > 250000:
            return
        conf = _coerce_enrichment_confidence(confidence, default=0.0)
        evidence = _normalize_enrichment_evidence(evidence_payload)
        if conf < min_confidence or not evidence:
            return
        out[anchor_key] = {
            "value": numeric,
            "confidence": conf,
            "source": source,
            "updated_at": now_iso,
            "evidence": evidence,
        }

    _apply_anchor(
        "steps_goal",
        payload_obj.get("steps_goal"),
        payload_obj.get("steps_goal_confidence"),
        payload_obj.get("steps_goal_evidence"),
    )
    _apply_anchor(
        "minimum_steps",
        payload_obj.get("minimum_steps"),
        payload_obj.get("minimum_steps_confidence"),
        payload_obj.get("minimum_steps_evidence"),
    )

    for row in small_human_details or []:
        text = _normalize_text(row.get("text"))
        if not text or not _looks_like_daily_anchor_signal(text):
            continue
        value = _extract_steps_anchor_value(text)
        if value is None:
            continue
        conf = _coerce_enrichment_confidence(row.get("confidence"), default=0.0)
        evidence = _normalize_enrichment_evidence(row.get("evidence"))
        if conf < min_confidence or not evidence:
            continue
        key = "steps_goal" if ("goal" in text.lower() or value >= 8000) else "minimum_steps"
        if key not in out:
            out[key] = {
                "value": int(value),
                "confidence": conf,
                "source": source,
                "updated_at": now_iso,
                "evidence": evidence,
            }
    return out


def _proposal_from_enrichment_payload(
    payload: Dict[str, Any],
    min_confidence: float,
    source: str,
    now_iso: str
) -> Dict[str, Any]:
    proposal: Dict[str, Any] = {}

    relationships = _coerce_enrichment_relationships(
        payload.get("key_relationships"), min_confidence=min_confidence, source=source, now_iso=now_iso
    )
    if relationships:
        proposal["key_relationships"] = relationships

    patterns = _coerce_enrichment_patterns(
        payload.get("patterns"), min_confidence=min_confidence, source=source, now_iso=now_iso
    )
    if patterns:
        proposal["patterns"] = patterns

    recent_signals = _coerce_recent_signals(
        payload.get("recent_signals"), min_confidence=min_confidence, source=source, now_iso=now_iso
    )
    if recent_signals:
        proposal["recent_signals"] = recent_signals

    for key, target in (
        ("current_focus", "current_focus"),
        ("work_context", "work_context"),
        ("health", "health"),
        ("spirituality", "spirituality"),
    ):
        normalized = _coerce_enrichment_scalar_field(
            payload.get(key), min_confidence=min_confidence, source=source, now_iso=now_iso
        )
        if normalized:
            proposal[target] = normalized

    preferences_notes = _coerce_small_human_details(
        payload.get("small_human_details"), min_confidence=min_confidence, source=source, now_iso=now_iso
    )
    daily_anchors = _coerce_daily_anchors(
        payload.get("daily_anchors"),
        min_confidence=min_confidence,
        source=source,
        now_iso=now_iso,
        small_human_details=payload.get("small_human_details") if isinstance(payload.get("small_human_details"), list) else [],
    )
    if daily_anchors:
        proposal["daily_anchors"] = daily_anchors

    if preferences_notes:
        notes = [_normalize_text(x.get("text"))[:200] for x in preferences_notes if _normalize_text(x.get("text"))]
        if notes:
            proposal["preferences"] = {
                "notes": notes[:4],
                "confidence": max(min_confidence, 0.72),
                "source": source,
                "updated_at": now_iso,
                "evidence": [x.get("evidence") for x in preferences_notes[:4] if isinstance(x.get("evidence"), dict)],
            }

    north_star = _coerce_enrichment_north_star(
        payload.get("north_star"), min_confidence=min_confidence, source=source, now_iso=now_iso
    )
    if north_star:
        proposal["north_star"] = north_star

    return proposal


async def _generate_user_model_enrichment_proposal(
    tenant_id: str,
    user_id: str,
    mode: str,
    window_start: datetime,
    window_end: datetime,
    session_summaries: List[Dict[str, Any]],
    min_confidence: float
) -> Dict[str, Any]:
    transcript_inputs = await _load_enrichment_transcript_inputs(
        tenant_id=tenant_id,
        user_id=user_id,
        window_start=window_start,
        window_end=window_end,
        session_index=session_summaries,
        max_sessions=14 if mode == "daily" else 28,
        max_user_turns=160 if mode == "daily" else 260,
        max_chars=10000 if mode == "daily" else 18000,
    )
    if not transcript_inputs:
        return {}
    llm = get_llm_client()
    prompt = (
        "You are enriching a long-lived user profile from memory evidence.\n"
        "Return strict JSON only with keys:\n"
        "{"
        "\"key_relationships\": [], "
        "\"patterns\": [], "
        "\"recent_signals\": [], "
        "\"daily_anchors\": {}, "
        "\"current_focus\": null, "
        "\"work_context\": null, "
        "\"health\": null, "
        "\"spirituality\": null, "
        "\"small_human_details\": [], "
        "\"north_star\": {}"
        "}\n"
        "Rules:\n"
        "- Use only direct evidence from raw transcript user turns.\n"
        "- Session summaries are index metadata only; do not treat as evidence.\n"
        "- If category has no direct evidence, return empty list or null.\n"
        "- No hallucinations. Keep updates concise.\n"
        "- Include confidence 0..1 for every extracted item/object.\n"
        "- PATTERN hard criteria:\n"
        "  * A pattern must be repeating behavior or stable trait over time.\n"
        "  * Must be supported by >=2 evidence quotes from different sessions OR be explicitly habitual (\"I always\", \"I tend to\", \"I usually\", \"I keep\").\n"
        "  * Must NOT be a single action/event (example rejects: \"went for a walk\", \"tidied\").\n"
        "  * If criteria fail, output no pattern item.\n"
        "- Every extracted item MUST include evidence: {session_id, msg_index or timestamp, quote}.\n"
        "- For patterns: include `evidences` array (2+ if not habitual) and `habitual_explicit` boolean.\n"
        "- For key_relationships: include an `evidence` object.\n"
        "- For recent_signals: include {text, confidence, evidence, expires_at}.\n"
        "- For daily_anchors: include {steps_goal?, minimum_steps?} plus confidence/evidence per key.\n"
        "- Step targets/habit anchors (e.g., 5k/10k steps) must be returned under daily_anchors, not preferences.\n"
        "- For current_focus/work_context/health/spirituality objects: include `evidence`.\n"
        "- For small_human_details: return objects {text, confidence, evidence}.\n"
        "- For north_star domain entries: include `vision_evidence` for vision and `goal_evidence` for goal.\n"
        "- Prefer durable personal signals over one-off trivia.\n\n"
        f"MODE: {mode}\n"
        f"WINDOW_START: {window_start.isoformat()}\n"
        f"WINDOW_END: {window_end.isoformat()}\n"
        f"MIN_CONFIDENCE_FOR_WRITE: {min_confidence}\n\n"
        f"SESSION_INDEX_JSON (ranking-only):\n{json.dumps(session_summaries, ensure_ascii=True)}\n\n"
        f"RAW_TRANSCRIPT_USER_TURNS_JSON:\n{json.dumps(transcript_inputs, ensure_ascii=True)}\n"
    )
    raw = await llm._call_llm(
        prompt=prompt,
        max_tokens=900,
        temperature=0.1,
        task="user_model_enrichment",
    )
    payload = _safe_parse_json_object(raw) if raw else None
    if not payload:
        return {}
    proposal = _proposal_from_enrichment_payload(
        payload=payload,
        min_confidence=min_confidence,
        source="llm_session_enricher",
        now_iso=datetime.utcnow().isoformat(),
    )
    try:
        existing = await db.fetchone(
            """
            SELECT model, narrative_stable, narrative_current
            FROM user_model
            WHERE tenant_id = $1 AND user_id = $2
            """,
            tenant_id,
            user_id,
        )
        current_model = _hydrate_user_model_narratives(
            existing.get("model") if existing else None,
            row=existing if isinstance(existing, dict) else None,
        )
        top_loops = await loops.get_top_loops_for_startbrief(
            tenant_id=tenant_id,
            user_id=user_id,
            limit=5,
        )
        latest_daily = await db.fetchone(
            """
            SELECT analysis_date, themes, scores, steering_note, confidence, updated_at
            FROM daily_analysis
            WHERE tenant_id = $1 AND user_id = $2
            ORDER BY analysis_date DESC
            LIMIT 1
            """,
            tenant_id,
            user_id,
        )
        loops_payload = [
            {
                "type": _normalize_text(getattr(loop_item, "type", "")),
                "text": _normalize_text(getattr(loop_item, "text", "")),
                "status": _normalize_text(getattr(loop_item, "status", "")),
                "time_horizon": _normalize_text(getattr(loop_item, "timeHorizon", "")),
                "salience": getattr(loop_item, "salience", None),
            }
            for loop_item in (top_loops or [])
            if _normalize_text(getattr(loop_item, "text", ""))
        ]
        reference_now_utc = window_end if window_end.tzinfo else window_end.replace(tzinfo=dt_timezone.utc)
        active_relationship_names = _extract_active_relationship_names(current_model)
        summary_texts_for_scoring = [_summary_text_for_scoring(s) for s in (session_summaries or []) if _summary_text_for_scoring(s)]
        loop_texts_for_scoring = [_normalize_text(l.get("text")) for l in loops_payload if _normalize_text(l.get("text"))]
        has_recent_reconciliation_signal = any(
            re.search(r"\b(reconciled|back together|got back together)\b", text.lower())
            for text in summary_texts_for_scoring
        )
        scored_session_rows: List[Dict[str, Any]] = []
        for row in session_summaries or []:
            metrics = _score_startbrief_summary(
                summary=row,
                reference_now_utc=reference_now_utc,
                active_relationship_names=active_relationship_names,
                has_recent_reconciliation_signal=has_recent_reconciliation_signal,
                all_summary_texts=summary_texts_for_scoring,
                loop_texts=loop_texts_for_scoring,
            )
            scored_session_rows.append(
                {
                    **metrics,
                    "summary_facts": _normalize_text(row.get("summary_facts"))[:220],
                    "created_at": _normalize_text(row.get("created_at")) or None,
                }
            )
        scored_session_rows.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
        scored_loop_rows: List[Dict[str, Any]] = []
        for row in loops_payload:
            metrics = _score_startbrief_loop(
                loop={
                    "id": None,
                    "text": row.get("text"),
                    "type": row.get("type"),
                    "salience": row.get("salience"),
                    "lastSeenAt": None,
                    "timeHorizon": row.get("time_horizon"),
                    "confidence": None,
                },
                reference_now_utc=reference_now_utc,
                has_recent_reconciliation_signal=has_recent_reconciliation_signal,
            )
            scored_loop_rows.append(metrics)
        scored_loop_rows.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
        daily_payload: Dict[str, Any] = {}
        if latest_daily:
            daily_payload = {
                "analysis_date": (
                    latest_daily.get("analysis_date").isoformat()
                    if latest_daily.get("analysis_date") is not None
                    else None
                ),
                "themes": latest_daily.get("themes") or [],
                "scores": latest_daily.get("scores") or {},
                "steering_note": _normalize_text(latest_daily.get("steering_note")),
                "confidence": latest_daily.get("confidence"),
                "updated_at": (
                    latest_daily.get("updated_at").isoformat()
                    if latest_daily.get("updated_at") is not None
                    else None
                ),
            }
        narrative_prompt = (
            "Write/maintain two narrative sections for this user.\n"
            "Return strict JSON only:\n"
            "{\"narrative_stable\":\"...\",\"narrative_current\":\"...\"}\n\n"
            "Rules:\n"
            "- narrative_stable: long-lived identity/background facts only. Update only when genuinely new permanent facts emerge.\n"
            "- narrative_stable: never use time-relative language (no recently, currently, today, this week).\n"
            "- narrative_current: actively refresh from recent sessions and recent evidence.\n"
            "- narrative_current: include approximate age markers for time-relative facts when possible (e.g., \"about 2 weeks ago\").\n"
            "- narrative_current: after ~30 days, drop stale items unless still evidenced; move to narrative_stable only if clearly durable.\n"
            "- Never assume current facts are still true without recent evidence.\n"
            "- Precedence rule: prefer higher score evidence from SCORED_SESSION_CLAIMS_JSON and SCORED_LOOP_CLAIMS_JSON.\n"
            "- If newer evidence contradicts older relationship status, keep newer evidence and drop older claim from narrative_current.\n"
            "- Describe the person only; do not give instructions/advice.\n\n"
            f"WINDOW_START: {window_start.isoformat()}\n"
            f"WINDOW_END: {window_end.isoformat()}\n\n"
            f"CURRENT_USER_MODEL_JSON:\n{json.dumps(current_model, ensure_ascii=True)}\n\n"
            f"SCORED_SESSION_CLAIMS_JSON:\n{json.dumps(scored_session_rows[:8], ensure_ascii=True)}\n\n"
            f"SCORED_LOOP_CLAIMS_JSON:\n{json.dumps(scored_loop_rows[:8], ensure_ascii=True)}\n\n"
            f"SESSION_INDEX_JSON:\n{json.dumps(session_summaries, ensure_ascii=True)}\n\n"
            f"TOP_5_LOOPS_JSON:\n{json.dumps(loops_payload, ensure_ascii=True)}\n\n"
            f"LATEST_DAILY_ANALYSIS_JSON:\n{json.dumps(daily_payload, ensure_ascii=True)}\n"
        )
        narrative_raw = await llm._call_llm(
            prompt=narrative_prompt,
            max_tokens=520,
            temperature=0.1,
            task="user_model_enrichment",
        )
        narrative_payload = _safe_parse_json_object(narrative_raw) if narrative_raw else None
        if isinstance(narrative_payload, dict):
            narrative_stable = _normalize_text(narrative_payload.get("narrative_stable"))
            narrative_current = _normalize_text(narrative_payload.get("narrative_current"))
            narrative_stable = _sanitize_enrichment_narrative_stable(
                narrative_stable=narrative_stable,
                current_model=current_model,
            ) or ""
            narrative_current = _sanitize_enrichment_narrative_current(
                narrative_current=narrative_current,
                session_summaries=session_summaries,
                loop_texts=loop_texts_for_scoring,
                current_model=current_model,
                reference_now_utc=reference_now_utc,
            ) or ""
            if narrative_stable:
                proposal["narrative_stable"] = narrative_stable[:2000]
            if narrative_current:
                proposal["narrative_current"] = narrative_current[:2000]
            combined = _normalize_text(" ".join([narrative_stable, narrative_current]).strip())
            if combined:
                proposal["narrative"] = combined[:1200]
    except Exception as e:
        logger.warning("user model enrichment narrative generation failed tenant=%s user=%s error=%s", tenant_id, user_id, e)
    return proposal


async def _update_enrichment_state_success(tenant_id: str, user_id: str, mode: str) -> None:
    await db.execute(
        """
        INSERT INTO user_model_enrichment_state (
            tenant_id, user_id, last_enriched_at, last_daily_enriched_at, last_weekly_enriched_at,
            next_retry_at, retry_attempts, last_error, last_mode, updated_at
        )
        VALUES (
            $1, $2, NOW(),
            CASE WHEN $3 = 'daily' THEN NOW() ELSE NULL END,
            CASE WHEN $3 IN ('weekly', 'backfill') THEN NOW() ELSE NULL END,
            NULL, 0, NULL, $3, NOW()
        )
        ON CONFLICT (tenant_id, user_id)
        DO UPDATE SET
            last_enriched_at = NOW(),
            last_daily_enriched_at = CASE WHEN $3 = 'daily' THEN NOW() ELSE user_model_enrichment_state.last_daily_enriched_at END,
            last_weekly_enriched_at = CASE WHEN $3 IN ('weekly', 'backfill') THEN NOW() ELSE user_model_enrichment_state.last_weekly_enriched_at END,
            next_retry_at = NULL,
            retry_attempts = 0,
            last_error = NULL,
            last_mode = $3,
            updated_at = NOW()
        """,
        tenant_id,
        user_id,
        mode,
    )


async def _update_enrichment_state_failure(
    tenant_id: str,
    user_id: str,
    mode: str,
    error: str,
    base_backoff_seconds: int,
    max_backoff_seconds: int
) -> None:
    row = await db.fetchone(
        """
        SELECT retry_attempts
        FROM user_model_enrichment_state
        WHERE tenant_id = $1 AND user_id = $2
        """,
        tenant_id,
        user_id,
    )
    attempts = int((row or {}).get("retry_attempts") or 0) + 1
    backoff = min(max_backoff_seconds, max(base_backoff_seconds, base_backoff_seconds * (2 ** (attempts - 1))))
    await db.execute(
        """
        INSERT INTO user_model_enrichment_state (
            tenant_id, user_id, next_retry_at, retry_attempts, last_error, last_mode, updated_at
        )
        VALUES ($1, $2, NOW() + ($3::int * INTERVAL '1 second'), $4, $5, $6, NOW())
        ON CONFLICT (tenant_id, user_id)
        DO UPDATE SET
            next_retry_at = NOW() + ($3::int * INTERVAL '1 second'),
            retry_attempts = $4,
            last_error = $5,
            last_mode = $6,
            updated_at = NOW()
        """,
        tenant_id,
        user_id,
        backoff,
        attempts,
        _normalize_text(error)[:1000],
        mode,
    )


async def _fetch_enrichment_candidates(
    mode: str,
    max_users: int,
    daily_lookback_hours: int,
    weekly_lookback_days: int
) -> List[Dict[str, Any]]:
    if mode == "daily":
        return await db.fetch(
            """
            WITH active_users AS (
                SELECT tenant_id, user_id, MAX(updated_at) AS last_activity
                FROM session_transcript
                WHERE updated_at >= NOW() - ($1::int * INTERVAL '1 hour')
                GROUP BY tenant_id, user_id
            )
            SELECT a.tenant_id, a.user_id
            FROM active_users a
            LEFT JOIN user_model_enrichment_state s
              ON s.tenant_id = a.tenant_id AND s.user_id = a.user_id
            WHERE (s.next_retry_at IS NULL OR s.next_retry_at <= NOW())
              AND (
                s.last_daily_enriched_at IS NULL
                OR s.last_daily_enriched_at <= NOW() - INTERVAL '20 hours'
              )
            ORDER BY a.last_activity DESC
            LIMIT $2
            """,
            daily_lookback_hours,
            max_users,
        )
    if mode == "weekly":
        return await db.fetch(
            """
            WITH active_users AS (
                SELECT tenant_id, user_id, MAX(updated_at) AS last_activity
                FROM session_transcript
                WHERE updated_at >= NOW() - ($1::int * INTERVAL '1 day')
                GROUP BY tenant_id, user_id
            )
            SELECT a.tenant_id, a.user_id
            FROM active_users a
            LEFT JOIN user_model_enrichment_state s
              ON s.tenant_id = a.tenant_id AND s.user_id = a.user_id
            WHERE (s.next_retry_at IS NULL OR s.next_retry_at <= NOW())
              AND (
                s.last_weekly_enriched_at IS NULL
                OR s.last_weekly_enriched_at <= NOW() - INTERVAL '6 days'
              )
            ORDER BY a.last_activity DESC
            LIMIT $2
            """,
            weekly_lookback_days,
            max_users,
        )
    return []


async def _run_user_model_enrichment_for_user(
    tenant_id: str,
    user_id: str,
    mode: str,
    min_confidence: float,
    retry_backoff_seconds: int,
    retry_max_seconds: int,
    now: Optional[datetime] = None
) -> bool:
    now_dt = now or datetime.utcnow().replace(tzinfo=dt_timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=dt_timezone.utc)
    if mode == "daily":
        window_start = now_dt - timedelta(hours=24)
    elif mode == "weekly":
        window_start = now_dt - timedelta(days=7)
    else:
        window_start = datetime(1970, 1, 1, tzinfo=dt_timezone.utc)
    window_end = now_dt

    try:
        async with _user_model_write_claim(tenant_id=tenant_id, user_id=user_id, ttl_seconds=240) as claimed:
            if not claimed:
                return False
            session_nodes = await graphiti_client.get_recent_session_summary_nodes(
                tenant_id=tenant_id,
                user_id=user_id,
                limit=60 if mode == "daily" else 200,
            )
            session_summaries = _extract_session_summary_evidence_rows(
                nodes=session_nodes or [],
                window_start=window_start,
                window_end=window_end,
            )

            proposal = await _generate_user_model_enrichment_proposal(
                tenant_id=tenant_id,
                user_id=user_id,
                mode=mode,
                window_start=window_start,
                window_end=window_end,
                session_summaries=session_summaries,
                min_confidence=min_confidence,
            )
            existing = await db.fetchone(
                """
                SELECT model, narrative_stable, narrative_current
                FROM user_model
                WHERE tenant_id = $1 AND user_id = $2
                """,
                tenant_id,
                user_id,
            )
            current = _hydrate_user_model_narratives(
                existing.get("model") if existing else None,
                row=existing if isinstance(existing, dict) else None,
            )
            merged = _apply_user_model_proposal(current, proposal or {})
            changed = merged != current
            if changed:
                await _upsert_user_model(
                    tenant_id=tenant_id,
                    user_id=user_id,
                    model=merged,
                    source="llm_session_enricher",
                )
            await _update_enrichment_state_success(tenant_id=tenant_id, user_id=user_id, mode=mode)
            return changed
    except Exception as e:
        await _update_enrichment_state_failure(
            tenant_id=tenant_id,
            user_id=user_id,
            mode=mode,
            error=str(e),
            base_backoff_seconds=retry_backoff_seconds,
            max_backoff_seconds=retry_max_seconds,
        )
        raise


async def _run_user_model_enrichment_mode_once(
    mode: str,
    max_users: int,
    min_confidence: float,
    daily_lookback_hours: int,
    weekly_lookback_days: int,
    retry_backoff_seconds: int,
    retry_max_seconds: int
) -> Dict[str, int]:
    users = await _fetch_enrichment_candidates(
        mode=mode,
        max_users=max_users,
        daily_lookback_hours=daily_lookback_hours,
        weekly_lookback_days=weekly_lookback_days,
    )
    counts = {"selected": len(users or []), "updated": 0, "failed": 0}
    for row in users or []:
        tenant_id = row.get("tenant_id")
        user_id = row.get("user_id")
        if not tenant_id or not user_id:
            continue
        try:
            changed = await _run_user_model_enrichment_for_user(
                tenant_id=tenant_id,
                user_id=user_id,
                mode=mode,
                min_confidence=min_confidence,
                retry_backoff_seconds=retry_backoff_seconds,
                retry_max_seconds=retry_max_seconds,
            )
            if changed:
                counts["updated"] += 1
        except Exception as e:
            logger.error("user model enrichment failed mode=%s tenant=%s user=%s error=%s", mode, tenant_id, user_id, e)
            counts["failed"] += 1
    return counts


async def _run_user_model_enrichment_backfill_once(
    max_users: int,
    min_confidence: float,
    retry_backoff_seconds: int,
    retry_max_seconds: int
) -> Dict[str, int]:
    users = await db.fetch(
        """
        SELECT tenant_id, user_id
        FROM (
            SELECT tenant_id, user_id, MAX(updated_at) AS ts
            FROM session_transcript
            GROUP BY tenant_id, user_id
        ) x
        ORDER BY ts DESC
        LIMIT $1
        """,
        max_users,
    )
    counts = {"selected": len(users or []), "updated": 0, "failed": 0}
    for row in users or []:
        tenant_id = row.get("tenant_id")
        user_id = row.get("user_id")
        if not tenant_id or not user_id:
            continue
        try:
            changed = await _run_user_model_enrichment_for_user(
                tenant_id=tenant_id,
                user_id=user_id,
                mode="backfill",
                min_confidence=min_confidence,
                retry_backoff_seconds=retry_backoff_seconds,
                retry_max_seconds=retry_max_seconds,
            )
            if changed:
                counts["updated"] += 1
        except Exception as e:
            logger.error("user model enrichment backfill failed tenant=%s user=%s error=%s", tenant_id, user_id, e)
            counts["failed"] += 1
    return counts


async def _fetch_hygiene_candidates(max_users: int) -> List[Dict[str, Any]]:
    return await db.fetch(
        """
        WITH session_touch AS (
            SELECT tenant_id, user_id, MAX(updated_at) AS ts
            FROM session_transcript
            WHERE updated_at >= NOW() - INTERVAL '48 hours'
            GROUP BY tenant_id, user_id
        ),
        model_touch AS (
            SELECT tenant_id, user_id, MAX(updated_at) AS ts
            FROM user_model
            WHERE updated_at >= NOW() - INTERVAL '48 hours'
            GROUP BY tenant_id, user_id
        ),
        touched AS (
            SELECT tenant_id, user_id, MAX(ts) AS ts
            FROM (
                SELECT * FROM session_touch
                UNION ALL
                SELECT * FROM model_touch
            ) x
            GROUP BY tenant_id, user_id
        )
        SELECT x.tenant_id, x.user_id
        FROM (
            SELECT t.tenant_id, t.user_id, t.ts
            FROM touched t
            LEFT JOIN user_model_enrichment_state s
              ON s.tenant_id = t.tenant_id AND s.user_id = t.user_id
            WHERE (
                s.last_hygiene_at IS NULL
                OR s.last_hygiene_at <= NOW() - INTERVAL '24 hours'
            )
            AND (
                s.next_hygiene_at IS NULL
                OR s.next_hygiene_at <= NOW()
            )
        ) x
        ORDER BY x.ts DESC
        LIMIT $1
        """,
        max(1, int(max_users)),
    )


async def _update_hygiene_state_success(tenant_id: str, user_id: str) -> None:
    await db.execute(
        """
        INSERT INTO user_model_enrichment_state (
            tenant_id, user_id, last_hygiene_at, next_hygiene_at, updated_at
        )
        VALUES (
            $1, $2, NOW(), NOW() + INTERVAL '24 hours', NOW()
        )
        ON CONFLICT (tenant_id, user_id)
        DO UPDATE SET
            last_hygiene_at = NOW(),
            next_hygiene_at = NOW() + INTERVAL '24 hours',
            updated_at = NOW()
        """,
        tenant_id,
        user_id,
    )


async def _run_user_model_hygiene_for_user(tenant_id: str, user_id: str) -> bool:
    async with _user_model_write_claim(tenant_id=tenant_id, user_id=user_id, ttl_seconds=180) as claimed:
        if not claimed:
            return False
        state = await db.fetchone(
            """
            SELECT last_hygiene_at
            FROM user_model_enrichment_state
            WHERE tenant_id = $1 AND user_id = $2
            """,
            tenant_id,
            user_id,
        )
        last_hygiene_at = _parse_optional_dt((state or {}).get("last_hygiene_at"))
        now_dt = datetime.utcnow().replace(tzinfo=dt_timezone.utc)
        if last_hygiene_at:
            if last_hygiene_at.tzinfo is None:
                last_hygiene_at = last_hygiene_at.replace(tzinfo=dt_timezone.utc)
            if (now_dt - last_hygiene_at) < timedelta(hours=24):
                return False

        row = await db.fetchone(
            """
            SELECT model, narrative_stable, narrative_current
            FROM user_model
            WHERE tenant_id = $1 AND user_id = $2
            """,
            tenant_id,
            user_id,
        )
        if not row:
            await _update_hygiene_state_success(tenant_id=tenant_id, user_id=user_id)
            return False
        raw_model = row.get("model")
        if not isinstance(raw_model, dict):
            await _update_hygiene_state_success(tenant_id=tenant_id, user_id=user_id)
            return False
        cleaned = _hydrate_user_model_narratives(raw_model, row=row if isinstance(row, dict) else None)
        if not _has_meaningful_user_model_diff(raw_model, cleaned):
            await _update_hygiene_state_success(tenant_id=tenant_id, user_id=user_id)
            return False
        await _upsert_user_model(
            tenant_id=tenant_id,
            user_id=user_id,
            model=cleaned,
            source="hygiene_pass",
        )
        await _update_hygiene_state_success(tenant_id=tenant_id, user_id=user_id)
        return True


async def _run_user_model_hygiene_mode_once(
    max_users: int
) -> Dict[str, int]:
    users = await _fetch_hygiene_candidates(max_users=max_users)
    counts = {"selected": len(users or []), "updated": 0, "failed": 0}
    for row in users or []:
        tenant_id = row.get("tenant_id")
        user_id = row.get("user_id")
        if not tenant_id or not user_id:
            continue
        try:
            changed = await _run_user_model_hygiene_for_user(tenant_id=tenant_id, user_id=user_id)
            if changed:
                counts["updated"] += 1
        except Exception as e:
            logger.error("user model hygiene failed tenant=%s user=%s error=%s", tenant_id, user_id, e)
            counts["failed"] += 1
    return counts


async def user_model_enrichment_loop(
    interval_seconds: int,
    max_users: int,
    min_confidence: float,
    daily_lookback_hours: int,
    weekly_lookback_days: int,
    retry_backoff_seconds: int,
    retry_max_seconds: int
) -> None:
    last_hygiene_run: Optional[datetime] = None
    while True:
        try:
            daily_counts = await _run_user_model_enrichment_mode_once(
                mode="daily",
                max_users=max_users,
                min_confidence=min_confidence,
                daily_lookback_hours=daily_lookback_hours,
                weekly_lookback_days=weekly_lookback_days,
                retry_backoff_seconds=retry_backoff_seconds,
                retry_max_seconds=retry_max_seconds,
            )
            weekly_counts = await _run_user_model_enrichment_mode_once(
                mode="weekly",
                max_users=max_users,
                min_confidence=min_confidence,
                daily_lookback_hours=daily_lookback_hours,
                weekly_lookback_days=weekly_lookback_days,
                retry_backoff_seconds=retry_backoff_seconds,
                retry_max_seconds=retry_max_seconds,
            )
            hygiene_counts = {"selected": 0, "updated": 0, "failed": 0}
            now_dt = datetime.utcnow()
            should_run_hygiene = (
                last_hygiene_run is None
                or (now_dt - last_hygiene_run) >= timedelta(hours=20)
            )
            if should_run_hygiene:
                hygiene_counts = await _run_user_model_hygiene_mode_once(
                    max_users=max_users,
                )
                last_hygiene_run = now_dt

            if (
                daily_counts.get("updated")
                or weekly_counts.get("updated")
                or hygiene_counts.get("updated")
                or daily_counts.get("failed")
                or weekly_counts.get("failed")
                or hygiene_counts.get("failed")
            ):
                logger.info(
                    "user model enrichment loop daily=%s weekly=%s hygiene=%s",
                    daily_counts,
                    weekly_counts,
                    hygiene_counts,
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"user model enrichment loop error: {e}")
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


def _is_unreasonably_future(
    candidate: Optional[datetime],
    reference_now_utc: datetime,
    tolerance: timedelta = timedelta(hours=2),
) -> bool:
    if not candidate:
        return False
    dt = candidate
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=dt_timezone.utc)
    ref = reference_now_utc if reference_now_utc.tzinfo else reference_now_utc.replace(tzinfo=dt_timezone.utc)
    return dt > (ref + tolerance)


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


def _normalize_habit_dedupe_plan(
    raw: Optional[Dict[str, Any]],
    valid_ids: set[str],
) -> Dict[str, Any]:
    plan = raw if isinstance(raw, dict) else {}
    merge_groups_out: List[Dict[str, Any]] = []
    for item in (plan.get("merge_groups") or []):
        if not isinstance(item, dict):
            continue
        group_ids = [
            _normalize_text(x)
            for x in (item.get("habit_ids") or [])
            if _normalize_text(x) in valid_ids
        ]
        group_ids = _dedupe_keep_order(group_ids, limit=20)
        if len(group_ids) < 2:
            continue
        canonical_id = _normalize_text(item.get("canonical_habit_id"))
        if canonical_id not in group_ids:
            canonical_id = group_ids[0]
        merge_groups_out.append(
            {
                "habit_ids": group_ids,
                "canonical_habit_id": canonical_id,
                "reason": _normalize_text(item.get("reason")),
            }
        )

    flagged_out: List[Dict[str, str]] = []
    for item in (plan.get("flagged_uncertain") or []):
        if isinstance(item, str):
            habit_id = _normalize_text(item)
            if habit_id in valid_ids:
                flagged_out.append({"habit_id": habit_id, "reason": ""})
            continue
        if not isinstance(item, dict):
            continue
        habit_id = _normalize_text(item.get("habit_id"))
        if habit_id not in valid_ids:
            continue
        flagged_out.append(
            {
                "habit_id": habit_id,
                "reason": _normalize_text(item.get("reason")),
            }
        )
    return {
        "merge_groups": merge_groups_out,
        "flagged_uncertain": flagged_out,
    }


async def _mark_habit_dedupe_run_state(
    tenant_id: str,
    user_id: str,
    run_date: date,
) -> None:
    await db.execute(
        """
        INSERT INTO habit_dedupe_state (tenant_id, user_id, last_run_date, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (tenant_id, user_id)
        DO UPDATE SET
            last_run_date = EXCLUDED.last_run_date,
            updated_at = NOW()
        """,
        tenant_id,
        user_id,
        run_date,
    )


async def _run_habit_dedupe_for_user(
    tenant_id: str,
    user_id: str,
    run_date: date,
) -> Dict[str, int]:
    rows = await db.fetch(
        """
        SELECT id::text AS id, text, time_horizon, confidence, salience, metadata,
               last_seen_at, updated_at, created_at
        FROM loops
        WHERE tenant_id = $1
          AND user_id = $2
          AND type = 'habit'
          AND status = 'active'
        ORDER BY COALESCE(last_seen_at, updated_at, created_at AT TIME ZONE 'UTC') DESC
        """,
        tenant_id,
        user_id,
    )
    if not rows:
        return {"staled": 0, "flagged": 0}

    habits_payload: List[Dict[str, Any]] = []
    valid_ids: set[str] = set()
    for row in rows:
        habit_id = _normalize_text(row.get("id"))
        habit_text = _normalize_text(row.get("text"))
        if not habit_id or not habit_text:
            continue
        valid_ids.add(habit_id)
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        habits_payload.append(
            {
                "habit_id": habit_id,
                "text": habit_text,
                "time_horizon": _normalize_text(row.get("time_horizon")) or "ongoing",
                "confidence": _normalize_confidence(row.get("confidence"), default=0.72),
                "salience": int(row.get("salience") or 0),
                "reason": _normalize_text(metadata.get("reason")),
                "existing_needs_confirmation": bool(metadata.get("needs_confirmation")),
            }
        )
    if not habits_payload:
        return {"staled": 0, "flagged": 0}

    prompt = (
        "Given these active daily habits for one user, identify:\n"
        "(a) duplicates or near-duplicates that mean the same thing,\n"
        "(b) entries unlikely to be genuine daily habits (aspirations/themes/relationship goals/one-off intentions).\n\n"
        "Rules:\n"
        "- Use only provided habit_ids.\n"
        "- A merge group must contain at least 2 habit_ids.\n"
        "- canonical_habit_id must be one of habit_ids and should be the clearest/most specific phrasing.\n"
        "- For uncertain entries, return habit_id and short reason.\n"
        "- Return strict JSON only.\n\n"
        "JSON schema:\n"
        "{\n"
        "  \"merge_groups\": [\n"
        "    {\"habit_ids\": [\"...\", \"...\"], \"canonical_habit_id\": \"...\", \"reason\": \"...\"}\n"
        "  ],\n"
        "  \"flagged_uncertain\": [\n"
        "    {\"habit_id\": \"...\", \"reason\": \"...\"}\n"
        "  ]\n"
        "}\n\n"
        f"RUN_DATE_UTC: {run_date.isoformat()}\n"
        f"HABITS_JSON: {json.dumps(habits_payload, ensure_ascii=True)}"
    )
    llm_client = get_llm_client()
    raw = await llm_client._call_llm(
        prompt=prompt,
        max_tokens=900,
        temperature=0.1,
        task="loops",
    )
    parsed = _safe_parse_json_object(raw) if raw else None
    plan = _normalize_habit_dedupe_plan(parsed, valid_ids=valid_ids)

    stale_ids: set[str] = set()
    keep_ids: set[str] = set()
    for group in plan.get("merge_groups") or []:
        group_ids = group.get("habit_ids") if isinstance(group.get("habit_ids"), list) else []
        canonical = _normalize_text(group.get("canonical_habit_id"))
        if canonical:
            keep_ids.add(canonical)
        for hid in group_ids:
            clean_hid = _normalize_text(hid)
            if clean_hid and clean_hid != canonical:
                stale_ids.add(clean_hid)
    stale_ids = {hid for hid in stale_ids if hid not in keep_ids}

    stale_count = 0
    if stale_ids:
        stale_count = int(
            await db.fetchval(
                """
                WITH updated AS (
                    UPDATE loops
                    SET status = 'stale',
                        updated_at = NOW()
                    WHERE tenant_id = $1
                      AND user_id = $2
                      AND status = 'active'
                      AND id = ANY($3::uuid[])
                    RETURNING id
                )
                SELECT count(*) FROM updated
                """,
                tenant_id,
                user_id,
                list(stale_ids),
            )
            or 0
        )

    flagged_count = 0
    for item in plan.get("flagged_uncertain") or []:
        habit_id = _normalize_text(item.get("habit_id"))
        if not habit_id or habit_id in stale_ids:
            continue
        reason = _normalize_text(item.get("reason"))
        if reason:
            row = await db.fetchone(
                """
                UPDATE loops
                SET metadata = jsonb_set(
                        jsonb_set(COALESCE(metadata, '{}'::jsonb), '{needs_confirmation}', 'true'::jsonb, true),
                        '{needs_confirmation_reason}',
                        to_jsonb($4::text),
                        true
                    ),
                    updated_at = NOW()
                WHERE tenant_id = $1
                  AND user_id = $2
                  AND id = $3::uuid
                  AND status = 'active'
                RETURNING id
                """,
                tenant_id,
                user_id,
                habit_id,
                reason,
            )
        else:
            row = await db.fetchone(
                """
                UPDATE loops
                SET metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{needs_confirmation}', 'true'::jsonb, true),
                    updated_at = NOW()
                WHERE tenant_id = $1
                  AND user_id = $2
                  AND id = $3::uuid
                  AND status = 'active'
                RETURNING id
                """,
                tenant_id,
                user_id,
                habit_id,
            )
        if row:
            flagged_count += 1

    return {"staled": stale_count, "flagged": flagged_count}


async def _run_daily_habit_dedupe_once(
    run_date: date,
    max_users: int,
) -> Dict[str, int]:
    users = await db.fetch(
        """
        SELECT u.tenant_id, u.user_id
        FROM (
            SELECT l.tenant_id, l.user_id,
                   MAX(COALESCE(l.last_seen_at, l.updated_at, l.created_at AT TIME ZONE 'UTC')) AS latest_seen
            FROM loops l
            WHERE l.type = 'habit'
              AND l.status = 'active'
            GROUP BY l.tenant_id, l.user_id
        ) u
        LEFT JOIN habit_dedupe_state s
          ON s.tenant_id = u.tenant_id
         AND s.user_id = u.user_id
        WHERE s.last_run_date IS NULL OR s.last_run_date < $1
        ORDER BY u.latest_seen DESC NULLS LAST
        LIMIT $2
        """,
        run_date,
        max(1, int(max_users)),
    )
    counts = {"users_seen": len(users or []), "users_updated": 0, "staled": 0, "flagged": 0}
    for row in users or []:
        tenant_id = _normalize_text(row.get("tenant_id"))
        user_id = _normalize_text(row.get("user_id"))
        if not tenant_id or not user_id:
            continue
        try:
            result = await _run_habit_dedupe_for_user(
                tenant_id=tenant_id,
                user_id=user_id,
                run_date=run_date,
            )
            await _mark_habit_dedupe_run_state(tenant_id=tenant_id, user_id=user_id, run_date=run_date)
            staled = int((result or {}).get("staled") or 0)
            flagged = int((result or {}).get("flagged") or 0)
            counts["staled"] += staled
            counts["flagged"] += flagged
            if staled or flagged:
                counts["users_updated"] += 1
        except Exception as e:
            logger.error("habit dedupe failed tenant=%s user=%s error=%s", tenant_id, user_id, e)
    return counts


async def daily_habit_dedupe_loop(
    interval_seconds: int,
    max_users: int,
) -> None:
    while True:
        try:
            run_date = datetime.utcnow().date()
            counts = await _run_daily_habit_dedupe_once(
                run_date=run_date,
                max_users=max_users,
            )
            if counts.get("users_updated"):
                logger.info(
                    "daily habit dedupe run_date=%s users_seen=%s users_updated=%s staled=%s flagged=%s",
                    run_date.isoformat(),
                    counts.get("users_seen"),
                    counts.get("users_updated"),
                    counts.get("staled"),
                    counts.get("flagged"),
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"daily habit dedupe loop error: {e}")
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
        SELECT analysis_date, themes, steering_note, confidence, updated_at
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
    confidence: Optional[float] = None
    updated_at: Optional[str] = None
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
        confidence = _normalize_confidence(row.get("confidence"), default=0.0) if row.get("confidence") is not None else None
        updated_value = row.get("updated_at")
        if isinstance(updated_value, datetime):
            updated_at = updated_value.astimezone(dt_timezone.utc).isoformat().replace("+00:00", "Z")
        else:
            updated_at = _normalize_text(updated_value) or None
    return {
        "date": yesterday_date.isoformat(),
        "themes": themes,
        "steering_note": steering_note,
        "confidence": confidence,
        "updated_at": updated_at,
    }


def _build_user_model_hint_rows(model: Dict[str, Any], threshold: float, max_items: int = 8) -> List[Dict[str, Any]]:
    hints: List[Dict[str, Any]] = []

    current_focus = model.get("current_focus")
    if isinstance(current_focus, dict):
        conf = _normalize_confidence(current_focus.get("confidence"), default=0.0)
        focus_text = _normalize_text(current_focus.get("text"))
        if focus_text and conf >= threshold:
            hints.append(
                {
                    "text": f"Current focus: {focus_text}",
                    "confidence": conf,
                    "updated_at": _normalize_text(current_focus.get("updated_at")) or None,
                    "source": _normalize_text(current_focus.get("source")) or "user_model",
                }
            )

    work_context = model.get("work_context")
    if isinstance(work_context, dict):
        conf = _normalize_confidence(work_context.get("confidence"), default=0.0)
        text = _normalize_text(work_context.get("text"))
        if text and conf >= threshold:
            hints.append(
                {
                    "text": f"Work context: {text}",
                    "confidence": conf,
                    "updated_at": _normalize_text(work_context.get("updated_at")) or None,
                    "source": _normalize_text(work_context.get("source")) or "user_model",
                }
            )

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
                hints.append(
                    {
                        "text": f"{domain} vision: {vision}",
                        "confidence": v_conf,
                        "updated_at": _normalize_text(entry.get("updated_at")) or None,
                        "source": "user_model",
                    }
                )
            if goal and g_conf >= threshold:
                hints.append(
                    {
                        "text": f"{domain} goal: {goal}",
                        "confidence": g_conf,
                        "updated_at": _normalize_text(entry.get("updated_at")) or None,
                        "source": "user_model",
                    }
                )

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
                hint = f"Relationship: {name} ({who}), currently {status}"
            elif name and who:
                hint = f"Relationship: {name} ({who})"
            else:
                hint = ""
            if hint:
                hints.append(
                    {
                        "text": hint,
                        "confidence": conf,
                        "updated_at": _normalize_text(rel.get("updated_at")) or None,
                        "source": _normalize_text(rel.get("source")) or "user_model",
                    }
                )
            if len(hints) >= max_items:
                break

    patterns = model.get("patterns")
    if isinstance(patterns, list):
        for row in patterns:
            if not isinstance(row, dict):
                continue
            conf = _normalize_confidence(row.get("confidence"), default=0.0)
            text = _normalize_text(row.get("text"))
            if text and conf >= threshold:
                hints.append(
                    {
                        "text": f"Pattern: {text}",
                        "confidence": conf,
                        "updated_at": _normalize_text(row.get("updated_at")) or None,
                        "source": _normalize_text(row.get("source")) or "user_model",
                    }
                )
            if len(hints) >= max_items:
                break

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for row in hints:
        text = _normalize_text(row.get("text"))
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(
            {
                "text": text,
                "confidence": _normalize_confidence(row.get("confidence"), default=0.0),
                "updated_at": _normalize_text(row.get("updated_at")) or None,
                "source": _normalize_text(row.get("source")) or "user_model",
            }
        )
        if len(deduped) >= max_items:
            break
    return deduped


def _extract_high_confidence_user_model_hints(model: Dict[str, Any], threshold: float) -> List[str]:
    return [
        _normalize_text(row.get("text"))
        for row in _build_user_model_hint_rows(model, threshold=threshold, max_items=8)
        if _normalize_text(row.get("text"))
    ]


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
    now_local = _normalize_text(narrative_ingredients.get("now_local"))
    gap_human = _normalize_text(narrative_ingredients.get("gap_human"))
    last_thread = _normalize_text(narrative_ingredients.get("last_thread"))
    user_tone = _normalize_text(narrative_ingredients.get("user_tone"))
    open_threads = [_normalize_text(x) for x in (narrative_ingredients.get("open_threads") or []) if _normalize_text(x)]
    open_thread_reasons = [_normalize_text(x) for x in (narrative_ingredients.get("open_thread_reasons") or []) if _normalize_text(x)]
    open_loops = [_normalize_text(x) for x in (narrative_ingredients.get("open_loops") or []) if _normalize_text(x)]
    yesterday_themes = [_normalize_text(x) for x in (narrative_ingredients.get("yesterday_themes") or []) if _normalize_text(x)]
    steering_note = _normalize_text(narrative_ingredients.get("steering_note"))
    continuation_hint = _normalize_text(narrative_ingredients.get("continuation_hint"))
    user_narrative_stable = _normalize_text(narrative_ingredients.get("user_narrative_stable"))
    user_narrative_current = _normalize_text(narrative_ingredients.get("user_narrative_current"))
    if rewrite_only:
        prompt = (
            "Rewrite the STARTBRIEF to satisfy all constraints exactly.\n"
            "Do not add facts. Do not remove real facts. Rephrase only.\n\n"
            "Constraints:\n"
            "- Address the assistant as \"you\" and refer to the human as \"the user\".\n"
            "- Do not mention conversation count.\n"
            "- If OPEN_THREAD_REASONS exists, pair each reason with its thread text.\n"
            "- Keep complete sentences only; no clipped endings.\n"
            "- No artificial word cap; use only the length needed for meaningful context.\n"
            "- No advice or imperatives: avoid should/could/try/maybe/a good idea.\n"
            "- No filler verbs: reported/stated/noted/mentioned/said.\n"
            "- No event labels or significance scoring words: avoid significant/development/event/context labels.\n"
            "- No interpretation framing: avoid indicating/suggesting/it means/may symbolize.\n"
            "- No years (20xx), no full calendar dates, no clock time format HH:MM.\n"
            "- One paragraph only.\n"
            "- If little happened, keep it brief (about 2 sentences).\n"
            "- If a lot happened, include enough detail (up to about 8 sentences).\n\n"
            "Inputs:\n"
            f"DEPTH: {depth_label}\n"
            f"NOW_LOCAL: {now_local}\n"
            f"GAP_HUMAN: {gap_human}\n"
            f"LAST_THREAD: {last_thread}\n"
            f"USER_TONE: {user_tone}\n"
            f"OPEN_THREADS: {json.dumps(open_threads, ensure_ascii=True)}\n\n"
            f"OPEN_THREAD_REASONS: {json.dumps(open_thread_reasons, ensure_ascii=True)}\n"
            f"OPEN_LOOPS: {json.dumps(open_loops, ensure_ascii=True)}\n"
            f"YESTERDAY_THEMES: {json.dumps(yesterday_themes, ensure_ascii=True)}\n"
            f"STEERING_NOTE: {steering_note}\n"
            f"CONTINUATION_HINT: {continuation_hint}\n\n"
            f"USER_NARRATIVE_STABLE: {user_narrative_stable}\n"
            f"USER_NARRATIVE_CURRENT: {user_narrative_current}\n\n"
            "Return ONLY the rewritten paragraph."
        )
    else:
        prompt = (
            "Write a STARTBRIEF paragraph addressed to the assistant (\"you\") about the user (\"the user\").\n"
            "This text will be prepended as context for a stateless LLM call. It is not shown to the user.\n\n"
            "Constraints:\n"
            "- Use \"you\" only for the assistant; use \"the user\" for the human.\n"
            "- Do not mention conversation count. That is handled separately.\n"
            "- Write like a sharp friend making quick notes before a call. Specific details only — what actually happened, what was said, what mattered. No interpretation, no inference, no 'may symbolize', no 'indicating a possible.' If the user saw a butterfly, say he saw a butterfly. Don't explain what it means.\n"
            "- No event labelling, no significance scoring, no case-note framing.\n"
            "- No indicating that/suggesting that phrasing.\n"
            "- Keep complete sentences only; never end mid-thought.\n"
            "- No artificial word cap. Use as much length as needed for meaningful context and no more.\n"
            "- If little happened, two sentences is fine. If a lot happened, up to eight sentences is fine.\n"
            "- No advice or imperatives. Avoid should/could/try/maybe/a good idea.\n"
            "- No filler verbs: reported/stated/noted/mentioned/said.\n"
            "- No years (20xx). Avoid full calendar dates and HH:MM clock times.\n"
            "- One paragraph only.\n"
            "- If OPEN_THREAD_REASONS exists, pair each reason with its thread text.\n\n"
            "Inputs:\n"
            f"DEPTH: {depth_label}\n"
            f"NOW_LOCAL: {now_local}\n"
            f"GAP_HUMAN: {gap_human}\n"
            f"LAST_THREAD: {last_thread}\n"
            f"USER_TONE: {user_tone}\n"
            f"OPEN_THREADS: {json.dumps(open_threads, ensure_ascii=True)}\n"
            f"OPEN_THREAD_REASONS: {json.dumps(open_thread_reasons, ensure_ascii=True)}\n"
            f"OPEN_LOOPS: {json.dumps(open_loops, ensure_ascii=True)}\n"
            f"YESTERDAY_THEMES: {json.dumps(yesterday_themes, ensure_ascii=True)}\n"
            f"STEERING_NOTE: {steering_note}\n"
            f"CONTINUATION_HINT: {continuation_hint}\n\n"
            f"USER_NARRATIVE_STABLE: {user_narrative_stable}\n"
            f"USER_NARRATIVE_CURRENT: {user_narrative_current}\n\n"
            "Return ONLY the paragraph."
        )
    response = await llm_client._call_llm(
        prompt=prompt,
        max_tokens=400,
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

_STARTBRIEF_LOW_VALUE_SENTENCE_PATTERNS = (
    r"\bsession began with\b",
    r"\bbrief exchange of presence\b",
    r"\bpresence and readiness\b",
    r"\bconfirmed availability\b",
    r"\balways available\b",
    r"\binsisted .* prior memory\b",
    r"\bchecked if .* present\b",
)

_LOW_VALUE_LOOP_PATTERNS = (
    r"\bcoffee\b",
    r"\bweather\b",
    r"\bstroll\b",
    r"\bmovie time\b",
    r"\bgo out after\b",
)


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
    if _has_near_duplicate_sentences(clean):
        reasons.append("duplicated_content")
    return reasons


def _truncate_at_word_boundary(value: str, limit: int) -> str:
    clean = _normalize_text(value)
    if len(clean) <= limit:
        return clean
    sentences = [
        _normalize_text(s)
        for s in re.split(r"(?<=[.!?])\s+", clean)
        if _normalize_text(s)
    ]
    kept: List[str] = []
    for sentence in sentences:
        candidate = " ".join(kept + [sentence]).strip()
        if len(candidate) <= limit:
            kept.append(sentence)
        else:
            break
    if kept:
        return " ".join(kept).strip()
    return sentences[0] if sentences else clean


def _truncate_to_word_cap(value: str, word_cap: int) -> str:
    clean = _normalize_text(value)
    words = re.findall(r"\S+", clean)
    if len(words) <= word_cap:
        return clean
    sentences = [
        _normalize_text(s)
        for s in re.split(r"(?<=[.!?])\s+", clean)
        if _normalize_text(s)
    ]
    kept: List[str] = []
    used_words = 0
    for sentence in sentences:
        sentence_words = len(re.findall(r"\S+", sentence))
        if used_words + sentence_words <= word_cap:
            kept.append(sentence)
            used_words += sentence_words
        else:
            break
    if kept:
        return _normalize_text(" ".join(kept))
    return sentences[0] if sentences else clean


def _ensure_sentence_spacing(value: str) -> str:
    clean = _normalize_text(value)
    if not clean:
        return ""
    return re.sub(r"([.!?])([A-Za-z])", r"\1 \2", clean)


def _sentence_tokens(text: str) -> set[str]:
    return {t for t in re.findall(r"[a-z0-9]+", _normalize_text(text).lower()) if t}


def _has_near_duplicate_sentences(text: str, threshold: float = 0.84) -> bool:
    sentences = [_normalize_text(s) for s in re.split(r"(?<=[.!?])\s+", _normalize_text(text)) if _normalize_text(s)]
    tokenized = [_sentence_tokens(s) for s in sentences]
    for i in range(len(tokenized)):
        a = tokenized[i]
        if not a:
            continue
        for j in range(i + 1, len(tokenized)):
            b = tokenized[j]
            if not b:
                continue
            overlap = len(a & b) / max(1, len(a | b))
            if overlap >= threshold:
                return True
    return False


def _compress_handover_text(text: Optional[str], depth_label: str) -> Optional[str]:
    clean = _normalize_text(text)
    if not clean:
        return None
    sentence_caps = {
        "continuation": 3,
        "today": 4,
        "yesterday": 5,
        "multi_day": 6,
    }
    cap = sentence_caps.get(_normalize_text(depth_label).lower(), 4)
    raw_sentences = [_normalize_text(s) for s in re.split(r"(?<=[.!?])\s+", clean) if _normalize_text(s)]
    kept: List[str] = []
    seen_keys: List[set[str]] = []
    for sentence in raw_sentences:
        lower = sentence.lower()
        if any(re.search(pattern, lower) for pattern in _STARTBRIEF_LOW_VALUE_SENTENCE_PATTERNS):
            continue
        tokens = _sentence_tokens(sentence)
        if not tokens:
            continue
        duplicate = False
        for prev in seen_keys:
            overlap = len(tokens & prev) / max(1, len(tokens | prev))
            if overlap >= 0.84:
                duplicate = True
                break
        if duplicate:
            continue
        seen_keys.append(tokens)
        kept.append(sentence)
        if len(kept) >= cap:
            break
    if not kept:
        kept = raw_sentences[:cap]
    return _ensure_sentence_spacing(" ".join(kept)).strip()


def _join_sentence_parts(parts: List[str]) -> str:
    """Join narrative fragments while enforcing sentence boundaries between fragments."""
    joined: List[str] = []
    for raw in parts:
        part = _normalize_text(raw)
        if not part:
            continue
        if joined and not re.search(r"[.!?]$", joined[-1]):
            joined[-1] = f"{joined[-1]}."
        joined.append(part)
    return _ensure_sentence_spacing(" ".join(joined))


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
    open_thread_reasons = [_normalize_text(x) for x in (ingredients.get("open_thread_reasons") or []) if _normalize_text(x)]
    now_local = _normalize_text(ingredients.get("now_local")) or "the current local time context is available"
    gap_human = _normalize_text(ingredients.get("gap_human")) or "an unknown interval"

    relationship_keywords = ("relationship", "wife", "husband", "partner", "girlfriend", "boyfriend", "friend", "family", "mother", "father", "sister", "brother", "rupture", "repair")
    relationship_clause = ""
    relationship_source = " ".join([last_thread] + open_threads).lower()
    if any(k in relationship_source for k in relationship_keywords):
        relationship_clause = f"A relationship thread is open: {last_thread or open_threads[0]}."

    emotional_pairs: List[str] = []
    for idx, thread in enumerate(open_threads[:2]):
        reason = open_thread_reasons[idx] if idx < len(open_thread_reasons) else ""
        if reason:
            emotional_pairs.append(f"{thread} (reason: {reason})")
        else:
            emotional_pairs.append(thread)

    sentences: List[str] = [
        f"It is {now_local}.",
        f"You last spoke with the user {gap_human} ago.",
    ]
    if relationship_clause:
        sentences.append(relationship_clause)
    if emotional_pairs:
        sentences.append(f"Open emotional threads: {'; '.join(emotional_pairs)}.")
    if last_thread:
        sentences.append(f"Recent context: {last_thread}.")
    elif user_tone:
        sentences.append(f"The user tone was {user_tone}.")
    if not relationship_clause and not emotional_pairs and not last_thread:
        sentences.append("No major new events were recorded; recent context was limited.")

    return _normalize_text(" ".join(sentences))


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
    # Graphiti may flatten custom SessionSummary attributes to top-level properties.
    if not attrs:
        attrs = {
            key: node.get(key)
            for key in (
                "summary_text",
                "bridge_text",
                "session_id",
                "reference_time",
                "created_at",
                "summary_facts",
                "tone",
                "moment",
                "decisions",
                "unresolved",
                "index_text",
                "salience",
            )
            if node.get(key) is not None
        }

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


_NARRATIVE_RELATIVE_TIME_RE = re.compile(
    r"\b(yesterday|today|recently|last night|last week|about \d+\s+(day|days|week|weeks|month|months)\s+ago)\b",
    flags=re.IGNORECASE,
)


def _sentence_split(text: Optional[str]) -> List[str]:
    clean = _normalize_text(text)
    if not clean:
        return []
    return [_normalize_text(s) for s in re.split(r"(?<=[.!?])\s+", clean) if _normalize_text(s)]


def _extract_active_relationship_names(model: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    relationships = model.get("key_relationships")
    if not isinstance(relationships, list):
        return out
    for row in relationships:
        if not isinstance(row, dict):
            continue
        if _normalize_text(row.get("status")).lower() != "active":
            continue
        name = _normalize_text(row.get("name"))
        if not name:
            continue
        if name.lower() in {x.lower() for x in out}:
            continue
        out.append(name)
    return out


def _sentence_conflicts_with_active_relationships(sentence: str, active_names: List[str]) -> bool:
    lower = _normalize_text(sentence).lower()
    if not lower or not active_names:
        return False
    for name in active_names:
        n = re.escape(name.lower())
        # If current relationship state is active, suppress stale breakup framing for that same entity.
        if re.search(rf"\b(broke up|break up|breakup|split up|ex[-\s]?girlfriend|ex[-\s]?boyfriend)\b.*\b{n}\b", lower):
            return True
        if re.search(rf"\b{n}\b.*\b(broke up|break up|breakup|split up|ex[-\s]?girlfriend|ex[-\s]?boyfriend)\b", lower):
            return True
    return False


def _sentence_conflicts_with_recent_reconciliation(sentence: str, evidence_texts: List[str]) -> bool:
    lower = _normalize_text(sentence).lower()
    if not lower:
        return False
    if not re.search(r"\b(broke up|break up|breakup|split up)\b", lower):
        return False
    evidence_blob = " ".join(_normalize_text(t).lower() for t in (evidence_texts or []) if _normalize_text(t))
    if not evidence_blob:
        return False
    # Fresh reconciliation signal should dominate stale breakup summaries.
    return bool(re.search(r"\b(reconciled|back together|got back together)\b", evidence_blob))


def _build_startbrief_narrative(
    narrative_stable: Optional[str],
    narrative_current: Optional[str],
    selected_summaries: List[Dict[str, Any]],
    top_active_loops: List[Dict[str, Any]],
    user_model: Dict[str, Any],
) -> Optional[str]:
    current_sentences = _sentence_split(narrative_current)
    stable_sentences = _sentence_split(narrative_stable)
    if not current_sentences and not stable_sentences:
        return None

    evidence_texts: List[str] = []
    for s in selected_summaries or []:
        for key in ("summary_facts", "summary_text", "moment", "index_text"):
            value = _normalize_text(s.get(key))
            if value:
                evidence_texts.append(value)
    for loop in top_active_loops or []:
        text = _normalize_text(loop.get("text"))
        if text:
            evidence_texts.append(text)

    evidence_terms = _query_terms(" ".join(evidence_texts))
    active_relationship_names = _extract_active_relationship_names(user_model if isinstance(user_model, dict) else {})

    kept_current: List[str] = []
    for sentence in current_sentences:
        if _sentence_conflicts_with_active_relationships(sentence, active_relationship_names):
            continue
        if _sentence_conflicts_with_recent_reconciliation(sentence, evidence_texts):
            continue
        overlap = _query_overlap_score(sentence, evidence_terms)
        is_relative = bool(_NARRATIVE_RELATIVE_TIME_RE.search(sentence))
        # Relative-time details must stay anchored to current evidence or they drift into stale trivia.
        if is_relative and overlap < 0.45:
            continue
        if overlap < 0.2 and len(current_sentences) > 2:
            continue
        kept_current.append(sentence)
        if len(kept_current) >= 4:
            break

    if kept_current:
        return _truncate_at_word_boundary(" ".join(kept_current), 520)

    kept_stable = [s for s in stable_sentences if not _NARRATIVE_RELATIVE_TIME_RE.search(s)]
    if kept_stable:
        return _truncate_at_word_boundary(" ".join(kept_stable[:2]), 320)
    for s in selected_summaries or []:
        fallback = _normalize_text(s.get("summary_facts") or s.get("summary_text") or s.get("moment"))
        if fallback:
            return _truncate_at_word_boundary(fallback, 260)
    return None


def _sanitize_enrichment_narrative_current(
    narrative_current: Optional[str],
    session_summaries: List[Dict[str, Any]],
    loop_texts: List[str],
    current_model: Dict[str, Any],
    reference_now_utc: Optional[datetime] = None,
) -> Optional[str]:
    sentences = _sentence_split(narrative_current)
    if not sentences:
        return None
    now_utc = reference_now_utc or datetime.utcnow().replace(tzinfo=dt_timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=dt_timezone.utc)
    active_relationship_names = _extract_active_relationship_names(current_model if isinstance(current_model, dict) else {})
    evidence_texts: List[str] = []
    for row in session_summaries or []:
        for key in ("summary_facts", "moment", "bridge_text"):
            value = _normalize_text(row.get(key))
            if value:
                evidence_texts.append(value)
    for text in loop_texts or []:
        clean = _normalize_text(text)
        if clean:
            evidence_texts.append(clean)
    evidence_terms = _query_terms(" ".join(evidence_texts))
    kept: List[str] = []
    for sentence in sentences:
        if _sentence_conflicts_with_active_relationships(sentence, active_relationship_names):
            continue
        if _sentence_conflicts_with_recent_reconciliation(sentence, evidence_texts):
            continue
        if _NARRATIVE_RELATIVE_TIME_RE.search(sentence) and _query_overlap_score(sentence, evidence_terms) < 0.45:
            continue
        kept.append(sentence)
        if len(kept) >= 6:
            break
    if kept:
        return _truncate_at_word_boundary(" ".join(kept), 1200)
    # Fallback to best recent summary claims if LLM narrative is fully filtered.
    scored_rows: List[Tuple[float, str]] = []
    all_summary_texts = [_summary_text_for_scoring(s) for s in (session_summaries or []) if _summary_text_for_scoring(s)]
    for row in session_summaries or []:
        metrics = _score_startbrief_summary(
            summary=row,
            reference_now_utc=now_utc,
            active_relationship_names=active_relationship_names,
            has_recent_reconciliation_signal=any(
                re.search(r"\b(reconciled|back together|got back together)\b", t.lower()) for t in all_summary_texts
            ),
            all_summary_texts=all_summary_texts,
            loop_texts=loop_texts or [],
        )
        text = _summary_text_for_scoring(row)
        if text:
            scored_rows.append((metrics.get("score", 0.0), text))
    scored_rows.sort(key=lambda x: x[0], reverse=True)
    fallback_sentences = [text for _score, text in scored_rows[:2] if text]
    if fallback_sentences:
        return _truncate_at_word_boundary(" ".join(fallback_sentences), 420)
    return None


def _sanitize_enrichment_narrative_stable(
    narrative_stable: Optional[str],
    current_model: Dict[str, Any],
) -> Optional[str]:
    sentences = _sentence_split(narrative_stable)
    if not sentences:
        return None
    active_relationship_names = _extract_active_relationship_names(current_model if isinstance(current_model, dict) else {})
    kept: List[str] = []
    for sentence in sentences:
        if _NARRATIVE_RELATIVE_TIME_RE.search(sentence):
            continue
        if _sentence_conflicts_with_active_relationships(sentence, active_relationship_names):
            continue
        kept.append(sentence)
        if len(kept) >= 10:
            break
    return _truncate_at_word_boundary(" ".join(kept), 2000) if kept else None


def _summary_text_for_scoring(summary: Dict[str, Any]) -> str:
    return _normalize_text(
        summary.get("latest_thread_text")
        or summary.get("summary_facts")
        or summary.get("summary_text")
        or summary.get("summary")
        or summary.get("index_text")
    )


def _summary_salience_weight(raw: Any) -> float:
    value = _normalize_text(raw).lower()
    return {"high": 1.0, "medium": 0.7, "low": 0.4}.get(value, 0.5)


def _summary_confidence_weight(raw_salience: Any, text: str) -> float:
    base = {"high": 0.85, "medium": 0.65, "low": 0.45}.get(_normalize_text(raw_salience).lower(), 0.5)
    if re.search(r"\blimited actionable detail\b", _normalize_text(text).lower()):
        base = min(base, 0.35)
    return max(0.0, min(1.0, base))


def _summary_recency_weight(created_at: Optional[datetime], reference_now_utc: datetime) -> float:
    if not created_at:
        return 0.1
    dt = created_at if created_at.tzinfo else created_at.replace(tzinfo=dt_timezone.utc)
    ref = reference_now_utc if reference_now_utc.tzinfo else reference_now_utc.replace(tzinfo=dt_timezone.utc)
    age_hours = max(0.0, (ref - dt).total_seconds() / 3600.0)
    if age_hours <= 2:
        return 1.0
    if age_hours <= 12:
        return 0.85
    if age_hours <= 24:
        return 0.7
    if age_hours <= 72:
        return 0.45
    return 0.2


def _estimate_summary_importance(
    text: str,
    all_summary_texts: List[str],
    loop_texts: List[str],
) -> float:
    # Importance = enduring relevance across repeated mentions and active loop alignment.
    # Salience is immediate intensity/urgency at the time of mention.
    terms = _query_terms(text)
    if not terms:
        return 0.35
    recurrence_hits = 0
    for other in all_summary_texts:
        other_clean = _normalize_text(other)
        if not other_clean or other_clean == text:
            continue
        if _query_overlap_score(other_clean, terms) >= 0.55:
            recurrence_hits += 1
    loop_hits = 0
    for loop_text in loop_texts:
        if _query_overlap_score(loop_text, terms) >= 0.45:
            loop_hits += 1
    score = 0.35 + min(0.35, recurrence_hits * 0.18) + min(0.3, loop_hits * 0.15)
    return max(0.0, min(1.0, score))


def _score_startbrief_summary(
    summary: Dict[str, Any],
    reference_now_utc: datetime,
    active_relationship_names: List[str],
    has_recent_reconciliation_signal: bool,
    all_summary_texts: List[str],
    loop_texts: List[str],
) -> Dict[str, Any]:
    text = _summary_text_for_scoring(summary)
    created_dt = _parse_optional_dt(summary.get("created_at"))
    recency = _summary_recency_weight(created_dt, reference_now_utc)
    salience = _summary_salience_weight(summary.get("salience"))
    confidence = _summary_confidence_weight(summary.get("salience"), text)
    importance = _estimate_summary_importance(text, all_summary_texts, loop_texts)

    contradiction_penalty = 0.0
    if _sentence_conflicts_with_active_relationships(text, active_relationship_names):
        contradiction_penalty -= 0.35
    if has_recent_reconciliation_signal and re.search(r"\b(broke up|break up|breakup|split up)\b", text.lower()):
        contradiction_penalty -= 0.25
    if re.search(r"\blimited actionable detail\b", text.lower()):
        contradiction_penalty -= 0.15

    total = (0.40 * recency) + (0.20 * salience) + (0.25 * importance) + (0.15 * confidence) + contradiction_penalty
    return {
        "session_id": _normalize_text(summary.get("session_id")),
        "score": round(float(total), 4),
        "recency": round(float(recency), 4),
        "salience": round(float(salience), 4),
        "importance": round(float(importance), 4),
        "confidence": round(float(confidence), 4),
        "contradiction_penalty": round(float(contradiction_penalty), 4),
    }


def _loop_salience_weight(raw: Any) -> float:
    try:
        v = float(raw)
    except Exception:
        return 0.5
    # Loop salience is stored in 0..5 (mostly); normalize to 0..1.
    if v > 1.0:
        v = v / 5.0
    return max(0.0, min(1.0, v))


def _loop_recency_weight(last_seen_at: Any, reference_now_utc: datetime) -> float:
    dt = _parse_optional_dt(last_seen_at)
    if not dt:
        return 0.2
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=dt_timezone.utc)
    ref = reference_now_utc if reference_now_utc.tzinfo else reference_now_utc.replace(tzinfo=dt_timezone.utc)
    age_hours = max(0.0, (ref - dt).total_seconds() / 3600.0)
    if age_hours <= 6:
        return 1.0
    if age_hours <= 24:
        return 0.8
    if age_hours <= 72:
        return 0.55
    return 0.3


def _loop_importance_weight(loop: Dict[str, Any]) -> float:
    text = _normalize_text(loop.get("text"))
    loop_type = _normalize_text(loop.get("type")).lower()
    horizon = _normalize_text(loop.get("timeHorizon")).lower()
    base = 0.35
    if loop_type in {"thread", "commitment", "decision"}:
        base += 0.25
    if loop_type == "habit":
        base += 0.18
    if horizon in {"ongoing", "this_week"}:
        base += 0.12
    if _is_durable_commitment_text(text):
        base += 0.1
    return max(0.0, min(1.0, base))


def _loop_confidence_weight(loop: Dict[str, Any]) -> float:
    raw = loop.get("confidence")
    if raw is not None:
        return _normalize_confidence(raw, default=0.6)
    salience = _loop_salience_weight(loop.get("salience"))
    return max(0.35, min(0.9, 0.35 + (0.55 * salience)))


def _loop_contradiction_penalty(loop: Dict[str, Any], has_recent_reconciliation_signal: bool) -> float:
    text = _normalize_text(loop.get("text")).lower()
    if not text:
        return 0.0
    if has_recent_reconciliation_signal and re.search(r"\b(breakup|break up|split up|process relationship guilt)\b", text):
        return -0.25
    return 0.0


def _score_startbrief_loop(
    loop: Dict[str, Any],
    reference_now_utc: datetime,
    has_recent_reconciliation_signal: bool,
) -> Dict[str, Any]:
    recency = _loop_recency_weight(loop.get("lastSeenAt"), reference_now_utc)
    salience = _loop_salience_weight(loop.get("salience"))
    importance = _loop_importance_weight(loop)
    confidence = _loop_confidence_weight(loop)
    contradiction_penalty = _loop_contradiction_penalty(loop, has_recent_reconciliation_signal)
    total = (0.35 * recency) + (0.20 * salience) + (0.30 * importance) + (0.15 * confidence) + contradiction_penalty
    return {
        "id": _normalize_text(loop.get("id")) or None,
        "text": _normalize_text(loop.get("text")),
        "type": _normalize_text(loop.get("type")) or None,
        "score": round(float(total), 4),
        "recency": round(float(recency), 4),
        "salience": round(float(salience), 4),
        "importance": round(float(importance), 4),
        "confidence": round(float(confidence), 4),
        "contradiction_penalty": round(float(contradiction_penalty), 4),
    }


def select_startbrief_ingredients(
    reference_now: datetime,
    last_session_end: Optional[datetime],
    sessions_today_count: int,
    time_of_day_label: str,
    recent_session_summaries: List[Dict[str, Any]],
    yesterday_daily_analysis: Dict[str, Any],
    top_active_loops: List[Dict[str, Any]],
    user_model_hints: List[str],
    user_model_hint_rows: Optional[List[Dict[str, Any]]] = None,
    user_model: Optional[Dict[str, Any]] = None,
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

    loop_texts_for_importance = [_normalize_text(l.get("text")) for l in (top_active_loops or []) if _normalize_text(l.get("text"))]
    summary_texts_for_importance = [_summary_text_for_scoring(s) for s in in_24h if _summary_text_for_scoring(s)]
    active_relationship_names = _extract_active_relationship_names(user_model if isinstance(user_model, dict) else {})
    has_recent_reconciliation_signal = any(
        re.search(r"\b(reconciled|back together|got back together)\b", _summary_text_for_scoring(s).lower())
        for s in in_24h
    )

    scored_summaries: List[Tuple[Dict[str, Any], Dict[str, Any], float]] = []
    for s in in_24h:
        metrics = _score_startbrief_summary(
            summary=s,
            reference_now_utc=now_utc,
            active_relationship_names=active_relationship_names,
            has_recent_reconciliation_signal=has_recent_reconciliation_signal,
            all_summary_texts=summary_texts_for_importance,
            loop_texts=loop_texts_for_importance,
        )
        created_dt = _parse_optional_dt(s.get("created_at"))
        created_ts = (
            (created_dt.astimezone(dt_timezone.utc) if created_dt and created_dt.tzinfo else (created_dt.replace(tzinfo=dt_timezone.utc) if created_dt else datetime.min.replace(tzinfo=dt_timezone.utc))).timestamp()
        )
        scored_summaries.append((s, metrics, float(created_ts)))

    scored_summaries.sort(key=lambda row: (row[1].get("score", 0.0), row[2]), reverse=True)
    all_summary_scores: List[Dict[str, Any]] = [metrics for _summary_row, metrics, _created_ts in scored_summaries]
    selected: List[Dict[str, Any]] = []
    selected_summary_scores: List[Dict[str, Any]] = []
    seen_session_ids = set()
    for summary_row, metrics, _created_ts in scored_summaries:
        sid = _normalize_text(summary_row.get("session_id"))
        if sid and sid in seen_session_ids:
            continue
        if sid:
            seen_session_ids.add(sid)
        selected.append(summary_row)
        selected_summary_scores.append(metrics)
        if len(selected) >= 2:
            break

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

    scored_loops: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for loop in filtered_loops:
        metrics = _score_startbrief_loop(
            loop=loop,
            reference_now_utc=now_utc,
            has_recent_reconciliation_signal=has_recent_reconciliation_signal,
        )
        scored_loops.append((loop, metrics))
    scored_loops.sort(
        key=lambda pair: (
            pair[1].get("score", 0.0),
            int(pair[0].get("salience") or 0),
        ),
        reverse=True,
    )
    all_loop_scores = [pair[1] for pair in scored_loops]
    selected_loops = [pair[0] for pair in scored_loops[:2]]
    selected_loop_scores = [pair[1] for pair in scored_loops[:5]]

    loop_reason_by_text: Dict[str, str] = {}
    for l in selected_loops:
        text = _normalize_text(l.get("text"))
        reason = _normalize_text(l.get("reason"))
        if text and reason:
            loop_reason_by_text[text.lower()] = reason

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
        line = _join_sentence_parts([facts, tone, moment])
        if line:
            last_thread_parts.append(line)
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

    open_threads = _dedupe_keep_order(unresolved_items + [_normalize_text(l.get("text")) for l in selected_loops], limit=2)
    open_thread_reasons = _dedupe_keep_order(
        [loop_reason_by_text.get(t.lower(), "") for t in open_threads if loop_reason_by_text.get(t.lower(), "")],
        limit=2,
    )
    gap_human = _format_gap_human(gap_minutes)
    session_frequency = _session_frequency_phrase(sessions_today_count)
    now_local = _format_now_local(reference_now, time_of_day_label)
    hint_rows_filtered: List[Dict[str, Any]] = []
    for row in (user_model_hint_rows or []):
        text = _normalize_text((row or {}).get("text"))
        if not text:
            continue
        updated_raw = _normalize_text((row or {}).get("updated_at"))
        keep = True
        if updated_raw:
            updated_dt = _parse_optional_dt(updated_raw)
            if updated_dt:
                updated_utc = updated_dt.astimezone(dt_timezone.utc) if updated_dt.tzinfo else updated_dt.replace(tzinfo=dt_timezone.utc)
                ref_utc = reference_now.astimezone(dt_timezone.utc) if reference_now.tzinfo else reference_now.replace(tzinfo=dt_timezone.utc)
                age_days = max(0.0, (ref_utc - updated_utc).total_seconds() / 86400.0)
                if age_days > 45:
                    keep = False
        if keep:
            hint_rows_filtered.append(row)
    if not hint_rows_filtered and user_model_hints:
        hint_rows_filtered = [{"text": _normalize_text(x), "confidence": None, "updated_at": None, "source": "user_model"} for x in user_model_hints if _normalize_text(x)]
    hints_from_rows = [_normalize_text((row or {}).get("text")) for row in hint_rows_filtered if _normalize_text((row or {}).get("text"))]

    return {
        "depth_label": depth_label,
        "gap_minutes": gap_minutes,
        "sessions_today_count": sessions_today_count,
        "selected_summaries": selected,
        "selected_summary_scores": selected_summary_scores[:5],
        "all_summary_scores": all_summary_scores[:20],
        "last_thread": _join_sentence_parts(last_thread_parts),
        "user_tone": _normalize_text(" ".join(_dedupe_keep_order(tone_parts, limit=2))),
        "open_threads": open_threads,
        "open_thread_reasons": open_thread_reasons,
        "open_loops": [_normalize_text(l.get("text")) for l in selected_loops[:2] if _normalize_text(l.get("text"))],
        "selected_loops": selected_loops[:2],
        "selected_loop_scores": selected_loop_scores[:5],
        "all_loop_scores": all_loop_scores[:20],
        "continuation_hint": continuation_hint,
        "now_state": _normalize_text(selected_loops[0].get("text")) if selected_loops else "",
        "now_local": now_local,
        "gap_human": gap_human,
        "session_frequency": session_frequency,
        "yesterday_themes": [_normalize_text(x) for x in (yesterday_daily_analysis.get("themes") or []) if _normalize_text(x)][:2],
        "steering_note": _normalize_text(yesterday_daily_analysis.get("steering_note")) or None,
        "daily_analysis_date": _normalize_text(yesterday_daily_analysis.get("date")) or None,
        "daily_analysis_confidence": (
            _normalize_confidence(yesterday_daily_analysis.get("confidence"), default=0.0)
            if yesterday_daily_analysis.get("confidence") is not None
            else None
        ),
        "daily_analysis_updated_at": _normalize_text(yesterday_daily_analysis.get("updated_at")) or None,
        "user_model_hints": hints_from_rows[:6],
        "user_model_hint_rows": [
            {
                "text": _normalize_text((row or {}).get("text")),
                "confidence": (
                    _normalize_confidence((row or {}).get("confidence"), default=0.0)
                    if (row or {}).get("confidence") is not None
                    else None
                ),
                "updated_at": _normalize_text((row or {}).get("updated_at")) or None,
                "source": _normalize_text((row or {}).get("source")) or None,
            }
            for row in hint_rows_filtered
            if _normalize_text((row or {}).get("text"))
        ][:6],
    }


def compose_ops_context(
    ingredients: Dict[str, Any],
    top_active_loops: List[Dict[str, Any]],
) -> Dict[str, Any]:
    top_loops_today: List[Dict[str, Any]] = []
    filtered = ingredients.get("selected_loops") if isinstance(ingredients.get("selected_loops"), list) else []
    if not filtered:
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
    for loop in filtered[:2]:
        text = _normalize_text(loop.get("text"))
        if not text:
            continue
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
        "user_model_hint_rows": ingredients.get("user_model_hint_rows", [])[:6],
        "yesterday_themes": ingredients.get("yesterday_themes", [])[:2],
        "steering_note": ingredients.get("steering_note"),
        "daily_analysis": {
            "date": ingredients.get("daily_analysis_date"),
            "confidence": ingredients.get("daily_analysis_confidence"),
            "updated_at": ingredients.get("daily_analysis_updated_at"),
        },
    }


SIGNAL_PACK_CLASSES = ("identity", "trajectory", "today", "open_loops", "state", "relationships", "habits", "momentum", "stale_threads")
SIGNAL_PACK_MAX_PER_CLASS = 3
SIGNAL_PACK_CLASS_CAPS: Dict[str, Optional[int]] = {
    "identity": SIGNAL_PACK_MAX_PER_CLASS,
    "trajectory": SIGNAL_PACK_MAX_PER_CLASS,
    "today": SIGNAL_PACK_MAX_PER_CLASS,
    "open_loops": SIGNAL_PACK_MAX_PER_CLASS,
    "state": SIGNAL_PACK_MAX_PER_CLASS,
    "relationships": SIGNAL_PACK_MAX_PER_CLASS,
    # Habits are always surfaced and should not be dropped by caps.
    "habits": None,
    # Momentum is supportive context; keep small and low-precedence.
    "momentum": 3,
    # Stale threads are low-priority check-ins; at most one per session.
    "stale_threads": 1,
}
SIGNAL_HIGH_SENSITIVITY_TERMS = (
    "abuse", "assault", "suicide", "self-harm", "addiction", "relapse",
    "depressed", "panic", "trauma", "pregnan", "medical", "diagnos",
)
SIGNAL_MEDIUM_SENSITIVITY_TERMS = ("anxious", "anxiety", "therapy", "money", "debt", "fight", "conflict")


def _normalize_signal_salience(raw: Any, default: float = 0.6) -> float:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    if value > 1.0:
        value = value / 5.0 if value <= 5.0 else value / 10.0
    return max(0.0, min(1.0, value))


def _classify_signal_sensitivity(text: str) -> str:
    lower = _normalize_text(text).lower()
    if not lower:
        return "LOW"
    if any(term in lower for term in SIGNAL_HIGH_SENSITIVITY_TERMS):
        return "HIGH"
    if any(term in lower for term in SIGNAL_MEDIUM_SENSITIVITY_TERMS):
        return "MEDIUM"
    return "LOW"


def _signal_id(signal_class: str, source: str, text: str) -> str:
    payload = {
        "signal_class": _normalize_text(signal_class).lower(),
        "source": _normalize_text(source).lower(),
        "text": _normalize_text(text).lower(),
    }
    return stable_short_hash(payload, length=16)


def _record_signal_rejection(debug: Dict[str, Any], signal_class: str, reason: str, source: str) -> None:
    reasons = debug.setdefault("rejection_reasons", {})
    reasons[reason] = int(reasons.get(reason) or 0) + 1
    by_class = debug.setdefault("rejections_by_class", {})
    class_reasons = by_class.setdefault(signal_class, {})
    class_reasons[reason] = int(class_reasons.get(reason) or 0) + 1
    debug.setdefault("rejections", []).append({"class": signal_class, "reason": reason, "source": source})


def _coerce_datetime_utc(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value.astimezone(dt_timezone.utc).replace(tzinfo=None) if value.tzinfo else value
    if isinstance(value, str):
        parsed = _parse_optional_dt(value)
        if parsed:
            return parsed.astimezone(dt_timezone.utc).replace(tzinfo=None) if parsed.tzinfo else parsed
    return None


def _days_since(reference_now: datetime, when: Optional[datetime]) -> Optional[int]:
    if not when:
        return None
    gap = reference_now - when
    return max(0, int(gap.total_seconds() // 86400))


def _is_covered_by_existing_signal(candidate: str, existing_texts: List[str]) -> bool:
    candidate_clean = _normalize_text(candidate).lower()
    if not candidate_clean:
        return True
    for existing in existing_texts:
        existing_clean = _normalize_text(existing).lower()
        if not existing_clean:
            continue
        if candidate_clean == existing_clean:
            return True
        if candidate_clean in existing_clean or existing_clean in candidate_clean:
            return True
    return False


def _append_signal(
    classes: Dict[str, List[Dict[str, Any]]],
    debug: Dict[str, Any],
    *,
    signal_class: str,
    text: str,
    confidence: float,
    salience: float,
    recency_ts: Optional[str],
    source: str,
    evidence: Optional[Dict[str, Any]] = None,
) -> None:
    class_list = classes.setdefault(signal_class, [])
    debug_counts = debug.setdefault("counts", {})
    considered = debug_counts.setdefault("considered", {})
    emitted = debug_counts.setdefault("emitted", {})
    considered[signal_class] = int(considered.get(signal_class) or 0) + 1

    clean_text = _normalize_text(text)
    if not clean_text:
        _record_signal_rejection(debug, signal_class, "empty_text", source)
        return
    existing = {_normalize_text(item.get("text")).lower() for item in class_list}
    if clean_text.lower() in existing:
        _record_signal_rejection(debug, signal_class, "duplicate", source)
        return
    class_cap = SIGNAL_PACK_CLASS_CAPS.get(signal_class, SIGNAL_PACK_MAX_PER_CLASS)
    if class_cap is not None and len(class_list) >= class_cap:
        _record_signal_rejection(debug, signal_class, "class_cap", source)
        return

    sensitivity = _classify_signal_sensitivity(clean_text)
    rendered_text = clean_text
    signal_evidence = evidence.copy() if isinstance(evidence, dict) else None
    signal: Dict[str, Any] = {
        "id": _signal_id(signal_class, source, clean_text),
        "class": signal_class,
        "text": rendered_text,
        "confidence": _normalize_confidence(confidence, default=0.6),
        "salience": _normalize_signal_salience(salience, default=0.6),
        "sensitivity": sensitivity,
        "recency_ts": recency_ts,
        "source": source,
    }
    if sensitivity == "HIGH":
        signal["surface_policy"] = "steer_only"
        signal["text"] = "Sensitive topic present. Use gentle steering, avoid verbatim repetition."
        if signal_evidence is None:
            signal_evidence = {}
        signal_evidence["redacted"] = True
    if signal_evidence:
        signal["evidence"] = signal_evidence

    class_list.append(signal)
    emitted[signal_class] = int(emitted.get(signal_class) or 0) + 1


async def _build_signals_pack(
    tenantId: str,
    userId: str,
    sessionId: Optional[str] = None,
    now: Optional[str] = None,
    capacity: Optional[str] = None,
    tactic_appetite: Optional[str] = None,
):
    try:
        reference_now = datetime.utcnow()
        if now:
            parsed_now = _parse_optional_dt(now)
            if parsed_now:
                reference_now = parsed_now.astimezone(dt_timezone.utc).replace(tzinfo=None) if parsed_now.tzinfo else parsed_now

        generated_at = reference_now.replace(tzinfo=dt_timezone.utc).isoformat().replace("+00:00", "Z")
        resolved_session_id = sessionId or await _get_latest_session_id(tenantId, userId)
        classes: Dict[str, List[Dict[str, Any]]] = {name: [] for name in SIGNAL_PACK_CLASSES}
        debug: Dict[str, Any] = {
            "counts": {
                "considered": {name: 0 for name in SIGNAL_PACK_CLASSES},
                "emitted": {name: 0 for name in SIGNAL_PACK_CLASSES},
            },
            "sources_used": [],
            "rejection_reasons": {},
            "rejections_by_class": {},
            "rejections": [],
            "triage_trace": {"checkin_tactic_fired": False},
        }

        top_loops = await loops.get_top_loops_for_startbrief(
            tenant_id=tenantId,
            user_id=userId,
            limit=10,
            persona_id=None
        )
        if top_loops:
            debug["sources_used"].append("loops")
        for loop_item in top_loops or []:
            loop_text = _normalize_text(getattr(loop_item, "text", ""))
            loop_ts = _normalize_text(getattr(loop_item, "lastSeenAt", "") or getattr(loop_item, "updatedAt", ""))
            loop_type = _normalize_text(getattr(loop_item, "type", ""))
            loop_horizon = _normalize_text(getattr(loop_item, "timeHorizon", ""))
            loop_salience = getattr(loop_item, "salience", None)
            loop_confidence = getattr(loop_item, "confidence", None)
            _append_signal(
                classes,
                debug,
                signal_class="open_loops",
                text=loop_text,
                confidence=loop_confidence if loop_confidence is not None else 0.72,
                salience=loop_salience if loop_salience is not None else 0.75,
                recency_ts=loop_ts or generated_at,
                source="loops",
                evidence={"loop_type": loop_type or None, "time_horizon": loop_horizon or None},
            )
            if loop_horizon == "today":
                _append_signal(
                    classes,
                    debug,
                    signal_class="today",
                    text=f"Today priority: {loop_text}",
                    confidence=0.7,
                    salience=loop_salience if loop_salience is not None else 0.7,
                    recency_ts=loop_ts or generated_at,
                    source="loops",
                )
            if loop_type in {"commitment", "decision", "thread"}:
                _append_signal(
                    classes,
                    debug,
                    signal_class="trajectory",
                    text=f"{loop_type.title()}: {loop_text}",
                    confidence=0.68,
                    salience=loop_salience if loop_salience is not None else 0.66,
                    recency_ts=loop_ts or generated_at,
                    source="loops",
                )

        # Habits need dedicated daily visibility, independent of top loop ranking caps.
        active_habit_rows = await db.fetch(
            """
            SELECT
                l.id,
                l.text,
                l.hint,
                l.metadata,
                l.confidence,
                l.salience,
                l.time_horizon,
                l.updated_at,
                l.last_seen_at,
                hdl.completed,
                hdl.nudged,
                hdl.acknowledged
            FROM loops l
            LEFT JOIN habit_daily_log hdl
              ON hdl.habit_id = l.id
             AND hdl.date = $3
            WHERE l.tenant_id = $1
              AND l.user_id = $2
              AND l.type = 'habit'
              AND l.status = 'active'
            ORDER BY
              CASE WHEN l.time_horizon = 'ongoing' THEN 0 ELSE 1 END,
              COALESCE(l.last_seen_at, l.updated_at) DESC
            """,
            tenantId,
            userId,
            reference_now.date(),
        )
        if active_habit_rows:
            if "loops" not in debug["sources_used"]:
                debug["sources_used"].append("loops")
            for habit_row in active_habit_rows:
                habit_text = _normalize_text(habit_row.get("text"))
                if not habit_text:
                    continue
                habit_horizon = _normalize_text(habit_row.get("time_horizon")) or "ongoing"
                habit_hint = _normalize_text(habit_row.get("hint"))
                habit_completed = bool(habit_row.get("completed"))
                habit_nudged = bool(habit_row.get("nudged"))
                habit_acknowledged = bool(habit_row.get("acknowledged"))
                habit_metadata = habit_row.get("metadata") if isinstance(habit_row.get("metadata"), dict) else {}
                needs_confirmation = bool(habit_metadata.get("needs_confirmation"))
                habit_today_status = "done" if habit_completed else "not yet"
                habit_nudged_status = "yes" if habit_nudged else "no"
                habit_acknowledged_status = "yes" if habit_acknowledged else "no"
                habit_line = f"[habit] {habit_text} ({habit_horizon})"
                if habit_hint:
                    habit_line += f" — {habit_hint}"
                if needs_confirmation:
                    habit_line += " | needs confirmation"
                else:
                    habit_line += (
                        f" | today: {habit_today_status} | nudged: {habit_nudged_status}"
                        f" | acknowledged: {habit_acknowledged_status}"
                    )
                habit_ts = None
                if isinstance(habit_row.get("last_seen_at"), datetime):
                    habit_ts = habit_row["last_seen_at"].isoformat().replace("+00:00", "Z")
                elif isinstance(habit_row.get("updated_at"), datetime):
                    habit_ts = habit_row["updated_at"].isoformat().replace("+00:00", "Z")
                _append_signal(
                    classes,
                    debug,
                    signal_class="habits",
                    text=habit_line,
                    confidence=_normalize_confidence(habit_row.get("confidence"), default=0.72),
                    salience=habit_row.get("salience") if habit_row.get("salience") is not None else 0.7,
                    recency_ts=habit_ts or generated_at,
                    source="loops",
                    evidence={
                        "loop_type": "habit",
                        "habit_text": habit_text,
                        "time_horizon": habit_horizon,
                        "completed_today": habit_completed,
                        "nudged_today": habit_nudged,
                        "acknowledged_today": habit_acknowledged,
                        "needs_confirmation": needs_confirmation,
                    },
                )

        # Momentum: recent completions and streaks (low-precedence, max 3).
        momentum_candidates: List[Dict[str, Any]] = []
        momentum_seen = set()
        surfaced_habit_texts = {
            _normalize_text((item or {}).get("evidence", {}).get("habit_text", ""))
            for item in classes.get("habits", [])
            if isinstance(item, dict) and isinstance(item.get("evidence"), dict)
        }
        surfaced_habit_texts = {t for t in surfaced_habit_texts if t}

        def _add_momentum_candidate(
            text: str,
            *,
            ts: Optional[datetime],
            confidence: float,
            salience: float,
            source: str,
            allow_habit_repeat: bool = False,
            evidence: Optional[Dict[str, Any]] = None,
        ) -> None:
            clean = _normalize_text(text)
            if not clean:
                return
            key = clean.lower()
            if key in momentum_seen:
                return
            if not allow_habit_repeat and any(_normalize_text(h).lower() in clean.lower() for h in surfaced_habit_texts):
                return
            momentum_seen.add(key)
            momentum_candidates.append(
                {
                    "text": clean,
                    "ts": ts,
                    "confidence": confidence,
                    "salience": salience,
                    "source": source,
                    "evidence": evidence or {},
                }
            )

        # Completed habits and streaks from habit_daily_log.
        if active_habit_rows:
            habit_ids: List[str] = []
            habit_rows_by_id: Dict[str, Dict[str, Any]] = {}
            for row in active_habit_rows:
                raw_id = row.get("id")
                habit_id = str(raw_id) if raw_id is not None else ""
                habit_text = _normalize_text(row.get("text"))
                if not habit_id or not habit_text:
                    continue
                habit_ids.append(habit_id)
                habit_rows_by_id[habit_id] = row
                if bool(row.get("completed")):
                    _add_momentum_candidate(
                        f"[momentum] {habit_text} — completed today",
                        ts=_coerce_datetime_utc(row.get("last_seen_at") or row.get("updated_at") or reference_now),
                        confidence=_normalize_confidence(row.get("confidence"), default=0.74),
                        salience=0.74,
                        source="habit_daily_log",
                        allow_habit_repeat=True,
                        evidence={"kind": "habit_completed_today", "habit_id": habit_id, "habit_text": habit_text},
                    )
            if habit_ids:
                streak_rows = await db.fetch(
                    """
                    SELECT hdl.habit_id::text AS habit_id, hdl.date, hdl.completed
                    FROM habit_daily_log hdl
                    WHERE hdl.user_id = $1
                      AND hdl.habit_id = ANY($2::uuid[])
                      AND hdl.date >= ($3::date - INTERVAL '31 days')
                    ORDER BY hdl.habit_id, hdl.date DESC
                    """,
                    userId,
                    habit_ids,
                    reference_now.date(),
                )
                by_habit: Dict[str, Dict[date, bool]] = {}
                for row in streak_rows or []:
                    hid = _normalize_text(row.get("habit_id"))
                    day = row.get("date")
                    if not hid or not isinstance(day, date):
                        continue
                    by_habit.setdefault(hid, {})[day] = bool(row.get("completed"))
                for habit_id, logs_by_day in by_habit.items():
                    row = habit_rows_by_id.get(habit_id) or {}
                    habit_text = _normalize_text(row.get("text"))
                    if not habit_text:
                        continue
                    streak = 0
                    cursor = reference_now.date()
                    while logs_by_day.get(cursor) is True:
                        streak += 1
                        cursor = cursor - timedelta(days=1)
                    if streak >= 2:
                        _add_momentum_candidate(
                            f"[momentum] {habit_text} — {streak} day streak",
                            ts=_coerce_datetime_utc(row.get("last_seen_at") or row.get("updated_at") or reference_now),
                            confidence=_normalize_confidence(row.get("confidence"), default=0.76),
                            salience=0.78,
                            source="habit_daily_log",
                            allow_habit_repeat=True,
                            evidence={"kind": "habit_streak", "habit_id": habit_id, "habit_text": habit_text, "streak_days": streak},
                        )

        # Recently completed loops in the last 48 hours.
        done_rows = await db.fetch(
            """
            SELECT id, text, type, updated_at, confidence, salience
            FROM loops
            WHERE tenant_id = $1
              AND user_id = $2
              AND status = 'completed'
              AND updated_at >= (NOW() - INTERVAL '48 hours')
            ORDER BY updated_at DESC
            LIMIT 20
            """,
            tenantId,
            userId,
        )
        if done_rows:
            if "loops" not in debug["sources_used"]:
                debug["sources_used"].append("loops")
            for row in done_rows:
                text = _normalize_text(row.get("text"))
                if not text:
                    continue
                updated_dt = _coerce_datetime_utc(row.get("updated_at")) or reference_now
                days_ago = _days_since(reference_now, updated_dt)
                when = "today" if days_ago == 0 else f"{days_ago} day{'s' if days_ago != 1 else ''} ago"
                _add_momentum_candidate(
                    f"[momentum] {text} — completed {when}",
                    ts=updated_dt,
                    confidence=_normalize_confidence(row.get("confidence"), default=0.72),
                    salience=row.get("salience") if row.get("salience") is not None else 0.7,
                    source="loops",
                    evidence={"kind": "loop_done_recent", "loop_type": _normalize_text(row.get("type")) or None},
                )

        # Kept commitments recently resolved (trajectory-oriented commitments).
        commitment_rows = await db.fetch(
            """
            SELECT id, text, type, status, updated_at, confidence, salience, metadata
            FROM loops
            WHERE tenant_id = $1
              AND user_id = $2
              AND type = 'commitment'
              AND (
                    status = 'completed'
                    OR COALESCE(metadata->>'resolved', 'false') = 'true'
                  )
              AND updated_at >= (NOW() - INTERVAL '48 hours')
            ORDER BY updated_at DESC
            LIMIT 10
            """,
            tenantId,
            userId,
        )
        if commitment_rows:
            if "loops" not in debug["sources_used"]:
                debug["sources_used"].append("loops")
            for row in commitment_rows:
                text = _normalize_text(row.get("text"))
                if not text:
                    continue
                updated_dt = _coerce_datetime_utc(row.get("updated_at")) or reference_now
                days_ago = _days_since(reference_now, updated_dt)
                when = "today" if days_ago == 0 else f"{days_ago} day{'s' if days_ago != 1 else ''} ago"
                _add_momentum_candidate(
                    f"[momentum] {text} — completed {when}",
                    ts=updated_dt,
                    confidence=_normalize_confidence(row.get("confidence"), default=0.73),
                    salience=row.get("salience") if row.get("salience") is not None else 0.72,
                    source="loops",
                    evidence={"kind": "kept_commitment_recent", "loop_type": "commitment"},
                )

        if momentum_candidates:
            momentum_candidates.sort(
                key=lambda item: item.get("ts") or datetime.min,
                reverse=True,
            )
            for item in momentum_candidates[:3]:
                ts = item.get("ts")
                recency_ts = (
                    ts.replace(tzinfo=dt_timezone.utc).isoformat().replace("+00:00", "Z")
                    if isinstance(ts, datetime) else generated_at
                )
                _append_signal(
                    classes,
                    debug,
                    signal_class="momentum",
                    text=item.get("text") or "",
                    confidence=item.get("confidence") or 0.7,
                    salience=item.get("salience") if item.get("salience") is not None else 0.7,
                    recency_ts=recency_ts,
                    source=_normalize_text(item.get("source")) or "loops",
                    evidence=item.get("evidence") if isinstance(item.get("evidence"), dict) else None,
                )

        user_model_row = await db.fetchone(
            """
            SELECT model, updated_at
            FROM user_model
            WHERE tenant_id = $1 AND user_id = $2
            LIMIT 1
            """,
            tenantId,
            userId
        )
        user_model = _normalize_user_model((user_model_row or {}).get("model"))
        user_model_updated_at = (user_model_row or {}).get("updated_at")
        user_model_ts = user_model_updated_at.isoformat().replace("+00:00", "Z") if isinstance(user_model_updated_at, datetime) else generated_at
        if user_model_row:
            debug["sources_used"].append("user_model")

        current_focus = user_model.get("current_focus")
        if isinstance(current_focus, dict):
            focus_text = _normalize_text(current_focus.get("text"))
            if focus_text:
                focus_conf = _normalize_confidence(current_focus.get("confidence"), default=0.65)
                _append_signal(
                    classes,
                    debug,
                    signal_class="today",
                    text=f"Current focus: {focus_text}",
                    confidence=focus_conf,
                    salience=0.78,
                    recency_ts=user_model_ts,
                    source="user_model",
                )
                _append_signal(
                    classes,
                    debug,
                    signal_class="state",
                    text=f"Active thread: {focus_text}",
                    confidence=focus_conf,
                    salience=0.65,
                    recency_ts=user_model_ts,
                    source="user_model",
                )

        north_star = user_model.get("north_star")
        if isinstance(north_star, dict):
            for domain in ("work", "relationships", "health", "spirituality", "general"):
                entry = north_star.get(domain)
                if not isinstance(entry, dict):
                    continue
                goal = _normalize_text(entry.get("goal"))
                vision = _normalize_text(entry.get("vision"))
                if goal:
                    _append_signal(
                        classes,
                        debug,
                        signal_class="trajectory",
                        text=f"{domain} goal: {goal}",
                        confidence=_normalize_confidence(entry.get("goal_confidence"), default=0.62),
                        salience=0.64,
                        recency_ts=user_model_ts,
                        source="user_model",
                    )
                if vision:
                    _append_signal(
                        classes,
                        debug,
                        signal_class="identity",
                        text=f"{domain} vision: {vision}",
                        confidence=_normalize_confidence(entry.get("vision_confidence"), default=0.6),
                        salience=0.58,
                        recency_ts=user_model_ts,
                        source="user_model",
                    )

        relationships = user_model.get("key_relationships")
        if isinstance(relationships, list):
            for rel in relationships:
                if not isinstance(rel, dict):
                    continue
                name = _normalize_text(rel.get("name"))
                who = _normalize_text(rel.get("who"))
                status = _normalize_text(rel.get("status"))
                relationship_text = _build_relationship_signal_text(name, who, status)
                if not relationship_text:
                    continue
                _append_signal(
                    classes,
                    debug,
                    signal_class="relationships",
                    text=relationship_text,
                    confidence=_normalize_confidence(rel.get("confidence"), default=0.6),
                    salience=0.65,
                    recency_ts=user_model_ts,
                    source="user_model",
                )

        daily_row = await db.fetchone(
            """
            SELECT analysis_date, themes, steering_note, confidence, updated_at
            FROM daily_analysis
            WHERE tenant_id = $1 AND user_id = $2
            ORDER BY analysis_date DESC
            LIMIT 1
            """,
            tenantId,
            userId
        )
        if daily_row:
            debug["sources_used"].append("daily_analysis")
            daily_conf = _normalize_confidence(daily_row.get("confidence"), default=0.58)
            analysis_date = daily_row.get("analysis_date")
            daily_ts = None
            if isinstance(daily_row.get("updated_at"), datetime):
                daily_ts = daily_row["updated_at"].isoformat().replace("+00:00", "Z")
            elif analysis_date:
                daily_ts = f"{analysis_date.isoformat()}T00:00:00Z"
            themes = daily_row.get("themes") if isinstance(daily_row.get("themes"), list) else []
            for theme in themes[:4]:
                theme_text = _normalize_text(theme)
                if not theme_text:
                    continue
                _append_signal(
                    classes,
                    debug,
                    signal_class="trajectory",
                    text=f"Recent theme: {theme_text}",
                    confidence=daily_conf,
                    salience=0.57,
                    recency_ts=daily_ts or generated_at,
                    source="daily_analysis",
                )
            steering_note = _normalize_text(daily_row.get("steering_note"))
            if steering_note:
                _append_signal(
                    classes,
                    debug,
                    signal_class="today",
                    text=f"Steering note: {steering_note}",
                    confidence=daily_conf,
                    salience=0.62,
                    recency_ts=daily_ts or generated_at,
                    source="daily_analysis",
                )

        summary_rows: List[Dict[str, Any]] = []
        try:
            summary_nodes = await graphiti_client.get_recent_session_summary_nodes(
                tenant_id=tenantId,
                user_id=userId,
                limit=2
            )
            for node in summary_nodes or []:
                normalized = _normalize_startbrief_session_summary_node(node)
                if not normalized:
                    continue
                summary_rows.append({
                    "summary_text": _normalize_text(normalized.get("summary_text")),
                    "tone": _normalize_text(normalized.get("tone")),
                    "moment": _normalize_text(normalized.get("moment")),
                    "reference_time": _normalize_text(normalized.get("reference_time") or normalized.get("created_at")),
                })
            if summary_rows:
                debug["sources_used"].append("session_summary")
        except Exception as e:
            logger.info("signals pack summary lookup failed: %s", e)

        if not summary_rows:
            transcript_row = await db.fetchone(
                """
                SELECT messages, updated_at
                FROM session_transcript
                WHERE tenant_id = $1 AND user_id = $2
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                tenantId,
                userId
            )
            messages = (transcript_row or {}).get("messages") if isinstance(transcript_row, dict) else None
            if isinstance(messages, list) and messages:
                tail = [_normalize_text((m or {}).get("text")) for m in messages[-2:] if _normalize_text((m or {}).get("text"))]
                if tail:
                    debug["sources_used"].append("session_transcript_tail")
                    _append_signal(
                        classes,
                        debug,
                        signal_class="state",
                        text=f"Recent session tail: {' | '.join(tail)}",
                        confidence=0.5,
                        salience=0.5,
                        recency_ts=(transcript_row.get("updated_at").isoformat().replace("+00:00", "Z") if isinstance(transcript_row.get("updated_at"), datetime) else generated_at),
                        source="session_transcript_tail",
                    )
        else:
            for summary in summary_rows:
                summary_text = _normalize_text(summary.get("summary_text"))
                summary_ts = _normalize_text(summary.get("reference_time")) or generated_at
                if summary_text:
                    _append_signal(
                        classes,
                        debug,
                        signal_class="state",
                        text=f"Session summary: {summary_text}",
                        confidence=0.74,
                        salience=0.68,
                        recency_ts=summary_ts,
                        source="session_summary",
                    )
                    _append_signal(
                        classes,
                        debug,
                        signal_class="trajectory",
                        text=f"Recent direction: {summary_text}",
                        confidence=0.7,
                        salience=0.63,
                        recency_ts=summary_ts,
                        source="session_summary",
                    )
                tone = _normalize_text(summary.get("tone"))
                if tone:
                    _append_signal(
                        classes,
                        debug,
                        signal_class="state",
                        text=f"Tone: {tone}",
                        confidence=0.58,
                        salience=0.5,
                        recency_ts=summary_ts,
                        source="session_summary",
                    )

        # Stale thread candidates from Graphiti facts/entities.
        try:
            if not graphiti_client._initialized:
                await graphiti_client.initialize()
            driver = getattr(graphiti_client.client, "driver", None) if graphiti_client.client else None
            if driver:
                composite_user_id = graphiti_client._make_composite_user_id(tenantId, userId)
                scoped_driver = driver
                clone = getattr(driver, "clone", None)
                if callable(clone):
                    try:
                        scoped_driver = clone(database=composite_user_id)
                    except Exception:
                        scoped_driver = driver
                stale_cutoff = reference_now - timedelta(days=7)
                rows = await scoped_driver.execute_query(
                    """
                    MATCH (n)
                    WHERE n.group_id = $group_id
                      AND COALESCE(n.salience, 0) >= 4
                      AND COALESCE(n.last_seen_at, n.updated_at, n.created_at) <= $stale_cutoff
                    RETURN n.name AS name,
                           n.summary AS summary,
                           n.text AS text,
                           n.last_seen_at AS last_seen_at,
                           n.updated_at AS updated_at,
                           n.created_at AS created_at,
                           n.salience AS salience
                    ORDER BY COALESCE(n.last_seen_at, n.updated_at, n.created_at) ASC,
                             COALESCE(n.salience, 0) DESC
                    LIMIT 25
                    """,
                    group_id=composite_user_id,
                    stale_cutoff=stale_cutoff,
                )
                if rows:
                    debug["sources_used"].append("graphiti")

                existing_surface_texts = [
                    _normalize_text(item.get("text"))
                    for class_name in ("habits", "trajectory", "open_loops", "today")
                    for item in classes.get(class_name, [])
                    if isinstance(item, dict) and _normalize_text(item.get("text"))
                ]
                for row in rows or []:
                    if isinstance(row, dict):
                        raw_text = _normalize_text(row.get("text") or row.get("summary") or row.get("name"))
                        raw_last_seen = row.get("last_seen_at") or row.get("updated_at") or row.get("created_at")
                        raw_salience = row.get("salience")
                    elif isinstance(row, (list, tuple)):
                        raw_text = _normalize_text(
                            (row[2] if len(row) > 2 else None)
                            or (row[1] if len(row) > 1 else None)
                            or (row[0] if len(row) > 0 else None)
                        )
                        raw_last_seen = (
                            (row[3] if len(row) > 3 else None)
                            or (row[4] if len(row) > 4 else None)
                            or (row[5] if len(row) > 5 else None)
                        )
                        raw_salience = row[6] if len(row) > 6 else None
                    else:
                        continue
                    if not raw_text:
                        continue
                    if _is_covered_by_existing_signal(raw_text, existing_surface_texts):
                        continue
                    last_seen_dt = _coerce_datetime_utc(raw_last_seen)
                    days_ago = _days_since(reference_now, last_seen_dt)
                    if days_ago is None or days_ago < 7:
                        continue
                    stale_line = f"[stale] {raw_text} — last mentioned {days_ago} day{'s' if days_ago != 1 else ''} ago"
                    stale_ts = (
                        last_seen_dt.replace(tzinfo=dt_timezone.utc).isoformat().replace("+00:00", "Z")
                        if isinstance(last_seen_dt, datetime) else generated_at
                    )
                    _append_signal(
                        classes,
                        debug,
                        signal_class="stale_threads",
                        text=stale_line,
                        confidence=0.58,
                        salience=raw_salience if raw_salience is not None else 0.55,
                        recency_ts=stale_ts,
                        source="graphiti",
                        evidence={
                            "stale_entity": raw_text,
                            "days_since_last_seen": days_ago,
                        },
                    )
                    if classes.get("stale_threads"):
                        break
        except Exception as e:
            logger.info("signals pack stale_threads lookup failed: %s", e)

        overlay_tactics: List[Dict[str, Any]] = []
        triage_trace = debug.setdefault("triage_trace", {})
        triage_trace["checkin_tactic_fired"] = False
        stale_items = classes.get("stale_threads") or []
        capacity_label = _normalize_text(capacity).upper()
        appetite_label = _normalize_text(tactic_appetite).upper()
        if stale_items and capacity_label == "HIGH" and appetite_label == "HIGH":
            chosen = stale_items[0]
            evidence = chosen.get("evidence") if isinstance(chosen.get("evidence"), dict) else {}
            stale_entity = _normalize_text(evidence.get("stale_entity")) or _normalize_text(chosen.get("text"))
            thread_key = _normalize_text(chosen.get("id")) or stable_short_hash(
                {
                    "kind": "checkin_tactic_thread",
                    "tenant_id": tenantId,
                    "user_id": userId,
                    "stale_entity": stale_entity,
                },
                length=32,
            )
            cooldown_cutoff = reference_now - timedelta(hours=72)
            latest_fire = await db.fetchone(
                """
                SELECT fired_at
                FROM checkin_tactic_log
                WHERE tenant_id = $1
                  AND user_id = $2
                  AND thread_key = $3
                ORDER BY fired_at DESC
                LIMIT 1
                """,
                tenantId,
                userId,
                thread_key,
            )
            last_fired_at = _coerce_datetime_utc((latest_fire or {}).get("fired_at"))
            cooldown_ok = not last_fired_at or last_fired_at <= cooldown_cutoff
            if cooldown_ok and stale_entity:
                overlay_text = (
                    f"A thread you know about hasn't come up in a while — {stale_entity}. "
                    "If the moment feels right and natural, ask about it once, genuinely. "
                    "Not \"my system shows...\" — just curiosity. One question, then drop it."
                )
                overlay_tactics.append(
                    {
                        "name": "checkin",
                        "stale_entity": stale_entity,
                        "text": overlay_text,
                    }
                )
                triage_trace["checkin_tactic_fired"] = True
                await db.execute(
                    """
                    INSERT INTO checkin_tactic_log (tenant_id, user_id, thread_key, stale_entity, session_id, fired_at)
                    VALUES ($1, $2, $3, $4, $5, NOW())
                    """,
                    tenantId,
                    userId,
                    thread_key,
                    stale_entity,
                    resolved_session_id,
                )
            else:
                triage_trace["checkin_reason"] = "cooldown"
        else:
            triage_trace["checkin_reason"] = "gate_not_met"
        triage_trace["capacity"] = capacity_label or None
        triage_trace["tactic_appetite"] = appetite_label or None

        for class_name in SIGNAL_PACK_CLASSES:
            class_cap = SIGNAL_PACK_CLASS_CAPS.get(class_name, SIGNAL_PACK_MAX_PER_CLASS)
            if class_cap is None:
                continue
            classes[class_name] = classes[class_name][:class_cap]

        return {
            "generated_at": generated_at,
            "session_id": resolved_session_id,
            "classes": classes,
            "overlay_tactics": overlay_tactics,
            "debug": debug,
        }
    except Exception as e:
        logger.error(f"Signals pack failed: {e}")
        raise HTTPException(status_code=500, detail="Signals pack failed")


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


async def _upsert_habit_daily_log_today(
    tenant_id: str,
    user_id: str,
    habit_id: str,
    completed: Optional[bool] = None,
    nudged: Optional[bool] = None,
    user_response: Optional[str] = None,
    inferred_from: Optional[str] = None,
) -> Dict[str, Any]:
    try:
        parsed_habit_id = str(uuid.UUID(_normalize_text(habit_id)))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid habitId")

    today_utc = datetime.utcnow().date()
    row = await db.fetchone(
        """
        INSERT INTO habit_daily_log (
            user_id, habit_id, date, completed, nudged, user_response, inferred_from, created_at
        )
        SELECT
            $2,
            l.id,
            $4,
            COALESCE($5, FALSE),
            COALESCE($6, FALSE),
            NULLIF($7, ''),
            NULLIF($8, ''),
            NOW()
        FROM loops l
        WHERE l.tenant_id = $1
          AND l.user_id = $2
          AND l.id = $3::uuid
          AND l.type = 'habit'
        ON CONFLICT (habit_id, date)
        DO UPDATE SET
            completed = COALESCE($5, habit_daily_log.completed),
            nudged = COALESCE($6, habit_daily_log.nudged),
            user_response = COALESCE(NULLIF($7, ''), habit_daily_log.user_response),
            inferred_from = COALESCE(NULLIF($8, ''), habit_daily_log.inferred_from)
        RETURNING
            user_id,
            habit_id::text AS habit_id,
            date,
            completed,
            nudged,
            user_response,
            inferred_from
        """,
        tenant_id,
        user_id,
        parsed_habit_id,
        today_utc,
        completed,
        nudged,
        _normalize_text(user_response) or None,
        _normalize_text(inferred_from) or None,
    )
    if not row:
        raise HTTPException(status_code=404, detail="Active habit not found for user")
    return row


async def _get_session_ingest_freshness(
    tenant_id: str,
    user_id: str,
    reference_now_utc: datetime,
    session_id: Optional[str] = None
) -> Dict[str, Any]:
    filters = ["tenant_id = $1", "user_id = $2", "status = 'pending'", "job_type IN ($3, $4)"]
    params: List[Any] = [
        tenant_id,
        user_id,
        session.JOB_TYPE_SESSION_RAW_EPISODE,
        session.JOB_TYPE_POST_INGEST_HOOK,
    ]
    if session_id:
        filters.append("session_id = $5")
        params.append(session_id)
    where = " AND ".join(filters)
    rows = await db.fetch(
        f"""
        SELECT session_id, job_type, status, next_attempt_at, created_at
        FROM graphiti_outbox
        WHERE {where}
        ORDER BY created_at ASC
        LIMIT 200
        """,
        *params
    )
    pending_raw = 0
    pending_hooks = 0
    oldest_created_at: Optional[datetime] = None
    latest_pending_session_id: Optional[str] = None
    for row in rows or []:
        if row.get("job_type") == session.JOB_TYPE_SESSION_RAW_EPISODE:
            pending_raw += 1
        elif row.get("job_type") == session.JOB_TYPE_POST_INGEST_HOOK:
            pending_hooks += 1
        created_at = row.get("created_at")
        if isinstance(created_at, datetime):
            if oldest_created_at is None or created_at < oldest_created_at:
                oldest_created_at = created_at
        sid = _normalize_text(row.get("session_id"))
        if sid:
            latest_pending_session_id = sid
    backlog_age_seconds = None
    if oldest_created_at:
        backlog_age_seconds = int(
            max(0, (reference_now_utc - oldest_created_at.astimezone(dt_timezone.utc)).total_seconds())
        )
    has_backlog = (pending_raw + pending_hooks) > 0
    return {
        "has_pending_session_ingest_jobs": has_backlog,
        "pending_raw_episode_jobs": pending_raw,
        "pending_post_ingest_hook_jobs": pending_hooks,
        "oldest_pending_age_seconds": backlog_age_seconds,
        "latest_pending_session_id": latest_pending_session_id,
    }


async def _execute_post_ingest_hook(hook_name: str, payload: Dict[str, Any]) -> bool:
    tenant_id = _normalize_text(payload.get("tenant_id"))
    user_id = _normalize_text(payload.get("user_id"))
    session_id = _normalize_text(payload.get("session_id"))
    reference_time = _parse_optional_dt(payload.get("reference_time")) or datetime.utcnow()

    if not tenant_id or not user_id or not session_id:
        raise TypeError("validation error: invalid post-ingest hook payload identity")

    transcript_row = await db.fetchone(
        """
        SELECT messages
        FROM session_transcript
        WHERE tenant_id = $1 AND session_id = $2
        """,
        tenant_id,
        session_id
    )
    messages_raw = transcript_row.get("messages") if transcript_row else []
    if isinstance(messages_raw, str):
        try:
            messages_raw = json.loads(messages_raw)
        except Exception:
            messages_raw = []
    messages_payload = [
        m for m in (messages_raw if isinstance(messages_raw, list) else [])
        if isinstance(m, dict)
    ]
    messages_payload = session.SessionManager._cap_messages_for_processing(
        messages_payload,
        max_messages=session.SESSION_INGEST_HOOK_MAX_MESSAGES,
        max_chars=session.SESSION_INGEST_HOOK_MAX_CHARS
    )

    if hook_name == session.POST_INGEST_HOOK_SESSION_SUMMARY:
        if not messages_payload:
            return True
        summary_payload = await session.summarize_session_messages(messages_payload)
        summary_text = (summary_payload or {}).get("summary_text")
        if not summary_text:
            return True
        response = await graphiti_client.add_session_summary(
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            summary_text=summary_text,
            bridge_text=(summary_payload or {}).get("bridge_text"),
            reference_time=reference_time,
            episode_uuid=_normalize_text(payload.get("episode_uuid")) or None,
            extra_attributes={
                "summary_quality_tier": (summary_payload or {}).get("summary_quality_tier"),
                "summary_source": (summary_payload or {}).get("summary_source"),
                "summary_facts": (summary_payload or {}).get("summary_facts"),
                "tone": (summary_payload or {}).get("tone"),
                "moment": (summary_payload or {}).get("moment"),
                "decisions": (summary_payload or {}).get("decisions") or [],
                "unresolved": (summary_payload or {}).get("unresolved") or [],
                "index_text": (summary_payload or {}).get("index_text") or "",
                "salience": (summary_payload or {}).get("salience") or "low",
            },
            replace_existing_session=True
        )
        if isinstance(response, dict) and response.get("success") is False:
            session.SessionManager._raise_for_graphiti_response_failure(
                response,
                context="session_summary_hook"
            )
        return True

    if hook_name == session.POST_INGEST_HOOK_OPEN_LOOPS:
        if not messages_payload:
            return True
        last_user_text = next(
            (
                m.get("text")
                for m in reversed(messages_payload)
                if isinstance(m, dict) and (m.get("role") or "").lower() == "user" and m.get("text")
            ),
            None
        )
        if not last_user_text:
            fallback = messages_payload[-1] if messages_payload else {}
            if isinstance(fallback, dict):
                last_user_text = _normalize_text(fallback.get("text"))
        if not last_user_text:
            return True
        ts_values = [_parse_optional_dt((m or {}).get("timestamp")) for m in messages_payload if isinstance(m, dict)]
        ts_values = [t for t in ts_values if t]
        start_ts = min(ts_values) if ts_values else None
        end_ts = max(ts_values) if ts_values else reference_time
        await loops.extract_and_create_loops(
            tenant_id=tenant_id,
            user_id=user_id,
            persona_id="default",
            user_text=last_user_text,
            recent_turns=messages_payload,
            source_turn_ts=end_ts or datetime.utcnow(),
            session_id=session_id,
            provenance={
                "session_id": session_id,
                "start_ts": start_ts.isoformat() if start_ts else None,
                "end_ts": end_ts.isoformat() if end_ts else None
            }
        )
        return True

    if hook_name == session.POST_INGEST_HOOK_EXTRACT_RESULTS:
        settings = get_settings()
        if not bool(settings.extract_results_enabled):
            return True
        await persist_extract_result(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages_payload,
            extractor_model_version=str(settings.extract_results_model_version or "").strip() or "t4-extractor-v1",
            prompt_version=str(settings.extract_results_prompt_version or "").strip() or "t4-prompt-v1",
            policy_version=_normalize_text(settings.extract_results_policy_version) or None,
            reference_time=reference_time,
            candidate_payload=payload.get("candidate_payload"),
            raw_output=payload.get("raw_output"),
            metadata={
                "hook": session.POST_INGEST_HOOK_EXTRACT_RESULTS,
                "episode_uuid": _normalize_text(payload.get("episode_uuid")) or None,
                "turn_range": {
                    "start_index": 0 if messages_payload else None,
                    "end_index": (len(messages_payload) - 1) if messages_payload else None,
                },
            },
        )
        return True

    if hook_name in {
        session.POST_INGEST_HOOK_USER_MODEL_DELTA,
        session.POST_INGEST_HOOK_DAILY_ANALYSIS,
    }:
        raise TypeError(f"validation error: hook disabled by policy {hook_name}")

    raise TypeError(f"validation error: unknown post-ingest hook {hook_name}")


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
        session.set_post_ingest_hook_executor(_execute_post_ingest_hook)
        logger.info("Post-ingest hook executor initialized")
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
        if settings.user_model_enrichment_enabled:
            app.state.user_model_enrichment_task = asyncio.create_task(
                user_model_enrichment_loop(
                    interval_seconds=settings.user_model_enrichment_interval_seconds,
                    max_users=settings.user_model_enrichment_max_users,
                    min_confidence=settings.user_model_enrichment_min_confidence,
                    daily_lookback_hours=settings.user_model_enrichment_daily_lookback_hours,
                    weekly_lookback_days=settings.user_model_enrichment_weekly_lookback_days,
                    retry_backoff_seconds=settings.user_model_enrichment_retry_backoff_seconds,
                    retry_max_seconds=settings.user_model_enrichment_retry_max_seconds
                )
            )
            logger.info("User model enrichment loop started")
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
            app.state.daily_habit_dedupe_task = asyncio.create_task(
                daily_habit_dedupe_loop(
                    interval_seconds=settings.daily_analysis_interval_seconds,
                    max_users=settings.daily_analysis_max_users,
                )
            )
            logger.info("Daily habit dedupe loop started")

    except Exception as e:
        logger.error(f"Failed to initialize services: {e}")
        raise

    yield

    # Shutdown
    logger.info("Shutting down Synapse Memory API")
    session.set_post_ingest_hook_executor(None)
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
    if getattr(app.state, "user_model_enrichment_task", None):
        app.state.user_model_enrichment_task.cancel()
        try:
            await app.state.user_model_enrichment_task
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
    if getattr(app.state, "daily_habit_dedupe_task", None):
        app.state.daily_habit_dedupe_task.cancel()
        try:
            await app.state.daily_habit_dedupe_task
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


@app.middleware("http")
async def tenant_alias_normalization_middleware(request: Request, call_next):
    scope = request.scope
    query_string = scope.get("query_string", b"")
    if query_string:
        try:
            params = parse_qsl(query_string.decode("latin-1"), keep_blank_values=True)
            changed = False
            rebuilt: List[Tuple[str, str]] = []
            for key, value in params:
                if key in {"tenantId", "tenant_id"}:
                    canonical = _canonical_tenant_id(value)
                    if canonical != value:
                        changed = True
                    rebuilt.append((key, str(canonical)))
                else:
                    rebuilt.append((key, value))
            if changed:
                scope["query_string"] = urlencode(rebuilt, doseq=True).encode("latin-1")
        except Exception:
            pass

    content_type = _normalize_text(request.headers.get("content-type")).lower()
    should_normalize_body = (
        request.method.upper() in {"POST", "PUT", "PATCH"}
        and "application/json" in content_type
    )
    if not should_normalize_body:
        return await call_next(request)

    try:
        raw_body = await request.body()
    except Exception:
        raw_body = b""
    if not raw_body:
        return await call_next(request)

    try:
        parsed = json.loads(raw_body.decode("utf-8"))
    except Exception:
        return await call_next(request)

    normalized = _normalize_tenant_aliases_in_payload(parsed)
    if normalized == parsed:
        return await call_next(request)

    new_body = json.dumps(normalized, ensure_ascii=True).encode("utf-8")

    async def _receive():
        return {"type": "http.request", "body": new_body, "more_body": False}

    rewritten_request = Request(scope, _receive)
    return await call_next(rewritten_request)


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

    except (EvidenceContractError, ExtractionContractError) as e:
        raise HTTPException(
            status_code=400,
            detail={"code": e.code, "message": e.message},
        )
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


def _parse_reference_time_or_none(raw_value: Optional[str]) -> Optional[datetime]:
    value = _normalize_text(raw_value)
    if not value:
        return None
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value)


async def _v2_collect_factual_items(
    *,
    tenant_scope: List[str],
    canonical_tenant: str,
    user_id: str,
    query: str,
    limit: int,
    reference_time: Optional[datetime],
) -> List[MemoryQueryV2Item]:
    safe_limit = max(1, min(int(limit or 10), 50))
    fetch_limit = max(20, min(safe_limit * 6, 260))
    query_terms = _query_terms(query)
    now_utc = (
        reference_time.astimezone(dt_timezone.utc)
        if isinstance(reference_time, datetime) and reference_time.tzinfo is not None
        else datetime.utcnow().replace(tzinfo=dt_timezone.utc)
    )
    rows = await db.fetch(
        """
        SELECT
          c.tenant_id,
          c.claim_slot_key,
          c.claim_event_key,
          c.lifecycle_status,
          c.predicate,
          c.subject_text,
          c.object_payload,
          c.extraction_confidence,
          c.truth_confidence,
          c.predicate_policy_version,
          c.updated_at,
          e.canonical_name AS subject_name,
          COALESCE(
            jsonb_agg(
              jsonb_build_object(
                'claimEvidenceId', ce.claim_evidence_id,
                'sessionId', ce.session_id,
                'turnIndex', t.turn_index,
                'charStart', ce.evidence_start_char,
                'charEnd', ce.evidence_end_char,
                'evidenceText', COALESCE(ce.evidence_text, t.content)
              )
              ORDER BY t.turn_index ASC NULLS LAST, ce.claim_evidence_id ASC
            ) FILTER (WHERE ce.claim_evidence_id IS NOT NULL),
            '[]'::jsonb
          ) AS evidence_links
        FROM claims c
        LEFT JOIN entities e
          ON e.tenant_id = c.tenant_id
         AND e.entity_id = c.subject_entity_id
        LEFT JOIN claim_evidence ce
          ON ce.tenant_id = c.tenant_id
         AND ce.claim_id = c.claim_id
        LEFT JOIN turns_v2 t
          ON t.tenant_id = ce.tenant_id
         AND t.session_id = ce.session_id
         AND t.turn_id = ce.turn_id
        WHERE c.tenant_id = ANY($1::text[])
          AND c.user_id = $2
          AND c.lifecycle_status = 'active'
        GROUP BY
          c.tenant_id,
          c.claim_slot_key,
          c.claim_event_key,
          c.lifecycle_status,
          c.predicate,
          c.subject_text,
          c.object_payload,
          c.extraction_confidence,
          c.truth_confidence,
          c.predicate_policy_version,
          c.updated_at,
          e.canonical_name
        ORDER BY c.updated_at DESC NULLS LAST, c.claim_event_key ASC
        LIMIT $3
        """,
        tenant_scope,
        user_id,
        fetch_limit,
    )
    ranked: List[Tuple[float, str, Dict[str, Any]]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        evidence = row.get("evidence_links")
        if not isinstance(evidence, list) or not evidence:
            # Hard rule: no factual row without evidence links.
            continue
        line = _canonical_claim_line(row)
        if not line:
            continue
        base_score = 0.15 + (0.12 * _normalize_confidence(row.get("truth_confidence"), default=0.5))
        score = _score_text_match(
            text=line,
            query_terms=query_terms,
            reference_ts=row.get("updated_at"),
            now_utc=now_utc,
            base_score=base_score,
        )
        if query_terms and score < 0.08:
            continue
        event_key = _normalize_text(row.get("claim_event_key"))
        if not event_key:
            continue
        ranked.append((score, event_key, row))

    ranked.sort(key=lambda item: (-float(item[0]), item[1]))
    items: List[MemoryQueryV2Item] = []
    for score, _event_key, row in ranked[:safe_limit]:
        evidence_links = row.get("evidence_links")
        evidence_rows = evidence_links if isinstance(evidence_links, list) else []
        items.append(
            MemoryQueryV2Item(
                lane="factual",
                itemType="claim",
                text=_canonical_claim_line(row),
                relevance=round(float(score), 4),
                source="canonical_claims",
                sourceTenant=_normalize_text(row.get("tenant_id")) or canonical_tenant,
                derived=False,
                dataClassification="canonical factual",
                claimSlotKey=_normalize_text(row.get("claim_slot_key")) or None,
                claimEventKey=_normalize_text(row.get("claim_event_key")) or None,
                lifecycleStatus=_normalize_text(row.get("lifecycle_status")) or None,
                evidence=evidence_rows,
                metadata={
                    "predicate": _normalize_text(row.get("predicate")) or None,
                    "predicatePolicyVersion": _normalize_text(row.get("predicate_policy_version")) or None,
                    "truthConfidence": float(row.get("truth_confidence") or 0.0),
                    "extractionConfidence": float(row.get("extraction_confidence") or 0.0),
                    **_retrieval_row_metadata(
                        source_type="canonical factual",
                        derived=False,
                        evidence_backed=True,
                        data_classification="canonical factual",
                    ),
                },
            )
        )
    return items


async def _v2_collect_episodic_items(
    *,
    tenant_scope: List[str],
    canonical_tenant: str,
    user_id: str,
    query: str,
    limit: int,
    reference_time: Optional[datetime],
) -> List[MemoryQueryV2Item]:
    recall_items, _audit = await _build_episodic_recall_items(
        tenant_scope=tenant_scope,
        canonical_tenant=canonical_tenant,
        user_id=user_id,
        query=query,
        query_terms=_query_terms(query),
        reference_time=reference_time,
        entity_hints=[],
        limit=limit,
    )
    out: List[MemoryQueryV2Item] = []
    for item in recall_items:
        evidence = [x for x in (item.evidence or []) if _normalize_text(x)]
        out.append(
            MemoryQueryV2Item(
                lane="episodic",
                itemType="episode",
                text=_normalize_text(item.summary) or None,
                relevance=(round(float(item.score), 4) if item.score is not None else None),
                source="episodic_recall",
                sourceTenant=item.sourceTenant or canonical_tenant,
                derived=False,
                dataClassification="episodic",
                evidence=[{"evidenceText": value} for value in evidence],
                episodeId=item.episodeId,
                sessionId=item.sessionId,
                referenceTime=item.referenceTime,
                linkedEntities=item.linkedEntities or [],
                metadata={
                    **_retrieval_row_metadata(
                        source_type="episodic",
                        derived=False,
                        evidence_backed=bool(evidence),
                        data_classification="episodic",
                    ),
                },
            )
        )
    return out


async def _v2_collect_continuity_items(
    *,
    tenant_scope: List[str],
    canonical_tenant: str,
    user_id: str,
    query: str,
    limit: int,
    reference_time: Optional[datetime],
) -> List[MemoryQueryV2Item]:
    safe_limit = max(1, min(int(limit or 10), 50))
    tasks = [
        _pg_search_continuity_facts(
            tenant_id=tenant_id,
            user_id=user_id,
            query=query,
            limit=safe_limit,
            reference_time=reference_time,
        )
        for tenant_id in tenant_scope
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True) if tasks else []
    rows: List[Dict[str, Any]] = []
    for tenant_id, tenant_rows in zip(tenant_scope, results):
        if isinstance(tenant_rows, Exception):
            logger.warning("v2_memory_query continuity search failed tenant=%s user=%s err=%s", tenant_id, user_id, tenant_rows)
            continue
        for row in (tenant_rows or []):
            if not isinstance(row, dict):
                continue
            # Hard rule: continuity lane is derived-only.
            if not bool(row.get("derived")):
                continue
            if _normalize_text(row.get("source_type")).lower() != "derived continuity/projection":
                continue
            text = _normalize_text(row.get("text"))
            if not text:
                continue
            rows.append({**row, "tenant_id": tenant_id, "text": text})

    deduped: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for row in sorted(
        rows,
        key=lambda item: (
            float(item.get("relevance") or 0.0),
            _normalize_text(item.get("valid_at")),
            _normalize_text(item.get("text")),
        ),
        reverse=True,
    ):
        key = _normalize_text(row.get("text")).lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
        if len(deduped) >= safe_limit:
            break

    return [
        MemoryQueryV2Item(
            lane="continuity",
            itemType="continuity_fact",
            text=_normalize_text(row.get("text")),
            relevance=(round(float(row.get("relevance")), 4) if row.get("relevance") is not None else None),
            source=_normalize_text(row.get("source")) or "continuity_projection",
            sourceTenant=_normalize_text(row.get("tenant_id")) or canonical_tenant,
            derived=True,
            dataClassification="derived continuity/projection",
            evidence=[],
            metadata={
                **_retrieval_row_metadata(
                    source_type="derived continuity/projection",
                    derived=True,
                    evidence_backed=False,
                    data_classification="derived continuity/projection",
                ),
            },
        )
        for row in deduped
    ]


@app.post(
    "/v2/memory/query",
    response_model=MemoryQueryV2Response,
    response_model_exclude_none=True,
)
async def memory_query_v2(request: MemoryQueryV2Request):
    """
    Authoritative Synapse v2 retrieval contract with strict lane separation.
    """
    try:
        lane = _resolve_memory_lane_v2(request.lane)
        if lane not in {"factual", "episodic", "continuity", "hybrid"}:
            raise HTTPException(status_code=400, detail="Unsupported lane")
        canonical_tenant, tenant_scope = _resolve_tenant_scope(request.tenantId)
        reference_time = _parse_reference_time_or_none(request.referenceTime)
        limit = max(1, request.limit or 10)

        factual_items: List[MemoryQueryV2Item] = []
        episodic_items: List[MemoryQueryV2Item] = []
        continuity_items: List[MemoryQueryV2Item] = []

        if lane in {"factual", "hybrid"}:
            factual_items = await _v2_collect_factual_items(
                tenant_scope=tenant_scope,
                canonical_tenant=canonical_tenant,
                user_id=request.userId,
                query=request.query,
                limit=limit,
                reference_time=reference_time,
            )
        if lane in {"episodic", "hybrid"}:
            episodic_items = await _v2_collect_episodic_items(
                tenant_scope=tenant_scope,
                canonical_tenant=canonical_tenant,
                user_id=request.userId,
                query=request.query,
                limit=limit,
                reference_time=reference_time,
            )
        if lane in {"continuity", "hybrid"}:
            continuity_items = await _v2_collect_continuity_items(
                tenant_scope=tenant_scope,
                canonical_tenant=canonical_tenant,
                user_id=request.userId,
                query=request.query,
                limit=limit,
                reference_time=reference_time,
            )

        if lane == "factual":
            items = factual_items
        elif lane == "episodic":
            items = episodic_items
        elif lane == "continuity":
            items = continuity_items
        else:
            items = factual_items + episodic_items + continuity_items

        return MemoryQueryV2Response(
            lane=lane,
            items=items,
            metadata={
                "query": request.query,
                "lane": lane,
                "limit": limit,
                "tenantCanonical": canonical_tenant,
                "tenantScope": tenant_scope,
                "counts": {
                    "factual": len(factual_items),
                    "episodic": len(episodic_items),
                    "continuity": len(continuity_items),
                    "total": len(items),
                },
                "lanes": {
                    "factual": {"derived": False, "requiresEvidence": True},
                    "episodic": {"derived": False, "requiresEvidence": False},
                    "continuity": {"derived": True, "requiresEvidence": False},
                },
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"v2 memory query failed: {e}")
        raise HTTPException(status_code=500, detail="v2 memory query failed")


def _legacy_intent_to_v2_lane(memory_intent: str) -> str:
    intent = _resolve_memory_intent(memory_intent)
    if intent == "exact":
        return "factual"
    if intent == "episodic":
        return "episodic"
    return "hybrid"


def _map_v2_item_to_legacy_fact(item: MemoryQueryV2Item) -> Optional[Fact]:
    text = _normalize_text(item.text)
    if not text:
        return None
    return Fact(
        text=text,
        relevance=item.relevance,
        source=item.source,
        sourceTenant=item.sourceTenant,
    )


def _map_v2_item_to_legacy_episode(item: MemoryQueryV2Item) -> Optional[EpisodeRecallItem]:
    if item.itemType != "episode":
        return None
    evidence_lines = []
    for row in (item.evidence or []):
        if not isinstance(row, dict):
            continue
        line = _normalize_text(row.get("evidenceText"))
        if line:
            evidence_lines.append(line)
    return EpisodeRecallItem(
        episodeId=item.episodeId,
        sessionId=item.sessionId,
        referenceTime=item.referenceTime,
        score=item.relevance,
        summary=item.text,
        evidence=evidence_lines,
        linkedEntities=item.linkedEntities or [],
        sourceTenant=item.sourceTenant,
    )


async def _memory_query_legacy_adapter(request: MemoryQueryRequest) -> MemoryQueryResponse:
    """
    T11 transitional compatibility adapter.
    Legacy /memory/query maps to v2 lanes and response contracts without invoking legacy mixed-authority retrieval.
    TODO(T15): remove once legacy clients are fully migrated to /v2/memory/query.
    """
    memory_intent = _resolve_memory_intent(request.memoryIntent)
    lane = _legacy_intent_to_v2_lane(memory_intent)
    v2_response = await memory_query_v2(
        MemoryQueryV2Request(
            tenantId=request.tenantId,
            userId=request.userId,
            query=request.query,
            lane=lane,
            limit=request.limit,
            referenceTime=request.referenceTime,
        )
    )

    fact_items: List[Fact] = []
    episodes: List[EpisodeRecallItem] = []
    continuity_items: List[Fact] = []
    for item in (v2_response.items or []):
        if item.lane == "factual":
            mapped = _map_v2_item_to_legacy_fact(item)
            if mapped is not None:
                fact_items.append(mapped)
            continue
        if item.lane == "episodic":
            mapped_episode = _map_v2_item_to_legacy_episode(item)
            if mapped_episode is not None:
                episodes.append(mapped_episode)
            continue
        if item.lane == "continuity":
            # Conservative adapter degradation: continuity remains explicitly derived.
            mapped_continuity = _map_v2_item_to_legacy_fact(item)
            if mapped_continuity is not None:
                continuity_items.append(mapped_continuity)

    # Keep legacy shape stable while making adapter origin explicit for clients/operators.
    facts = [item.text for item in fact_items if _normalize_text(item.text)]
    if memory_intent == "hybrid":
        facts.extend([item.text for item in continuity_items if _normalize_text(item.text)])

    metadata = {
        "query": request.query,
        "memoryIntent": memory_intent,
        "responseMode": "context" if request.includeContext else "recall",
        "adapter": {
            "mode": "t11_legacy_to_v2_only",
            "v2Lane": lane,
            "legacyMixedAuthorityFallback": False,
            "continuityMappedAsDerivedFacts": bool(memory_intent == "hybrid" and continuity_items),
            "includeContextSupported": False,
        },
        "v2Metadata": v2_response.metadata,
        "facts": len(fact_items),
        "entities": 0,
        "episodes": len(episodes),
        "continuityFacts": len(continuity_items),
    }
    if request.includeContext:
        metadata["contextCompatibilityNotice"] = "Legacy includeContext is not available in T11 adapter mode."

    return MemoryQueryResponse(
        facts=facts,
        factItems=(fact_items + continuity_items) if memory_intent == "hybrid" else fact_items,
        entities=[],
        episodes=episodes,
        metadata=metadata,
    )


@app.post(
    "/memory/query",
    response_model=MemoryQueryResponse,
    response_model_exclude_none=True
)
async def memory_query(request: MemoryQueryRequest):
    """
    Legacy compatibility endpoint.
    """
    # T11 transitional rule: route exclusively through v2 adapter; no mixed-authority legacy fallback.
    return await _memory_query_legacy_adapter(request)

    # Legacy implementation retained below for temporary reference during migration;
    # unreachable by design in T11 and scheduled for removal in T15.
    try:
        fact_limit = max(1, request.limit or 10)
        memory_intent = _resolve_memory_intent(request.memoryIntent)
        exact_enabled = memory_intent in {"exact", "hybrid"}
        episodic_enabled = memory_intent in {"episodic", "hybrid"}
        canonical_tenant, tenant_scope = _resolve_tenant_scope(request.tenantId)
        reference_time = None
        if request.referenceTime:
            value = request.referenceTime
            if value.endswith("Z"):
                value = value.replace("Z", "+00:00")
            reference_time = datetime.fromisoformat(value)
        query_terms = _query_terms(request.query)
        requested_node_types = infer_query_node_types(request.query)
        identity_target = _extract_narrow_entity_identity_target(request.query) if exact_enabled else None

        user_model_rows = await _fetch_user_model_rows_for_scope(tenant_scope=tenant_scope, user_id=request.userId)
        primary_user_model = {}
        if user_model_rows:
            primary_user_model = _hydrate_user_model_narratives(
                user_model_rows[0].get("model"),
                row=user_model_rows[0] if isinstance(user_model_rows[0], dict) else None,
            )

        scoped_entity = None
        if identity_target:
            try:
                scoped_entity = await _resolve_canonical_entity_candidate(
                    tenant_id=canonical_tenant,
                    user_id=request.userId,
                    reference_time=reference_time or datetime.utcnow().replace(tzinfo=dt_timezone.utc),
                    entity_id=None,
                    name=identity_target,
                    user_model=primary_user_model,
                )
            except Exception as e:
                logger.warning("memory_query entity resolution failed tenant=%s user=%s target=%s err=%s", canonical_tenant, request.userId, identity_target, e)
                scoped_entity = None

        # GRAPHITI_REPLACED: search_facts (exact semantic fact retrieval)
        # TEMPORARY_DEGRADED_REPLACEMENT: strict factual path only admits canonical evidence-backed rows.
        graphiti_fact_tasks = (
            [
                _pg_search_facts(
                    tenant_id=tenant_id,
                    user_id=request.userId,
                    query=request.query,
                    limit=fact_limit,
                    reference_time=reference_time,
                    include_derived=False,
                )
                for tenant_id in tenant_scope
            ]
            if exact_enabled and not scoped_entity
            else []
        )
        # GRAPHITI_REPLACED: search_nodes (entity retrieval for memory query)
        # TEMPORARY_DEGRADED_REPLACEMENT: node/entity payload is continuity-oriented and not canonical factual evidence.
        graphiti_node_tasks = [
            _pg_search_nodes(
                tenant_id=tenant_id,
                user_id=request.userId,
                query=request.query,
                limit=min(request.limit or 10, 10),
                reference_time=reference_time,
                allowed_types=requested_node_types or ONTOLOGY_NODE_TYPES,
            )
            for tenant_id in tenant_scope
        ]
        graphiti_fact_results, graphiti_node_results = await asyncio.gather(
            asyncio.gather(*graphiti_fact_tasks, return_exceptions=True) if graphiti_fact_tasks else asyncio.sleep(0, result=[]),
            asyncio.gather(*graphiti_node_tasks, return_exceptions=True),
        )

        fact_items: List[Dict[str, Any]] = []
        for tenant_id, rows in zip(tenant_scope, graphiti_fact_results):
            if isinstance(rows, Exception):
                logger.warning("memory_query fact search failed tenant=%s user=%s err=%s", tenant_id, request.userId, rows)
                continue
            for row in (rows or []):
                if not isinstance(row, dict):
                    continue
                enriched = dict(row)
                enriched["tenant_id"] = tenant_id
                fact_items.append(enriched)

        now_utc = (
            reference_time.astimezone(dt_timezone.utc)
            if isinstance(reference_time, datetime) and reference_time.tzinfo is not None
            else (
                reference_time.replace(tzinfo=dt_timezone.utc)
                if isinstance(reference_time, datetime)
                else datetime.utcnow().replace(tzinfo=dt_timezone.utc)
            )
        )
        temporal_rows: List[Dict[str, Any]] = []
        for idx, row in enumerate(fact_items):
            include, tier, priority = _fact_temporal_relevance(row, now_utc)
            if not include:
                continue
            temporal_rows.append({
                "row": row,
                "tier": tier,
                "priority": priority,
                "idx": idx,
            })
        temporal_rows.sort(key=lambda item: (item["priority"], item["idx"]))
        ordered_fact_items = [item["row"] for item in temporal_rows]
        tier_by_text: Dict[str, str] = {}
        for item in temporal_rows:
            text_key = _normalize_text(item["row"].get("text")).lower()
            if text_key and text_key not in tier_by_text:
                tier_by_text[text_key] = item["tier"]

        entities_raw: List[Dict[str, Any]] = []
        for tenant_id, nodes in zip(tenant_scope, graphiti_node_results):
            if isinstance(nodes, Exception):
                logger.warning("memory_query node search failed tenant=%s user=%s err=%s", tenant_id, request.userId, nodes)
                continue
            for node in (nodes or []):
                if not isinstance(node, dict):
                    continue
                enriched = dict(node)
                enriched["tenant_id"] = tenant_id
                entities_raw.append(enriched)

        def _include_memory_query_node(node: Any) -> bool:
            if not isinstance(node, dict):
                return False
            summary = canonicalize_entity_name(node.get("summary"))
            labels = node.get("labels")
            node_type = infer_ontology_type(
                node.get("type"),
                labels=labels if isinstance(labels, list) else None,
                name=summary,
            )
            return is_allowed_runtime_node(
                name=summary,
                raw_type=node_type,
                labels=labels if isinstance(labels, list) else None,
                allowed_types=requested_node_types or ONTOLOGY_NODE_TYPES,
                include_internal=False,
            )

        entities = [node for node in (entities_raw or []) if _include_memory_query_node(node)]

        fact_candidates: List[Dict[str, Any]] = []
        if exact_enabled:
            for row in ordered_fact_items:
                text = _normalize_text(row.get("text"))
                if not text:
                    continue
                if not _is_canonical_factual_row(row):
                    continue
                if not (_allow_claim(text) and _allow_fact_text(text) and not _is_explicit_user_state_claim(text)):
                    continue
                fact_candidates.append({
                    "text": text,
                    "relevance": row.get("relevance"),
                    "source": _normalize_text(row.get("source")) or "graphiti",
                    "relevance_tier": tier_by_text.get(text.lower()),
                    "tenant_id": _normalize_text(row.get("tenant_id")) or canonical_tenant,
                    "source_type": row.get("source_type"),
                    "derived": row.get("derived"),
                    "evidence_backed": row.get("evidence_backed"),
                    "data_classification": row.get("data_classification"),
                })

        # T1 containment: do not inject derived/projection prose into factual candidates.
        # Specifically blocked sources: session summary prose and user_model prose.

        if exact_enabled and scoped_entity:
            fact_candidates = await _build_narrow_identity_fact_candidates(
                tenant_id=canonical_tenant,
                user_id=request.userId,
                selected_entity=scoped_entity,
                user_model=primary_user_model,
                limit=fact_limit,
            )
            entities = [scoped_entity.get("raw")] if isinstance(scoped_entity.get("raw"), dict) else []

        deduped_fact_rows: List[Dict[str, Any]] = []
        fact_index: Dict[str, int] = {}
        for candidate in fact_candidates:
            text = _normalize_text(candidate.get("text"))
            if not text:
                continue
            key = text.lower()
            existing_idx = fact_index.get(key)
            if existing_idx is None:
                fact_index[key] = len(deduped_fact_rows)
                deduped_fact_rows.append(candidate)
                continue
            current = deduped_fact_rows[existing_idx]
            current_score = float(current.get("relevance") or 0.0)
            new_score = float(candidate.get("relevance") or 0.0)
            if new_score > current_score:
                deduped_fact_rows[existing_idx] = candidate

        deduped_fact_rows.sort(
            key=lambda row: (
                float(row.get("relevance") or 0.0),
                1 if str(row.get("source")).startswith("graphiti") else 0,
            ),
            reverse=True,
        )
        deduped_fact_rows = deduped_fact_rows[:fact_limit]

        semantic_items = [
            {
                "text": _normalize_text(row.get("text")),
                "source": _normalize_text(row.get("source")),
            }
            for row in deduped_fact_rows
            if _normalize_text(row.get("text"))
        ]
        semantic_audit: Dict[str, Any] = {}
        semantic_enabled = bool(get_settings().memory_semantic_enabled)
        semantic_results = await classify_memory_candidates_semantic(
            semantic_items,
            llm_client=get_llm_client(),
            semantic_enabled=semantic_enabled,
            enable_embedding_fallback=bool(get_settings().memory_semantic_embedding_enabled),
            embedding_model=get_settings().memory_semantic_embedding_model,
            audit_stats=semantic_audit,
        )
        fallback_semantic_results = [
            classify_memory_semantic_fallback(
                item.get("text"),
                source_hint=item.get("source"),
            )
            for item in semantic_items
        ]

        query_semantic_rows = await classify_memory_candidates_semantic(
            [{"text": _normalize_text(request.query), "source": "query"}],
            llm_client=get_llm_client(),
            semantic_enabled=semantic_enabled,
            enable_embedding_fallback=bool(get_settings().memory_semantic_embedding_enabled),
            embedding_model=get_settings().memory_semantic_embedding_model,
            audit_stats=None,
        )
        query_semantic = (
            query_semantic_rows[0]
            if query_semantic_rows
            else classify_memory_semantic_fallback(request.query, source_hint="query")
        )
        query_domain = _normalize_text(query_semantic.domain)
        query_intent = _normalize_text(query_semantic.intent)
        query_memory_type = _normalize_text(query_semantic.memory_type)
        query_domain_scores = query_semantic.domain_scores or {}
        ranked_query_domains = sorted(
            (query_domain_scores or {}).items(),
            key=lambda kv: float(kv[1]),
            reverse=True,
        )
        query_domain_peak = float(ranked_query_domains[0][1]) if ranked_query_domains else 0.0
        query_domain_focus: set[str] = set()
        for idx, (key, raw_score) in enumerate(ranked_query_domains):
            score = float(raw_score or 0.0)
            # Keep focus sparse so weak query-domain probabilities do not add noise.
            if idx >= 2 and score < 0.22:
                continue
            normalized = _normalize_text(key)
            if normalized:
                query_domain_focus.add(normalized)
            if len(query_domain_focus) >= 4:
                break
        if query_domain_peak < 0.20:
            # Low-certainty query domain outputs are treated as weak evidence.
            query_domain_focus.clear()
        if query_domain and not query_domain_focus:
            query_domain_focus.add(query_domain)

        emotion_signal = max(
            float(query_domain_scores.get("worries") or 0.0),
            float(query_domain_scores.get("wellness") or 0.0),
            float(query_domain_scores.get("relationships") or 0.0),
        )
        finance_signal = float(query_domain_scores.get("finance") or 0.0)
        emotion_focus_query = (
            query_intent in {"express_emotion", "reflect"}
            or emotion_signal >= 0.34
        )
        finance_focus_query = (
            query_domain == "finance"
            or finance_signal >= 0.34
            or ("finance" in query_domain_focus and query_intent in {"express_emotion", "reflect", "share_update"})
        )

        def _semantic_rank_bonus(
            semantic: Optional[Any],
            row_source: Optional[str] = None,
            fallback_semantic: Optional[Any] = None,
        ) -> float:
            if not semantic_enabled:
                return 0.0
            if not semantic:
                return 0.0
            score = 0.0
            source = _normalize_text(row_source)
            domain = _normalize_text(getattr(semantic, "domain", None))
            if domain and domain in query_domain_focus:
                score += 0.10
            domain_scores = getattr(semantic, "domain_scores", None) or {}
            if isinstance(domain_scores, dict):
                for domain_key in query_domain_focus:
                    score += min(0.03, float(domain_scores.get(domain_key) or 0.0) * 0.06)
            intent = _normalize_text(getattr(semantic, "intent", None))
            if query_intent and intent and intent == query_intent:
                score += 0.05
            memory_type = _normalize_text(getattr(semantic, "memory_type", None))
            if (
                query_intent not in {"make_commitment", "reflect"}
                and query_memory_type
                and memory_type
                and memory_type == query_memory_type
            ):
                score += 0.05
            if query_intent == "make_commitment":
                if intent in {"make_commitment", "reflect", "express_emotion"}:
                    score += 0.06
                if memory_type in {"habit", "goal", "event", "relationship"}:
                    score += 0.06
                if intent == "ask_help" and memory_type in {"relationship", "state", "habit"}:
                    score += 0.03
                if intent == "share_update" and memory_type in {"state", "fact"}:
                    score -= 0.05
            elif query_intent == "reflect":
                if intent in {"reflect", "express_emotion", "make_commitment"}:
                    score += 0.05
                if memory_type in {"state", "habit", "relationship", "goal"}:
                    score += 0.05
            if (
                source == "user_model"
                and query_intent in {"make_commitment", "reflect"}
                and intent in {"reflect", "share_update"}
                and memory_type in {"habit", "state", "fact"}
            ):
                score -= 0.05
            fallback_intent = _normalize_text(getattr(fallback_semantic, "intent", None))
            fallback_memory_type = _normalize_text(getattr(fallback_semantic, "memory_type", None))
            # Soft tie-break: if the semantic classifier is generic but fallback carries
            # a stronger intent signal, use it as a small nudge (never as primary logic).
            if (
                query_intent in {"make_commitment", "reflect", "express_emotion"}
                and intent == "share_update"
                and fallback_intent
                and fallback_intent != "share_update"
            ):
                score += 0.04
                if fallback_intent == query_intent:
                    score += 0.03
                if fallback_memory_type in {"habit", "goal", "relationship", "state"}:
                    score += 0.02
            confidence = float(getattr(semantic, "confidence", 0.0) or 0.0)
            score += min(0.12, max(0.0, confidence) * 0.12)

            # Query-text guardrails to reduce broad share_update/event over-ranking on
            # emotional and finance retrieval asks when query semantic fallback is noisy.
            if emotion_focus_query:
                if intent in {"express_emotion", "reflect"}:
                    score += 0.11
                if memory_type in {"state", "habit", "relationship"}:
                    score += 0.06
                if domain in {"worries", "wellness", "relationships", "finance"}:
                    score += 0.05
                if intent in {"share_update", "ask_help"}:
                    score -= 0.07
                if memory_type in {"event", "goal", "preference"}:
                    score -= 0.05
                if source == "user_model" and intent == "share_update" and memory_type == "event":
                    score -= 0.08

            if finance_focus_query:
                if domain in {"finance", "worries"}:
                    score += 0.14
                if intent == "express_emotion":
                    score += 0.08
                if memory_type in {"state", "habit", "relationship"}:
                    score += 0.04
                if domain == "work" and intent == "share_update" and memory_type == "event":
                    score -= 0.14
                if source == "user_model" and domain not in {"finance", "worries"} and memory_type == "event":
                    score -= 0.10
            return score

        reranked_rows: List[Tuple[float, int, Dict[str, Any], Optional[Any]]] = []
        for idx, row in enumerate(deduped_fact_rows):
            semantic = semantic_results[idx] if idx < len(semantic_results) else None
            fallback_semantic = (
                fallback_semantic_results[idx]
                if idx < len(fallback_semantic_results)
                else None
            )
            base_score = float(row.get("relevance") or 0.0)
            semantic_bonus = _semantic_rank_bonus(
                semantic,
                row_source=row.get("source"),
                fallback_semantic=fallback_semantic,
            )
            source_bonus = 0.03 if str(row.get("source")).startswith("graphiti") else 0.0
            final_score = base_score + semantic_bonus + source_bonus
            reranked_rows.append((final_score, idx, row, semantic))
        reranked_rows.sort(key=lambda item: item[0], reverse=True)
        reranked_rows = reranked_rows[:fact_limit]

        fact_models: List[Fact] = []
        memory_candidates: List[Dict[str, Any]] = []
        for final_score, _idx, row, semantic in reranked_rows:
            text = _normalize_text(row.get("text"))
            if not text:
                continue
            fact_models.append(
                Fact(
                    text=text,
                    relevance=round(final_score, 4),
                    source=row.get("source"),
                    relevance_tier=row.get("relevance_tier"),
                    domain=(semantic.domain if semantic else None),
                    intent=(semantic.intent if semantic else None),
                    memoryType=(semantic.memory_type if semantic else None),
                    domainScores=(semantic.domain_scores if semantic else None),
                    confidence=(semantic.confidence if semantic else None),
                    classificationMethod=(semantic.classification_method if semantic else None),
                    sourceTenant=_normalize_text(row.get("tenant_id")) or canonical_tenant,
                )
            )
            if semantic:
                memory_candidates.append(
                    {
                        "text": text,
                        "domain": semantic.domain,
                        "domain_scores": semantic.domain_scores,
                        "intent": semantic.intent,
                        "memory_type": semantic.memory_type,
                        "confidence": semantic.confidence,
                        "classification_method": semantic.classification_method,
                        "source": _normalize_text(row.get("source")) or "unknown",
                    }
                )
        fact_texts = [item.text for item in fact_models]
        fact_texts_raw = fact_texts[:]

        entity_models = []
        for e in entities:
            if _is_session_summary_node(e):
                continue
            summary = _normalize_text(e.get("summary"))
            if not summary:
                continue
            entity_models.append(Entity(summary=summary, type=e.get("type"), uuid=e.get("uuid")))

        episodic_items: List[EpisodeRecallItem] = []
        episodic_audit: Dict[str, Any] = {}
        if episodic_enabled:
            episodic_items, episodic_audit = await _build_episodic_recall_items(
                tenant_scope=tenant_scope,
                canonical_tenant=canonical_tenant,
                user_id=request.userId,
                query=request.query,
                query_terms=query_terms,
                reference_time=reference_time,
                entity_hints=[entity.summary for entity in entity_models[:12] if _normalize_text(entity.summary)],
                limit=fact_limit,
            )
            try:
                coverage_stats = await get_user_episodic_embedding_stats(
                    db=db,
                    tenant_scope=tenant_scope,
                    user_id=request.userId,
                    reference_time=reference_time,
                )
            except Exception:
                coverage_stats = {"rows": 0, "sessions": 0}
            episodic_audit["embeddingCoverage"] = coverage_stats

        provenance_counts: Dict[str, int] = {}
        for item in fact_models:
            source = _normalize_text(item.source) or "unknown"
            provenance_counts[source] = int(provenance_counts.get(source) or 0) + 1

        confidence_bucket_counts = {"lt_0_50": 0, "0_50_to_0_69": 0, "0_70_to_0_84": 0, "ge_0_85": 0}
        for item in fact_models:
            conf = float(item.confidence or 0.0)
            if conf < 0.50:
                confidence_bucket_counts["lt_0_50"] += 1
            elif conf < 0.70:
                confidence_bucket_counts["0_50_to_0_69"] += 1
            elif conf < 0.85:
                confidence_bucket_counts["0_70_to_0_84"] += 1
            else:
                confidence_bucket_counts["ge_0_85"] += 1

        method_breakdown = summarize_label_distribution(
            [{"classification_method": item.classificationMethod} for item in fact_models],
            key="classification_method",
            default="unknown",
        )
        total_classified = max(1, len(fact_models))
        fallback_rate = float(method_breakdown.get("fallback", 0)) / float(total_classified)
        embedding_failure_rate = 1.0 if bool(semantic_audit.get("embedding_failed")) else 0.0
        logger.info(
            "memory_query_semantic_metrics tenant=%s user=%s methods=%s fallback_rate=%.3f embedding_failure_rate=%.3f confidence_buckets=%s",
            canonical_tenant,
            request.userId,
            method_breakdown,
            fallback_rate,
            embedding_failure_rate,
            confidence_bucket_counts,
        )

        response = MemoryQueryResponse(
            facts=fact_texts,
            factItems=fact_models,
            entities=entity_models,
            episodes=episodic_items,
            metadata={
                "query": request.query,
                "memoryIntent": memory_intent,
                "responseMode": "context" if request.includeContext else "recall",
                "facts": len(fact_models),
                "entities": len(entity_models),
                "episodes": len(episodic_items),
                "episodicWeakRecall": bool(episodic_audit.get("weakRecall")) if episodic_enabled else None,
                "limit": fact_limit,
                "tenantCanonical": canonical_tenant,
                "tenantScope": tenant_scope,
                "provenanceCounts": provenance_counts,
                "domainBreakdown": summarize_domain_distribution(
                    [{"domain": item.domain} for item in fact_models]
                ),
                "intentBreakdown": summarize_label_distribution(
                    [{"intent": item.intent} for item in fact_models],
                    key="intent",
                ),
                "memoryTypeBreakdown": summarize_label_distribution(
                    [{"memory_type": item.memoryType} for item in fact_models],
                    key="memory_type",
                ),
                "memoryCandidates": memory_candidates,
                "classificationMethodBreakdown": method_breakdown,
                "semanticClassificationAudit": semantic_audit,
                "semanticFallbackRate": fallback_rate,
                "embeddingFailureRate": embedding_failure_rate,
                "confidenceDistribution": confidence_bucket_counts,
                "rankingSignals": {
                    "queryDomain": query_domain,
                    "queryIntent": query_intent,
                    "queryMemoryType": query_memory_type,
                    "queryDomainFocus": sorted([x for x in query_domain_focus if x]),
                    "ontologyNodeTypes": requested_node_types or ONTOLOGY_NODE_TYPES,
                    "exactEntityScoped": bool(scoped_entity),
                    "exactEntityTarget": identity_target if scoped_entity else None,
                },
                "episodicRanking": {
                    "enabled": episodic_enabled,
                    "embeddingEnabled": bool(get_settings().episodic_embedding_enabled),
                    "embeddingModel": _normalize_text(get_settings().episodic_embedding_model) or None,
                    "queryProfile": _normalize_text(episodic_audit.get("queryProfile")) if episodic_enabled else None,
                    "weights": (
                        episodic_audit.get("effectiveWeights")
                        if isinstance(episodic_audit.get("effectiveWeights"), dict)
                        else {
                            "embeddingSimilarity": 0.50,
                            "lexicalOverlap": 0.20,
                            "recency": 0.12,
                            "linkedEntityOverlap": 0.18,
                            "userTurnDensity": 0.04,
                            "continuationIntentBonus": 0.05,
                        }
                    ),
                    "audit": episodic_audit,
                },
                "memoryOntology": {
                    "nodeTypes": ONTOLOGY_NODE_TYPES,
                    "edgeTypes": ONTOLOGY_EDGE_TYPES,
                    "domainFacets": ONTOLOGY_DOMAIN_FACETS,
                },
            }
        )
        if not request.includeContext:
            return response

        focus_query = _normalize_text(request.focusQuery) or "current focus priority focused on right now today i need"
        # GRAPHITI_REPLACED: search_nodes (focus node retrieval for includeContext)
        # TEMPORARY_DEGRADED_REPLACEMENT: includeContext focus nodes are continuity/debug aids (derived).
        focus_tasks = [
            _pg_search_nodes(
                tenant_id=tenant_id,
                user_id=request.userId,
                query=focus_query,
                limit=3,
                reference_time=reference_time,
                allowed_types=["goal", "loop", "project", "event"],
            )
            for tenant_id in tenant_scope
        ]
        focus_results = await asyncio.gather(*focus_tasks, return_exceptions=True)
        focus_nodes_raw: List[Dict[str, Any]] = []
        for tenant_id, rows in zip(tenant_scope, focus_results):
            if isinstance(rows, Exception):
                logger.warning("memory_query focus search failed tenant=%s user=%s err=%s", tenant_id, request.userId, rows)
                continue
            for row in (rows or []):
                if isinstance(row, dict):
                    enriched = dict(row)
                    enriched["tenant_id"] = tenant_id
                    focus_nodes_raw.append(enriched)
        focus_nodes = [node for node in (focus_nodes_raw or []) if _include_memory_query_node(node) and not _is_session_summary_node(node)]

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
        latest_session_id = await _get_latest_session_id(canonical_tenant, request.userId)
        last_interaction = None
        for tenant_id in tenant_scope:
            try:
                eps = await graphiti_client.get_recent_episode_summaries(
                    tenant_id=tenant_id,
                    user_id=request.userId,
                    limit=1
                )
                if eps:
                    last_interaction = eps[0].get("reference_time")
                    if last_interaction:
                        break
            except Exception:
                continue

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
        canonical_tenant, tenant_scope = _resolve_tenant_scope(tenantId)
        scoped_rows = await _fetch_user_model_rows_for_scope(tenant_scope=tenant_scope, user_id=userId)
        row = scoped_rows[0] if scoped_rows else None
        exists = bool(row)
        model = _hydrate_user_model_narratives(
            row.get("model") if row else None,
            row=row if isinstance(row, dict) else None,
        )
        metadata = _build_user_model_staleness_metadata(model)
        if isinstance(metadata, dict):
            metadata["tenantCanonical"] = canonical_tenant
            metadata["tenantScope"] = tenant_scope
            metadata["sourceTenant"] = _normalize_text(row.get("tenant_id")) if row else None
        return UserModelResponse(
            tenantId=canonical_tenant or tenantId,
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
            SELECT model, narrative_stable, narrative_current
            FROM user_model
            WHERE tenant_id = $1 AND user_id = $2
            """,
            request.tenantId,
            request.userId
        )

        current_model = _hydrate_user_model_narratives(
            existing.get("model") if existing else None,
            row=existing if isinstance(existing, dict) else None,
        )
        merged_model = _normalize_user_model(_deep_merge_patch(current_model, request.patch))
        narrative_stable = _normalize_text(merged_model.get("narrative_stable")) or None
        narrative_current = _normalize_text(merged_model.get("narrative_current")) or None

        row = await db.fetchone(
            """
            INSERT INTO user_model (
                tenant_id, user_id, model, version, last_source, created_at, updated_at,
                narrative_stable, narrative_current
            )
            VALUES ($1, $2, $3::jsonb, 1, $4, NOW(), NOW(), $5, $6)
            ON CONFLICT (tenant_id, user_id)
            DO UPDATE SET
                model = $3::jsonb,
                version = user_model.version + 1,
                last_source = COALESCE($4, user_model.last_source),
                narrative_stable = COALESCE($5, user_model.narrative_stable),
                narrative_current = COALESCE($6, user_model.narrative_current),
                updated_at = NOW()
            RETURNING model, version, created_at, updated_at, last_source
            """,
            request.tenantId,
            request.userId,
            merged_model,
            request.source,
            narrative_stable,
            narrative_current,
        )

        normalized = _hydrate_user_model_narratives(
            row.get("model") if row else merged_model,
            row=row if isinstance(row, dict) else None,
        )
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


@app.get("/signals/pack")
async def signals_pack(
    tenantId: str,
    userId: str,
    sessionId: Optional[str] = None,
    now: Optional[str] = None,
    capacity: Optional[str] = None,
    tacticAppetite: Optional[str] = None,
):
    return await _build_signals_pack(
        tenantId=tenantId,
        userId=userId,
        sessionId=sessionId,
        now=now,
        capacity=capacity,
        tactic_appetite=tacticAppetite,
    )


@app.post("/internal/habits/daily-log/upsert", response_model=HabitDailyLogUpsertResponse)
async def upsert_habit_daily_log(
    request: HabitDailyLogUpsertRequest,
    x_internal_token: str | None = Header(default=None)
):
    _require_internal_token(x_internal_token)
    row = await _upsert_habit_daily_log_today(
        tenant_id=request.tenantId,
        user_id=request.userId,
        habit_id=request.habitId,
        completed=request.completed,
        nudged=request.nudged,
        user_response=request.userResponse,
        inferred_from=request.inferredFrom,
    )
    return HabitDailyLogUpsertResponse(
        status="ok",
        userId=row.get("user_id"),
        habitId=row.get("habit_id"),
        date=row.get("date").isoformat() if row.get("date") else datetime.utcnow().date().isoformat(),
        completed=bool(row.get("completed")),
        nudged=bool(row.get("nudged")),
        userResponse=_normalize_text(row.get("user_response")) or None,
        inferredFrom=_normalize_text(row.get("inferred_from")) or None,
    )


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
        tenantId = _normalize_text(_canonical_tenant_id(tenantId)) or tenantId
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

        # GRAPHITI_REPLACED: search_nodes (unresolved tension/loop retrieval for session brief)
        # TEMPORARY_DEGRADED_REPLACEMENT: session-brief loops/tensions are derived continuity projections.
        tensions = await _pg_search_nodes(
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

        # GRAPHITI_REPLACED: search_nodes (key entities retrieval for session brief)
        # TEMPORARY_DEGRADED_REPLACEMENT: key entities here are continuity hints, not canonical facts.
        key_entities = await _pg_search_nodes(
            tenant_id=tenantId,
            user_id=userId,
            query="named people places projects tools organizations priorities",
            limit=6,
            reference_time=reference_now,
            search_filter=current_filter
        )
        # GRAPHITI_REPLACED: search_nodes (commitment candidates retrieval for session brief)
        # TEMPORARY_DEGRADED_REPLACEMENT: commitment candidates are inferred from continuity nodes.
        commitment_entities = await _pg_search_nodes(
            tenant_id=tenantId,
            user_id=userId,
            query="commitment schedule deadline todo follow up plan",
            limit=6,
            reference_time=reference_now,
            search_filter=current_filter
        )
        # GRAPHITI_REPLACED: search_nodes (current focus retrieval for session brief)
        # TEMPORARY_DEGRADED_REPLACEMENT: current focus extraction uses derived continuity nodes.
        focus_nodes = await _pg_search_nodes(
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
        tenantId = _normalize_text(_canonical_tenant_id(tenantId)) or tenantId
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
        user_model_hint_rows: List[Dict[str, Any]] = []
        user_model_data: Dict[str, Any] = {}
        user_model_narrative: Optional[str] = None
        user_model_narrative_stable: Optional[str] = None
        user_model_narrative_current: Optional[str] = None
        canonical_signal_rows: Dict[str, Any] = {"claims": [], "entities": [], "watermarks": []}
        canonical_recent_changes: List[str] = []
        canonical_entity_hint_rows: List[Dict[str, Any]] = []
        entity_hints: List[SessionStartBriefEntityHint] = []
        entity_profiles: List[SessionStartBriefEntityProfile] = []
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
        ingest_freshness: Dict[str, Any] = {
            "has_pending_session_ingest_jobs": False,
            "pending_raw_episode_jobs": 0,
            "pending_post_ingest_hook_jobs": 0,
            "oldest_pending_age_seconds": None,
            "latest_pending_session_id": None,
        }

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
                if _is_unreasonably_future(created_dt, reference_now_utc):
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
                        if _is_unreasonably_future(created_dt, reference_now_utc):
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
                if _is_unreasonably_future(summary_end, reference_now_utc):
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
            reason = _normalize_text((loop_item.metadata or {}).get("last_action_reason")) if isinstance(loop_item.metadata, dict) else ""
            loop_items.append({
                "kind": "loop",
                "type": loop_item.type,
                "text": loop_item.text,
                "timeHorizon": loop_item.timeHorizon,
                "dueDate": loop_item.dueDate,
                "salience": loop_item.salience,
                "lastSeenAt": loop_item.lastSeenAt,
                "reason": reason or None,
            })
            if len(loop_items) >= 5:
                break

        yesterday_analysis = {"date": None, "themes": [], "steering_note": None}

        try:
            _, tenant_scope = _resolve_tenant_scope(tenantId)
            scoped_user_models = await _fetch_user_model_rows_for_scope(tenant_scope=tenant_scope, user_id=userId)
            user_model_row = scoped_user_models[0] if scoped_user_models else None
            if user_model_row:
                user_model = _normalize_user_model(user_model_row.get("model"))
                user_model_data = user_model
        except Exception as e:
            logger.error(f"startbrief user model hint lookup failed: {e}")

        try:
            canonical_signal_rows = await _fetch_canonical_signal_rows(
                tenant_id=tenantId,
                user_id=userId,
                claim_limit=120,
                entity_limit=80,
            )
            for row in canonical_signal_rows.get("claims") or []:
                if int(row.get("evidence_count") or 0) <= 0:
                    continue
                line = _shorten_line(_canonical_claim_line(row), 180)
                if line:
                    canonical_recent_changes.append(line)
                if len(canonical_recent_changes) >= 6:
                    break
            for entity_row in (canonical_signal_rows.get("entities") or []):
                name = canonicalize_entity_name(entity_row.get("canonical_name"))
                if not name:
                    continue
                entity_type = infer_ontology_type(entity_row.get("entity_type"))
                if entity_type not in {"person", "project"}:
                    continue
                canonical_entity_hint_rows.append(
                    {
                        "entityId": _normalize_text(entity_row.get("entity_id")) or None,
                        "name": name,
                        "type": entity_type,
                        "role": _entity_role_from_user_model(user_model_data, name),
                        "importance": 0.62,
                        "salience": 0.55,
                        "lastSeenAt": _normalize_text(entity_row.get("updated_at")) or None,
                        "source": "canonical_entities",
                        "confidence": 0.62,
                        "updatedAt": _normalize_text(entity_row.get("updated_at")) or None,
                    }
                )
                if len(canonical_entity_hint_rows) >= 6:
                    break
        except Exception as e:
            logger.warning("startbrief canonical signal fallback lookup failed: %s", e)
            canonical_signal_rows = {"claims": [], "entities": [], "watermarks": []}
            canonical_recent_changes = []
            canonical_entity_hint_rows = []

        try:
            raw_entity_hints = await _build_entity_candidates(
                tenant_id=tenantId,
                user_id=userId,
                reference_time=reference_now,
                user_model=user_model_data,
                context_texts=[
                    *[
                        _normalize_text(x.get("text"))
                        for x in loop_items[:4]
                        if isinstance(x, dict) and _normalize_text(x.get("text"))
                    ],
                    _normalize_text(last_user_text),
                ],
                max_hints=5,
            )
            entity_hints = [
                SessionStartBriefEntityHint(
                    entityId=_normalize_text(item.get("entityId")) or None,
                    name=_normalize_text(item.get("name")) or "unknown",
                    type=_normalize_text(item.get("type")) or "other",
                    role=_normalize_text(item.get("role")) or None,
                    importance=_importance_label(item.get("importance")),
                    salience=float(item.get("salience")) if item.get("salience") is not None else None,
                    lastSeenAt=_normalize_text(item.get("lastSeenAt")) or None,
                    source=_normalize_text(item.get("source")) or None,
                    confidence=(
                        _normalize_confidence(item.get("confidence"), default=0.0)
                        if item.get("confidence") is not None
                        else None
                    ),
                    updatedAt=_normalize_text(item.get("updatedAt")) or _normalize_text(item.get("lastSeenAt")) or None,
                )
                for item in (raw_entity_hints or [])
                if _normalize_text(item.get("name"))
            ][:5]
            entity_hints = sorted(
                entity_hints,
                key=lambda x: (
                    {"high": 3, "medium": 2, "low": 1}.get(_normalize_text(x.importance).lower(), 0),
                    float(x.salience or 0.0),
                    _normalize_text(x.lastSeenAt),
                    _normalize_text(x.name).lower(),
                ),
                reverse=True,
            )[:5]
        except Exception as e:
            logger.error("startbrief entity hints build failed: %s", e)
            entity_hints = []
        if not entity_hints and canonical_entity_hint_rows:
            entity_hints = [
                SessionStartBriefEntityHint(
                    entityId=_normalize_text(item.get("entityId")) or None,
                    name=_normalize_text(item.get("name")) or "unknown",
                    type=_normalize_text(item.get("type")) or "other",
                    role=_normalize_text(item.get("role")) or None,
                    importance=_importance_label(item.get("importance")),
                    salience=float(item.get("salience")) if item.get("salience") is not None else None,
                    lastSeenAt=_normalize_text(item.get("lastSeenAt")) or None,
                    source=_normalize_text(item.get("source")) or None,
                    confidence=(
                        _normalize_confidence(item.get("confidence"), default=0.0)
                        if item.get("confidence") is not None
                        else None
                    ),
                    updatedAt=_normalize_text(item.get("updatedAt")) or _normalize_text(item.get("lastSeenAt")) or None,
                )
                for item in canonical_entity_hint_rows[:5]
            ]

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

        gap_minutes: Optional[int] = None
        if last_activity_time:
            gap_minutes = int(max(0, (reference_now_utc - last_activity_time.astimezone(dt_timezone.utc)).total_seconds() // 60))
        if gap_minutes is not None and gap_minutes <= 30:
            depth_label = "continuation"
        elif gap_minutes is not None and gap_minutes <= 360:
            depth_label = "today"
        elif gap_minutes is not None and gap_minutes <= 1440:
            depth_label = "yesterday"
        else:
            depth_label = "multi_day"

        identity_basics = _trusted_identity_basics(user_model_data)
        recent_changes = _build_recent_high_signal_changes(recent_session_summaries, loop_items, max_items=3)
        if not recent_changes and canonical_recent_changes:
            recent_changes = _dedupe_keep_order(canonical_recent_changes, limit=3)
        local_time = reference_now.strftime("%H:%M")
        first_session_today = sessions_today_count == 0
        handover_text = _compose_structured_handover_text(
            depth_label=depth_label,
            time_of_day_label=time_of_day_label,
            time_gap_human=time_gap_human,
            identity_basics=identity_basics,
            entity_hints=entity_hints,
            top_loops=loop_items,
            recent_changes=recent_changes,
        )

        ops_context = {
            "identity": identity_basics,
            "top_loops_today": [
                {
                    "text": _normalize_text(loop.get("text")),
                    "type": _normalize_text(loop.get("type")) or None,
                    "time_horizon": _normalize_text(loop.get("timeHorizon")) or None,
                    "salience": int(loop.get("salience") or 0) if loop.get("salience") is not None else None,
                }
                for loop in loop_items[:3]
                if _normalize_text(loop.get("text"))
            ],
            "recent_changes": recent_changes[:3],
            "assistant_guidance": [],
            "user_model_hints": [],
            "user_model_hint_rows": [],
            "yesterday_themes": [],
            "steering_note": None,
            "daily_analysis": {"date": None, "confidence": None, "updated_at": None},
        }
        used_summary_ids = [
            _normalize_text(s.get("session_id"))
            for s in recent_session_summaries[:2]
            if _normalize_text(s.get("session_id")) and not _is_placeholder_value(s.get("session_id"))
        ]
        evidence_ids = used_summary_ids or fetched_summary_ids[:2]
        summary_content_quality = "not_used_by_default"
        if int(summary_norm_stats.get("nodes_seen") or 0) <= 0:
            summary_content_quality = "none_fetched"
        elif recent_changes:
            summary_content_quality = "used_for_recent_changes"
        if summary_content_quality == "none_fetched" and canonical_recent_changes:
            summary_content_quality = "canonical_claim_fallback"
        summary_norm_stats["evidence_ids_used_count"] = len(evidence_ids)
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
        try:
            ingest_freshness = await _get_session_ingest_freshness(
                tenant_id=tenantId,
                user_id=userId,
                reference_now_utc=reference_now_utc,
                session_id=session_id,
            )
        except Exception as e:
            logger.warning("startbrief ingest freshness lookup failed: %s", e)

        response = SessionStartBriefResponse(
            handover_text=_normalize_text(handover_text),
            narrative=None,
            handover_depth=depth_label,
            time_context={
                "local_time": local_time,
                "time_of_day": time_of_day_label,
                "gap_minutes": gap_minutes,
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
                "claim_ranking": [],
                "loop_ranking": [],
                "claim_ranking_defs": {
                    "salience": "Immediate intensity/urgency of a session claim (short-horizon prominence).",
                    "importance": "Durable relevance inferred from recurrence across recent sessions and alignment with active loops.",
                    "confidence": "Trust in claim quality based on summary quality and salience band.",
                },
                "loop_ranking_defs": {
                    "salience": "Immediate urgency/intensity of the loop signal.",
                    "importance": "Durable relevance by loop type/time horizon and commitment durability.",
                    "confidence": "Trust in loop signal quality derived from explicit confidence or salience proxy.",
                },
                "summary_fetch_count": len(fetched_summary_ids),
                "summary_used_count": len(used_summary_ids),
                "summary_content_quality": summary_content_quality,
                "fallback_used": bool(summary_norm_stats.get("fallback_used")),
                "fallback_success": bool(summary_norm_stats.get("fallback_success")),
                "daily_analysis_date_used": None,
                "freshness": ingest_freshness,
                "canonical_provenance": _canonical_provenance_payload(
                    projection_version="t9a.startbrief_reanchor.v1",
                    signal_rows=canonical_signal_rows,
                ),
            },
            entity_hints=entity_hints,
            entity_profiles=[],
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
                    "recent_changes": recent_changes,
                }
            )
        except Exception as e:
            logger.error("startbrief history log failed: %s", e)
        return response
    except Exception as e:
        logger.error(f"Session startbrief failed: {e}")
        raise HTTPException(status_code=500, detail="Session startbrief failed")


@app.post("/entities/profile", response_model=EntityProfileResponse)
async def entities_profile(request: EntityProfileRequest):
    try:
        tenant_id = _normalize_text(_canonical_tenant_id(request.tenantId)) or request.tenantId
        user_id = _normalize_text(request.userId)
        entity_id = _normalize_text(request.entityId)
        name = _normalize_text(request.name)
        if not entity_id and not name:
            raise HTTPException(status_code=400, detail="Provide entityId or name")

        reference_time = datetime.utcnow().replace(tzinfo=dt_timezone.utc)
        if request.referenceTime:
            value = _normalize_text(request.referenceTime)
            if value.endswith("Z"):
                value = value.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(value)
            reference_time = parsed if parsed.tzinfo else parsed.replace(tzinfo=dt_timezone.utc)

        user_model: Dict[str, Any] = {}
        try:
            _, tenant_scope = _resolve_tenant_scope(tenant_id)
            rows = await _fetch_user_model_rows_for_scope(tenant_scope=tenant_scope, user_id=user_id)
            if rows:
                user_model = _normalize_user_model(rows[0].get("model"))
        except Exception:
            user_model = {}
        # GRAPHITI_REPLACED: get_entity_role_grounding (entity role lookup for profile)
        # TEMPORARY_DEGRADED_REPLACEMENT: role is continuity-derived and not evidence-backed.
        role_grounding = await _pg_get_entity_role_grounding(
            tenant_id=tenant_id,
            user_id=user_id,
            name=name or None,
            entity_id=entity_id or None,
        ) or {}

        selected = await _resolve_canonical_entity_candidate(
            tenant_id=tenant_id,
            user_id=user_id,
            reference_time=reference_time,
            entity_id=entity_id,
            name=name,
            user_model=user_model,
        )
        if not selected:
            raise HTTPException(status_code=404, detail="Entity not found")

        canonical_name = _normalize_text(selected.get("name"))
        aliases: List[str] = []
        raw_attrs = selected.get("raw", {}).get("attributes") if isinstance(selected.get("raw"), dict) else {}
        if isinstance(raw_attrs, dict):
            aliases_raw = raw_attrs.get("aliases")
            if isinstance(aliases_raw, list):
                aliases = _dedupe_keep_order([_normalize_text(x) for x in aliases_raw if _normalize_text(x)], limit=8)

        facts_limit = max(1, min(int(request.factsLimit or 6), 10))
        loops_limit = max(0, min(int(request.loopsLimit or 3), 8))
        # GRAPHITI_REPLACED: get_entity_facts_exact (exact profile facts)
        # TEMPORARY_DEGRADED_REPLACEMENT: strict factual mode blocks derived continuity-only rows.
        exact_facts_rows = await _pg_get_entity_facts_exact(
            tenant_id=tenant_id,
            user_id=user_id,
            name=canonical_name,
            entity_id=_normalize_text(selected.get("entityId")) or None,
            limit=max(facts_limit * 2, 8),
            include_derived=False,
        )
        # GRAPHITI_REPLACED: search_facts (semantic profile fact expansion)
        # TEMPORARY_DEGRADED_REPLACEMENT: semantic expansion constrained to canonical evidence-backed rows only.
        semantic_facts_rows = await _pg_search_facts(
            tenant_id=tenant_id,
            user_id=user_id,
            query=canonical_name,
            limit=max(facts_limit * 2, 8),
            reference_time=reference_time,
            include_derived=False,
        )
        facts_rows = list(exact_facts_rows or [])
        seen_fact_texts = {str(row.get("text") or "").strip().lower() for row in facts_rows if isinstance(row, dict)}
        for row in semantic_facts_rows or []:
            if not isinstance(row, dict):
                continue
            text = str(row.get("text") or "").strip().lower()
            if text and text not in seen_fact_texts:
                facts_rows.append(row)
                seen_fact_texts.add(text)
        entity_terms = _query_terms(canonical_name)
        key_facts: List[Dict[str, Any]] = []
        seen_facts = set()
        for row in facts_rows or []:
            if not isinstance(row, dict):
                continue
            text = _normalize_text(row.get("text"))
            if not text or not _allow_fact_text(text):
                continue
            if not _is_canonical_factual_row(row):
                continue
            invalid_at = row.get("invalid_at")
            if invalid_at:
                continue
            if entity_terms and _query_overlap_score(text, entity_terms) < 0.35 and canonical_name.lower() not in text.lower():
                continue
            key = text.lower()
            if key in seen_facts:
                continue
            seen_facts.add(key)
            valid_at = row.get("valid_at")
            invalid_at = row.get("invalid_at")
            key_facts.append(
                {
                    "text": _shorten_line(text, 220),
                    "confidence": _normalize_confidence(row.get("relevance"), default=0.6),
                    "validAt": valid_at.isoformat() if isinstance(valid_at, datetime) else _normalize_text(valid_at) or None,
                    "invalidAt": invalid_at.isoformat() if isinstance(invalid_at, datetime) else _normalize_text(invalid_at) or None,
                }
            )
            if len(key_facts) >= facts_limit:
                break

        open_loops: List[Dict[str, Any]] = []
        if bool(request.includeOpenLoops) and loops_limit > 0:
            top_loops = await loops.get_top_loops_for_startbrief(
                tenant_id=tenant_id,
                user_id=user_id,
                limit=18,
                persona_id=None,
            )
            for item in top_loops or []:
                text = _normalize_text(getattr(item, "text", ""))
                if not text:
                    continue
                if entity_terms and _query_overlap_score(text, entity_terms) < 0.45:
                    continue
                open_loops.append(
                    {
                        "id": _normalize_text(getattr(item, "id", None)) or "",
                        "type": _normalize_text(getattr(item, "type", "")) or None,
                        "text": _shorten_line(text, 180),
                        "status": _normalize_text(getattr(item, "status", "")) or None,
                        "salience": int(getattr(item, "salience", 0) or 0),
                    }
                )
                if len(open_loops) >= loops_limit:
                    break

        raw_summary = None
        raw_node = selected.get("raw") if isinstance(selected.get("raw"), dict) else {}
        if isinstance(raw_node, dict):
            raw_summary = _normalize_text(raw_node.get("summary"))
            if not raw_summary:
                raw_summary = _normalize_text(raw_node.get("name"))

        summary_parts: List[str] = []
        if key_facts:
            summary_parts.append(_shorten_line(key_facts[0].get("text") or "", 180))
        if len(key_facts) > 1:
            summary_parts.append(_shorten_line(key_facts[1].get("text") or "", 180))
        attrs = raw_node.get("attributes") if isinstance(raw_node, dict) and isinstance(raw_node.get("attributes"), dict) else {}
        profile_summary = _normalize_text(attrs.get("profile_summary")) if isinstance(attrs, dict) else ""
        if not key_facts and profile_summary:
            key_facts.append(
                {
                    "text": _shorten_line(profile_summary, 220),
                    "confidence": _normalize_confidence(attrs.get("confidence"), default=0.75),
                    "validAt": None,
                    "invalidAt": None,
                }
            )
        summary = " ".join([p for p in summary_parts if p]) or profile_summary or raw_summary or f"{canonical_name} appears in memory context."
        last_seen = _normalize_text(selected.get("lastSeenAt")) or None
        response_payload = EntityProfileResponse(
            entity={
                "entityId": _normalize_text(selected.get("entityId")) or None,
                "canonicalName": canonical_name,
                "type": _normalize_text(selected.get("type")) or "other",
                "aliases": aliases,
                "summary": summary,
                "role": _normalize_text(role_grounding.get("role")) or _normalize_text(selected.get("role")) or _entity_role_from_user_model(user_model, canonical_name) or None,
                "relationship": _normalize_text(role_grounding.get("relationship")) or _normalize_text(selected.get("role")) or _entity_role_from_user_model(user_model, canonical_name) or None,
                "importance": float(selected.get("importance") or 0.0),
                "salience": float(selected.get("salience") or 0.0),
                "confidence": _normalize_confidence((raw_attrs or {}).get("confidence"), default=float(selected.get("confidence") or 0.6)),
                "recency": {
                    "lastSeenAt": last_seen,
                    "daysSinceSeen": _days_since_iso(last_seen, reference_time),
                    "firstSeenAt": _normalize_text((raw_attrs or {}).get("first_seen_at")) or None,
                },
            },
            keyFacts=key_facts,
            openLoops=open_loops,
            provenance={
                "sources": ["postgres_continuity_nodes", "postgres_continuity_facts", "loops"] + (["postgres_role_hint"] if role_grounding else []) + (["user_model"] if _entity_role_from_user_model(user_model, canonical_name) else []),
                "resolvedBy": "entityId" if entity_id else "name",
                "queryUsed": canonical_name,
                "generatedAt": reference_time.isoformat(),
                "nodeProvenance": {
                    "firstSeenAt": _normalize_text((raw_attrs or {}).get("first_seen_at")) or None,
                    "lastSeenAt": _normalize_text((raw_attrs or {}).get("last_seen_at")) or last_seen,
                },
            },
        )
        return response_payload
    except HTTPException:
        raise
    except Exception as e:
        logger.error("entities/profile failed: %s", e)
        raise HTTPException(status_code=500, detail="Entity profile failed")


@app.post("/internal/debug/entities/profile")
async def debug_entities_profile(
    request: EntityProfileRequest,
    x_internal_token: str | None = Header(default=None)
):
    _require_internal_token(x_internal_token)
    try:
        profile = await entities_profile(request)
        profile_payload = profile.model_dump() if hasattr(profile, "model_dump") else profile.dict()

        tenant_id = _normalize_text(_canonical_tenant_id(request.tenantId)) or request.tenantId
        user_id = _normalize_text(request.userId)
        entity_id = _normalize_text(request.entityId)
        name = _normalize_text(request.name)

        reference_time = datetime.utcnow().replace(tzinfo=dt_timezone.utc)
        if request.referenceTime:
            value = _normalize_text(request.referenceTime)
            if value.endswith("Z"):
                value = value.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(value)
            reference_time = parsed if parsed.tzinfo else parsed.replace(tzinfo=dt_timezone.utc)

        user_model: Dict[str, Any] = {}
        try:
            _, tenant_scope = _resolve_tenant_scope(tenant_id)
            rows = await _fetch_user_model_rows_for_scope(tenant_scope=tenant_scope, user_id=user_id)
            if rows:
                user_model = _normalize_user_model(rows[0].get("model"))
        except Exception:
            user_model = {}

        candidates = await _build_entity_candidates(
            tenant_id=tenant_id,
            user_id=user_id,
            reference_time=reference_time,
            user_model=user_model,
            context_texts=[name] if name else None,
            max_hints=10,
        )
        selected = _resolve_entity_candidate(candidates, entity_id=entity_id, name=name)
        canonical_name = (
            _normalize_text((profile_payload or {}).get("entity", {}).get("canonicalName"))
            or _normalize_text((selected or {}).get("name"))
            or name
        )

        facts_limit = max(1, min(int(request.factsLimit or 6), 10))
        # GRAPHITI_REPLACED: search_facts (debug fact retrieval)
        # TEMPORARY_DEGRADED_REPLACEMENT: debug endpoint intentionally includes derived continuity rows.
        raw_fact_rows = await _pg_search_facts(
            tenant_id=tenant_id,
            user_id=user_id,
            query=canonical_name,
            limit=max(facts_limit * 3, 12),
            reference_time=reference_time,
            include_derived=True,
        )
        entity_terms = _query_terms(canonical_name)
        kept_facts: List[Dict[str, Any]] = []
        dropped_facts: List[Dict[str, Any]] = []
        seen = set()
        for row in raw_fact_rows or []:
            if not isinstance(row, dict):
                continue
            text = _normalize_text(row.get("text"))
            if not text:
                dropped_facts.append({"text": text, "reason": "empty_text"})
                continue
            if not _allow_fact_text(text):
                dropped_facts.append({"text": text, "reason": "fact_quality_filter"})
                continue
            if entity_terms and _query_overlap_score(text, entity_terms) < 0.35 and canonical_name.lower() not in text.lower():
                dropped_facts.append({"text": text, "reason": "entity_overlap_filter"})
                continue
            key = text.lower()
            if key in seen:
                dropped_facts.append({"text": text, "reason": "duplicate"})
                continue
            seen.add(key)
            kept_facts.append(
                {
                    "text": text,
                    "relevance": row.get("relevance"),
                    "source_type": row.get("source_type"),
                    "derived": row.get("derived"),
                    "evidence_backed": row.get("evidence_backed"),
                    "data_classification": row.get("data_classification"),
                }
            )
            if len(kept_facts) >= facts_limit:
                break

        loop_matches: List[Dict[str, Any]] = []
        if bool(request.includeOpenLoops):
            top_loops = await loops.get_top_loops_for_startbrief(
                tenant_id=tenant_id,
                user_id=user_id,
                limit=20,
                persona_id=None,
            )
            for item in top_loops or []:
                text = _normalize_text(getattr(item, "text", ""))
                if not text:
                    continue
                score = _query_overlap_score(text, entity_terms) if entity_terms else 0.0
                if score < 0.35:
                    continue
                loop_matches.append(
                    {
                        "id": _normalize_text(getattr(item, "id", None)) or None,
                        "type": _normalize_text(getattr(item, "type", "")) or None,
                        "text": text,
                        "status": _normalize_text(getattr(item, "status", "")) or None,
                        "salience": int(getattr(item, "salience", 0) or 0),
                        "entity_overlap_score": round(float(score), 4),
                    }
                )
        loop_matches.sort(key=lambda x: (float(x.get("entity_overlap_score") or 0.0), int(x.get("salience") or 0)), reverse=True)

        return {
            "profile": profile_payload,
            "debug": {
                "request": request.model_dump() if hasattr(request, "model_dump") else request.dict(),
                "selected_entity": selected,
                "candidate_count": len(candidates),
                "candidates": candidates[:10],
                "facts": {
                    "raw_count": len(raw_fact_rows or []),
                    "kept_count": len(kept_facts),
                    "kept": kept_facts,
                    "dropped": dropped_facts[:20],
                },
                "loop_matches": loop_matches[:10],
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("debug entities/profile failed: %s", e)
        raise HTTPException(status_code=500, detail="Debug entity profile failed")


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
        "episodic_memory_embeddings",
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
    Session-only ingestion: durably enqueue full transcript for async processing.
    """
    try:
        started_at = request.startedAt
        ended_at = request.endedAt
        if not started_at and request.messages:
            started_at = request.messages[0].timestamp
        if not ended_at and request.messages:
            ended_at = request.messages[-1].timestamp

        messages_payload = [m.model_dump() for m in request.messages]
        valid_messages = [
            m for m in messages_payload
            if _normalize_text(m.get("text")) and _normalize_text(m.get("role"))
        ]
        await session.enqueue_session_ingest(
            tenant_id=request.tenantId,
            session_id=request.sessionId,
            user_id=request.userId,
            messages=messages_payload,
            started_at=started_at,
            ended_at=ended_at,
        )

        return SessionIngestResponse(
            status="ingested" if valid_messages else "skipped_empty_transcript",
            sessionId=request.sessionId,
            graphitiAdded=False
        )
    except (EvidenceContractError, ExtractionContractError) as e:
        raise HTTPException(
            status_code=400,
            detail={"code": e.code, "message": e.message},
        )
    except Exception as e:
        logger.error(f"Session ingest failed: {e}")
        raise HTTPException(status_code=500, detail="Session ingest failed")


@app.post("/internal/drain")
async def drain(
    limit: int = 200,
    tenant_id: str | None = None,
    job_types: list[str] | None = Query(default=None),
    budget_seconds: float = 2.0,
    per_row_timeout_seconds: float = 8.0
):
    """Internal outbox drain endpoint."""
    try:
        counts = await session.drain_outbox(
            graphiti_client=graphiti_client,
            limit=limit,
            tenant_id=tenant_id,
            job_types=job_types,
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
            # GRAPHITI_REPLACED: search_nodes (debug top entities)
            # TEMPORARY_DEGRADED_REPLACEMENT: debug top entities are continuity-node projections.
            entities = await _pg_search_nodes(
                tenant_id=tenantId,
                user_id=userId,
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
                SELECT id, tenant_id, user_id, session_id, job_type, dedupe_key, status, attempts,
                       next_attempt_at, last_error, created_at, sent_at, payload
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
                SELECT id, tenant_id, user_id, session_id, job_type, dedupe_key, status, attempts,
                       next_attempt_at, last_error, created_at, sent_at, payload
                FROM graphiti_outbox
                WHERE status IN ('pending', 'failed')
                ORDER BY id DESC
                LIMIT $1
                """,
                limit
            )
        summarized: List[Dict[str, Any]] = []
        for row in rows:
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            payload_summary = {
                "keys": sorted([str(k) for k in payload.keys()])[:12],
                "hook": _normalize_text(payload.get("hook")) or None,
                "reference_time": _normalize_text(payload.get("reference_time")) or None,
                "target_date": _normalize_text(payload.get("target_date")) or None,
                "has_messages": isinstance(payload.get("messages"), list),
                "message_count": len(payload.get("messages")) if isinstance(payload.get("messages"), list) else 0,
            }
            summarized.append(
                {
                    "id": row.get("id"),
                    "tenant_id": row.get("tenant_id"),
                    "user_id": row.get("user_id"),
                    "session_id": row.get("session_id"),
                    "job_type": row.get("job_type"),
                    "dedupe_key": row.get("dedupe_key"),
                    "status": row.get("status"),
                    "attempts": row.get("attempts"),
                    "next_attempt_at": row.get("next_attempt_at"),
                    "last_error": row.get("last_error"),
                    "created_at": row.get("created_at"),
                    "sent_at": row.get("sent_at"),
                    "payload_summary": payload_summary,
                }
            )
        return {"rows": summarized}
    except Exception as e:
        logger.error(f"Debug outbox endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/internal/debug/session_ingest_status")
async def debug_session_ingest_status(
    tenant_id: str,
    user_id: str,
    session_id: str,
    x_internal_token: str | None = Header(default=None)
):
    _require_internal_token(x_internal_token)
    try:
        transcript_row = await db.fetchone(
            """
            SELECT updated_at, jsonb_array_length(messages) AS message_count
            FROM session_transcript
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
            LIMIT 1
            """,
            tenant_id,
            user_id,
            session_id
        )
        outbox_rows = await db.fetch(
            """
            SELECT id, job_type, dedupe_key, status, attempts, next_attempt_at, last_error, created_at, sent_at, payload
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
              AND (
                dedupe_key = $4
                OR dedupe_key LIKE $5
              )
            ORDER BY id ASC
            """,
            tenant_id,
            user_id,
            session_id,
            f"session_ingest_raw:{tenant_id}:{user_id}:{session_id}",
            f"session_hook_%:{tenant_id}:{user_id}:{session_id}%"
        )
        items: List[Dict[str, Any]] = []
        for row in outbox_rows:
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            items.append(
                {
                    "id": row.get("id"),
                    "job_type": row.get("job_type"),
                    "dedupe_key": row.get("dedupe_key"),
                    "status": row.get("status"),
                    "attempts": row.get("attempts"),
                    "next_attempt_at": row.get("next_attempt_at"),
                    "last_error": row.get("last_error"),
                    "created_at": row.get("created_at"),
                    "sent_at": row.get("sent_at"),
                    "payload_summary": {
                        "keys": sorted([str(k) for k in payload.keys()])[:12],
                        "hook": _normalize_text(payload.get("hook")) or None,
                        "reference_time": _normalize_text(payload.get("reference_time")) or None,
                        "target_date": _normalize_text(payload.get("target_date")) or None,
                    }
                }
            )
        last_success = await db.fetch(
            """
            SELECT job_type, MAX(sent_at) AS last_success_at
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
              AND status = 'sent'
              AND sent_at IS NOT NULL
            GROUP BY job_type
            ORDER BY job_type ASC
            """,
            tenant_id,
            user_id,
            session_id
        )
        transcript_message_count = int((transcript_row or {}).get("message_count") or 0)
        graph_episode_status = "not_enqueued"
        if transcript_message_count <= 0:
            graph_episode_status = "skipped_empty_transcript"
        elif any((row.get("job_type") or "").lower() == "session_raw_episode" for row in items):
            graph_episode_status = "enqueued"
            if any((row.get("job_type") or "").lower() == "session_raw_episode" and (row.get("status") or "") == "sent" for row in items):
                graph_episode_status = "sent"
            elif any((row.get("job_type") or "").lower() == "session_raw_episode" and (row.get("status") or "") == "failed" for row in items):
                graph_episode_status = "failed"
        return {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "session_id": session_id,
            "transcript": {
                "exists": bool(transcript_row),
                "updated_at": transcript_row.get("updated_at") if transcript_row else None,
                "message_count": transcript_message_count,
            },
            "graph_episode_status": graph_episode_status,
            "jobs": items,
            "last_success": [
                {
                    "job_type": row.get("job_type"),
                    "last_success_at": row.get("last_success_at"),
                }
                for row in last_success
            ],
        }
    except Exception as e:
        logger.error(f"Debug session ingest status error: {e}")
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


@app.post("/internal/debug/backfill/user_model_enrichment")
async def debug_backfill_user_model_enrichment(
    limit: int = 500,
    x_internal_token: str | None = Header(default=None)
):
    """Run a bounded idempotent backfill for user model enrichment."""
    _require_internal_token(x_internal_token)
    settings = get_settings()
    try:
        counts = await _run_user_model_enrichment_backfill_once(
            max_users=max(1, min(int(limit), 5000)),
            min_confidence=float(settings.user_model_enrichment_min_confidence),
            retry_backoff_seconds=int(settings.user_model_enrichment_retry_backoff_seconds),
            retry_max_seconds=int(settings.user_model_enrichment_retry_max_seconds),
        )
        return {"ok": True, "counts": counts}
    except Exception as e:
        logger.error(f"Debug user model enrichment backfill failed: {e}")
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
        # GRAPHITI_REPLACED: get_recent_episodes (debug episodic list)
        # TEMPORARY_DEGRADED_REPLACEMENT: episodic debug output includes derived summaries plus transcript evidence.
        episodes = await _pg_get_recent_episodes(
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
                    "uuid": episode.get("uuid"),
                    "source_type": episode.get("source_type"),
                    "derived": episode.get("derived"),
                    "evidence_backed": episode.get("evidence_backed"),
                    "data_classification": episode.get("data_classification"),
                })
            else:
                results.append({
                    "name": getattr(episode, "name", None),
                    "summary": getattr(episode, "summary", None) or getattr(episode, "episode_summary", None),
                    "reference_time": getattr(episode, "reference_time", None) or getattr(episode, "created_at", None),
                    "content": getattr(episode, "episode_body", None) or getattr(episode, "content", None),
                    "uuid": str(getattr(episode, "uuid", None)) if getattr(episode, "uuid", None) else None,
                    "source_type": getattr(episode, "source_type", None),
                    "derived": getattr(episode, "derived", None),
                    "evidence_backed": getattr(episode, "evidence_backed", None),
                    "data_classification": getattr(episode, "data_classification", None),
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


@app.get("/internal/debug/startbrief/ranking")
async def debug_startbrief_ranking(
    tenantId: str,
    userId: str,
    now: Optional[str] = None,
    timezone: Optional[str] = None,
    limit: int = 12,
    x_internal_token: str | None = Header(default=None)
):
    _require_internal_token(x_internal_token)
    try:
        tenantId = _normalize_text(_canonical_tenant_id(tenantId)) or tenantId
        safe_limit = max(3, min(int(limit or 12), 20))
        reference_now = datetime.utcnow()
        if now:
            reference_now = datetime.fromisoformat(now.replace("Z", "+00:00"))
        tzinfo = _resolve_timezone(timezone)
        if reference_now.tzinfo is None:
            reference_now = reference_now.replace(tzinfo=tzinfo or dt_timezone.utc)
        if tzinfo:
            reference_now = reference_now.astimezone(tzinfo)
        reference_now_utc = reference_now.astimezone(dt_timezone.utc)

        recent_session_summaries: List[Dict[str, Any]] = []
        summary_nodes = await graphiti_client.get_recent_session_summary_nodes(
            tenant_id=tenantId,
            user_id=userId,
            limit=safe_limit,
        )
        for node in summary_nodes or []:
            normalized = _normalize_startbrief_session_summary_node(node)
            if not normalized:
                continue
            created_at = _normalize_text(normalized.get("created_at"))
            created_dt = _parse_optional_dt(created_at)
            if _is_unreasonably_future(created_dt, reference_now_utc):
                continue
            summary_facts = _normalize_text(normalized.get("latest_thread_text"))
            moment = _normalize_text(normalized.get("moment"))
            if not summary_facts and not moment:
                continue
            recent_session_summaries.append({
                "session_id": _normalize_text(normalized.get("session_id")),
                "created_at": created_at,
                "summary_facts": summary_facts,
                "tone": _normalize_text(normalized.get("tone")),
                "moment": moment,
                "unresolved": normalized.get("unresolved") if isinstance(normalized.get("unresolved"), list) else [],
                "decisions": normalized.get("decisions") if isinstance(normalized.get("decisions"), list) else [],
                "salience": _normalize_text(normalized.get("salience")) or "low",
                "summary_text": _normalize_text(normalized.get("summary_text")),
                "bridge_text": _normalize_text(normalized.get("bridge_text")),
                "reference_time": _normalize_text(normalized.get("reference_time") or created_at),
            })

        loop_items: List[Dict[str, Any]] = []
        active_loops = await loops.get_top_loops_for_startbrief(
            tenant_id=tenantId,
            user_id=userId,
            limit=safe_limit,
            persona_id=None,
        )
        for loop_item in active_loops or []:
            text = _normalize_text(getattr(loop_item, "text", ""))
            if not text:
                continue
            loop_items.append(
                {
                    "id": _normalize_text(getattr(loop_item, "id", None)) or None,
                    "kind": "loop",
                    "text": text,
                    "type": _normalize_text(getattr(loop_item, "type", "")) or None,
                    "timeHorizon": _normalize_text(getattr(loop_item, "timeHorizon", "")) or None,
                    "dueDate": _normalize_text(getattr(loop_item, "dueDate", "")) or None,
                    "salience": int(getattr(loop_item, "salience", 0) or 0),
                    "lastSeenAt": _normalize_text(getattr(loop_item, "lastSeenAt", "")) or None,
                    "reason": _normalize_text((getattr(loop_item, "metadata", {}) or {}).get("reason")) or None,
                    "confidence": getattr(loop_item, "confidence", None),
                }
            )

        user_model_hints: List[str] = []
        user_model_data: Dict[str, Any] = {}
        try:
            _, tenant_scope = _resolve_tenant_scope(tenantId)
            scoped_user_models = await _fetch_user_model_rows_for_scope(tenant_scope=tenant_scope, user_id=userId)
            user_model_row = scoped_user_models[0] if scoped_user_models else None
            if user_model_row:
                user_model_data = _normalize_user_model(user_model_row.get("model"))
                user_model_hints = _extract_high_confidence_user_model_hints(
                    user_model_data,
                    threshold=float(get_settings().user_model_high_confidence)
                )
        except Exception:
            user_model_data = {}
            user_model_hints = []

        yesterday_analysis = {"date": None, "themes": [], "steering_note": None}
        try:
            yesterday_analysis = await _get_yesterday_analysis_context(
                tenant_id=tenantId,
                user_id=userId,
                reference_date=reference_now.date()
            )
        except Exception:
            pass

        last_activity_time: Optional[datetime] = None
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
        except Exception:
            last_activity_time = None

        ingredients = select_startbrief_ingredients(
            reference_now=reference_now,
            last_session_end=last_activity_time,
            sessions_today_count=0,
            time_of_day_label=_time_of_day_label(reference_now.replace(tzinfo=None)),
            recent_session_summaries=recent_session_summaries,
            yesterday_daily_analysis=yesterday_analysis,
            top_active_loops=loop_items,
            user_model_hints=user_model_hints,
            user_model=user_model_data,
        )
        selected_summary_ids = [
            _normalize_text(s.get("session_id"))
            for s in ingredients.get("selected_summaries", [])
            if _normalize_text(s.get("session_id"))
        ]
        return {
            "tenantId": tenantId,
            "userId": userId,
            "reference_now": reference_now.isoformat(),
            "summary_candidates_count": len(recent_session_summaries),
            "loop_candidates_count": len(loop_items),
            "selected_summary_ids": selected_summary_ids,
            "selected_loop_texts": [_normalize_text(l.get("text")) for l in ingredients.get("selected_loops", []) if _normalize_text(l.get("text"))],
            "summary_ranking": ingredients.get("all_summary_scores", []),
            "loop_ranking": ingredients.get("all_loop_scores", []),
            "defs": {
                "salience": "Immediate urgency/intensity signal.",
                "importance": "Durable relevance over time (recurrence + loop/domain persistence).",
                "confidence": "Trust estimate for claim quality.",
                "contradiction_penalty": "Negative weight when a claim conflicts with newer evidence."
            }
        }
    except Exception as e:
        logger.error(f"Debug startbrief ranking failed: {e}")
        raise HTTPException(status_code=500, detail="Debug startbrief ranking failed")


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

        # GRAPHITI_REPLACED: search_facts (debug query facts)
        # TEMPORARY_DEGRADED_REPLACEMENT: debug query path intentionally returns derived continuity rows.
        facts = await _pg_search_facts(
            tenant_id=request.tenantId,
            user_id=request.userId,
            query=request.query,
            limit=request.limit or 10,
            reference_time=reference_time,
            include_derived=True,
        )
        # GRAPHITI_REPLACED: search_nodes (debug query entities)
        # TEMPORARY_DEGRADED_REPLACEMENT: debug entities are continuity nodes, not canonical factual entities.
        entities = await _pg_search_nodes(
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


@app.get("/session/handover")
async def session_handover(
    user_id: Optional[str] = Query(default=None),
    userId: Optional[str] = Query(default=None),
):
    effective_user_id = _normalize_text(user_id) or _normalize_text(userId)
    if not effective_user_id:
        raise HTTPException(status_code=400, detail="user_id (or userId) is required")
    try:
        return await _build_handover_packet(effective_user_id)
    except Exception as e:
        logger.error(f"session handover failed user=%s err=%s", effective_user_id, e)
        raise HTTPException(status_code=500, detail="session handover failed")


@app.post("/memory/search")
async def memory_search(payload: Dict[str, Any]):
    # T1 containment: legacy mixed-authority retrieval contract disabled.
    raise HTTPException(
        status_code=410,
        detail="Deprecated endpoint: /memory/search is disabled. Use /memory/query episodic mode.",
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
