from fastapi import FastAPI, HTTPException, BackgroundTasks, Header, Response
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
from datetime import datetime, timezone
import logging
import re
from typing import Optional, Dict, Any, List, Tuple

from .models import (
    IngestRequest,
    BriefRequest,
    IngestResponse,
    BriefResponse,
    MemoryQueryRequest,
    MemoryQueryResponse,
    Fact,
    Entity,
    SessionStartBriefResponse,
    SessionStartBriefItem,
    SessionCloseRequest,
    SessionIngestRequest,
    SessionIngestResponse,
    SessionBriefResponse,
    PurgeUserRequest,
)
from .utils import extract_location
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import io
import csv
from .config import get_settings
from .db import Database
from .graphiti_client import GraphitiClient
from . import session
from . import loops
from .ingestion import ingest as process_ingest
from .briefing import build_briefing
from .migrate import run_migrations

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


def _normalize_text(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


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


@app.post("/memory/query", response_model=MemoryQueryResponse)
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
        focus_nodes = await graphiti_client.search_nodes(
            tenant_id=request.tenantId,
            user_id=request.userId,
            query="current focus priority focused on right now today i need",
            limit=3,
            reference_time=reference_time
        )

        fact_texts_raw = [_normalize_text(f.get("text")) for f in facts if f.get("text")]
        user_stated_state = None
        for text in fact_texts_raw:
            state = _extract_explicit_user_state(text)
            if state:
                user_stated_state = state
                break

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

        return MemoryQueryResponse(
            facts=fact_texts,
            openLoops=open_loop_items,
            commitments=commitment_items,
            contextAnchors=anchors,
            userStatedState=user_stated_state,
            currentFocus=current_focus,
            factItems=fact_models,
            entities=entity_models,
            recallSheet=recall_sheet,
            supplementalContext=recall_sheet,
            metadata={
                "query": request.query,
                "facts": len(fact_models),
                "entities": len(entity_models),
                "openLoops": len(open_loop_items)
            }
        )
    except Exception as e:
        logger.error(f"Memory query failed: {e}")
        raise HTTPException(status_code=500, detail="Memory query failed")


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
            reference_now = reference_now.replace(tzinfo=tzinfo or timezone.utc)
        if tzinfo:
            reference_now = reference_now.astimezone(tzinfo)

        time_of_day_label = _time_of_day_label(reference_now.replace(tzinfo=None))
        time_gap_human = None
        last_session_summary = None
        last_activity_time: Optional[datetime] = None

        # Prefer session/message timestamps for time gap.
        session_id = sessionId or await _get_latest_session_id(tenantId, userId)
        last_user_text = None
        if session_id:
            try:
                last_activity_time = await session.get_last_interaction_time(tenantId, session_id)
            except Exception:
                last_activity_time = None
            try:
                working_memory = await session.get_working_memory(tenantId, session_id)
                for msg in reversed(working_memory):
                    if msg.role == "user" and msg.text:
                        last_user_text = msg.text
                        break
            except Exception:
                last_user_text = None

        try:
            logger.info("startbrief graphiti: get_recent_episode_summaries")
            episodes = await graphiti_client.get_recent_episode_summaries(
                tenant_id=tenantId,
                user_id=userId,
                limit=1
            )
            if episodes:
                last_session_summary = episodes[0].get("summary")
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
            last_session_summary = None

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
                last_activity_time = last_activity_time.replace(tzinfo=tzinfo or timezone.utc)
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

        bridge_text = None
        if last_session_summary:
            candidates = []
            allow_environment = _explicit_environment_in_text(last_user_text)
            logger.info(
                "startbrief env guard: allow_environment=%s last_user_text=%s",
                allow_environment,
                (last_user_text or "")[:120]
            )
            for claim in _split_claims(last_session_summary):
                if not _allow_claim(claim) or _is_explicit_user_state_claim(claim):
                    continue
                if not _allow_fact_text(claim):
                    continue
                if _looks_like_environment(claim) and not allow_environment:
                    continue
                candidates.append(_shorten_line(claim, 140))
                if len(candidates) >= 2:
                    break
            if candidates:
                logger.info("startbrief bridge_parts=%s", candidates)
                bridge_text = " ".join(candidates)
                logger.info("startbrief bridge_text=%s", bridge_text)

        logger.info("startbrief loops: get_top_loops_for_startbrief")
        top_loops = await loops.get_top_loops_for_startbrief(
            tenant_id=tenantId,
            user_id=userId,
            limit=5,
            persona_id=personaId
        )

        items: List[SessionStartBriefItem] = []
        for loop_item in top_loops:
            items.append(SessionStartBriefItem(
                kind="loop",
                type=loop_item.type,
                text=loop_item.text,
                timeHorizon=loop_item.timeHorizon,
                dueDate=loop_item.dueDate,
                salience=loop_item.salience,
                lastSeenAt=loop_item.lastSeenAt
            ))
            if len(items) >= 5:
                break

        if len(items) < 5:
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
                if any(i.text.lower() == description.lower() for i in items):
                    continue
                items.append(SessionStartBriefItem(
                    kind="tension",
                    text=_shorten_line(description, 120)
                ))
                if len(items) >= 5:
                    break

        if not bridge_text and items:
            bridge_text = f"Last time you spoke, you mentioned: {items[0].text}."

        if bridge_text and len(bridge_text) > 280:
            bridge_text = bridge_text[:277].rstrip() + "..."

        return SessionStartBriefResponse(
            timeOfDayLabel=time_of_day_label,
            timeGapHuman=time_gap_human,
            bridgeText=bridge_text,
            items=items
        )
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

        await graphiti_client.add_session_episode(
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


@app.get("/internal/debug/graphiti/session_summaries")
async def debug_graphiti_session_summaries(
    tenantId: str,
    userId: str,
    limit: int = 5,
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
        rows = await driver.execute_query(
            """
            MATCH (n:SessionSummary {group_id: $group_id})
            RETURN n.name AS name,
                   n.summary AS summary,
                   n.attributes AS attributes,
                   n.created_at AS created_at,
                   n.uuid AS uuid
            ORDER BY n.created_at DESC
            LIMIT $limit
            """,
            group_id=composite_user_id,
            limit=limit
        )
        summaries = []
        for row in rows or []:
            if isinstance(row, dict):
                summaries.append({
                    "name": row.get("name"),
                    "summary": row.get("summary"),
                    "attributes": row.get("attributes") or {},
                    "created_at": row.get("created_at"),
                    "uuid": row.get("uuid")
                })
            elif isinstance(row, (list, tuple)):
                name = row[0] if len(row) > 0 else None
                summary = row[1] if len(row) > 1 else None
                attributes = row[2] if len(row) > 2 else {}
                created_at = row[3] if len(row) > 3 else None
                uuid = row[4] if len(row) > 4 else None
                summaries.append({
                    "name": name,
                    "summary": summary,
                    "attributes": attributes or {},
                    "created_at": created_at,
                    "uuid": uuid
                })
        return {"count": len(summaries), "summaries": summaries}
    except Exception as e:
        logger.error(f"Debug graphiti session_summaries failed: {e}")
        raise HTTPException(status_code=500, detail="Debug graphiti session_summaries failed")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
