from fastapi import FastAPI, HTTPException, BackgroundTasks, Header
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
from datetime import datetime
import logging

from .models import IngestRequest, BriefRequest, IngestResponse, BriefResponse
from .config import get_settings
from .db import Database
from .graphiti_client import GraphitiClient
from . import session, loops, identity
from . import identity_cache
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
        loops.init_loop_manager(db)
        identity.init_identity_manager(db)
        identity_cache.init_identity_cache_manager(db)
        logger.info("All managers initialized")

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
        except Exception:
            pass
    if getattr(app.state, "outbox_drain_task", None):
        app.state.outbox_drain_task.cancel()
        try:
            await app.state.outbox_drain_task
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
    - Extracts identity updates
    - Stores episodic memory in Graphiti
    - Detects and creates loops (commitments, habits, frictions)
    - Updates session state (mood, activity, location)
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
    Generate a comprehensive briefing for the AI companion.

    This endpoint assembles:
    - Identity (canonical facts)
    - Temporal authority (current time/day)
    - Session state (working memory state)
    - Working memory (recent messages)
    - Episode bridge (narrative since last interaction)
    - Semantic context (relevant facts from query)
    - Active loops (pending commitments, habits, etc.)
    - Instructions (contextual guidance for the AI)
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


@app.get("/internal/debug/loops")
async def debug_loops(tenantId: str, userId: str, x_internal_token: str | None = Header(default=None)):
    """Debug endpoint for active loops."""
    _require_internal_token(x_internal_token)
    try:
        rows = await loops.get_active_loops_debug(tenantId, userId)
        return {"loops": rows}
    except Exception as e:
        logger.error(f"Debug loops endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/internal/debug/nudges")
async def debug_nudges(tenantId: str, userId: str, x_internal_token: str | None = Header(default=None)):
    """Debug endpoint for pending nudge candidates."""
    _require_internal_token(x_internal_token)
    try:
        rows = await db.fetch(
            """
            SELECT id, type, text, status, metadata
            FROM loops
            WHERE tenant_id = $1
              AND user_id = $2
              AND status = 'active'
              AND metadata ? 'pending_nudge'
            ORDER BY updated_at DESC
            """,
            tenantId,
            userId
        )
        return {"nudges": rows}
    except Exception as e:
        logger.error(f"Debug nudges endpoint error: {e}")
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
        active_loops = await loops.get_active_loops_debug(tenantId, userId)
        session_state = session_row.get("session_state") if session_row else {}
        return {
            "session": session_row,
            "transcript": transcript,
            "activeLoops": active_loops,
            "sessionEpisode": {
                "name": session_state.get("session_episode_name") if session_state else None,
                "text": session_state.get("session_episode_text") if session_state else None
            }
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
        episode_bridge = None
        try:
            episode_bridge = await graphiti_client.get_latest_session_summary(
                tenant_id=tenantId,
                user_id=userId
            )
        except Exception:
            episode_bridge = None
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
        active_loops = await loops.get_active_loops_debug(tenantId, userId)
        return {
            "latestSessionId": session_id,
            "lastInteractionTime": last_interaction.isoformat() if last_interaction else None,
            "episodeBridge": episode_bridge,
            "activeLoops": active_loops,
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
