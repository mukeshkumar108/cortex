"""
Ingestion Pipeline - Sliding Window Architecture

Flow:
1. Add turn to session buffer
2. If buffer exceeds 12 messages, trigger janitor (background)
3. Check for session close (gap > 30 min)
4. Return immediately (don't block on background tasks)

Graphiti-native: no local semantic extraction on ingest.
"""

from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4
import logging
from fastapi import BackgroundTasks
from .models import IngestRequest, IngestResponse
from .graphiti_client import GraphitiClient
from . import session
from .utils import is_noise

logger = logging.getLogger(__name__)


async def ingest(
    request: IngestRequest,
    graphiti_client: GraphitiClient,
    background_tasks: BackgroundTasks
) -> IngestResponse:
    """
    Ingestion pipeline with sliding window architecture.

    Returns immediately - all heavy lifting happens in background.
    """
    try:
        # Parse timestamp
        timestamp = datetime.fromisoformat(request.timestamp.replace('Z', '+00:00'))

        # Resolve session ID (prefer top-level, then metadata, else auto-generate)
        session_id = request.sessionId or (request.metadata.get('sessionId') if request.metadata else None)
        if not session_id:
            session_id = f"session-{uuid4().hex}"

        # 1. CHECK FOR SESSION CLOSE (gap > 30 min)
        should_close = await session.should_close_session(
            tenant_id=request.tenantId,
            session_id=session_id,
            current_time=timestamp
        )

        if should_close:
            logger.info(f"Closing session {session_id} due to 30min gap")
            background_tasks.add_task(
                session.close_session,
                tenant_id=request.tenantId,
                session_id=session_id,
                user_id=request.userId,
                graphiti_client=graphiti_client,
                persona_id=request.personaId
            )

        # 2. ADD TURN TO BUFFER (returns oldest_turn if evicted)
        oldest_turn = await session.add_turn(
            tenant_id=request.tenantId,
            session_id=session_id,
            user_id=request.userId,
            role=request.role,
            text=request.text,
            timestamp=request.timestamp
        )

        # 3. IF BUFFER EXCEEDED, TRIGGER JANITOR (background, gated)
        if oldest_turn and request.role == "user":
            outbox_count = await session.get_outbox_count(
                tenant_id=request.tenantId,
                session_id=session_id,
                user_id=request.userId
            )
            last_run = await session.get_last_janitor_run_at(
                tenant_id=request.tenantId,
                session_id=session_id
            )
            now_ts = datetime.utcnow()
            elapsed = (now_ts - last_run).total_seconds() if last_run else None
            if outbox_count >= 8 and (elapsed is None or elapsed >= 60):
                await session.set_last_janitor_run_at(
                    tenant_id=request.tenantId,
                    session_id=session_id,
                    ts=now_ts
                )
                logger.info(
                    f"Triggering janitor for {session_id} "
                    f"(reason=threshold+cooldown, outbox_count={outbox_count}, "
                    f"elapsed={int(elapsed) if elapsed is not None else 'none'}s)"
                )
                background_tasks.add_task(
                    session.janitor_process,
                    tenant_id=request.tenantId,
                    session_id=session_id,
                    user_id=request.userId,
                    graphiti_client=graphiti_client
                )

        # 4. IDENTITY UPDATES
        # Disabled: identity is derived from Graphiti and cached in Postgres.

        # 5. CHECK IF NOISE
        if is_noise(request.text):
            logger.info(f"Skipping noise: {request.text[:50]}")
            return IngestResponse(
                status="skipped",
                sessionId=session_id,
                graphitiAdded=False
            )

        # 6. RETURN IMMEDIATELY
        return IngestResponse(
            status="ingested",
            sessionId=session_id,
            identityUpdates=None,
            loopsDetected=None,
            loopsCompleted=None,
            graphitiAdded=False
        )

    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        return IngestResponse(
            status="error",
            sessionId=session_id if 'session_id' in locals() else None,
            graphitiAdded=False
        )
