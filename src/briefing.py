"""
Briefing Builder - Graphiti-native v1

Briefing is intentionally minimal. Semantic memory comes from Graphiti queries
on-demand by the orchestrator, not from a monolithic brief.
"""

from datetime import datetime
from typing import Optional, List
import logging
from .models import BriefResponse, TemporalAuthority, Fact, Entity
from .graphiti_client import GraphitiClient
from . import session
from .utils import get_time_of_day, format_time_gap

logger = logging.getLogger(__name__)


async def build_briefing(
    tenant_id: str,
    user_id: str,
    persona_id: str,
    session_id: Optional[str],
    query: Optional[str],
    now: datetime,
    graphiti_client: GraphitiClient
) -> BriefResponse:
    """
    Build a minimal briefing for session start.

    Postgres only: rolling summary + working memory + temporal authority.
    Semantic memory is queried from Graphiti on-demand by the orchestrator.
    """
    try:
        # 1. TEMPORAL AUTHORITY (always present)
        temporal_authority = _build_temporal_authority(now)

        # 2. SESSION BUFFER (rolling summary + working memory)
        rolling_summary = ""
        working_memory = []

        if session_id:
            rolling_summary = await session.get_rolling_summary(tenant_id, session_id)
            working_memory = await session.get_working_memory(tenant_id, session_id)

            # Calculate time gap for temporal authority
            last_interaction = await session.get_last_interaction_time(tenant_id, session_id)
            if last_interaction:
                time_gap = now - last_interaction
                time_gap_minutes = int(time_gap.total_seconds() / 60)

                if time_gap_minutes > 30:
                    temporal_authority.timeSinceLastInteraction = format_time_gap(time_gap_minutes)

        # 3. No semantic enrichment here. Orchestrator queries Graphiti on-demand.
        semantic_context: List[Fact] = []
        entities: List[Entity] = []
        episode_bridge: Optional[str] = None
        instructions: List[str] = []
        nudge_candidates: List = []
        active_loops = []

        # 4. METADATA
        metadata = {
            "queryTime": now.isoformat(),
            "bufferSize": len(working_memory),
            "hasRollingSummary": bool(rolling_summary),
            "graphitiFacts": len(semantic_context),
            "graphitiEntities": len(entities),
            "loopsCount": 0
        }

        return BriefResponse(
            identity={"name": None, "isDefault": True},
            temporalAuthority=temporal_authority,
            sessionState=None,
            workingMemory=working_memory,
            rollingSummary=rolling_summary,
            activeLoops=active_loops,
            nudgeCandidates=nudge_candidates,
            episodeBridge=episode_bridge,
            semanticContext=semantic_context,
            entities=entities,
            instructions=instructions,
            metadata=metadata
        )

    except Exception as e:
        logger.error(f"Failed to build briefing: {e}")
        raise


def _build_temporal_authority(now: datetime) -> TemporalAuthority:
    """Build temporal authority context"""
    return TemporalAuthority(
        currentTime=now.strftime("%I:%M %p"),
        currentDay=now.strftime("%A"),
        timeOfDay=get_time_of_day(now.hour),
        timeSinceLastInteraction=None
    )

