"""
Briefing Builder - Simplified Architecture

Simple structure:
- Postgres: Fast retrieval of buffer (rolling summary + last 6 turns) and loops
- Graphiti: Enrichment with semantic facts and entities
- Graceful degradation: Works even if Graphiti fails

No tier separation, no complex timeout logic - just straightforward retrieval.
"""

from datetime import datetime
from typing import Optional, List
import logging
from .models import BriefResponse, TemporalAuthority, Fact, Entity, Message, NudgeCandidate
from .graphiti_client import GraphitiClient
from . import session, loops, identity_cache
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
    Build a comprehensive briefing with clean architecture.

    Fast path: Postgres (buffer + loops)
    Enrichment: Graphiti (facts + entities)
    """
    try:
        # 1. TEMPORAL AUTHORITY (always present)
        temporal_authority = _build_temporal_authority(now)

        # 2. IDENTITY (derived from Graphiti, cached in Postgres)
        identity_data = await identity_cache.get_identity_for_brief(
            tenant_id,
            user_id,
            graphiti_client
        )

        # 3. SESSION BUFFER (rolling summary + working memory)
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

        # 4. ACTIVE LOOPS (procedural state)
        active_loops = await loops.get_active_loops(tenant_id, user_id, persona_id, limit=5)
        nudge_candidates = _build_nudge_candidates(active_loops, now)

        # 5. GRAPHITI ENRICHMENT (best-effort)
        semantic_context: List[Fact] = []
        entities: List[Entity] = []
        episode_bridge: Optional[str] = None

        # If new session, try to fetch latest session summary as episode bridge
        if session_id and not working_memory and not rolling_summary:
            try:
                episode_bridge = await graphiti_client.get_latest_session_summary(
                    tenant_id=tenant_id,
                    user_id=user_id
                )
            except Exception as e:
                logger.warning(f"Graphiti session summary fetch failed: {e}")

        if query:
            query_hint = _build_query_hint(working_memory) if len(query.split()) <= 3 else None
            try:
                # Search for facts
                fact_results = await graphiti_client.search_facts(
                    tenant_id,
                    user_id,
                    query,
                    limit=10,
                    reference_time=now,
                    query_hint=query_hint
                )
                semantic_context = [
                    Fact(
                        text=r['text'],
                        relevance=r.get('relevance'),
                        source=r.get('source', 'graphiti')
                    )
                    for r in fact_results
                ]
            except Exception as e:
                logger.warning(f"Graphiti fact search failed: {e}")

            try:
                # Search for entities
                entity_results = await graphiti_client.search_nodes(
                    tenant_id,
                    user_id,
                    query,
                    limit=5,
                    reference_time=now,
                    query_hint=query_hint
                )
                entities = [
                    Entity(
                        summary=e['summary'],
                        type=e.get('type'),
                        uuid=e.get('uuid')
                    )
                    for e in entity_results
                    if e.get('summary')
                ]
            except Exception as e:
                logger.warning(f"Graphiti entity search failed: {e}")

        # 6. INSTRUCTIONS (contextual guidance)
        instructions = _generate_instructions(
            temporal_authority,
            active_loops
        )

        # 7. METADATA
        metadata = {
            "queryTime": now.isoformat(),
            "bufferSize": len(working_memory),
            "hasRollingSummary": bool(rolling_summary),
            "graphitiFacts": len(semantic_context),
            "graphitiEntities": len(entities),
            "loopsCount": len(active_loops)
        }

        return BriefResponse(
            identity=identity_data or {"name": None, "isDefault": True},
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


def _generate_instructions(
    temporal_authority: TemporalAuthority,
    active_loops: list
) -> List[str]:
    """
    Generate contextual instructions for the AI companion.

    Simplified from previous version - only time and loop-based.
    """
    instructions = []

    # Time-based instructions
    time_of_day = temporal_authority.timeOfDay
    if time_of_day == "morning":
        instructions.append("It's morning - be energizing and encouraging")
    elif time_of_day == "evening":
        instructions.append("It's evening - be warm and gentle")
    elif time_of_day == "night":
        instructions.append("It's late - be calm and supportive")

    # Loop-based instructions
    if active_loops:
        loop_types = {loop.type for loop in active_loops}
        if 'commitment' in loop_types:
            instructions.append("User has pending commitments - gently reference them if relevant")
        if 'friction' in loop_types:
            instructions.append("User has open frictions - be empathetic and supportive")

    return instructions


def _build_query_hint(working_memory: List[Message]) -> Optional[str]:
    if not working_memory:
        return None

    last_turns = working_memory[-2:]
    snippets = []
    for turn in last_turns:
        role = turn.role if hasattr(turn, "role") else turn.get("role")
        text = turn.text if hasattr(turn, "text") else turn.get("text")
        if not text:
            continue
        snippets.append(f"{role}: {text[:120]}")

    if not snippets:
        return None

    return "Recent turns: " + " | ".join(snippets)


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value.replace("Z", "+00:00")
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _build_nudge_candidates(active_loops: list, now: datetime) -> List[NudgeCandidate]:
    candidates: List[NudgeCandidate] = []
    if not active_loops:
        return candidates

    for loop in active_loops:
        metadata = loop.metadata or {}
        pending = metadata.get("pending_nudge") if isinstance(metadata, dict) else None
        if not isinstance(pending, dict):
            continue

        last_nudged_at = _parse_iso_datetime(metadata.get("last_nudged_at"))
        if last_nudged_at and (now - last_nudged_at).total_seconds() < 6 * 3600:
            continue

        question = pending.get("question") or f"Is this done: {loop.text}?"
        confidence = float(pending.get("confidence", 0.0))
        evidence_text = pending.get("evidence_text") or metadata.get("last_action_evidence_text")

        candidates.append(NudgeCandidate(
            loopId=loop.id,
            type=loop.type,
            text=loop.text,
            question=question,
            confidence=confidence,
            evidenceText=evidence_text
        ))

    return candidates
