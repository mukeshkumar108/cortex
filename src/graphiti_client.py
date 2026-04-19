import inspect
import asyncio
import re
from graphiti_core import Graphiti
from graphiti_core.llm_client.client import LLMClient
from graphiti_core.llm_client.config import LLMConfig
from graphiti_core.nodes import EpisodeType, EpisodicNode, EntityNode
from graphiti_core.edges import EntityEdge
from graphiti_core.driver.falkordb_driver import FalkorDriver
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone as dt_timezone
import logging
from .config import get_settings
from .falkor_utils import extract_node_dicts, extract_query_rows, pick_first_node
from .memory_ontology import (
    CANONICAL_EDGE_METADATA_FIELDS,
    CANONICAL_NODE_METADATA_FIELDS,
    CANONICAL_PROVENANCE_FIELDS,
    ONTOLOGY_NODE_TYPES,
    canonicalize_entity_name,
    choose_preferred_node,
    infer_ontology_type,
    validate_ontology_node,
)
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

_SEMANTIC_QUERY_STOPWORDS = {
    "the", "a", "an", "and", "or", "for", "to", "of", "in", "on", "at", "by",
    "with", "from", "is", "are", "was", "were", "be", "been", "being", "do",
    "did", "does", "why", "what", "when", "where", "who", "how", "i", "me",
    "my", "you", "your", "we", "our", "it", "this", "that", "these", "those",
}

class MemoryNodeMetadata(BaseModel):
    domains: List[str] = Field(
        default_factory=list,
        description=f"Subset of ontology domains. Canonical node metadata fields: {', '.join(CANONICAL_NODE_METADATA_FIELDS['all_nodes'])}",
    )
    confidence: Optional[float] = Field(None, description="0..1 trust in this node's current semantic fit")
    importance: Optional[str] = Field(None, description="low, medium, or high durable importance")
    salience: Optional[str] = Field(None, description="low, medium, or high short-horizon prominence")
    first_seen_at: Optional[str] = Field(None, description="ISO timestamp for earliest known supporting evidence")
    last_seen_at: Optional[str] = Field(None, description="ISO timestamp for latest known supporting evidence")


class Person(MemoryNodeMetadata):
    """A durable person in the user's autobiographical memory."""
    aliases: List[str] = Field(
        default_factory=list,
        description=f"Stable alternative names only. Identity node metadata fields: {', '.join(CANONICAL_NODE_METADATA_FIELDS['identity_nodes'])}",
    )


class Project(MemoryNodeMetadata):
    """A durable project, product, organization, or work stream."""
    aliases: List[str] = Field(
        default_factory=list,
        description=f"Stable alternative names only. Identity node metadata fields: {', '.join(CANONICAL_NODE_METADATA_FIELDS['identity_nodes'])}",
    )


class Goal(MemoryNodeMetadata):
    """A durable user goal or pursuit."""
    status: Optional[str] = Field(None, description="active, paused, completed, or unknown")
    time_horizon: Optional[str] = Field(None, description="short_term, ongoing, quarterly, yearly, or long_term")


class Loop(MemoryNodeMetadata):
    """An unresolved ongoing thread tied to a durable person/project/goal."""
    status: Optional[str] = Field(None, description="open, blocked, waiting, or unknown")
    time_horizon: Optional[str] = Field(None, description="today, this_week, near_term, or unknown")


class Preference(MemoryNodeMetadata):
    """A stable preference explicitly expressed by the user."""
    polarity: Optional[str] = Field(None, description="prefer, avoid, or neutral")


class Event(MemoryNodeMetadata):
    """A concrete autobiographical event or episode."""
    occurred_at: Optional[str] = Field(None, description="ISO timestamp or coarse date when known")
    significance: Optional[str] = Field(None, description="low, medium, or high life-event significance")


class MemoryEdgeMetadata(BaseModel):
    confidence: Optional[float] = Field(
        None,
        description=f"0..1 trust in the claim carried by this edge. Canonical edge metadata fields: {', '.join(CANONICAL_EDGE_METADATA_FIELDS['all_edges'])}",
    )
    importance: Optional[str] = Field(None, description="low, medium, or high durable importance of this relationship")
    valid_at: Optional[str] = Field(None, description="ISO timestamp when a time-bounded claim is known to be valid")
    invalid_at: Optional[str] = Field(None, description="ISO timestamp when a time-bounded claim is no longer valid")


class RelatedToUserAs(MemoryEdgeMetadata):
    """Edge: relationship between the user and a person with explicit role metadata."""
    role: Optional[str] = Field(None, description="girlfriend, daughter, friend, etc.")


class WorkingOn(MemoryEdgeMetadata):
    """Edge: the user is actively working on a project."""
    pass


class Pursuing(MemoryEdgeMetadata):
    """Edge: the user is actively pursuing a goal."""
    pass


class About(MemoryEdgeMetadata):
    """Fallback edge only when a more specific relation does not apply."""
    pass


class Prefers(MemoryEdgeMetadata):
    """Edge: the user prefers a thing or way of working."""
    pass


class InvolvedIn(MemoryEdgeMetadata):
    """Edge: a person or project is involved in an event."""
    pass


class CaresAbout(MemoryEdgeMetadata):
    """Edge: durable concern or care relationship to an entity."""
    pass


class EvidenceFor(MemoryEdgeMetadata):
    """Edge: provenance link grounding a memory node or fact in an event/episode."""
    provenance: Optional[str] = Field(
        None,
        description=f"Compact provenance summary. Canonical provenance fields: {', '.join(CANONICAL_PROVENANCE_FIELDS)}",
    )
    session_id: Optional[str] = Field(None, description="Session identifier when known")
    episode_uuid: Optional[str] = Field(None, description="Episode UUID when known")
    timestamp: Optional[str] = Field(None, description="Message or event timestamp when known")
    message_index: Optional[int] = Field(None, description="Supporting transcript message index when known")
    evidence_summary: Optional[str] = Field(None, description="Short grounding summary, not a new abstraction layer")


NARRATIVE_ENTITY_TYPES: Dict[str, type[BaseModel]] = {
    "Person": Person,
    "Project": Project,
    "Goal": Goal,
    "Loop": Loop,
    "Preference": Preference,
    "Event": Event,
}

NARRATIVE_EDGE_TYPES: Dict[str, type[BaseModel]] = {
    "RELATED_TO_USER_AS": RelatedToUserAs,
    "WORKING_ON": WorkingOn,
    "PURSUING": Pursuing,
    "ABOUT": About,
    "PREFERS": Prefers,
    "INVOLVED_IN": InvolvedIn,
    "CARES_ABOUT": CaresAbout,
    "EVIDENCE_FOR": EvidenceFor,
}

NARRATIVE_EDGE_TYPE_MAP: Dict[Tuple[str, str], List[str]] = {
    ("Person", "Person"): ["RELATED_TO_USER_AS", "CARES_ABOUT", "EVIDENCE_FOR"],
    ("Person", "Project"): ["WORKING_ON", "CARES_ABOUT", "ABOUT", "EVIDENCE_FOR"],
    ("Person", "Goal"): ["PURSUING", "ABOUT", "EVIDENCE_FOR"],
    ("Person", "Loop"): ["ABOUT", "EVIDENCE_FOR"],
    ("Person", "Preference"): ["PREFERS", "EVIDENCE_FOR"],
    ("Person", "Event"): ["INVOLVED_IN", "EVIDENCE_FOR"],
    ("Goal", "Person"): ["ABOUT", "EVIDENCE_FOR"],
    ("Goal", "Project"): ["ABOUT", "EVIDENCE_FOR"],
    ("Loop", "Person"): ["ABOUT", "EVIDENCE_FOR"],
    ("Loop", "Project"): ["ABOUT", "EVIDENCE_FOR"],
    ("Event", "Person"): ["ABOUT", "INVOLVED_IN", "EVIDENCE_FOR"],
    ("Event", "Project"): ["ABOUT", "INVOLVED_IN", "EVIDENCE_FOR"],
}

NARRATIVE_EXTRACTION_INSTRUCTIONS = (
    "Extract autobiographical memory for a premium assistant. Prefer a small durable ontology only: "
    "Person, Project, Goal, Loop, Preference, and Event. Treat domain as metadata, not as a standalone entity. "
    "Do not create Fact as a standalone node type. Facts are grounded claims carried by nodes and edges with provenance. "
    "Preserve connected structure around durable hubs such as important people and projects. "
    "Interpret meaning from context, not from isolated keywords. "
    "Goal means a desired outcome or direction only; never use Goal for an active unresolved follow-up thread. "
    "Loop means an active unresolved thread requiring continuation or attention; never use Loop for long-horizon aspiration. "
    "Preference must be stable and behaviorally relevant; never create it from one-off requests, temporary context, or inferred personality traits. "
    "Event must be a meaningful autobiographical occurrence or interaction; do not create Event for every transcript turn or trivial fragment. "
    "Store trust and ranking signals as metadata, not as extra nodes: use confidence, importance, salience, first_seen_at, last_seen_at, and occurred_at when known. "
    "Use life-event significance only as metadata on Event when the event is genuinely consequential. "
    "Use related_to_user_as edges with explicit role metadata for relationship people. "
    "Prefer specific edges such as working_on, pursuing, involved_in, and prefers. Use about only sparingly as a fallback when a more specific edge does not fit. "
    "Every Event must be evidence-bearing. Every evidence_for edge must include provenance with session_id, timestamp, and message_index when available, plus episode_uuid or a short evidence_summary when known. "
    "An Event should usually connect to at least one canonical node such as a Person, Project, Goal, Loop, or Preference. "
    "If an Event cannot be tied to something meaningful and evidence-backed, suppress it. "
    "The graph is not a transcript mirror: do not promote raw conversational fragments into nodes unless they are meaningful, durable, and evidence-backed. "
    "Suppress generic noun phrases, summary artifacts, debug/system phrases, and one-off conceptual junk when they do not carry durable autobiographical meaning. "
    "Do not create entities like session-summary placeholders or generic artifact labels when they are only conversational scaffolding. "
    "Avoid duplicate Goal and Loop nodes for the same concept. Prefer one clean canonical node. "
    "Apply precedence when candidates overlap: canonical Person/Project identity beats weaker variants; Loop beats Goal for active unresolved follow-up; Goal beats Loop for longer-horizon direction; Event supports canonical nodes via evidence_for and does not replace them. "
    "When recency matters, prefer first_seen_at and last_seen_at metadata over vague recency wording. "
    "Only create a node when it is durable, autobiographically meaningful, and evidence-backed by the transcript."
)


def _is_edge_result(result: Any) -> bool:
    """Check if a result object is an edge (relationship) rather than a node"""
    # Edges typically have source_node_id and target_node_id
    if hasattr(result, 'source_node_id') and hasattr(result, 'target_node_id'):
        return True

    # Edges may have a 'fact' field
    if hasattr(result, 'fact') and not hasattr(result, 'entity_type'):
        return True

    return False


def _is_predicate_string(text: Optional[str]) -> bool:
    """Check if a string looks like a predicate (edge name) rather than an entity"""
    if not text or not isinstance(text, str):
        return False

    text_upper = text.upper()

    # Common edge predicates
    predicates = [
        "IS_", "HAS_", "WORKS_", "LIVES_", "KNOWS_", "OWNS_",
        "MANAGES_", "REPORTS_", "BELONGS_", "CONTAINS_", "LOCATED_",
        "_OF", "_AT", "_IN", "_TO", "_WITH", "_FOR"
    ]

    # Check if it matches predicate patterns
    for pred in predicates:
        if pred in text_upper:
            return True

    # Check if it's all caps with underscores (typical predicate format)
    if text_upper == text and "_" in text:
        return True

    return False


def _semantic_query_terms(query: Optional[str]) -> List[str]:
    text = str(query or "").strip().lower()
    if not text:
        return []
    tokens = re.findall(r"[a-z0-9]+", text)
    return [token for token in tokens if len(token) >= 2 and token not in _SEMANTIC_QUERY_STOPWORDS]


def _format_predicate_as_fact(result: Any) -> str:
    """
    Convert a predicate/edge result into a readable fact sentence.

    If result has source/target nodes, construct: "Source predicate Target"
    Otherwise, return the predicate name cleaned up.
    """
    predicate = getattr(result, 'name', '')

    # Try to get source and target node names
    source_name = None
    target_name = None

    if hasattr(result, 'source_node_uuid'):
        # If we have node references but not names, use the predicate
        pass

    # Clean up predicate name (IS_SIBLING_OF -> "is sibling of")
    if predicate:
        readable = predicate.replace('_', ' ').lower()
        return readable

    return str(result)


class GraphitiClient:
    def __init__(self):
        self.client: Optional[Graphiti] = None
        self.settings = get_settings()
        self._initialized = False
        self._metrics: Dict[str, int] = {
            "session_summary_attempted": 0,
            "session_summary_verified": 0,
            "session_summary_failed": 0,
        }

    def _inc_metric(self, name: str, amount: int = 1) -> None:
        self._metrics[name] = int(self._metrics.get(name, 0)) + amount
        logger.info("metric %s=%s", name, self._metrics[name])

    def get_session_summary_write_metrics(self) -> Dict[str, int]:
        return dict(self._metrics)

    @staticmethod
    def _rows(result: Any) -> List[Any]:
        return extract_query_rows(result)

    @staticmethod
    def _failure_response(reason: str, error: Optional[Exception] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"success": False, "reason": reason}
        if error is not None:
            payload["error"] = str(error)
            status = getattr(error, "status_code", None)
            if status is None:
                status = getattr(error, "status", None)
            if status is not None:
                try:
                    payload["status_code"] = int(status)
                except Exception:
                    pass
        return payload

    async def _session_summary_exists(self, driver: Any, group_id: str, session_id: str) -> bool:
        try:
            rows = await driver.execute_query(
                """
                MATCH (n:SessionSummary {group_id: $group_id, session_id: $session_id})
                RETURN properties(n) AS props
                LIMIT 1
                """,
                group_id=group_id,
                session_id=session_id
            )
            for row in self._rows(rows):
                for node in extract_node_dicts(row, required_keys=("uuid",)):
                    if node.get("uuid"):
                        return True
            return False
        except Exception as e:
            logger.warning("SessionSummary verify query failed session_id=%s error=%s", session_id, e)
            return False

    async def _session_summary_uuid_exists(self, driver: Any, group_id: str, summary_uuid: str) -> bool:
        try:
            rows = await driver.execute_query(
                """
                MATCH (n:SessionSummary {group_id: $group_id, uuid: $summary_uuid})
                RETURN properties(n) AS props
                LIMIT 1
                """,
                group_id=group_id,
                summary_uuid=summary_uuid,
            )
            for row in self._rows(rows):
                for node in extract_node_dicts(row, required_keys=("uuid",)):
                    if str(node.get("uuid") or "") == str(summary_uuid):
                        return True
            return False
        except Exception as e:
            logger.warning(
                "SessionSummary uuid verify query failed summary_uuid=%s error=%s",
                summary_uuid,
                e,
            )
            return False

    async def _get_session_summary_uuids(self, driver: Any, group_id: str, session_id: str) -> List[str]:
        try:
            rows = await driver.execute_query(
                """
                MATCH (n:SessionSummary {group_id: $group_id, session_id: $session_id})
                RETURN properties(n) AS props
                """,
                group_id=group_id,
                session_id=session_id,
            )
            uuids: List[str] = []
            seen = set()
            for row in self._rows(rows):
                for node in extract_node_dicts(row, required_keys=("uuid",)):
                    uuid = str(node.get("uuid") or "").strip()
                    if not uuid or uuid in seen:
                        continue
                    seen.add(uuid)
                    uuids.append(uuid)
            return uuids
        except Exception as e:
            logger.warning(
                "Failed to list existing SessionSummary uuids session_id=%s error=%s",
                session_id,
                e,
            )
            return []

    async def _delete_session_summary_uuids(
        self,
        driver: Any,
        group_id: str,
        uuids: List[str],
        *,
        session_id: str
    ) -> None:
        targets = [str(x).strip() for x in (uuids or []) if str(x).strip()]
        if not targets:
            return
        removed = 0
        for summary_uuid in targets:
            try:
                await driver.execute_query(
                    """
                    MATCH (n:SessionSummary {group_id: $group_id, uuid: $summary_uuid})
                    DETACH DELETE n
                    """,
                    group_id=group_id,
                    summary_uuid=summary_uuid,
                )
                removed += 1
            except Exception as e:
                logger.warning(
                    "SessionSummary cleanup delete failed session_id=%s uuid=%s error=%s",
                    session_id,
                    summary_uuid,
                    e,
                )
        if removed:
            logger.info(
                "SessionSummary cleanup removed %s stale node(s) session_id=%s",
                removed,
                session_id,
            )

    async def initialize(self):
        """Initialize Graphiti with FalkorDB connection"""
        if self._initialized:
            return

        try:
            logger.info("Initializing Graphiti client")

            username = self.settings.model_extra.get("FALKORDB_USERNAME")
            password = self.settings.model_extra.get("FALKORDB_PASSWORD")
            driver_kwargs = {
                "host": self.settings.falkordb_host,
                "port": self.settings.falkordb_port
            }
            if username:
                driver_kwargs["username"] = username
            if password:
                driver_kwargs["password"] = password

            falkor_driver = FalkorDriver(**driver_kwargs)

            llm_client = None
            api_key = self.settings.graphiti_llm_api_key or self.settings.openai_api_key
            if (
                self.settings.graphiti_llm_model
                or self.settings.graphiti_llm_base_url
                or self.settings.graphiti_llm_small_model
                or self.settings.graphiti_llm_api_key
            ):
                llm_config = LLMConfig(
                    api_key=api_key,
                    model=self.settings.graphiti_llm_model,
                    base_url=self.settings.graphiti_llm_base_url,
                    temperature=self.settings.graphiti_llm_temperature,
                    max_tokens=self.settings.graphiti_llm_max_tokens,
                    small_model=self.settings.graphiti_llm_small_model
                )
                llm_client = LLMClient(config=llm_config)

            # Initialize Graphiti with LLM extraction enabled
            # If llm_client is None, Graphiti uses its defaults (OpenAI if API key set)
            self.client = Graphiti(graph_driver=falkor_driver, llm_client=llm_client)

            await self._maybe_build_indices()

            self._initialized = True
            logger.info("Graphiti client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Graphiti: {e}")
            self.client = None
            self._initialized = False
            return

    async def _maybe_build_indices(self) -> None:
        if not self.client:
            return

        build_indices = getattr(self.client, "build_indices", None)
        if not callable(build_indices):
            return

        try:
            result = build_indices()
            if inspect.isawaitable(result):
                await result
        except Exception as e:
            logger.warning(f"Graphiti build_indices failed: {e}")

    def _make_composite_user_id(self, tenant_id: str, user_id: str) -> str:
        """Create composite user ID for Graphiti"""
        return f"{tenant_id}__{user_id}"

    def _group_driver(self, tenant_id: str, user_id: str):
        """Return a driver scoped to the user's graph/database."""
        if not self.client:
            return None
        driver = getattr(self.client, "driver", None)
        if not driver:
            return None
        composite_user_id = self._make_composite_user_id(tenant_id, user_id)
        clone = getattr(driver, "clone", None)
        if callable(clone):
            try:
                return clone(database=composite_user_id)
            except Exception:
                return driver
        return driver

    def _format_message_transcript(self, messages: List[Dict[str, Any]]) -> str:
        lines: List[str] = []
        for idx, msg in enumerate(messages):
            role = (msg.get("role") or "").lower()
            if role == "assistant":
                role_label = "Assistant"
            elif role == "system":
                role_label = "System"
            else:
                role_label = "User"
            text = msg.get("text") or ""
            timestamp = str(msg.get("timestamp") or "").strip()
            prefix = f"[message_index={idx}]"
            if timestamp:
                prefix += f"[timestamp={timestamp}]"
            lines.append(f"{prefix} {role_label}: {text}")
        return "\n".join(lines)

    async def add_episode(
        self,
        tenant_id: str,
        user_id: str,
        text: str,
        timestamp: datetime,
        role: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        episode_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Add a conversation turn to Graphiti for LLM-based extraction.

        Graphiti will analyze the episode and extract entities, facts, and relationships.
        """
        if not self._initialized:
            await self.initialize()
        if not self._initialized or self.client is None:
            logger.warning("Graphiti client unavailable; skipping add_episode")
            return self._failure_response("graphiti_client_unavailable")

        try:
            composite_user_id = self._make_composite_user_id(tenant_id, user_id)

            # Prefix episode body with stable speaker tag
            tagged_text = text
            if role:
                role_tag = "ASSISTANT" if role.lower() == "assistant" else "USER" if role.lower() == "user" else "SYSTEM"
                tagged_text = f"{role_tag}: {text}"

            result = await self.client.add_episode(
                name=episode_name or f"episode_{timestamp.isoformat()}",
                episode_body=tagged_text,
                source=EpisodeType.message,
                source_description="synapse_conversation",
                reference_time=timestamp,
                group_id=composite_user_id,
                entity_types=NARRATIVE_ENTITY_TYPES,
                edge_types=NARRATIVE_EDGE_TYPES,
                edge_type_map=NARRATIVE_EDGE_TYPE_MAP,
                custom_extraction_instructions=NARRATIVE_EXTRACTION_INSTRUCTIONS
            )

            logger.info(f"Added episode for user {composite_user_id} (role={role})")
            episode_uuid = None
            try:
                episode = getattr(result, "episode", None)
                episode_uuid = getattr(episode, "uuid", None)
            except Exception:
                episode_uuid = None
            return {"success": True, "episode_uuid": episode_uuid}

        except Exception as e:
            logger.error(f"Failed to add episode: {e}")
            return self._failure_response("add_episode_exception", e)

    async def add_session_episode(
        self,
        tenant_id: str,
        user_id: str,
        messages: List[Dict[str, Any]],
        reference_time: datetime,
        episode_name: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        transcript = self._format_message_transcript(messages)
        return await self.add_episode(
            tenant_id=tenant_id,
            user_id=user_id,
            text=transcript,
            timestamp=reference_time,
            role=None,
            metadata=metadata,
            episode_name=episode_name
        )

    async def add_session_summary(
        self,
        tenant_id: str,
        user_id: str,
        session_id: str,
        summary_text: str,
        reference_time: datetime,
        bridge_text: Optional[str] = None,
        episode_uuid: Optional[str] = None,
        extra_attributes: Optional[Dict[str, Any]] = None,
        replace_existing_session: bool = False
    ) -> Dict[str, Any]:
        """Store a SessionSummary node in Graphiti and optionally link to an episode."""
        self._inc_metric("session_summary_attempted")
        if not self._initialized:
            await self.initialize()
        if not self._initialized or self.client is None:
            self._inc_metric("session_summary_failed")
            logger.warning("Graphiti client unavailable; skipping add_session_summary")
            return self._failure_response("graphiti_client_unavailable")

        driver = self._group_driver(tenant_id, user_id)
        if not driver:
            self._inc_metric("session_summary_failed")
            logger.warning("Graphiti driver unavailable; skipping add_session_summary")
            return self._failure_response("graphiti_driver_unavailable")

        composite_user_id = self._make_composite_user_id(tenant_id, user_id)
        stale_uuids: List[str] = []
        if replace_existing_session:
            stale_uuids = await self._get_session_summary_uuids(
                driver=driver,
                group_id=composite_user_id,
                session_id=session_id,
            )

        name = f"session_summary_{session_id}_{int(reference_time.timestamp())}"
        attributes: Dict[str, Any] = {
            "summary_text": summary_text,
            "bridge_text": bridge_text,
            "session_id": session_id,
            "reference_time": reference_time.isoformat()
        }
        if extra_attributes:
            attributes.update(extra_attributes)
        index_text = str(attributes.get("index_text") or "").strip()
        if not index_text:
            index_text = summary_text
            attributes["index_text"] = index_text
        node = EntityNode(
            name=name,
            group_id=composite_user_id,
            labels=["SessionSummary"],
            summary=index_text,
            attributes=attributes
        )
        try:
            saved = node.save(driver)
            if inspect.isawaitable(saved):
                await saved
            logger.info(
                f"SessionSummary node saved name={name} uuid={node.uuid} len={len(summary_text)}"
            )
        except Exception as e:
            self._inc_metric("session_summary_failed")
            logger.error(f"Failed to save SessionSummary node: {e}")
            return self._failure_response("session_summary_save_exception", e)

        initial_uuid = str(node.uuid)
        verified = await self._session_summary_uuid_exists(
            driver=driver,
            group_id=composite_user_id,
            summary_uuid=str(node.uuid),
        )
        if not verified:
            logger.warning(
                "SessionSummary verify miss by uuid; retrying once session_id=%s group_id=%s uuid=%s",
                session_id,
                composite_user_id,
                node.uuid,
            )
            await asyncio.sleep(0.25)

            retry_name = f"{name}_retry"
            retry_node = EntityNode(
                name=retry_name,
                group_id=composite_user_id,
                labels=["SessionSummary"],
                summary=index_text,
                attributes=attributes
            )
            try:
                retry_saved = retry_node.save(driver)
                if inspect.isawaitable(retry_saved):
                    await retry_saved
                logger.info(
                    "SessionSummary retry save completed name=%s uuid=%s len=%s",
                    retry_name,
                    retry_node.uuid,
                    len(summary_text)
                )
                node = retry_node
            except Exception as e:
                self._inc_metric("session_summary_failed")
                logger.error("SessionSummary retry save failed session_id=%s error=%s", session_id, e)
                return self._failure_response("session_summary_retry_save_exception", e)

            verified = await self._session_summary_uuid_exists(
                driver=driver,
                group_id=composite_user_id,
                summary_uuid=str(node.uuid),
            )
            if verified and initial_uuid and initial_uuid != str(node.uuid):
                await self._delete_session_summary_uuids(
                    driver=driver,
                    group_id=composite_user_id,
                    uuids=[initial_uuid],
                    session_id=session_id,
                )

        if not verified:
            self._inc_metric("session_summary_failed")
            logger.error(
                "ALERT write_verify_failed session_id=%s group_id=%s",
                session_id,
                composite_user_id
            )
            return {"success": False, "reason": "write_verify_failed"}

        if replace_existing_session and stale_uuids:
            stale_candidates = [
                uuid for uuid in stale_uuids
                if uuid and uuid != str(node.uuid)
            ]
            await self._delete_session_summary_uuids(
                driver=driver,
                group_id=composite_user_id,
                uuids=stale_candidates,
                session_id=session_id,
            )

        self._inc_metric("session_summary_verified")

        if episode_uuid:
            try:
                edge = EntityEdge(
                    group_id=composite_user_id,
                    source_node_uuid=node.uuid,
                    target_node_uuid=episode_uuid,
                    created_at=reference_time,
                    name="SUMMARIZES",
                    fact=f"Summary of session {session_id}",
                    attributes={
                        "session_id": session_id,
                        "reference_time": reference_time.isoformat()
                    }
                )
                saved_edge = edge.save(driver)
                if inspect.isawaitable(saved_edge):
                    await saved_edge
            except Exception as e:
                logger.error(f"Failed to save SUMMARIZES edge: {e}")

        return {"success": True, "summary_uuid": node.uuid}

    async def search_facts(
        self,
        tenant_id: str,
        user_id: str,
        query: str,
        limit: int = 10,
        reference_time: Optional[datetime] = None,
        query_hint: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for relevant facts (edges) in Graphiti using semantic query.

        Returns fact strings representing relationships and attributes
        extracted by Graphiti's LLM. Formats facts as readable sentences.
        """
        if not query or not str(query).strip():
            return []
        if not _semantic_query_terms(query):
            logger.info("search_facts skipped due to empty semantic query terms query=%r", query)
            return []
        if not self._initialized:
            await self.initialize()
        if not self._initialized or self.client is None:
            logger.warning("Graphiti client unavailable; skipping search_facts")
            return []

        try:
            composite_user_id = self._make_composite_user_id(tenant_id, user_id)
            scoped_driver = self._group_driver(tenant_id, user_id)

            # Search for relevant facts (edges)
            search_fn = self.client.search
            params = inspect.signature(search_fn).parameters
            kwargs: Dict[str, Any] = {
                "query": query,
                "group_ids": [composite_user_id],
                "num_results": limit
            }
            if "rerank" in params:
                kwargs["rerank"] = True
            if reference_time and "reference_time" in params:
                kwargs["reference_time"] = reference_time
            if scoped_driver is not None and "driver" in params:
                kwargs["driver"] = scoped_driver
            if query_hint:
                if "query_hint" in params:
                    kwargs["query_hint"] = query_hint
                elif "context" in params:
                    kwargs["context"] = query_hint

            results = await search_fn(**kwargs)

            facts = []
            for result in results:
                fact_text = None

                # Try to get human-readable fact text
                if hasattr(result, 'fact') and result.fact:
                    fact_text = result.fact
                elif hasattr(result, 'content'):
                    fact_text = result.content
                elif hasattr(result, 'name'):
                    # If it's just a predicate name, try to construct a readable fact
                    fact_text = _format_predicate_as_fact(result)
                else:
                    fact_text = str(result)

                # Skip if fact is empty or just a predicate without context
                if not fact_text or len(fact_text) < 3:
                    continue

                facts.append({
                    "text": fact_text,
                    "relevance": result.score if hasattr(result, 'score') else None,
                    "source": "graphiti",
                    "valid_at": getattr(result, "valid_at", None),
                    "invalid_at": getattr(result, "invalid_at", None),
                })

            logger.info(f"search_facts returned {len(facts)} results for query: {query}")
            return facts

        except Exception as e:
            if "Syntax error" in str(e) and "()'" not in str(e):
                logger.warning("search_facts skipped invalid semantic query query=%r error=%s", query, e)
            elif "Syntax error" in str(e):
                logger.warning("search_facts skipped invalid semantic query query=%r error=%s", query, e)
            else:
                logger.error(f"search_facts failed: {e}")
            return []

    async def search_nodes(
        self,
        tenant_id: str,
        user_id: str,
        query: str,
        limit: int = 5,
        reference_time: Optional[datetime] = None,
        query_hint: Optional[str] = None,
        search_filter: Optional[Any] = None,
        allowed_types: Optional[List[str]] = None,
        include_internal: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Search for relevant entities (nodes) in Graphiti using semantic query.

        Returns entity summaries representing people, places, concepts
        extracted by Graphiti's LLM. Filters out edges/predicates.
        """
        if not query or not str(query).strip():
            return []
        if not _semantic_query_terms(query):
            logger.info("search_nodes skipped due to empty semantic query terms query=%r", query)
            return []
        if not self._initialized:
            await self.initialize()
        if not self._initialized or self.client is None:
            logger.warning("Graphiti client unavailable; skipping search_nodes")
            return []

        try:
            composite_user_id = self._make_composite_user_id(tenant_id, user_id)
            scoped_driver = self._group_driver(tenant_id, user_id)

            # Prefer node-only search when available
            search_method = getattr(self.client, "search_", None)
            if callable(search_method):
                try:
                    from graphiti_core.search.search_config_recipes import NODE_HYBRID_SEARCH_RRF
                    config = NODE_HYBRID_SEARCH_RRF.model_copy(deep=True)
                    config.limit = limit * 2
                    method_params = inspect.signature(search_method).parameters
                    search_kwargs: Dict[str, Any] = {
                        "query": query,
                        "config": config,
                        "group_ids": [composite_user_id],
                        "search_filter": search_filter,
                    }
                    if reference_time and "reference_time" in method_params:
                        search_kwargs["reference_time"] = reference_time
                    if query_hint:
                        if "query_hint" in method_params:
                            search_kwargs["query_hint"] = query_hint
                        elif "context" in method_params:
                            search_kwargs["context"] = query_hint
                    if scoped_driver is not None and "driver" in method_params:
                        search_kwargs["driver"] = scoped_driver
                    results = await search_method(
                        **search_kwargs
                    )
                    results = getattr(results, "nodes", [])
                except Exception:
                    generic_params = inspect.signature(self.client.search).parameters
                    generic_kwargs: Dict[str, Any] = {
                        "query": query,
                        "group_ids": [composite_user_id],
                        "num_results": limit * 2,
                    }
                    if "rerank" in generic_params:
                        generic_kwargs["rerank"] = True
                    if reference_time and "reference_time" in generic_params:
                        generic_kwargs["reference_time"] = reference_time
                    if query_hint:
                        if "query_hint" in generic_params:
                            generic_kwargs["query_hint"] = query_hint
                        elif "context" in generic_params:
                            generic_kwargs["context"] = query_hint
                    if scoped_driver is not None and "driver" in generic_params:
                        generic_kwargs["driver"] = scoped_driver
                    results = await self.client.search(
                        **generic_kwargs
                    )
            else:
                # Fallback to generic search
                search_fn = self.client.search
                params = inspect.signature(search_fn).parameters
                kwargs: Dict[str, Any] = {
                    "query": query,
                    "group_ids": [composite_user_id],
                    "num_results": limit * 2
                }
                if "rerank" in params:
                    kwargs["rerank"] = True
                if reference_time and "reference_time" in params:
                    kwargs["reference_time"] = reference_time
                if scoped_driver is not None and "driver" in params:
                    kwargs["driver"] = scoped_driver
                if query_hint:
                    if "query_hint" in params:
                        kwargs["query_hint"] = query_hint
                    elif "context" in params:
                        kwargs["context"] = query_hint

                results = await search_fn(**kwargs)

            entities_by_key: Dict[str, Dict[str, Any]] = {}
            for result in results:
                # Skip if this looks like an edge (has source_node_id, target_node_id, or is a predicate)
                if _is_edge_result(result):
                    continue

                entity_data = {
                    "summary": None,
                    "type": None,
                    "uuid": None
                }

                # Extract entity information based on available attributes
                # Prefer entity-specific fields
                if hasattr(result, 'name'):
                    entity_data["summary"] = result.name
                elif hasattr(result, 'summary'):
                    entity_data["summary"] = result.summary
                elif hasattr(result, 'entity_name'):
                    entity_data["summary"] = result.entity_name
                elif hasattr(result, 'content'):
                    entity_data["summary"] = result.content
                else:
                    entity_data["summary"] = str(result)

                # Skip if summary is a predicate-like string
                if _is_predicate_string(entity_data["summary"]):
                    continue

                if hasattr(result, 'entity_type'):
                    entity_data["type"] = result.entity_type
                elif hasattr(result, 'type'):
                    entity_data["type"] = result.type
                elif hasattr(result, 'labels'):
                    labels = [l for l in getattr(result, 'labels', []) if l not in ("Entity", "Episodic", "Community")]
                    if labels:
                        entity_data["type"] = labels[0]

                if hasattr(result, 'uuid'):
                    entity_data["uuid"] = str(result.uuid)
                elif hasattr(result, 'id'):
                    entity_data["uuid"] = str(result.id)

                if hasattr(result, 'attributes'):
                    entity_data["attributes"] = result.attributes
                elif hasattr(result, 'metadata'):
                    entity_data["attributes"] = result.metadata
                if hasattr(result, 'labels'):
                    labels = getattr(result, 'labels', None)
                    if isinstance(labels, list):
                        entity_data["labels"] = labels

                # Best-effort timestamps for recency selection
                if hasattr(result, 'created_at'):
                    entity_data["created_at"] = result.created_at
                if hasattr(result, 'updated_at'):
                    entity_data["updated_at"] = result.updated_at
                if hasattr(result, 'reference_time'):
                    entity_data["reference_time"] = result.reference_time

                canonical_name = canonicalize_entity_name(entity_data.get("summary"))
                original_name = str(entity_data.get("summary") or "").strip()
                entity_data["summary"] = canonical_name or entity_data.get("summary")
                labels = entity_data.get("labels") if isinstance(entity_data.get("labels"), list) else []
                inferred_type = infer_ontology_type(
                    entity_data.get("type"),
                    labels=labels,
                    name=entity_data.get("summary"),
                )
                if inferred_type == "other" and allowed_types and len(allowed_types) == 1:
                    inferred_type = str(allowed_types[0]).strip().lower()
                entity_data["type"] = inferred_type
                entity_data["canonical_name"] = canonical_name or entity_data.get("summary")
                attrs = entity_data.get("attributes") if isinstance(entity_data.get("attributes"), dict) else {}

                allowed, canonical_name, disciplined_type, reason, discipline_score = validate_ontology_node(
                    name=entity_data.get("summary"),
                    raw_type=inferred_type,
                    labels=labels,
                    attributes=attrs,
                    allowed_types=allowed_types or ONTOLOGY_NODE_TYPES,
                    include_internal=include_internal,
                )
                entity_data["summary"] = canonical_name or entity_data.get("summary")
                entity_data["type"] = disciplined_type
                entity_data["discipline_score"] = discipline_score

                if original_name and canonical_name and original_name != canonical_name:
                    logger.info("graphiti node canonicalized from=%s to=%s", original_name, canonical_name)

                if not allowed:
                    logger.info(
                        "graphiti node suppressed name=%s type=%s reason=%s",
                        entity_data.get("summary"),
                        disciplined_type,
                        reason,
                    )
                    continue

                dedupe_key = (
                    str(entity_data.get("canonical_name") or "").strip().lower()
                    or str(entity_data.get("uuid") or "").strip().lower()
                )
                if not dedupe_key:
                    continue

                existing = entities_by_key.get(dedupe_key)
                if existing is None:
                    entities_by_key[dedupe_key] = entity_data
                else:
                    preferred = choose_preferred_node(existing, entity_data)
                    if preferred is entity_data:
                        logger.info(
                            "graphiti node merged canonical_name=%s preferred_type=%s suppressed_type=%s",
                            dedupe_key,
                            entity_data.get("type"),
                            existing.get("type"),
                        )
                        entities_by_key[dedupe_key] = entity_data
                    else:
                        logger.info(
                            "graphiti node suppressed overlapping candidate canonical_name=%s kept_type=%s dropped_type=%s",
                            dedupe_key,
                            existing.get("type"),
                            entity_data.get("type"),
                        )

            entities = list(entities_by_key.values())[:limit]
            logger.info(f"search_nodes returned {len(entities)} entities (filtered from {len(results)} results)")
            return entities

        except Exception as e:
            if "Syntax error" in str(e):
                logger.warning("search_nodes skipped invalid semantic query query=%r error=%s", query, e)
            else:
                logger.error(f"search_nodes failed: {e}")
            return []

    async def get_canonical_entity_nodes(
        self,
        tenant_id: str,
        user_id: str,
        *,
        name: Optional[str] = None,
        entity_id: Optional[str] = None,
        allowed_types: Optional[List[str]] = None,
        include_internal: bool = False,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """Return exact canonical entity matches by UUID or exact name before semantic search."""
        lookup_name = canonicalize_entity_name(name)
        lookup_id = str(entity_id or "").strip()
        if not lookup_name and not lookup_id:
            return []

        if not self._initialized:
            await self.initialize()
        if not self._initialized or self.client is None:
            logger.warning("Graphiti client unavailable; skipping get_canonical_entity_nodes")
            return []

        driver = self._group_driver(tenant_id, user_id)
        if not driver:
            logger.warning("Graphiti driver unavailable; skipping get_canonical_entity_nodes")
            return []

        group_id = self._make_composite_user_id(tenant_id, user_id)
        safe_limit = max(1, min(int(limit or 5), 20))

        conditions: List[str] = ["n.group_id = $group_id"]
        params: Dict[str, Any] = {"group_id": group_id, "limit": safe_limit}
        if lookup_id:
            conditions.append("n.uuid = $entity_id")
            params["entity_id"] = lookup_id
        if lookup_name:
            conditions.append("toLower(coalesce(n.name, '')) = $name_lower")
            params["name_lower"] = lookup_name.lower()

        where_clause = " OR ".join(f"({condition})" for condition in conditions[1:])
        if where_clause:
            where_clause = f"({conditions[0]}) AND ({where_clause})"
        else:
            where_clause = conditions[0]

        try:
            rows = await driver.execute_query(
                f"""
                MATCH (n:Entity)
                WHERE {where_clause}
                RETURN
                    n.uuid AS uuid,
                    n.name AS name,
                    n.summary AS summary,
                    n.group_id AS group_id,
                    n.created_at AS created_at,
                    n.updated_at AS updated_at,
                    properties(n) AS attributes,
                    labels(n) AS labels
                ORDER BY n.updated_at DESC, n.created_at DESC
                LIMIT $limit
                """,
                **params,
            )
            entities_by_key: Dict[str, Dict[str, Any]] = {}
            for row in self._rows(rows):
                if not isinstance(row, dict):
                    continue
                row_dict = dict(row)
                labels = row_dict.get("labels") if isinstance(row_dict.get("labels"), list) else []
                entity_data = {
                    "uuid": row_dict.get("uuid"),
                    "name": row_dict.get("name"),
                    "summary": row_dict.get("summary"),
                    "group_id": row_dict.get("group_id"),
                    "created_at": row_dict.get("created_at"),
                    "updated_at": row_dict.get("updated_at"),
                    "attributes": row_dict.get("attributes") if isinstance(row_dict.get("attributes"), dict) else {},
                    "labels": labels,
                }
                canonical_name = canonicalize_entity_name(entity_data.get("name") or entity_data.get("summary"))
                entity_data["summary"] = entity_data.get("summary") or canonical_name or entity_data.get("name")
                entity_data["canonical_name"] = canonical_name or entity_data.get("name") or entity_data.get("summary")
                inferred_type = infer_ontology_type(
                    entity_data.get("type"),
                    labels=labels,
                    name=entity_data.get("canonical_name"),
                )
                if inferred_type == "other" and allowed_types and len(allowed_types) == 1:
                    inferred_type = str(allowed_types[0]).strip().lower()
                entity_data["type"] = inferred_type
                attrs = entity_data.get("attributes") if isinstance(entity_data.get("attributes"), dict) else {}
                allowed, canonical_name, disciplined_type, reason, discipline_score = validate_ontology_node(
                    name=entity_data.get("canonical_name"),
                    raw_type=inferred_type,
                    labels=labels,
                    attributes=attrs,
                    allowed_types=allowed_types or ONTOLOGY_NODE_TYPES,
                    include_internal=include_internal,
                )
                entity_data["canonical_name"] = canonical_name or entity_data.get("canonical_name")
                entity_data["type"] = disciplined_type
                entity_data["discipline_score"] = discipline_score
                if not allowed:
                    logger.info(
                        "graphiti exact node suppressed name=%s type=%s reason=%s",
                        entity_data.get("canonical_name"),
                        disciplined_type,
                        reason,
                    )
                    continue
                dedupe_key = (
                    str(entity_data.get("canonical_name") or "").strip().lower()
                    or str(entity_data.get("uuid") or "").strip().lower()
                )
                if not dedupe_key:
                    continue
                existing = entities_by_key.get(dedupe_key)
                if existing is None:
                    entities_by_key[dedupe_key] = entity_data
                else:
                    entities_by_key[dedupe_key] = choose_preferred_node(existing, entity_data)
            entities = list(entities_by_key.values())[:safe_limit]
            logger.info(
                "get_canonical_entity_nodes returned %s entities for name=%s entity_id=%s",
                len(entities),
                lookup_name,
                lookup_id,
            )
            return entities
        except Exception as e:
            logger.error("get_canonical_entity_nodes failed: %s", e)
            return []

    async def get_ranked_canonical_nodes(
        self,
        tenant_id: str,
        user_id: str,
        *,
        allowed_types: Optional[List[str]] = None,
        include_internal: bool = False,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Return exact canonical nodes ordered by durability/recency, without semantic search."""
        if not self._initialized:
            await self.initialize()
        if not self._initialized or self.client is None:
            logger.warning("Graphiti client unavailable; skipping get_ranked_canonical_nodes")
            return []

        driver = self._group_driver(tenant_id, user_id)
        if not driver:
            logger.warning("Graphiti driver unavailable; skipping get_ranked_canonical_nodes")
            return []

        group_id = self._make_composite_user_id(tenant_id, user_id)
        safe_limit = max(1, min(int(limit or 20), 50))
        try:
            rows = await driver.execute_query(
                """
                MATCH (n:Entity {group_id: $group_id})
                RETURN
                    n.uuid AS uuid,
                    n.name AS name,
                    n.summary AS summary,
                    n.group_id AS group_id,
                    n.created_at AS created_at,
                    n.updated_at AS updated_at,
                    properties(n) AS attributes,
                    labels(n) AS labels
                ORDER BY coalesce(n.updated_at, n.created_at) DESC
                LIMIT $limit
                """,
                group_id=group_id,
                limit=safe_limit,
            )
            entities: List[Dict[str, Any]] = []
            for row in rows or []:
                if not isinstance(row, dict):
                    continue
                labels = row.get("labels") if isinstance(row.get("labels"), list) else []
                attrs = row.get("attributes") if isinstance(row.get("attributes"), dict) else {}
                canonical_name = canonicalize_entity_name(row.get("name") or row.get("summary"))
                inferred_type = infer_ontology_type(None, labels=labels, name=canonical_name)
                allowed, disciplined_name, disciplined_type, _reason, discipline_score = validate_ontology_node(
                    name=canonical_name,
                    raw_type=inferred_type,
                    labels=labels,
                    attributes=attrs,
                    allowed_types=allowed_types or ONTOLOGY_NODE_TYPES,
                    include_internal=include_internal,
                )
                if not allowed:
                    continue
                entities.append(
                    {
                        "uuid": row.get("uuid"),
                        "name": row.get("name"),
                        "summary": row.get("summary") or disciplined_name,
                        "canonical_name": disciplined_name,
                        "type": disciplined_type,
                        "group_id": row.get("group_id"),
                        "created_at": row.get("created_at"),
                        "updated_at": row.get("updated_at"),
                        "attributes": attrs,
                        "labels": labels,
                        "discipline_score": discipline_score,
                    }
                )
            logger.info("get_ranked_canonical_nodes returned %s entities", len(entities))
            return entities
        except Exception as e:
            logger.error("get_ranked_canonical_nodes failed: %s", e)
            return []

    async def get_exact_named_entity_nodes(
        self,
        tenant_id: str,
        user_id: str,
        *,
        names: List[str],
        allowed_types: Optional[List[str]] = None,
        include_internal: bool = False,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Return raw exact-name matches for a cluster of candidate aliases."""
        clean_names = []
        seen = set()
        for name in names or []:
            canonical = canonicalize_entity_name(name)
            lower = canonical.lower()
            if not canonical or lower in seen:
                continue
            seen.add(lower)
            clean_names.append(lower)
        if not clean_names:
            return []

        if not self._initialized:
            await self.initialize()
        if not self._initialized or self.client is None:
            logger.warning("Graphiti client unavailable; skipping get_exact_named_entity_nodes")
            return []

        driver = self._group_driver(tenant_id, user_id)
        if not driver:
            logger.warning("Graphiti driver unavailable; skipping get_exact_named_entity_nodes")
            return []

        group_id = self._make_composite_user_id(tenant_id, user_id)
        safe_limit = max(1, min(int(limit or 50), 100))
        try:
            rows = await driver.execute_query(
                """
                UNWIND $names AS needle
                MATCH (n)
                WHERE n.group_id = $group_id
                  AND toLower(coalesce(n.name, '')) = needle
                RETURN
                    n.uuid AS uuid,
                    n.name AS name,
                    n.summary AS summary,
                    n.group_id AS group_id,
                    n.created_at AS created_at,
                    n.updated_at AS updated_at,
                    properties(n) AS attributes,
                    labels(n) AS labels
                ORDER BY coalesce(n.updated_at, n.created_at) DESC
                LIMIT $limit
                """,
                group_id=group_id,
                names=clean_names,
                limit=safe_limit,
            )
        except Exception as e:
            logger.error("get_exact_named_entity_nodes failed: %s", e)
            return []

        out: List[Dict[str, Any]] = []
        for row in self._rows(rows):
            if not isinstance(row, dict):
                continue
            labels = row.get("labels") if isinstance(row.get("labels"), list) else []
            attrs = row.get("attributes") if isinstance(row.get("attributes"), dict) else {}
            canonical_name = canonicalize_entity_name(row.get("name") or row.get("summary"))
            inferred_type = infer_ontology_type(None, labels=labels, name=canonical_name)
            if inferred_type == "other" and allowed_types and len(allowed_types) == 1:
                inferred_type = str(allowed_types[0]).strip().lower()
            allowed, disciplined_name, disciplined_type, _reason, discipline_score = validate_ontology_node(
                name=canonical_name,
                raw_type=inferred_type,
                labels=labels,
                attributes=attrs,
                allowed_types=allowed_types or ONTOLOGY_NODE_TYPES,
                include_internal=include_internal,
            )
            if not allowed:
                continue
            out.append(
                {
                    "uuid": row.get("uuid"),
                    "name": row.get("name"),
                    "summary": row.get("summary") or disciplined_name,
                    "canonical_name": disciplined_name,
                    "type": disciplined_type,
                    "group_id": row.get("group_id"),
                    "created_at": row.get("created_at"),
                    "updated_at": row.get("updated_at"),
                    "attributes": attrs,
                    "labels": labels,
                    "discipline_score": discipline_score,
                }
            )
        logger.info("get_exact_named_entity_nodes returned %s entities for names=%s", len(out), clean_names)
        return out

    @staticmethod
    def _parse_dt_like(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=dt_timezone.utc)
        text = str(value or "").strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt_timezone.utc)
        except Exception:
            return datetime.now(dt_timezone.utc)

    async def merge_canonical_entity_cluster(
        self,
        tenant_id: str,
        user_id: str,
        *,
        canonical_name: str,
        node_type: str,
        candidate_names: List[str],
        summary: str,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Merge duplicate exact-name nodes into one durable canonical node with edge rewiring."""
        canonical = canonicalize_entity_name(canonical_name)
        disciplined_type = str(node_type or "").strip().lower()
        cluster_names = [canonical, *candidate_names]
        cluster_nodes = await self.get_exact_named_entity_nodes(
            tenant_id=tenant_id,
            user_id=user_id,
            names=cluster_names,
            allowed_types=[disciplined_type],
            limit=100,
        )

        if not cluster_nodes:
            return await self.upsert_canonical_entity_node(
                tenant_id=tenant_id,
                user_id=user_id,
                name=canonical,
                node_type=disciplined_type,
                summary=summary,
                attributes=attributes,
            )

        def _node_sort_key(node: Dict[str, Any]) -> Tuple[int, float, float]:
            exact = 1 if str(node.get("name") or "").strip().lower() == canonical.lower() else 0
            updated = self._parse_dt_like(node.get("updated_at") or node.get("created_at")).timestamp()
            score = float(node.get("discipline_score") or 0.0)
            return (exact, score, updated)

        survivor = sorted(cluster_nodes, key=_node_sort_key, reverse=True)[0]
        survivor_uuid = str(survivor.get("uuid") or "").strip()
        if not survivor_uuid:
            return None

        if not self._initialized:
            await self.initialize()
        if not self._initialized or self.client is None:
            logger.warning("Graphiti client unavailable; skipping merge_canonical_entity_cluster")
            return survivor

        driver = self._group_driver(tenant_id, user_id)
        if not driver:
            logger.warning("Graphiti driver unavailable; skipping merge_canonical_entity_cluster")
            return survivor

        group_id = self._make_composite_user_id(tenant_id, user_id)
        merged_attrs = dict(survivor.get("attributes") if isinstance(survivor.get("attributes"), dict) else {})
        merged_attrs.update(attributes or {})
        merged_attrs.setdefault("canonical_name", canonical)
        merged_attrs.setdefault("profile_summary", summary)
        merged_attrs.setdefault("aliases", [])
        alias_bucket = []
        seen_alias = set()
        for node in cluster_nodes:
            for value in [
                str(node.get("name") or "").strip(),
                str(node.get("canonical_name") or "").strip(),
                *(
                    node.get("attributes", {}).get("aliases", [])
                    if isinstance(node.get("attributes"), dict) and isinstance(node.get("attributes", {}).get("aliases"), list)
                    else []
                ),
            ]:
                alias = str(value or "").strip()
                if not alias or alias.lower() == canonical.lower() or alias.lower() in seen_alias:
                    continue
                seen_alias.add(alias.lower())
                alias_bucket.append(alias)
        merged_attrs["aliases"] = alias_bucket[:12]

        await driver.execute_query(
            """
            MATCH (n {group_id: $group_id, uuid: $uuid})
            SET n.name = $name,
                n.summary = $summary,
                n.updated_at = $updated_at,
                n += $attributes
            RETURN n.uuid AS uuid
            """,
            group_id=group_id,
            uuid=survivor_uuid,
            name=canonical,
            summary=str(summary or canonical).strip(),
            updated_at=datetime.now(dt_timezone.utc).isoformat(),
            attributes=merged_attrs,
        )

        duplicates = [node for node in cluster_nodes if str(node.get("uuid") or "").strip() and str(node.get("uuid") or "").strip() != survivor_uuid]
        for duplicate in duplicates:
            duplicate_uuid = str(duplicate.get("uuid") or "").strip()
            edge_rows = await driver.execute_query(
                """
                MATCH (s)-[e]->(t)
                WHERE s.group_id = $group_id
                  AND t.group_id = $group_id
                  AND (s.uuid = $duplicate_uuid OR t.uuid = $duplicate_uuid)
                RETURN
                    s.uuid AS source_uuid,
                    t.uuid AS target_uuid,
                    e.name AS edge_name,
                    e.fact AS fact,
                    properties(e) AS attributes,
                    e.created_at AS created_at
                """,
                group_id=group_id,
                duplicate_uuid=duplicate_uuid,
            )
            for row in self._rows(edge_rows):
                if not isinstance(row, dict):
                    continue
                source_uuid = survivor_uuid if str(row.get("source_uuid") or "").strip() == duplicate_uuid else str(row.get("source_uuid") or "").strip()
                target_uuid = survivor_uuid if str(row.get("target_uuid") or "").strip() == duplicate_uuid else str(row.get("target_uuid") or "").strip()
                if not source_uuid or not target_uuid or source_uuid == target_uuid:
                    continue
                edge_name = str(row.get("edge_name") or "").strip()
                fact = str(row.get("fact") or "").strip()
                edge_attrs = row.get("attributes") if isinstance(row.get("attributes"), dict) else {}
                existing = await driver.execute_query(
                    """
                    MATCH (s {group_id: $group_id, uuid: $source_uuid})-[e {group_id: $group_id, name: $edge_name}]->(t {group_id: $group_id, uuid: $target_uuid})
                    WHERE coalesce(e.fact, '') = $fact
                    RETURN e.uuid AS uuid
                    LIMIT 1
                    """,
                    group_id=group_id,
                    source_uuid=source_uuid,
                    target_uuid=target_uuid,
                    edge_name=edge_name,
                    fact=fact,
                )
                if self._rows(existing):
                    continue
                edge = EntityEdge(
                    group_id=group_id,
                    source_node_uuid=source_uuid,
                    target_node_uuid=target_uuid,
                    created_at=self._parse_dt_like(row.get("created_at")),
                    name=edge_name,
                    fact=fact,
                    attributes=dict(edge_attrs),
                )
                saved = edge.save(driver)
                if inspect.isawaitable(saved):
                    await saved
            await driver.execute_query(
                """
                MATCH (n {group_id: $group_id, uuid: $duplicate_uuid})
                DETACH DELETE n
                """,
                group_id=group_id,
                duplicate_uuid=duplicate_uuid,
            )
            logger.info("merged duplicate node duplicate_uuid=%s survivor_uuid=%s canonical=%s", duplicate_uuid, survivor_uuid, canonical)

        merged = await self.get_canonical_entity_nodes(
            tenant_id=tenant_id,
            user_id=user_id,
            entity_id=survivor_uuid,
            name=canonical,
            allowed_types=[disciplined_type],
            limit=1,
        )
        return merged[0] if merged else survivor

    async def upsert_canonical_entity_node(
        self,
        tenant_id: str,
        user_id: str,
        *,
        name: str,
        node_type: str,
        summary: str,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Create a canonical entity node if missing, otherwise return the existing exact node."""
        canonical_name = canonicalize_entity_name(name)
        disciplined_type = str(node_type or "").strip().lower()
        if not canonical_name or disciplined_type not in ONTOLOGY_NODE_TYPES:
            return None

        existing = await self.get_canonical_entity_nodes(
            tenant_id=tenant_id,
            user_id=user_id,
            name=canonical_name,
            allowed_types=[disciplined_type],
            limit=1,
        )
        if existing:
            existing_node = existing[0]
            existing_uuid = str(existing_node.get("uuid") or "").strip()
            if existing_uuid:
                try:
                    driver = self._group_driver(tenant_id, user_id)
                    if driver:
                        merged_attrs = dict(existing_node.get("attributes") if isinstance(existing_node.get("attributes"), dict) else {})
                        merged_attrs.update(attributes or {})
                        merged_attrs.setdefault("profile_summary", summary)
                        await driver.execute_query(
                            """
                            MATCH (n:Entity {group_id: $group_id, uuid: $uuid})
                            SET n.summary = $summary,
                                n.updated_at = $updated_at,
                                n += $attributes
                            RETURN n.uuid AS uuid
                            """,
                            group_id=self._make_composite_user_id(tenant_id, user_id),
                            uuid=existing_uuid,
                            summary=str(summary or canonical_name).strip(),
                            updated_at=datetime.utcnow().isoformat(),
                            attributes=merged_attrs,
                        )
                        existing_node["summary"] = str(summary or canonical_name).strip()
                        existing_node["attributes"] = merged_attrs
                except Exception as e:
                    logger.warning("Failed to update canonical entity node name=%s error=%s", canonical_name, e)
            return existing_node

        if not self._initialized:
            await self.initialize()
        if not self._initialized or self.client is None:
            logger.warning("Graphiti client unavailable; skipping upsert_canonical_entity_node")
            return None

        driver = self._group_driver(tenant_id, user_id)
        if not driver:
            logger.warning("Graphiti driver unavailable; skipping upsert_canonical_entity_node")
            return None

        entity_attrs = dict(attributes or {})
        entity_attrs.setdefault("profile_summary", summary)
        node = EntityNode(
            name=canonical_name,
            group_id=self._make_composite_user_id(tenant_id, user_id),
            labels=[disciplined_type.title()],
            summary=str(summary or canonical_name).strip(),
            attributes=entity_attrs,
        )
        try:
            saved = node.save(driver)
            if inspect.isawaitable(saved):
                await saved
            logger.info(
                "canonical entity node saved name=%s type=%s uuid=%s",
                canonical_name,
                disciplined_type,
                node.uuid,
            )
            return {
                "uuid": str(node.uuid),
                "summary": canonical_name,
                "canonical_name": canonical_name,
                "type": disciplined_type,
                "labels": ["Entity", disciplined_type.title()],
                "attributes": entity_attrs,
                "created_at": getattr(node, "created_at", None),
                "updated_at": getattr(node, "created_at", None),
            }
        except Exception as e:
            logger.error("Failed to save canonical entity node name=%s error=%s", canonical_name, e)
            return None

    async def upsert_canonical_fact_edge(
        self,
        tenant_id: str,
        user_id: str,
        *,
        source_name: str,
        source_type: str,
        target_name: str,
        target_type: str,
        edge_name: str,
        fact: str,
        reference_time: datetime,
        source_summary: Optional[str] = None,
        target_summary: Optional[str] = None,
        source_attributes: Optional[Dict[str, Any]] = None,
        target_attributes: Optional[Dict[str, Any]] = None,
        edge_attributes: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Ensure a canonical grounding edge exists between two canonical nodes."""
        source_node = await self.upsert_canonical_entity_node(
            tenant_id=tenant_id,
            user_id=user_id,
            name=source_name,
            node_type=source_type,
            summary=source_summary or source_name,
            attributes=source_attributes,
        )
        target_node = await self.upsert_canonical_entity_node(
            tenant_id=tenant_id,
            user_id=user_id,
            name=target_name,
            node_type=target_type,
            summary=target_summary or target_name,
            attributes=target_attributes,
        )
        if not source_node or not target_node:
            return False

        if not self._initialized:
            await self.initialize()
        if not self._initialized or self.client is None:
            logger.warning("Graphiti client unavailable; skipping upsert_canonical_fact_edge")
            return False

        driver = self._group_driver(tenant_id, user_id)
        if not driver:
            logger.warning("Graphiti driver unavailable; skipping upsert_canonical_fact_edge")
            return False

        group_id = self._make_composite_user_id(tenant_id, user_id)
        edge_type = str(edge_name or "").strip().upper()
        fact_text = str(fact or "").strip()
        if not edge_type or not fact_text:
            return False

        try:
            existing_rows = await driver.execute_query(
                """
                MATCH (s:Entity {group_id: $group_id, uuid: $source_uuid})-[e {group_id: $group_id, name: $edge_name}]->(t:Entity {group_id: $group_id, uuid: $target_uuid})
                WHERE e.fact = $fact
                RETURN e.uuid AS uuid
                LIMIT 1
                """,
                group_id=group_id,
                source_uuid=str(source_node.get("uuid") or ""),
                target_uuid=str(target_node.get("uuid") or ""),
                edge_name=edge_type,
                fact=fact_text,
            )
            existing_rows = self._rows(existing_rows)
            if existing_rows:
                existing_uuid = None
                if isinstance(existing_rows[0], dict):
                    existing_uuid = str(existing_rows[0].get("uuid") or "").strip()
                if existing_uuid:
                    try:
                        await driver.execute_query(
                            """
                            MATCH (s:Entity {group_id: $group_id, uuid: $source_uuid})-[e {group_id: $group_id, uuid: $edge_uuid}]->(t:Entity {group_id: $group_id, uuid: $target_uuid})
                            SET e += $attributes
                            RETURN e.uuid AS uuid
                            """,
                            group_id=group_id,
                            source_uuid=str(source_node.get("uuid") or ""),
                            target_uuid=str(target_node.get("uuid") or ""),
                            edge_uuid=existing_uuid,
                            attributes=dict(edge_attributes or {}),
                        )
                    except Exception as e:
                        logger.warning(
                            "canonical fact edge update failed source=%s target=%s edge=%s error=%s",
                            source_name,
                            target_name,
                            edge_type,
                            e,
                        )
                logger.info(
                    "canonical fact edge already exists source=%s target=%s edge=%s",
                    source_name,
                    target_name,
                    edge_type,
                )
                return True
        except Exception as e:
            logger.warning(
                "canonical fact edge existence check failed source=%s target=%s edge=%s error=%s",
                source_name,
                target_name,
                edge_type,
                e,
            )

        edge = EntityEdge(
            group_id=group_id,
            source_node_uuid=str(source_node.get("uuid") or ""),
            target_node_uuid=str(target_node.get("uuid") or ""),
            created_at=reference_time,
            name=edge_type,
            fact=fact_text,
            attributes=dict(edge_attributes or {}),
        )
        try:
            saved = edge.save(driver)
            if inspect.isawaitable(saved):
                await saved
            logger.info(
                "canonical fact edge saved source=%s target=%s edge=%s",
                source_name,
                target_name,
                edge_type,
            )
            return True
        except Exception as e:
            logger.error(
                "Failed to save canonical fact edge source=%s target=%s edge=%s error=%s",
                source_name,
                target_name,
                edge_type,
                e,
            )
            return False

    async def get_entity_facts_exact(
        self,
        tenant_id: str,
        user_id: str,
        *,
        name: Optional[str] = None,
        entity_id: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Return exact edge facts touching a canonical entity node before semantic fact search."""
        node_ids = []
        if entity_id:
            node_ids.append(str(entity_id).strip())
        elif name:
            exact_nodes = await self.get_exact_named_entity_nodes(
                tenant_id=tenant_id,
                user_id=user_id,
                names=[name],
                allowed_types=ONTOLOGY_NODE_TYPES,
                limit=50,
            )
            node_ids.extend([str(node.get("uuid") or "").strip() for node in exact_nodes if str(node.get("uuid") or "").strip()])
        else:
            nodes = await self.get_canonical_entity_nodes(
                tenant_id=tenant_id,
                user_id=user_id,
                name=name,
                entity_id=None,
                limit=5,
                allowed_types=ONTOLOGY_NODE_TYPES,
            )
            node_ids.extend([str(node.get("uuid") or "").strip() for node in nodes if str(node.get("uuid") or "").strip()])
        node_ids = [x for i, x in enumerate(node_ids) if x and x not in node_ids[:i]]
        if not node_ids:
            return []

        if not self._initialized:
            await self.initialize()
        if not self._initialized or self.client is None:
            logger.warning("Graphiti client unavailable; skipping get_entity_facts_exact")
            return []

        driver = self._group_driver(tenant_id, user_id)
        if not driver:
            logger.warning("Graphiti driver unavailable; skipping get_entity_facts_exact")
            return []

        group_id = self._make_composite_user_id(tenant_id, user_id)
        safe_limit = max(1, min(int(limit or 10), 50))
        try:
            rows = await driver.execute_query(
                """
                MATCH (s:Entity {group_id: $group_id})-[e]->(t:Entity {group_id: $group_id})
                WHERE (s.uuid IN $node_ids OR t.uuid IN $node_ids)
                  AND e.fact IS NOT NULL
                RETURN e.fact AS fact, e.valid_at AS valid_at, e.invalid_at AS invalid_at, e.created_at AS created_at
                ORDER BY e.created_at DESC
                LIMIT $limit
                """,
                group_id=group_id,
                node_ids=node_ids,
                limit=safe_limit,
            )
            facts: List[Dict[str, Any]] = []
            seen = set()
            for row in self._rows(rows):
                if not isinstance(row, dict):
                    continue
                fact_text = str(row.get("fact") or "").strip()
                if not fact_text:
                    continue
                fact_key = fact_text.lower()
                if fact_key in seen:
                    continue
                seen.add(fact_key)
                facts.append(
                    {
                        "text": fact_text,
                        "relevance": 1.0,
                        "source": "graphiti_exact",
                        "valid_at": row.get("valid_at"),
                        "invalid_at": row.get("invalid_at"),
                        "created_at": row.get("created_at"),
                    }
                )
            logger.info(
                "get_entity_facts_exact returned %s facts for name=%s entity_id=%s",
                len(facts),
                name,
                entity_id,
            )
            return facts
        except Exception as e:
            logger.error("get_entity_facts_exact failed: %s", e)
            return []

    async def get_entity_role_grounding(
        self,
        tenant_id: str,
        user_id: str,
        *,
        name: Optional[str] = None,
        entity_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return exact graph-readable role grounding for a canonical entity."""
        node_ids = []
        if entity_id:
            node_ids.append(str(entity_id).strip())
        elif name:
            exact_nodes = await self.get_exact_named_entity_nodes(
                tenant_id=tenant_id,
                user_id=user_id,
                names=[name],
                allowed_types=ONTOLOGY_NODE_TYPES,
                limit=50,
            )
            node_ids.extend([str(node.get("uuid") or "").strip() for node in exact_nodes if str(node.get("uuid") or "").strip()])
        else:
            nodes = await self.get_canonical_entity_nodes(
                tenant_id=tenant_id,
                user_id=user_id,
                name=name,
                entity_id=None,
                limit=5,
                allowed_types=ONTOLOGY_NODE_TYPES,
            )
            node_ids.extend([str(node.get("uuid") or "").strip() for node in nodes if str(node.get("uuid") or "").strip()])
        node_ids = [x for i, x in enumerate(node_ids) if x and x not in node_ids[:i]]
        if not node_ids:
            return None

        if not self._initialized:
            await self.initialize()
        if not self._initialized or self.client is None:
            return None
        driver = self._group_driver(tenant_id, user_id)
        if not driver:
            return None
        group_id = self._make_composite_user_id(tenant_id, user_id)
        try:
            rows = await driver.execute_query(
                """
                MATCH (s)-[e]->(t)
                WHERE s.group_id = $group_id
                  AND t.group_id = $group_id
                  AND (s.uuid IN $node_ids OR t.uuid IN $node_ids)
                  AND e.name IN ['RELATED_TO_USER_AS', 'WORKING_ON', 'ABOUT']
                RETURN
                    s.uuid AS source_uuid,
                    coalesce(s.name, '') AS source_name,
                    t.uuid AS target_uuid,
                    coalesce(t.name, '') AS target_name,
                    e.name AS edge_name,
                    coalesce(e.fact, '') AS fact,
                    properties(e) AS attributes,
                    e.created_at AS created_at
                ORDER BY coalesce(e.created_at, '') DESC
                LIMIT 20
                """,
                group_id=group_id,
                node_ids=node_ids,
            )
        except Exception as e:
            logger.error("get_entity_role_grounding failed: %s", e)
            return None

        for row in self._rows(rows):
            if not isinstance(row, dict):
                continue
            attrs = row.get("attributes") if isinstance(row.get("attributes"), dict) else {}
            edge_name = str(row.get("edge_name") or "").strip().upper()
            source_name = canonicalize_entity_name(row.get("source_name"))
            target_name = canonicalize_entity_name(row.get("target_name"))
            role = str(attrs.get("role_display") or attrs.get("role") or "").strip()
            importance = attrs.get("importance")
            if edge_name == "RELATED_TO_USER_AS":
                if target_name == canonicalize_entity_name(name or target_name) or str(row.get("target_uuid") or "") in node_ids:
                    return {
                        "role": role or None,
                        "relationship": role or None,
                        "edge_name": edge_name,
                        "source_name": source_name,
                        "target_name": target_name,
                        "importance": importance,
                    }
            if edge_name == "WORKING_ON":
                if target_name == canonicalize_entity_name(name or target_name) or str(row.get("target_uuid") or "") in node_ids:
                    return {
                        "role": role or "active_project",
                        "relationship": role or "active_project",
                        "edge_name": edge_name,
                        "source_name": source_name,
                        "target_name": target_name,
                        "importance": importance,
                    }
            if edge_name == "ABOUT":
                if target_name == canonicalize_entity_name(name or target_name) or str(row.get("target_uuid") or "") in node_ids:
                    return {
                        "role": role or None,
                        "relationship": role or None,
                        "edge_name": edge_name,
                        "source_name": source_name,
                        "target_name": target_name,
                        "importance": importance,
                    }
            if edge_name == "ABOUT":
                if target_name == canonicalize_entity_name(name or target_name) or str(row.get("target_uuid") or "") in node_ids:
                    return {
                        "role": role or None,
                        "relationship": role or None,
                        "edge_name": edge_name,
                        "source_name": source_name,
                        "target_name": target_name,
                        "importance": importance,
                    }
        return None

    async def inspect_exact_entity_cluster(
        self,
        tenant_id: str,
        user_id: str,
        *,
        canonical_name: str,
        candidate_names: Optional[List[str]] = None,
        allowed_types: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        canonical = canonicalize_entity_name(canonical_name)
        nodes = await self.get_exact_named_entity_nodes(
            tenant_id=tenant_id,
            user_id=user_id,
            names=[canonical, *(candidate_names or [])],
            allowed_types=allowed_types or ONTOLOGY_NODE_TYPES,
            limit=100,
        )
        survivor = None
        if nodes:
            survivor = sorted(
                nodes,
                key=lambda node: (
                    1 if str(node.get("name") or "").strip().lower() == canonical.lower() else 0,
                    float(node.get("discipline_score") or 0.0),
                    self._parse_dt_like(node.get("updated_at") or node.get("created_at")).timestamp(),
                ),
                reverse=True,
            )[0]
        return {
            "canonical_name": canonical,
            "node_count": len(nodes),
            "duplicate_count": max(0, len(nodes) - 1),
            "survivor_uuid": str((survivor or {}).get("uuid") or "").strip() or None,
            "survivor_summary": str((survivor or {}).get("summary") or "").strip() or None,
            "survivor_aliases": (
                list(((survivor or {}).get("attributes") or {}).get("aliases") or [])
                if isinstance((survivor or {}).get("attributes"), dict)
                else []
            ),
            "nodes": [
                {
                    "uuid": str(node.get("uuid") or "").strip() or None,
                    "name": str(node.get("name") or "").strip() or None,
                    "type": str(node.get("type") or "").strip() or None,
                    "summary": str(node.get("summary") or "").strip() or None,
                    "labels": list(node.get("labels") or []),
                    "aliases": list(((node.get("attributes") or {}).get("aliases") or []))
                    if isinstance(node.get("attributes"), dict)
                    else [],
                    "created_at": node.get("created_at"),
                    "updated_at": node.get("updated_at"),
                }
                for node in nodes
            ],
        }

    async def inspect_exact_role_edges(
        self,
        tenant_id: str,
        user_id: str,
        *,
        entity_name: Optional[str] = None,
        entity_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        node_ids: List[str] = []
        if entity_id:
            node_ids.append(str(entity_id).strip())
        elif entity_name:
            nodes = await self.get_exact_named_entity_nodes(
                tenant_id=tenant_id,
                user_id=user_id,
                names=[entity_name],
                allowed_types=ONTOLOGY_NODE_TYPES,
                limit=100,
            )
            node_ids.extend([str(node.get("uuid") or "").strip() for node in nodes if str(node.get("uuid") or "").strip()])
        node_ids = [x for i, x in enumerate(node_ids) if x and x not in node_ids[:i]]
        if not node_ids:
            return []

        if not self._initialized:
            await self.initialize()
        if not self._initialized or self.client is None:
            return []
        driver = self._group_driver(tenant_id, user_id)
        if not driver:
            return []
        group_id = self._make_composite_user_id(tenant_id, user_id)
        rows = await driver.execute_query(
            """
            MATCH (s)-[e]->(t)
            WHERE s.group_id = $group_id
              AND t.group_id = $group_id
              AND (s.uuid IN $node_ids OR t.uuid IN $node_ids)
            RETURN
                type(e) AS rel_type,
                e.name AS edge_name,
                e.uuid AS edge_uuid,
                e.fact AS fact,
                properties(e) AS attributes,
                s.name AS source_name,
                s.uuid AS source_uuid,
                t.name AS target_name,
                t.uuid AS target_uuid
            ORDER BY coalesce(e.created_at, '') DESC
            LIMIT 100
            """,
            group_id=group_id,
            node_ids=node_ids,
        )
        out: List[Dict[str, Any]] = []
        for row in self._rows(rows):
            if not isinstance(row, dict):
                continue
            out.append(
                {
                    "rel_type": row.get("rel_type"),
                    "edge_name": row.get("edge_name"),
                    "edge_uuid": row.get("edge_uuid"),
                    "fact": row.get("fact"),
                    "attributes": row.get("attributes") if isinstance(row.get("attributes"), dict) else {},
                    "source_name": row.get("source_name"),
                    "source_uuid": row.get("source_uuid"),
                    "target_name": row.get("target_name"),
                    "target_uuid": row.get("target_uuid"),
                }
            )
        return out

    def _extract_episode_summary(self, episode: Any) -> Dict[str, Any]:
        if isinstance(episode, dict):
            summary = (
                episode.get("summary")
                or episode.get("episode_summary")
            )
            reference_time = episode.get("reference_time") or episode.get("created_at")
            edge_ids = episode.get("entity_edges") or []
        else:
            summary = (
                getattr(episode, "summary", None)
                or getattr(episode, "episode_summary", None)
            )
            reference_time = getattr(episode, "reference_time", None) or getattr(episode, "created_at", None)
            edge_ids = getattr(episode, "entity_edges", None) or []

        return {
            "summary": summary,
            "reference_time": reference_time,
            "edge_ids": edge_ids
        }

    async def search(
        self,
        tenant_id: str,
        user_id: str,
        query: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Legacy method - delegates to search_facts for backward compatibility"""
        return await self.search_facts(tenant_id, user_id, query, limit)

    async def get_recent_episodes(
        self,
        tenant_id: str,
        user_id: str,
        since: Optional[datetime],
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get recent episodes since a timestamp"""
        if not self._initialized:
            await self.initialize()
        if not self._initialized or self.client is None:
            logger.warning("Graphiti client unavailable; skipping get_recent_episodes")
            return []

        try:
            composite_user_id = self._make_composite_user_id(tenant_id, user_id)
            getter = getattr(self.client, "get_recent_episodes", None)
            if callable(getter):
                kwargs: Dict[str, Any] = {}
                params = inspect.signature(getter).parameters
                if "group_id" in params:
                    kwargs["group_id"] = composite_user_id
                elif "group_ids" in params:
                    kwargs["group_ids"] = [composite_user_id]
                if "since" in params:
                    kwargs["since"] = since
                elif "start_time" in params and since is not None:
                    kwargs["start_time"] = since
                if "limit" in params:
                    kwargs["limit"] = limit
                elif "num_results" in params:
                    kwargs["num_results"] = limit

                result = getter(**kwargs)
                episodes = await result if inspect.isawaitable(result) else result
                episodes = episodes or []
                logger.info(f"Retrieved {len(episodes)} recent episodes for {composite_user_id}")
                return episodes

            driver = self._group_driver(tenant_id, user_id)
            if driver:
                try:
                    rows = await driver.execute_query(
                        """
                        MATCH (e:Episodic)
                        RETURN properties(e) AS props
                        ORDER BY e.created_at DESC
                        LIMIT $limit
                        """,
                        limit=limit,
                    )
                    episodes: List[Dict[str, Any]] = []
                    for row in rows or []:
                        if isinstance(row, dict):
                            props = row.get("props")
                            if isinstance(props, dict):
                                episodes.append(props)
                    if episodes:
                        logger.info(f"Retrieved {len(episodes)} recent episodes for {composite_user_id} via scoped query")
                        return episodes
                except Exception as e:
                    logger.warning(
                        "Scoped episodic query failed tenant=%s user=%s err=%s; falling back to EpisodicNode.get_by_group_ids",
                        tenant_id,
                        user_id,
                        e,
                    )

                episodes = await EpisodicNode.get_by_group_ids(
                    driver=driver,
                    group_ids=[composite_user_id],
                    limit=limit
                )
                episodes = episodes or []
                episodes.sort(key=lambda e: getattr(e, "created_at", None) or datetime.min, reverse=True)
                return episodes[:limit]

            logger.info(f"Graphiti client has no get_recent_episodes; returning [] for {composite_user_id}")
            return []

        except Exception as e:
            logger.error(f"Failed to get recent episodes: {e}")
            return []

    async def get_latest_session_summary(
        self,
        tenant_id: str,
        user_id: str,
        limit: int = 20
    ) -> Optional[str]:
        """Return latest session_summary episode text if present."""
        episodes = await self.get_recent_episodes(tenant_id, user_id, since=None, limit=limit)
        if not episodes:
            return None

        for episode in episodes:
            name = None
            if isinstance(episode, dict):
                name = episode.get("name") or episode.get("episode_name")
                text = episode.get("text") or episode.get("episode_body") or episode.get("content")
            else:
                name = getattr(episode, "name", None)
                text = getattr(episode, "episode_body", None) or getattr(episode, "content", None)

            if name and name.startswith("session_summary_"):
                return text

        return None

    async def get_recent_episode_summaries(
        self,
        tenant_id: str,
        user_id: str,
        limit: int = 3
    ) -> List[Dict[str, Any]]:
        episodes = await self.get_recent_episodes(tenant_id, user_id, since=None, limit=limit)
        results: List[Dict[str, Any]] = []
        driver = getattr(self.client, "driver", None) if self.client else None
        for episode in episodes:
            extracted = self._extract_episode_summary(episode)
            if driver:
                edge_ids = extracted.get("edge_ids") or []
                if edge_ids:
                    edges = await EntityEdge.get_by_uuids(driver=driver, uuids=edge_ids)
                    facts = [e.fact for e in edges if getattr(e, "fact", None)]
                    if facts:
                        extracted["summary"] = "; ".join(facts[:3])
            if extracted.get("summary"):
                results.append({
                    "summary": extracted.get("summary"),
                    "reference_time": extracted.get("reference_time")
                })
        return results

    async def get_latest_session_summary_node(
        self,
        tenant_id: str,
        user_id: str
    ) -> Optional[Dict[str, Any]]:
        """Return latest SessionSummary node for a user group."""
        if not self._initialized:
            await self.initialize()
        if not self._initialized or self.client is None:
            logger.warning("Graphiti client unavailable; skipping get_latest_session_summary_node")
            return None

        driver = self._group_driver(tenant_id, user_id)
        if not driver:
            logger.warning("Graphiti driver unavailable; skipping get_latest_session_summary_node")
            return None

        composite_user_id = self._make_composite_user_id(tenant_id, user_id)
        try:
            rows = await driver.execute_query(
                """
                MATCH (n:SessionSummary {group_id: $group_id})
                RETURN properties(n) AS props
                ORDER BY n.created_at DESC
                LIMIT 1
                """,
                group_id=composite_user_id
            )
            if not rows:
                return None
            for row in rows or []:
                for node in extract_node_dicts(row, required_keys=("uuid",)):
                    normalized = self._normalize_session_summary_props(node)
                    if normalized:
                        return normalized
            node = pick_first_node(rows)
            if isinstance(node, dict):
                normalized = self._normalize_session_summary_props(node)
                if normalized:
                    return normalized
            return None
        except Exception as e:
            logger.error(f"get_latest_session_summary_node failed: {e}")
            return None

    async def get_recent_session_summary_nodes(
        self,
        tenant_id: str,
        user_id: str,
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """Return recent SessionSummary nodes for a user group."""
        if not self._initialized:
            await self.initialize()
        if not self._initialized or self.client is None:
            logger.warning("Graphiti client unavailable; skipping get_recent_session_summary_nodes")
            return []

        driver = self._group_driver(tenant_id, user_id)
        if not driver:
            logger.warning("Graphiti driver unavailable; skipping get_recent_session_summary_nodes")
            return []

        composite_user_id = self._make_composite_user_id(tenant_id, user_id)
        safe_limit = max(1, min(int(limit or 5), 10))
        try:
            rows = await driver.execute_query(
                """
                MATCH (n:SessionSummary {group_id: $group_id})
                RETURN properties(n) AS props
                ORDER BY n.created_at DESC
                LIMIT $limit
                """,
                group_id=composite_user_id,
                limit=safe_limit
            )
            out: List[Dict[str, Any]] = []
            for row in rows or []:
                for node in extract_node_dicts(row, required_keys=("uuid",)):
                    normalized = self._normalize_session_summary_props(node)
                    if normalized:
                        out.append(normalized)
            return out
        except Exception as e:
            logger.error(f"get_recent_session_summary_nodes failed: {e}")
            return []

    @staticmethod
    def _normalize_session_summary_props(node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(node, dict):
            return None

        attrs_raw = node.get("attributes")
        attrs: Dict[str, Any] = attrs_raw if isinstance(attrs_raw, dict) else {}
        synthesized_attrs = dict(attrs)
        # Graphiti currently flattens custom attributes to top-level node properties.
        # Rebuild a stable attributes map so downstream code can read either shape.
        for key in (
            "summary_text",
            "bridge_text",
            "session_id",
            "reference_time",
            "summary_facts",
            "tone",
            "moment",
            "decisions",
            "unresolved",
            "index_text",
            "salience",
        ):
            value = node.get(key)
            if value is not None and key not in synthesized_attrs:
                synthesized_attrs[key] = value

        summary_text = node.get("summary_text")
        if not summary_text:
            summary_text = synthesized_attrs.get("summary_text")
        bridge_text = node.get("bridge_text")
        if not bridge_text:
            bridge_text = synthesized_attrs.get("bridge_text")

        return {
            "name": node.get("name"),
            "summary": node.get("summary"),
            "summary_text": summary_text,
            "bridge_text": bridge_text,
            "session_id": node.get("session_id") or synthesized_attrs.get("session_id"),
            "reference_time": node.get("reference_time") or synthesized_attrs.get("reference_time"),
            "created_at": node.get("created_at"),
            "uuid": node.get("uuid"),
            "group_id": node.get("group_id"),
            "attributes": synthesized_attrs,
        }

    async def get_session_summary_nodes_by_ids(
        self,
        tenant_id: str,
        user_id: str,
        ids: List[str],
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """Bounded fallback fetch by session_id/uuid/name using properties(n)."""
        if not ids:
            return []
        if not self._initialized:
            await self.initialize()
        if not self._initialized or self.client is None:
            logger.warning("Graphiti client unavailable; skipping get_session_summary_nodes_by_ids")
            return []

        driver = self._group_driver(tenant_id, user_id)
        if not driver:
            logger.warning("Graphiti driver unavailable; skipping get_session_summary_nodes_by_ids")
            return []

        composite_user_id = self._make_composite_user_id(tenant_id, user_id)
        safe_limit = max(1, min(int(limit or 5), 10))
        safe_ids = [str(x) for x in ids if str(x).strip()][:safe_limit]
        if not safe_ids:
            return []

        try:
            rows = await driver.execute_query(
                """
                MATCH (n:SessionSummary {group_id: $group_id})
                WHERE n.session_id IN $ids OR n.uuid IN $ids OR n.name IN $ids
                RETURN properties(n) AS props
                ORDER BY n.created_at DESC
                LIMIT $limit
                """,
                group_id=composite_user_id,
                ids=safe_ids,
                limit=safe_limit,
            )
            out: List[Dict[str, Any]] = []
            for row in rows or []:
                nodes = extract_node_dicts(row)
                for node in nodes:
                    if isinstance(node, dict):
                        out.append(node)
                        break
                if not nodes and isinstance(row, dict) and isinstance(row.get("props"), dict):
                    out.append(row.get("props"))
            return out[:safe_limit]
        except Exception as e:
            logger.warning("get_session_summary_nodes_by_ids failed ids=%s error=%s", safe_ids, e)
            return []

    async def smoke_test(
        self,
        tenant_id: str,
        user_id: str,
        now: datetime,
        text: str = "hello"
    ) -> Dict[str, Any]:
        """Simple smoke test to verify Graphiti add/search/recent behavior."""
        composite_user_id = self._make_composite_user_id(tenant_id, user_id)
        results: Dict[str, Any] = {
            "compositeUserId": composite_user_id,
            "addEpisode": False,
            "searchCount": 0,
            "recentCount": 0
        }

        add_result = await self.add_episode(
            tenant_id=tenant_id,
            user_id=user_id,
            text=text,
            timestamp=now,
            metadata=None
        )
        if isinstance(add_result, dict):
            results["addEpisode"] = bool(add_result.get("success"))

        search_results = await self.search(tenant_id, user_id, text, limit=3)
        results["searchCount"] = len(search_results)

        recent_results = await self.get_recent_episodes(tenant_id, user_id, since=None, limit=3)
        results["recentCount"] = len(recent_results)

        return results

    async def purge_user_graph(self, tenant_id: str, user_id: str) -> Dict[str, Any]:
        """Delete Graphiti nodes/edges for a given tenant/user group_id."""
        if not self._initialized:
            await self.initialize()
        if not self._initialized or self.client is None:
            logger.warning("Graphiti client unavailable; skipping purge")
            return {"success": False, "reason": "client_unavailable"}

        composite_user_id = self._make_composite_user_id(tenant_id, user_id)
        driver = self._group_driver(tenant_id, user_id)
        if not driver:
            logger.warning("Graphiti driver unavailable; skipping purge")
            return {"success": False, "reason": "driver_unavailable"}

        try:
            count_nodes = 0
            count_edges = 0
            # Count nodes
            count_res = await driver.execute_query(
                "MATCH (n {group_id: $group_id}) RETURN count(n) AS c",
                group_id=composite_user_id
            )
            if count_res and isinstance(count_res, list) and count_res[0]:
                count_nodes = int(count_res[0].get("c", 0))

            # Count edges
            edge_res = await driver.execute_query(
                "MATCH ()-[r {group_id: $group_id}]-() RETURN count(r) AS c",
                group_id=composite_user_id
            )
            if edge_res and isinstance(edge_res, list) and edge_res[0]:
                count_edges = int(edge_res[0].get("c", 0))

            # Delete edges then nodes
            await driver.execute_query(
                "MATCH ()-[r {group_id: $group_id}]-() DELETE r",
                group_id=composite_user_id
            )
            await driver.execute_query(
                "MATCH (n {group_id: $group_id}) DETACH DELETE n",
                group_id=composite_user_id
            )

            logger.info(f"Purged Graphiti group_id {composite_user_id} (nodes={count_nodes}, edges={count_edges})")
            return {"success": True, "nodes": count_nodes, "edges": count_edges}
        except Exception as e:
            logger.error(f"Graphiti purge failed: {e}")
            return {"success": False, "reason": str(e)}
