import inspect
from graphiti_core import Graphiti
from graphiti_core.llm_client.client import LLMClient
from graphiti_core.llm_client.config import LLMConfig
from graphiti_core.nodes import EpisodeType, EpisodicNode, EntityNode
from graphiti_core.edges import EntityEdge
from graphiti_core.driver.falkordb_driver import FalkorDriver
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import logging
from .config import get_settings
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

class MentalState(BaseModel):
    """User-stated state captured only when explicitly stated."""
    mood: Optional[str] = Field(None, description="Explicit user phrase only, e.g., 'I feel anxious'")
    energy_level: Optional[str] = Field(None, description="Explicit user phrase only, e.g., 'I am tired'")


class Tension(BaseModel):
    """An unresolved problem, task, or blocker explicitly mentioned by the user."""
    description: str = Field(..., description="Short factual description of the unresolved item")
    status: str = Field("unresolved", description="Current state of this loop")


class Environment(BaseModel):
    """The physical or situational context mentioned in the session."""
    location_type: Optional[str] = Field(None, description="e.g., Cafe, Home, Gym, Outside")
    vibe: Optional[str] = Field(None, description="Concrete context detail only (e.g., 'Noisy', 'Raining')")

class Observation(BaseModel):
    """A small incidental human detail (sensory or phrasing)."""
    detail: Optional[str] = Field(None, description="e.g., 'raining outside', 'drinking matcha'")

class UserFocus(BaseModel):
    """User-stated focus captured only when explicitly stated."""
    focus: Optional[str] = Field(None, description="Explicit user phrase only, keep concise and neutral")


class Feels(BaseModel):
    """Edge: User explicitly stated a mental state."""
    pass


class StrugglingWith(BaseModel):
    """Edge: User has an unresolved item."""
    pass


class LocatedIn(BaseModel):
    """Edge: Person is located in an Environment."""
    pass

class Observed(BaseModel):
    """Edge: Person observed a small incidental detail."""
    pass

class FocusedOn(BaseModel):
    """Edge: User explicitly stated current focus."""
    pass


NARRATIVE_ENTITY_TYPES: Dict[str, type[BaseModel]] = {
    "MentalState": MentalState,
    "Tension": Tension,
    "Environment": Environment,
    "Observation": Observation,
    "UserFocus": UserFocus,
}

NARRATIVE_EDGE_TYPES: Dict[str, type[BaseModel]] = {
    "FEELS": Feels,
    "STRUGGLING_WITH": StrugglingWith,
    "LOCATED_IN": LocatedIn,
    "OBSERVED": Observed,
    "FOCUSED_ON": FocusedOn,
}

NARRATIVE_EDGE_TYPE_MAP: Dict[Tuple[str, str], List[str]] = {
    ("Person", "MentalState"): ["FEELS"],
    ("Person", "Tension"): ["STRUGGLING_WITH"],
    ("Person", "Environment"): ["LOCATED_IN"],
    ("Person", "Observation"): ["OBSERVED"],
    ("Person", "UserFocus"): ["FOCUSED_ON"],
}

NARRATIVE_EXTRACTION_INSTRUCTIONS = (
    "Extract factual, user-stated memory only. Do not infer emotions, intent, or therapy-style framing. "
    "Capture MentalState only when the user explicitly states it in first person (for example: 'I feel anxious', "
    "'I am tired'). Capture unresolved tasks/blockers as Tension only when explicitly mentioned. "
    "Capture UserFocus only when explicitly stated (for example: 'I'm focused on X', "
    "'My priority this week is X', 'Today I need to X', 'Right now I'm trying to X'). "
    "Do not infer focus from complaints, lists of tasks, or implied goals. Do not treat emotions as focus. "
    "Keep focus concise (<= 80 chars), neutral, and user-phrased. "
    "Capture environment as concrete context details (location/noise/weather) and keep phrasing neutral. "
    "Continue extracting generic entities (names, places, projects). Optionally store one concrete Observation detail."
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

    def _format_message_transcript(self, messages: List[Dict[str, Any]]) -> str:
        lines: List[str] = []
        for msg in messages:
            role = (msg.get("role") or "").lower()
            if role == "assistant":
                role_label = "Assistant"
            elif role == "system":
                role_label = "System"
            else:
                role_label = "User"
            text = msg.get("text") or ""
            lines.append(f"{role_label}: {text}")
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
            return {"success": False}

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
            return {"success": False}

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
        episode_uuid: Optional[str] = None
    ) -> Dict[str, Any]:
        """Store a SessionSummary node in Graphiti and optionally link to an episode."""
        if not self._initialized:
            await self.initialize()
        if not self._initialized or self.client is None:
            logger.warning("Graphiti client unavailable; skipping add_session_summary")
            return {"success": False}

        driver = getattr(self.client, "driver", None)
        if not driver:
            logger.warning("Graphiti driver unavailable; skipping add_session_summary")
            return {"success": False}

        composite_user_id = self._make_composite_user_id(tenant_id, user_id)
        name = f"session_summary_{session_id}_{int(reference_time.timestamp())}"
        node = EntityNode(
            name=name,
            group_id=composite_user_id,
            labels=["SessionSummary"],
            summary=summary_text,
            attributes={
                "summary_text": summary_text,
                "session_id": session_id,
                "reference_time": reference_time.isoformat()
            }
        )
        try:
            saved = node.save(driver)
            if inspect.isawaitable(saved):
                await saved
            logger.info(
                f"SessionSummary node saved name={name} uuid={node.uuid} len={len(summary_text)}"
            )
        except Exception as e:
            logger.error(f"Failed to save SessionSummary node: {e}")
            return {"success": False}

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
        if not self._initialized:
            await self.initialize()
        if not self._initialized or self.client is None:
            logger.warning("Graphiti client unavailable; skipping search_facts")
            return []

        try:
            composite_user_id = self._make_composite_user_id(tenant_id, user_id)

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
                    "source": "graphiti"
                })

            logger.info(f"search_facts returned {len(facts)} results for query: {query}")
            return facts

        except Exception as e:
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
        search_filter: Optional[Any] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for relevant entities (nodes) in Graphiti using semantic query.

        Returns entity summaries representing people, places, concepts
        extracted by Graphiti's LLM. Filters out edges/predicates.
        """
        if not query or not str(query).strip():
            return []
        if not self._initialized:
            await self.initialize()
        if not self._initialized or self.client is None:
            logger.warning("Graphiti client unavailable; skipping search_nodes")
            return []

        try:
            composite_user_id = self._make_composite_user_id(tenant_id, user_id)

            # Prefer node-only search when available
            search_method = getattr(self.client, "_search", None)
            if callable(search_method):
                try:
                    from graphiti_core.search.search_config_recipes import NODE_HYBRID_SEARCH_RRF
                    config = NODE_HYBRID_SEARCH_RRF.model_copy(deep=True)
                    config.limit = limit * 2
                    results = await search_method(
                        query=query,
                        config=config,
                        group_ids=[composite_user_id],
                        search_filter=search_filter
                    )
                    results = getattr(results, "nodes", [])
                except Exception:
                    results = await self.client.search(
                        query=query,
                        group_ids=[composite_user_id],
                        num_results=limit * 2
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
                if query_hint:
                    if "query_hint" in params:
                        kwargs["query_hint"] = query_hint
                    elif "context" in params:
                        kwargs["context"] = query_hint

                results = await search_fn(**kwargs)

            entities = []
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

                # Best-effort timestamps for recency selection
                if hasattr(result, 'created_at'):
                    entity_data["created_at"] = result.created_at
                if hasattr(result, 'updated_at'):
                    entity_data["updated_at"] = result.updated_at
                if hasattr(result, 'reference_time'):
                    entity_data["reference_time"] = result.reference_time

                entities.append(entity_data)

                # Stop once we have enough valid entities
                if len(entities) >= limit:
                    break

            logger.info(f"search_nodes returned {len(entities)} entities (filtered from {len(results)} results)")
            return entities

        except Exception as e:
            logger.error(f"search_nodes failed: {e}")
            return []

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

            driver = getattr(self.client, "driver", None)
            if driver:
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

        driver = getattr(self.client, "driver", None)
        if not driver:
            logger.warning("Graphiti driver unavailable; skipping get_latest_session_summary_node")
            return None

        composite_user_id = self._make_composite_user_id(tenant_id, user_id)
        try:
            rows = await driver.execute_query(
                """
                MATCH (n:SessionSummary {group_id: $group_id})
                RETURN n.name AS name,
                       n.summary AS summary,
                       n.created_at AS created_at,
                       n.uuid AS uuid,
                       n.group_id AS group_id,
                       n.attributes AS attributes
                ORDER BY n.created_at DESC
                LIMIT 1
                """,
                group_id=composite_user_id
            )
            if not rows:
                return None
            row = rows[0]
            if isinstance(row, dict):
                # Some drivers return nested dicts; pick the first dict with name/summary.
                if any(isinstance(v, dict) for v in row.values()):
                    for v in row.values():
                        if isinstance(v, dict) and v.get("name") and v.get("summary"):
                            row = v
                            break
                return {
                    "name": row.get("name"),
                    "summary": row.get("summary"),
                    "created_at": row.get("created_at"),
                    "uuid": row.get("uuid"),
                    "group_id": row.get("group_id"),
                    "attributes": row.get("attributes") or {}
                }
            if isinstance(row, (list, tuple)):
                return {
                    "name": row[0] if len(row) > 0 else None,
                    "summary": row[1] if len(row) > 1 else None,
                    "created_at": row[2] if len(row) > 2 else None,
                    "uuid": row[3] if len(row) > 3 else None,
                    "group_id": row[4] if len(row) > 4 else None,
                    "attributes": row[5] if len(row) > 5 else {}
                }
            return None
        except Exception as e:
            logger.error(f"get_latest_session_summary_node failed: {e}")
            return None

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
        driver = getattr(self.client, "driver", None)
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
