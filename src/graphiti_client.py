import inspect
from graphiti_core import Graphiti
from graphiti_core.nodes import EpisodeType
from graphiti_core.driver.falkordb_driver import FalkorDriver
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
from .config import get_settings

logger = logging.getLogger(__name__)


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

            # Initialize Graphiti with LLM extraction enabled
            # llm_client defaults to OpenAI if OPENAI_API_KEY is set in environment
            self.client = Graphiti(graph_driver=falkor_driver)

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

            await self.client.add_episode(
                name=episode_name or f"episode_{timestamp.isoformat()}",
                episode_body=tagged_text,
                source=EpisodeType.message,
                source_description="synapse_conversation",
                reference_time=timestamp,
                group_id=composite_user_id
            )

            logger.info(f"Added episode for user {composite_user_id} (role={role})")
            return {"success": True}

        except Exception as e:
            logger.error(f"Failed to add episode: {e}")
            return {"success": False}

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
        query_hint: Optional[str] = None
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

            # Check if Graphiti has a dedicated node search method
            node_search = getattr(self.client, "search_nodes", None)
            search_fn = node_search if callable(node_search) else self.client.search
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

                if hasattr(result, 'uuid'):
                    entity_data["uuid"] = str(result.uuid)
                elif hasattr(result, 'id'):
                    entity_data["uuid"] = str(result.id)

                entities.append(entity_data)

                # Stop once we have enough valid entities
                if len(entities) >= limit:
                    break

            logger.info(f"search_nodes returned {len(entities)} entities (filtered from {len(results)} results)")
            return entities

        except Exception as e:
            logger.error(f"search_nodes failed: {e}")
            return []

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
