from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import json
import logging

from .db import Database
from .openrouter_client import get_llm_client
from .config import get_settings

logger = logging.getLogger(__name__)


class IdentityCacheManager:
    def __init__(self, db: Database):
        self.db = db
        self.settings = get_settings()
        self.llm_client = get_llm_client()

    async def get_cached_identity(
        self,
        tenant_id: str,
        user_id: str
    ) -> Optional[Dict[str, Any]]:
        try:
            row = await self.db.fetchone(
                """
                SELECT preferred_name, timezone, facts, last_synced_at
                FROM identity_cache
                WHERE tenant_id = $1 AND user_id = $2
                """,
                tenant_id,
                user_id
            )
            if not row:
                return None
            return {
                "preferred_name": row.get("preferred_name"),
                "timezone": row.get("timezone"),
                "facts": row.get("facts") or {},
                "last_synced_at": row.get("last_synced_at")
            }
        except Exception as e:
            logger.error(f"Failed to get identity_cache for {tenant_id}:{user_id}: {e}")
            return None

    async def upsert_identity_cache(
        self,
        tenant_id: str,
        user_id: str,
        preferred_name: Optional[str],
        timezone: Optional[str],
        facts: Dict[str, Any]
    ) -> None:
        await self.db.execute(
            """
            INSERT INTO identity_cache (tenant_id, user_id, preferred_name, timezone, facts, last_synced_at)
            VALUES ($1, $2, $3, $4, $5::jsonb, NOW())
            ON CONFLICT (tenant_id, user_id)
            DO UPDATE SET
                preferred_name = EXCLUDED.preferred_name,
                timezone = EXCLUDED.timezone,
                facts = EXCLUDED.facts,
                last_synced_at = NOW()
            """,
            tenant_id,
            user_id,
            preferred_name,
            timezone,
            facts or {}
        )

    def _is_stale(self, last_synced_at: Optional[datetime]) -> bool:
        if not last_synced_at:
            return True
        ttl = timedelta(hours=int(self.settings.identity_cache_ttl_hours))
        return datetime.utcnow() - last_synced_at > ttl

    async def sync_from_graphiti(
        self,
        tenant_id: str,
        user_id: str,
        graphiti_client
    ) -> Optional[Dict[str, Any]]:
        try:
            now = datetime.utcnow()
            facts = await graphiti_client.search_facts(
                tenant_id,
                user_id,
                query="user identity name home timezone location preferences",
                limit=12,
                reference_time=now
            )
            nodes = await graphiti_client.search_nodes(
                tenant_id,
                user_id,
                query="user person profile",
                limit=6,
                reference_time=now
            )

            if not facts and not nodes:
                return None

            prompt = (
                "Extract user identity fields from the provided memory facts/entities.\n"
                "Return STRICT JSON with keys:\n"
                "{"
                "\"preferred_name\": string|null, "
                "\"home\": string|null, "
                "\"timezone\": string|null, "
                "\"facts\": object"
                "}\n"
                "Only include fields that are strongly supported by the facts.\n"
                "If unknown, return null. Keep facts as a small object.\n\n"
                f"FACTS:\n{json.dumps(facts, ensure_ascii=True)}\n\n"
                f"ENTITIES:\n{json.dumps(nodes, ensure_ascii=True)}\n"
            )

            response = await self.llm_client._call_llm(
                prompt=prompt,
                max_tokens=200,
                temperature=0.0,
                task="identity"
            )

            if not response:
                return None

            data = None
            if isinstance(response, dict):
                data = response
            else:
                try:
                    data = json.loads(response)
                except json.JSONDecodeError:
                    return None

            if not isinstance(data, dict):
                return None

            preferred_name = data.get("preferred_name")
            home = data.get("home")
            timezone = data.get("timezone")
            facts_obj = data.get("facts") or {}
            if home:
                facts_obj = {**facts_obj, "home": home}

            await self.upsert_identity_cache(
                tenant_id=tenant_id,
                user_id=user_id,
                preferred_name=preferred_name,
                timezone=timezone,
                facts=facts_obj
            )

            return {
                "preferred_name": preferred_name,
                "home": home,
                "timezone": timezone,
                "facts": facts_obj,
                "last_synced_at": now
            }
        except Exception as e:
            logger.error(f"Failed to sync identity_cache from Graphiti: {e}")
            return None

    async def get_identity_for_brief(
        self,
        tenant_id: str,
        user_id: str,
        graphiti_client
    ) -> Dict[str, Any]:
        cached = await self.get_cached_identity(tenant_id, user_id)
        if not cached or self._is_stale(cached.get("last_synced_at")):
            refreshed = await self.sync_from_graphiti(tenant_id, user_id, graphiti_client)
            if refreshed:
                cached = refreshed

        preferred_name = cached.get("preferred_name") if cached else None
        facts = cached.get("facts") if cached else {}
        timezone = cached.get("timezone") if cached else None
        home = None
        if isinstance(facts, dict):
            home = facts.get("home")

        return {
            "name": preferred_name,
            "home": home,
            "timezone": timezone,
            "isDefault": not bool(preferred_name)
        }


_manager: Optional[IdentityCacheManager] = None


def init_identity_cache_manager(db: Database):
    global _manager
    _manager = IdentityCacheManager(db)


async def get_identity_for_brief(tenant_id: str, user_id: str, graphiti_client) -> Dict[str, Any]:
    if _manager is None:
        raise RuntimeError("IdentityCacheManager not initialized")
    return await _manager.get_identity_for_brief(tenant_id, user_id, graphiti_client)
