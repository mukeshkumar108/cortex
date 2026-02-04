from typing import Optional, Dict, Any
import json
import logging
import re
from .db import Database

logger = logging.getLogger(__name__)


class IdentityManager:
    def __init__(self, db: Database):
        self.db = db

    async def get_identity(self, tenant_id: str, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user identity"""
        try:
            query = """
                SELECT data
                FROM user_identity
                WHERE tenant_id = $1 AND user_id = $2
            """
            result = await self.db.fetchone(query, tenant_id, user_id)

            if result:
                data = result['data']
                if isinstance(data, str):
                    try:
                        data = json.loads(data)
                    except json.JSONDecodeError:
                        logger.warning(
                            f"Identity data for {tenant_id}:{user_id} is invalid JSON string"
                        )
                        return None

                if not isinstance(data, dict):
                    logger.warning(
                        f"Identity data for {tenant_id}:{user_id} is not a dict; ignoring"
                    )
                    return None

                return data
            return None

        except Exception as e:
            logger.error(f"Failed to get identity for {tenant_id}:{user_id}: {e}")
            return None

    async def update_identity(
        self,
        tenant_id: str,
        user_id: str,
        updates: Dict[str, Any]
    ) -> None:
        """Update user identity (merge with existing)"""
        try:
            # Get existing identity
            existing = await self.get_identity(tenant_id, user_id)

            if existing is None:
                # Create new identity with defaults
                existing = self._get_default_identity()
            if isinstance(existing, dict) and "isDefault" not in existing:
                existing = {**existing, "isDefault": False}

            updates = dict(updates) if updates else {}
            candidate_name = updates.get("name")
            existing_name = existing.get("name") if isinstance(existing, dict) else None
            existing_is_default = bool(existing.get("isDefault")) if isinstance(existing, dict) else True

            if isinstance(candidate_name, str):
                if not _is_valid_name_candidate(candidate_name):
                    updates.pop("name", None)
                else:
                    existing_good = bool(existing_name) and not _is_placeholder_name(existing_name)
                    if not existing_is_default and existing_good:
                        updates.pop("name", None)
            elif "name" in updates:
                updates.pop("name", None)

            # Merge updates
            merged = {**existing, **updates}

            # Merge updates
            query = """
                INSERT INTO user_identity (tenant_id, user_id, data, updated_at)
                VALUES ($1, $2, $3::jsonb, NOW())
                ON CONFLICT (tenant_id, user_id)
                DO UPDATE SET
                    data = EXCLUDED.data,
                    updated_at = NOW()
            """

            if isinstance(merged, dict):
                name = merged.get("name")
                if isinstance(name, str) and name.strip() and not _is_placeholder_name(name):
                    merged["isDefault"] = False
                else:
                    merged["isDefault"] = True

            await self.db.execute(query, tenant_id, user_id, merged)
            logger.info(f"Updated identity for {tenant_id}:{user_id}")

        except Exception as e:
            logger.error(f"Failed to update identity: {e}")
            raise

    async def ensure_identity_exists(
        self,
        tenant_id: str,
        user_id: str
    ) -> Dict[str, Any]:
        """Ensure identity exists, create with defaults if missing"""
        try:
            identity = await self.get_identity(tenant_id, user_id)

            if identity is None:
                # Create default identity
                default_identity = self._get_default_identity()
                default_identity["isDefault"] = True
                await self.update_identity(tenant_id, user_id, default_identity)
                return default_identity

            return identity

        except Exception as e:
            logger.error(f"Failed to ensure identity exists: {e}")
            raise

    def _get_default_identity(self) -> Dict[str, Any]:
        """Get default identity structure"""
        return {
            "name": None,
            "timezone": "UTC",
            "home": None,
            "preferences": {}
        }


def _is_placeholder_name(name: str) -> bool:
    if not isinstance(name, str):
        return True
    return name.strip().lower() in {
        "there",
        "here",
        "working",
        "busy",
        "fine",
        "ok",
        "okay",
        "unknown",
        "n/a",
        "na",
        "none",
        "null",
        ""
    }


def _is_valid_name_candidate(name: str) -> bool:
    if not isinstance(name, str):
        return False
    candidate = name.strip()
    if len(candidate) < 2:
        return False
    if _is_placeholder_name(candidate):
        return False
    return re.fullmatch(r"[A-Za-z][A-Za-z\\s'\\-]*", candidate) is not None


# Module-level functions for easier imports
_manager: Optional[IdentityManager] = None


def init_identity_manager(db: Database):
    """Initialize the identity manager"""
    global _manager
    _manager = IdentityManager(db)


async def get_identity(tenant_id: str, user_id: str) -> Optional[Dict[str, Any]]:
    """Get user identity"""
    if _manager is None:
        raise RuntimeError("IdentityManager not initialized")
    return await _manager.get_identity(tenant_id, user_id)


async def update_identity(tenant_id: str, user_id: str, updates: Dict[str, Any]) -> None:
    """Update user identity"""
    if _manager is None:
        raise RuntimeError("IdentityManager not initialized")
    await _manager.update_identity(tenant_id, user_id, updates)


async def ensure_identity_exists(tenant_id: str, user_id: str) -> Dict[str, Any]:
    """Ensure identity exists"""
    if _manager is None:
        raise RuntimeError("IdentityManager not initialized")
    return await _manager.ensure_identity_exists(tenant_id, user_id)
