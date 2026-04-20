from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone as dt_timezone
from typing import Any, Dict, List, Optional
import uuid

from .canonicalization import normalize_text
from .canonical_mutation_log import CanonicalMutationLogger
from .db import Database


ENTITY_RESOLVER_VERSION = "t6.entity_resolver.v1"
RESOLUTION_STATUS_RESOLVED = "resolved_existing"
RESOLUTION_STATUS_CREATED = "created_new"
RESOLUTION_STATUS_AMBIGUOUS = "ambiguous"
RESOLUTION_STATUS_UNRESOLVED = "unresolved"


class EntityResolutionError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class EntityResolutionResult:
    resolved_entity_id: Optional[str]
    resolution_status: str
    resolution_confidence: float
    resolver_version: str
    candidate_entity_ids: List[str]
    alias_normalized: str


@dataclass(frozen=True)
class EntityMergeResult:
    source_entity_id: str
    survivor_entity_id: str
    resolution_status: str
    resolver_version: str
    copied_aliases: int


def _utc_now() -> datetime:
    return datetime.utcnow().replace(tzinfo=dt_timezone.utc)


def _normalize_alias(value: Any) -> str:
    return normalize_text(value, casefold=True)


def _normalize_name(value: Any) -> str:
    return normalize_text(value, casefold=False)


class EntityResolver:
    def __init__(self, db: Database):
        self.db = db
        self.mutation_logger = CanonicalMutationLogger(db)

    async def resolve_entity_mention(
        self,
        *,
        tenant_id: str,
        user_id: str,
        mention_text: str,
        entity_type: str,
        allow_create: bool = True,
        resolution_confidence_hint: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> EntityResolutionResult:
        tenant = str(tenant_id or "").strip()
        user = str(user_id or "").strip()
        canonical_name = _normalize_name(mention_text)
        alias_normalized = _normalize_alias(mention_text)
        normalized_type = _normalize_alias(entity_type)
        if not tenant or not user:
            raise EntityResolutionError("ENTITY_RESOLUTION_INVALID_SCOPE", "tenant_id and user_id are required")
        if not alias_normalized:
            return EntityResolutionResult(
                resolved_entity_id=None,
                resolution_status=RESOLUTION_STATUS_UNRESOLVED,
                resolution_confidence=0.0,
                resolver_version=ENTITY_RESOLVER_VERSION,
                candidate_entity_ids=[],
                alias_normalized="",
            )
        if not normalized_type:
            raise EntityResolutionError("ENTITY_RESOLUTION_INVALID_TYPE", "entity_type is required")

        alias_matches = await self.db.fetch(
            """
            SELECT DISTINCT e.entity_id::text AS entity_id
            FROM entity_aliases a
            JOIN entities e
              ON e.tenant_id = a.tenant_id
             AND e.entity_id = a.entity_id
            WHERE a.tenant_id = $1
              AND a.user_id = $2
              AND a.alias_normalized = $3
              AND e.user_id = $2
              AND e.status = 'active'
            ORDER BY e.entity_id::text ASC
            """,
            tenant,
            user,
            alias_normalized,
        )
        alias_entity_ids = [str(r.get("entity_id")) for r in alias_matches if str(r.get("entity_id") or "").strip()]
        if len(alias_entity_ids) > 1:
            await self._record_ambiguity(
                tenant_id=tenant,
                user_id=user,
                candidate_entity_ids=alias_entity_ids,
                mention_text=canonical_name,
                alias_normalized=alias_normalized,
                entity_type=normalized_type,
                metadata=metadata,
            )
            return EntityResolutionResult(
                resolved_entity_id=None,
                resolution_status=RESOLUTION_STATUS_AMBIGUOUS,
                resolution_confidence=0.0,
                resolver_version=ENTITY_RESOLVER_VERSION,
                candidate_entity_ids=alias_entity_ids,
                alias_normalized=alias_normalized,
            )
        if len(alias_entity_ids) == 1:
            entity_id = alias_entity_ids[0]
            await self._ensure_alias_linked(
                tenant_id=tenant,
                user_id=user,
                entity_id=entity_id,
                alias_text=canonical_name,
                alias_normalized=alias_normalized,
                confidence=resolution_confidence_hint,
                is_primary=False,
                metadata=metadata,
            )
            return EntityResolutionResult(
                resolved_entity_id=entity_id,
                resolution_status=RESOLUTION_STATUS_RESOLVED,
                resolution_confidence=1.0,
                resolver_version=ENTITY_RESOLVER_VERSION,
                candidate_entity_ids=[entity_id],
                alias_normalized=alias_normalized,
            )

        name_row = await self.db.fetchone(
            """
            SELECT entity_id::text AS entity_id
            FROM entities
            WHERE tenant_id = $1
              AND user_id = $2
              AND canonical_name_normalized = $3
              AND status = 'active'
            LIMIT 1
            """,
            tenant,
            user,
            alias_normalized,
        )
        if name_row and str(name_row.get("entity_id") or "").strip():
            entity_id = str(name_row["entity_id"])
            await self._ensure_alias_linked(
                tenant_id=tenant,
                user_id=user,
                entity_id=entity_id,
                alias_text=canonical_name,
                alias_normalized=alias_normalized,
                confidence=resolution_confidence_hint,
                is_primary=False,
                metadata=metadata,
            )
            return EntityResolutionResult(
                resolved_entity_id=entity_id,
                resolution_status=RESOLUTION_STATUS_RESOLVED,
                resolution_confidence=0.95,
                resolver_version=ENTITY_RESOLVER_VERSION,
                candidate_entity_ids=[entity_id],
                alias_normalized=alias_normalized,
            )

        if not allow_create:
            return EntityResolutionResult(
                resolved_entity_id=None,
                resolution_status=RESOLUTION_STATUS_UNRESOLVED,
                resolution_confidence=0.0,
                resolver_version=ENTITY_RESOLVER_VERSION,
                candidate_entity_ids=[],
                alias_normalized=alias_normalized,
            )

        created = await self.db.fetchone(
            """
            INSERT INTO entities (
                tenant_id,
                user_id,
                canonical_name,
                canonical_name_normalized,
                entity_type,
                status,
                metadata,
                created_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, 'active', $6::jsonb, $7, $7
            )
            RETURNING entity_id::text AS entity_id
            """,
            tenant,
            user,
            canonical_name,
            alias_normalized,
            normalized_type,
            {
                "resolver_version": ENTITY_RESOLVER_VERSION,
                "source": "entity_resolution_v2",
            },
            _utc_now(),
        )
        if not created or not str(created.get("entity_id") or "").strip():
            raise EntityResolutionError("ENTITY_RESOLUTION_CREATE_FAILED", "failed to create canonical entity")
        entity_id = str(created["entity_id"])

        await self._ensure_alias_linked(
            tenant_id=tenant,
            user_id=user,
            entity_id=entity_id,
            alias_text=canonical_name,
            alias_normalized=alias_normalized,
            confidence=resolution_confidence_hint,
            is_primary=True,
            metadata=metadata,
        )
        await self._write_mutation(
            tenant_id=tenant,
            user_id=user,
            mutation_type="entity_create",
            entity_id=entity_id,
            payload={
                "entity_id": entity_id,
                "canonical_name": canonical_name,
                "canonical_name_normalized": alias_normalized,
                "entity_type": normalized_type,
                "resolver_version": ENTITY_RESOLVER_VERSION,
            },
            committed_by="t6.entity_resolver",
            metadata=metadata,
        )
        return EntityResolutionResult(
            resolved_entity_id=entity_id,
            resolution_status=RESOLUTION_STATUS_CREATED,
            resolution_confidence=0.9,
            resolver_version=ENTITY_RESOLVER_VERSION,
            candidate_entity_ids=[entity_id],
            alias_normalized=alias_normalized,
        )

    async def merge_entity_into_survivor(
        self,
        *,
        tenant_id: str,
        user_id: str,
        source_entity_id: str,
        survivor_entity_id: str,
        committed_by: str,
        reason: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> EntityMergeResult:
        tenant = str(tenant_id or "").strip()
        user = str(user_id or "").strip()
        source_id = str(source_entity_id or "").strip()
        survivor_id = str(survivor_entity_id or "").strip()
        actor = str(committed_by or "").strip() or "t6.entity_resolver"
        merge_reason = str(reason or "").strip() or "manual_merge"
        if not tenant or not user:
            raise EntityResolutionError("ENTITY_MERGE_INVALID_SCOPE", "tenant_id and user_id are required")
        if not source_id or not survivor_id:
            raise EntityResolutionError("ENTITY_MERGE_INVALID_ID", "source_entity_id and survivor_entity_id are required")
        if source_id == survivor_id:
            raise EntityResolutionError("ENTITY_MERGE_SAME_ENTITY", "cannot merge entity into itself")
        try:
            source_uuid = uuid.UUID(source_id)
            survivor_uuid = uuid.UUID(survivor_id)
        except Exception:
            raise EntityResolutionError("ENTITY_MERGE_INVALID_UUID", "entity ids must be UUIDs")

        pool = await self.db.get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                source_row = await conn.fetchrow(
                    """
                    SELECT entity_id::text AS entity_id, status, merged_into_entity_id::text AS merged_into_entity_id
                    FROM entities
                    WHERE tenant_id = $1 AND user_id = $2 AND entity_id = $3::uuid
                    FOR UPDATE
                    """,
                    tenant,
                    user,
                    source_uuid,
                )
                survivor_row = await conn.fetchrow(
                    """
                    SELECT entity_id::text AS entity_id, status
                    FROM entities
                    WHERE tenant_id = $1 AND user_id = $2 AND entity_id = $3::uuid
                    FOR UPDATE
                    """,
                    tenant,
                    user,
                    survivor_uuid,
                )
                if not source_row or not survivor_row:
                    raise EntityResolutionError(
                        "ENTITY_MERGE_NOT_FOUND",
                        "source/survivor entity not found in tenant/user scope",
                    )
                if str(source_row.get("status") or "") != "active":
                    raise EntityResolutionError("ENTITY_MERGE_SOURCE_NOT_ACTIVE", "source entity must be active")
                if str(survivor_row.get("status") or "") != "active":
                    raise EntityResolutionError("ENTITY_MERGE_SURVIVOR_NOT_ACTIVE", "survivor entity must be active")

                now = _utc_now()
                await conn.execute(
                    """
                    UPDATE entities
                    SET status = 'merged',
                        merged_into_entity_id = $4::uuid,
                        updated_at = $5,
                        metadata = COALESCE(metadata, '{}'::jsonb) || $6::jsonb
                    WHERE tenant_id = $1
                      AND user_id = $2
                      AND entity_id = $3::uuid
                    """,
                    tenant,
                    user,
                    source_uuid,
                    survivor_uuid,
                    now,
                    {
                        "merged_by": actor,
                        "merge_reason": merge_reason,
                        "merged_at": now.isoformat(),
                        "resolver_version": ENTITY_RESOLVER_VERSION,
                    },
                )

                copied = await conn.execute(
                    """
                    INSERT INTO entity_aliases (
                        tenant_id,
                        user_id,
                        entity_id,
                        alias_text,
                        alias_normalized,
                        is_primary,
                        confidence,
                        metadata,
                        first_seen_at,
                        last_seen_at
                    )
                    SELECT
                        a.tenant_id,
                        a.user_id,
                        $3::uuid,
                        a.alias_text,
                        a.alias_normalized,
                        false,
                        a.confidence,
                        COALESCE(a.metadata, '{}'::jsonb) || $4::jsonb,
                        a.first_seen_at,
                        a.last_seen_at
                    FROM entity_aliases a
                    WHERE a.tenant_id = $1
                      AND a.user_id = $2
                      AND a.entity_id = $5::uuid
                      AND NOT EXISTS (
                          SELECT 1
                          FROM entity_aliases existing
                          WHERE existing.tenant_id = a.tenant_id
                            AND existing.entity_id = $3::uuid
                            AND existing.alias_normalized = a.alias_normalized
                      )
                    """,
                    tenant,
                    user,
                    survivor_uuid,
                    {
                        "copied_from_entity_id": source_id,
                        "copied_by": actor,
                        "resolver_version": ENTITY_RESOLVER_VERSION,
                    },
                    source_uuid,
                )
                copied_count = 0
                if isinstance(copied, str) and copied.startswith("INSERT "):
                    try:
                        copied_count = int(copied.split()[-1])
                    except Exception:
                        copied_count = 0

                await self.mutation_logger.append_mutation(
                    conn=conn,
                    tenant_id=tenant,
                    user_id=user,
                    mutation_type="entity_merge",
                    object_type="entity",
                    object_id=source_id,
                    resolver_version=ENTITY_RESOLVER_VERSION,
                    entity_id=source_id,
                    source_run_id=str(metadata.get("source_run_id")) if isinstance(metadata, dict) and metadata.get("source_run_id") else None,
                    committed_by=actor,
                    payload={
                        "source_entity_id": source_id,
                        "survivor_entity_id": survivor_id,
                        "reason": merge_reason,
                        "copied_aliases": copied_count,
                        "resolver_version": ENTITY_RESOLVER_VERSION,
                    },
                    metadata=metadata if isinstance(metadata, dict) else {},
                )

        return EntityMergeResult(
            source_entity_id=source_id,
            survivor_entity_id=survivor_id,
            resolution_status="merged",
            resolver_version=ENTITY_RESOLVER_VERSION,
            copied_aliases=copied_count,
        )

    async def _ensure_alias_linked(
        self,
        *,
        tenant_id: str,
        user_id: str,
        entity_id: str,
        alias_text: str,
        alias_normalized: str,
        confidence: Optional[float],
        is_primary: bool,
        metadata: Optional[Dict[str, Any]],
    ) -> None:
        conflict = await self.db.fetch(
            """
            SELECT DISTINCT e.entity_id::text AS entity_id
            FROM entity_aliases a
            JOIN entities e
              ON e.tenant_id = a.tenant_id
             AND e.entity_id = a.entity_id
            WHERE a.tenant_id = $1
              AND a.user_id = $2
              AND a.alias_normalized = $3
              AND e.user_id = $2
              AND e.status = 'active'
            ORDER BY e.entity_id::text ASC
            """,
            tenant_id,
            user_id,
            alias_normalized,
        )
        conflict_ids = sorted({str(r.get("entity_id")) for r in conflict if str(r.get("entity_id") or "").strip()})
        if conflict_ids and any(eid != entity_id for eid in conflict_ids):
            raise EntityResolutionError(
                "ENTITY_ALIAS_AMBIGUOUS",
                f"alias '{alias_normalized}' is already mapped to multiple active entities",
            )

        await self.db.execute(
            """
            INSERT INTO entity_aliases (
                tenant_id,
                user_id,
                entity_id,
                alias_text,
                alias_normalized,
                is_primary,
                confidence,
                metadata,
                first_seen_at,
                last_seen_at
            )
            VALUES (
                $1, $2, $3::uuid, $4, $5, $6, $7, $8::jsonb, $9, $9
            )
            ON CONFLICT (tenant_id, entity_id, alias_normalized)
            DO UPDATE SET
              alias_text = EXCLUDED.alias_text,
              confidence = COALESCE(EXCLUDED.confidence, entity_aliases.confidence),
              metadata = COALESCE(entity_aliases.metadata, '{}'::jsonb) || EXCLUDED.metadata,
              last_seen_at = EXCLUDED.last_seen_at
            """,
            tenant_id,
            user_id,
            entity_id,
            alias_text,
            alias_normalized,
            bool(is_primary),
            confidence,
            {
                "resolver_version": ENTITY_RESOLVER_VERSION,
                **(metadata if isinstance(metadata, dict) else {}),
            },
            _utc_now(),
        )

    async def _record_ambiguity(
        self,
        *,
        tenant_id: str,
        user_id: str,
        mention_text: str,
        alias_normalized: str,
        entity_type: str,
        candidate_entity_ids: List[str],
        metadata: Optional[Dict[str, Any]],
    ) -> None:
        await self._write_mutation(
            tenant_id=tenant_id,
            user_id=user_id,
            mutation_type="entity_resolution_ambiguous",
            entity_id=None,
            payload={
                "mention_text": mention_text,
                "alias_normalized": alias_normalized,
                "entity_type": entity_type,
                "candidate_entity_ids": list(candidate_entity_ids or []),
                "resolver_version": ENTITY_RESOLVER_VERSION,
            },
            committed_by="t6.entity_resolver",
            metadata=metadata,
        )

    async def _write_mutation(
        self,
        *,
        tenant_id: str,
        user_id: str,
        mutation_type: str,
        entity_id: Optional[str],
        payload: Dict[str, Any],
        committed_by: str,
        metadata: Optional[Dict[str, Any]],
    ) -> None:
        object_type = "entity" if entity_id else "entity_resolution"
        object_id = str(entity_id) if entity_id else str(payload.get("alias_normalized") or mutation_type)
        await self.mutation_logger.append_mutation(
            tenant_id=tenant_id,
            user_id=user_id,
            mutation_type=mutation_type,
            object_type=object_type,
            object_id=object_id,
            resolver_version=ENTITY_RESOLVER_VERSION,
            entity_id=str(entity_id) if entity_id else None,
            source_run_id=str(metadata.get("source_run_id")) if isinstance(metadata, dict) and metadata.get("source_run_id") else None,
            committed_by=committed_by,
            payload=payload if isinstance(payload, dict) else {},
            metadata=metadata if isinstance(metadata, dict) else {},
        )
