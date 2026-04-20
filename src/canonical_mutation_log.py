from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone as dt_timezone
from typing import Any, Dict, List, Optional

from .db import Database


COMMIT_STATUS_COMMITTED = "committed"


def _utc_now() -> datetime:
    return datetime.utcnow().replace(tzinfo=dt_timezone.utc)


@dataclass(frozen=True)
class CanonicalMutationRecord:
    mutation_id: int
    tenant_id: str
    tenant_sequence: int
    commit_status: str


class CanonicalMutationLogger:
    """
    T8 contract:
    - Watermark model: per-tenant monotonic sequence (`tenant_sequence`).
    - Downstream readers consume only rows where commit_status='committed'.
    """

    def __init__(self, db: Database):
        self.db = db

    async def append_mutation(
        self,
        *,
        tenant_id: str,
        mutation_type: str,
        object_type: str,
        object_id: str,
        resolver_version: str,
        user_id: Optional[str] = None,
        claim_id: Optional[int] = None,
        entity_id: Optional[str] = None,
        source_extract_result_id: Optional[int] = None,
        source_run_id: Optional[str] = None,
        committed_by: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        conn: Any = None,
    ) -> CanonicalMutationRecord:
        if conn is None:
            pool = await self.db.get_pool()
            async with pool.acquire() as pooled_conn:
                async with pooled_conn.transaction():
                    return await self._append_with_conn(
                        conn=pooled_conn,
                        tenant_id=tenant_id,
                        mutation_type=mutation_type,
                        object_type=object_type,
                        object_id=object_id,
                        resolver_version=resolver_version,
                        user_id=user_id,
                        claim_id=claim_id,
                        entity_id=entity_id,
                        source_extract_result_id=source_extract_result_id,
                        source_run_id=source_run_id,
                        committed_by=committed_by,
                        payload=payload,
                        metadata=metadata,
                    )
        return await self._append_with_conn(
            conn=conn,
            tenant_id=tenant_id,
            mutation_type=mutation_type,
            object_type=object_type,
            object_id=object_id,
            resolver_version=resolver_version,
            user_id=user_id,
            claim_id=claim_id,
            entity_id=entity_id,
            source_extract_result_id=source_extract_result_id,
            source_run_id=source_run_id,
            committed_by=committed_by,
            payload=payload,
            metadata=metadata,
        )

    async def _append_with_conn(
        self,
        *,
        conn: Any,
        tenant_id: str,
        mutation_type: str,
        object_type: str,
        object_id: str,
        resolver_version: str,
        user_id: Optional[str],
        claim_id: Optional[int],
        entity_id: Optional[str],
        source_extract_result_id: Optional[int],
        source_run_id: Optional[str],
        committed_by: Optional[str],
        payload: Optional[Dict[str, Any]],
        metadata: Optional[Dict[str, Any]],
    ) -> CanonicalMutationRecord:
        now = _utc_now()
        tenant_sequence = await conn.fetchval(
            """
            INSERT INTO canonical_tenant_watermarks (tenant_id, last_sequence, updated_at)
            VALUES ($1, 1, $2)
            ON CONFLICT (tenant_id)
            DO UPDATE SET
              last_sequence = canonical_tenant_watermarks.last_sequence + 1,
              updated_at = EXCLUDED.updated_at
            RETURNING last_sequence
            """,
            tenant_id,
            now,
        )
        row = await conn.fetchrow(
            """
            INSERT INTO canonical_mutations (
                tenant_id,
                user_id,
                mutation_type,
                object_type,
                object_id,
                claim_id,
                entity_id,
                source_extract_result_id,
                source_run_id,
                resolver_version,
                tenant_sequence,
                commit_status,
                payload,
                committed_at,
                committed_by,
                metadata
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7::uuid, $8, $9, $10, $11, $12, $13::jsonb, $14, $15, $16::jsonb
            )
            RETURNING mutation_id, tenant_id, tenant_sequence, commit_status
            """,
            tenant_id,
            user_id,
            mutation_type,
            object_type,
            object_id,
            claim_id,
            entity_id,
            source_extract_result_id,
            source_run_id,
            resolver_version,
            int(tenant_sequence),
            COMMIT_STATUS_COMMITTED,
            payload if isinstance(payload, dict) else {},
            now,
            committed_by,
            metadata if isinstance(metadata, dict) else {},
        )
        return CanonicalMutationRecord(
            mutation_id=int(row["mutation_id"]),
            tenant_id=str(row["tenant_id"]),
            tenant_sequence=int(row["tenant_sequence"]),
            commit_status=str(row["commit_status"]),
        )

    async def get_tenant_watermark(self, *, tenant_id: str) -> int:
        value = await self.db.fetchval(
            """
            SELECT last_sequence
            FROM canonical_tenant_watermarks
            WHERE tenant_id = $1
            LIMIT 1
            """,
            tenant_id,
        )
        return int(value or 0)

    async def read_committed_mutations(
        self,
        *,
        tenant_id: str,
        after_sequence: int = 0,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        rows = await self.db.fetch(
            """
            SELECT
              mutation_id,
              tenant_id,
              user_id,
              tenant_sequence,
              mutation_type,
              object_type,
              object_id,
              source_run_id,
              resolver_version,
              committed_at,
              commit_status,
              payload,
              metadata
            FROM canonical_mutations
            WHERE tenant_id = $1
              AND commit_status = $2
              AND tenant_sequence > $3
            ORDER BY tenant_sequence ASC
            LIMIT $4
            """,
            tenant_id,
            COMMIT_STATUS_COMMITTED,
            int(after_sequence or 0),
            max(1, min(int(limit or 100), 1000)),
        )
        return rows or []
