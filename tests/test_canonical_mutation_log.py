from datetime import datetime, timezone as dt_timezone
from uuid import uuid4

import pytest

from src.canonical_mutation_log import CanonicalMutationLogger
from src.claim_resolution import ClaimResolver
from src.db import Database
from src.entity_resolution import EntityResolver
from src.main import app
from src.predicate_policy import PredicatePolicyService


async def _seed_session_and_turns(
    db: Database,
    *,
    tenant_id: str,
    user_id: str,
    session_id: str,
    turn_count: int = 2,
) -> None:
    now = datetime.utcnow().replace(tzinfo=dt_timezone.utc)
    await db.execute(
        """
        INSERT INTO sessions_v2 (
            tenant_id, session_id, user_id, started_at, ended_at, status, source, metadata, updated_at
        )
        VALUES ($1, $2, $3, $4, $4, 'open', 'test', '{}'::jsonb, $4)
        ON CONFLICT (tenant_id, session_id)
        DO UPDATE SET updated_at = EXCLUDED.updated_at
        """,
        tenant_id,
        session_id,
        user_id,
        now,
    )
    for i in range(turn_count):
        await db.execute(
            """
            INSERT INTO turns_v2 (
                tenant_id, session_id, user_id, turn_index, role, content, occurred_at, metadata
            )
            VALUES ($1, $2, $3, $4, 'user', $5, $6, '{}'::jsonb)
            ON CONFLICT DO NOTHING
            """,
            tenant_id,
            session_id,
            user_id,
            i,
            f"turn-{i}",
            now,
        )


async def _insert_extract_result(
    db: Database,
    *,
    tenant_id: str,
    user_id: str,
    session_id: str,
    policy_version: str,
    candidates: list[dict],
) -> int:
    now = datetime.utcnow().replace(tzinfo=dt_timezone.utc)
    return int(
        await db.fetchval(
            """
            INSERT INTO extract_results (
                tenant_id,
                user_id,
                session_id,
                model_version,
                prompt_version,
                predicate_policy_version,
                status,
                candidates,
                started_at,
                completed_at
            )
            VALUES ($1, $2, $3, 't4-extractor-v1', 't4-prompt-v1', $4, 'succeeded', $5::jsonb, $6, $6)
            RETURNING extract_result_id
            """,
            tenant_id,
            user_id,
            session_id,
            policy_version,
            {"schema_version": "t4.extract_results.v1", "candidates": candidates},
            now,
        )
    )


@pytest.mark.asyncio
async def test_t8_entity_mutation_records_written():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"

    async with app.router.lifespan_context(app):
        db = Database()
        resolver = EntityResolver(db)
        result = await resolver.resolve_entity_mention(
            tenant_id=tenant,
            user_id=user,
            mention_text="Project Orion",
            entity_type="project",
        )
        row = await db.fetchone(
            """
            SELECT tenant_id, user_id, mutation_type, object_type, object_id, resolver_version, tenant_sequence, commit_status
            FROM canonical_mutations
            WHERE tenant_id = $1 AND mutation_type = 'entity_create'
            ORDER BY tenant_sequence DESC
            LIMIT 1
            """,
            tenant,
        )
        await db.close()

    assert row is not None
    assert row["tenant_id"] == tenant
    assert row["user_id"] == user
    assert row["mutation_type"] == "entity_create"
    assert row["object_type"] == "entity"
    assert row["object_id"] == result.resolved_entity_id
    assert row["resolver_version"] == result.resolver_version
    assert int(row["tenant_sequence"]) >= 1
    assert row["commit_status"] == "committed"


@pytest.mark.asyncio
async def test_t8_claim_lifecycle_mutation_records_written():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"

    async with app.router.lifespan_context(app):
        db = Database()
        policy_version = await PredicatePolicyService(db).get_current_policy_version()
        entity_id = await db.fetchval(
            """
            INSERT INTO entities (
                tenant_id, user_id, canonical_name, canonical_name_normalized, entity_type, status, metadata
            )
            VALUES ($1, $2, 'Ashley', 'ashley', 'person', 'active', '{}'::jsonb)
            RETURNING entity_id
            """,
            tenant,
            user,
        )
        await _seed_session_and_turns(db, tenant_id=tenant, user_id=user, session_id=session_id, turn_count=3)
        first_extract = await _insert_extract_result(
            db,
            tenant_id=tenant,
            user_id=user,
            session_id=session_id,
            policy_version=policy_version,
            candidates=[
                {
                    "type": "claim_candidate",
                    "predicate": "relationship.status",
                    "subject_entity_id": str(entity_id),
                    "object_payload": {"value": "dating"},
                    "confidence": 0.9,
                    "evidence_spans": [{"turn_index": 0, "start": 0, "end": 8}],
                }
            ],
        )
        second_extract = await _insert_extract_result(
            db,
            tenant_id=tenant,
            user_id=user,
            session_id=session_id,
            policy_version=policy_version,
            candidates=[
                {
                    "type": "claim_candidate",
                    "predicate": "relationship.status",
                    "subject_entity_id": str(entity_id),
                    "object_payload": {"value": "single"},
                    "confidence": 0.93,
                    "evidence_spans": [{"turn_index": 1, "start": 0, "end": 6}],
                }
            ],
        )
        resolver = ClaimResolver(db)
        await resolver.resolve_extract_result_claims(tenant_id=tenant, extract_result_id=first_extract)
        await resolver.resolve_extract_result_claims(tenant_id=tenant, extract_result_id=second_extract)

        rows = await db.fetch(
            """
            SELECT mutation_type, object_type, commit_status, tenant_sequence
            FROM canonical_mutations
            WHERE tenant_id = $1
              AND object_type = 'claim'
            ORDER BY tenant_sequence ASC
            """,
            tenant,
        )
        await db.close()

    mutation_types = [str(r["mutation_type"]) for r in rows]
    assert "claim_created" in mutation_types
    assert "claim_superseded" in mutation_types
    assert all(str(r["object_type"]) == "claim" for r in rows)
    assert all(str(r["commit_status"]) == "committed" for r in rows)


@pytest.mark.asyncio
async def test_t8_ordering_deterministic_for_repeated_runs():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"

    async with app.router.lifespan_context(app):
        db = Database()
        policy_version = await PredicatePolicyService(db).get_current_policy_version()
        entity_id = await db.fetchval(
            """
            INSERT INTO entities (
                tenant_id, user_id, canonical_name, canonical_name_normalized, entity_type, status, metadata
            )
            VALUES ($1, $2, 'Ashley', 'ashley', 'person', 'active', '{}'::jsonb)
            RETURNING entity_id
            """,
            tenant,
            user,
        )
        await _seed_session_and_turns(db, tenant_id=tenant, user_id=user, session_id=session_id, turn_count=2)
        extract_result = await _insert_extract_result(
            db,
            tenant_id=tenant,
            user_id=user,
            session_id=session_id,
            policy_version=policy_version,
            candidates=[
                {
                    "type": "claim_candidate",
                    "predicate": "relationship.status",
                    "subject_entity_id": str(entity_id),
                    "object_payload": {"value": "dating"},
                    "confidence": 0.9,
                    "evidence_spans": [{"turn_index": 0, "start": 0, "end": 8}],
                }
            ],
        )
        resolver = ClaimResolver(db)
        await resolver.resolve_extract_result_claims(tenant_id=tenant, extract_result_id=extract_result)
        await resolver.resolve_extract_result_claims(tenant_id=tenant, extract_result_id=extract_result)

        rows = await db.fetch(
            """
            SELECT mutation_type, tenant_sequence
            FROM canonical_mutations
            WHERE tenant_id = $1
            ORDER BY tenant_sequence ASC
            """,
            tenant,
        )
        await db.close()

    assert [int(r["tenant_sequence"]) for r in rows] == [1, 2]
    assert [str(r["mutation_type"]) for r in rows] == ["claim_created", "claim_reinforced"]


@pytest.mark.asyncio
async def test_t8_per_tenant_watermark_semantics():
    tenant_a = f"tenant-{uuid4().hex}"
    tenant_b = f"tenant-{uuid4().hex}"

    async with app.router.lifespan_context(app):
        db = Database()
        logger = CanonicalMutationLogger(db)
        await logger.append_mutation(
            tenant_id=tenant_a,
            user_id="u1",
            mutation_type="entity_create",
            object_type="entity",
            object_id="obj-a1",
            resolver_version="test.v1",
        )
        await logger.append_mutation(
            tenant_id=tenant_a,
            user_id="u1",
            mutation_type="entity_create",
            object_type="entity",
            object_id="obj-a2",
            resolver_version="test.v1",
        )
        await logger.append_mutation(
            tenant_id=tenant_b,
            user_id="u2",
            mutation_type="entity_create",
            object_type="entity",
            object_id="obj-b1",
            resolver_version="test.v1",
        )
        watermark_a = await logger.get_tenant_watermark(tenant_id=tenant_a)
        watermark_b = await logger.get_tenant_watermark(tenant_id=tenant_b)
        rows_a = await logger.read_committed_mutations(tenant_id=tenant_a, after_sequence=0, limit=10)
        rows_b = await logger.read_committed_mutations(tenant_id=tenant_b, after_sequence=0, limit=10)
        await db.close()

    assert watermark_a == 2
    assert watermark_b == 1
    assert [int(r["tenant_sequence"]) for r in rows_a] == [1, 2]
    assert [int(r["tenant_sequence"]) for r in rows_b] == [1]
