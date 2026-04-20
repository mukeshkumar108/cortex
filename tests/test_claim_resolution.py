from datetime import datetime, timezone as dt_timezone
import os
from uuid import uuid4

import asyncpg
import pytest

from src.claim_resolution import ClaimResolutionError, ClaimResolver
from src.db import Database
from src.main import app
from src.predicate_policy import PredicatePolicyService


def _db_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    password = os.getenv("POSTGRES_PASSWORD", "password")
    return f"postgresql://synapse:{password}@postgres:5432/synapse"


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


async def _create_entity(
    db: Database,
    *,
    tenant_id: str,
    user_id: str,
    canonical_name: str,
    entity_type: str,
) -> str:
    return str(
        await db.fetchval(
            """
            INSERT INTO entities (
                tenant_id, user_id, canonical_name, canonical_name_normalized, entity_type, status, metadata
            )
            VALUES ($1, $2, $3, LOWER($3), $4, 'active', '{}'::jsonb)
            RETURNING entity_id
            """,
            tenant_id,
            user_id,
            canonical_name,
            entity_type,
        )
    )


@pytest.mark.asyncio
async def test_t7_new_active_claim_creation():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    async with app.router.lifespan_context(app):
        db = Database()
        policy_version = await PredicatePolicyService(db).get_current_policy_version()
        subject_entity_id = await _create_entity(
            db,
            tenant_id=tenant,
            user_id=user,
            canonical_name="Ashley",
            entity_type="person",
        )
        await _seed_session_and_turns(db, tenant_id=tenant, user_id=user, session_id=session_id, turn_count=2)
        extract_result_id = await _insert_extract_result(
            db,
            tenant_id=tenant,
            user_id=user,
            session_id=session_id,
            policy_version=policy_version,
            candidates=[
                {
                    "type": "claim_candidate",
                    "predicate": "relationship.status",
                    "subject_entity_id": subject_entity_id,
                    "subject_text": "Ashley",
                    "object_payload": {"value": "dating"},
                    "confidence": 0.91,
                    "truth_confidence": 0.87,
                    "evidence_spans": [{"turn_index": 0, "start": 0, "end": 14}],
                }
            ],
        )
        resolver = ClaimResolver(db)
        result = await resolver.resolve_extract_result_claims(tenant_id=tenant, extract_result_id=extract_result_id)
        claim_row = await db.fetchone(
            """
            SELECT claim_id, claim_slot_key, claim_event_key, lifecycle_status, extraction_confidence, truth_confidence, predicate_policy_version, metadata
            FROM claims
            WHERE tenant_id = $1 AND source_extract_result_id = $2
            LIMIT 1
            """,
            tenant,
            extract_result_id,
        )
        evidence_count = await db.fetchval(
            """
            SELECT COUNT(*)
            FROM claim_evidence
            WHERE tenant_id = $1 AND claim_id = $2
            """,
            tenant,
            int(claim_row["claim_id"]),
        )
        await db.close()

    assert len(result.created_claim_ids) == 1
    assert claim_row is not None
    assert claim_row["claim_slot_key"]
    assert claim_row["claim_event_key"]
    assert claim_row["lifecycle_status"] == "active"
    assert float(claim_row["extraction_confidence"]) == pytest.approx(0.91, rel=0.0, abs=1e-6)
    assert float(claim_row["truth_confidence"]) == pytest.approx(0.87, rel=0.0, abs=1e-6)
    assert claim_row["predicate_policy_version"] == policy_version
    assert (claim_row["metadata"] or {}).get("resolver_version") == result.resolver_version
    assert int(evidence_count or 0) >= 1


@pytest.mark.asyncio
async def test_t7_reinforce_same_slot_equivalent_claim():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    async with app.router.lifespan_context(app):
        db = Database()
        policy_version = await PredicatePolicyService(db).get_current_policy_version()
        subject_entity_id = await _create_entity(
            db,
            tenant_id=tenant,
            user_id=user,
            canonical_name="Ashley",
            entity_type="person",
        )
        await _seed_session_and_turns(db, tenant_id=tenant, user_id=user, session_id=session_id, turn_count=3)
        first_extract_id = await _insert_extract_result(
            db,
            tenant_id=tenant,
            user_id=user,
            session_id=session_id,
            policy_version=policy_version,
            candidates=[
                {
                    "type": "claim_candidate",
                    "predicate": "relationship.status",
                    "subject_entity_id": subject_entity_id,
                    "object_payload": {"value": "dating"},
                    "confidence": 0.90,
                    "evidence_spans": [{"turn_index": 0, "start": 0, "end": 10}],
                }
            ],
        )
        second_extract_id = await _insert_extract_result(
            db,
            tenant_id=tenant,
            user_id=user,
            session_id=session_id,
            policy_version=policy_version,
            candidates=[
                {
                    "type": "claim_candidate",
                    "predicate": "relationship.status",
                    "subject_entity_id": subject_entity_id,
                    "object_payload": {"value": "dating"},
                    "confidence": 0.92,
                    "evidence_spans": [{"turn_index": 1, "start": 0, "end": 12}],
                }
            ],
        )
        resolver = ClaimResolver(db)
        first = await resolver.resolve_extract_result_claims(tenant_id=tenant, extract_result_id=first_extract_id)
        second = await resolver.resolve_extract_result_claims(tenant_id=tenant, extract_result_id=second_extract_id)

        claim_count = await db.fetchval(
            "SELECT COUNT(*) FROM claims WHERE tenant_id = $1 AND user_id = $2",
            tenant,
            user,
        )
        active_claim_id = await db.fetchval(
            """
            SELECT claim_id
            FROM claims
            WHERE tenant_id = $1 AND user_id = $2 AND lifecycle_status = 'active'
            LIMIT 1
            """,
            tenant,
            user,
        )
        evidence_count = await db.fetchval(
            """
            SELECT COUNT(*)
            FROM claim_evidence
            WHERE tenant_id = $1 AND claim_id = $2
            """,
            tenant,
            int(active_claim_id),
        )
        await db.close()

    assert len(first.created_claim_ids) == 1
    assert len(second.reinforced_claim_ids) == 1
    assert int(claim_count or 0) == 1
    assert int(evidence_count or 0) == 2


@pytest.mark.asyncio
async def test_t7_supersede_conflicting_exclusive_slot_claim():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    async with app.router.lifespan_context(app):
        db = Database()
        policy_version = await PredicatePolicyService(db).get_current_policy_version()
        subject_entity_id = await _create_entity(
            db,
            tenant_id=tenant,
            user_id=user,
            canonical_name="Ashley",
            entity_type="person",
        )
        await _seed_session_and_turns(db, tenant_id=tenant, user_id=user, session_id=session_id, turn_count=3)
        first_extract_id = await _insert_extract_result(
            db,
            tenant_id=tenant,
            user_id=user,
            session_id=session_id,
            policy_version=policy_version,
            candidates=[
                {
                    "type": "claim_candidate",
                    "predicate": "relationship.status",
                    "subject_entity_id": subject_entity_id,
                    "object_payload": {"value": "dating"},
                    "confidence": 0.9,
                    "evidence_spans": [{"turn_index": 0, "start": 0, "end": 10}],
                }
            ],
        )
        second_extract_id = await _insert_extract_result(
            db,
            tenant_id=tenant,
            user_id=user,
            session_id=session_id,
            policy_version=policy_version,
            candidates=[
                {
                    "type": "claim_candidate",
                    "predicate": "relationship.status",
                    "subject_entity_id": subject_entity_id,
                    "object_payload": {"value": "single"},
                    "confidence": 0.93,
                    "evidence_spans": [{"turn_index": 1, "start": 0, "end": 9}],
                }
            ],
        )
        resolver = ClaimResolver(db)
        first = await resolver.resolve_extract_result_claims(tenant_id=tenant, extract_result_id=first_extract_id)
        second = await resolver.resolve_extract_result_claims(tenant_id=tenant, extract_result_id=second_extract_id)
        active_count = await db.fetchval(
            """
            SELECT COUNT(*)
            FROM claims
            WHERE tenant_id = $1 AND user_id = $2 AND lifecycle_status = 'active'
            """,
            tenant,
            user,
        )
        superseded_row = await db.fetchone(
            """
            SELECT claim_id, superseded_by_claim_id
            FROM claims
            WHERE tenant_id = $1 AND user_id = $2 AND lifecycle_status = 'superseded'
            LIMIT 1
            """,
            tenant,
            user,
        )
        await db.close()

    assert len(first.created_claim_ids) == 1
    assert len(second.created_claim_ids) == 1
    assert len(second.superseded_claim_ids) == 1
    assert int(active_count or 0) == 1
    assert superseded_row is not None
    assert int(superseded_row["superseded_by_claim_id"]) == second.created_claim_ids[0]


@pytest.mark.asyncio
async def test_t7_reject_claim_with_no_evidence():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    async with app.router.lifespan_context(app):
        db = Database()
        policy_version = await PredicatePolicyService(db).get_current_policy_version()
        subject_entity_id = await _create_entity(
            db,
            tenant_id=tenant,
            user_id=user,
            canonical_name="Ashley",
            entity_type="person",
        )
        await _seed_session_and_turns(db, tenant_id=tenant, user_id=user, session_id=session_id, turn_count=1)
        extract_result_id = await _insert_extract_result(
            db,
            tenant_id=tenant,
            user_id=user,
            session_id=session_id,
            policy_version=policy_version,
            candidates=[
                {
                    "type": "claim_candidate",
                    "predicate": "relationship.status",
                    "subject_entity_id": subject_entity_id,
                    "object_payload": {"value": "dating"},
                    "confidence": 0.9,
                    "evidence_spans": [],
                }
            ],
        )
        resolver = ClaimResolver(db)
        result = await resolver.resolve_extract_result_claims(tenant_id=tenant, extract_result_id=extract_result_id)
        claim_count = await db.fetchval(
            "SELECT COUNT(*) FROM claims WHERE tenant_id = $1 AND user_id = $2",
            tenant,
            user,
        )
        await db.close()

    assert len(result.created_claim_ids) == 0
    assert any(r.reason_code == "missing_evidence" for r in result.rejected_candidates)
    assert int(claim_count or 0) == 0


@pytest.mark.asyncio
async def test_t7_reject_unknown_policy_version():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    async with app.router.lifespan_context(app):
        db = Database()
        policy_version = await PredicatePolicyService(db).get_current_policy_version()
        subject_entity_id = await _create_entity(
            db,
            tenant_id=tenant,
            user_id=user,
            canonical_name="Ashley",
            entity_type="person",
        )
        await _seed_session_and_turns(db, tenant_id=tenant, user_id=user, session_id=session_id, turn_count=1)
        extract_result_id = await _insert_extract_result(
            db,
            tenant_id=tenant,
            user_id=user,
            session_id=session_id,
            policy_version=policy_version,
            candidates=[
                {
                    "type": "claim_candidate",
                    "predicate": "relationship.status",
                    "subject_entity_id": subject_entity_id,
                    "object_payload": {"value": "dating"},
                    "confidence": 0.9,
                    "evidence_spans": [{"turn_index": 0, "start": 0, "end": 10}],
                }
            ],
        )
        resolver = ClaimResolver(db)
        with pytest.raises(ClaimResolutionError) as exc:
            await resolver.resolve_extract_result_claims(
                tenant_id=tenant,
                extract_result_id=extract_result_id,
                policy_version_override="v2.unknown",
            )
        await db.close()

    assert exc.value.code == "CLAIM_RESOLUTION_UNKNOWN_POLICY_VERSION"


@pytest.mark.asyncio
async def test_t7_reject_assistant_authored_claim_by_default():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    async with app.router.lifespan_context(app):
        db = Database()
        policy_version = await PredicatePolicyService(db).get_current_policy_version()
        subject_entity_id = await _create_entity(
            db,
            tenant_id=tenant,
            user_id=user,
            canonical_name="Ashley",
            entity_type="person",
        )
        await _seed_session_and_turns(db, tenant_id=tenant, user_id=user, session_id=session_id, turn_count=1)
        extract_result_id = await _insert_extract_result(
            db,
            tenant_id=tenant,
            user_id=user,
            session_id=session_id,
            policy_version=policy_version,
            candidates=[
                {
                    "type": "claim_candidate",
                    "predicate": "relationship.status",
                    "subject_entity_id": subject_entity_id,
                    "object_payload": {"value": "dating"},
                    "source_role": "assistant",
                    "confidence": 0.9,
                    "evidence_spans": [{"turn_index": 0, "start": 0, "end": 10}],
                }
            ],
        )
        resolver = ClaimResolver(db)
        result = await resolver.resolve_extract_result_claims(tenant_id=tenant, extract_result_id=extract_result_id)
        claim_count = await db.fetchval(
            "SELECT COUNT(*) FROM claims WHERE tenant_id = $1 AND user_id = $2",
            tenant,
            user,
        )
        await db.close()

    assert len(result.created_claim_ids) == 0
    assert any(r.reason_code == "assistant_authored_disallowed" for r in result.rejected_candidates)
    assert int(claim_count or 0) == 0


@pytest.mark.asyncio
async def test_t7_deterministic_repeat_execution_behavior():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    async with app.router.lifespan_context(app):
        db = Database()
        policy_version = await PredicatePolicyService(db).get_current_policy_version()
        subject_entity_id = await _create_entity(
            db,
            tenant_id=tenant,
            user_id=user,
            canonical_name="Ashley",
            entity_type="person",
        )
        await _seed_session_and_turns(db, tenant_id=tenant, user_id=user, session_id=session_id, turn_count=1)
        extract_result_id = await _insert_extract_result(
            db,
            tenant_id=tenant,
            user_id=user,
            session_id=session_id,
            policy_version=policy_version,
            candidates=[
                {
                    "type": "claim_candidate",
                    "predicate": "relationship.status",
                    "subject_entity_id": subject_entity_id,
                    "object_payload": {"value": "dating"},
                    "confidence": 0.9,
                    "evidence_spans": [{"turn_index": 0, "start": 0, "end": 10}],
                }
            ],
        )
        resolver = ClaimResolver(db)
        first = await resolver.resolve_extract_result_claims(tenant_id=tenant, extract_result_id=extract_result_id)
        second = await resolver.resolve_extract_result_claims(tenant_id=tenant, extract_result_id=extract_result_id)
        claim_count = await db.fetchval(
            "SELECT COUNT(*) FROM claims WHERE tenant_id = $1 AND user_id = $2",
            tenant,
            user,
        )
        claim_id = await db.fetchval(
            """
            SELECT claim_id
            FROM claims
            WHERE tenant_id = $1 AND user_id = $2
            LIMIT 1
            """,
            tenant,
            user,
        )
        evidence_count = await db.fetchval(
            """
            SELECT COUNT(*)
            FROM claim_evidence
            WHERE tenant_id = $1 AND claim_id = $2
            """,
            tenant,
            int(claim_id),
        )
        await db.close()

    assert len(first.created_claim_ids) == 1
    assert len(second.created_claim_ids) == 0
    assert int(claim_count or 0) == 1
    assert int(evidence_count or 0) == 1


@pytest.mark.asyncio
async def test_t7_unsupported_predicate_is_rejected_fail_closed():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    async with app.router.lifespan_context(app):
        db = Database()
        policy_version = await PredicatePolicyService(db).get_current_policy_version()
        subject_entity_id = await _create_entity(
            db,
            tenant_id=tenant,
            user_id=user,
            canonical_name="Ashley",
            entity_type="person",
        )
        await _seed_session_and_turns(db, tenant_id=tenant, user_id=user, session_id=session_id, turn_count=1)
        extract_result_id = await _insert_extract_result(
            db,
            tenant_id=tenant,
            user_id=user,
            session_id=session_id,
            policy_version=policy_version,
            candidates=[
                {
                    "type": "claim_candidate",
                    "predicate": "unsupported.predicate",
                    "subject_entity_id": subject_entity_id,
                    "object_payload": {"value": "x"},
                    "confidence": 0.9,
                    "evidence_spans": [{"turn_index": 0, "start": 0, "end": 10}],
                }
            ],
        )
        resolver = ClaimResolver(db)
        result = await resolver.resolve_extract_result_claims(tenant_id=tenant, extract_result_id=extract_result_id)
        claim_count = await db.fetchval(
            "SELECT COUNT(*) FROM claims WHERE tenant_id = $1 AND user_id = $2",
            tenant,
            user,
        )
        await db.close()

    assert len(result.created_claim_ids) == 0
    assert any(r.reason_code == "unsupported_predicate" for r in result.rejected_candidates)
    assert int(claim_count or 0) == 0
