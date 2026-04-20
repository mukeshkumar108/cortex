from datetime import datetime, timezone as dt_timezone
from pathlib import Path
from uuid import uuid4

import pytest

from src.db import Database
from src.main import app
from src.predicate_policy import PredicatePolicyService
from src.replay_audit import OfflineReplayHarness, ReplayRunConfig, load_golden_corpus


async def _seed_session_and_turns(
    db: Database,
    *,
    tenant_id: str,
    user_id: str,
    session_id: str,
    turn_count: int = 3,
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
async def test_t12a_deterministic_replay_state_hash():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    async with app.router.lifespan_context(app):
        db = Database()
        policy_version = await PredicatePolicyService(db).get_current_policy_version()
        await _seed_session_and_turns(db, tenant_id=tenant, user_id=user, session_id=session_id, turn_count=2)
        extract_id = await _insert_extract_result(
            db,
            tenant_id=tenant,
            user_id=user,
            session_id=session_id,
            policy_version=policy_version,
            candidates=[
                {
                    "type": "claim_candidate",
                    "predicate": "relationship.status",
                    "subject_text": "Ashley",
                    "subject_entity_type": "person",
                    "object_payload": {"value": "dating"},
                    "confidence": 0.9,
                    "evidence_spans": [{"turn_index": 0, "start": 0, "end": 8}],
                }
            ],
        )
        harness = OfflineReplayHarness(db)
        config = ReplayRunConfig(
            tenant_id=tenant,
            extract_result_ids=[extract_id],
            policy_version=policy_version,
            reset_before_run=True,
        )
        first = await harness.run_replay(config)
        second = await harness.run_replay(config)
        await db.close()

    assert first.errors == []
    assert second.errors == []
    assert first.state_hash == second.state_hash


@pytest.mark.asyncio
async def test_t12a_diff_report_generation():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    async with app.router.lifespan_context(app):
        db = Database()
        policy_version = await PredicatePolicyService(db).get_current_policy_version()
        await _seed_session_and_turns(db, tenant_id=tenant, user_id=user, session_id=session_id, turn_count=3)
        extract_a = await _insert_extract_result(
            db,
            tenant_id=tenant,
            user_id=user,
            session_id=session_id,
            policy_version=policy_version,
            candidates=[
                {
                    "type": "claim_candidate",
                    "predicate": "relationship.status",
                    "subject_text": "Ashley",
                    "subject_entity_type": "person",
                    "object_payload": {"value": "dating"},
                    "confidence": 0.9,
                    "evidence_spans": [{"turn_index": 0, "start": 0, "end": 8}],
                }
            ],
        )
        extract_b = await _insert_extract_result(
            db,
            tenant_id=tenant,
            user_id=user,
            session_id=session_id,
            policy_version=policy_version,
            candidates=[
                {
                    "type": "claim_candidate",
                    "predicate": "relationship.status",
                    "subject_text": "Ashley",
                    "subject_entity_type": "person",
                    "object_payload": {"value": "single"},
                    "confidence": 0.93,
                    "evidence_spans": [{"turn_index": 1, "start": 0, "end": 6}],
                }
            ],
        )
        harness = OfflineReplayHarness(db)
        baseline = await harness.run_replay(
            ReplayRunConfig(
                tenant_id=tenant,
                extract_result_ids=[extract_a],
                policy_version=policy_version,
                reset_before_run=True,
            )
        )
        candidate = await harness.run_replay(
            ReplayRunConfig(
                tenant_id=tenant,
                extract_result_ids=[extract_a, extract_b],
                policy_version=policy_version,
                reset_before_run=True,
            )
        )
        diff = harness.build_diff_report(baseline=baseline.snapshot, candidate=candidate.snapshot)
        await db.close()

    assert "claim_creation" in diff
    assert "claim_supersession" in diff
    assert "claim_retraction" in diff
    assert "evidence_linkage_differences" in diff
    assert "mutation_ordering_differences" in diff
    assert len(diff["claim_creation"]) >= 1
    assert len(diff["claim_supersession"]) >= 1


@pytest.mark.asyncio
async def test_t12a_repeat_run_equality_for_canonical_outputs():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    async with app.router.lifespan_context(app):
        db = Database()
        policy_version = await PredicatePolicyService(db).get_current_policy_version()
        await _seed_session_and_turns(db, tenant_id=tenant, user_id=user, session_id=session_id, turn_count=2)
        extract_id = await _insert_extract_result(
            db,
            tenant_id=tenant,
            user_id=user,
            session_id=session_id,
            policy_version=policy_version,
            candidates=[
                {
                    "type": "claim_candidate",
                    "predicate": "relationship.status",
                    "subject_text": "Ashley",
                    "subject_entity_type": "person",
                    "object_payload": {"value": "dating"},
                    "confidence": 0.91,
                    "evidence_spans": [{"turn_index": 0, "start": 0, "end": 8}],
                }
            ],
        )
        harness = OfflineReplayHarness(db)
        one = await harness.run_replay(
            ReplayRunConfig(
                tenant_id=tenant,
                extract_result_ids=[extract_id],
                policy_version=policy_version,
                reset_before_run=True,
            )
        )
        two = await harness.run_replay(
            ReplayRunConfig(
                tenant_id=tenant,
                extract_result_ids=[extract_id],
                policy_version=policy_version,
                reset_before_run=True,
            )
        )
        diff = harness.build_diff_report(baseline=one.snapshot, candidate=two.snapshot)
        await db.close()

    assert one.state_hash == two.state_hash
    assert diff["states_equal"] is True
    assert diff["claim_creation"] == []
    assert diff["evidence_linkage_differences"]["added"] == []
    assert diff["evidence_linkage_differences"]["removed"] == []


def test_t12a_golden_corpus_loader_sample_fixture():
    fixture = Path(__file__).resolve().parent / "fixtures" / "t12a_golden_set.sample.json"
    loaded = load_golden_corpus(str(fixture))
    assert loaded["schema_version"] == "t12a.golden.v1"
    assert isinstance(loaded.get("cases"), list)
    assert len(loaded["cases"]) >= 1
