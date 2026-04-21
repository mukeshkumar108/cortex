from datetime import datetime, timedelta
from uuid import uuid4

import pytest

from src.main import app, _build_minimal_claim_candidate_payload, _execute_post_ingest_hook
from src import session as session_module


@pytest.mark.asyncio
async def test_canonical_live_minimal_extractor_emits_claim_candidates():
    payload = _build_minimal_claim_candidate_payload(
        user_id="u1",
        messages=[
            {"role": "user", "text": "I'm dating Ashley and I moved to Berlin", "timestamp": "2026-04-20T10:00:00Z"},
            {"role": "assistant", "text": "thanks", "timestamp": "2026-04-20T10:00:05Z"},
        ],
    )
    candidates = payload.get("candidates") or []
    predicates = {str(c.get("predicate")) for c in candidates if isinstance(c, dict)}
    assert payload.get("schema_version") == "t4.extract_results.v1"
    assert "relationship.status" in predicates
    assert "user.location" in predicates


@pytest.mark.asyncio
async def test_canonical_live_hook_writes_entities_claims_evidence_and_mutations():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    t0 = datetime.utcnow().replace(microsecond=0)
    t1 = t0 + timedelta(minutes=5)

    async with app.router.lifespan_context(app):
        db = session_module._manager.db
        await db.execute(
            """
            INSERT INTO sessions_v2 (tenant_id, session_id, user_id, started_at, ended_at, status, source, metadata, updated_at)
            VALUES ($1, $2, $3, $4, $5, 'open', 'test', '{}'::jsonb, NOW())
            ON CONFLICT (tenant_id, session_id) DO NOTHING
            """,
            tenant,
            session_id,
            user,
            t0,
            t1,
        )
        await db.execute(
            """
            INSERT INTO turns_v2 (tenant_id, session_id, user_id, turn_index, role, content, occurred_at, metadata)
            VALUES
              ($1, $2, $3, 0, 'user', 'I am dating Ashley.', $4, '{}'::jsonb),
              ($1, $2, $3, 1, 'user', 'I broke up with Ashley.', $5, '{}'::jsonb)
            ON CONFLICT (tenant_id, session_id, turn_index) DO NOTHING
            """,
            tenant,
            session_id,
            user,
            t0,
            t1,
        )
        await db.execute(
            """
            INSERT INTO session_transcript (tenant_id, user_id, session_id, messages, created_at, updated_at)
            VALUES ($1, $2, $3, $4::jsonb, NOW(), NOW())
            ON CONFLICT (tenant_id, session_id)
            DO UPDATE SET messages = EXCLUDED.messages, updated_at = NOW()
            """,
            tenant,
            user,
            session_id,
            [
                {"role": "user", "text": "I am dating Ashley.", "timestamp": t0.isoformat() + "Z"},
                {"role": "user", "text": "I broke up with Ashley.", "timestamp": t1.isoformat() + "Z"},
            ],
        )

        ok = await _execute_post_ingest_hook(
            session_module.POST_INGEST_HOOK_EXTRACT_RESULTS,
            {
                "tenant_id": tenant,
                "user_id": user,
                "session_id": session_id,
                "reference_time": t1.isoformat() + "Z",
            },
        )
        assert ok is True

        entity_count = await db.fetchval(
            """
            SELECT COUNT(*)::int
            FROM entities
            WHERE tenant_id = $1 AND user_id = $2 AND status = 'active'
            """,
            tenant,
            user,
        )
        assert int(entity_count or 0) == 1

        claim_rows = await db.fetch(
            """
            SELECT claim_id, lifecycle_status, predicate
            FROM claims
            WHERE tenant_id = $1 AND user_id = $2
            ORDER BY claim_id ASC
            """,
            tenant,
            user,
        )
        assert len(claim_rows or []) >= 2
        statuses = {str(r.get("lifecycle_status")) for r in (claim_rows or [])}
        assert "active" in statuses
        assert "superseded" in statuses

        evidence_count = await db.fetchval(
            """
            SELECT COUNT(*)::int
            FROM claim_evidence ce
            JOIN claims c
              ON c.tenant_id = ce.tenant_id
             AND c.claim_id = ce.claim_id
            WHERE ce.tenant_id = $1
              AND c.user_id = $2
            """,
            tenant,
            user,
        )
        assert int(evidence_count or 0) >= 2

        mutation_count = await db.fetchval(
            """
            SELECT COUNT(*)::int
            FROM canonical_mutations
            WHERE tenant_id = $1
              AND user_id = $2
            """,
            tenant,
            user,
        )
        assert int(mutation_count or 0) >= 3
