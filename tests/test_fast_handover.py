from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from src.main import app, db
from src.fast_handover import build_fast_handover_packet, get_latest_fast_handover_packet


@pytest.mark.asyncio
async def test_build_fast_handover_packet_extracts_questions_actions_and_state():
    now = datetime(2026, 5, 19, 10, 0, tzinfo=timezone.utc)
    packet = build_fast_handover_packet(
        tenant_id="default",
        user_id="u1",
        session_id="s1",
        created_at=now,
        messages=[
            {"role": "user", "text": "Ashley called in sick. I have to handle the event takedown myself.", "timestamp": now.isoformat()},
            {"role": "user", "text": "Should I follow up with her tomorrow morning?", "timestamp": now.isoformat()},
            {"role": "user", "text": "I am pretty overwhelmed and tired right now.", "timestamp": now.isoformat()},
        ],
    )

    assert packet is not None
    assert "Open question" in packet["summary"] or "Pending decision" in packet["summary"]
    assert packet["open_questions"] == ["Should I follow up with her tomorrow morning?"]
    assert any("follow up with her tomorrow morning" in item.lower() for item in packet["pending_actions"])
    assert packet["recent_state_note"] == "I am pretty overwhelmed and tired right now."
    assert "Ashley" in packet["important_people"]
    assert "Fast handover only; do not treat this as durable profile or identity." in packet["do_not_overdo"]


@pytest.mark.asyncio
async def test_session_ingest_creates_handover_packet():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime(2026, 5, 19, 10, 0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(
                "/session/ingest",
                json={
                    "tenantId": tenant,
                    "userId": user,
                    "sessionId": session_id,
                    "startedAt": now,
                    "endedAt": now,
                    "messages": [
                        {"role": "user", "text": "I still need to chase Acme about invoice #102.", "timestamp": now},
                        {"role": "user", "text": "Should I draft the email today?", "timestamp": now},
                    ],
                },
            )
            assert response.status_code == 200
        row = await db.fetchone(
            """
            SELECT summary, pending_actions, open_questions, important_people, status
            FROM session_handover_packets
            WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
            """,
            tenant,
            user,
            session_id,
        )
    assert row is not None
    assert any("chase Acme about invoice #102" in item for item in row["pending_actions"])
    assert row["open_questions"] == ["Should I draft the email today?"]
    assert "Acme" in row["important_people"]
    assert row["status"] == "active"


@pytest.mark.asyncio
async def test_latest_non_expired_packet_is_returned_and_expired_hidden_by_default():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    now = datetime.now(timezone.utc)

    async with app.router.lifespan_context(app):
        await db.execute(
            """
            INSERT INTO session_handover_packets (
                tenant_id, user_id, session_id, summary, open_questions, unresolved_decisions,
                pending_actions, recent_state_note, important_people, active_topics, do_not_overdo,
                created_at, expires_at, source_turn_refs, status
            )
            VALUES
            ($1,$2,'old-expired','Expired packet','[]'::jsonb,'[]'::jsonb,'[]'::jsonb,NULL,'[]'::jsonb,'[]'::jsonb,'[]'::jsonb,$3,$4,'[]'::jsonb,'expired'),
            ($1,$2,'fresh-active','Fresh packet','[]'::jsonb,'[]'::jsonb,'[]'::jsonb,NULL,'[]'::jsonb,'[]'::jsonb,'[]'::jsonb,$5,$6,'[]'::jsonb,'active')
            """,
            tenant,
            user,
            now - timedelta(days=2),
            now - timedelta(days=1),
            now - timedelta(minutes=5),
            now + timedelta(hours=12),
        )
        latest = await get_latest_fast_handover_packet(
            db,
            tenant_id=tenant,
            user_id=user,
            as_of=now,
        )
        latest_with_expired = await get_latest_fast_handover_packet(
            db,
            tenant_id=tenant,
            user_id=user,
            include_expired=True,
            as_of=now,
        )
        assert latest is not None
        assert latest["session_id"] == "fresh-active"
        assert latest_with_expired is not None
        assert latest_with_expired["session_id"] == "fresh-active"

        await db.execute(
            """
            DELETE FROM session_handover_packets
            WHERE tenant_id=$1 AND user_id=$2 AND session_id='fresh-active'
            """,
            tenant,
            user,
        )
        hidden = await get_latest_fast_handover_packet(db, tenant_id=tenant, user_id=user, as_of=now)
        shown = await get_latest_fast_handover_packet(db, tenant_id=tenant, user_id=user, include_expired=True, as_of=now)
        assert hidden is None
        assert shown is not None
        assert shown["session_id"] == "old-expired"


@pytest.mark.asyncio
async def test_debug_session_handover_endpoint_returns_latest_non_expired_packet():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"
    now = datetime.now(timezone.utc)

    async with app.router.lifespan_context(app):
        await db.execute(
            """
            INSERT INTO session_handover_packets (
                tenant_id, user_id, session_id, summary, open_questions, unresolved_decisions,
                pending_actions, recent_state_note, important_people, active_topics, do_not_overdo,
                created_at, expires_at, source_turn_refs, status
            )
            VALUES ($1,$2,$3,$4,'["What should I send?"]'::jsonb,'[]'::jsonb,'["draft the invoice chase"]'::jsonb,$5,'["Acme"]'::jsonb,'["invoice #102"]'::jsonb,'["Confirm before assuming state."]'::jsonb,$6,$7,'[]'::jsonb,'active')
            """,
            tenant,
            user,
            session_id,
            "Recent session focused on invoice #102.",
            "A bit tired after the call.",
            now,
            now + timedelta(hours=12),
        )
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(
                "/internal/debug/session-handover",
                params={"tenantId": tenant, "userId": user},
                headers={"X-Internal-Token": "test-token"},
            )
            assert response.status_code == 200
            data = response.json()
            assert data["exists"] is True
            assert data["sessionId"] == session_id
            assert data["pendingActions"] == ["draft the invoice chase"]
            assert data["openQuestions"] == ["What should I send?"]
