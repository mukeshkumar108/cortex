from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from src.main import _build_handover_packet, app, db, session_startbrief


@pytest.mark.asyncio
async def test_startbrief_safety_gate_stays_derived_only(monkeypatch):
    user_id = f"derived-only-startbrief-user-{uuid4().hex}"
    session_id = f"derived-only-startbrief-session-{uuid4().hex}"
    now = datetime.now(timezone.utc)
    async with app.router.lifespan_context(app):
        await db.execute(
            """
            INSERT INTO session_transcript (tenant_id, session_id, user_id, messages, created_at, updated_at)
            VALUES ('default',$1,$2,$3::jsonb,NOW(),NOW())
            ON CONFLICT (tenant_id, session_id) DO NOTHING
            """,
            session_id,
            user_id,
            [{"role": "user", "text": "Remember Riley and the walking goal.", "timestamp": now.isoformat()}],
        )
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy, session_kind,
                one_line_summary, entity_mentions, run_entity_pass, run_threads_pass,
                identity_relevant, emotional_weight, processed_at, model_used,
                raw_triage_output, context_relevant
            ) VALUES (
                $1,$2,NOW(),true,'personal','derived only',$3::text[],true,true,true,
                'medium',NOW(),'fixture',$4::jsonb,true
            )
            ON CONFLICT (session_id) DO UPDATE SET processed_at=EXCLUDED.processed_at
            """,
            session_id,
            user_id,
            ["Riley"],
            {"memory_deltas": ["User is trying to become more consistent."]},
        )
        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases,
                status, relationship_to_user, profile_text, confidence, mention_count,
                first_seen_at, last_seen_at, source_session_ids, importance_score, salience_score
            ) VALUES (
                $1,'Riley','riley','person',$2::text[],'active','partner',
                'Riley is relationally important.',0.9,3,NOW(),NOW(),$3::text[],0.9,0.9
            )
            ON CONFLICT (user_id, canonical_name_normalized) DO UPDATE SET last_seen_at=NOW()
            """,
            user_id,
            ["Riley"],
            [session_id],
        )
        async def _unexpected(*_args, **_kwargs):
            raise AssertionError("startbrief touched canonical fallback helper")

        monkeypatch.setattr("src.main._fetch_canonical_signal_rows", _unexpected, raising=True)
        monkeypatch.setattr("src.main._pg_get_entity_role_hint", _unexpected, raising=True)
        monkeypatch.setattr("src.main._pg_get_entity_continuity_facts", _unexpected, raising=True)
        monkeypatch.setattr("src.main._pg_search_nodes", _unexpected, raising=True)
        monkeypatch.setattr("src.main._pg_search_continuity_facts", _unexpected, raising=True)

        response = await session_startbrief(
            tenantId="default",
            userId=user_id,
            now=now.isoformat(),
            sessionId=session_id,
            timezone="UTC",
        )
        payload = response.model_dump()

        provenance = ((payload.get("evidence") or {}).get("canonical_provenance") or {})
        assert provenance.get("projection_version") == "derived.startbrief.v1"
        assert provenance.get("canonical_claims_considered") == 0
        assert provenance.get("canonical_entities_considered") == 0


@pytest.mark.asyncio
async def test_handover_safety_gate_stays_derived_only(monkeypatch):
    user_id = f"derived-only-handover-user-{uuid4().hex}"
    async with app.router.lifespan_context(app):
        await db.execute(
            """
            INSERT INTO living_context (
                user_id, current_focus, primary_tension, unspoken_goal, emotional_texture, relationship_pulse
            )
            VALUES ($1, 'derived focus', 'derived tension', 'derived goal', 'derived texture', 'derived pulse')
            ON CONFLICT (user_id)
            DO UPDATE SET current_focus=EXCLUDED.current_focus
            """,
            user_id,
        )
        await db.execute(
            """
            INSERT INTO identity_profile (
                user_id, current_chapter, core_values, persistent_goals, what_they_want
            )
            VALUES ($1, 'derived chapter', '[{"value":"family"}]'::jsonb, '[{"goal":"stability"}]'::jsonb, 'derived want')
            ON CONFLICT (user_id)
            DO UPDATE SET current_chapter=EXCLUDED.current_chapter
            """,
            user_id,
        )
        async def _unexpected(*_args, **_kwargs):
            raise AssertionError("handover touched canonical fallback helper")

        monkeypatch.setattr("src.main._fetch_canonical_signal_rows", _unexpected, raising=True)
        monkeypatch.setattr("src.main._pg_get_entity_role_hint", _unexpected, raising=True)
        monkeypatch.setattr("src.main._pg_get_entity_continuity_facts", _unexpected, raising=True)
        monkeypatch.setattr("src.main._pg_search_nodes", _unexpected, raising=True)
        monkeypatch.setattr("src.main._pg_search_continuity_facts", _unexpected, raising=True)

        packet = await _build_handover_packet(user_id)

        assert packet["provenance"]["projection_version"] == "derived.handover.v1"
        assert packet["provenance"]["canonical_claims_considered"] == 0
        assert packet["provenance"]["canonical_entities_considered"] == 0
