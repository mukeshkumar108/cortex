from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import uuid

import pytest

from src.main import app, db, _build_handover_packet, session_startbrief

FIXTURE = Path(__file__).parent / "fixtures" / "derived_pipeline" / "startbrief_handover_golden.json"


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


async def _seed_derived_user(user_id: str, session_id: str) -> None:
    now = datetime.now(timezone.utc)
    await db.execute(
        """
        INSERT INTO session_transcript (tenant_id, session_id, user_id, messages, created_at, updated_at)
        VALUES ('default',$1,$2,$3::jsonb,NOW(),NOW())
        ON CONFLICT (tenant_id, session_id) DO NOTHING
        """,
        session_id,
        user_id,
        [{"role": "user", "text": "Remember Ashley and the walking goal.", "timestamp": now.isoformat()}],
    )
    await db.execute(
        """
        INSERT INTO session_classifications (
            session_id, user_id, session_date, is_memory_worthy, session_kind,
            one_line_summary, entity_mentions, run_entity_pass, run_threads_pass,
            identity_relevant, emotional_weight, emotional_note, tension_signal,
            processed_at, model_used, raw_triage_output, context_relevant
        ) VALUES (
            $1,$2,$3,true,'personal',$4,$5::text[],true,true,true,'medium',$6,$7,NOW(),'fixture',$8::jsonb,true
        )
        ON CONFLICT (session_id) DO UPDATE SET processed_at=EXCLUDED.processed_at
        """,
        session_id,
        user_id,
        now - timedelta(hours=2),
        "User is trying to become more consistent.",
        ["Ashley"],
        "Determined but strained.",
        "Wanting change while avoiding the deeper stakes.",
        {
            "memory_deltas": ["User is trying to become more consistent."],
            "identity_signals": ["User values integrity over approval."],
            "thread_signals": ["Walking goal remains unresolved."],
        },
    )
    await db.execute(
        """
        INSERT INTO entity_profiles (
            user_id, canonical_name, canonical_name_normalized, type, aliases,
            status, relationship_to_user, profile_text, last_known_status,
            confidence, mention_count, first_seen_at, last_seen_at,
            source_session_ids, importance_score, salience_score
        ) VALUES (
            $1,'Ashley','ashley','person',$2::text[],'active','partner',
            'Ashley is relationally important.', 'present in current context',
            0.9,3,NOW(),NOW(),$3::text[],0.9,0.9
        )
        ON CONFLICT (user_id, canonical_name_normalized) DO UPDATE SET last_seen_at=NOW()
        """,
        user_id,
        ["Ashley"],
        [session_id],
    )
    await db.execute(
        """
        INSERT INTO open_threads (
            thread_id, user_id, title, detail, status, priority, category,
            source_session_ids, first_seen_at, last_updated_at, last_mentioned_at,
            lifecycle_state, evidence_turn_refs, salience_score, importance_score
        ) VALUES (
            $1,$2,'Walking goal','Walking goal remains unresolved.','open','medium','goal',
            $3::text[],NOW(),NOW(),NOW(),'active',$4::jsonb,0.8,0.8
        )
        ON CONFLICT (thread_id) DO NOTHING
        """,
        _unique("thread"),
        user_id,
        [session_id],
        [{"session_id": session_id, "turn_index": 0}],
    )
    await db.execute(
        """
        INSERT INTO identity_profile (
            user_id, who_they_are, core_values, persistent_goals, current_chapter,
            what_they_want, last_synthesized_at, source_session_count, synthesis_model
        ) VALUES (
            $1,'A person rebuilding from integrity rather than approval.',
            $2::jsonb,$3::jsonb,'A period of reconstruction.',
            'To be steady and present.',NOW(),1,'fixture'
        )
        ON CONFLICT (user_id) DO UPDATE SET current_chapter=EXCLUDED.current_chapter
        """,
        user_id,
        [{"value": "integrity", "confidence": 0.9}],
        [{"goal": "daily walks"}],
    )
    await db.execute(
        """
        INSERT INTO living_context (
            user_id, current_focus, recent_narrative, relationship_pulse,
            emotional_texture, primary_tension, unspoken_goal, active_contradictions,
            sophie_directives, last_synthesized_at, sessions_since_last,
            source_session_count, synthesis_model
        ) VALUES (
            $1,'Becoming consistent while holding relational pressure.',
            'The same walking and consistency signal has returned.',
            'Relationships are present as pressure and motivation.',
            'Determined but strained.',
            'Wanting change while avoiding the deeper stakes.',
            'To become someone who can be relied on.',
            $2::jsonb,$3::jsonb,NOW(),0,1,'fixture'
        )
        ON CONFLICT (user_id) DO UPDATE SET current_focus=EXCLUDED.current_focus
        """,
        user_id,
        [{"topic": "consistency"}],
        [{"directive": "do not flatten this into productivity advice", "confidence": 0.9}],
    )


@pytest.mark.asyncio
async def test_startbrief_and_handover_golden_derived_only():
    expected = json.loads(FIXTURE.read_text(encoding="utf-8"))
    async with app.router.lifespan_context(app):
        user_id = _unique("golden-user")
        session_id = _unique("golden-session")
        await _seed_derived_user(user_id, session_id)

        startbrief = await session_startbrief(
            tenantId="default",
            userId=user_id,
            now=datetime.now(timezone.utc).isoformat(),
            sessionId=session_id,
            timezone="UTC",
        )
        start_payload = startbrief.model_dump()
        assert start_payload["handover_depth"] == expected["startbrief"]["handover_depth"]
        assert expected["startbrief"]["entity_hints"][0] in [e["name"] for e in start_payload["entity_hints"]]
        assert expected["startbrief"]["recent_change_contains"] in json.dumps(start_payload["ops_context"], ensure_ascii=False)
        assert start_payload["evidence"]["canonical_provenance"]["canonical_claims_considered"] == 0

        handover = await _build_handover_packet(user_id)
        assert handover["living_context"]["current_focus"] == expected["handover"]["living_context"]["current_focus"]
        assert handover["living_context"]["primary_tension"] == expected["handover"]["living_context"]["primary_tension"]
        assert handover["identity"]["current_chapter"] == expected["handover"]["identity"]["current_chapter"]
        assert expected["handover"]["open_threads"][0] in [t["title"] for t in handover["open_threads"]]
        assert expected["handover"]["people"][0] in [p["name"] for p in handover["people"]]
        assert handover["provenance"]["canonical_claims_considered"] == 0
