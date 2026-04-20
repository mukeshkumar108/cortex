from __future__ import annotations

import json
from datetime import datetime, timezone as dt_timezone
from pathlib import Path
from uuid import uuid4

import pytest

from src.db import Database
from src.main import _build_handover_packet, app


def _load_golden_fixture() -> dict:
    fixture_path = Path(__file__).resolve().parent / "fixtures" / "t9a_handover_quality_golden.json"
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def _assert_packet_matches_fixture(packet: dict, fixture: dict) -> None:
    for key in fixture.get("required_top_keys", []):
        assert key in packet
    for key in fixture.get("required_living_keys", []):
        assert key in (packet.get("living_context") or {})
    for key in fixture.get("required_identity_keys", []):
        assert key in (packet.get("identity") or {})
    for key in fixture.get("required_provenance_keys", []):
        assert key in (packet.get("provenance") or {})
    open_threads = packet.get("open_threads") or []
    assert isinstance(open_threads, list)
    if open_threads:
        for key in fixture.get("required_thread_keys", []):
            assert key in (open_threads[0] or {})


async def _seed_session_and_turn(db: Database, *, tenant_id: str, user_id: str, session_id: str) -> int:
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
    return int(
        await db.fetchval(
            """
            INSERT INTO turns_v2 (
                tenant_id, session_id, user_id, turn_index, role, content, occurred_at, metadata
            )
            VALUES ($1, $2, $3, 0, 'user', 'seed turn', $4, '{}'::jsonb)
            RETURNING turn_id
            """,
            tenant_id,
            session_id,
            user_id,
            now,
        )
    )


@pytest.mark.asyncio
async def test_t9a_handover_uses_canonical_fallback_with_provenance():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"

    async with app.router.lifespan_context(app):
        db = Database()
        turn_id = await _seed_session_and_turn(db, tenant_id=tenant, user_id=user, session_id=session_id)

        person_entity_id = await db.fetchval(
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
        project_entity_id = await db.fetchval(
            """
            INSERT INTO entities (
                tenant_id, user_id, canonical_name, canonical_name_normalized, entity_type, status, metadata
            )
            VALUES ($1, $2, 'Onboarding Launch', 'onboarding launch', 'project', 'active', '{}'::jsonb)
            RETURNING entity_id
            """,
            tenant,
            user,
        )
        first_claim_id = await db.fetchval(
            """
            INSERT INTO claims (
                tenant_id, user_id, claim_slot_key, claim_event_key, predicate,
                subject_entity_id, subject_text, object_payload, lifecycle_status,
                extraction_confidence, truth_confidence, predicate_policy_version, metadata
            )
            VALUES (
                $1, $2, $3, $4, 'project.focus',
                $5::uuid, 'Onboarding Launch', '{"value":"ship onboarding this week"}'::jsonb, 'active',
                0.90, 0.91, 'v2.p1', '{}'::jsonb
            )
            RETURNING claim_id
            """,
            tenant,
            user,
            f"slot-{uuid4().hex}",
            f"event-{uuid4().hex}",
            project_entity_id,
        )
        second_claim_id = await db.fetchval(
            """
            INSERT INTO claims (
                tenant_id, user_id, claim_slot_key, claim_event_key, predicate,
                subject_entity_id, subject_text, object_payload, lifecycle_status,
                extraction_confidence, truth_confidence, predicate_policy_version, metadata
            )
            VALUES (
                $1, $2, $3, $4, 'identity.core_value',
                $5::uuid, 'The user', '{"value":"honesty and reliability"}'::jsonb, 'active',
                0.88, 0.90, 'v2.p1', '{}'::jsonb
            )
            RETURNING claim_id
            """,
            tenant,
            user,
            f"slot-{uuid4().hex}",
            f"event-{uuid4().hex}",
            person_entity_id,
        )
        third_claim_id = await db.fetchval(
            """
            INSERT INTO claims (
                tenant_id, user_id, claim_slot_key, claim_event_key, predicate,
                subject_entity_id, subject_text, object_payload, lifecycle_status,
                extraction_confidence, truth_confidence, predicate_policy_version, metadata
            )
            VALUES (
                $1, $2, $3, $4, 'goal.persistent',
                $5::uuid, 'The user', '{"value":"show up consistently as a partner"}'::jsonb, 'active',
                0.86, 0.87, 'v2.p1', '{}'::jsonb
            )
            RETURNING claim_id
            """,
            tenant,
            user,
            f"slot-{uuid4().hex}",
            f"event-{uuid4().hex}",
            person_entity_id,
        )
        for claim_id in [first_claim_id, second_claim_id, third_claim_id]:
            await db.execute(
                """
                INSERT INTO claim_evidence (
                    tenant_id, claim_id, session_id, turn_id, evidence_kind, evidence_text, evidence_start_char, evidence_end_char, metadata
                )
                VALUES ($1, $2, $3, $4, 'span', 'seed evidence', 0, 12, '{}'::jsonb)
                """,
                tenant,
                claim_id,
                session_id,
                turn_id,
            )
        await db.execute(
            """
            INSERT INTO canonical_tenant_watermarks (tenant_id, last_sequence, updated_at)
            VALUES ($1, 7, NOW())
            ON CONFLICT (tenant_id)
            DO UPDATE SET last_sequence = EXCLUDED.last_sequence, updated_at = EXCLUDED.updated_at
            """,
            tenant,
        )

        packet = await _build_handover_packet(user)
        await db.close()

    fixture = _load_golden_fixture()
    _assert_packet_matches_fixture(packet, fixture)

    assert "living_context" in packet
    assert "identity" in packet
    assert "open_threads" in packet
    assert "people" in packet
    assert "projects" in packet
    assert "provenance" in packet

    assert packet["living_context"]["current_focus"]
    assert packet["identity"]["core_values"]
    assert packet["identity"]["persistent_goals"]
    assert len(packet["open_threads"]) >= 1
    assert any((p.get("name") or "").lower() == "ashley" for p in (packet.get("people") or []))
    assert packet["projects"]
    assert any(int(w.get("last_sequence") or 0) == 7 for w in (packet["provenance"].get("canonical_watermarks") or []))


@pytest.mark.asyncio
async def test_t9a_handover_preserves_derived_values_when_present():
    user = f"user-{uuid4().hex}"

    async with app.router.lifespan_context(app):
        db = Database()
        await db.execute(
            """
            INSERT INTO living_context (
                user_id, current_focus, primary_tension, unspoken_goal, emotional_texture, relationship_pulse
            )
            VALUES ($1, 'derived focus', 'derived tension', 'derived goal', 'derived texture', 'derived pulse')
            ON CONFLICT (user_id)
            DO UPDATE SET
              current_focus = EXCLUDED.current_focus,
              primary_tension = EXCLUDED.primary_tension,
              unspoken_goal = EXCLUDED.unspoken_goal,
              emotional_texture = EXCLUDED.emotional_texture,
              relationship_pulse = EXCLUDED.relationship_pulse
            """,
            user,
        )
        await db.execute(
            """
            INSERT INTO identity_profile (
                user_id, current_chapter, core_values, persistent_goals, what_they_want
            )
            VALUES (
                $1,
                'derived chapter',
                '[{"value":"family"}]'::jsonb,
                '[{"goal":"stability"}]'::jsonb,
                'derived want'
            )
            ON CONFLICT (user_id)
            DO UPDATE SET
              current_chapter = EXCLUDED.current_chapter,
              core_values = EXCLUDED.core_values,
              persistent_goals = EXCLUDED.persistent_goals,
              what_they_want = EXCLUDED.what_they_want
            """,
            user,
        )
        await db.execute(
            """
            INSERT INTO open_threads (
                user_id, title, detail, status, priority, category, thread_type, salience_score
            )
            VALUES ($1, 'derived thread', 'derived detail', 'open', 'high', 'relationship', 'situational', 0.95)
            """,
            user,
        )
        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, status, profile_text, key_facts, open_questions, last_known_status, confidence, mention_count
            )
            VALUES (
                $1, 'Derived Person', 'derived person', 'person', 'active',
                'Derived profile text', '["derived fact"]'::jsonb, '[]'::jsonb, 'derived status', 0.9, 5
            )
            ON CONFLICT (user_id, canonical_name_normalized)
            DO UPDATE SET
              profile_text = EXCLUDED.profile_text,
              key_facts = EXCLUDED.key_facts,
              last_known_status = EXCLUDED.last_known_status,
              confidence = EXCLUDED.confidence
            """,
            user,
        )

        packet = await _build_handover_packet(user)
        await db.close()

    assert packet["living_context"]["current_focus"] == "derived focus"
    assert packet["living_context"]["primary_tension"] == "derived tension"
    assert packet["identity"]["current_chapter"] == "derived chapter"
    assert packet["open_threads"][0]["title"] == "derived thread"
    assert packet["people"][0]["name"] == "Derived Person"


def test_t9a_handover_output_contract_against_golden_fixture():
    fixture = _load_golden_fixture()

    assert fixture.get("schema_version") == "t9a.handover.golden.v1"
    for key in fixture.get("required_top_keys", []):
        assert key
    for key in fixture.get("required_living_keys", []):
        assert key
    for key in fixture.get("required_identity_keys", []):
        assert key
    for key in fixture.get("required_thread_keys", []):
        assert key
    for key in fixture.get("required_provenance_keys", []):
        assert key
