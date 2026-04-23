from __future__ import annotations

import json
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


@pytest.mark.asyncio
async def test_t9a_handover_remains_derived_only_even_if_canonical_rows_exist():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"

    async with app.router.lifespan_context(app):
        db = Database()
        await db.execute(
            """
            INSERT INTO entities (
                tenant_id, user_id, canonical_name, canonical_name_normalized, entity_type, status, metadata
            )
            VALUES ($1, $2, 'Ashley', 'ashley', 'person', 'active', '{}'::jsonb)
            """,
            tenant,
            user,
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
        await db.execute(
            """
            INSERT INTO living_context (
                user_id, current_focus, primary_tension, unspoken_goal, emotional_texture, relationship_pulse
            )
            VALUES ($1, 'derived focus', 'derived tension', 'derived goal', 'derived texture', 'derived pulse')
            ON CONFLICT (user_id)
            DO UPDATE SET current_focus=EXCLUDED.current_focus
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
            DO UPDATE SET current_chapter=EXCLUDED.current_chapter
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
                user_id, canonical_name, canonical_name_normalized, type, status, relationship_to_user,
                profile_text, key_facts, open_questions, last_known_status, confidence, mention_count, salience_score
            )
            VALUES (
                $1, 'Derived Person', 'derived person', 'person', 'active', 'partner',
                'Derived profile text', '["derived fact"]'::jsonb, '[]'::jsonb, 'derived status', 0.9, 5, 0.95
            )
            ON CONFLICT (user_id, canonical_name_normalized)
            DO UPDATE SET profile_text = EXCLUDED.profile_text
            """,
            user,
        )

        packet = await _build_handover_packet(user)
        await db.close()

    fixture = _load_golden_fixture()
    _assert_packet_matches_fixture(packet, fixture)

    assert packet["living_context"]["current_focus"] == "derived focus"
    assert packet["identity"]["current_chapter"] == "derived chapter"
    assert packet["open_threads"][0]["title"] == "derived thread"
    assert packet["people"][0]["name"] == "Derived Person"
    assert packet["provenance"]["projection_version"] == "derived.handover.v1"
    assert packet["provenance"]["canonical_claims_considered"] == 0
    assert packet["provenance"]["canonical_entities_considered"] == 0
    assert packet["provenance"]["canonical_watermarks"] == []


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
