from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import uuid

import pytest

import src.derived_pipeline as derived_pipeline
from src.canonicalization import stable_short_hash
from src.config import get_settings
from src.main import app, db, _build_handover_packet, _execute_post_ingest_hook, session_startbrief
from src import session

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "derived_pipeline" / "parity_cases.json"
SCRIPT_DIR = Path(__file__).parents[1] / "scripts"


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def _load_script(name: str):
    path = SCRIPT_DIR / name
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


PASS1_SCRIPT = _load_script("run_pass1_memory_triage_batch.py")
PASS15_SCRIPT = _load_script("run_entity_pipeline.py")
PASS3_SCRIPT = _load_script("run_threads_pipeline.py")
PASS4_SCRIPT = _load_script("run_identity_synthesis.py")
PASS5_SCRIPT = _load_script("run_living_context.py")


def _text(messages: list[dict]) -> str:
    return "\n".join(str(m.get("text") or "") for m in messages if (m.get("role") or "").lower() == "user")


def _payload_for_text(text: str) -> dict:
    lowered = text.lower()
    if "dad's birthday" in lowered or "dad's birthday" in lowered or "losing him to cancer" in lowered:
        return {
            "is_memory_worthy": True,
            "session_kind": "personal",
            "memory_deltas": ["Dad's birthday is bringing bereavement grief back into the foreground."],
            "entity_mentions": ["Dad"],
            "thread_signals": ["Dad birthday grief"],
            "identity_signals": ["User keeps functioning while carrying specific grief around his father."],
            "emotional_weight": "high",
            "emotional_note": "Bereaved and guarded against cheerleading.",
            "tension_signal": "The user needs the grief witnessed without cheerleading or minimization.",
            "context_relevant": True,
            "run_entity_pass": True,
            "run_threads_pass": True,
            "identity_relevant": True,
        }
    if "bluum" in lowered and ("not right" in lowered or "abandoned" in lowered or "overwhelmed" in lowered):
        return {
            "is_memory_worthy": True,
            "session_kind": "mixed",
            "memory_deltas": ["User corrected the Bluum narrative: paused from overwhelm, not abandonment."],
            "entity_mentions": ["Bluum"],
            "thread_signals": ["Correcting the Bluum narrative"],
            "identity_signals": ["User cares deeply about Bluum but can misread overwhelm as failure."],
            "emotional_weight": "medium",
            "emotional_note": "Protective and corrective.",
            "tension_signal": "The user fears failure enough that care can look like withdrawal.",
            "context_relevant": True,
            "run_entity_pass": True,
            "run_threads_pass": True,
            "identity_relevant": True,
        }
    if "maya" in lowered:
        return {
            "is_memory_worthy": True,
            "session_kind": "personal",
            "memory_deltas": ["Maya relationship status changed to ended and should not be treated as partner context."],
            "entity_mentions": ["Maya"],
            "thread_signals": ["Maya breakup rupture"],
            "identity_signals": ["User is trying to accept a relational rupture without softening it."],
            "emotional_weight": "high",
            "emotional_note": "Raw and final.",
            "tension_signal": "The user wants to explain himself while also accepting the relationship is over.",
            "context_relevant": True,
            "run_entity_pass": True,
            "run_threads_pass": True,
            "identity_relevant": True,
        }
    if "hmrc" in lowered:
        return {
            "is_memory_worthy": True,
            "session_kind": "personal",
            "memory_deltas": ["HMRC letter remains unopened and is becoming an avoidance pattern."],
            "entity_mentions": ["HMRC"],
            "thread_signals": ["Avoiding the HMRC letter"],
            "identity_signals": ["User can turn fear into jokes and administrative displacement."],
            "emotional_weight": "medium",
            "emotional_note": "Anxious and deflecting.",
            "tension_signal": "The user knows opening the letter matters but keeps converting fear into deflection.",
            "context_relevant": True,
            "run_entity_pass": True,
            "run_threads_pass": True,
            "identity_relevant": True,
        }
    if "gym" in lowered:
        return {
            "is_memory_worthy": True,
            "session_kind": "personal",
            "memory_deltas": ["The gym thread reactivated after months of silence."],
            "entity_mentions": ["Gym"],
            "thread_signals": ["Gym return after silence"],
            "identity_signals": ["User is trying to return to embodied care without making it a performance."],
            "emotional_weight": "medium",
            "emotional_note": "Embarrassed but quietly proud.",
            "tension_signal": "The user wants the return noticed without being turned into a big plan.",
            "context_relevant": True,
            "run_entity_pass": True,
            "run_threads_pass": True,
            "identity_relevant": True,
        }
    if "riley" in lowered:
        return {
            "is_memory_worthy": True,
            "session_kind": "personal",
            "memory_deltas": ["Riley relationship carries both hope and unresolved tension."],
            "entity_mentions": ["Riley"],
            "thread_signals": ["Riley relationship tension"],
            "identity_signals": ["User is trying to be steady without pretending tension is gone."],
            "emotional_weight": "medium",
            "emotional_note": "Hopeful but guarded.",
            "tension_signal": "The user wants repair with Riley but avoids the hard conversation.",
            "context_relevant": True,
            "run_entity_pass": True,
            "run_threads_pass": True,
            "identity_relevant": True,
        }
    if "jordan" in lowered:
        return {
            "is_memory_worthy": True,
            "session_kind": "personal",
            "memory_deltas": ["Jordan remains emotionally present through silence and reconnection intent."],
            "entity_mentions": ["Jordan"],
            "thread_signals": ["Reconnecting with Jordan"],
            "identity_signals": ["User wants relational repair but freezes before acting."],
            "emotional_weight": "high",
            "emotional_note": "Tender and unfinished.",
            "tension_signal": "The user wants reconnection but freezes before acting.",
            "context_relevant": True,
            "run_entity_pass": True,
            "run_threads_pass": True,
            "identity_relevant": True,
        }
    return {
        "is_memory_worthy": True,
        "session_kind": "personal",
        "memory_deltas": ["Walking and hydration routine remains unresolved and meaningful."],
        "entity_mentions": ["Sophie"],
        "thread_signals": ["Walking and hydration routine"],
        "identity_signals": ["User wants consistency through care rather than pressure."],
        "emotional_weight": "medium",
        "emotional_note": "Frustrated but committed.",
        "tension_signal": "The user wants consistency but resists shallow productivity advice.",
        "context_relevant": True,
        "run_entity_pass": True,
        "run_threads_pass": True,
        "identity_relevant": True,
    }


def _identity_for_case(case_id: str) -> dict:
    if case_id == "relationship_contradiction":
        chapter = "A period of learning steadiness without pretending tension is gone."
        who = "Someone trying to repair relationship tension without losing honesty."
    elif case_id == "silence_entity_evolution":
        chapter = "A period of facing relational repair without forcing it."
        who = "Someone carrying a quiet but persistent desire to reconnect."
    elif case_id == "unresolved_goal_health":
        chapter = "A period of rebuilding consistency through care rather than pressure."
        who = "Someone trying to rebuild health routines without being flattened into productivity advice."
    elif case_id == "grief_bereavement":
        chapter = "A period where bereavement is resurfacing around Dad's birthday."
        who = "Someone who keeps functioning while carrying specific cancer grief around his father."
    elif case_id == "user_correction_contradiction":
        chapter = "A period of correcting false closure around Bluum."
        who = "Someone who cares deeply about Bluum but can misread being overwhelmed as failure."
    elif case_id == "relational_rupture_status_change":
        chapter = "A period of absorbing the finality of the Maya breakup."
        who = "Someone trying to accept a relational rupture without softening it into a wobble."
    elif case_id == "repeated_deflection_avoidance":
        chapter = "A period where avoidance is clustering around the HMRC letter."
        who = "Observed pattern: turns fear into jokes and administrative displacement."
    elif case_id == "silence_reactivation":
        chapter = "A period of cautiously reactivating the gym after silence."
        who = "Someone who returned to embodied care while feeling embarrassed, without making it a performance."
    else:
        raise AssertionError(f"unhandled fixture {case_id}")
    return {
        "who_they_are": who,
        "core_values": [{"value": "integrity", "evidence": "identity signals", "confidence": 0.86}],
        "recurring_patterns": [{"pattern": "returns to unresolved commitments", "evidence": "thread signals"}],
        "family_history": "Family and relationships remain emotionally meaningful where present.",
        "faith_and_beliefs": "Not enough evidence to synthesize a durable belief pattern.",
        "what_they_want": "To become steady without being misunderstood.",
        "recurring_fears": [{"fear": "failing what matters", "evidence": "tension signals", "confidence": 0.74}],
        "what_they_avoid": "Naming the highest-stakes part too directly.",
        "how_they_relate": "Cares through responsibility and return, even when action stalls.",
        "persistent_goals": [{"goal": "be steady", "evidence": "identity signals"}],
        "current_chapter": chapter,
    }


def _living_for_case(case_id: str) -> dict:
    if case_id == "relationship_contradiction":
        focus = "Holding hope with Riley while not flattening the unresolved tension."
        tension = "The user wants repair with Riley but is still avoiding the hard conversation."
    elif case_id == "silence_entity_evolution":
        focus = "Jordan is present through both mention and silence."
        tension = "The user wants reconnection but freezes before acting."
    elif case_id == "unresolved_goal_health":
        focus = "Rebuilding walking and hydration without turning it into productivity pressure."
        tension = "The user wants consistency but resists being handled with shallow advice."
    elif case_id == "grief_bereavement":
        focus = "Approaching Dad's birthday without turning cancer grief into a generic mood problem."
        tension = "The user needs the grief witnessed without cheerleading or minimization."
    elif case_id == "user_correction_contradiction":
        focus = "Holding the correction that Bluum is paused from being overwhelmed, not abandoned."
        tension = "The user fears failure enough that care can look like withdrawal."
    elif case_id == "relational_rupture_status_change":
        focus = "The Maya relationship has ended and should be held as rupture, not partner tension."
        tension = "The user wants to explain himself while also accepting the relationship is over."
    elif case_id == "repeated_deflection_avoidance":
        focus = "The HMRC letter in the drawer is not just admin; it is an active avoidance pattern."
        tension = "The user knows opening the letter matters but keeps converting fear into deflection."
    elif case_id == "silence_reactivation":
        focus = "The gym has become active again after a long silence, and the returned effort matters."
        tension = "The user wants the return noticed without being turned into a big plan."
    else:
        raise AssertionError(f"unhandled fixture {case_id}")
    return {
        "current_focus": focus,
        "recent_narrative": "Recent sessions contain a repeated high-signal unresolved thread.",
        "relationship_pulse": "Relational context matters where named; absence is held as signal, not erased.",
        "emotional_texture": "Meaningful, unresolved, and guarded.",
        "primary_tension": tension,
        "what_theyre_avoiding": "Moving too quickly into a simple answer.",
        "unspoken_goal": "To feel held accurately while changing slowly.",
        "why_it_matters": "The thread is about identity and relationship to self, not only a task.",
        "active_contradictions": [{"topic": "change", "earlier_view": "wants movement", "recent_view": "still stuck"}],
        "sophie_directives": [{"directive": "hold the tension without flattening it", "confidence": 0.9}],
    }


async def _seed_sessions(user_id: str, case: dict) -> None:
    for idx, item in enumerate(case["sessions"]):
        session_id = f"{user_id}-{item['session_id']}"
        created_at = datetime.fromisoformat(item["messages"][0]["timestamp"])
        await db.execute(
            """
            INSERT INTO session_transcript (tenant_id, session_id, user_id, messages, created_at, updated_at)
            VALUES ('default',$1,$2,$3::jsonb,$4,$4)
            """,
            session_id,
            user_id,
            item["messages"],
            created_at,
        )
    if case["fixture_id"] == "silence_reactivation":
        thread_id = stable_short_hash({"user_id": user_id, "thread": "gym return after silence"}, length=20)
        await db.execute(
            """
            INSERT INTO open_threads (
                thread_id, user_id, title, detail, status, priority, category,
                source_session_ids, first_seen_at, last_updated_at, last_mentioned_at,
                lifecycle_state, evidence_turn_refs
            )
            VALUES (
                $1,$2,'Gym return after silence','Previously quiet gym thread.','snoozed','medium','goal',
                '{}'::text[],NOW() - INTERVAL '90 days',NOW() - INTERVAL '90 days',NOW() - INTERVAL '90 days',
                'snoozed','[]'::jsonb
            )
            ON CONFLICT (thread_id) DO NOTHING
            """,
            thread_id,
            user_id,
        )


async def _run_script_path(user_id: str) -> None:
    await PASS1_SCRIPT.run_batch(user_id=user_id, tenant_id="default", limit=20, include_existing=True)
    await PASS15_SCRIPT.run_batch(user_id=user_id, tenant_id="default", limit=20)
    await PASS3_SCRIPT.run_batch(user_id=user_id, tenant_id="default", limit=20)
    await PASS4_SCRIPT.run_once(user_id=user_id, tenant_id="default")
    await PASS5_SCRIPT.run_once(user_id=user_id, tenant_id="default")


async def _run_live_path(user_id: str, case: dict) -> None:
    for item in case["sessions"]:
        session_id = f"{user_id}-{item['session_id']}"
        reference_time = item["messages"][0]["timestamp"]
        payload = {
            "tenant_id": "default",
            "user_id": user_id,
            "session_id": session_id,
            "reference_time": reference_time,
        }
        assert await _execute_post_ingest_hook(session.POST_INGEST_HOOK_PASS1_TRIAGE, payload) is True
        assert await _execute_post_ingest_hook(session.POST_INGEST_HOOK_PASS1_5_ENTITIES, payload) is True
        assert await _execute_post_ingest_hook(session.POST_INGEST_HOOK_PASS3_THREADS, payload) is True
    final_session_id = f"{user_id}-{case['sessions'][-1]['session_id']}"
    final_payload = {
        "tenant_id": "default",
        "user_id": user_id,
        "session_id": final_session_id,
        "reference_time": case["sessions"][-1]["messages"][0]["timestamp"],
    }
    assert await _execute_post_ingest_hook(session.POST_INGEST_HOOK_PASS4_IDENTITY, final_payload) is True
    assert await _execute_post_ingest_hook(session.POST_INGEST_HOOK_PASS5_LIVING_CONTEXT, final_payload) is True


async def _snapshot(user_id: str, session_id: str) -> dict:
    classifications = await db.fetch(
        """
        SELECT one_line_summary, entity_mentions, run_entity_pass, run_threads_pass,
               identity_relevant, emotional_weight, emotional_note, tension_signal,
               raw_triage_output, context_relevant
        FROM session_classifications
        WHERE user_id=$1
        ORDER BY session_id
        """,
        user_id,
    )
    entities = await db.fetch(
        """
        SELECT canonical_name, canonical_name_normalized, type, status,
               relationship_to_user, profile_text, key_facts, open_questions,
               last_known_status, source_session_ids, memory_layer
        FROM entity_profiles
        WHERE user_id=$1
        ORDER BY canonical_name_normalized
        """,
        user_id,
    )
    threads = await db.fetch(
        """
        SELECT title, detail, status, priority, category, lifecycle_state,
               source_session_ids, evidence_turn_refs, memory_layer, semantic_category
        FROM open_threads
        WHERE user_id=$1
        ORDER BY title
        """,
        user_id,
    )
    identity = await db.fetchone(
        """
        SELECT who_they_are, core_values, recurring_patterns, what_they_want,
               recurring_fears, what_they_avoid, how_they_relate, persistent_goals,
               current_chapter, assertions
        FROM identity_profile
        WHERE user_id=$1
        """,
        user_id,
    )
    living = await db.fetchone(
        """
        SELECT current_focus, recent_narrative, relationship_pulse, emotional_texture,
               primary_tension, what_theyre_avoiding, unspoken_goal, why_it_matters,
               active_contradictions, sophie_directives, assertions
        FROM living_context
        WHERE user_id=$1
        """,
        user_id,
    )
    startbrief = await session_startbrief(
        tenantId="default",
        userId=user_id,
        now=datetime.now(timezone.utc).isoformat(),
        sessionId=session_id,
        timezone="UTC",
    )
    handover = await _build_handover_packet(user_id)
    return {
        "classifications": [dict(row) for row in classifications],
        "entities": [dict(row) for row in entities],
        "threads": [dict(row) for row in threads],
        "identity": dict(identity) if identity else {},
        "living": dict(living) if living else {},
        "startbrief": startbrief.model_dump(),
        "handover": handover,
    }


def _strip_user_specific(value):
    if isinstance(value, dict):
        cleaned = {}
        for key, item in value.items():
            if key in {
                "generated_at",
                "generatedAt",
                "request_id",
                "canonical_watermarks",
                "canonical_provenance",
                "provenance",
                "source_session_ids",
                "evidence_turn_refs",
                "assertions",
                "sourceSessionIds",
                "sourceTurnRefs",
            }:
                continue
            cleaned[key] = _strip_user_specific(item)
        return cleaned
    if isinstance(value, list):
        return [_strip_user_specific(item) for item in value]
    if isinstance(value, str):
        return value.replace("script-user", "USER").replace("live-user", "USER")
    return value


def _meaningful_projection(snapshot: dict) -> dict:
    return {
        "classifications": _strip_user_specific(snapshot["classifications"]),
        "entities": _strip_user_specific(snapshot["entities"]),
        "threads": _strip_user_specific(snapshot["threads"]),
        "identity": _strip_user_specific(snapshot["identity"]),
        "living": _strip_user_specific(snapshot["living"]),
        "startbrief_entities": sorted(e["name"] for e in snapshot["startbrief"].get("entity_hints", [])),
        "startbrief_depth": snapshot["startbrief"].get("handover_depth"),
        "handover_living": _strip_user_specific(snapshot["handover"].get("living_context", {})),
        "handover_threads": sorted(t["title"] for t in snapshot["handover"].get("open_threads", [])),
        "handover_people": sorted(p["name"] for p in snapshot["handover"].get("people", [])),
        "handover_identity": _strip_user_specific(snapshot["handover"].get("identity", {})),
    }


async def _assert_evidence_coverage(user_id: str) -> None:
    missing = await db.fetch(
        """
        SELECT surface, statement_text
        FROM derived_assertions
        WHERE user_id=$1
          AND surface = ANY($2::text[])
          AND (COALESCE(array_length(source_session_ids, 1), 0) = 0 OR source_turn_refs = '[]'::jsonb)
        """,
        user_id,
        ["identity_trait", "thread_update", "living_context_statement"],
    )
    assert missing == []
    threads_missing = await db.fetch(
        """
        SELECT title
        FROM open_threads
        WHERE user_id=$1 AND evidence_turn_refs = '[]'::jsonb
        """,
        user_id,
    )
    assert threads_missing == []
    living = await db.fetchone("SELECT assertions FROM living_context WHERE user_id=$1", user_id)
    assert living and living["assertions"]
    assert all(item.get("source_session_ids") for item in living["assertions"])
    identity = await db.fetchone("SELECT assertions FROM identity_profile WHERE user_id=$1", user_id)
    assert identity and identity["assertions"]
    assert all(item.get("source_session_ids") for item in identity["assertions"])


@pytest.mark.asyncio
@pytest.mark.parametrize("case", json.loads(FIXTURE_PATH.read_text(encoding="utf-8")), ids=lambda c: c["fixture_id"])
async def test_script_and_live_pipeline_material_parity(case, monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_enabled = True
        settings.derived_pipeline_llm_enabled = True
        settings.derived_pipeline_access_bump_enabled = False

        async def _pass1_stub(messages, model):
            return _payload_for_text(_text(messages))

        async def _entity_stub(existing_entities, mentions, model):
            return [
                {
                    "decision": "NEW",
                    "mention": item["mention"],
                    "canonical_name": item["mention"],
                    "type": "person" if item["mention"] != "Sophie" else "other",
                    "status": "active",
                    "relationship_to_user": "important_person" if item["mention"] != "Sophie" else "assistant",
                    "aliases": [item["mention"]],
                    "confidence": 0.88,
                }
                for item in mentions
            ]

        async def _entity_profile_stub(canonical_name, entity_type, relationship_to_user, messages, existing_profile_text, model):
            expected = case["expected"]
            return {
                "profile_text": f"{canonical_name} is tied to this fixture-specific context: {expected['living_current_focus']}",
                "key_facts": [{"fact": f"{canonical_name} appears in: {expected['pass1_summary']}", "confidence": 0.88}],
                "open_questions": [],
                "last_known_status": expected["living_primary_tension"],
            }

        async def _thread_stub(messages, session_date, emotional_weight, emotional_note, thread_signals, existing_threads, model):
            return [
                {
                    "action": "CREATE",
                    "title": signal,
                    "detail": signal,
                    "category": "goal" if "walking" in signal.lower() else "relationship",
                    "priority": "high",
                }
                for signal in thread_signals
            ]

        async def _identity_stub(**_kwargs):
            return _identity_for_case(case["fixture_id"])

        async def _living_stub(**_kwargs):
            return _living_for_case(case["fixture_id"])

        monkeypatch.setattr(derived_pipeline, "run_rich_pass1_llm", _pass1_stub, raising=True)
        monkeypatch.setattr(derived_pipeline, "resolve_entity_mentions", _entity_stub, raising=True)
        monkeypatch.setattr(derived_pipeline, "build_entity_profile", _entity_profile_stub, raising=True)
        monkeypatch.setattr(derived_pipeline, "extract_thread_actions", _thread_stub, raising=True)
        monkeypatch.setattr(derived_pipeline, "synthesize_identity_profile", _identity_stub, raising=True)
        monkeypatch.setattr(derived_pipeline, "synthesize_living_context", _living_stub, raising=True)

        script_user = _unique("script-user")
        live_user = _unique("live-user")
        await _seed_sessions(script_user, case)
        await _seed_sessions(live_user, case)

        await _run_script_path(script_user)
        await _run_live_path(live_user, case)

        script_session_id = f"{script_user}-{case['sessions'][-1]['session_id']}"
        live_session_id = f"{live_user}-{case['sessions'][-1]['session_id']}"
        script_snapshot = await _snapshot(script_user, script_session_id)
        live_snapshot = await _snapshot(live_user, live_session_id)

        assert _meaningful_projection(script_snapshot) == _meaningful_projection(live_snapshot)

        expected = case["expected"]
        entity_names = [row["canonical_name"] for row in live_snapshot["entities"]]
        for name in expected["entities"]:
            assert name in entity_names
        thread_titles = [row["title"].lower() for row in live_snapshot["threads"]]
        for title in expected["thread_titles"]:
            assert title.lower() in thread_titles
        assert live_snapshot["identity"]["current_chapter"] == expected["identity_current_chapter"]
        assert live_snapshot["identity"]["who_they_are"] == expected["identity_who"]
        assert live_snapshot["living"]["current_focus"] == expected["living_current_focus"]
        assert live_snapshot["living"]["primary_tension"] == expected["living_primary_tension"]
        assert expected["startbrief_signal"] in json.dumps(live_snapshot["startbrief"], ensure_ascii=False)
        assert expected["handover_thread"].lower() in [t["title"].lower() for t in live_snapshot["handover"].get("open_threads", [])]
        assert live_snapshot["startbrief"]["evidence"]["canonical_provenance"]["canonical_claims_considered"] == 0
        assert live_snapshot["handover"]["provenance"]["canonical_claims_considered"] == 0
        rich_text = json.dumps(
            {
                "identity": live_snapshot["identity"],
                "living": live_snapshot["living"],
                "handover": live_snapshot["handover"],
                "startbrief": live_snapshot["startbrief"],
            },
            ensure_ascii=False,
        ).lower()
        for term in expected.get("specific_terms", []):
            assert term.lower() in rich_text
        for term in expected.get("contradiction_terms", []):
            assert term.lower() in rich_text
        for term in expected.get("reactivation_terms", []):
            assert term.lower() in rich_text
        if case["fixture_id"] == "user_correction_contradiction":
            assert "paused" in rich_text and "abandoned" in rich_text
        if case["fixture_id"] == "silence_reactivation":
            assert "active again" in rich_text or "reactivating" in rich_text
            reactivated = await db.fetchone(
                """
                SELECT status, lifecycle_state
                FROM open_threads
                WHERE user_id=$1 AND lower(title)='gym return after silence'
                """,
                live_user,
            )
            assert reactivated
            assert reactivated["status"] == "open"
            assert reactivated["lifecycle_state"] == "active"

        await _assert_evidence_coverage(script_user)
        await _assert_evidence_coverage(live_user)


def _tokens(text: str) -> set[str]:
    stop = {
        "the", "and", "that", "this", "with", "into", "from", "while", "without",
        "user", "someone", "period", "context", "current", "trying", "through",
        "rather", "than", "because", "again", "about", "being", "become",
    }
    cleaned = "".join(ch.lower() if ch.isalnum() else " " for ch in text)
    return {t for t in cleaned.split() if len(t) >= 4 and t not in stop}


def test_fixture_rich_outputs_are_specific_not_boilerplate():
    cases = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    combined_by_case = {}
    for case in cases:
        expected = case["expected"]
        rich = " ".join(
            [
                expected["identity_who"],
                expected["identity_current_chapter"],
                expected["living_current_focus"],
                expected["living_primary_tension"],
            ]
        )
        lower_rich = rich.lower()
        for forbidden in [
            "important in the current memory context",
            "recent sessions contain a repeated high-signal unresolved thread",
            "to become steady without being misunderstood",
        ]:
            assert forbidden not in lower_rich
        for term in expected.get("specific_terms", []):
            assert term.lower() in lower_rich
        combined_by_case[case["fixture_id"]] = _tokens(rich)

    ids = list(combined_by_case)
    for i, left_id in enumerate(ids):
        for right_id in ids[i + 1:]:
            left = combined_by_case[left_id]
            right = combined_by_case[right_id]
            overlap = len(left & right) / max(1, len(left | right))
            assert overlap < 0.38, f"fixture prose too similar: {left_id} vs {right_id} overlap={overlap:.2f}"
