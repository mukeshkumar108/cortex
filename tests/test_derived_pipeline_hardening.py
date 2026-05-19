from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import uuid

import asyncpg
import pytest
from httpx import ASGITransport, AsyncClient

from src.config import get_settings
from src.derived_pipeline import (
    PASS1_5_ENTITIES,
    PASS1_TRIAGE,
    PASS3_THREADS,
    PASS4_IDENTITY,
    PASS5_LIVING_CONTEXT,
    PASS_RETROSPECTIVE_V1,
    build_pass4_identity_packet,
    build_pass5_living_packet,
    _write_relationship_link,
    run_pass1_5_entities,
    run_pass2_actionable,
    run_pass1_triage,
    run_pass3_threads,
    run_pass4_identity,
    run_pass5_living_context,
    run_proactive_shadow_candidates,
    run_retrospective_worker_v1,
    run_entity_audit,
    run_actionable_candidate_audit,
    run_session_change_audit,
    run_entity_candidate_audit,
    run_silence_detection,
    run_thread_audit,
    run_conservative_memory_audits,
)
import src.derived_pipeline as derived_pipeline
from src.derived_passes.pass4_identity import IDENTITY_SYNTHESIS_PROMPT, normalize_identity_output
from src.derived_passes.pass5_living_context import LIVING_CONTEXT_PROMPT, normalize_living_context_output
from src.derived_passes.pass1_triage import PASS1_PROMPT
from src.derived_passes.pass3_threads import THREAD_EXTRACTION_PROMPT
from src.derived_passes.synthesis_quality import conservative_rewrite_text, synthesis_quality_flags
from src.main import app, db, _build_handover_packet, _execute_post_ingest_hook, build_always_on_memory_packet, session_startbrief
from src import session


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "derived_pipeline" / "basic_six_pass.json"


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def _messages() -> list[dict]:
    fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    return fixture["messages"]


def test_pass4_pass5_prompts_keep_operational_constraints():
    pass1_prompt = PASS1_PROMPT.lower()
    pass3_prompt = THREAD_EXTRACTION_PROMPT.lower()
    identity_prompt = IDENTITY_SYNTHESIS_PROMPT.lower()
    living_prompt = LIVING_CONTEXT_PROMPT.lower()

    assert "your job is triage and routing" in pass1_prompt
    assert "later passes will go back to the raw transcript" in pass1_prompt
    assert "do not infer motives" in pass1_prompt
    assert "you are probably doing later-pass work too early" in pass1_prompt

    assert "routing only, not authority" in pass3_prompt
    assert "do not add emotional narration" in pass3_prompt
    assert "do not use it" in pass3_prompt
    assert "name the underlying situation" in pass3_prompt
    assert "neutral situation labels" in pass3_prompt

    assert "identify this person, not interpret them" in identity_prompt
    assert "do not synthesize a worldview" in identity_prompt
    assert "do not produce a philosophy" in identity_prompt
    assert "what are their durable roles" in identity_prompt
    assert "what are they building or working on" in identity_prompt
    assert "who matters to them and why" in identity_prompt
    assert "plain identification over synthesis" in identity_prompt
    assert "declared profile truth" in identity_prompt
    assert "durable profile facts" in identity_prompt
    assert "translate jargon into plain language" in identity_prompt
    assert "do not let lower-authority synthesis overwrite higher-authority facts" in identity_prompt
    assert "preserve that concrete meaning instead of abstracting it" in identity_prompt
    assert "do not rewrite it as \"wellness application\"" in identity_prompt
    assert "preserve concrete relationship-state terms exactly" in identity_prompt
    assert "\"estranged\" means estranged" in identity_prompt
    assert "\"long distance\" means long distance" in identity_prompt
    assert "not a character study" in identity_prompt
    assert "not a therapy note" in identity_prompt
    assert "raw user excerpts" in identity_prompt
    assert "do not dramatize the user" in identity_prompt
    assert "do not infer hidden motives" in identity_prompt
    assert "personality verdicts" in identity_prompt
    assert "trying to prove" in identity_prompt
    assert "if it is merely clever, flattering, dramatic, or poetic" in identity_prompt
    assert "what do they want, stated plainly and not inferred" in identity_prompt
    assert "do not freeze a hard season into identity" in identity_prompt

    assert "not a therapy note" in living_prompt
    assert "raw user excerpts" in living_prompt
    assert "do not invent hidden motives" in living_prompt
    assert "do not frame the user as tragic" in living_prompt
    assert "unspoken_goal should be null unless strongly supported" in living_prompt
    assert "sophie directives must be grounded in explicit" in living_prompt
    assert "would not help sophie respond better" in living_prompt
    assert "not a mood summary" in living_prompt
    assert "sophie needs current operating context, not emotional weather" in living_prompt


def test_pass4_validator_removes_personality_verdicts_and_motive_inference():
    parsed = {
        "who_they_are": (
            "Example User is someone who seeks deep complexity and is trying to prove he can survive. "
            "He explicitly asks for accurate memory and direct correction."
        ),
        "core_values": [
            {
                "value": "Accuracy",
                "evidence": "User explicitly asked for accurate memory. Beneath it all he wants rescue.",
                "confidence": 0.9,
            }
        ],
        "recurring_patterns": [
            {
                "pattern": "Uses direct correction when memory is wrong.",
                "evidence": "Repeated corrections across sessions.",
            }
        ],
        "family_history": "Family facts are present.",
        "faith_and_beliefs": "Faith appears in stated routine.",
        "what_they_want": "He wants to prove his capability through technical work. He wants accurate memory support.",
        "recurring_fears": [{"fear": "Driven by shame", "evidence": "thin inference"}],
        "what_they_avoid": "Avoids the weight of failure.",
        "how_they_relate": "He sends low-pressure messages when reconnecting.",
        "persistent_goals": [{"goal": "Daily walks", "evidence": "Repeatedly stated."}],
        "current_chapter": "Current work includes memory hardening and relationship repair.",
    }

    normalized = normalize_identity_output(parsed)
    text = json.dumps(normalized).lower()

    assert "someone who seeks" not in text
    assert "deep complexity" not in text
    assert "trying to prove" not in text
    assert "prove his capability" not in text
    assert "beneath it all" not in text
    assert "driven by" not in text
    assert "weight of" not in text
    assert normalized["who_they_are"] == "He explicitly asks for accurate memory and direct correction."
    assert normalized["what_they_want"] == "He wants accurate memory support."
    assert normalized["current_chapter"] == "Current work includes memory hardening and relationship repair."


def test_pass5_validator_suppresses_dramatic_language_weak_unspoken_goal_and_inferred_directives():
    parsed = {
        "current_focus": "The user is working on memory quality and seeded review behavior.",
        "recent_narrative": "The latest change seeded durable state before replay.",
        "relationship_pulse": "The vibe is a rollercoaster. Jordan is tracked as daughter; Riley is tracked as girlfriend.",
        "emotional_texture": "The vibe is intense. The user is frustrated by memory failures.",
        "primary_tension": "Trying to prove the system can work. The observable tension is whether Sophie can use stored memory without rediscovery.",
        "what_theyre_avoiding": "The weight of broken tools.",
        "unspoken_goal": "To become someone who can be relied on.",
        "why_it_matters": "It affects whether Sophie can respond accurately.",
        "active_contradictions": [
            {
                "topic": "memory",
                "earlier_view": "Sophie should know durable state.",
                "recent_view": "The review harness previously rediscovered from scratch.",
                "note": "Beneath it all, this is about trust.",
            }
        ],
        "sophie_directives": [
            {
                "directive": "Use explicit uncertainty when data is missing.",
                "reason": "User explicitly asked the assistant not to make things up.",
                "confidence": 1.0,
            },
            {
                "directive": "Be extra tender because of his care style.",
                "reason": "Inferred from care style.",
                "confidence": 0.8,
            },
        ],
    }

    normalized = normalize_living_context_output(parsed)
    text = json.dumps(normalized).lower()

    assert "rollercoaster" not in text
    assert "the vibe is" not in text
    assert "trying to prove" not in text
    assert "weight of" not in text
    assert "beneath it all" not in text
    assert normalized["unspoken_goal"] is None
    assert normalized["relationship_pulse"] == "Jordan is tracked as daughter; Riley is tracked as girlfriend."
    assert normalized["primary_tension"] == "The observable tension is whether Sophie can use stored memory without rediscovery."
    assert normalized["sophie_directives"] == [
        {
            "directive": "Use explicit uncertainty when data is missing.",
            "reason": "User explicitly asked the assistant not to make things up.",
            "confidence": 1.0,
        }
    ]


def test_synthesis_quality_flags_cover_banned_stock_phrases():
    flags = synthesis_quality_flags(
        "Beneath it all, the vibe is a rollercoaster driven by the weight of proof. "
        "He is trying to prove his capability while carrying guilt and shame."
    )
    assert "banned_phrase:beneath it all" in flags
    assert "banned_phrase:the vibe is" in flags
    assert "banned_phrase:rollercoaster" in flags
    assert "banned_phrase:driven by" in flags
    assert "banned_phrase:prove his capability" in flags
    assert "banned_phrase:guilt and shame" in flags


def test_synthesis_quality_rewrites_entity_profile_overreach_without_shape_change():
    profile = (
        "Jordan is the user's daughter, and she is someone he cares about deeply. "
        "They have been estranged for about six years. "
        "He carries a lot of guilt and shame about letting her down. "
        "Recently, they have begun a delicate reconciliation through Instagram. "
        "He holds onto many precious memories of her childhood."
    )

    rewritten = conservative_rewrite_text(profile)

    assert rewritten == (
        "They have been estranged for about six years. "
        "Recently, they have begun a delicate reconciliation through Instagram."
    )


def test_synthesis_quality_rewrites_mixed_clause_sentence_and_keeps_factual_tail():
    profile = (
        "Ashley is the user's long-term, long-distance girlfriend. "
        "Their relationship has been quite a rollercoaster lately; they went through a difficult breakup in late February."
    )

    rewritten = conservative_rewrite_text(profile)

    assert rewritten == (
        "Ashley is the user's long-term, long-distance girlfriend. "
        "they went through a difficult breakup in late February."
    )


@pytest.mark.asyncio
async def test_pass1_is_idempotent_and_noops_without_llm():
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = False
        tenant_id = "default"
        user_id = _unique("derived-user")
        session_id = _unique("derived-session")
        messages = _messages()
        await db.execute(
            """
            INSERT INTO session_transcript (
                tenant_id, session_id, user_id, messages, created_at, updated_at
            )
            VALUES ($1,$2,$3,$4::jsonb,NOW(),NOW())
            """,
            tenant_id,
            session_id,
            user_id,
            messages,
        )

        first = await run_pass1_triage(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            reference_time=datetime.now(timezone.utc),
            settings=settings,
        )
        second = await run_pass1_triage(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            reference_time=datetime.now(timezone.utc),
            settings=settings,
        )

        assert first.output_hash == second.output_hash
        assert first.run_entity_pass is False
        assert first.run_threads_pass is False

        classification = await db.fetchone(
            """
            SELECT raw_triage_output, context_relevant
            FROM session_classifications
            WHERE user_id=$1 AND session_id=$2
            """,
            user_id,
            session_id,
        )
        assert classification
        assert classification["context_relevant"] is False
        assert classification["raw_triage_output"]["source"] == "noop_fallback"
        assert classification["raw_triage_output"]["run_entity_pass"] is False
        assert classification["raw_triage_output"]["run_threads_pass"] is False

        assertions = await db.fetch(
            """
            SELECT surface, source_session_ids, source_turn_refs, memory_layer, semantic_category
            FROM derived_assertions
            WHERE user_id=$1 AND run_id=$2
            ORDER BY assertion_id
            """,
            user_id,
            first.run_id,
        )
        assert assertions == []

        runs = await db.fetch(
            """
            SELECT status, output_hash
            FROM pipeline_runs
            WHERE user_id=$1 AND pass_name=$2
            """,
            user_id,
            PASS1_TRIAGE,
        )
        assert len(runs) == 1
        assert runs[0]["status"] == "succeeded"


@pytest.mark.asyncio
async def test_disabled_llm_pipeline_does_not_fabricate_downstream_state():
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = False
        tenant_id = "default"
        user_id = _unique("derived-user")
        session_id = _unique("derived-session")
        messages = _messages()
        await db.execute(
            """
            INSERT INTO session_transcript (
                tenant_id, session_id, user_id, messages, created_at, updated_at
            )
            VALUES ($1,$2,$3,$4::jsonb,NOW(),NOW())
            """,
            tenant_id,
            session_id,
            user_id,
            messages,
        )

        pass1 = await run_pass1_triage(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            reference_time=datetime.now(timezone.utc),
            settings=settings,
        )
        assert pass1.should_run_identity is False
        assert pass1.should_run_living_context is False

        entity_count = await db.fetchval(
            "SELECT COUNT(*)::int FROM entity_profiles WHERE user_id=$1",
            user_id,
        )
        thread_count = await db.fetchval(
            "SELECT COUNT(*)::int FROM open_threads WHERE user_id=$1",
            user_id,
        )
        identity = await db.fetchone("SELECT assertions FROM identity_profile WHERE user_id=$1", user_id)
        living = await db.fetchone("SELECT assertions FROM living_context WHERE user_id=$1", user_id)
        assert int(entity_count or 0) == 0
        assert int(thread_count or 0) == 0
        assert identity is None
        assert living is None


@pytest.mark.asyncio
async def test_phase2_primitives_are_written_selectively_and_silence_is_daily_job_safe():
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = False
        tenant_id = "default"
        user_id = _unique("primitive-user")
        session_id = _unique("primitive-session")
        messages = [
            {
                "role": "user",
                "text": (
                    "Maybe my relationship with Riley is unclear. "
                    "Riley's birthday next week is something I should remember. "
                    "I need to follow up on the walking goal."
                ),
                "timestamp": "2026-04-21T10:00:00+00:00",
            }
        ]
        await db.execute(
            """
            INSERT INTO session_transcript (
                tenant_id, session_id, user_id, messages, created_at, updated_at
            )
            VALUES ($1,$2,$3,$4::jsonb,NOW(),NOW())
            """,
            tenant_id,
            session_id,
            user_id,
            messages,
        )

        pass1 = await run_pass1_triage(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            reference_time=datetime.now(timezone.utc),
            settings=settings,
        )

        event = await db.fetchone(
            """
            SELECT event_type, title, source_session_ids, source_turn_refs
            FROM memory_events
            WHERE user_id=$1 AND run_id=$2
            ORDER BY event_id DESC
            LIMIT 1
            """,
            user_id,
            pass1.run_id,
        )
        assert event is None

        low_conf = await db.fetchone(
            """
            SELECT surface, statement_text, source_session_ids, source_turn_refs
            FROM low_confidence_items
            WHERE user_id=$1 AND run_id=$2
            ORDER BY item_id DESC
            LIMIT 1
            """,
            user_id,
            pass1.run_id,
        )
        assert low_conf is None

        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases,
                status, relationship_to_user, profile_text, last_known_status,
                confidence, mention_count, first_seen_at, last_seen_at,
                source_session_ids, importance_score, salience_score
            ) VALUES (
                $1,'Jordan','jordan','person',$2::text[],'active','daughter',
                'Jordan is important.', 'quiet recently',
                0.9,5,NOW() - INTERVAL '90 days',NOW() - INTERVAL '45 days',
                $3::text[],0.95,0.9
            )
            ON CONFLICT (user_id, canonical_name_normalized) DO UPDATE
            SET last_seen_at=EXCLUDED.last_seen_at, salience_score=EXCLUDED.salience_score
            """,
            user_id,
            ["Jordan"],
            [session_id],
        )
        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases,
                status, relationship_to_user, profile_text, last_known_status,
                confidence, mention_count, first_seen_at, last_seen_at,
                source_session_ids, importance_score, salience_score
            ) VALUES (
                $1,'Synapse Repository','synapse repository','project',$2::text[],'active','tool',
                'A generic repository/system entity.', 'quiet recently',
                0.9,5,NOW() - INTERVAL '90 days',NOW() - INTERVAL '45 days',
                $3::text[],0.99,0.99
            )
            ON CONFLICT (user_id, canonical_name_normalized) DO UPDATE
            SET last_seen_at=EXCLUDED.last_seen_at, salience_score=EXCLUDED.salience_score
            """,
            user_id,
            ["Synapse Repository"],
            [session_id],
        )
        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases,
                status, relationship_to_user, profile_text, last_known_status,
                confidence, mention_count, first_seen_at, last_seen_at,
                source_session_ids, importance_score, salience_score
            ) VALUES (
                $1,'Bluum','bluum','project',$2::text[],'active','user_project',
                'Bluum is the user''s primary project and emotionally central work.', 'quiet recently',
                0.9,5,NOW() - INTERVAL '90 days',NOW() - INTERVAL '45 days',
                $3::text[],0.92,0.9
            )
            ON CONFLICT (user_id, canonical_name_normalized) DO UPDATE
            SET last_seen_at=EXCLUDED.last_seen_at, salience_score=EXCLUDED.salience_score
            """,
            user_id,
            ["Bluum"],
            [session_id],
        )
        await db.execute(
            """
            INSERT INTO open_threads (
                thread_id, user_id, title, detail, status, priority, category,
                source_session_ids, first_seen_at, last_updated_at, last_mentioned_at,
                lifecycle_state, evidence_turn_refs, salience_score, importance_score
            ) VALUES (
                $1,$2,'Walking goal','Walking goal has gone quiet.','open','medium','goal',
                $3::text[],NOW() - INTERVAL '90 days',NOW() - INTERVAL '45 days',
                NOW() - INTERVAL '45 days','active',$4::jsonb,0.85,0.8
            )
            ON CONFLICT (thread_id) DO NOTHING
            """,
            _unique("thread"),
            user_id,
            [session_id],
            [{"session_id": session_id, "turn_index": 0, "text": "User sent a no-expectation message to Riley."}],
        )

        written = await run_silence_detection(db=db, tenant_id=tenant_id)
        assert written >= 2
        silence_flags = await db.fetch(
            """
            SELECT target_type, target_name, silence_days, status
            FROM memory_silence_flags
            WHERE user_id=$1 AND status='active'
            ORDER BY target_type, target_name
            """,
            user_id,
        )
        assert {row["target_type"] for row in silence_flags} == {"entity", "thread"}
        names = {row["target_name"] for row in silence_flags}
        assert {"Jordan", "Bluum", "Walking goal"} <= names
        assert "Synapse Repository" not in names
        assert all(row["silence_days"] >= 30 for row in silence_flags)


@pytest.mark.asyncio
async def test_pass1_does_not_persist_low_confidence_extractions(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        tenant_id = "default"
        user_id = _unique("lowconf-user")
        session_id = _unique("lowconf-session")
        messages = [
            {
                "role": "user",
                "text": (
                    "Riley might be close again, but I do not know what we are. "
                    "Bluum work feels loaded, Jordan's role is hard to name, "
                    "and I keep joking around HMRC letters instead of opening them."
                ),
                "timestamp": "2026-04-21T10:00:00+00:00",
            }
        ]

        async def _stub_pass1(messages, model):
            return {
                "is_memory_worthy": True,
                "session_kind": "personal",
                "entity_mentions": ["Riley", "Bluum", "Jordan", "HMRC"],
                "thread_signals": ["Hold unresolved signals gently without treating them as facts."],
                "emotional_weight": "medium",
                "emotional_note": "Careful and unresolved.",
                "tension_signal": "The user might be avoiding work pressure, but this career tension is ambiguous.",
                "context_relevant": True,
                "run_entity_pass": True,
                "run_threads_pass": True,
                "identity_relevant": True,
            }

        monkeypatch.setattr(derived_pipeline, "run_rich_pass1_llm", _stub_pass1)
        await db.execute(
            """
            INSERT INTO session_transcript (
                tenant_id, session_id, user_id, messages, created_at, updated_at
            )
            VALUES ($1,$2,$3,$4::jsonb,NOW(),NOW())
            """,
            tenant_id,
            session_id,
            user_id,
            messages,
        )
        pass1 = await run_pass1_triage(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            reference_time=datetime.now(timezone.utc),
            settings=settings,
        )

        rows = await db.fetch(
            """
            SELECT surface, statement_text, question_text, metadata, source_session_ids, source_turn_refs
            FROM low_confidence_items
            WHERE user_id=$1 AND run_id=$2
            ORDER BY item_id
            """,
            user_id,
            pass1.run_id,
        )
        assert rows == []

        startbrief = await session_startbrief(
            tenantId=tenant_id,
            userId=user_id,
            sessionId=session_id,
            now=datetime.now(timezone.utc).isoformat(),
            timezone="UTC",
        )
        start_payload = startbrief.model_dump()
        guidance = start_payload["ops_context"]["assistant_guidance"]
        assert not any(item.get("type") == "low_confidence" for item in guidance)

        handover = await _build_handover_packet(user_id)
        assert not any(item.get("reason") == "low_confidence_queue" for item in handover["sophie_directives"])
        surfaced_text = json.dumps(
            {
                "threads": handover.get("open_threads"),
                "contradictions": handover.get("active_contradictions"),
            },
            ensure_ascii=False,
        )
        assert "Maybe Riley is becoming a partner again" not in surfaced_text


@pytest.mark.asyncio
async def test_pass1_hook_enqueues_async_followup_jobs_only():
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = False
        settings.derived_pipeline_enabled = True
        tenant_id = "default"
        user_id = _unique("derived-user")
        session_id = _unique("derived-session")
        messages = _messages()
        await db.execute(
            """
            INSERT INTO session_transcript (
                tenant_id, session_id, user_id, messages, created_at, updated_at
            )
            VALUES ($1,$2,$3,$4::jsonb,NOW(),NOW())
            """,
            tenant_id,
            session_id,
            user_id,
            messages,
        )

        ok = await _execute_post_ingest_hook(
            "pass1_triage",
            {
                "tenant_id": tenant_id,
                "user_id": user_id,
                "session_id": session_id,
                "reference_time": datetime.now(timezone.utc).isoformat(),
            },
        )
        assert ok is True

        queued = await db.fetch(
            """
            SELECT payload->>'hook' AS hook
            FROM graphiti_outbox
            WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
              AND job_type='post_ingest_hook'
              AND status='pending'
            ORDER BY id
            """,
            tenant_id,
            user_id,
            session_id,
        )
        hooks = {row["hook"] for row in queued}
        triage_row = await db.fetchone(
            """
            SELECT raw_triage_output
            FROM session_classifications
            WHERE session_id=$1 AND user_id=$2
            """,
            session_id,
            user_id,
        ) or {}
        raw = triage_row.get("raw_triage_output") if isinstance(triage_row, dict) else {}
        run_actionable = bool((raw or {}).get("run_actionable_pass")) if isinstance(raw, dict) else False
        identity_relevant = bool((raw or {}).get("identity_relevant")) if isinstance(raw, dict) else False
        context_relevant = bool((raw or {}).get("context_relevant")) if isinstance(raw, dict) else False
        required = set()
        if run_actionable:
            required.add(session.POST_INGEST_HOOK_PASS2_ACTIONABLE)
        if bool((raw or {}).get("run_session_changes_pass")) if isinstance(raw, dict) else False:
            required.add(session.POST_INGEST_HOOK_PASS2B_SESSION_CHANGES)
        if bool((raw or {}).get("run_entity_pass")) if isinstance(raw, dict) else False:
            required.add(session.POST_INGEST_HOOK_PASS2C_ENTITY_CANDIDATES)
        if bool((raw or {}).get("run_threads_pass")) if isinstance(raw, dict) else False:
            required.add(PASS3_THREADS)
        if identity_relevant:
            required.add(PASS4_IDENTITY)
        if context_relevant:
            required.add(PASS5_LIVING_CONTEXT)
        assert required.issubset(hooks)


@pytest.mark.asyncio
async def test_pass1_triage_does_not_persist_actionable_candidates():
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = False
        settings.derived_pipeline_enabled = True
        tenant_id = "default"
        user_id = _unique("derived-user")
        session_id = _unique("derived-session")
        messages = [
            {"role": "user", "text": "Remind me to call John tomorrow.", "timestamp": "2026-04-25T08:00:00Z"},
        ]

        await run_pass1_triage(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            reference_time=datetime.now(timezone.utc),
            settings=settings,
        )

        count = await db.fetchval(
            """
            SELECT COUNT(*)::int
            FROM actionable_candidates
            WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
            """,
            tenant_id,
            user_id,
            session_id,
        )
        assert int(count or 0) == 0


@pytest.mark.asyncio
async def test_pass2_actionable_requires_llm_and_does_not_use_heuristics():
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = False
        settings.derived_pipeline_enabled = True
        tenant_id = "default"
        user_id = _unique("derived-user")
        session_id = _unique("derived-session")
        messages = [
            {"role": "user", "text": "Remind me to call John tomorrow.", "timestamp": "2026-04-25T08:00:00Z"},
        ]

        await run_pass2_actionable(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            reference_time=datetime.now(timezone.utc),
            settings=settings,
        )

        count = await db.fetchval(
            """
            SELECT COUNT(*)::int
            FROM actionable_candidates
            WHERE tenant_id=$1 AND user_id=$2 AND session_id=$3
            """,
            tenant_id,
            user_id,
            session_id,
        )
        assert int(count or 0) == 0


@pytest.mark.asyncio
async def test_pass1_5_entity_integrity_guards(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        tenant_id = "default"
        user_id = _unique("derived-user")
        session_id = _unique("entity-session")
        messages = [
            {"role": "user", "text": "Sophie, Riley is my girlfriend and we are trying to stay steady.", "timestamp": "2026-04-21T10:00:00Z"},
            {"role": "user", "text": "I need Sophie to stop mixing assistant memory with real people.", "timestamp": "2026-04-21T10:01:00Z"},
        ]
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy, session_kind,
                one_line_summary, entity_mentions, run_entity_pass, run_threads_pass,
                identity_relevant, emotional_weight, processed_at, model_used,
                raw_triage_output, context_relevant
            )
            VALUES ($1,$2,NOW(),true,'personal','entities',$3::text[],true,false,false,
                    'medium',NOW(),'fixture',$4::jsonb,true)
            """,
            session_id,
            user_id,
            ["Sophie", "Riley"],
            {"entity_mentions": ["Sophie", "Riley"]},
        )

        async def _entity_stub(existing_entities, mentions, model):
            return [
                {
                    "decision": "NEW",
                    "mention": "Sophie",
                    "canonical_name": "Sophie",
                    "type": "person",
                    "status": "active",
                    "relationship_to_user": "friend",
                    "aliases": ["Sophie"],
                    "confidence": 0.95,
                },
                {
                    "decision": "NEW",
                    "mention": "Riley",
                    "canonical_name": "Riley",
                    "type": "person",
                    "status": "active",
                    "relationship_to_user": "friend",
                    "aliases": ["Riley"],
                    "confidence": 0.95,
                },
            ]

        profile_calls = []

        async def _profile_stub(canonical_name, entity_type, relationship_to_user, messages, existing_profile_text, model):
            profile_calls.append(
                {
                    "canonical_name": canonical_name,
                    "entity_type": entity_type,
                    "relationship_to_user": relationship_to_user,
                    "messages": messages,
                }
            )
            return {
                "profile_text": f"{canonical_name} profile",
                "key_facts": [{"fact": f"{canonical_name} fact", "confidence": 0.8}],
                "open_questions": [],
                "last_known_status": "active",
            }

        monkeypatch.setattr(derived_pipeline, "resolve_entity_mentions", _entity_stub, raising=True)
        monkeypatch.setattr(derived_pipeline, "build_entity_profile", _profile_stub, raising=True)

        await run_pass1_5_entities(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            settings=settings,
        )

        sophie = await db.fetchone(
            """
            SELECT type, relationship_to_user, profile_text
            FROM entity_profiles
            WHERE user_id=$1 AND canonical_name_normalized='sophie'
            """,
            user_id,
        )
        riley = await db.fetchone(
            """
            SELECT type, relationship_to_user, profile_text
            FROM entity_profiles
            WHERE user_id=$1 AND canonical_name_normalized='riley'
            """,
            user_id,
        )
        assert sophie["type"] == "assistant"
        assert sophie["relationship_to_user"] == "assistant"
        assert sophie["profile_text"] is None
        assert riley["relationship_to_user"] == "girlfriend"
        assert riley["profile_text"] == "Riley profile"
        assert [call["canonical_name"] for call in profile_calls] == ["Riley"]
        assert all("Riley" in msg["text"] for msg in profile_calls[0]["messages"])


@pytest.mark.asyncio
async def test_pass1_5_weak_entity_stays_tentative_without_profile(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        tenant_id = "default"
        user_id = _unique("derived-user")
        session_id = _unique("weak-entity-session")
        messages = [
            {
                "role": "user",
                "text": "I bumped into Nova at a meetup, but I don't really know if they matter yet.",
                "timestamp": "2026-04-21T10:00:00Z",
            }
        ]
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy, session_kind,
                one_line_summary, entity_mentions, run_entity_pass, run_threads_pass,
                identity_relevant, emotional_weight, processed_at, model_used,
                raw_triage_output, context_relevant
            )
            VALUES ($1,$2,NOW(),true,'personal','weak entity',$3::text[],true,false,false,
                    'low',NOW(),'fixture',$4::jsonb,true)
            """,
            session_id,
            user_id,
            ["Nova"],
            {"entity_mentions": ["Nova"]},
        )

        async def _entity_stub(existing_entities, mentions, model):
            return [
                {
                    "decision": "NEW",
                    "mention": "Nova",
                    "canonical_name": "Nova",
                    "type": "person",
                    "status": "active",
                    "relationship_to_user": "other",
                    "confidence": 0.42,
                    "evidence_strength": "weak",
                    "memory_relevance": "low",
                    "relationship_confidence": 0.1,
                    "why_this_matters": "The user mentioned Nova once but said the significance is unclear.",
                }
            ]

        profile_calls = []

        async def _profile_stub(canonical_name, entity_type, relationship_to_user, messages, existing_profile_text, model):
            profile_calls.append(canonical_name)
            return {"profile_text": "should not be written", "key_facts": [], "open_questions": [], "last_known_status": None}

        monkeypatch.setattr(derived_pipeline, "resolve_entity_mentions", _entity_stub, raising=True)
        monkeypatch.setattr(derived_pipeline, "build_entity_profile", _profile_stub, raising=True)

        await run_pass1_5_entities(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            settings=settings,
        )

        entity = await db.fetchone(
            """
            SELECT status, confidence, profile_text, key_facts, open_questions
            FROM entity_profiles
            WHERE user_id=$1 AND canonical_name_normalized='nova'
            """,
            user_id,
        )
        assert entity["status"] == "tentative"
        assert entity["confidence"] <= 0.55
        assert entity["profile_text"] is None
        assert entity["key_facts"] == []
        assert entity["open_questions"] == []
        assert profile_calls == []


@pytest.mark.asyncio
async def test_pass1_5_tiered_relationship_seeds_scores_and_promotes_core_anchor(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        tenant_id = "default"
        user_id = _unique("tiered-entity-user")
        session_id = _unique("tiered-entity-session")
        messages = [
            {
                "role": "user",
                "text": "Jordan is my daughter and she is central to my life decisions.",
                "timestamp": "2026-04-21T10:00:00Z",
            }
        ]
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy, session_kind,
                one_line_summary, entity_mentions, run_entity_pass, run_threads_pass,
                identity_relevant, emotional_weight, processed_at, model_used,
                raw_triage_output, context_relevant
            )
            VALUES ($1,$2,NOW(),true,'personal','tiered entity',$3::text[],true,false,true,
                    'medium',NOW(),'fixture',$4::jsonb,true)
            """,
            session_id,
            user_id,
            ["Jordan"],
            {"entity_mentions": ["Jordan"]},
        )

        async def _entity_stub(existing_entities, mentions, model):
            return [
                {
                    "decision": "NEW",
                    "mention": "Jordan",
                    "canonical_name": "Jordan",
                    "type": "person",
                    "status": "tentative",
                    "relationship_to_user": "daughter",
                    "confidence": 0.6,
                    "evidence_strength": "medium",
                    "memory_relevance": "high",
                    "relationship_confidence": 0.8,
                    "aliases": ["Jordan"],
                }
            ]

        async def _profile_stub(canonical_name, entity_type, relationship_to_user, messages, existing_profile_text, model):
            return {
                "profile_text": f"{canonical_name} profile",
                "key_facts": [],
                "open_questions": [],
                "last_known_status": None,
            }

        monkeypatch.setattr(derived_pipeline, "resolve_entity_mentions", _entity_stub, raising=True)
        monkeypatch.setattr(derived_pipeline, "build_entity_profile", _profile_stub, raising=True)

        await run_pass1_5_entities(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            settings=settings,
        )

        entity = await db.fetchone(
            """
            SELECT relationship_to_user, status, salience_score, importance_score
            FROM entity_profiles
            WHERE user_id=$1 AND canonical_name_normalized='jordan'
            """,
            user_id,
        )
        assert entity["relationship_to_user"] == "daughter"
        assert entity["status"] == "active"
        assert float(entity["salience_score"] or 0.0) >= 0.8
        assert float(entity["importance_score"] or 0.0) >= 0.9


@pytest.mark.asyncio
async def test_empty_entities_do_not_surface_in_startbrief_or_handover():
    async with app.router.lifespan_context(app):
        user_id = _unique("empty-entity-user")
        session_id = _unique("empty-entity-session")
        await db.execute(
            """
            INSERT INTO session_transcript (tenant_id, session_id, user_id, messages, created_at, updated_at)
            VALUES ('default',$1,$2,$3::jsonb,NOW(),NOW())
            """,
            session_id,
            user_id,
            [{"role": "user", "text": "I mentioned Nova once.", "timestamp": "2026-04-21T10:00:00Z"}],
        )
        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases,
                status, relationship_to_user, profile_text, key_facts, open_questions,
                confidence, mention_count, first_seen_at, last_seen_at,
                source_session_ids, salience_score, importance_score
            ) VALUES (
                $1,'Nova','nova','person',$2::text[],'active','other',NULL,'[]'::jsonb,'[]'::jsonb,
                0.5,1,NOW(),NOW(),$3::text[],0.9,0.9
            )
            ON CONFLICT (user_id, canonical_name_normalized) DO NOTHING
            """,
            user_id,
            ["Nova"],
            [session_id],
        )

        startbrief = await session_startbrief(
            tenantId="default",
            userId=user_id,
            now=datetime.now(timezone.utc).isoformat(),
            sessionId=session_id,
            timezone="UTC",
        )
        handover = await _build_handover_packet(user_id)

        assert "Nova" not in [hint.name for hint in startbrief.entity_hints]
        assert "Nova" not in [person["name"] for person in handover["people"]]


@pytest.mark.asyncio
async def test_active_project_entities_do_not_surface_as_people():
    async with app.router.lifespan_context(app):
        user_id = _unique("project-entity-user")
        session_id = _unique("project-entity-session")
        await db.execute(
            """
            INSERT INTO session_transcript (tenant_id, session_id, user_id, messages, created_at, updated_at)
            VALUES ('default',$1,$2,$3::jsonb,NOW(),NOW())
            """,
            session_id,
            user_id,
            [{"role": "user", "text": "Working on the Sophie Repository today.", "timestamp": "2026-04-21T10:00:00Z"}],
        )
        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases,
                status, relationship_to_user, profile_text, key_facts, open_questions,
                confidence, mention_count, first_seen_at, last_seen_at,
                source_session_ids, salience_score, importance_score
            ) VALUES (
                $1,'Sophie Repository','sophie repository','person',$2::text[],'active','active_project',
                'Sophie Repository is an active project.','[]'::jsonb,'[]'::jsonb,
                0.9,4,NOW(),NOW(),$3::text[],0.9,0.9
            )
            ON CONFLICT (user_id, canonical_name_normalized) DO NOTHING
            """,
            user_id,
            ["Sophie Repository"],
            [session_id],
        )

        startbrief = await session_startbrief(
            tenantId="default",
            userId=user_id,
            now=datetime.now(timezone.utc).isoformat(),
            sessionId=session_id,
            timezone="UTC",
        )
        handover = await _build_handover_packet(user_id)

        hints = {hint.name: hint for hint in startbrief.entity_hints}
        assert hints["Sophie Repository"].type == "project"
        assert "Sophie Repository" not in [person["name"] for person in handover["people"]]


@pytest.mark.asyncio
async def test_derived_pipeline_schema_exists():
    async with app.router.lifespan_context(app):
        conn = await asyncpg.connect(db.settings.get_database_url())
        try:
            for table in ["pipeline_runs", "derived_assertions", "derived_quarantine", "consolidated_insights"]:
                regclass = await conn.fetchval("SELECT to_regclass($1)", f"public.{table}")
                assert regclass == table

            cols = await conn.fetch(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name='open_threads'
                """
            )
            names = {row["column_name"] for row in cols}
            assert {"lifecycle_state", "evidence_turn_refs", "memory_layer", "access_count"}.issubset(names)
        finally:
            await conn.close()


@pytest.mark.asyncio
async def test_write_relationship_link_backwards_compatible_defaults():
    async with app.router.lifespan_context(app):
        user_id = _unique("link-user")

        await _write_relationship_link(
            db=db,
            tenant_id="default",
            user_id=user_id,
            source_type="entity",
            source_id="jordan",
            target_type="session",
            target_id="sess-1",
            relationship_type="mentioned_in",
            source_session_ids=["sess-1"],
            source_turn_refs=[{"session_id": "sess-1", "turn_index": 0}],
            confidence=0.7,
            metadata={"entity_name": "Jordan"},
        )

        row = await db.fetchone(
            """
            SELECT source_domain, target_domain, status, strength, valid_from, valid_until, expires_at, confidence
            FROM memory_relationship_links
            WHERE tenant_id='default' AND user_id=$1
            """,
            user_id,
        )
        assert row["source_domain"] is None
        assert row["target_domain"] is None
        assert row["status"] == "active"
        assert row["strength"] is None
        assert row["valid_from"] is None
        assert row["valid_until"] is None
        assert row["expires_at"] is None
        assert float(row["confidence"] or 0.0) == 0.7


@pytest.mark.asyncio
async def test_write_relationship_link_supports_domains_lifecycle_and_upsert_merge():
    async with app.router.lifespan_context(app):
        user_id = _unique("link-user")
        valid_from = datetime.now(timezone.utc) - timedelta(days=2)
        valid_until = datetime.now(timezone.utc) + timedelta(days=5)
        expires_at = datetime.now(timezone.utc) + timedelta(days=7)

        await _write_relationship_link(
            db=db,
            tenant_id="default",
            user_id=user_id,
            source_domain="workstream",
            source_type="thread",
            source_id="thread-1",
            target_domain="people",
            target_type="entity",
            target_id="riley",
            relationship_type="involves",
            source_session_ids=["sess-1"],
            source_turn_refs=[{"session_id": "sess-1", "turn_index": 0}],
            status="detected",
            confidence=0.4,
            strength=0.65,
            valid_from=valid_from,
            valid_until=valid_until,
            expires_at=expires_at,
            metadata={"entity_name": "Riley"},
        )
        await _write_relationship_link(
            db=db,
            tenant_id="default",
            user_id=user_id,
            source_domain="workstream",
            source_type="thread",
            source_id="thread-1",
            target_domain="people",
            target_type="entity",
            target_id="riley",
            relationship_type="involves",
            source_session_ids=["sess-2", "sess-1"],
            source_turn_refs=[{"session_id": "sess-2", "turn_index": 1}],
            confidence=0.9,
            metadata={"entity_name": "Riley v2"},
        )

        row = await db.fetchone(
            """
            SELECT source_domain, target_domain, status, strength, valid_from, valid_until, expires_at,
                   confidence, source_session_ids, source_turn_refs, metadata
            FROM memory_relationship_links
            WHERE tenant_id='default' AND user_id=$1
              AND source_type='thread' AND source_id='thread-1'
              AND target_type='entity' AND target_id='riley'
              AND relationship_type='involves'
            """,
            user_id,
        )
        assert row["source_domain"] == "workstream"
        assert row["target_domain"] == "people"
        assert row["status"] == "detected"
        assert float(row["strength"] or 0.0) == 0.65
        assert row["valid_from"] == valid_from
        assert row["valid_until"] == valid_until
        assert row["expires_at"] == expires_at
        assert float(row["confidence"] or 0.0) == 0.9
        assert set(row["source_session_ids"]) == {"sess-1", "sess-2"}
        assert len(row["source_turn_refs"]) == 2
        assert row["metadata"] == {"entity_name": "Riley"}


@pytest.mark.asyncio
async def test_pass3_thread_entity_links_write_domains(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        tenant_id = "default"
        user_id = _unique("thread-link-user")
        session_id = _unique("thread-link-session")
        text = "I need to keep coordinating with Riley because this situation is still open."
        messages = [{"role": "user", "text": text, "timestamp": "2026-04-21T10:00:00Z"}]
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy, session_kind,
                one_line_summary, entity_mentions, run_entity_pass, run_threads_pass,
                identity_relevant, emotional_weight, processed_at, model_used,
                raw_triage_output, context_relevant
            )
            VALUES ($1,$2,NOW(),true,'personal','thread link',$3::text[],false,true,false,
                    'high',NOW(),'fixture',$4::jsonb,true)
            """,
            session_id,
            user_id,
            ["Riley"],
            {"thread_signals": [text]},
        )

        async def _actions_stub(**_kwargs):
            return [
                {
                    "action": "CREATE",
                    "title": "Coordinate with Riley",
                    "detail": text,
                    "category": "relationship",
                    "priority": "high",
                    "related_entities": ["Riley"],
                    "unresolvedness": "open",
                    "follow_up_value": "high",
                    "evidence_strength": "strong",
                    "why_this_matters_later": "Riley remains part of the active situation.",
                }
            ]

        monkeypatch.setattr(derived_pipeline, "extract_thread_actions", _actions_stub, raising=True)

        await run_pass3_threads(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            settings=settings,
        )

        row = await db.fetchone(
            """
            SELECT source_domain, target_domain, status, source_type, target_type, relationship_type
            FROM memory_relationship_links
            WHERE tenant_id=$1 AND user_id=$2
            ORDER BY link_id DESC
            LIMIT 1
            """,
            tenant_id,
            user_id,
        )
        assert row["source_domain"] == "workstream"
        assert row["target_domain"] == "people"
        assert row["status"] == "active"
        assert row["source_type"] == "thread"
        assert row["target_type"] == "entity"
        assert row["relationship_type"] == "involves"


@pytest.mark.asyncio
async def test_pass4_and_pass5_write_rich_synthesis_fields(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        tenant_id = "default"
        user_id = _unique("derived-user")
        session_id = _unique("identity-session")
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy, session_kind,
                one_line_summary, entity_mentions, run_entity_pass, run_threads_pass,
                identity_relevant, emotional_weight, emotional_note, tension_signal,
                processed_at, model_used, raw_triage_output, context_relevant
            )
            VALUES (
                $1,$2,NOW(),true,'personal','identity signal','{}'::text[],false,true,
                true,'medium','emotionally loaded','avoids naming the deeper stakes',
                NOW(),'fixture',$3::jsonb,true
            )
            """,
            session_id,
            user_id,
            {
                "memory_deltas": ["User is trying to become more consistent."],
                "identity_signals": ["User values integrity over approval."],
                "thread_signals": ["User keeps returning to the walking goal."],
            },
        )

        async def _identity_stub(**_kwargs):
            return {
                "who_they_are": "A person rebuilding from integrity rather than approval.",
                "core_values": [{"value": "integrity", "evidence": "identity signal", "confidence": 0.9}],
                "recurring_patterns": [{"pattern": "uses work as regulation", "evidence": "memory deltas"}],
                "family_history": "Family history remains contextually important.",
                "faith_and_beliefs": "Faith appears as grounding.",
                "what_they_want": "To be steady and present.",
                "recurring_fears": [{"fear": "failure", "evidence": "identity signal", "confidence": 0.7}],
                "what_they_avoid": "Naming the deeper stakes directly.",
                "how_they_relate": "Cares through responsibility.",
                "persistent_goals": [{"goal": "daily walks", "evidence": "thread signal"}],
                "current_chapter": "A period of reconstruction.",
            }

        async def _living_stub(**_kwargs):
            return {
                "current_focus": "Becoming consistent while holding relational pressure.",
                "recent_narrative": "The same walking and consistency signal has returned.",
                "relationship_pulse": "Relationships are present as pressure and motivation.",
                "emotional_texture": "Determined but strained.",
                "primary_tension": "Wanting change while avoiding the deeper stakes.",
                "what_theyre_avoiding": "Naming what failure would mean.",
                "unspoken_goal": "To become someone who can be relied on.",
                "why_it_matters": "This is about identity, not only tasks.",
                "active_contradictions": [{"topic": "consistency", "earlier_view": "wants it", "recent_view": "still stuck"}],
                "sophie_directives": [
                    {
                        "directive": "do not flatten this into productivity advice",
                        "reason": "User explicitly asked for this.",
                        "confidence": 0.9,
                    }
                ],
            }

        monkeypatch.setattr(derived_pipeline, "synthesize_identity_profile", _identity_stub, raising=True)
        monkeypatch.setattr(derived_pipeline, "synthesize_living_context", _living_stub, raising=True)

        await run_pass4_identity(db=db, tenant_id=tenant_id, user_id=user_id, settings=settings)
        await run_pass5_living_context(db=db, tenant_id=tenant_id, user_id=user_id, settings=settings)

        identity = await db.fetchone(
            "SELECT who_they_are, core_values, current_chapter, assertions FROM identity_profile WHERE user_id=$1",
            user_id,
        )
        living = await db.fetchone(
            """
            SELECT current_focus, primary_tension, relationship_pulse, emotional_texture,
                   sophie_directives, active_contradictions, assertions
            FROM living_context
            WHERE user_id=$1
            """,
            user_id,
        )
        assert identity["who_they_are"] == "A person rebuilding from integrity rather than approval."
        assert identity["current_chapter"] == "A period of reconstruction."
        assert identity["core_values"][0]["value"] == "integrity"
        assert identity["assertions"] == []
        assert living["current_focus"] == "Becoming consistent while holding relational pressure."
        assert living["primary_tension"] == "Wanting change while avoiding the deeper stakes."
        assert living["sophie_directives"][0]["directive"] == "do not flatten this into productivity advice"
        assert living["active_contradictions"][0]["topic"] == "consistency"
        assert living["assertions"] == []


@pytest.mark.asyncio
async def test_pass5_uses_manual_pipeline_evidence_filters(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        tenant_id = "default"
        user_id = _unique("derived-user")

        for idx, days_ago in enumerate([2, 7, 45, 75]):
            await db.execute(
                """
                INSERT INTO session_classifications (
                    session_id, user_id, session_date, is_memory_worthy, session_kind,
                    one_line_summary, entity_mentions, run_entity_pass, run_threads_pass,
                    identity_relevant, emotional_weight, emotional_note, tension_signal,
                    processed_at, model_used, raw_triage_output, context_relevant
                )
                VALUES (
                    $1,$2,NOW() - ($3::text || ' days')::interval,true,'personal',$4,
                    '{}'::text[],false,true,false,'medium','note','tension',
                    NOW(),'fixture',$5::jsonb,true
                )
                """,
                f"{user_id}-session-{idx}",
                user_id,
                str(days_ago),
                f"summary {idx}",
                {
                    "memory_deltas": [f"delta {idx}"],
                    "thread_signals": [f"thread signal {idx}"],
                },
            )

        await db.execute(
            """
            INSERT INTO open_threads (
                thread_id, user_id, title, detail, status, priority, category,
                thread_type, source_session_ids, salience_score, created_at, last_mentioned_at
            )
            VALUES
              ($1,$3,'High thread','Important thread','open','high','relationship','situational','{}'::text[],0.8,NOW(),NOW()),
              ($2,$3,'Low thread','Low signal thread','open','low','other','situational','{}'::text[],0.2,NOW(),NOW())
            """,
            f"{user_id}-thread-high",
            f"{user_id}-thread-low",
            user_id,
        )
        await db.execute(
            """
            UPDATE open_threads
            SET source_session_ids=$2::text[],
                evidence_turn_refs=$3::jsonb,
                distinct_session_count=1
            WHERE user_id=$1
            """,
            user_id,
            [f"{user_id}-session-0"],
            [{"session_id": f"{user_id}-session-0", "turn_index": 0}],
        )
        long_profile = "Riley is the user's girlfriend. " + ("extra " * 80)
        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases,
                status, relationship_to_user, profile_text, last_known_status,
                confidence, mention_count, first_seen_at, last_seen_at,
                source_session_ids, salience_score
            )
            VALUES
              ($1,'Riley','riley','person','{}'::text[],'active','girlfriend',$2,'steady',0.9,3,NOW(),NOW(),$3::text[],0.9),
              ($1,'Jordan','jordan','person','{}'::text[],'tentative','daughter','tentative profile','unclear',0.7,1,NOW(),NOW(),'{}'::text[],0.95),
              ($1,'Sophie','sophie','assistant','{}'::text[],'active','assistant','assistant profile','active',0.9,8,NOW(),NOW(),'{}'::text[],1.0),
              ($1,'Old Person','old person','person','{}'::text[],'active','friend','old profile','stale',0.9,5,NOW(),NOW() - interval '45 days','{}'::text[],0.9)
            ON CONFLICT (user_id, canonical_name_normalized) DO NOTHING
            """,
            user_id,
            long_profile,
            [f"{user_id}-session-0"],
        )

        captured = {}

        async def _living_stub(**kwargs):
            captured.update(kwargs)
            return {
                "current_focus": "Grounded current focus.",
                "recent_narrative": "Recent narrative.",
                "relationship_pulse": "Relationship pulse.",
                "emotional_texture": "Texture.",
                "primary_tension": "Tension.",
                "what_theyre_avoiding": "Avoidance.",
                "unspoken_goal": "Goal.",
                "why_it_matters": "Reason.",
                "active_contradictions": [],
                "sophie_directives": [],
            }

        monkeypatch.setattr(derived_pipeline, "synthesize_living_context", _living_stub, raising=True)

        await run_pass5_living_context(db=db, tenant_id=tenant_id, user_id=user_id, settings=settings)

        session_ids = [row["session_id"] for row in captured["recent_sessions"]]
        assert f"{user_id}-session-0" in session_ids
        assert f"{user_id}-session-1" in session_ids
        assert f"{user_id}-session-2" in session_ids
        assert f"{user_id}-session-3" not in session_ids
        assert [row["title"] for row in captured["open_threads"]] == ["High thread"]
        assert [row["canonical_name"] for row in captured["active_entities"]] == ["Riley"]
        assert len(captured["active_entities"][0]["profile_text"]) <= 200


@pytest.mark.asyncio
async def test_invalid_thread_lifecycle_transition_is_quarantined(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        tenant_id = "default"
        user_id = _unique("derived-user")
        session_id = _unique("thread-session")
        thread_id = _unique("thread")
        messages = _messages()
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy, session_kind,
                one_line_summary, entity_mentions, run_entity_pass, run_threads_pass,
                identity_relevant, emotional_weight, processed_at, model_used,
                raw_triage_output, context_relevant
            )
            VALUES ($1,$2,NOW(),true,'personal','thread','{}'::text[],false,true,false,
                    'medium',NOW(),'fixture',$3::jsonb,true)
            """,
            session_id,
            user_id,
            {"thread_signals": ["User says the resolved thread is active again without new evidence."]},
        )
        await db.execute(
            """
            INSERT INTO open_threads (
                thread_id, user_id, title, status, lifecycle_state, source_session_ids, created_at
            )
            VALUES ($1,$2,'Resolved thread','resolved','resolved','{}'::text[],NOW())
            """,
            thread_id,
            user_id,
        )

        async def _actions_stub(**_kwargs):
            return [{"action": "UPDATE", "thread_id": thread_id, "detail": "unsafe reopen"}]

        monkeypatch.setattr(derived_pipeline, "extract_thread_actions", _actions_stub, raising=True)

        await run_pass3_threads(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            settings=settings,
        )

        quarantine = await db.fetchone(
            """
            SELECT reason_code, payload
            FROM derived_quarantine
            WHERE user_id=$1 AND pass_name='pass3_threads'
            ORDER BY quarantine_id DESC
            LIMIT 1
            """,
            user_id,
        )
        assert quarantine
        assert quarantine["reason_code"] == "invalid_lifecycle_transition"


@pytest.mark.asyncio
async def test_pass3_rejects_weak_one_off_thread_create(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        tenant_id = "default"
        user_id = _unique("derived-user")
        session_id = _unique("weak-thread-session")
        messages = [
            {
                "role": "user",
                "text": "I might make pasta tonight and then watch a film.",
                "timestamp": "2026-04-21T10:00:00Z",
            }
        ]
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy, session_kind,
                one_line_summary, entity_mentions, run_entity_pass, run_threads_pass,
                identity_relevant, emotional_weight, processed_at, model_used,
                raw_triage_output, context_relevant
            )
            VALUES ($1,$2,NOW(),true,'personal','weak thread','{}'::text[],false,true,false,
                    'low',NOW(),'fixture',$3::jsonb,true)
            """,
            session_id,
            user_id,
            {"thread_signals": ["I might make pasta tonight and then watch a film."]},
        )

        async def _actions_stub(**_kwargs):
            return [
                {
                    "action": "CREATE",
                    "title": "Possible pasta plan",
                    "detail": "I might make pasta tonight and then watch a film.",
                    "category": "commitment",
                    "priority": "low",
                    "unresolvedness": "unclear",
                    "follow_up_value": "low",
                    "evidence_strength": "weak",
                    "why_this_matters_later": "",
                }
            ]

        monkeypatch.setattr(derived_pipeline, "extract_thread_actions", _actions_stub, raising=True)

        await run_pass3_threads(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            settings=settings,
        )

        threads = await db.fetch("SELECT title FROM open_threads WHERE user_id=$1", user_id)
        assert threads == []


@pytest.mark.asyncio
async def test_pass3_keeps_evidence_backed_high_followup_thread(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        tenant_id = "default"
        user_id = _unique("derived-user")
        session_id = _unique("strong-thread-session")
        text = "I need to keep checking in with Mara because the relationship still feels unresolved."
        messages = [{"role": "user", "text": text, "timestamp": "2026-04-21T10:00:00Z"}]
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy, session_kind,
                one_line_summary, entity_mentions, run_entity_pass, run_threads_pass,
                identity_relevant, emotional_weight, processed_at, model_used,
                raw_triage_output, context_relevant
            )
            VALUES ($1,$2,NOW(),true,'personal','strong thread','{}'::text[],false,true,false,
                    'high',NOW(),'fixture',$3::jsonb,true)
            """,
            session_id,
            user_id,
            {"thread_signals": [text]},
        )

        async def _actions_stub(**_kwargs):
            return [
                {
                    "action": "CREATE",
                    "title": "Relationship with Mara remains unresolved",
                    "detail": text,
                    "category": "relationship",
                    "priority": "high",
                    "unresolvedness": "open",
                    "follow_up_value": "high",
                    "evidence_strength": "strong",
                    "why_this_matters_later": "The user explicitly says the relationship still feels unresolved.",
                }
            ]

        monkeypatch.setattr(derived_pipeline, "extract_thread_actions", _actions_stub, raising=True)

        await run_pass3_threads(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            settings=settings,
        )

        thread = await db.fetchone(
            "SELECT title, category, priority, evidence_turn_refs FROM open_threads WHERE user_id=$1",
            user_id,
        )
        assert thread["title"] == "relationship with mara remains unresolved"
        assert thread["category"] == "relationship"
        assert thread["priority"] == "high"
        assert isinstance(thread["evidence_turn_refs"], list) and thread["evidence_turn_refs"]


@pytest.mark.asyncio
async def test_pass3_dedupes_semantic_hydration_create_into_existing_thread(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        tenant_id = "default"
        user_id = _unique("thread-dedupe-user")
        old_session_id = _unique("old-hydration-session")
        session_id = _unique("new-hydration-session")
        text = "I need to keep up with hydration and electrolytes so kidney stones do not flare again."
        messages = [{"role": "user", "text": text, "timestamp": "2026-04-21T10:00:00Z"}]
        existing_thread_id = _unique("hydration-thread")
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy, session_kind,
                one_line_summary, entity_mentions, run_entity_pass, run_threads_pass,
                identity_relevant, emotional_weight, processed_at, model_used,
                raw_triage_output, context_relevant
            )
            VALUES ($1,$2,NOW(),true,'personal','hydration update','{}'::text[],false,true,false,
                    'medium',NOW(),'fixture',$3::jsonb,true)
            """,
            session_id,
            user_id,
            {"thread_signals": [text]},
        )
        await db.execute(
            """
            INSERT INTO open_threads (
                thread_id, user_id, title, detail, status, priority, category,
                source_session_ids, evidence_turn_refs, distinct_session_count,
                salience_score, importance_score, lifecycle_state,
                created_at, last_updated_at, last_mentioned_at
            )
            VALUES (
                $1,$2,'Morning routine and hydration goal',
                'User is rebuilding lemon water and hydration after kidney stones.',
                'open','medium','health',$3::text[],$4::jsonb,1,
                0.7,0.7,'active',NOW(),NOW(),NOW()
            )
            """,
            existing_thread_id,
            user_id,
            [old_session_id],
            [{"session_id": old_session_id, "turn_index": 0, "text": "Lemon water and hydration matter after kidney stones."}],
        )

        async def _actions_stub(**_kwargs):
            return [
                {
                    "action": "CREATE",
                    "title": "Hydration and electrolytes after kidney stones",
                    "detail": text,
                    "category": "health",
                    "priority": "medium",
                    "unresolvedness": "open",
                    "follow_up_value": "medium",
                    "evidence_strength": "strong",
                    "why_this_matters_later": "The user explicitly connects hydration and electrolytes to kidney stone prevention.",
                }
            ]

        monkeypatch.setattr(derived_pipeline, "extract_thread_actions", _actions_stub, raising=True)

        await run_pass3_threads(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            settings=settings,
        )

        threads = await db.fetch(
            """
            SELECT thread_id, detail, source_session_ids, distinct_session_count
            FROM open_threads
            WHERE user_id=$1 AND category='health'
            ORDER BY created_at
            """,
            user_id,
        )
        assert len(threads) == 1
        assert threads[0]["thread_id"] == existing_thread_id
        assert "hydration and electrolytes" in threads[0]["detail"]
        assert set(threads[0]["source_session_ids"]) == {old_session_id, session_id}
        assert threads[0]["distinct_session_count"] == 2


@pytest.mark.asyncio
async def test_pass3_reactivates_matching_resolved_thread_instead_of_creating_sibling(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        tenant_id = "default"
        user_id = _unique("thread-reactivate-user")
        old_session_id = _unique("old-reactivate-session")
        session_id = _unique("new-reactivate-session")
        thread_id = _unique("reactivate-thread")
        text = "I am back to working on the gym routine after letting it go quiet for months."
        messages = [{"role": "user", "text": text, "timestamp": "2026-04-21T10:00:00Z"}]
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy, session_kind,
                one_line_summary, entity_mentions, run_entity_pass, run_threads_pass,
                identity_relevant, emotional_weight, processed_at, model_used,
                raw_triage_output, context_relevant
            )
            VALUES ($1,$2,NOW(),true,'personal','reactivate thread','{}'::text[],false,true,false,
                    'medium',NOW(),'fixture',$3::jsonb,true)
            """,
            session_id,
            user_id,
            {"thread_signals": [text]},
        )
        await db.execute(
            """
            INSERT INTO open_threads (
                thread_id, user_id, title, detail, status, priority, category,
                source_session_ids, evidence_turn_refs, distinct_session_count,
                lifecycle_state, created_at, last_updated_at, last_mentioned_at, resolved_at
            )
            VALUES (
                $1,$2,'Gym routine restart','User had stopped going to the gym.','resolved','medium','goal',
                $3::text[],$4::jsonb,1,'resolved',
                NOW() - interval '60 days',NOW() - interval '45 days',NOW() - interval '45 days',NOW() - interval '45 days'
            )
            """,
            thread_id,
            user_id,
            [old_session_id],
            [{"session_id": old_session_id, "turn_index": 0, "text": "The gym routine had dropped off."}],
        )

        async def _actions_stub(**_kwargs):
            return [
                {
                    "action": "CREATE",
                    "title": "Gym routine restart",
                    "detail": text,
                    "category": "goal",
                    "priority": "medium",
                    "unresolvedness": "open",
                    "follow_up_value": "medium",
                    "evidence_strength": "strong",
                    "why_this_matters_later": "The user explicitly says the routine is back in motion.",
                }
            ]

        monkeypatch.setattr(derived_pipeline, "extract_thread_actions", _actions_stub, raising=True)

        await run_pass3_threads(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            settings=settings,
        )

        rows = await db.fetch(
            """
            SELECT thread_id, status, lifecycle_state, detail, source_session_ids, distinct_session_count
            FROM open_threads
            WHERE user_id=$1
            """,
            user_id,
        )
        assert len(rows) == 1
        assert rows[0]["thread_id"] == thread_id
        assert rows[0]["status"] == "open"
        assert rows[0]["lifecycle_state"] == "active"
        assert "back to working on the gym routine" in rows[0]["detail"]
        assert set(rows[0]["source_session_ids"]) == {old_session_id, session_id}
        assert rows[0]["distinct_session_count"] == 2


@pytest.mark.asyncio
async def test_pass3_relationship_resolution_supersedes_old_conflict_thread(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        tenant_id = "default"
        user_id = _unique("thread-resolution-user")
        session_id = _unique("thread-resolution-session")
        old_thread_id = _unique("old-relationship-thread")
        text = "Things with Riley feel stable again and we are back on good terms."
        messages = [{"role": "user", "text": text, "timestamp": "2026-04-21T10:00:00Z"}]
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy, session_kind,
                one_line_summary, entity_mentions, run_entity_pass, run_threads_pass,
                identity_relevant, emotional_weight, processed_at, model_used,
                raw_triage_output, context_relevant
            )
            VALUES ($1,$2,NOW(),true,'personal','relationship resolution',$3::text[],false,true,false,
                    'high',NOW(),'fixture',$4::jsonb,true)
            """,
            session_id,
            user_id,
            ["Riley"],
            {"thread_signals": [text]},
        )
        await db.execute(
            """
            INSERT INTO open_threads (
                thread_id, user_id, title, detail, status, priority, category,
                related_entities, source_session_ids, evidence_turn_refs,
                lifecycle_state, created_at, last_updated_at, last_mentioned_at
            )
            VALUES (
                $1,$2,'Relationship tension with Riley','The relationship with Riley still feels unresolved and strained.',
                'open','high','relationship',$3::text[],$4::text[],$5::jsonb,
                'active',NOW() - interval '20 days',NOW() - interval '5 days',NOW() - interval '5 days'
            )
            """,
            old_thread_id,
            user_id,
            ["Riley"],
            ["old-session"],
            [{"session_id": "old-session", "turn_index": 0}],
        )

        async def _actions_stub(**_kwargs):
            return [
                {
                    "action": "CREATE",
                    "title": "Relationship with Riley is stable again",
                    "detail": text,
                    "category": "relationship",
                    "priority": "high",
                    "related_entities": ["Riley"],
                    "unresolvedness": "resolved",
                    "follow_up_value": "high",
                    "evidence_strength": "strong",
                    "why_this_matters_later": "The user explicitly says things are stable again.",
                }
            ]

        monkeypatch.setattr(derived_pipeline, "extract_thread_actions", _actions_stub, raising=True)

        await run_pass3_threads(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            settings=settings,
        )

        rows = await db.fetch(
            """
            SELECT thread_id, status, lifecycle_state, superseded_by_thread_id, resolution_note
            FROM open_threads
            WHERE user_id=$1
            ORDER BY created_at
            """,
            user_id,
        )
        assert len(rows) == 1
        assert rows[0]["thread_id"] == old_thread_id
        assert rows[0]["status"] == "resolved"
        assert rows[0]["lifecycle_state"] == "resolved"
        assert rows[0]["superseded_by_thread_id"] is None
        assert "stable again" in (rows[0]["resolution_note"] or "").lower()


@pytest.mark.asyncio
async def test_pass3_rejects_emotional_summary_relationship_thread_create(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        tenant_id = "default"
        user_id = _unique("thread-same-person-user")
        session_id = _unique("thread-same-person-session")
        existing_thread_id = _unique("existing-relationship-thread")
        text = "I feel a lot of grief and guilt about Jasmine and the years we lost."
        messages = [{"role": "user", "text": text, "timestamp": "2026-04-21T10:00:00Z"}]
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy, session_kind,
                one_line_summary, entity_mentions, run_entity_pass, run_threads_pass,
                identity_relevant, emotional_weight, processed_at, model_used,
                raw_triage_output, context_relevant
            )
            VALUES ($1,$2,NOW(),true,'personal','same person relationship','{Jasmine}'::text[],false,true,false,
                    'high',NOW(),'fixture',$3::jsonb,true)
            """,
            session_id,
            user_id,
            {"thread_signals": ["User feels grief and guilt about Jasmine."]},
        )
        await db.execute(
            """
            INSERT INTO open_threads (
                thread_id, user_id, title, detail, status, priority, category,
                related_entities, source_session_ids, evidence_turn_refs,
                distinct_session_count, lifecycle_state, created_at, last_updated_at, last_mentioned_at
            )
            VALUES (
                $1,$2,'Reconnecting with Jasmine','User has been estranged from Jasmine for years and is trying to reconnect.',
                'open','high','relationship',$3::text[],$4::text[],$5::jsonb,
                1,'active',NOW() - interval '10 days',NOW() - interval '2 days',NOW() - interval '2 days'
            )
            """,
            existing_thread_id,
            user_id,
            ["Jasmine"],
            ["old-session"],
            [{"session_id": "old-session", "turn_index": 0, "text": "I want to reconnect with Jasmine."}],
        )

        async def _actions_stub(**_kwargs):
            return [
                {
                    "action": "CREATE",
                    "title": "User's grief and guilt regarding Jasmine",
                    "detail": text,
                    "category": "relationship",
                    "priority": "high",
                    "unresolvedness": "open",
                    "follow_up_value": "high",
                    "evidence_strength": "strong",
                    "why_this_matters_later": "Jasmine remains a major unresolved relationship.",
                }
            ]

        monkeypatch.setattr(derived_pipeline, "extract_thread_actions", _actions_stub, raising=True)

        await run_pass3_threads(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            settings=settings,
        )

        rows = await db.fetch(
            """
            SELECT thread_id, title, detail, related_entities, source_session_ids, distinct_session_count
            FROM open_threads
            WHERE user_id=$1
            ORDER BY created_at
            """,
            user_id,
        )
        assert len(rows) == 1
        assert rows[0]["thread_id"] == existing_thread_id
        assert rows[0]["related_entities"] == ["Jasmine"]
        assert rows[0]["source_session_ids"] == ["old-session"]
        assert rows[0]["distinct_session_count"] == 1
        assert "trying to reconnect" in rows[0]["detail"].lower()


@pytest.mark.asyncio
async def test_thread_audit_merges_emotional_relationship_summary_into_keeper():
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = False
        tenant_id = "default"
        user_id = _unique("thread-audit-user")
        keeper_id = _unique("keeper-thread")
        emotional_id = _unique("emotion-thread")

        await db.execute(
            """
            INSERT INTO open_threads (
                thread_id, user_id, title, detail, status, priority, category,
                related_entities, source_session_ids, evidence_turn_refs,
                distinct_session_count, lifecycle_state, created_at, last_updated_at, last_mentioned_at
            )
            VALUES
            (
                $1,$3,'Reconnecting with Jasmine','User has been estranged from Jasmine and is trying to reconnect.',
                'open','high','relationship',$4::text[],$5::text[],'[]'::jsonb,
                2,'active',NOW() - interval '8 days',NOW() - interval '2 days',NOW() - interval '2 days'
            ),
            (
                $2,$3,'user''s grief and guilt regarding jasmine','User described grief and guilt about the years lost with Jasmine.',
                'open','medium','relationship',$4::text[],$6::text[],'[]'::jsonb,
                1,'active',NOW() - interval '6 days',NOW() - interval '1 day',NOW() - interval '1 day'
            )
            """,
            keeper_id,
            emotional_id,
            user_id,
            ["Jasmine"],
            ["older-session"],
            ["newer-session"],
        )

        summary = await run_thread_audit(db=db, tenant_id=tenant_id, user_id=user_id, settings=settings)

        keeper = await db.fetchone(
            "SELECT status, lifecycle_state FROM open_threads WHERE thread_id=$1",
            keeper_id,
        )
        emotional = await db.fetchone(
            "SELECT status, lifecycle_state, superseded_by_thread_id, resolution_note FROM open_threads WHERE thread_id=$1",
            emotional_id,
        )

        assert summary["merged"] >= 1
        assert keeper["status"] == "open"
        assert emotional["status"] == "resolved"
        assert emotional["lifecycle_state"] == "superseded"
        assert emotional["superseded_by_thread_id"] == keeper_id
        assert "underlying relationship thread" in (emotional["resolution_note"] or "").lower()


@pytest.mark.asyncio
async def test_pass3_quarantines_explicit_conflicting_thread_entity_create(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        tenant_id = "default"
        user_id = _unique("thread-conflict-user")
        session_id = _unique("thread-conflict-session")
        text = "User explicitly said the no-expectation message was to Riley."
        messages = [{"role": "user", "text": text, "timestamp": "2026-04-21T10:00:00Z"}]
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy, session_kind,
                one_line_summary, entity_mentions, run_entity_pass, run_threads_pass,
                identity_relevant, emotional_weight, processed_at, model_used,
                raw_triage_output, context_relevant
            )
            VALUES ($1,$2,NOW(),true,'personal','conflicting thread','{}'::text[],false,true,false,
                    'high',NOW(),'fixture',$3::jsonb,true)
            """,
            session_id,
            user_id,
            {"thread_signals": [text]},
        )
        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases,
                status, relationship_to_user, profile_text, key_facts, open_questions,
                confidence, mention_count, first_seen_at, last_seen_at,
                source_session_ids, distinct_session_count, salience_score, importance_score
            )
            VALUES
              ($1,'Jordan','jordan','person',$2::text[],'active','daughter','Jordan profile.','[]'::jsonb,'[]'::jsonb,
               1.0,4,NOW(),NOW(),$4::text[],2,1.0,1.0),
              ($1,'Riley','riley','person',$3::text[],'active','girlfriend','Riley profile.','[]'::jsonb,'[]'::jsonb,
               1.0,4,NOW(),NOW(),$4::text[],2,1.0,1.0)
            """,
            user_id,
            ["Jordan"],
            ["Riley"],
            [session_id],
        )

        async def _actions_stub(**_kwargs):
            return [
                {
                    "action": "CREATE",
                    "title": "Gentle contact with Jordan",
                    "detail": text,
                    "category": "relationship",
                    "priority": "high",
                    "related_entities": ["Jordan"],
                    "unresolvedness": "open",
                    "follow_up_value": "high",
                    "evidence_strength": "strong",
                    "why_this_matters_later": "The model claimed a Jordan thread while the evidence names Riley.",
                }
            ]

        monkeypatch.setattr(derived_pipeline, "extract_thread_actions", _actions_stub, raising=True)

        await run_pass3_threads(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            settings=settings,
        )

        threads = await db.fetch("SELECT thread_id FROM open_threads WHERE user_id=$1", user_id)
        quarantine = await db.fetchone(
            """
            SELECT reason_code, payload
            FROM derived_quarantine
            WHERE user_id=$1 AND pass_name='pass3_threads'
            ORDER BY quarantine_id DESC
            LIMIT 1
            """,
            user_id,
        )
        assert threads == []
        assert quarantine
        assert quarantine["reason_code"] == "parse_failure"
        assert quarantine["payload"]["reason"] == "thread_title_detail_entity_mismatch"
        assert quarantine["payload"]["title_entities"] == ["jordan"]
        assert quarantine["payload"]["detail_entities"] == ["riley"]


@pytest.mark.asyncio
async def test_lightweight_temporal_reinforcement_counts_are_written():
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = False
        tenant_id = "default"
        user_id = _unique("temporal-user")
        session_id = _unique("temporal-session")
        entity_text = "Riley is important to me."
        thread_text = "I need to keep working on the walking goal."
        messages = [
            {"role": "user", "text": entity_text, "timestamp": "2026-04-21T10:00:00Z"},
            {"role": "user", "text": thread_text, "timestamp": "2026-04-21T10:01:00Z"},
        ]
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy, session_kind,
                one_line_summary, entity_mentions, run_entity_pass, run_threads_pass,
                identity_relevant, emotional_weight, processed_at, model_used,
                raw_triage_output, context_relevant
            )
            VALUES ($1,$2,NOW(),true,'personal','temporal',$3::text[],true,true,false,
                    'medium',NOW(),'fixture',$4::jsonb,true)
            """,
            session_id,
            user_id,
            ["Riley"],
            {"thread_signals": [thread_text]},
        )

        await run_pass1_5_entities(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            settings=settings,
        )
        await run_pass3_threads(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            messages=messages,
            settings=settings,
        )

        entity = await db.fetchone(
            """
            SELECT first_seen_at, last_seen_at, distinct_session_count
            FROM entity_profiles
            WHERE user_id=$1 AND canonical_name_normalized='riley'
            """,
            user_id,
        )
        thread = await db.fetchone(
            """
            SELECT first_seen_at, last_mentioned_at, distinct_session_count
            FROM open_threads
            WHERE user_id=$1
            """,
            user_id,
        )
        assertion = await db.fetchone(
            """
            SELECT first_seen_at, last_seen_at, distinct_session_count
            FROM derived_assertions
            WHERE user_id=$1 AND surface='thread_update'
            LIMIT 1
            """,
            user_id,
        )

        assert entity["first_seen_at"] is not None
        assert entity["last_seen_at"] is not None
        assert entity["distinct_session_count"] == 1
        assert thread["first_seen_at"] is not None
        assert thread["last_mentioned_at"] is not None
        assert thread["distinct_session_count"] == 1
        assert assertion["first_seen_at"] is not None
        assert assertion["last_seen_at"] is not None
        assert assertion["distinct_session_count"] == 1


@pytest.mark.asyncio
async def test_thread_audit_safe_merge_applies(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        user_id = _unique("audit-user")
        keep_id = _unique("keep-thread")
        absorb_id = _unique("absorb-thread")
        await db.execute(
            """
            INSERT INTO open_threads (
                thread_id, user_id, title, detail, status, priority, category,
                lifecycle_state, source_session_ids, evidence_turn_refs,
                created_at, last_updated_at, last_mentioned_at
            )
            VALUES
              ($1,$3,'Riley communication','User is navigating communication with Riley.','open','high','relationship',
               'active',$4::text[],$5::jsonb,NOW(),NOW(),NOW()),
              ($2,$3,'Messaging Riley','User is navigating communication with Riley.','open','medium','relationship',
               'active',$4::text[],$5::jsonb,NOW(),NOW(),NOW())
            """,
            keep_id,
            absorb_id,
            user_id,
            ["session-a"],
            [{"session_id": "session-a", "turn_index": 0}],
        )

        async def _audit_stub(open_threads, model):
            return [
                {
                    "action": "MERGE",
                    "keep_thread_id": keep_id,
                    "absorb_thread_ids": [absorb_id],
                    "merged_title": "Riley communication",
                    "merged_detail": "User is navigating communication with Riley.",
                    "merged_category": "relationship",
                    "reason": "clear duplicate",
                    "confidence": 0.95,
                }
            ]

        monkeypatch.setattr(derived_pipeline, "audit_thread_registry", _audit_stub, raising=True)
        summary = await run_thread_audit(db=db, tenant_id="default", user_id=user_id, settings=settings)

        absorbed = await db.fetchone("SELECT status, lifecycle_state, superseded_by_thread_id FROM open_threads WHERE thread_id=$1", absorb_id)
        assert summary["merged"] == 1
        assert absorbed["status"] == "resolved"
        assert absorbed["lifecycle_state"] == "superseded"
        assert absorbed["superseded_by_thread_id"] == keep_id


@pytest.mark.asyncio
async def test_thread_audit_ambiguous_merge_flags_without_mutation(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        user_id = _unique("audit-user")
        thread_id = _unique("thread")
        await db.execute(
            """
            INSERT INTO open_threads (
                thread_id, user_id, title, detail, status, priority, category,
                lifecycle_state, source_session_ids, evidence_turn_refs,
                created_at, last_updated_at, last_mentioned_at
            )
            VALUES ($1,$2,'Ambiguous thread','Could be duplicate.','open','medium','relationship',
                    'active',$3::text[],$4::jsonb,NOW(),NOW(),NOW())
            """,
            thread_id,
            user_id,
            ["session-a"],
            [{"session_id": "session-a", "turn_index": 0}],
        )

        async def _audit_stub(open_threads, model):
            return [{"action": "RESOLVE", "thread_id": thread_id, "reason": "maybe resolved", "confidence": 0.5}]

        monkeypatch.setattr(derived_pipeline, "audit_thread_registry", _audit_stub, raising=True)
        summary = await run_thread_audit(db=db, tenant_id="default", user_id=user_id, settings=settings)
        row = await db.fetchone("SELECT status, lifecycle_state FROM open_threads WHERE thread_id=$1", thread_id)

        assert summary["flagged"] == 1
        assert row["status"] == "open"
        assert row["lifecycle_state"] == "active"


@pytest.mark.asyncio
async def test_thread_audit_snoozes_static_zombie_thread_without_followup(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = False
        user_id = _unique("zombie-audit-user")
        thread_id = _unique("zombie-thread")
        await db.execute(
            """
            INSERT INTO open_threads (
                thread_id, user_id, title, detail, status, priority, category,
                thread_type, lifecycle_state, source_session_ids, evidence_turn_refs,
                salience_score, importance_score,
                created_at, last_updated_at, last_mentioned_at
            )
            VALUES (
                $1,$2,'Old logistics thread','A thread that has gone nowhere for months.',
                'open','medium','other','situational','active',$3::text[],$4::jsonb,
                0.4,0.45,
                NOW() - interval '90 days',NOW() - interval '70 days',NOW() - interval '70 days'
            )
            """,
            thread_id,
            user_id,
            ["session-a"],
            [{"session_id": "session-a", "turn_index": 0}],
        )

        summary = await run_thread_audit(db=db, tenant_id="default", user_id=user_id, settings=settings)
        row = await db.fetchone("SELECT status, lifecycle_state FROM open_threads WHERE thread_id=$1", thread_id)

        assert summary["snoozed"] == 1
        assert row["status"] == "snoozed"
        assert row["lifecycle_state"] == "snoozed"


@pytest.mark.asyncio
async def test_retrospective_worker_runs_after_three_new_memory_worthy_sessions_and_writes_checkpoint():
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = False
        user_id = _unique("retro-user")

        for idx in range(3):
            await db.execute(
                """
                INSERT INTO session_classifications (
                    session_id, user_id, session_date, is_memory_worthy, session_kind,
                    one_line_summary, run_entity_pass, run_threads_pass,
                    identity_relevant, emotional_weight, processed_at, model_used, context_relevant
                )
                VALUES (
                    $1,$2,NOW() - ($3::text || ' days')::interval,true,'personal',
                    'retrospective candidate',false,false,false,
                    'low',NOW() - ($3::text || ' days')::interval,'fixture',false
                )
                """,
                _unique(f"retro-session-{idx}"),
                user_id,
                str(3 - idx),
            )

        first = await run_retrospective_worker_v1(
            db=db,
            tenant_id="default",
            settings=settings,
        )
        checkpoint = await db.fetchone(
            """
            SELECT pipeline_name, last_success_run_id, last_output_hash
            FROM pipeline_checkpoints
            WHERE user_id=$1 AND pipeline_name=$2
            """,
            user_id,
            PASS_RETROSPECTIVE_V1,
        )
        assert first["users_considered"] >= 1
        assert first["users_processed"] >= 1
        assert checkpoint
        assert checkpoint["pipeline_name"] == PASS_RETROSPECTIVE_V1
        assert checkpoint["last_success_run_id"] is not None
        assert checkpoint["last_output_hash"]


@pytest.mark.asyncio
async def test_retrospective_worker_closes_and_prunes_stale_low_confidence_items():
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = False
        user_id = _unique("retro-low-conf-user")

        await db.execute(
            """
            INSERT INTO low_confidence_items (
                tenant_id, user_id, surface, statement_text, question_text, confidence,
                source_session_ids, source_turn_refs, first_seen_at, last_seen_at, status
            )
            VALUES
            (
                'default',$1,'memory_delta','Relationship status is unclear.','Is the relationship still current?',0.45,
                ARRAY['session-close']::text[],'[]'::jsonb,
                NOW() - interval '30 days',NOW() - interval '30 days','open'
            ),
            (
                'default',$1,'entity_profile','A weak maybe-fact.','Did this ever matter?',0.2,
                ARRAY['session-prune']::text[],'[]'::jsonb,
                NOW() - interval '60 days',NOW() - interval '50 days','open'
            )
            """,
            user_id,
        )

        summary = await run_retrospective_worker_v1(
            db=db,
            tenant_id="default",
            settings=settings,
        )
        rows = await db.fetch(
            """
            SELECT statement_text, status, resolved_at
            FROM low_confidence_items
            WHERE user_id=$1
            ORDER BY item_id
            """,
            user_id,
        )

        assert summary["low_confidence_closed"] >= 1
        assert summary["low_confidence_pruned"] >= 1
        assert [row["status"] for row in rows] == ["expired", "dismissed"]
        assert all(row["resolved_at"] is not None for row in rows)


@pytest.mark.asyncio
async def test_retrospective_worker_reinterprets_low_confidence_item_from_explicit_anchor():
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = False
        user_id = _unique("retro-reinterpret-user")

        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases,
                status, relationship_to_user, confidence, mention_count,
                first_seen_at, last_seen_at, source_session_ids,
                distinct_session_count, reinforcement_count
            )
            VALUES (
                $1,'Jordan','jordan','person',$2::text[],'active','daughter',0.92,3,
                NOW() - interval '20 days',NOW() - interval '1 day',$3::text[],
                3,0
            )
            """,
            user_id,
            ["Jordan"],
            ["session-a", "session-b", "session-c"],
        )
        await db.execute(
            """
            INSERT INTO low_confidence_items (
                tenant_id, user_id, surface, statement_text, question_text, confidence,
                source_session_ids, source_turn_refs, first_seen_at, last_seen_at, status, metadata
            )
            VALUES (
                'default',$1,'entity_profile','Jordan might be a friend or other relation.',
                'Family role is unclear: Jordan might be a friend or other relation.',0.3,
                ARRAY['session-a']::text[],'[]'::jsonb,
                NOW() - interval '10 days',NOW() - interval '1 day','open',
                $2::jsonb
            )
            """,
            user_id,
            {"reason_code": "unclear_family_role"},
        )

        summary = await run_retrospective_worker_v1(
            db=db,
            tenant_id="default",
            user_id=user_id,
            settings=settings,
        )
        row = await db.fetchone(
            """
            SELECT status, metadata, resolved_at
            FROM low_confidence_items
            WHERE user_id=$1
            ORDER BY item_id DESC
            LIMIT 1
            """,
            user_id,
        )
        anchor = await db.fetchone(
            """
            SELECT reinforcement_count, last_reinforced_at
            FROM entity_profiles
            WHERE user_id=$1 AND canonical_name_normalized='jordan'
            """,
            user_id,
        )

        assert summary["processing_order"] == [
            "contradictions",
            "durable_anchors",
            "threads",
            "low_confidence",
            "tentative_entities",
        ]
        assert summary["low_confidence_reinterpreted"] == 1
        assert summary["anchors_reinforced"] == 1
        assert row["status"] == "answered"
        assert row["resolved_at"] is not None
        assert row["metadata"]["retrospective_action"] == "REINTERPRET"
        assert row["metadata"]["resolved_by_entity"] == "Jordan"
        assert row["metadata"]["resolved_role"] == "daughter"
        assert anchor["reinforcement_count"] >= 3
        assert anchor["last_reinforced_at"] is not None


@pytest.mark.asyncio
async def test_retrospective_worker_blocks_false_certainty_without_explicit_anchor_match():
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = False
        user_id = _unique("retro-false-certainty-user")

        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases,
                status, relationship_to_user, confidence, mention_count,
                first_seen_at, last_seen_at, source_session_ids,
                distinct_session_count
            )
            VALUES (
                $1,'Jordan','jordan','person',$2::text[],'active','daughter',0.85,3,
                NOW() - interval '20 days',NOW() - interval '1 day',$3::text[],
                3
            )
            """,
            user_id,
            ["Jordan"],
            ["session-a", "session-b", "session-c"],
        )
        await db.execute(
            """
            INSERT INTO low_confidence_items (
                tenant_id, user_id, surface, statement_text, question_text, confidence,
                source_session_ids, source_turn_refs, first_seen_at, last_seen_at, status, metadata
            )
            VALUES (
                'default',$1,'entity_profile','Jordan might be a romantic relationship.',
                'Relationship interpretation is uncertain: Jordan might be a romantic relationship.',0.3,
                ARRAY['session-a']::text[],'[]'::jsonb,
                NOW() - interval '10 days',NOW() - interval '1 day','open',
                $2::jsonb
            )
            """,
            user_id,
            {"reason_code": "uncertain_relationship_interpretation"},
        )

        summary = await run_retrospective_worker_v1(
            db=db,
            tenant_id="default",
            user_id=user_id,
            settings=settings,
        )
        row = await db.fetchone(
            """
            SELECT status, metadata
            FROM low_confidence_items
            WHERE user_id=$1
            ORDER BY item_id DESC
            LIMIT 1
            """,
            user_id,
        )

        assert summary["low_confidence_reinterpreted"] == 0
        assert summary["anti_false_certainty_blocked"] >= 1
        assert row["status"] == "open"
        assert "retrospective_action" not in (row["metadata"] or {})


@pytest.mark.asyncio
async def test_retrospective_worker_promotes_and_prunes_tentative_entities_conservatively():
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = False
        user_id = _unique("retro-tentative-user")

        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases,
                status, relationship_to_user, confidence, mention_count,
                first_seen_at, last_seen_at, source_session_ids,
                distinct_session_count
            )
            VALUES
            (
                $1,'Jordan','jordan','person',$2::text[],'tentative','daughter',0.7,2,
                NOW() - interval '20 days',NOW() - interval '1 day',$3::text[],
                2
            ),
            (
                $1,'Casey','casey','person',$4::text[],'tentative','other',0.3,1,
                NOW() - interval '80 days',NOW() - interval '70 days',$5::text[],
                1
            )
            """,
            user_id,
            ["Jordan"],
            ["session-a", "session-b"],
            ["Casey"],
            ["session-old"],
        )

        summary = await run_retrospective_worker_v1(
            db=db,
            tenant_id="default",
            user_id=user_id,
            settings=settings,
        )
        rows = await db.fetch(
            """
            SELECT canonical_name_normalized, status
            FROM entity_profiles
            WHERE user_id=$1
            ORDER BY canonical_name_normalized
            """,
            user_id,
        )
        by_name = {row["canonical_name_normalized"]: row["status"] for row in rows}

        assert summary["tentative_entities_promoted"] == 1
        assert summary["tentative_entities_pruned"] == 1
        assert by_name["jordan"] == "active"
        assert by_name["casey"] == "archived"


@pytest.mark.asyncio
async def test_conservative_memory_audits_include_retrospective_summary():
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = False
        user_id = _unique("retro-audit-loop-user")

        await db.execute(
            """
            INSERT INTO low_confidence_items (
                tenant_id, user_id, surface, statement_text, question_text, confidence,
                source_session_ids, source_turn_refs, first_seen_at, last_seen_at, status
            )
            VALUES (
                'default',$1,'memory_delta','Old ambiguous note.','Does this still matter?',0.45,
                ARRAY['session-retro']::text[],'[]'::jsonb,
                NOW() - interval '30 days',NOW() - interval '30 days','open'
            )
            """,
            user_id,
        )

        summary = await run_conservative_memory_audits(
            db=db,
            tenant_id="default",
            settings=settings,
        )

        assert set(summary.keys()) == {"threads", "entities", "actionable_candidates", "session_changes", "entity_candidates", "retrospective"}
        assert summary["retrospective"]["users_considered"] >= 1
        assert summary["retrospective"]["low_confidence_closed"] >= 1


@pytest.mark.asyncio
async def test_actionable_candidate_audit_dismisses_placeholder_candidate(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        user_id = _unique("actionable-audit-user")
        await db.execute(
            """
            INSERT INTO actionable_candidates (
                tenant_id, user_id, session_id, candidate_key, record_type, title, summary, source,
                provenance, confidence_score, confidence_label, status, metadata
            )
            VALUES (
                'default',$1,'s1','audit-key-1','task_candidate','We need to do that.','We need to do that.','chat',
                '{}'::jsonb,0.6,'medium','detected','{}'::jsonb
            )
            """,
            user_id,
        )

        async def _audit_stub(*, candidates, model):
            assert candidates[0]["title"] == "We need to do that."
            return [{"candidate_id": candidates[0]["candidate_id"], "action": "DISMISS", "reason": "placeholder without referent", "confidence": 0.95}]

        monkeypatch.setattr(derived_pipeline, "audit_actionable_candidates", _audit_stub, raising=True)

        summary = await run_actionable_candidate_audit(db=db, tenant_id="default", user_id=user_id, settings=settings)
        row = await db.fetchone("SELECT status FROM actionable_candidates WHERE tenant_id='default' AND user_id=$1", user_id)
        assert summary["dismissed"] >= 1
        assert row["status"] == "dismissed"


@pytest.mark.asyncio
async def test_session_change_and_entity_candidate_audits_can_apply_llm_actions(monkeypatch):
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.derived_pipeline_llm_enabled = True
        user_id = _unique("candidate-audit-user")
        await db.execute(
            """
            INSERT INTO session_changes (
                tenant_id, user_id, session_id, change_key, kind, title, summary, source,
                provenance, confidence_score, confidence_label, status, metadata
            )
            VALUES (
                'default',$1,'s1','change-key-1','focus_change','Project discussion','We discussed some project ideas.','chat',
                '{}'::jsonb,0.6,'medium','detected','{}'::jsonb
            )
            """,
            user_id,
        )
        await db.execute(
            """
            INSERT INTO entity_candidates (
                tenant_id, user_id, session_id, candidate_key, name, candidate_type, summary, source,
                provenance, confidence_score, confidence_label, status, metadata
            )
            VALUES (
                'default',$1,'s1','entity-key-1','Someone','other','generic mention','chat',
                '{}'::jsonb,0.4,'low','detected','{}'::jsonb
            )
            """,
            user_id,
        )

        async def _session_audit_stub(*, candidates, model):
            return [{"change_id": candidates[0]["change_id"], "action": "DISMISS", "reason": "discussion summary", "confidence": 0.95}]

        async def _entity_audit_stub(*, candidates, model):
            return [{"candidate_id": candidates[0]["candidate_id"], "action": "DISMISS", "reason": "generic non-entity", "confidence": 0.95}]

        monkeypatch.setattr(derived_pipeline, "audit_session_changes", _session_audit_stub, raising=True)
        monkeypatch.setattr(derived_pipeline, "audit_entity_candidates", _entity_audit_stub, raising=True)

        session_summary = await run_session_change_audit(db=db, tenant_id="default", user_id=user_id, settings=settings)
        entity_summary = await run_entity_candidate_audit(db=db, tenant_id="default", user_id=user_id, settings=settings)

        session_row = await db.fetchone("SELECT status FROM session_changes WHERE tenant_id='default' AND user_id=$1", user_id)
        entity_row = await db.fetchone("SELECT status FROM entity_candidates WHERE tenant_id='default' AND user_id=$1", user_id)
        assert session_summary["dismissed"] >= 1
        assert entity_summary["dismissed"] >= 1
        assert session_row["status"] == "dismissed"
        assert entity_row["status"] == "dismissed"


@pytest.mark.asyncio
async def test_always_on_packet_prefers_explicit_profile_truth_and_persists():
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.always_on_memory_packet_llm_enabled = False
        user_id = _unique("always-on-user")
        tenant_id = "default"

        await db.execute(
            """
            INSERT INTO user_identity (tenant_id, user_id, data, updated_at)
            VALUES (
                $1,$2,$3::jsonb,NOW()
            )
            ON CONFLICT (tenant_id, user_id)
            DO UPDATE SET data=EXCLUDED.data, updated_at=NOW()
            """,
            tenant_id,
            user_id,
            {
                "name": "Kaiser",
                "location": "Cambridge",
                "faith": "LDS",
                "projects": ["Sophie", "Bluum"],
            },
        )
        await db.execute(
            """
            INSERT INTO identity_profile (
                user_id, who_they_are, faith_and_beliefs, how_they_relate,
                current_chapter, core_values, persistent_goals, what_they_want, assertions, last_synthesized_at,
                source_session_count, synthesis_model, created_at, updated_at
            )
            VALUES (
                $1,'Founder and builder working on memory systems.',
                'Faith informs how he thinks about care and continuity.',
                'He prefers directness over cushioning.',
                'Building a memory system.', $2::jsonb, $3::jsonb, 'He wants grounded continuity.',
                '[]'::jsonb,NOW(),2,'fixture',NOW(),NOW()
            )
            ON CONFLICT (user_id) DO UPDATE SET
                who_they_are=EXCLUDED.who_they_are,
                faith_and_beliefs=EXCLUDED.faith_and_beliefs,
                how_they_relate=EXCLUDED.how_they_relate,
                current_chapter=EXCLUDED.current_chapter,
                core_values=EXCLUDED.core_values,
                persistent_goals=EXCLUDED.persistent_goals,
                what_they_want=EXCLUDED.what_they_want,
                updated_at=NOW()
            """,
            user_id,
            ["truth", "continuity", "faith"],
            ["Build Sophie well"],
        )
        await db.execute(
            """
            INSERT INTO living_context (
                user_id, current_focus, primary_tension, relationship_pulse, emotional_texture,
                sophie_directives, active_contradictions, assertions, last_synthesized_at,
                source_session_count, synthesis_model, created_at, updated_at
            )
            VALUES (
                $1,'Current focus is building Sophie and Bluum.',
                'Primary tension is shipping memory without drift.',
                'Jasmine reconnection matters.', 'The user is trying to stay steady.',
                '[]'::jsonb,'[]'::jsonb,'[]'::jsonb,NOW(),2,'fixture',NOW(),NOW()
            )
            ON CONFLICT (user_id) DO UPDATE SET
                current_focus=EXCLUDED.current_focus,
                primary_tension=EXCLUDED.primary_tension,
                relationship_pulse=EXCLUDED.relationship_pulse,
                emotional_texture=EXCLUDED.emotional_texture,
                updated_at=NOW()
            """,
            user_id,
        )
        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases,
                status, relationship_to_user, profile_text, last_known_status,
                confidence, mention_count, first_seen_at, last_seen_at,
                source_session_ids, salience_score, importance_score
            ) VALUES (
                $1,'Jasmine','jasmine','person',$2::text[],'active','daughter',
                'Jasmine is the user''s daughter.','Reconnection is meaningful.',
                0.95,3,NOW(),NOW(),$3::text[],0.95,0.95
            )
            ON CONFLICT (user_id, canonical_name_normalized) DO UPDATE SET
                profile_text=EXCLUDED.profile_text,
                last_known_status=EXCLUDED.last_known_status,
                salience_score=EXCLUDED.salience_score,
                last_updated_at=NOW()
            """,
            user_id,
            ["Jasmine"],
            ["session-a", "session-b"],
        )

        packet = await build_always_on_memory_packet(
            tenant_id=tenant_id,
            user_id=user_id,
            force_rebuild=True,
        )
        stored = await db.fetchone(
            """
            SELECT packet_version, profile_truth_used, sections, packet_text
            FROM always_on_memory_packets
            WHERE tenant_id=$1 AND user_id=$2
            """,
            tenant_id,
            user_id,
        )

        assert packet["profile_truth_used"] is True
        assert "Name: Kaiser." in packet["sections"]["enduring_identity"]
        assert any("Based in Cambridge." == item for item in packet["sections"]["enduring_identity"])
        assert any("Faith / worldview: LDS." == item for item in packet["sections"]["enduring_identity"])
        assert any("Founder and builder" in item for item in packet["sections"]["enduring_identity"])
        assert len(packet["sections"]["enduring_identity"]) >= 4
        assert any("Sophie" in item for item in packet["sections"]["work_and_building"])
        assert any("Jasmine" in item for item in packet["sections"]["important_people"])
        assert all("The user is trying to stay steady." != item for item in packet["sections"]["handle_carefully"])
        assert packet["sections"].get("open_questions", []) == []
        assert stored["packet_version"] == "always_on_memory_packet.v1"
        assert stored["profile_truth_used"] is True
        assert isinstance(stored["sections"], dict)
        assert stored["packet_text"]


@pytest.mark.asyncio
async def test_always_on_packet_debug_endpoint_returns_cached_artifact():
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.always_on_memory_packet_llm_enabled = False
        user_id = _unique("always-on-endpoint-user")
        tenant_id = "default"

        await db.execute(
            """
            INSERT INTO user_identity (tenant_id, user_id, data, updated_at)
            VALUES ($1,$2,$3::jsonb,NOW())
            ON CONFLICT (tenant_id, user_id)
            DO UPDATE SET data=EXCLUDED.data, updated_at=NOW()
            """,
            tenant_id,
            user_id,
            {"name": "Casey", "location": "London"},
        )

        await build_always_on_memory_packet(
            tenant_id=tenant_id,
            user_id=user_id,
            force_rebuild=True,
        )

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(
                "/internal/debug/always-on-packet",
                params={"tenantId": tenant_id, "userId": user_id, "forceRebuild": "false"},
            )

        assert response.status_code == 200
        payload = response.json()
        assert payload["version"] == "always_on_memory_packet.v1"
        assert payload["profile_truth_used"] is True
        assert "sections" in payload
        assert payload["packet_text"]


@pytest.mark.asyncio
async def test_always_on_packet_filters_contextual_relationship_entities():
    async with app.router.lifespan_context(app):
        settings = get_settings()
        settings.always_on_memory_packet_llm_enabled = False
        user_id = _unique("always-on-tier-user")
        tenant_id = "default"

        await db.execute(
            """
            INSERT INTO user_identity (tenant_id, user_id, data, updated_at)
            VALUES ($1,$2,$3::jsonb,NOW())
            ON CONFLICT (tenant_id, user_id)
            DO UPDATE SET data=EXCLUDED.data, updated_at=NOW()
            """,
            tenant_id,
            user_id,
            {
                "declared_profile_truth": {
                    "preferred_name": "Mukesh",
                    "roles": ["founder"],
                    "projects": ["Sophie — voice-first AI companion"],
                }
            },
        )

        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases,
                status, relationship_to_user, profile_text, key_facts, open_questions,
                last_known_status, confidence, mention_count, distinct_session_count,
                first_seen_at, last_seen_at, source_session_ids, salience_score, importance_score
            ) VALUES
            (
                $1,'Jasmine','jasmine','person',$2::text[],'active','daughter',
                'Jasmine is the user''s daughter.','[{\"fact\":\"The user and Jasmine have been estranged for approximately six years\"}]'::jsonb,'[]'::jsonb,
                'In early reconciliation',0.95,4,4,NOW(),NOW(),$3::text[],0.95,0.95
            ),
            (
                $1,'Ashley','ashley','person',$4::text[],'active','girlfriend',
                'Ashley is the user''s long-term, long-distance girlfriend.','[{\"fact\":\"The relationship is long-distance\"}]'::jsonb,'[]'::jsonb,
                'Back together after recent visit',0.95,4,4,NOW(),NOW(),$5::text[],0.94,0.94
            ),
            (
                $1,'Yoshi','yoshi','person',$6::text[],'active','other',
                'Yoshi is Ashley''s little girl.','[{\"fact\":\"Yoshi is Ashley''s daughter\"}]'::jsonb,'[]'::jsonb,
                'In contact with the user',0.95,6,6,NOW(),NOW(),$7::text[],0.97,0.97
            ),
            (
                $1,'Jasmine''s Mother','jasmine_s_mother','person',$8::text[],'active','other',
                'Jasmine''s mother is acting as a communication bridge.','[{\"fact\":\"She is the mother of the user''s daughter\"}]'::jsonb,'[]'::jsonb,
                'Acting as a bridge of communication',0.95,6,6,NOW(),NOW(),$9::text[],0.97,0.97
            )
            ON CONFLICT (user_id, canonical_name_normalized) DO UPDATE SET
                profile_text=EXCLUDED.profile_text,
                key_facts=EXCLUDED.key_facts,
                last_known_status=EXCLUDED.last_known_status,
                salience_score=EXCLUDED.salience_score,
                importance_score=EXCLUDED.importance_score,
                distinct_session_count=EXCLUDED.distinct_session_count,
                last_updated_at=NOW()
            """,
            user_id,
            ["Jasmine"],
            ["session-a", "session-b", "session-c", "session-d"],
            ["Ashley"],
            ["session-e", "session-f", "session-g", "session-h"],
            ["Yoshi"],
            ["session-i", "session-j", "session-k", "session-l", "session-m", "session-n"],
            ["Jasmine's Mother"],
            ["session-o", "session-p", "session-q", "session-r", "session-s", "session-t"],
        )

        packet = await build_always_on_memory_packet(
            tenant_id=tenant_id,
            user_id=user_id,
            force_rebuild=True,
        )

        important = packet["sections"]["important_people"]
        assert any("Jasmine" in item for item in important)
        assert any("Ashley" in item for item in important)
        assert all("Yoshi" not in item for item in important)
        assert all("Jasmine's Mother" not in item for item in important)


@pytest.mark.asyncio
async def test_entity_audit_clears_assistant_contaminated_profile():
    async with app.router.lifespan_context(app):
        user_id = _unique("entity-audit-user")
        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases,
                status, relationship_to_user, profile_text, key_facts, open_questions,
                confidence, mention_count, first_seen_at, last_seen_at,
                source_session_ids, salience_score, importance_score
            ) VALUES (
                $1,'Sophie','sophie','person',$2::text[],'active','friend',
                'Sophie is incorrectly treated as a real person.',$3::jsonb,'[]'::jsonb,
                0.9,3,NOW(),NOW(),$4::text[],0.9,0.9
            )
            ON CONFLICT (user_id, canonical_name_normalized) DO NOTHING
            """,
            user_id,
            ["Sophie"],
            [{"fact": "bad contamination"}],
            ["session-a"],
        )

        summary = await run_entity_audit(db=db, tenant_id="default", user_id=user_id)
        row = await db.fetchone(
            """
            SELECT type, relationship_to_user, profile_text, key_facts, open_questions
            FROM entity_profiles
            WHERE user_id=$1 AND canonical_name_normalized='sophie'
            """,
            user_id,
        )

        assert summary["cleared"] == 1
        assert row["type"] == "assistant"
        assert row["relationship_to_user"] == "assistant"
        assert row["profile_text"] is None
        assert row["key_facts"] == []
        assert row["open_questions"] == []


@pytest.mark.asyncio
async def test_entity_audit_corrects_project_typed_as_person():
    async with app.router.lifespan_context(app):
        user_id = _unique("entity-audit-project-user")
        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases,
                status, relationship_to_user, profile_text, key_facts, open_questions,
                confidence, mention_count, first_seen_at, last_seen_at,
                source_session_ids, salience_score, importance_score
            ) VALUES (
                $1,'Sophie Repository','sophie repository','person',$2::text[],'active','active_project',
                'Sophie Repository is an active project.','[]'::jsonb,'[]'::jsonb,
                0.9,3,NOW(),NOW(),$3::text[],0.9,0.9
            )
            ON CONFLICT (user_id, canonical_name_normalized) DO NOTHING
            """,
            user_id,
            ["Sophie Repository"],
            ["session-a"],
        )

        summary = await run_entity_audit(db=db, tenant_id="default", user_id=user_id)
        row = await db.fetchone(
            "SELECT type, relationship_to_user FROM entity_profiles WHERE user_id=$1 AND canonical_name_normalized='sophie repository'",
            user_id,
        )

        assert summary["corrected"] == 1
        assert row["type"] == "project"
        assert row["relationship_to_user"] == "active_project"


@pytest.mark.asyncio
async def test_entity_audit_sanitizes_strongly_supported_interpretive_profile_text():
    async with app.router.lifespan_context(app):
        user_id = _unique("entity-audit-sanitize-user")
        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases,
                status, relationship_to_user, profile_text, key_facts, open_questions,
                last_known_status, confidence, mention_count, distinct_session_count,
                first_seen_at, last_seen_at, source_session_ids, salience_score, importance_score
            ) VALUES (
                $1,'Riley','riley','person',$2::text[],'active','partner',
                'Riley is someone who seeks deep complexity and is trying to prove their love. Riley lives in Berlin.',
                $3::jsonb,'[]'::jsonb,'The vibe is intense.',0.95,4,3,
                NOW(),NOW(),$4::text[],0.9,0.9
            )
            """,
            user_id,
            ["Riley"],
            [{"fact": "Riley lives in Berlin."}],
            ["session-a", "session-b", "session-c"],
        )

        summary = await run_entity_audit(db=db, tenant_id="default", user_id=user_id)
        row = await db.fetchone(
            """
            SELECT profile_text, last_known_status, key_facts
            FROM entity_profiles
            WHERE user_id=$1 AND canonical_name_normalized='riley'
            """,
            user_id,
        )

        assert summary["sanitized_profiles"] == 1
        assert row["profile_text"] == "Riley lives in Berlin."
        assert row["last_known_status"] is None
        assert row["key_facts"] == [{"fact": "Riley lives in Berlin."}]


@pytest.mark.asyncio
async def test_entity_audit_flags_low_support_interpretive_profile_without_mutation():
    async with app.router.lifespan_context(app):
        user_id = _unique("entity-audit-flag-user")
        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases,
                status, relationship_to_user, profile_text, key_facts, open_questions,
                confidence, mention_count, distinct_session_count,
                first_seen_at, last_seen_at, source_session_ids, salience_score, importance_score
            ) VALUES (
                $1,'Casey','casey','person',$2::text[],'active','friend',
                'Casey is someone who seeks deep complexity.', '[]'::jsonb,'[]'::jsonb,
                0.45,1,1,NOW(),NOW(),$3::text[],0.3,0.3
            )
            """,
            user_id,
            ["Casey"],
            ["session-a"],
        )

        summary = await run_entity_audit(db=db, tenant_id="default", user_id=user_id)
        row = await db.fetchone(
            """
            SELECT profile_text
            FROM entity_profiles
            WHERE user_id=$1 AND canonical_name_normalized='casey'
            """,
            user_id,
        )

        assert summary["flagged"] == 1
        assert row["profile_text"] == "Casey is someone who seeks deep complexity."


@pytest.mark.asyncio
async def test_entity_audit_scans_beyond_first_batch():
    async with app.router.lifespan_context(app):
        user_id = _unique("entity-audit-batch-user")
        for idx, name in enumerate(["Riley", "Jordan", "Ashley"], start=1):
            await db.execute(
                """
                INSERT INTO entity_profiles (
                    user_id, canonical_name, canonical_name_normalized, type, aliases,
                    status, relationship_to_user, profile_text, key_facts, open_questions,
                    confidence, mention_count, distinct_session_count,
                    first_seen_at, last_seen_at, source_session_ids, salience_score, importance_score
                ) VALUES (
                    $1,$2,$3,'person',$4::text[],'active','partner',
                    'This relationship has been quite a rollercoaster lately; they reconciled after a difficult breakup.',
                    '[]'::jsonb,'[]'::jsonb,
                    0.95,4,3,NOW(),NOW(),$5::text[],0.9,0.9
                )
                """,
                user_id,
                name,
                name.lower(),
                [name],
                [f"session-{idx}"],
            )

        summary = await run_entity_audit(db=db, tenant_id="default", user_id=user_id, batch_size=2)
        rows = await db.fetch(
            """
            SELECT canonical_name, profile_text
            FROM entity_profiles
            WHERE user_id=$1
            ORDER BY canonical_name
            """,
            user_id,
        )

        assert summary["sanitized_profiles"] == 3
        assert all("rollercoaster" not in (row["profile_text"] or "").lower() for row in rows)


@pytest.mark.asyncio
async def test_proactive_shadow_candidates_populate_all_three_queues():
    async with app.router.lifespan_context(app):
        tenant_id = "default"
        user_id = _unique("proactive-shadow-user")
        session_id = _unique("proactive-shadow-session")
        run_row = await db.fetchone(
            """
            INSERT INTO pipeline_runs (
                tenant_id, user_id, session_id, pass_name, model_version, prompt_version,
                policy_version, input_hash, status, started_at, completed_at
            ) VALUES (
                $1,$2,$3,$4,'fixture','fixture','derived.v1','hash-fixture','succeeded',NOW(),NOW()
            )
            RETURNING run_id
            """,
            tenant_id,
            user_id,
            session_id,
            PASS5_LIVING_CONTEXT,
        )
        run_id = int((run_row or {}).get("run_id") or 0)
        assert run_id > 0

        await db.execute(
            """
            INSERT INTO open_threads (
                thread_id, user_id, title, detail, status, priority, category,
                source_session_ids, evidence_turn_refs, distinct_session_count,
                salience_score, importance_score, follow_up_after,
                lifecycle_state, created_at, last_updated_at, last_mentioned_at
            ) VALUES (
                $1,$2,'Client call follow-up',
                'User had a difficult client call and wanted to revisit the outcome.',
                'open','high','project',$3::text[],$4::jsonb,2,0.84,0.88,
                NOW() + interval '2 hours','active',NOW(),NOW(),NOW()
            )
            """,
            _unique("proactive-thread"),
            user_id,
            ["s1", "s2"],
            [{"session_id": "s2", "turn_index": 0}],
        )

        await db.execute(
            """
            INSERT INTO low_confidence_items (
                tenant_id, user_id, surface, statement_text, question_text, confidence,
                status, source_session_ids, source_turn_refs, run_id, first_seen_at, last_seen_at
            ) VALUES (
                $1,$2,'living_context.statement',
                'User mentioned changing travel plans but details were unclear.',
                'You mentioned changing travel plans — is that still active?',
                0.31,'open',$3::text[],$4::jsonb,$5,NOW(),NOW()
            )
            """,
            tenant_id,
            user_id,
            ["s2"],
            [{"session_id": "s2", "turn_index": 1}],
            run_id,
        )

        await db.execute(
            """
            INSERT INTO derived_assertions (
                tenant_id, user_id, pass_name, surface, statement_text, lifecycle_state,
                salience, importance, confidence_extraction, confidence_validity,
                source_session_ids, source_turn_refs, run_id, metadata, created_at, updated_at
            ) VALUES (
                $1,$2,$3,'memory_delta',
                'User is shifting focus toward calmer daily structure.',
                'active',0.78,0.81,0.72,0.76,$4::text[],$5::jsonb,$6,'{}'::jsonb,NOW(),NOW()
            )
            """,
            tenant_id,
            user_id,
            PASS5_LIVING_CONTEXT,
            ["s2"],
            [{"session_id": "s2", "turn_index": 2}],
            run_id,
        )

        summary = await run_proactive_shadow_candidates(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            max_users=10,
        )

        assert summary["users_processed"] == 1
        assert summary["follow_up_candidates_active"] >= 1
        assert summary["clarification_candidates_active"] >= 1
        assert summary["recent_change_candidates_active"] >= 1

        follow_up = await db.fetch(
            "SELECT candidate_key, status FROM follow_up_candidates WHERE tenant_id=$1 AND user_id=$2",
            tenant_id,
            user_id,
        )
        clarification = await db.fetch(
            "SELECT candidate_key, status FROM clarification_candidates WHERE tenant_id=$1 AND user_id=$2",
            tenant_id,
            user_id,
        )
        recent_change = await db.fetch(
            "SELECT candidate_key, status FROM recent_change_candidates WHERE tenant_id=$1 AND user_id=$2",
            tenant_id,
            user_id,
        )

        assert any(str(row.get("candidate_key", "")).startswith("thread:") for row in follow_up)
        assert any((row.get("status") or "") == "shadow_open" for row in follow_up)
        assert any(str(row.get("candidate_key", "")).startswith("low_conf:") for row in clarification)
        assert any((row.get("status") or "") == "shadow_open" for row in clarification)
        assert any(
            str(row.get("candidate_key", "")).startswith("thread_change:")
            or str(row.get("candidate_key", "")).startswith("entity_change:")
            for row in recent_change
        )
        assert any((row.get("status") or "") == "shadow_open" for row in recent_change)


@pytest.mark.asyncio
async def test_proactive_shadow_recent_change_lookback_includes_older_assertions():
    async with app.router.lifespan_context(app):
        tenant_id = "default"
        user_id = _unique("proactive-shadow-lookback-user")
        session_id = _unique("proactive-shadow-lookback-session")
        await db.execute(
            """
            INSERT INTO open_threads (
                thread_id, user_id, title, detail, status, priority, category, thread_type,
                source_session_ids, evidence_turn_refs, distinct_session_count,
                salience_score, importance_score, created_at, last_updated_at, last_mentioned_at
            ) VALUES (
                $1,$2,'Re-evaluating long-term work direction',
                'User is re-evaluating long-term work direction.',
                'open','medium','project','situational',$3::text[],$4::jsonb,1,
                0.77,0.82, NOW() - interval '20 days', NOW() - interval '20 days', NOW() - interval '20 days'
            )
            """,
            _unique("thread-lookback"),
            user_id,
            ["s20"],
            [{"session_id": "s20", "turn_index": 0}],
        )

        seven_day = await run_proactive_shadow_candidates(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            max_users=1,
            lookback_days=7,
        )
        assert seven_day["recent_change_candidates_active"] == 0

        thirty_day = await run_proactive_shadow_candidates(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            max_users=1,
            lookback_days=30,
        )
        assert thirty_day["recent_change_candidates_active"] >= 1


@pytest.mark.asyncio
async def test_proactive_shadow_follow_up_excludes_tier3_contextual_entity_threads():
    async with app.router.lifespan_context(app):
        tenant_id = "default"
        user_id = _unique("proactive-shadow-tier3-user")
        contextual_name = "Bridge Person"
        strong_name = "Ashley"

        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases, status,
                relationship_to_user, profile_text, key_facts, open_questions, confidence,
                mention_count, first_seen_at, last_seen_at, source_session_ids,
                distinct_session_count, salience_score, importance_score
            ) VALUES
            (
                $1,$2,$3,'person',$4::text[],'active','other',
                'Acting as a bridge between user and partner family.',
                '[]'::jsonb,'[]'::jsonb,0.9,3,NOW()-interval '20 days',NOW(),
                $5::text[],2,0.7,0.7
            ),
            (
                $1,$6,$7,'person',$8::text[],'active','girlfriend',
                'Long-term girlfriend.',
                '[]'::jsonb,'[]'::jsonb,0.9,6,NOW()-interval '20 days',NOW(),
                $5::text[],4,0.9,0.9
            )
            """,
            user_id,
            contextual_name,
            "bridge person",
            [contextual_name],
            ["s1"],
            strong_name,
            "ashley",
            [strong_name],
        )

        await db.execute(
            """
            INSERT INTO open_threads (
                thread_id, user_id, title, detail, status, priority, category, thread_type,
                related_entities, source_session_ids, evidence_turn_refs,
                distinct_session_count, salience_score, importance_score,
                created_at, last_updated_at, last_mentioned_at
            ) VALUES
            (
                $1,$2,'Bridge person check-in','Contextual bridge thread should be blocked.',
                'open','high','relationship','situational',$3::text[],$4::text[],$5::jsonb,
                1,0.9,0.9,NOW(),NOW(),NOW()
            ),
            (
                $6,$2,'Ashley relationship check-in','Core relationship thread should remain.',
                'open','high','relationship','situational',$7::text[],$4::text[],$5::jsonb,
                1,0.9,0.9,NOW(),NOW(),NOW()
            )
            """,
            _unique("thread-contextual"),
            user_id,
            [contextual_name],
            ["s1"],
            [{"session_id": "s1", "turn_index": 0}],
            _unique("thread-strong"),
            [strong_name],
        )

        summary = await run_proactive_shadow_candidates(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            max_users=1,
            lookback_days=30,
        )
        assert summary["follow_up_candidates_active"] >= 1

        rows = await db.fetch(
            """
            SELECT title
            FROM follow_up_candidates
            WHERE tenant_id=$1 AND user_id=$2 AND status='shadow_open'
            ORDER BY candidate_id
            """,
            tenant_id,
            user_id,
        )
        titles = [str(r.get("title") or "").lower() for r in rows]
        assert any("ashley relationship check-in" in title for title in titles)
        assert not any("bridge person check-in" in title for title in titles)


@pytest.mark.asyncio
async def test_proactive_shadow_recent_change_ignores_raw_assertions():
    async with app.router.lifespan_context(app):
        tenant_id = "default"
        user_id = _unique("proactive-shadow-no-assertions-user")
        session_id = _unique("proactive-shadow-no-assertions-session")
        run_row = await db.fetchone(
            """
            INSERT INTO pipeline_runs (
                tenant_id, user_id, session_id, pass_name, model_version, prompt_version,
                policy_version, input_hash, status, started_at, completed_at
            ) VALUES (
                $1,$2,$3,$4,'fixture','fixture','derived.v1','hash-fixture','succeeded',NOW(),NOW()
            )
            RETURNING run_id
            """,
            tenant_id,
            user_id,
            session_id,
            PASS5_LIVING_CONTEXT,
        )
        run_id = int((run_row or {}).get("run_id") or 0)
        assert run_id > 0

        await db.execute(
            """
            INSERT INTO derived_assertions (
                tenant_id, user_id, pass_name, surface, statement_text, lifecycle_state,
                salience, importance, confidence_extraction, confidence_validity,
                source_session_ids, source_turn_refs, run_id, metadata, created_at, updated_at
            ) VALUES (
                $1,$2,$3,'memory_delta',
                'User was born in England.',
                'active',0.9,0.9,0.8,0.9,$4::text[],$5::jsonb,$6,'{}'::jsonb,NOW(),NOW()
            )
            """,
            tenant_id,
            user_id,
            PASS5_LIVING_CONTEXT,
            ["s1"],
            [{"session_id": "s1", "turn_index": 0}],
            run_id,
        )

        summary = await run_proactive_shadow_candidates(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            max_users=1,
            lookback_days=30,
        )
        assert summary["recent_change_candidates_active"] == 0


@pytest.mark.asyncio
async def test_proactive_shadow_follow_up_includes_high_value_health_loops():
    async with app.router.lifespan_context(app):
        tenant_id = "default"
        user_id = _unique("proactive-shadow-loop-health-user")
        loop_id = str(uuid.uuid4())

        await db.execute(
            """
            INSERT INTO loops (
                id, tenant_id, user_id, persona_id, type, status, text,
                confidence, salience, time_horizon, metadata, updated_at, created_at
            ) VALUES (
                $1,$2,$3,'sophie','habit','needs_review','Drink 3 litres of water daily',
                0.86,5,'ongoing',$4::jsonb,NOW(),NOW()
            )
            """,
            loop_id,
            tenant_id,
            user_id,
            {"domain": "health", "reason": "Kidney stone prevention habit."},
        )

        summary = await run_proactive_shadow_candidates(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            max_users=1,
            lookback_days=30,
        )
        assert summary["follow_up_candidates_active"] >= 1

        rows = await db.fetch(
            """
            SELECT candidate_key, source_surface, title
            FROM follow_up_candidates
            WHERE tenant_id=$1 AND user_id=$2 AND status='shadow_open'
            ORDER BY candidate_id
            """,
            tenant_id,
            user_id,
        )
        assert any(str(r.get("candidate_key", "")).startswith("loop:") for r in rows)
        assert any((r.get("source_surface") or "") == "health_loop" for r in rows)
        assert any("water" in str(r.get("title", "")).lower() for r in rows)


@pytest.mark.asyncio
async def test_packet_compilers_inject_durable_anchor_and_explain_inclusion():
    async with app.router.lifespan_context(app):
        tenant_id = "default"
        user_id = _unique("packet-user")
        session_id = _unique("packet-session")
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy, session_kind,
                one_line_summary, entity_mentions, run_entity_pass, run_threads_pass,
                identity_relevant, emotional_weight, processed_at, model_used,
                raw_triage_output, context_relevant
            )
            VALUES ($1,$2,NOW(),true,'personal','packet',$3::text[],true,true,true,
                    'medium',NOW(),'fixture',$4::jsonb,true)
            """,
            session_id,
            user_id,
            ["Jules"],
            {
                "memory_deltas": ["User returned to the rebuilding routine."],
                "identity_signals": ["User values directness when trust is strained."],
                "thread_signals": ["User is still working on rebuilding routine."],
            },
        )
        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases,
                status, relationship_to_user, profile_text, key_facts, open_questions,
                confidence, mention_count, first_seen_at, last_seen_at,
                source_session_ids, distinct_session_count, salience_score, importance_score
            ) VALUES (
                $1,'Jules','jules','person',$2::text[],'active','daughter',
                'Jules is a durable family anchor.','[]'::jsonb,'[]'::jsonb,
                0.9,4,NOW() - interval '90 days',NOW() - interval '70 days',
                $3::text[],2,0.4,0.95
            )
            ON CONFLICT (user_id, canonical_name_normalized) DO NOTHING
            """,
            user_id,
            ["Jules"],
            ["old-session", session_id],
        )
        await db.execute(
            """
            INSERT INTO open_threads (
                thread_id, user_id, title, detail, status, priority, category,
                thread_type, source_session_ids, evidence_turn_refs,
                distinct_session_count, salience_score, importance_score,
                created_at, last_updated_at, last_mentioned_at
            )
            VALUES (
                $1,$2,'Rebuilding routine','User is still working on rebuilding routine.',
                'open','high','goal','persistent_goal',$3::text[],$4::jsonb,
                2,0.8,0.85,NOW(),NOW(),NOW()
            )
            """,
            _unique("packet-thread"),
            user_id,
            ["old-session", session_id],
            [{"session_id": session_id, "turn_index": 0}],
        )

        rows = await db.fetch("SELECT session_id, session_date, raw_triage_output, tension_signal FROM session_classifications WHERE user_id=$1", user_id)
        pass4_packet = await build_pass4_identity_packet(db=db, tenant_id=tenant_id, user_id=user_id, rows=[dict(row) for row in rows])
        pass5_packet = await build_pass5_living_packet(db=db, user_id=user_id, rows=[dict(row) for row in rows])

        assert pass4_packet["durable_anchors"][0]["canonical_name"] == "Jules"
        assert pass4_packet["durable_anchors"][0]["why_included"] == "durable_relationship_anchor"
        assert pass4_packet["durable_anchors"][0]["evidence_strength"] in {"medium", "strong"}
        assert isinstance(pass4_packet["declared_profile_truth"], dict)
        assert isinstance(pass4_packet["declared_truth_facts"], list)
        assert isinstance(pass4_packet["durable_profile_facts"], list)
        assert pass5_packet["key_entities"][0]["canonical_name"] == "Jules"
        assert pass5_packet["key_entities"][0]["why_included"] == "active_or_durable_entity"
        assert pass5_packet["active_threads"][0]["why_included"] == "high_signal_active_thread"
        assert pass5_packet["active_threads"][0]["evidence_refs"]


@pytest.mark.asyncio
async def test_pass4_packet_includes_declared_truth_and_durable_profile_facts():
    async with app.router.lifespan_context(app):
        tenant_id = "default"
        user_id = _unique("pass4-anchors-user")
        await db.execute(
            """
            INSERT INTO user_identity (tenant_id, user_id, data, updated_at)
            VALUES ($1,$2,$3::jsonb,NOW())
            ON CONFLICT (tenant_id, user_id)
            DO UPDATE SET data=EXCLUDED.data, updated_at=NOW()
            """,
            tenant_id,
            user_id,
            {
                "name": "Kaiser",
                "location": "Cambridge",
                "faith": "LDS",
                "roles": ["founder", "product lead"],
                "projects": ["Sophie", "Bluum"],
                "writing": ["Substack essays on theology and psychology"],
            },
        )
        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases,
                status, relationship_to_user, profile_text, last_known_status,
                confidence, mention_count, first_seen_at, last_seen_at,
                source_session_ids, distinct_session_count, salience_score, importance_score
            ) VALUES
            (
                $1,'Jasmine','jasmine','person',$2::text[],'active','daughter',
                'Jasmine is the user''s daughter.','Active reconciliation.',
                0.95,3,NOW(),NOW(),$3::text[],2,0.95,0.95
            ),
            (
                $1,'Sophie','sophie','project',$4::text[],'active','owned_project',
                'Sophie is a core product.','Testing phase.',
                0.9,4,NOW(),NOW(),$5::text[],3,0.9,0.9
            )
            ON CONFLICT (user_id, canonical_name_normalized) DO UPDATE
            SET last_updated_at=NOW()
            """,
            user_id,
            ["Jasmine"],
            ["session-a", "session-b"],
            ["Sophie"],
            ["session-a", "session-b", "session-c"],
        )
        await db.execute(
            """
            INSERT INTO open_threads (
                thread_id, user_id, title, detail, status, priority, category,
                source_session_ids, evidence_turn_refs, distinct_session_count,
                created_at, last_updated_at, last_mentioned_at
            )
            VALUES (
                $1,$2,'Morning Routine and Hydration Goals',
                'Hydration matters after recent kidney stones.',
                'open','medium','goal',$3::text[],$4::jsonb,2,NOW(),NOW(),NOW()
            )
            """,
            _unique("health-anchor-thread"),
            user_id,
            ["session-a", "session-b"],
            [{"session_id": "session-b", "turn_index": 0}],
        )
        await db.execute(
            """
            INSERT INTO pipeline_runs (
                run_id, tenant_id, user_id, session_id, pass_name,
                model_version, prompt_version, policy_version,
                input_watermark, input_hash, status, attempt_count, started_at
            ) VALUES (
                $1,$2,$3,NULL,'pass1_triage',
                'fixture-model','fixture-prompt','fixture-policy',
                NULL,'fixture-hash','succeeded',1,NOW()
            )
            ON CONFLICT (run_id) DO NOTHING
            """,
            999001,
            tenant_id,
            user_id,
        )
        await db.execute(
            """
            INSERT INTO derived_assertions (
                tenant_id, user_id, pass_name, surface, run_id, statement_text, lifecycle_state,
                salience, importance, confidence_extraction, confidence_validity,
                source_session_ids, source_turn_refs, memory_layer, semantic_category,
                retention_floor, first_seen_at, last_seen_at, distinct_session_count,
                created_at, updated_at
            ) VALUES (
                $1,$2,'pass1_triage','identity_signal',$3,
                'User writes Substack essays on LDS theology and psychology.','active',
                0.7,0.8,0.8,0.8,$4::text[],$5::jsonb,'LML','identity_signal',0.5,
                NOW(),NOW(),2,NOW(),NOW()
            )
            """,
            tenant_id,
            user_id,
            999001,
            ["session-a", "session-b"],
            [{"session_id": "session-a", "turn_index": 0}],
        )

        packet = await build_pass4_identity_packet(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            rows=[],
        )

        assert packet["declared_profile_truth"]["preferred_name"] == "Kaiser"
        assert any(f["fact_type"] == "role" and f["source_type"] == "declared_truth" for f in packet["declared_truth_facts"])
        assert any(f["fact_type"] == "important_person" and f["source_type"] == "durable_derived" for f in packet["durable_profile_facts"])
        assert any(f["fact_type"] == "work_or_project" and "Sophie" in f["fact_value"] for f in packet["durable_profile_facts"])
        assert any(f["fact_type"] == "health_anchor" for f in packet["durable_profile_facts"])
        assert any(f["fact_type"] == "writing_or_public_work" and f["source_type"] == "repeated_explicit" for f in packet["durable_profile_facts"])


@pytest.mark.asyncio
async def test_packet_compilers_accept_legacy_source_session_evidence_without_turn_refs():
    async with app.router.lifespan_context(app):
        user_id = _unique("legacy-source-evidence")
        session_id = _unique("legacy-source-session")
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy, session_kind,
                one_line_summary, run_entity_pass, run_threads_pass,
                identity_relevant, emotional_weight, processed_at, model_used,
                raw_triage_output, context_relevant
            )
            VALUES ($1,$2,NOW(),true,'personal','legacy evidence',false,true,true,
                    'medium',NOW(),'fixture',$3::jsonb,true)
            """,
            session_id,
            user_id,
            {
                "memory_deltas": ["A durable thread remains active."],
                "thread_signals": ["A durable thread remains active."],
            },
        )
        await db.execute(
            """
            INSERT INTO open_threads (
                thread_id, user_id, title, detail, status, priority, category,
                thread_type, source_session_ids, evidence_turn_refs,
                distinct_session_count, salience_score, importance_score,
                created_at, last_updated_at, last_mentioned_at
            )
            VALUES (
                $1,$2,'Legacy durable thread','Historical thread with session-level evidence only.',
                'open','high','relationship','situational',$3::text[],'[]'::jsonb,
                3,0.9,0.9,NOW(),NOW(),NOW()
            )
            """,
            _unique("legacy-thread"),
            user_id,
            ["old-a", "old-b", session_id],
        )

        rows = await db.fetch(
            "SELECT session_id, session_date, raw_triage_output FROM session_classifications WHERE user_id=$1",
            user_id,
        )
        packet = await build_pass5_living_packet(db=db, user_id=user_id, rows=[dict(row) for row in rows])

        assert packet["active_threads"]
        assert packet["active_threads"][0]["evidence_refs"] == ["old-a", "old-b", session_id]


@pytest.mark.asyncio
async def test_pass5_packet_deduplicates_low_confidence_items():
    async with app.router.lifespan_context(app):
        user_id = _unique("low-confidence-dedupe-user")
        session_id = _unique("low-confidence-dedupe-session")
        question = "What is the specific relationship tension with Mara?"
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy, session_kind,
                one_line_summary, run_entity_pass, run_threads_pass,
                identity_relevant, emotional_weight, processed_at, model_used,
                raw_triage_output, context_relevant
            )
            VALUES ($1,$2,NOW(),true,'personal','low confidence',false,false,false,
                    'medium',NOW(),'fixture',$3::jsonb,true)
            """,
            session_id,
            user_id,
            {"memory_deltas": ["Relationship tension with Mara is unclear."]},
        )
        for _ in range(2):
            await db.execute(
                """
                INSERT INTO low_confidence_items (
                    user_id, surface, statement_text, question_text, confidence,
                    source_session_ids, source_turn_refs, first_seen_at, last_seen_at, status
                )
                VALUES (
                    $1,'entity_profile','Open question about Mara',$2,0.35,
                    $3::text[],$4::jsonb,NOW(),NOW(),'open'
                )
                """,
                user_id,
                question,
                [session_id],
                [{"session_id": session_id, "turn_index": 0}],
            )

        rows = await db.fetch(
            "SELECT session_id, session_date, raw_triage_output FROM session_classifications WHERE user_id=$1",
            user_id,
        )
        packet = await build_pass5_living_packet(db=db, user_id=user_id, rows=[dict(row) for row in rows])

        assert len(packet["low_confidence"]) == 1
        assert packet["low_confidence"][0]["question_text"] == question


@pytest.mark.asyncio
async def test_pass5_packet_drops_thread_with_conflicting_title_detail_entities():
    async with app.router.lifespan_context(app):
        user_id = _unique("thread-contamination-user")
        session_id = _unique("thread-contamination-session")
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy, session_kind,
                one_line_summary, run_entity_pass, run_threads_pass,
                identity_relevant, emotional_weight, processed_at, model_used,
                raw_triage_output, context_relevant
            )
            VALUES ($1,$2,NOW(),true,'personal','contaminated thread',false,true,false,
                    'medium',NOW(),'fixture',$3::jsonb,true)
            """,
            session_id,
            user_id,
            {"thread_signals": ["User is maintaining contact with Jordan."]},
        )
        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases,
                status, relationship_to_user, profile_text, key_facts, open_questions,
                confidence, mention_count, first_seen_at, last_seen_at,
                source_session_ids, distinct_session_count, salience_score, importance_score
            )
            VALUES
              ($1,'Jordan','jordan','person',$2::text[],'active','daughter','Jordan profile.','[]'::jsonb,'[]'::jsonb,
               1.0,4,NOW(),NOW(),$4::text[],2,1.0,1.0),
              ($1,'Riley','riley','person',$3::text[],'active','girlfriend','Riley profile.','[]'::jsonb,'[]'::jsonb,
               1.0,4,NOW(),NOW(),$4::text[],2,1.0,1.0)
            """,
            user_id,
            ["Jordan"],
            ["Riley"],
            [session_id],
        )
        await db.execute(
            """
            INSERT INTO open_threads (
                thread_id, user_id, title, detail, status, priority, category,
                thread_type, source_session_ids, evidence_turn_refs,
                distinct_session_count, salience_score, importance_score,
                created_at, last_updated_at, last_mentioned_at
            )
            VALUES (
                $1,$2,'Gentle contact with Jordan','User sent a no-expectation message to Riley.',
                'open','high','relationship','situational',$3::text[],$4::jsonb,
                2,0.9,0.9,NOW(),NOW(),NOW()
            )
            """,
            _unique("contaminated-thread"),
            user_id,
            [session_id],
            [{"session_id": session_id, "turn_index": 0, "text": "User explicitly said the no-expectation message was to Riley."}],
        )

        rows = await db.fetch(
            "SELECT session_id, session_date, raw_triage_output FROM session_classifications WHERE user_id=$1",
            user_id,
        )
        packet = await build_pass5_living_packet(db=db, user_id=user_id, rows=[dict(row) for row in rows])

        assert packet["active_threads"] == []
        assert packet["dropped"][0]["reason"] == "thread_title_detail_entity_mismatch"
        assert packet["dropped"][0]["title_entities"] == ["jordan"]
        assert packet["dropped"][0]["detail_entities"] == ["riley"]


@pytest.mark.asyncio
async def test_pass5_packet_strips_inferred_conflicting_thread_entity_when_evidence_is_pronoun_only():
    async with app.router.lifespan_context(app):
        user_id = _unique("thread-pronoun-user")
        session_id = _unique("thread-pronoun-session")
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy, session_kind,
                one_line_summary, run_entity_pass, run_threads_pass,
                identity_relevant, emotional_weight, processed_at, model_used,
                raw_triage_output, context_relevant
            )
            VALUES ($1,$2,NOW(),true,'personal','pronoun thread',false,true,false,
                    'medium',NOW(),'fixture',$3::jsonb,true)
            """,
            session_id,
            user_id,
            {"thread_signals": ["User is maintaining contact with Jordan."]},
        )
        await db.execute(
            """
            INSERT INTO entity_profiles (
                user_id, canonical_name, canonical_name_normalized, type, aliases,
                status, relationship_to_user, profile_text, key_facts, open_questions,
                confidence, mention_count, first_seen_at, last_seen_at,
                source_session_ids, distinct_session_count, salience_score, importance_score
            )
            VALUES
              ($1,'Jordan','jordan','person',$2::text[],'active','daughter','Jordan profile.','[]'::jsonb,'[]'::jsonb,
               1.0,4,NOW(),NOW(),$4::text[],2,1.0,1.0),
              ($1,'Riley','riley','person',$3::text[],'active','girlfriend','Riley profile.','[]'::jsonb,'[]'::jsonb,
               1.0,4,NOW(),NOW(),$4::text[],2,1.0,1.0)
            """,
            user_id,
            ["Jordan"],
            ["Riley"],
            [session_id],
        )
        await db.execute(
            """
            INSERT INTO open_threads (
                thread_id, user_id, title, detail, status, priority, category,
                thread_type, related_entities, source_session_ids, evidence_turn_refs,
                distinct_session_count, salience_score, importance_score,
                created_at, last_updated_at, last_mentioned_at
            )
            VALUES (
                $1,$2,'Gentle contact with Jordan','User sent a no-expectation message to Riley.',
                'open','high','relationship','situational',$5::text[],$3::text[],$4::jsonb,
                2,0.9,0.9,NOW(),NOW(),NOW()
            )
            """,
            _unique("pronoun-thread"),
            user_id,
            [session_id],
            [{"session_id": session_id, "turn_index": 0, "text": "I sent her a happy holiday message with no expectation of a reply."}],
            ["Jordan"],
        )

        rows = await db.fetch(
            "SELECT session_id, session_date, raw_triage_output FROM session_classifications WHERE user_id=$1",
            user_id,
        )
        packet = await build_pass5_living_packet(db=db, user_id=user_id, rows=[dict(row) for row in rows])

        assert packet["dropped"] == []
        assert packet["active_threads"]
        assert "Riley" not in packet["active_threads"][0]["detail"]
        assert "no-expectation message" in packet["active_threads"][0]["detail"]
