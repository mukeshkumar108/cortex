from datetime import datetime, timezone
import json
from pathlib import Path
import uuid

import asyncpg
import pytest

from src.config import get_settings
from src.derived_pipeline import (
    PASS1_5_ENTITIES,
    PASS1_TRIAGE,
    PASS3_THREADS,
    PASS4_IDENTITY,
    PASS5_LIVING_CONTEXT,
    run_pass1_5_entities,
    run_pass1_triage,
    run_pass3_threads,
    run_pass4_identity,
    run_pass5_living_context,
)
import src.derived_pipeline as derived_pipeline
from src.main import app, db, _execute_post_ingest_hook


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "derived_pipeline" / "basic_six_pass.json"


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def _messages() -> list[dict]:
    fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    return fixture["messages"]


@pytest.mark.asyncio
async def test_pass1_writes_traceable_assertions_and_is_idempotent():
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
        fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
        assert first.output_hash == fixture["expected"]["pass1"]["output_hash"]
        assert first.run_entity_pass is True
        assert first.run_threads_pass is True

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
        assert classification["context_relevant"] is fixture["expected"]["pass1"]["context_relevant"]
        assert classification["raw_triage_output"]["source"] == "heuristic_fallback"

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
        assert assertions
        assert {row["surface"] for row in assertions} >= set(fixture["expected"]["pass1"]["surfaces"])
        assert all(row["source_session_ids"] == [session_id] for row in assertions)
        assert all(isinstance(row["source_turn_refs"], list) and row["source_turn_refs"] for row in assertions)
        assert any(row["memory_layer"] == "LML" for row in assertions)

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
async def test_immediate_and_deferred_passes_update_serving_tables_with_evidence():
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
        assert pass1.should_run_identity is True
        assert pass1.should_run_living_context is True

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
        await run_pass4_identity(db=db, tenant_id=tenant_id, user_id=user_id, settings=settings)
        await run_pass5_living_context(db=db, tenant_id=tenant_id, user_id=user_id, settings=settings)

        entity = await db.fetchone(
            """
            SELECT canonical_name, source_session_ids
            FROM entity_profiles
            WHERE user_id=$1 AND canonical_name_normalized='ashley'
            """,
            user_id,
        )
        assert entity
        fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
        assert entity["canonical_name"] in fixture["expected"]["pass1_5_entities"]["entities"]
        assert entity["source_session_ids"] == [session_id]

        thread = await db.fetchone(
            """
            SELECT lifecycle_state, evidence_turn_refs, confidence_extraction
            FROM open_threads
            WHERE user_id=$1
            ORDER BY created_at DESC
            LIMIT 1
            """,
            user_id,
        )
        assert thread
        assert thread["lifecycle_state"] == fixture["expected"]["pass3_threads"]["lifecycle_state"]
        assert isinstance(thread["evidence_turn_refs"], list) and thread["evidence_turn_refs"]
        assert float(thread["confidence_extraction"]) > 0

        identity = await db.fetchone("SELECT assertions FROM identity_profile WHERE user_id=$1", user_id)
        living = await db.fetchone("SELECT assertions FROM living_context WHERE user_id=$1", user_id)
        assert identity and isinstance(identity["assertions"], list)
        assert living and isinstance(living["assertions"], list)

        checkpoints = await db.fetch(
            """
            SELECT pipeline_name, identity_signal_count_since_last, context_delta_count_since_last
            FROM pipeline_checkpoints
            WHERE user_id=$1 AND pipeline_name = ANY($2::text[])
            """,
            user_id,
            [PASS1_TRIAGE, PASS4_IDENTITY, PASS5_LIVING_CONTEXT],
        )
        by_name = {row["pipeline_name"]: row for row in checkpoints}
        assert by_name[PASS4_IDENTITY]["identity_signal_count_since_last"] == 0
        assert by_name[PASS5_LIVING_CONTEXT]["context_delta_count_since_last"] == 0


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
        assert {PASS1_5_ENTITIES, PASS3_THREADS, PASS4_IDENTITY, PASS5_LIVING_CONTEXT}.issubset(hooks)


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
                "sophie_directives": [{"directive": "do not flatten this into productivity advice", "confidence": 0.9}],
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
        assert identity["assertions"]
        assert living["current_focus"] == "Becoming consistent while holding relational pressure."
        assert living["primary_tension"] == "Wanting change while avoiding the deeper stakes."
        assert living["sophie_directives"][0]["directive"] == "do not flatten this into productivity advice"
        assert living["active_contradictions"][0]["topic"] == "consistency"
        assert living["assertions"]


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
