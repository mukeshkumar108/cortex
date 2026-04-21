#!/usr/bin/env python3
"""Inspect derived 6-pass pipeline output for a fixture or existing user.

Examples:
  python scripts/inspect_derived_pipeline_fixture.py --fixture-id grief_bereavement --output /tmp/grief.json
  python scripts/inspect_derived_pipeline_fixture.py --fixture-id grief_bereavement --live-model --output /tmp/grief.json --markdown-output /tmp/grief.md
  python scripts/inspect_derived_pipeline_fixture.py --batch-fixtures --live-model --output-dir /tmp/derived_live_review --markdown
  python scripts/inspect_derived_pipeline_fixture.py --user-id <user_id> --session-id latest --run-live-window --live-model --output /tmp/user_window.json
  python scripts/inspect_derived_pipeline_fixture.py --user-id <user_id> --session-id latest
"""

from __future__ import annotations

import argparse
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys
import uuid
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import src.derived_pipeline as derived_pipeline
from src.config import get_settings
from src.main import db, _build_handover_packet, _execute_post_ingest_hook, session_startbrief
from src.migrate import run_migrations
from src import loops
from src import session

FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "derived_pipeline" / "parity_cases.json"

GENERIC_PHRASES = [
    "important in the current memory context",
    "recent sessions contain a repeated high-signal unresolved thread",
    "to become steady without being misunderstood",
    "meaningful, unresolved, and guarded",
    "relational context matters where named",
    "held accurately",
    "fixture-specific",
]


@asynccontextmanager
async def _review_services():
    """Initialize only what this review harness needs; avoid app background loops."""
    if db.pool is not None:
        await db.close()
    await db.get_pool()
    await run_migrations(db)
    session.init_session_manager(db)
    session.set_post_ingest_hook_executor(_execute_post_ingest_hook)
    loops.init_loop_manager(db)
    try:
        yield
    finally:
        session.set_post_ingest_hook_executor(None)
        await db.close()


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def _text(messages: List[Dict[str, Any]]) -> str:
    return "\n".join(str(m.get("text") or "") for m in messages if (m.get("role") or "").lower() == "user")


def _case_from_text(text: str, cases: List[Dict[str, Any]]) -> Dict[str, Any]:
    lowered = text.lower()
    marker_to_fixture = [
        ("ashley", "relationship_contradiction"),
        ("jasmine", "silence_entity_evolution"),
        ("walking", "unresolved_goal_health"),
        ("hydration", "unresolved_goal_health"),
        ("dad", "grief_bereavement"),
        ("cancer", "grief_bereavement"),
        ("bluum", "user_correction_contradiction"),
        ("maya", "relational_rupture_status_change"),
        ("hmrc", "repeated_deflection_avoidance"),
        ("gym", "silence_reactivation"),
    ]
    by_id = {case["fixture_id"]: case for case in cases}
    for marker, fixture_id in marker_to_fixture:
        if marker in lowered and fixture_id in by_id:
            return by_id[fixture_id]
    raise ValueError("could not map fixture text to expected case")


def _payload_for_case(case: Dict[str, Any]) -> Dict[str, Any]:
    expected = case["expected"]
    return {
        "is_memory_worthy": True,
        "session_kind": "personal",
        "memory_deltas": [expected["pass1_summary"]],
        "entity_mentions": expected["entities"],
        "thread_signals": expected["thread_titles"],
        "identity_signals": [expected["identity_who"]],
        "emotional_weight": "high" if case["fixture_id"] in {"grief_bereavement", "relational_rupture_status_change"} else "medium",
        "emotional_note": case["description"],
        "tension_signal": expected["living_primary_tension"],
        "context_relevant": True,
        "run_entity_pass": True,
        "run_threads_pass": True,
        "identity_relevant": True,
    }


def _identity_for_case(case: Dict[str, Any]) -> Dict[str, Any]:
    expected = case["expected"]
    return {
        "who_they_are": expected["identity_who"],
        "core_values": [{"value": "integrity", "evidence": "fixture identity signal", "confidence": 0.86}],
        "recurring_patterns": [{"pattern": expected["living_primary_tension"], "evidence": "fixture tension signal"}],
        "family_history": "Only included where fixture evidence supports it.",
        "faith_and_beliefs": "Not enough evidence to synthesize a durable belief pattern.",
        "what_they_want": "To be understood without flattening the specific signal.",
        "recurring_fears": [{"fear": expected["living_primary_tension"], "evidence": "fixture tension signal", "confidence": 0.74}],
        "what_they_avoid": expected["living_primary_tension"],
        "how_they_relate": "Held through the fixture-specific relationship between action, avoidance, and meaning.",
        "persistent_goals": [{"goal": expected["handover_thread"], "evidence": "fixture thread signal"}],
        "current_chapter": expected["identity_current_chapter"],
    }


def _living_for_case(case: Dict[str, Any]) -> Dict[str, Any]:
    expected = case["expected"]
    contradictions = []
    if expected.get("contradiction_terms"):
        contradictions.append({"topic": expected["handover_thread"], "earlier_view": expected["contradiction_terms"][0], "recent_view": expected["contradiction_terms"][-1]})
    return {
        "current_focus": expected["living_current_focus"],
        "recent_narrative": expected["pass1_summary"],
        "relationship_pulse": expected["living_current_focus"],
        "emotional_texture": case["description"],
        "primary_tension": expected["living_primary_tension"],
        "what_theyre_avoiding": expected["living_primary_tension"],
        "unspoken_goal": "To have the specific signal held accurately.",
        "why_it_matters": "This fixture is explicitly checking meaning-first specificity.",
        "active_contradictions": contradictions,
        "sophie_directives": [{"directive": "stay specific to the fixture evidence", "confidence": 0.9}],
    }


async def _install_fixture_stubs(case: Dict[str, Any], all_cases: List[Dict[str, Any]]) -> None:
    async def _pass1_stub(messages, model):
        return _payload_for_case(_case_from_text(_text(messages), all_cases))

    async def _entity_stub(existing_entities, mentions, model):
        return [
            {
                "decision": "NEW",
                "mention": item["mention"],
                "canonical_name": item["mention"],
                "type": "other" if item["mention"] in {"Sophie", "HMRC", "Gym"} else "person",
                "status": "active",
                "relationship_to_user": "assistant" if item["mention"] == "Sophie" else "important_person",
                "aliases": [item["mention"]],
                "confidence": 0.88,
            }
            for item in mentions
        ]

    async def _entity_profile_stub(canonical_name, entity_type, relationship_to_user, messages, existing_profile_text, model):
        expected = case["expected"]
        return {
            "profile_text": f"{canonical_name}: {expected['living_current_focus']}",
            "key_facts": [{"fact": expected["pass1_summary"], "confidence": 0.88}],
            "open_questions": [],
            "last_known_status": expected["living_primary_tension"],
        }

    async def _thread_stub(messages, session_date, emotional_weight, emotional_note, thread_signals, existing_threads, model):
        return [
            {
                "action": "CREATE",
                "title": signal,
                "detail": signal,
                "category": "goal" if any(x in signal.lower() for x in ["walking", "gym"]) else "relationship",
                "priority": "high",
            }
            for signal in thread_signals
        ]

    async def _identity_stub(**_kwargs):
        return _identity_for_case(case)

    async def _living_stub(**_kwargs):
        return _living_for_case(case)

    derived_pipeline.run_rich_pass1_llm = _pass1_stub
    derived_pipeline.resolve_entity_mentions = _entity_stub
    derived_pipeline.build_entity_profile = _entity_profile_stub
    derived_pipeline.extract_thread_actions = _thread_stub
    derived_pipeline.synthesize_identity_profile = _identity_stub
    derived_pipeline.synthesize_living_context = _living_stub


async def _seed_fixture(case: Dict[str, Any]) -> str:
    user_id = _unique(f"inspect-{case['fixture_id']}")
    for item in case["sessions"]:
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
    return user_id


async def _run_live_fixture(user_id: str, case: Dict[str, Any]) -> str:
    for item in case["sessions"]:
        session_id = f"{user_id}-{item['session_id']}"
        payload = {"tenant_id": "default", "user_id": user_id, "session_id": session_id, "reference_time": item["messages"][0]["timestamp"]}
        await _execute_post_ingest_hook(session.POST_INGEST_HOOK_PASS1_TRIAGE, payload)
        await _execute_post_ingest_hook(session.POST_INGEST_HOOK_PASS1_5_ENTITIES, payload)
        await _execute_post_ingest_hook(session.POST_INGEST_HOOK_PASS3_THREADS, payload)
    final_session_id = f"{user_id}-{case['sessions'][-1]['session_id']}"
    payload = {"tenant_id": "default", "user_id": user_id, "session_id": final_session_id, "reference_time": case["sessions"][-1]["messages"][0]["timestamp"]}
    await _execute_post_ingest_hook(session.POST_INGEST_HOOK_PASS4_IDENTITY, payload)
    await _execute_post_ingest_hook(session.POST_INGEST_HOOK_PASS5_LIVING_CONTEXT, payload)
    return final_session_id


async def _latest_session_id(user_id: str) -> Optional[str]:
    row = await db.fetchone(
        """
        SELECT session_id FROM session_transcript
        WHERE user_id=$1
        ORDER BY created_at DESC NULLS LAST, updated_at DESC NULLS LAST
        LIMIT 1
        """,
        user_id,
    )
    return row["session_id"] if row else None


async def _inspect(user_id: str, session_id: Optional[str]) -> Dict[str, Any]:
    if not session_id or session_id == "latest":
        session_id = await _latest_session_id(user_id)
    transcript = await db.fetch(
        """
        SELECT session_id, created_at, messages
        FROM session_transcript
        WHERE user_id=$1 AND ($2::text IS NULL OR session_id=$2)
        ORDER BY created_at ASC NULLS LAST
        """,
        user_id,
        session_id,
    )
    classifications = await db.fetch("SELECT * FROM session_classifications WHERE user_id=$1 ORDER BY session_date ASC NULLS LAST", user_id)
    entities = await db.fetch("SELECT * FROM entity_profiles WHERE user_id=$1 ORDER BY canonical_name_normalized", user_id)
    threads = await db.fetch("SELECT * FROM open_threads WHERE user_id=$1 ORDER BY last_updated_at DESC NULLS LAST", user_id)
    identity = await db.fetchone("SELECT * FROM identity_profile WHERE user_id=$1", user_id)
    living = await db.fetchone("SELECT * FROM living_context WHERE user_id=$1", user_id)
    assertions = await db.fetch(
        """
        SELECT surface, statement_text, lifecycle_state, source_session_ids, source_turn_refs, confidence_extraction, confidence_validity
        FROM derived_assertions
        WHERE user_id=$1
        ORDER BY assertion_id
        """,
        user_id,
    )
    startbrief = None
    handover = None
    if session_id:
        startbrief = await session_startbrief(tenantId="default", userId=user_id, sessionId=session_id, now=datetime.now(timezone.utc).isoformat(), timezone="UTC")
        handover = await _build_handover_packet(user_id)
    return {
        "user_id": user_id,
        "session_id": session_id,
        "transcript": [dict(r) for r in transcript],
        "pass1_session_classifications": [dict(r) for r in classifications],
        "entity_profiles": [dict(r) for r in entities],
        "open_threads": [dict(r) for r in threads],
        "identity_profile": dict(identity) if identity else None,
        "living_context": dict(living) if living else None,
        "derived_assertions_evidence_refs": [dict(r) for r in assertions],
        "startbrief": startbrief.model_dump() if startbrief else None,
        "handover": handover,
    }


def _flatten_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return " ".join(_flatten_text(v) for v in value.values())
    if isinstance(value, list):
        return " ".join(_flatten_text(v) for v in value)
    return str(value)


def _token_set(text: str) -> set[str]:
    stop = {
        "the", "and", "that", "this", "with", "from", "into", "while", "without",
        "user", "someone", "current", "context", "because", "about", "being",
        "through", "rather", "period", "meaningful", "specific",
    }
    cleaned = re.sub(r"[^a-zA-Z0-9']+", " ", text.lower())
    return {tok for tok in cleaned.split() if len(tok) >= 4 and tok not in stop}


def _structural_diff(report: Dict[str, Any], case: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not case:
        return {"expected_fixture": None}
    expected = case.get("expected") or {}
    actual_text = _flatten_text(report).lower()
    actual_entities = {
        str(row.get("canonical_name") or "").lower()
        for row in report.get("entity_profiles") or []
        if row.get("canonical_name")
    }
    actual_threads = {
        str(row.get("title") or "").lower()
        for row in report.get("open_threads") or []
        if row.get("title")
    }
    return {
        "expected_fixture": case.get("fixture_id"),
        "missing_entities": [
            name for name in expected.get("entities", [])
            if name.lower() not in actual_entities
        ],
        "missing_threads": [
            title for title in expected.get("thread_titles", [])
            if title.lower() not in actual_threads
        ],
        "missing_specific_terms": [
            term for term in expected.get("specific_terms", [])
            if term.lower() not in actual_text
        ],
        "missing_contradiction_terms": [
            term for term in expected.get("contradiction_terms", [])
            if term.lower() not in actual_text
        ],
        "missing_reactivation_terms": [
            term for term in expected.get("reactivation_terms", [])
            if term.lower() not in actual_text
        ],
        "identity_current_chapter": {
            "expected": expected.get("identity_current_chapter"),
            "actual": ((report.get("identity_profile") or {}).get("current_chapter")),
        },
        "living_current_focus": {
            "expected": expected.get("living_current_focus"),
            "actual": ((report.get("living_context") or {}).get("current_focus")),
        },
        "living_primary_tension": {
            "expected": expected.get("living_primary_tension"),
            "actual": ((report.get("living_context") or {}).get("primary_tension")),
        },
    }


def _review_flags(report: Dict[str, Any], case: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    flags: List[Dict[str, Any]] = []
    text = _flatten_text({
        "entity_profiles": report.get("entity_profiles"),
        "open_threads": report.get("open_threads"),
        "identity_profile": report.get("identity_profile"),
        "living_context": report.get("living_context"),
        "startbrief": report.get("startbrief"),
        "handover": report.get("handover"),
    }).lower()
    for phrase in GENERIC_PHRASES:
        count = text.count(phrase)
        if count:
            flags.append({"code": "generic_stock_phrase", "phrase": phrase, "count": count})
    if case:
        missing_terms = [
            term for term in (case.get("expected") or {}).get("specific_terms", [])
            if term.lower() not in text
        ]
        if missing_terms:
            flags.append({"code": "missing_fixture_specific_terms", "terms": missing_terms})
    entities = report.get("entity_profiles") or []
    threads = report.get("open_threads") or []
    if not entities:
        flags.append({"code": "low_entity_specificity", "detail": "no entity profiles produced"})
    if not threads:
        flags.append({"code": "low_thread_specificity", "detail": "no open threads produced"})
    for row in entities:
        profile = str(row.get("profile_text") or "")
        name = str(row.get("canonical_name") or "")
        if name and name.lower() not in profile.lower():
            flags.append({"code": "low_entity_specificity", "entity": name, "detail": "profile text does not mention entity name"})
        if len(_token_set(profile)) < 6:
            flags.append({"code": "low_entity_specificity", "entity": name, "detail": "profile text is very short"})
    for row in threads:
        title = str(row.get("title") or "")
        if len(_token_set(title)) < 2:
            flags.append({"code": "low_thread_specificity", "thread": title, "detail": "thread title has too few meaningful tokens"})
    return flags


def _add_batch_similarity_flags(reports: List[Dict[str, Any]]) -> None:
    living_tokens = []
    for report in reports:
        living = report.get("living_context") or {}
        text = " ".join(
            str(living.get(k) or "")
            for k in ["current_focus", "primary_tension", "relationship_pulse", "emotional_texture"]
        )
        living_tokens.append((report.get("fixture_id") or report.get("user_id"), _token_set(text)))
    for i, (left_id, left_tokens) in enumerate(living_tokens):
        for right_id, right_tokens in living_tokens[i + 1:]:
            if not left_tokens or not right_tokens:
                continue
            overlap = len(left_tokens & right_tokens) / max(1, len(left_tokens | right_tokens))
            if overlap >= 0.42:
                flag = {
                    "code": "near_identical_living_context",
                    "other": right_id,
                    "overlap": round(overlap, 3),
                }
                reports[i].setdefault("review_flags", []).append(flag)
                reports[living_tokens.index((right_id, right_tokens))].setdefault("review_flags", []).append(
                    {"code": "near_identical_living_context", "other": left_id, "overlap": round(overlap, 3)}
                )


def _attach_review(report: Dict[str, Any], case: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    enriched = dict(report)
    if case:
        enriched["fixture_id"] = case.get("fixture_id")
        enriched["fixture_description"] = case.get("description")
        enriched["expected_signals"] = case.get("expected")
    enriched["structural_diff_vs_expected"] = _structural_diff(enriched, case)
    enriched["review_flags"] = _review_flags(enriched, case)
    enriched["reviewer_checklist"] = [
        "Does this feel specific to this person/window?",
        "Does it preserve contradiction rather than flatten it?",
        "Does it notice silence/reactivation correctly where relevant?",
        "Does startbrief feel like Sophie knows the person?",
        "Does handover surface what actually matters?",
        "Does anything feel generic, clinical, or surveillance-like?",
    ]
    return enriched


def _markdown_report(report: Dict[str, Any]) -> str:
    def block(title: str, value: Any) -> str:
        return f"## {title}\n\n```json\n{json.dumps(value, ensure_ascii=False, indent=2, default=str)}\n```\n"

    transcript_summary = [
        {
            "session_id": row.get("session_id"),
            "created_at": str(row.get("created_at")),
            "user_turns": [
                (msg or {}).get("text")
                for msg in (row.get("messages") or [])
                if isinstance(msg, dict) and (msg.get("role") or "").lower() == "user"
            ],
        }
        for row in report.get("transcript") or []
    ]
    lines = [
        f"# Derived Pipeline Live Review: {report.get('fixture_id') or report.get('user_id')}",
        "",
        f"- user_id: `{report.get('user_id')}`",
        f"- session_id: `{report.get('session_id')}`",
        f"- fixture: `{report.get('fixture_id') or ''}`",
        "",
        "## Review Flags",
        "",
        json.dumps(report.get("review_flags") or [], ensure_ascii=False, indent=2, default=str),
        "",
        "## Reviewer Checklist",
        "",
    ]
    lines.extend(f"- [ ] {item}" for item in report.get("reviewer_checklist") or [])
    lines.append("")
    lines.append(block("Structural Diff Vs Expected", report.get("structural_diff_vs_expected")))
    lines.append(block("Transcript Input", transcript_summary))
    lines.append(block("Pass 1 Output", report.get("pass1_session_classifications")))
    lines.append(block("Entity Profiles", report.get("entity_profiles")))
    lines.append(block("Threads", report.get("open_threads")))
    lines.append(block("Identity Profile", report.get("identity_profile")))
    lines.append(block("Living Context", report.get("living_context")))
    lines.append(block("Startbrief", report.get("startbrief")))
    lines.append(block("Handover", report.get("handover")))
    lines.append(block("Evidence Refs", report.get("derived_assertions_evidence_refs")))
    return "\n".join(lines)


async def _copy_real_window_to_review_user(source_user_id: str, session_id: Optional[str]) -> tuple[str, str]:
    if not session_id or session_id == "latest":
        session_id = await _latest_session_id(source_user_id)
    if not session_id:
        raise SystemExit(f"no session found for user {source_user_id}")
    rows = await db.fetch(
        """
        SELECT session_id, messages, created_at, updated_at
        FROM session_transcript
        WHERE user_id=$1 AND session_id=$2
        ORDER BY created_at ASC NULLS LAST
        """,
        source_user_id,
        session_id,
    )
    if not rows:
        raise SystemExit(f"session {session_id} not found for user {source_user_id}")
    review_user_id = _unique(f"live-review-{source_user_id[:12]}")
    final_session_id = ""
    for row in rows:
        review_session_id = f"{review_user_id}-{row['session_id']}"
        final_session_id = review_session_id
        await db.execute(
            """
            INSERT INTO session_transcript (tenant_id, session_id, user_id, messages, created_at, updated_at)
            VALUES ('default',$1,$2,$3::jsonb,$4,$5)
            """,
            review_session_id,
            review_user_id,
            row.get("messages") if isinstance(row.get("messages"), list) else [],
            row.get("created_at") or datetime.now(timezone.utc),
            row.get("updated_at") or row.get("created_at") or datetime.now(timezone.utc),
        )
    return review_user_id, final_session_id


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture-id")
    parser.add_argument("--batch-fixtures", action="store_true")
    parser.add_argument("--live-model", action="store_true", help="Use configured model/provider instead of deterministic fixture stubs")
    parser.add_argument("--user-id")
    parser.add_argument("--session-id", default="latest")
    parser.add_argument("--run-live-window", action="store_true", help="Copy selected real session into a temporary review user and run live pipeline")
    parser.add_argument("--output")
    parser.add_argument("--output-dir")
    parser.add_argument("--markdown", action="store_true")
    parser.add_argument("--markdown-output")
    args = parser.parse_args()

    async with _review_services():
        settings = get_settings()
        settings.derived_pipeline_enabled = True
        settings.derived_pipeline_llm_enabled = True
        settings.derived_pipeline_access_bump_enabled = False
        cases = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
        by_id = {c["fixture_id"]: c for c in cases}
        if args.batch_fixtures:
            output_dir = Path(args.output_dir or "/tmp/derived_live_review")
            output_dir.mkdir(parents=True, exist_ok=True)
            reports: List[Dict[str, Any]] = []
            for case in cases:
                if not args.live_model:
                    await _install_fixture_stubs(case, cases)
                user_id = await _seed_fixture(case)
                session_id = await _run_live_fixture(user_id, case)
                report = _attach_review(await _inspect(user_id, session_id), case)
                reports.append(report)
            _add_batch_similarity_flags(reports)
            for report in reports:
                fixture_id = report.get("fixture_id") or "unknown"
                (output_dir / f"{fixture_id}.json").write_text(
                    json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n",
                    encoding="utf-8",
                )
                if args.markdown:
                    (output_dir / f"{fixture_id}.md").write_text(_markdown_report(report), encoding="utf-8")
            index = {
                "output_dir": str(output_dir),
                "count": len(reports),
                "fixtures": [
                    {
                        "fixture_id": r.get("fixture_id"),
                        "review_flag_count": len(r.get("review_flags") or []),
                        "flags": r.get("review_flags") or [],
                    }
                    for r in reports
                ],
            }
            (output_dir / "index.json").write_text(json.dumps(index, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            print(json.dumps(index, ensure_ascii=False, indent=2))
            return
        if args.fixture_id:
            if args.fixture_id not in by_id:
                raise SystemExit(f"unknown fixture_id {args.fixture_id}; available={sorted(by_id)}")
            case = by_id[args.fixture_id]
            if not args.live_model:
                await _install_fixture_stubs(case, cases)
            user_id = await _seed_fixture(case)
            session_id = await _run_live_fixture(user_id, case)
        else:
            if not args.user_id:
                raise SystemExit("provide --fixture-id or --user-id")
            if args.run_live_window:
                user_id, session_id = await _copy_real_window_to_review_user(args.user_id, args.session_id)
                payload = {"tenant_id": "default", "user_id": user_id, "session_id": session_id, "reference_time": datetime.now(timezone.utc).isoformat()}
                await _execute_post_ingest_hook(session.POST_INGEST_HOOK_PASS1_TRIAGE, payload)
                await _execute_post_ingest_hook(session.POST_INGEST_HOOK_PASS1_5_ENTITIES, payload)
                await _execute_post_ingest_hook(session.POST_INGEST_HOOK_PASS3_THREADS, payload)
                await _execute_post_ingest_hook(session.POST_INGEST_HOOK_PASS4_IDENTITY, payload)
                await _execute_post_ingest_hook(session.POST_INGEST_HOOK_PASS5_LIVING_CONTEXT, payload)
            else:
                user_id = args.user_id
                session_id = args.session_id
            case = None
        report = _attach_review(await _inspect(user_id, session_id), case)

    text = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    else:
        print(text)
    if args.markdown_output:
        Path(args.markdown_output).write_text(_markdown_report(report), encoding="utf-8")


if __name__ == "__main__":
    asyncio.run(main())
