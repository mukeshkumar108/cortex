from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from src.main import (
    _apply_user_model_proposal,
    _has_meaningful_user_model_diff,
    _proposal_from_enrichment_payload,
    _compute_daily_analysis_quality_flag,
    _build_user_model_staleness_metadata,
    _default_user_model,
    _normalize_user_model,
    _infer_domain_from_text,
    _is_strategic_goal_candidate,
    _map_loop_domain_to_north_star,
    _normalize_daily_analysis_payload,
    should_use_bridge,
    _run_daily_analysis_for_user,
)
from src.loops import LoopManager


def test_infer_domain_uses_word_boundaries_for_work():
    assert _infer_domain_from_text("Clean the worktop and kitchen tonight") == "general"
    assert _infer_domain_from_text("Ship the product launch this quarter") == "work"


def test_map_loop_domain_to_north_star_respects_explicit_loop_domain():
    assert _map_loop_domain_to_north_star("health", "Build startup discipline") == "health"
    assert _map_loop_domain_to_north_star("work", "Do a 10k walk tomorrow") == "work"


def test_strategic_goal_candidate_rejects_tactical_timeboxed_text():
    tactical_loop = SimpleNamespace(type="commitment", timeHorizon="today")
    strategic_loop = SimpleNamespace(type="thread", timeHorizon="ongoing")

    assert not _is_strategic_goal_candidate(tactical_loop, "Wake at 7 and leave by 8.")
    assert _is_strategic_goal_candidate(
        strategic_loop,
        "Build a sustainable weekly cadence for portfolio shipping.",
    )


def test_apply_user_model_proposal_preserves_stronger_user_stated_north_star():
    current = _default_user_model()
    current["north_star"]["work"]["goal"] = "Build products that change behavior at scale"
    current["north_star"]["work"]["goal_confidence"] = 0.95
    current["north_star"]["work"]["goal_source"] = "user_stated"
    current["north_star"]["work"]["status"] = "active"

    proposal = {
        "north_star": {
            "work": {
                "goal": "Wake at 7 and leave by 8",
                "goal_confidence": 0.55,
                "goal_source": "inferred",
                "status": "active",
            }
        }
    }

    merged = _apply_user_model_proposal(current, proposal)
    assert merged["north_star"]["work"]["goal"] == "Build products that change behavior at scale"
    assert merged["north_star"]["work"]["goal_source"] == "user_stated"
    assert merged["north_star"]["work"]["goal_confidence"] == 0.95


def test_apply_user_model_proposal_llm_enricher_overrides_inferred_same_field():
    current = _default_user_model()
    current["work_context"] = {
        "text": "Legacy inferred work context",
        "confidence": 0.55,
        "source": "inferred",
        "updated_at": "2026-02-20T00:00:00Z",
    }

    proposal = {
        "work_context": {
            "text": "Shaping release quality for memory product",
            "confidence": 0.72,
            "source": "llm_session_enricher",
            "updated_at": "2026-02-22T00:00:00Z",
        }
    }

    merged = _apply_user_model_proposal(current, proposal)
    assert merged["work_context"]["text"] == "Shaping release quality for memory product"
    assert merged["work_context"]["source"] == "llm_session_enricher"


def test_apply_user_model_proposal_preserves_user_stated_over_llm_enricher():
    current = _default_user_model()
    current["current_focus"] = {
        "text": "Ship integration with reliability checks",
        "confidence": 0.92,
        "source": "user_stated",
        "updated_at": "2026-02-20T00:00:00Z",
    }

    proposal = {
        "current_focus": {
            "text": "Keep moving on project updates",
            "confidence": 0.8,
            "source": "llm_session_enricher",
            "updated_at": "2026-02-22T00:00:00Z",
        }
    }

    merged = _apply_user_model_proposal(current, proposal)
    assert merged["current_focus"]["text"] == "Ship integration with reliability checks"
    assert merged["current_focus"]["source"] == "user_stated"


def test_apply_user_model_proposal_is_idempotent_for_patterns_and_relationships():
    current = _default_user_model()
    proposal = {
        "patterns": [
            {
                "text": "Returns to long-standing threads after short pauses",
                "confidence": 0.78,
                "source": "llm_session_enricher",
                "updated_at": "2026-02-22T00:00:00Z",
                "evidence": [
                    {"session_id": "s1", "msg_index": 1, "quote": "I keep returning to the same thread."},
                    {"session_id": "s2", "msg_index": 2, "quote": "Again I am back on that thread."},
                ],
            }
        ],
        "key_relationships": [
            {
                "name": "Ashley",
                "who": "partner",
                "status": "repairing",
                "confidence": 0.79,
                "source": "llm_session_enricher",
                "updated_at": "2026-02-22T00:00:00Z",
            }
        ],
    }

    merged_once = _apply_user_model_proposal(current, proposal)
    merged_twice = _apply_user_model_proposal(merged_once, proposal)
    assert merged_once == merged_twice
    assert len(merged_twice["patterns"]) == 1
    assert len(merged_twice["key_relationships"]) == 1


def test_enrichment_payload_items_without_evidence_are_rejected():
    payload = {
        "key_relationships": [
            {
                "name": "Ashley",
                "who": "partner",
                "status": "repairing",
                "confidence": 0.88,
            }
        ],
        "patterns": [
            {
                "text": "Returns to unresolved threads",
                "confidence": 0.81,
            }
        ],
        "current_focus": {
            "text": "Ship memory reliability improvements",
            "confidence": 0.82,
        },
        "small_human_details": [
            {
                "text": "Feels pressure before shipping",
                "confidence": 0.8,
            }
        ],
        "north_star": {
            "work": {
                "goal": "Ship a reliable memory system",
                "goal_confidence": 0.85,
            }
        },
    }
    proposal = _proposal_from_enrichment_payload(
        payload=payload,
        min_confidence=0.72,
        source="llm_session_enricher",
        now_iso="2026-02-22T00:00:00Z",
    )
    assert proposal == {}


def test_enrichment_pattern_requires_two_sessions_or_habitual():
    payload = {
        "patterns": [
            {
                "text": "Returns to unresolved work threads",
                "confidence": 0.84,
                "evidences": [
                    {"session_id": "s1", "msg_index": 1, "quote": "I keep circling back to the same release problem."},
                    {"session_id": "s2", "msg_index": 3, "quote": "Again I'm stuck on that release reliability issue."},
                ],
                "habitual_explicit": False,
            },
            {
                "text": "went for a walk",
                "confidence": 0.95,
                "evidences": [
                    {"session_id": "s1", "msg_index": 2, "quote": "I went for a walk."},
                    {"session_id": "s2", "msg_index": 1, "quote": "I went for a walk again."},
                ],
                "habitual_explicit": False,
            },
            {
                "text": "tends to delay start until late evening",
                "confidence": 0.78,
                "evidences": [
                    {"session_id": "s3", "msg_index": 0, "quote": "I tend to delay starting until late evening."},
                ],
                "habitual_explicit": True,
            },
        ]
    }
    proposal = _proposal_from_enrichment_payload(
        payload=payload,
        min_confidence=0.72,
        source="llm_session_enricher",
        now_iso="2026-02-22T00:00:00Z",
    )
    texts = [p["text"] for p in proposal.get("patterns", [])]
    assert "Returns to unresolved work threads" in texts
    assert "tends to delay start until late evening" in texts
    assert "went for a walk" not in texts


def test_enrichment_steps_go_to_daily_anchors_not_preferences_notes():
    payload = {
        "small_human_details": [
            {
                "text": "10,000 steps goal",
                "confidence": 0.82,
                "evidence": {"session_id": "s1", "msg_index": 0, "quote": "I will do 10,000 steps tomorrow."},
            },
            {
                "text": "5,000 steps minimum",
                "confidence": 0.8,
                "evidence": {"session_id": "s2", "msg_index": 1, "quote": "At minimum I want 5,000 steps."},
            },
        ]
    }
    proposal = _proposal_from_enrichment_payload(
        payload=payload,
        min_confidence=0.72,
        source="llm_session_enricher",
        now_iso="2026-02-22T00:00:00Z",
    )
    assert "preferences" not in proposal
    anchors = proposal.get("daily_anchors") or {}
    assert anchors.get("steps_goal", {}).get("value") == 10000
    assert anchors.get("minimum_steps", {}).get("value") == 5000


def test_user_model_hygiene_drops_invalid_patterns_and_migrates_step_notes():
    model = _default_user_model()
    model["patterns"] = [
        {
            "text": "going for a walk",
            "source": "llm_session_enricher",
            "confidence": 0.9,
            "evidence": {"session_id": "s1", "msg_index": 0, "quote": "I am going for a walk."},
        },
        {
            "text": "Returns to unresolved release threads",
            "source": "llm_session_enricher",
            "confidence": 0.84,
            "evidence": [
                {"session_id": "s1", "msg_index": 1, "quote": "Still on that release issue."},
                {"session_id": "s2", "msg_index": 2, "quote": "Again back to release reliability."},
            ],
        },
    ]
    model["preferences"] = {
        "notes": ["5,000 steps today", "10,000 steps goal", "prefers direct language"],
        "confidence": 0.72,
        "source": "llm_session_enricher",
        "updated_at": "2026-02-22T13:01:11.686194",
        "evidence": [
            {"session_id": "s3", "msg_index": 3, "quote": "I did 5,000 steps today."},
            {"session_id": "s4", "msg_index": 0, "quote": "I will do a 10K walk tomorrow morning."},
        ],
    }

    normalized = _normalize_user_model(model)
    texts = [row.get("text") for row in normalized.get("patterns", []) if isinstance(row, dict)]
    assert "going for a walk" not in texts
    assert "Returns to unresolved release threads" in texts
    assert normalized.get("preferences", {}).get("notes") == ["prefers direct language"]
    assert normalized.get("daily_anchors", {}).get("steps_goal", {}).get("value") == 10000
    assert normalized.get("daily_anchors", {}).get("minimum_steps", {}).get("value") == 5000


def test_user_model_hygiene_drops_malformed_relationship_entries():
    model = _default_user_model()
    model["key_relationships"] = [
        {
            "name": "and",
            "who": "brother",
            "status": "active",
            "source": "llm_session_enricher",
            "confidence": 0.82,
        },
        {
            "name": "Nina",
            "who": "partner",
            "status": "active",
            "source": "user_stated",
            "confidence": 0.91,
        },
        {
            "name": "Alex",
            "who": "",
            "status": "active",
            "source": "inferred",
            "confidence": 0.7,
        },
    ]

    normalized = _normalize_user_model(model)
    relationships = normalized.get("key_relationships") or []
    assert len(relationships) == 1
    assert relationships[0]["name"] == "Nina"
    assert relationships[0]["who"] == "partner"
    assert relationships[0]["status"] == "active"


def test_hygiene_meaningful_diff_ignores_metadata_and_order():
    before = _default_user_model()
    before["patterns"] = [
        {
            "text": "Returns to unresolved release threads",
            "source": "llm_session_enricher",
            "confidence": 0.81,
            "updated_at": "2026-02-22T10:00:00Z",
        }
    ]
    before["preferences"] = {
        "notes": ["prefers direct language", "short check-ins"],
        "source": "llm_session_enricher",
        "updated_at": "2026-02-22T10:00:00Z",
        "confidence": 0.7,
    }

    after = _default_user_model()
    after["patterns"] = [
        {
            "text": "Returns to unresolved release threads",
            "source": "inferred",
            "confidence": 0.55,
            "updated_at": "2026-02-22T12:00:00Z",
        }
    ]
    after["preferences"] = {
        "notes": ["short check-ins", "prefers direct language"],
        "source": "inferred",
        "updated_at": "2026-02-22T12:00:00Z",
        "confidence": 0.55,
    }

    assert _has_meaningful_user_model_diff(before, after) is False


def test_user_model_staleness_metadata_thresholds():
    now = datetime(2026, 2, 19, 12, 0, 0)
    model = _default_user_model()
    model["current_focus"] = {
        "text": "Ship integration",
        "confidence": 0.8,
        "source": "user_stated",
        "updated_at": (now - timedelta(days=11)).isoformat() + "Z",
    }
    model["work_context"] = {
        "text": "Building Sophie + Synapse",
        "confidence": 0.7,
        "source": "inferred",
        "updated_at": (now - timedelta(days=12)).isoformat() + "Z",
    }
    model["north_star"]["work"]["goal"] = "Build durable products"
    model["north_star"]["work"]["goal_source"] = "user_stated"
    model["north_star"]["work"]["goal_confidence"] = 0.9
    model["north_star"]["work"]["updated_at"] = (now - timedelta(days=30)).isoformat() + "Z"

    metadata = _build_user_model_staleness_metadata(model, now=now)
    fields = metadata["staleness"]["fields"]
    assert fields["current_focus"]["stale"] is True
    assert fields["current_focus"]["thresholdDays"] == 10
    assert fields["work_context"]["stale"] is False
    assert fields["north_star.work"]["stale"] is True
    assert "north_star.work" in metadata["staleness"]["stalePaths"]


def test_loop_staleness_target_status_policy():
    now = datetime(2026, 2, 19, 12, 0, 0)
    assert LoopManager._staleness_target_status("today", now - timedelta(hours=49), now=now) == "stale"
    assert LoopManager._staleness_target_status("this_week", now - timedelta(days=11), now=now) == "stale"
    assert LoopManager._staleness_target_status("ongoing", now - timedelta(days=22), now=now) == "needs_review"
    assert LoopManager._staleness_target_status("ongoing", now - timedelta(days=5), now=now) is None


@pytest.mark.asyncio
async def test_apply_global_staleness_policy_returns_counts():
    class _FakeDB:
        def __init__(self):
            self.calls = 0

        async def fetchval(self, _query):
            self.calls += 1
            return 2

    manager = LoopManager(_FakeDB())
    result = await manager.apply_global_staleness_policy()
    assert result == {
        "stale_today": 2,
        "stale_this_week": 2,
        "needs_review_ongoing": 2,
    }
    assert manager.db.calls == 3


def test_normalize_daily_analysis_payload_clamps_and_falls_back():
    payload = {
        "themes": ["Pressure spiral", "Planning overload"],
        "scores": {"curiosity": 9, "warmth": 0, "usefulness": 3, "forward_motion": "4"},
        "steering_note": "Lead with one grounded question, then give one next-step suggestion.",
        "confidence": 1.2,
    }
    turns = [
        {"role": "user", "text": "I feel stressed and need structure."},
        {"role": "assistant", "text": "Want one step for tomorrow?"},
    ]
    out = _normalize_daily_analysis_payload(payload, turns)
    assert out["themes"] == ["Pressure spiral", "Planning overload"]
    assert out["scores"] == {
        "curiosity": 5,
        "warmth": 1,
        "usefulness": 3,
        "forward_motion": 4,
    }
    assert out["steering_note"]
    assert 0.0 <= out["confidence"] <= 1.0


@pytest.mark.asyncio
async def test_daily_analysis_quality_flag_insufficient_data():
    flag = await _compute_daily_analysis_quality_flag(
        tenant_id="t",
        user_id="u",
        analysis_date=datetime(2026, 2, 19).date(),
        confidence=0.9,
        turn_count=2
    )
    assert flag == "insufficient_data"


@pytest.mark.asyncio
async def test_daily_analysis_quality_flag_needs_review_streak(monkeypatch):
    async def _stub_fetchone(*_args, **_kwargs):
        return {"confidence": 0.4}

    monkeypatch.setattr("src.main.db.fetchone", _stub_fetchone, raising=False)

    flag = await _compute_daily_analysis_quality_flag(
        tenant_id="t",
        user_id="u",
        analysis_date=datetime(2026, 2, 19).date(),
        confidence=0.5,
        turn_count=12
    )
    assert flag == "needs_review"


def test_should_use_bridge_ttl_boundaries():
    now = datetime(2026, 2, 20, 12, 0, 0)
    assert should_use_bridge(now - timedelta(minutes=30), now, ttl_minutes=30) is True
    assert should_use_bridge(now - timedelta(minutes=31), now, ttl_minutes=30) is False


@pytest.mark.asyncio
async def test_daily_analysis_prefers_session_summaries_and_sets_metadata(monkeypatch):
    captured = {}

    async def _stub_turns(**_kwargs):
        return ([{"role": "user", "text": "turn fallback"}], ["sess-turn-1"])

    async def _stub_summaries(**_kwargs):
        return [{
            "session_id": "sess-s1",
            "created_at": "2026-02-19T10:00:00Z",
            "salience": "high",
            "summary_facts": "User committed to ship.",
            "tone": "focused",
            "moment": "User decided to ship.",
            "decisions": ["Ship today"],
            "unresolved": [],
            "index_text": "User committed to ship. Decisions: Ship today",
        }]

    async def _stub_generate(*, turns, session_summaries=None):
        captured["generate"] = {"turns": turns, "session_summaries": session_summaries}
        return {
            "themes": ["Avoidance before commitment"],
            "scores": {"curiosity": 3, "warmth": 3, "usefulness": 4, "forward_motion": 4},
            "steering_note": "They committed to ship today, so open by checking whether that happened.",
            "confidence": 0.8,
            "source": "llm",
        }

    async def _stub_quality(**_kwargs):
        return None

    async def _stub_upsert(*, tenant_id, user_id, analysis_date, analysis, metadata):
        captured["upsert"] = {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "analysis_date": analysis_date,
            "analysis": analysis,
            "metadata": metadata,
        }

    monkeypatch.setattr("src.main._get_user_daily_turns", _stub_turns, raising=True)
    monkeypatch.setattr("src.main._get_user_daily_session_summaries", _stub_summaries, raising=True)
    monkeypatch.setattr("src.main._generate_daily_analysis", _stub_generate, raising=True)
    monkeypatch.setattr("src.main._compute_daily_analysis_quality_flag", _stub_quality, raising=True)
    monkeypatch.setattr("src.main._upsert_daily_analysis", _stub_upsert, raising=True)

    ok = await _run_daily_analysis_for_user(
        tenant_id="t",
        user_id="u",
        target_date=datetime(2026, 2, 19).date(),
        max_turns=50,
    )
    assert ok is True
    assert captured["generate"]["session_summaries"]
    assert captured["upsert"]["metadata"]["input_mode"] == "session_summaries"
    assert captured["upsert"]["metadata"]["used_turn_tail"] is False
    assert captured["upsert"]["metadata"]["session_count"] == 1
    assert captured["upsert"]["metadata"]["salience_counts"]["high"] == 1
    assert captured["upsert"]["metadata"]["analysis_version"] == "v2"


@pytest.mark.asyncio
async def test_daily_analysis_falls_back_to_turns_when_no_summaries(monkeypatch):
    captured = {}

    async def _stub_turns(**_kwargs):
        return ([{"role": "user", "text": "raw turn"}], ["sess-turn-1"])

    async def _stub_summaries(**_kwargs):
        return []

    async def _stub_generate(*, turns, session_summaries=None):
        captured["generate"] = {"turns": turns, "session_summaries": session_summaries}
        return {
            "themes": ["Reflective processing without commitment"],
            "scores": {"curiosity": 3, "warmth": 3, "usefulness": 3, "forward_motion": 2},
            "steering_note": "They kept planning without action today, so start by asking for one concrete step.",
            "confidence": 0.5,
            "source": "llm",
        }

    async def _stub_quality(**_kwargs):
        return None

    async def _stub_upsert(*, tenant_id, user_id, analysis_date, analysis, metadata):
        captured["upsert"] = {"analysis": analysis, "metadata": metadata}

    monkeypatch.setattr("src.main._get_user_daily_turns", _stub_turns, raising=True)
    monkeypatch.setattr("src.main._get_user_daily_session_summaries", _stub_summaries, raising=True)
    monkeypatch.setattr("src.main._generate_daily_analysis", _stub_generate, raising=True)
    monkeypatch.setattr("src.main._compute_daily_analysis_quality_flag", _stub_quality, raising=True)
    monkeypatch.setattr("src.main._upsert_daily_analysis", _stub_upsert, raising=True)

    ok = await _run_daily_analysis_for_user(
        tenant_id="t",
        user_id="u",
        target_date=datetime(2026, 2, 19).date(),
        max_turns=50,
    )
    assert ok is True
    assert captured["generate"]["session_summaries"] is None
    assert captured["upsert"]["metadata"]["input_mode"] == "fallback_raw_turns"
    assert captured["upsert"]["metadata"]["used_turn_tail"] is True
