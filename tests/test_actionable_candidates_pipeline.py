from datetime import datetime, timezone

import pytest

from src.derived_passes.pass2_actionable import _normalize_actionable_payload, run_pass2_actionable_llm

def test_normalize_actionable_payload_dedupes_and_strips_speaker_prefixes():
    out = _normalize_actionable_payload(
        {
            "actionable_candidates": [
                {
                    "record_type": "task_candidate",
                    "title": "user: Send the deck",
                    "summary": "user: I need to send the deck",
                    "status": "detected",
                },
                {
                    "record_type": "task_candidate",
                    "title": "Send the deck",
                    "summary": "I need to send the deck",
                    "status": "detected",
                },
                {
                    "record_type": "task_candidate",
                    "title": "Send the deck",
                    "summary": "I need to send the deck",
                    "status": "detected",
                },
            ]
        }
    )

    assert len(out) == 1
    assert out[0]["title"] == "Send the deck"
    assert out[0]["summary"] == "I need to send the deck"


def test_normalize_actionable_payload_preserves_llm_status_for_underspecified_items():
    out = _normalize_actionable_payload(
        {
            "actionable_candidates": [
                {
                    "record_type": "event_candidate",
                    "title": "Meet Friday evening",
                    "summary": "we should meet Friday evening",
                    "due_iso": None,
                    "status": "needs_review",
                }
            ]
        }
    )

    assert len(out) == 1
    assert out[0]["status"] == "needs_review"


def test_normalize_actionable_payload_preserves_candidate_subtypes_and_support_fields():
    out = _normalize_actionable_payload(
        {
            "actionable_candidates": [
                {
                    "record_type": "task_candidate",
                    "candidate_subtype": "habit",
                    "title": "Daily morning walk",
                    "summary": "I want to walk every morning",
                    "cadence_text": "every morning",
                    "suggested_action": "ask_user",
                    "status": "detected",
                },
                {
                    "record_type": "task_candidate",
                    "candidate_subtype": "waiting_on",
                    "title": "Waiting on revised deck from John",
                    "summary": "I'm waiting for John to send the revised deck",
                    "waiting_on": "John",
                    "needs_response": True,
                    "suggested_action": "ask_user",
                    "status": "needs_review",
                },
                {
                    "record_type": "commitment",
                    "candidate_subtype": "nudge",
                    "title": "Prompt me to step away from work tonight",
                    "summary": "I may need a push to log off tonight",
                    "suggested_action": "ignore",
                    "status": "detected",
                },
            ]
        }
    )

    assert len(out) == 3
    assert out[0]["candidate_subtype"] == "habit"
    assert out[0]["cadence_text"] == "every morning"
    assert out[1]["candidate_subtype"] == "waiting_on"
    assert out[1]["waiting_on"] == "John"
    assert out[1]["needs_response"] is True
    assert out[2]["candidate_subtype"] == "nudge"


def test_normalize_actionable_payload_preserves_resolution_metadata_in_provenance():
    out = _normalize_actionable_payload(
        {
            "actionable_candidates": [
                {
                    "record_type": "task_candidate",
                    "candidate_subtype": "todo",
                    "title": "Ship iOS MVP",
                    "summary": "User needs to get the MVP shipped for testing.",
                    "provenance": {
                        "message_hint": "We need to get it delivered.",
                        "resolved_object": "iOS React Native MVP project",
                        "related_entities": ["Yale co-founder", "iOS app"],
                        "resolution_confidence": "medium",
                        "resolution_basis": "inferred",
                    },
                    "status": "needs_review",
                }
            ]
        }
    )

    assert len(out) == 1
    provenance = out[0]["provenance"]
    assert provenance["resolved_object"] == "iOS React Native MVP project"
    assert provenance["related_entities"] == ["Yale co-founder", "iOS app"]
    assert provenance["resolution_confidence"] == "medium"
    assert provenance["resolution_basis"] == "inferred"


def test_normalize_actionable_payload_preserves_temporal_raw_phrases_and_unresolved_basis():
    out = _normalize_actionable_payload(
        {
            "actionable_candidates": [
                {
                    "record_type": "task_candidate",
                    "candidate_subtype": "todo",
                    "title": "Send the deck",
                    "summary": "Send the deck soon.",
                    "due_iso": "2026-04-30T09:00:00Z",
                    "due_text": "soon",
                    "time_text": "later",
                    "provenance": {"date_resolution_basis": "unresolved"},
                    "status": "needs_review",
                }
            ]
        }
    )

    assert len(out) == 1
    assert out[0]["due_iso"] is None
    assert out[0]["provenance"]["due_text"] == "soon"
    assert out[0]["provenance"]["time_text"] == "later"
    assert out[0]["provenance"]["date_resolution_basis"] == "unresolved"


@pytest.mark.asyncio
async def test_run_pass2_actionable_llm_includes_temporal_reference_context(monkeypatch):
    captured = {}

    async def _stub_call_json_llm(*, prompt, model, max_tokens=1600, temperature=0.1):
        captured["prompt"] = prompt
        return {"actionable_candidates": []}

    monkeypatch.setattr("src.derived_passes.pass2_actionable.call_json_llm", _stub_call_json_llm, raising=True)

    await run_pass2_actionable_llm(
        messages=[{"role": "user", "text": "Remind me tomorrow.", "timestamp": "2026-04-27T09:00:00Z"}],
        model="test-model",
        reference_time=datetime(2026, 4, 27, 21, 15, tzinfo=timezone.utc),
        timezone_name="UTC",
    )

    prompt = captured["prompt"]
    assert "now_iso: 2026-04-27T21:15:00+00:00" in prompt
    assert "timezone: UTC" in prompt
    assert "local_date: 2026-04-27" in prompt
    assert "local_day: Monday" in prompt
    assert "time_of_day: evening" in prompt


def test_normalize_actionable_payload_handles_relative_and_missing_dates():
    out = _normalize_actionable_payload(
        {
            "actionable_candidates": [
                {
                    "record_type": "reminder_candidate",
                    "candidate_subtype": "reminder",
                    "title": "Call John",
                    "summary": "Call John tomorrow.",
                    "due_iso": "2026-04-28T09:00:00Z",
                    "due_text": "tomorrow",
                    "provenance": {"date_resolution_basis": "relative_date"},
                    "status": "detected",
                },
                {
                    "record_type": "event_candidate",
                    "candidate_subtype": "calendar_event",
                    "title": "Meet Friday evening",
                    "summary": "Meet Friday evening.",
                    "due_iso": "2026-05-01T18:00:00Z",
                    "due_text": "Friday",
                    "time_text": "evening",
                    "provenance": {"date_resolution_basis": "relative_date"},
                    "status": "needs_review",
                },
                {
                    "record_type": "task_candidate",
                    "candidate_subtype": "todo",
                    "title": "Send the deck",
                    "summary": "Send the deck.",
                    "status": "detected",
                },
                {
                    "record_type": "event_candidate",
                    "candidate_subtype": "calendar_event",
                    "title": "Dinner tonight",
                    "summary": "Dinner tonight.",
                    "due_iso": "2026-04-27T19:00:00Z",
                    "time_text": "tonight",
                    "provenance": {"date_resolution_basis": "relative_date"},
                    "status": "detected",
                },
            ]
        }
    )

    assert len(out) == 4
    assert out[0]["due_iso"] == "2026-04-28T09:00:00Z"
    assert out[0]["provenance"]["due_text"] == "tomorrow"
    assert out[1]["provenance"]["due_text"] == "Friday"
    assert out[1]["provenance"]["time_text"] == "evening"
    assert out[2]["due_iso"] is None
    assert out[3]["provenance"]["time_text"] == "tonight"


def test_explicit_reminder_command_structured_fields():
    out = _normalize_actionable_payload(
        {
            "actionable_candidates": [
                {
                    "record_type": "reminder_candidate",
                    "candidate_subtype": "reminder",
                    "title": "Remind me to call mom",
                    "summary": "Direct reminder request from user.",
                    "confidence": 0.94,
                    "provenance_summary": "direct reminder ask",
                    "risk_tags": [],
                    "has_direct_user_expression": True,
                    "requires_confirmation": False,
                    "suggested_action": "create_action_item",
                    "proposed_due_at": "2026-04-29T10:00:00Z",
                    "proposed_remind_at": "2026-04-29T09:30:00Z",
                    "status": "detected",
                }
            ]
        }
    )
    assert len(out) == 1
    row = out[0]
    assert row["candidate_subtype"] == "reminder"
    assert row["confidence"] == 0.94
    assert row["has_direct_user_expression"] is True
    assert row["requires_confirmation"] is False
    assert row["suggested_action"] == "create_action_item"


def test_implicit_todo_requires_confirmation():
    out = _normalize_actionable_payload(
        {
            "actionable_candidates": [
                {
                    "record_type": "task_candidate",
                    "candidate_subtype": "todo",
                    "title": "Prepare onboarding notes",
                    "summary": "Likely next step inferred from context.",
                    "confidence": 0.61,
                    "has_direct_user_expression": False,
                    "requires_confirmation": True,
                    "suggested_action": "ask_user",
                    "status": "needs_review",
                }
            ]
        }
    )
    assert len(out) == 1
    assert out[0]["requires_confirmation"] is True
    assert out[0]["suggested_action"] == "ask_user"


def test_calendar_event_candidate_is_confirmation_only():
    out = _normalize_actionable_payload(
        {
            "actionable_candidates": [
                {
                    "record_type": "event_candidate",
                    "candidate_subtype": "calendar_event",
                    "title": "Meeting with Ops Thursday",
                    "summary": "Potential meeting slot.",
                    "confidence": 0.9,
                    "requires_confirmation": True,
                    "suggested_action": "ask_user",
                    "status": "needs_review",
                }
            ]
        }
    )
    assert len(out) == 1
    assert out[0]["candidate_subtype"] == "calendar_event"
    assert out[0]["requires_confirmation"] is True


def test_private_source_signal_not_auto_actionable_fields():
    out = _normalize_actionable_payload(
        {
            "actionable_candidates": [
                {
                    "record_type": "task_candidate",
                    "candidate_subtype": "todo",
                    "title": "Check in later",
                    "summary": "Derived from private signal only.",
                    "confidence": 0.9,
                    "risk_tags": ["private_source"],
                    "has_direct_user_expression": False,
                    "requires_confirmation": True,
                    "suggested_action": "ask_user",
                    "status": "needs_review",
                }
            ]
        }
    )
    assert len(out) == 1
    assert "private_source" in out[0]["risk_tags"]
    assert out[0]["has_direct_user_expression"] is False
    assert out[0]["requires_confirmation"] is True


def test_high_confidence_low_risk_todo_can_auto_promote_fields():
    out = _normalize_actionable_payload(
        {
            "actionable_candidates": [
                {
                    "record_type": "task_candidate",
                    "candidate_subtype": "todo",
                    "title": "Send invoice PDF",
                    "summary": "Direct user request.",
                    "confidence": 0.88,
                    "risk_tags": [],
                    "has_direct_user_expression": True,
                    "requires_confirmation": False,
                    "suggested_action": "create_action_item",
                    "status": "detected",
                }
            ]
        }
    )
    assert len(out) == 1
    assert out[0]["confidence"] >= 0.85
    assert out[0]["candidate_subtype"] == "todo"
    assert out[0]["suggested_action"] == "create_action_item"
