from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pytest
from httpx import ASGITransport, AsyncClient

from src.attention_preview import build_attention_preview
from src import main as main_module


NOW = datetime(2026, 5, 19, 9, 0, tzinfo=timezone.utc)


class FakeDB:
    def __init__(self):
        self.actionable_rows: List[Dict[str, Any]] = []
        self.follow_up_rows: List[Dict[str, Any]] = []
        self.clarification_rows: List[Dict[str, Any]] = []
        self.thread_rows: List[Dict[str, Any]] = []
        self.action_item_rows: List[Dict[str, Any]] = []
        self.calendar_rows: List[Dict[str, Any]] = []
        self.living_context_row: Dict[str, Any] | None = None
        self.outcome_rows: List[Dict[str, Any]] = []

    async def fetchone(self, query: str, *args):
        if "FROM living_context" in query:
            return self.living_context_row
        if "INSERT INTO attention_outcomes" in query:
            row = {
                "outcome_id": f"outcome-{len(self.outcome_rows) + 1}",
                "tenant_id": args[0],
                "user_id": args[1],
                "companion_id": args[2],
                "attention_item_id": args[3],
                "source_table": args[4],
                "source_id": args[5],
                "outcome_type": args[6],
                "outcome_reason": args[7],
                "surface_mode": args[8],
                "action_policy": args[9],
                "snoozed_until": args[10],
                "suppress_until": args[11],
                "metadata": args[12] or {},
                "occurred_at": NOW,
                "created_at": NOW,
            }
            self.outcome_rows.append(row)
            return row
        return None

    async def fetch(self, query: str, *args):
        if "FROM actionable_candidates" in query:
            return self.actionable_rows
        if "FROM follow_up_candidates" in query:
            return self.follow_up_rows
        if "FROM clarification_candidates" in query:
            return self.clarification_rows
        if "FROM open_threads" in query:
            return self.thread_rows
        if "FROM action_items" in query:
            return self.action_item_rows
        if "FROM calendar_items" in query:
            return self.calendar_rows
        if "FROM attention_outcomes" in query:
            return self.outcome_rows
        return []


def _ashley_unwell_follow_up_row() -> Dict[str, Any]:
    return {
        "candidate_id": 11,
        "tenant_id": "default",
        "user_id": "u1",
        "candidate_key": "ashley-takedown",
        "title": "Check on Ashley and the takedown",
        "reason": "Ashley was unwell and the event takedown likely needs a gentle follow-up this morning.",
        "priority_score": 0.91,
        "confidence": 0.86,
        "due_at": NOW,
        "status": "shadow_open",
        "source_turn_refs": [{"session_id": "s1", "turn_id": "t3", "text": "Ashley called in sick"}],
        "metadata": {
            "source_object_ids": ["person:ashley", "workstream:event-takedown", "state:unwell"],
            "source_link_ids": ["link-involves", "link-affects"],
        },
        "created_at": NOW - timedelta(hours=10),
        "updated_at": NOW - timedelta(hours=1),
    }


def _invoice_actionable_row() -> Dict[str, Any]:
    return {
        "candidate_id": 21,
        "tenant_id": "default",
        "user_id": "u1",
        "record_type": "task_candidate",
        "candidate_subtype": "waiting_on",
        "title": "Nudge Acme about invoice #102",
        "summary": "Still waiting on Acme to pay; invoice is blocking the client workstream.",
        "due_iso": NOW + timedelta(hours=4),
        "relevant_from_iso": NOW - timedelta(hours=1),
        "relevant_until_iso": NOW + timedelta(days=3),
        "status": "detected",
        "confidence_score": 0.73,
        "confidence": 0.73,
        "provenance": {"source_turn_refs": [{"session_id": "s2", "turn_id": "t5", "text": "Still waiting on Acme to pay"}]},
        "metadata": {"source_object_ids": ["org:acme", "obligation:invoice-102"], "source_link_ids": ["link-waiting-on"]},
        "created_at": NOW - timedelta(days=1),
        "updated_at": NOW - timedelta(minutes=30),
        "proposed_due_at": NOW + timedelta(hours=4),
    }


def _upcoming_appointment_row() -> Dict[str, Any]:
    starts_at = NOW + timedelta(hours=26)
    return {
        "id": "event-1",
        "tenant_id": "default",
        "user_id": "u1",
        "title": "Dr. Smith appointment",
        "description": "Prep symptom list before the appointment.",
        "notes": None,
        "starts_at": starts_at,
        "ends_at": starts_at + timedelta(hours=1),
        "status": "confirmed",
        "source_ref": {"source_object_ids": ["event:dr-smith-appointment"], "source_link_ids": ["link-relates-to-state"]},
        "evidence_refs": [{"session_id": "s3", "turn_id": "t2", "text": "My knee pain has been worse this week"}],
        "confidence": 0.88,
        "created_at": NOW - timedelta(days=2),
        "updated_at": NOW - timedelta(hours=2),
    }


def _expired_smoke_row() -> Dict[str, Any]:
    stale_due = NOW - timedelta(days=9)
    return {
        "id": "action-old-1",
        "tenant_id": "default",
        "user_id": "u1",
        "kind": "todo",
        "title": "Ancient smoke reminder",
        "notes": "Old smoke data that should not dominate the default preview.",
        "status": "pending",
        "due_at": stale_due,
        "remind_at": stale_due,
        "source_ref": {},
        "confidence": 0.5,
        "created_at": NOW - timedelta(days=14),
        "updated_at": NOW - timedelta(days=10),
    }


@pytest.mark.asyncio
async def test_magic_moment_ashley_unwell_event_takedown():
    db = FakeDB()
    db.follow_up_rows = [_ashley_unwell_follow_up_row()]

    out = await build_attention_preview(db, tenant_id="default", user_id="u1", companion_id="sophie", as_of=NOW)

    assert len(out["items"]) == 1
    item = out["items"][0]
    assert item["attention_type"] in {"follow_up", "check_in"}
    assert item["surface_mode"] == "proactive_allowed"
    assert item["action_policy"] in {"suggest_only", "draft_only"}
    assert item["status"] in {"eligible", "pending"}
    assert item["expires_at"] == (NOW + timedelta(days=2)).isoformat()
    assert item["attention_type"] != "escalation_candidate"


@pytest.mark.asyncio
async def test_magic_moment_ashley_unwell_hidden_for_admin_only():
    db = FakeDB()
    db.follow_up_rows = [_ashley_unwell_follow_up_row()]

    out = await build_attention_preview(db, tenant_id="default", user_id="u1", companion_id="admin_only", as_of=NOW)

    assert out["items"] == []
    assert out["metadata"]["suppressedCount"] == 1


@pytest.mark.asyncio
async def test_magic_moment_unpaid_invoice_client_chase_for_ops_profiles():
    db = FakeDB()
    db.actionable_rows = [_invoice_actionable_row()]

    ashley_out = await build_attention_preview(db, tenant_id="default", user_id="u1", companion_id="ashley_ops", as_of=NOW)
    admin_out = await build_attention_preview(db, tenant_id="default", user_id="u1", companion_id="admin_only", as_of=NOW)

    item = ashley_out["items"][0]
    assert item["attention_type"] in {"follow_up", "opportunity"}
    assert item["action_policy"] == "draft_only"
    assert item["surface_mode"] in {"proactive_allowed", "proactive_recommended"}
    assert item["sensitivity"] == "low"
    assert admin_out["items"][0]["title"] == "Nudge Acme about invoice #102"


@pytest.mark.asyncio
async def test_magic_moment_upcoming_appointment_prep_expires_at_start():
    db = FakeDB()
    db.calendar_rows = [_upcoming_appointment_row()]

    out = await build_attention_preview(db, tenant_id="default", user_id="u1", companion_id="chronic_health", as_of=NOW)

    assert len(out["items"]) == 1
    item = out["items"][0]
    assert item["attention_type"] in {"prep", "reminder"}
    assert item["action_policy"] == "suggest_only"
    assert item["status"] == "pending"
    assert item["expires_at"] == (NOW + timedelta(hours=26)).isoformat()


@pytest.mark.asyncio
async def test_default_excludes_expired_items_and_include_expired_includes_them():
    db = FakeDB()
    db.follow_up_rows = [_ashley_unwell_follow_up_row()]
    db.action_item_rows = [_expired_smoke_row()]

    default_out = await build_attention_preview(db, tenant_id="default", user_id="u1", companion_id="sophie", as_of=NOW)
    include_out = await build_attention_preview(
        db,
        tenant_id="default",
        user_id="u1",
        companion_id="sophie",
        include_expired=True,
        as_of=NOW,
    )

    assert [item["title"] for item in default_out["items"]] == ["Check on Ashley and the takedown"]
    assert {item["title"] for item in include_out["items"]} == {"Check on Ashley and the takedown", "Ancient smoke reminder"}
    assert default_out["metadata"]["expiredCount"] == 1
    assert default_out["metadata"]["returnedCount"] == 1
    assert include_out["metadata"]["returnedCount"] == 2


@pytest.mark.asyncio
async def test_status_source_and_type_metadata_counts_are_correct():
    db = FakeDB()
    db.follow_up_rows = [_ashley_unwell_follow_up_row()]
    db.actionable_rows = [_invoice_actionable_row()]
    db.calendar_rows = [_upcoming_appointment_row()]
    db.action_item_rows = [_expired_smoke_row()]

    out = await build_attention_preview(db, tenant_id="default", user_id="u1", companion_id="sophie", as_of=NOW)
    meta = out["metadata"]

    assert meta["totalGenerated"] == 4
    assert meta["returnedCount"] == 3
    assert meta["suppressedCount"] == 0
    assert meta["expiredCount"] == 1
    assert meta["byStatus"]["eligible"] == 2
    assert meta["byStatus"]["pending"] == 1
    assert meta["byStatus"]["expired"] == 1
    assert meta["bySourceTable"]["follow_up_candidates"] == 1
    assert meta["bySourceTable"]["actionable_candidates"] == 1
    assert meta["bySourceTable"]["calendar_items"] == 1
    assert meta["bySourceTable"]["action_items"] == 1
    assert meta["byAttentionType"]["follow_up"] >= 1
    assert meta["profileApplied"] == "sophie"
    assert meta["readOnly"] is True


@pytest.mark.asyncio
async def test_living_context_does_not_create_attention_items_by_itself():
    db = FakeDB()
    db.living_context_row = {
        "current_focus": "recovery",
        "emotional_texture": "overwhelmed",
        "primary_tension": "too many obligations",
    }

    out = await build_attention_preview(db, tenant_id="default", user_id="u1", companion_id="sophie", as_of=NOW)

    assert out["items"] == []
    assert out["metadata"]["livingContextUsedAsModifier"] is True


@pytest.mark.asyncio
async def test_ranking_prefers_useful_items_before_expired_and_then_priority():
    db = FakeDB()
    later_follow_up = _ashley_unwell_follow_up_row()
    later_follow_up["candidate_id"] = 12
    later_follow_up["title"] = "Lower priority follow-up"
    later_follow_up["priority_score"] = 0.45
    later_follow_up["updated_at"] = NOW - timedelta(hours=3)
    db.follow_up_rows = [later_follow_up, _ashley_unwell_follow_up_row()]
    db.action_item_rows = [_expired_smoke_row()]

    out = await build_attention_preview(
        db,
        tenant_id="default",
        user_id="u1",
        companion_id="sophie",
        include_expired=True,
        as_of=NOW,
    )

    assert [item["title"] for item in out["items"]] == [
        "Check on Ashley and the takedown",
        "Lower priority follow-up",
        "Ancient smoke reminder",
    ]


@pytest.mark.asyncio
async def test_internal_attention_endpoint_uses_auth_and_passes_include_expired(monkeypatch):
    captured: Dict[str, Any] = {}
    payload = {
        "tenantId": "default",
        "userId": "u1",
        "companionId": "sophie",
        "asOf": NOW.isoformat(),
        "items": [
            {
                "id": "follow_up_candidates:11",
                "tenant_id": "default",
                "user_id": "u1",
                "companion_id": "sophie",
                "title": "Check on Ashley and the takedown",
                "reason": "Follow-up is due today.",
                "attention_type": "follow_up",
                "surface_mode": "proactive_allowed",
                "action_policy": "suggest_only",
                "priority": 90,
                "urgency": 85,
                "importance": 80,
                "confidence": 0.9,
                "sensitivity": "low",
                "earliest_surface_at": NOW.isoformat(),
                "ideal_surface_at": NOW.isoformat(),
                "latest_useful_at": (NOW + timedelta(days=1)).isoformat(),
                "expires_at": (NOW + timedelta(days=2)).isoformat(),
                "status": "eligible",
                "source_table": "follow_up_candidates",
                "source_id": "11",
                "source_object_ids": ["person:ashley"],
                "source_link_ids": ["link-1"],
                "evidence_refs": [{"session_id": "s1"}],
                "gaps": [],
                "missing_metadata": [],
            }
        ],
        "metadata": {
            "totalGenerated": 1,
            "returnedCount": 1,
            "suppressedCount": 0,
            "expiredCount": 0,
            "byStatus": {"eligible": 1},
            "bySourceTable": {"follow_up_candidates": 1},
            "byAttentionType": {"follow_up": 1},
            "profileApplied": "sophie",
            "readOnly": True,
        },
    }

    async def _stub_build_attention_preview(*_args, **kwargs):
        captured.update(kwargs)
        return payload

    monkeypatch.setattr(main_module, "build_attention_preview", _stub_build_attention_preview, raising=True)

    async with AsyncClient(transport=ASGITransport(app=main_module.app), base_url="http://test") as client:
        response = await client.get(
            "/internal/debug/attention",
            params={
                "tenantId": "default",
                "userId": "u1",
                "companionId": "sophie",
                "includeExpired": "true",
                "includeSuppressed": "true",
            },
            headers={"X-Internal-Token": "test-token"},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["userId"] == "u1"
    assert data["metadata"]["returnedCount"] == 1
    assert captured["include_expired"] is True
    assert captured["include_suppressed"] is True


@pytest.mark.asyncio
async def test_can_record_dismissed_attention_outcome_via_internal_endpoint(monkeypatch):
    db = FakeDB()
    monkeypatch.setattr(main_module, "db", db, raising=True)

    async with AsyncClient(transport=ASGITransport(app=main_module.app), base_url="http://test") as client:
        response = await client.post(
            "/internal/debug/attention/outcome",
            json={
                "tenantId": "default",
                "userId": "u1",
                "companionId": "sophie",
                "attentionItemId": "follow_up_candidates:11",
                "sourceTable": "follow_up_candidates",
                "sourceId": "11",
                "outcomeType": "dismissed",
                "outcomeReason": "User said this is already handled",
                "metadata": {"source": "test"},
            },
            headers={"X-Internal-Token": "test-token"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["outcomeType"] == "dismissed"
    assert payload["attentionItemId"] == "follow_up_candidates:11"
    assert payload["sourceTable"] == "follow_up_candidates"
    assert payload["metadata"] == {"source": "test"}


@pytest.mark.asyncio
async def test_dismissed_item_no_longer_appears_in_default_attention_preview():
    db = FakeDB()
    db.follow_up_rows = [_ashley_unwell_follow_up_row()]
    db.outcome_rows = [
        {
            "outcome_id": "out-1",
            "attention_item_id": "follow_up_candidates:11",
            "source_table": "follow_up_candidates",
            "source_id": "11",
            "outcome_type": "dismissed",
            "occurred_at": NOW - timedelta(minutes=5),
            "created_at": NOW - timedelta(minutes=5),
        }
    ]

    out = await build_attention_preview(db, tenant_id="default", user_id="u1", companion_id="sophie", as_of=NOW)

    assert out["items"] == []
    assert out["metadata"]["dismissedCount"] == 1
    assert out["metadata"]["outcomeSuppressedCount"] == 1


@pytest.mark.asyncio
async def test_dont_bring_up_again_suppresses_matching_source():
    db = FakeDB()
    db.follow_up_rows = [_ashley_unwell_follow_up_row()]
    db.outcome_rows = [
        {
            "outcome_id": "out-2",
            "attention_item_id": "different-item-id",
            "source_table": "follow_up_candidates",
            "source_id": "11",
            "outcome_type": "dont_bring_up_again",
            "occurred_at": NOW - timedelta(minutes=3),
            "created_at": NOW - timedelta(minutes=3),
            "suppress_until": None,
        }
    ]

    out = await build_attention_preview(db, tenant_id="default", user_id="u1", companion_id="sophie", as_of=NOW)

    assert out["items"] == []
    assert out["metadata"]["outcomeSuppressedCount"] == 1


@pytest.mark.asyncio
async def test_completed_item_is_hidden():
    db = FakeDB()
    db.actionable_rows = [_invoice_actionable_row()]
    db.outcome_rows = [
        {
            "outcome_id": "out-3",
            "attention_item_id": "actionable_candidates:21",
            "source_table": "actionable_candidates",
            "source_id": "21",
            "outcome_type": "completed",
            "occurred_at": NOW - timedelta(minutes=2),
            "created_at": NOW - timedelta(minutes=2),
        }
    ]

    out = await build_attention_preview(db, tenant_id="default", user_id="u1", companion_id="sophie", as_of=NOW)

    assert out["items"] == []
    assert out["metadata"]["completedCount"] == 1


@pytest.mark.asyncio
async def test_snoozed_item_hidden_before_snoozed_until_and_reappears_after():
    db = FakeDB()
    db.follow_up_rows = [_ashley_unwell_follow_up_row()]
    snoozed_until = NOW + timedelta(hours=2)
    db.outcome_rows = [
        {
            "outcome_id": "out-4",
            "attention_item_id": "follow_up_candidates:11",
            "source_table": "follow_up_candidates",
            "source_id": "11",
            "outcome_type": "snoozed",
            "occurred_at": NOW - timedelta(minutes=1),
            "created_at": NOW - timedelta(minutes=1),
            "snoozed_until": snoozed_until,
        }
    ]

    hidden = await build_attention_preview(db, tenant_id="default", user_id="u1", companion_id="sophie", as_of=NOW)
    visible = await build_attention_preview(
        db,
        tenant_id="default",
        user_id="u1",
        companion_id="sophie",
        as_of=snoozed_until + timedelta(minutes=1),
    )

    assert hidden["items"] == []
    assert hidden["metadata"]["snoozedCount"] == 1
    assert [item["title"] for item in visible["items"]] == ["Check on Ashley and the takedown"]


@pytest.mark.asyncio
async def test_ignored_twice_suppresses_matching_item():
    db = FakeDB()
    db.actionable_rows = [_invoice_actionable_row()]
    db.outcome_rows = [
        {
            "outcome_id": "out-5",
            "attention_item_id": "old-item-a",
            "source_table": "actionable_candidates",
            "source_id": "21",
            "outcome_type": "ignored",
            "occurred_at": NOW - timedelta(hours=2),
            "created_at": NOW - timedelta(hours=2),
        },
        {
            "outcome_id": "out-6",
            "attention_item_id": "old-item-b",
            "source_table": "actionable_candidates",
            "source_id": "21",
            "outcome_type": "ignored",
            "occurred_at": NOW - timedelta(hours=1),
            "created_at": NOW - timedelta(hours=1),
        },
    ]

    out = await build_attention_preview(db, tenant_id="default", user_id="u1", companion_id="sophie", as_of=NOW)

    assert out["items"] == []
    assert out["metadata"]["ignoredSuppressedCount"] == 1


@pytest.mark.asyncio
async def test_include_suppressed_true_shows_suppressed_items():
    db = FakeDB()
    db.follow_up_rows = [_ashley_unwell_follow_up_row()]
    db.outcome_rows = [
        {
            "outcome_id": "out-7",
            "attention_item_id": "follow_up_candidates:11",
            "source_table": "follow_up_candidates",
            "source_id": "11",
            "outcome_type": "dismissed",
            "occurred_at": NOW - timedelta(minutes=5),
            "created_at": NOW - timedelta(minutes=5),
        }
    ]

    out = await build_attention_preview(
        db,
        tenant_id="default",
        user_id="u1",
        companion_id="sophie",
        include_suppressed=True,
        as_of=NOW,
    )

    assert len(out["items"]) == 1
    assert out["items"][0]["status"] == "dismissed"


@pytest.mark.asyncio
async def test_include_expired_still_shows_stale_outcome_items():
    db = FakeDB()
    db.follow_up_rows = [_ashley_unwell_follow_up_row()]
    db.outcome_rows = [
        {
            "outcome_id": "out-8",
            "attention_item_id": "follow_up_candidates:11",
            "source_table": "follow_up_candidates",
            "source_id": "11",
            "outcome_type": "stale",
            "occurred_at": NOW - timedelta(minutes=5),
            "created_at": NOW - timedelta(minutes=5),
        }
    ]

    default_out = await build_attention_preview(db, tenant_id="default", user_id="u1", companion_id="sophie", as_of=NOW)
    include_out = await build_attention_preview(
        db,
        tenant_id="default",
        user_id="u1",
        companion_id="sophie",
        include_expired=True,
        as_of=NOW,
    )

    assert default_out["items"] == []
    assert len(include_out["items"]) == 1
    assert include_out["items"][0]["status"] == "expired"
