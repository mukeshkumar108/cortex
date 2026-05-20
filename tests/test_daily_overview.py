from __future__ import annotations

from datetime import datetime, timedelta, timezone
import re
from typing import Any, Dict, List

import pytest
from httpx import ASGITransport, AsyncClient

from src import main as main_module
from src.daily_overview import build_daily_overview


NOW = datetime(2026, 5, 19, 9, 0, tzinfo=timezone.utc)


class FakeDB:
    def __init__(self):
        self.identity_cache_row: Dict[str, Any] | None = None
        self.living_context_row: Dict[str, Any] | None = None
        self.actionable_rows: List[Dict[str, Any]] = []
        self.follow_up_rows: List[Dict[str, Any]] = []
        self.clarification_rows: List[Dict[str, Any]] = []
        self.thread_rows: List[Dict[str, Any]] = []
        self.action_item_rows: List[Dict[str, Any]] = []
        self.calendar_rows: List[Dict[str, Any]] = []
        self.handover_rows: List[Dict[str, Any]] = []
        self.outcome_rows: List[Dict[str, Any]] = []
        self.session_change_rows: List[Dict[str, Any]] = []
        self.action_update_rows: List[Dict[str, Any]] = []
        self.relationship_rows: List[Dict[str, Any]] = []

    async def fetchone(self, query: str, *args):
        if "FROM identity_cache" in query:
            return self.identity_cache_row
        if "FROM living_context" in query:
            return self.living_context_row
        if "FROM session_handover_packets" in query:
            include_expired = "expires_at >" not in query
            now = args[-1] if not include_expired else NOW
            rows = [row for row in self.handover_rows if row.get("tenant_id") == args[0] and row.get("user_id") == args[1]]
            if not include_expired:
                rows = [row for row in rows if row.get("expires_at") and row["expires_at"] > now]
            rows.sort(key=lambda row: (row.get("created_at") or NOW, row.get("updated_at") or NOW), reverse=True)
            return rows[0] if rows else None
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
        if "FROM attention_outcomes" in query:
            return self.outcome_rows
        if "FROM session_changes" in query:
            return self.session_change_rows
        if "FROM action_updates" in query:
            return self.action_update_rows
        if "FROM memory_relationship_links" in query:
            return self.relationship_rows
        if "FROM session_handover_packets" in query:
            return self.handover_rows
        if "FROM calendar_items" in query:
            if "COALESCE(ends_at, starts_at) >=" in query:
                start = args[2]
                end = args[3]
                return [
                    row
                    for row in self.calendar_rows
                    if row.get("tenant_id") == args[0]
                    and row.get("user_id") == args[1]
                    and (row.get("ends_at") or row.get("starts_at")) >= start
                    and row.get("starts_at") < end
                ]
            return self.calendar_rows
        if "FROM action_items" in query:
            return [row for row in self.action_item_rows if row.get("tenant_id") == args[0] and row.get("user_id") == args[1]]
        return []


class TypeCheckedDailyOverviewDB(FakeDB):
    async def fetchone(self, query: str, *args):
        if "FROM identity_cache" in query:
            assert "tenant_id=$1::text" in query
            assert "user_id=$2::text" in query
        return await super().fetchone(query, *args)

    async def fetch(self, query: str, *args):
        if "FROM calendar_items" in query and "COALESCE(ends_at, starts_at)" in query:
            assert "tenant_id=$1::text" in query
            assert "user_id=$2::text" in query
            assert ">= $3::timestamptz" in query
            assert "< $4::timestamptz" in query
        if "FROM action_items" in query and "completed_at >= $4::timestamptz" in query:
            assert "tenant_id=$1::text" in query
            assert "user_id=$2::text" in query
            assert "due_at < $3::timestamptz" in query
            assert "remind_at < $3::timestamptz" in query
            assert "completed_at >= $4::timestamptz" in query
            assert "updated_at >= $4::timestamptz" in query
        return await super().fetch(query, *args)


class QueryAuditDailyOverviewDB(FakeDB):
    def __init__(self):
        super().__init__()
        self.queries: List[str] = []

    async def fetchone(self, query: str, *args):
        self.queries.append(query)
        return await super().fetchone(query, *args)

    async def fetch(self, query: str, *args):
        self.queries.append(query)
        return await super().fetch(query, *args)


def _calendar_row(
    title: str,
    starts_at: datetime,
    *,
    status: str = "confirmed",
    ends_at: datetime | None = None,
    cancelled_at: datetime | None = None,
    archived_at: datetime | None = None,
) -> Dict[str, Any]:
    return {
        "id": f"cal-{title.lower().replace(' ', '-')}",
        "tenant_id": "default",
        "user_id": "u1",
        "title": title,
        "starts_at": starts_at,
        "ends_at": ends_at or (starts_at + timedelta(hours=1)),
        "status": status,
        "confidence": 0.8,
        "source_ref": {"source_object_ids": [f"event:{title.lower()}"], "source_link_ids": [f"link:{title.lower()}"]},
        "created_at": NOW - timedelta(days=1),
        "updated_at": NOW - timedelta(hours=1),
        "cancelled_at": cancelled_at,
        "archived_at": archived_at,
        "description": None,
        "notes": None,
        "evidence_refs": [{"session_id": "s-cal", "turn_id": "t-cal"}],
        "provenance_summary": None,
    }


def _task_row(
    title: str,
    *,
    status: str = "pending",
    due_at: datetime | None = None,
    completed_at: datetime | None = None,
    updated_at: datetime | None = None,
) -> Dict[str, Any]:
    return {
        "id": f"task-{title.lower().replace(' ', '-')}",
        "tenant_id": "default",
        "user_id": "u1",
        "kind": "todo",
        "title": title,
        "notes": None,
        "status": status,
        "due_at": due_at,
        "remind_at": None,
        "confidence": 0.9,
        "source_ref": {"source_object_ids": [f"obligation:{title.lower()}"], "source_link_ids": ["link-task"]},
        "created_at": NOW - timedelta(days=1),
        "updated_at": updated_at or NOW - timedelta(hours=1),
        "completed_at": completed_at,
        "dismissed_at": None,
        "cancelled_at": None,
        "provenance_summary": None,
    }


def _follow_up_attention_row(title: str = "Check on Ashley and the takedown") -> Dict[str, Any]:
    return {
        "candidate_id": 11,
        "tenant_id": "default",
        "user_id": "u1",
        "candidate_key": "ashley-takedown",
        "title": title,
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


def _handover_row(*, expires_at: datetime) -> Dict[str, Any]:
    return {
        "packet_id": "packet-1",
        "tenant_id": "default",
        "user_id": "u1",
        "session_id": "s-h1",
        "summary": "Recent session focused on invoice follow-up.",
        "open_questions": ["What should I send?"],
        "unresolved_decisions": ["Whether to send the invoice chase today."],
        "pending_actions": ["draft the invoice chase"],
        "recent_state_note": "I was tired after the call.",
        "important_people": ["Acme"],
        "active_topics": ["invoice #102"],
        "do_not_overdo": ["Fast handover only; do not treat this as durable profile or identity."],
        "source_turn_refs": [{"session_id": "s-h1", "turn_index": 0}],
        "created_at": NOW - timedelta(hours=2),
        "updated_at": NOW - timedelta(hours=1),
        "expires_at": expires_at,
        "status": "active" if expires_at > NOW else "expired",
    }


def _outcome_row(*, source_table: str, source_id: str, outcome_type: str) -> Dict[str, Any]:
    return {
        "outcome_id": f"out-{source_table}-{source_id}-{outcome_type}",
        "tenant_id": "default",
        "user_id": "u1",
        "companion_id": "sophie",
        "attention_item_id": f"{source_table}:{source_id}",
        "source_table": source_table,
        "source_id": source_id,
        "outcome_type": outcome_type,
        "outcome_reason": "Handled already.",
        "surface_mode": "proactive_allowed",
        "action_policy": "suggest_only",
        "occurred_at": NOW - timedelta(hours=1),
        "snoozed_until": None,
        "suppress_until": None,
        "metadata": {"source_object_ids": ["person:ashley"]},
        "created_at": NOW - timedelta(hours=1),
    }


def _session_change_row(*, title: str, effective_iso: datetime, updated_at: datetime, status: str = "confirmed") -> Dict[str, Any]:
    return {
        "change_id": 901,
        "tenant_id": "default",
        "user_id": "u1",
        "kind": "state_change",
        "title": title,
        "summary": title,
        "effective_iso": effective_iso,
        "provenance": {"source_turn_refs": [{"session_id": "s-state", "turn_id": "t1"}]},
        "confidence_score": 0.7,
        "status": status,
        "created_at": updated_at - timedelta(hours=1),
        "updated_at": updated_at,
    }


@pytest.mark.asyncio
async def test_endpoint_requires_internal_auth(monkeypatch):
    called: Dict[str, Any] = {}

    def _stub_require_internal_token(token: str | None) -> None:
        called["token"] = token
        raise main_module.HTTPException(status_code=401, detail="Unauthorized")

    monkeypatch.setattr(main_module, "_require_internal_token", _stub_require_internal_token, raising=True)
    async with AsyncClient(transport=ASGITransport(app=main_module.app), base_url="http://test") as client:
        response = await client.get("/internal/debug/daily-overview", params={"tenantId": "default", "userId": "u1"})
    assert response.status_code == 401
    assert called["token"] is None


@pytest.mark.asyncio
async def test_returns_empty_safe_overview_when_no_data_exists():
    db = FakeDB()
    out = await build_daily_overview(db, tenant_id="default", user_id="u1", date_value="2026-05-19", timezone_name="UTC", as_of=NOW)

    assert out["todaysSchedule"] == []
    assert out["tasksAndObligations"] == []
    assert out["worthAttention"] == []
    assert out["recentContinuity"] is None
    assert out["suggestedFocus"]["reason"] == "No active items found; avoid inventing a brief."
    assert out["readOnly"] is True


@pytest.mark.asyncio
async def test_includes_todays_calendar_event():
    db = FakeDB()
    db.calendar_rows = [_calendar_row("Morning meeting", NOW + timedelta(hours=2))]

    out = await build_daily_overview(db, tenant_id="default", user_id="u1", date_value="2026-05-19", timezone_name="UTC", as_of=NOW)

    assert [item["title"] for item in out["todaysSchedule"]] == ["Morning meeting"]


@pytest.mark.asyncio
async def test_excludes_cancelled_expired_calendar_event_by_default():
    db = FakeDB()
    db.calendar_rows = [
        _calendar_row("Cancelled event", NOW + timedelta(hours=2), status="cancelled", cancelled_at=NOW - timedelta(minutes=30)),
    ]

    out = await build_daily_overview(db, tenant_id="default", user_id="u1", date_value="2026-05-19", timezone_name="UTC", as_of=NOW)

    assert out["todaysSchedule"] == []


@pytest.mark.asyncio
async def test_includes_due_today_action_item():
    db = FakeDB()
    db.action_item_rows = [_task_row("Submit report", due_at=NOW + timedelta(hours=5))]

    out = await build_daily_overview(db, tenant_id="default", user_id="u1", date_value="2026-05-19", timezone_name="UTC", as_of=NOW)

    assert [item["title"] for item in out["tasksAndObligations"]] == ["Submit report"]
    assert out["tasksAndObligations"][0]["bucket"] == "due_today"


@pytest.mark.asyncio
async def test_excludes_archived_done_action_item_by_default():
    db = FakeDB()
    db.action_item_rows = [
        _task_row("Completed report", status="done", completed_at=NOW - timedelta(days=3), updated_at=NOW - timedelta(days=3)),
        _task_row("Archived thing", status="archived", due_at=NOW),
    ]

    out = await build_daily_overview(db, tenant_id="default", user_id="u1", date_value="2026-05-19", timezone_name="UTC", as_of=NOW)

    assert out["tasksAndObligations"] == []


@pytest.mark.asyncio
async def test_flags_or_excludes_smoke_demo_test_items_by_default():
    db = FakeDB()
    db.action_item_rows = [_task_row("demo smoke task", due_at=NOW + timedelta(hours=1))]

    out = await build_daily_overview(db, tenant_id="default", user_id="u1", date_value="2026-05-19", timezone_name="UTC", as_of=NOW)

    assert out["tasksAndObligations"] == []
    assert out["metadata"]["diagnosticNoiseCount"] == 1


@pytest.mark.asyncio
async def test_includes_eligible_attention_item_and_respects_companion_profile():
    db = FakeDB()
    db.follow_up_rows = [_follow_up_attention_row()]

    sophie = await build_daily_overview(
        db,
        tenant_id="default",
        user_id="u1",
        companion_id="sophie",
        date_value="2026-05-19",
        timezone_name="UTC",
        as_of=NOW,
    )
    admin = await build_daily_overview(
        db,
        tenant_id="default",
        user_id="u1",
        companion_id="admin_only",
        date_value="2026-05-19",
        timezone_name="UTC",
        as_of=NOW,
    )

    assert [item["title"] for item in sophie["worthAttention"]] == ["Check on Ashley and the takedown"]
    assert admin["worthAttention"] == []


@pytest.mark.asyncio
async def test_suppressed_dismissed_attention_item_does_not_appear():
    db = FakeDB()
    db.follow_up_rows = [_follow_up_attention_row()]
    db.outcome_rows = [_outcome_row(source_table="follow_up_candidates", source_id="11", outcome_type="dismissed")]

    out = await build_daily_overview(db, tenant_id="default", user_id="u1", date_value="2026-05-19", timezone_name="UTC", as_of=NOW)

    assert out["worthAttention"] == []


@pytest.mark.asyncio
async def test_includes_active_handover_packet():
    db = FakeDB()
    db.handover_rows = [_handover_row(expires_at=NOW + timedelta(hours=6))]

    out = await build_daily_overview(db, tenant_id="default", user_id="u1", date_value="2026-05-19", timezone_name="UTC", as_of=NOW)

    assert out["recentContinuity"] is not None
    assert out["recentContinuity"]["summary"] == "Recent session focused on invoice follow-up."


@pytest.mark.asyncio
async def test_does_not_include_expired_handover_packet():
    db = FakeDB()
    db.handover_rows = [_handover_row(expires_at=NOW - timedelta(hours=1))]

    out = await build_daily_overview(db, tenant_id="default", user_id="u1", date_value="2026-05-19", timezone_name="UTC", as_of=NOW)

    assert out["recentContinuity"] is None


@pytest.mark.asyncio
async def test_includes_recent_timeline_feedback_signal():
    db = FakeDB()
    db.outcome_rows = [_outcome_row(source_table="follow_up_candidates", source_id="11", outcome_type="acknowledged")]

    out = await build_daily_overview(db, tenant_id="default", user_id="u1", date_value="2026-05-19", timezone_name="UTC", as_of=NOW)

    assert any(item["source_table"] == "attention_outcomes" for item in out["recentTimelineSignals"]["items"])


@pytest.mark.asyncio
async def test_generated_suggested_focus_does_not_claim_stale_emotional_state_is_current():
    db = FakeDB()
    db.calendar_rows = [_calendar_row("Lunch", NOW + timedelta(hours=3))]
    db.session_change_rows = [
        _session_change_row(
            title="User felt overwhelmed after yesterday's call.",
            effective_iso=NOW - timedelta(days=9),
            updated_at=NOW - timedelta(days=8),
        )
    ]

    out = await build_daily_overview(db, tenant_id="default", user_id="u1", date_value="2026-05-19", timezone_name="UTC", as_of=NOW)

    assert out["suggestedFocus"]["avoid_overdoing"] == "do_not_claim_stale_emotions_are_current"
    assert "stale" in out["suggestedFocus"]["reason"].lower()


@pytest.mark.asyncio
async def test_include_expired_true_shows_expired_diagnostic_items():
    db = FakeDB()
    db.calendar_rows = [
        _calendar_row("Cancelled event", NOW + timedelta(hours=2), status="cancelled", cancelled_at=NOW - timedelta(hours=1)),
    ]
    db.action_item_rows = [
        _task_row("smoke demo task", due_at=NOW - timedelta(days=4), updated_at=NOW - timedelta(days=4)),
    ]

    out = await build_daily_overview(
        db,
        tenant_id="default",
        user_id="u1",
        date_value="2026-05-19",
        timezone_name="UTC",
        include_expired=True,
        as_of=NOW,
    )

    assert {item["title"] for item in out["todaysSchedule"]} == {"Cancelled event"}
    assert {item["title"] for item in out["tasksAndObligations"]} == {"smoke demo task"}


@pytest.mark.asyncio
async def test_metadata_counts_are_correct():
    db = FakeDB()
    db.calendar_rows = [_calendar_row("Morning meeting", NOW + timedelta(hours=2))]
    db.action_item_rows = [_task_row("Submit report", due_at=NOW + timedelta(hours=5))]
    db.follow_up_rows = [_follow_up_attention_row()]
    db.handover_rows = [_handover_row(expires_at=NOW + timedelta(hours=6))]
    db.outcome_rows = [_outcome_row(source_table="follow_up_candidates", source_id="11", outcome_type="acknowledged")]

    out = await build_daily_overview(db, tenant_id="default", user_id="u1", date_value="2026-05-19", timezone_name="UTC", as_of=NOW)
    counts = out["metadata"]["counts"]

    assert counts["schedule"] == 1
    assert counts["tasks"] == 1
    assert counts["worthAttention"] == 3
    assert counts["handover"] == 1
    assert out["metadata"]["readOnly"] is True
    assert "calendar_items" in out["metadata"]["sourceTablesUsed"]
    assert "action_items" in out["metadata"]["sourceTablesUsed"]


@pytest.mark.asyncio
async def test_internal_endpoint_returns_overview_payload(monkeypatch):
    db = FakeDB()
    db.calendar_rows = [_calendar_row("Morning meeting", NOW + timedelta(hours=2))]
    monkeypatch.setattr(main_module, "db", db, raising=True)

    def _ok_require_internal_token(token: str | None) -> None:
        assert token == "test-token"

    monkeypatch.setattr(main_module, "_require_internal_token", _ok_require_internal_token, raising=True)
    async with AsyncClient(transport=ASGITransport(app=main_module.app), base_url="http://test") as client:
        response = await client.get(
            "/internal/debug/daily-overview",
            params={"tenantId": "default", "userId": "u1", "date": "2026-05-19", "timezone": "UTC"},
            headers={"X-Internal-Token": "test-token"},
        )
    assert response.status_code == 200
    assert response.json()["readOnly"] is True


@pytest.mark.asyncio
async def test_live_request_shape_without_optional_params_uses_typed_sql_and_does_not_fail():
    db = TypeCheckedDailyOverviewDB()
    out = await build_daily_overview(
        db,
        tenant_id="default",
        user_id="cmoinj5wf0000y6xpn1chve02",
        companion_id="sophie",
        as_of=NOW,
    )

    assert out["tenantId"] == "default"
    assert out["userId"] == "cmoinj5wf0000y6xpn1chve02"
    assert out["companionId"] == "sophie"
    assert out["readOnly"] is True


@pytest.mark.asyncio
async def test_daily_overview_queries_use_contiguous_and_explicitly_typed_params():
    db = QueryAuditDailyOverviewDB()
    db.follow_up_rows = [_follow_up_attention_row()]

    await build_daily_overview(
        db,
        tenant_id="default",
        user_id="cmoinj5wf0000y6xpn1chve02",
        companion_id="sophie",
        as_of=NOW,
    )

    assert db.queries
    for query in db.queries:
        numbers = [int(match) for match in re.findall(r"\$([0-9]+)", query)]
        if not numbers:
            continue
        assert sorted(set(numbers)) == list(range(1, max(numbers) + 1)), query
        uncast = re.findall(r"\$([0-9]+)(?!::)", query)
        assert not uncast, query
