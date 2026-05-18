from datetime import datetime, timezone

import pytest
from httpx import AsyncClient, ASGITransport

from src.main import app


def _base_row(
    *,
    candidate_id: int,
    record_type: str,
    status: str,
    due_iso=None,
    relevant_from_iso=None,
    relevant_until_iso=None,
    title=None,
    summary=None,
    confidence_score=0.72,
    confidence_label="medium",
    metadata=None,
    candidate_subtype=None,
    waiting_on=None,
    needs_response=None,
    cadence_text=None,
):
    return {
        "candidate_id": candidate_id,
        "session_id": "s1",
        "record_type": record_type,
        "candidate_subtype": candidate_subtype,
        "title": title or f"title-{candidate_id}",
        "summary": summary or f"summary-{candidate_id}",
        "due_iso": due_iso,
        "relevant_from_iso": relevant_from_iso,
        "relevant_until_iso": relevant_until_iso,
        "waiting_on": waiting_on,
        "needs_response": needs_response,
        "cadence_text": cadence_text,
        "suggested_action": "review_candidate",
        "linked_external_id": None,
        "linked_external_type": None,
        "source": "chat",
        "provenance": {"message_hint": "hint"},
        "confidence_score": confidence_score,
        "confidence_label": confidence_label,
        "status": status,
        "metadata": metadata or {},
        "created_at": datetime(2026, 4, 27, 9, 0, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 4, 27, 9, 0, tzinfo=timezone.utc),
    }


@pytest.mark.asyncio
async def test_future_needs_review_candidate_appears_in_review_queue(monkeypatch):
    rows = [
        _base_row(
            candidate_id=101,
            record_type="event_candidate",
            status="needs_review",
            due_iso=datetime(2026, 5, 10, 15, 0, tzinfo=timezone.utc),
        )
    ]

    async def _stub_fetch(*_args, **_kwargs):
        return rows

    monkeypatch.setattr("src.main.db.fetch", _stub_fetch, raising=False)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get(
            "/review-queue",
            params={"tenantId": "t", "userId": "u", "now": "2026-04-27T10:00:00Z"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["items"]) == 1
        assert data["items"][0]["id"] == 101
        assert data["items"][0]["status"] == "needs_review"
        assert data["items"][0]["kind"] == "event_candidate"


@pytest.mark.asyncio
async def test_future_candidate_excluded_from_today_daily_unless_relevant_today(monkeypatch):
    rows = [
        _base_row(
            candidate_id=201,
            record_type="event_candidate",
            status="needs_review",
            due_iso=datetime(2026, 5, 12, 11, 0, tzinfo=timezone.utc),
            relevant_from_iso=None,
            relevant_until_iso=None,
        ),
        _base_row(
            candidate_id=202,
            record_type="event_candidate",
            status="needs_review",
            due_iso=datetime(2026, 5, 12, 11, 0, tzinfo=timezone.utc),
            relevant_from_iso=datetime(2026, 4, 27, 0, 0, tzinfo=timezone.utc),
            relevant_until_iso=datetime(2026, 4, 27, 23, 59, tzinfo=timezone.utc),
        ),
    ]

    async def _stub_fetch(*_args, **_kwargs):
        return rows

    monkeypatch.setattr("src.main.db.fetch", _stub_fetch, raising=False)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get(
            "/daily-candidates",
            params={"tenantId": "t", "userId": "u", "date": "2026-04-27"},
        )
        assert resp.status_code == 200
        data = resp.json()
        ids = [item["id"] for item in data["needsReview"]]
        assert 201 not in ids
        assert 202 in ids


@pytest.mark.asyncio
async def test_review_queue_groups_needs_review_items_by_type(monkeypatch):
    rows = [
        _base_row(candidate_id=301, record_type="event_candidate", status="needs_review"),
        _base_row(candidate_id=302, record_type="task_candidate", status="needs_review"),
        _base_row(candidate_id=303, record_type="reminder_candidate", status="needs_review"),
        _base_row(candidate_id=304, record_type="commitment", status="needs_review"),
    ]

    async def _stub_fetch(*_args, **_kwargs):
        return rows

    monkeypatch.setattr("src.main.db.fetch", _stub_fetch, raising=False)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get(
            "/review-queue",
            params={"tenantId": "t", "userId": "u"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["items"]) == 4
        assert all("kind" in item for item in data["items"])
        assert len(data["byType"]["event_candidate"]) == 1
        assert len(data["byType"]["task_candidate"]) == 1
        assert len(data["byType"]["reminder_candidate"]) == 1
        assert len(data["byType"]["commitment"]) == 1


@pytest.mark.asyncio
async def test_review_queue_exposes_candidate_subtypes_and_support_fields(monkeypatch):
    rows = [
        _base_row(
            candidate_id=305,
            record_type="task_candidate",
            candidate_subtype="waiting_on",
            status="needs_review",
            title="Waiting on invoice from supplier",
            waiting_on="supplier",
            needs_response=True,
        ),
        _base_row(
            candidate_id=306,
            record_type="task_candidate",
            candidate_subtype="habit",
            status="needs_review",
            title="Daily hydration",
            cadence_text="daily",
        ),
    ]

    async def _stub_fetch(*_args, **_kwargs):
        return rows

    monkeypatch.setattr("src.main.db.fetch", _stub_fetch, raising=False)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/review-queue", params={"tenantId": "t", "userId": "u"})
        assert resp.status_code == 200
        data = resp.json()
        waiting = next(item for item in data["items"] if item["id"] == 305)
        habit = next(item for item in data["items"] if item["id"] == 306)
        assert waiting["candidateSubtype"] == "waiting_on"
        assert waiting["waitingOn"] == "supplier"
        assert waiting["needsResponse"] is True
        assert habit["candidateSubtype"] == "habit"
        assert habit["cadenceText"] == "daily"


@pytest.mark.asyncio
async def test_daily_candidates_excludes_noisy_undated_detected_items(monkeypatch):
    rows = [
        _base_row(
            candidate_id=401,
            record_type="event_candidate",
            status="detected",
            title="I'm about to cook myself dinner.",
            summary="I'm about to cook myself dinner.",
            due_iso=None,
            relevant_from_iso=None,
            relevant_until_iso=None,
            confidence_score=0.95,
            confidence_label="high",
        ),
        _base_row(
            candidate_id=402,
            record_type="task_candidate",
            status="detected",
            title="Send the deck",
            summary="Need to send the deck.",
            due_iso=None,
            relevant_from_iso=None,
            relevant_until_iso=None,
            confidence_score=0.4,
            confidence_label="low",
        ),
        _base_row(
            candidate_id=403,
            record_type="task_candidate",
            status="detected",
            title="Daily hydration reminder",
            summary="Keep drinking water today.",
            due_iso=None,
            relevant_from_iso=None,
            relevant_until_iso=None,
            confidence_score=0.92,
            confidence_label="high",
            metadata={"daily_relevant": True},
        ),
        _base_row(
            candidate_id=404,
            record_type="event_candidate",
            status="confirmed",
            title="Dentist appointment",
            summary="Dentist appointment today.",
            due_iso=datetime(2026, 4, 27, 15, 0, tzinfo=timezone.utc),
        ),
    ]

    async def _stub_fetch(*_args, **_kwargs):
        return rows

    monkeypatch.setattr("src.main.db.fetch", _stub_fetch, raising=False)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get(
            "/daily-candidates",
            params={"tenantId": "t", "userId": "u", "date": "2026-04-27"},
        )
        assert resp.status_code == 200
        data = resp.json()
        ids = [item["id"] for item in data["needsReview"]]
        assert 401 not in ids
        assert 402 not in ids
        assert 403 in ids
        assert 404 not in ids


@pytest.mark.asyncio
async def test_daily_candidates_defaults_to_small_useful_limit(monkeypatch):
    rows = [
        _base_row(
            candidate_id=500 + i,
            record_type="task_candidate",
            status="needs_review",
            due_iso=datetime(2026, 4, 27, 9, 0, tzinfo=timezone.utc),
        )
        for i in range(20)
    ]

    async def _stub_fetch(*_args, **_kwargs):
        return rows

    monkeypatch.setattr("src.main.db.fetch", _stub_fetch, raising=False)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get(
            "/daily-candidates",
            params={"tenantId": "t", "userId": "u", "date": "2026-04-27"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["needsReview"]) == 12
        assert data["metadata"]["defaultLimit"] == 12
