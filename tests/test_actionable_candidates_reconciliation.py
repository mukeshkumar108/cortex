from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

import pytest

from src.derived_pipeline import _upsert_actionable_candidate


class _FakeDb:
    def __init__(self):
        self.rows: List[Dict[str, Any]] = []
        self.action_items: List[Dict[str, Any]] = []
        self._next_id = 1

    async def fetch(self, query: str, *args):
        if "FROM action_items" in query:
            tenant_id, user_id, kind = args[0], args[1], args[2]
            return [
                dict(r)
                for r in self.action_items
                if r.get("tenant_id") == tenant_id
                and r.get("user_id") == user_id
                and r.get("kind") == kind
            ]
        if "FROM actionable_candidates" not in query:
            return []
        tenant_id, user_id, statuses = args[0], args[1], set(args[2] or [])
        out = [
            dict(r)
            for r in self.rows
            if r.get("tenant_id") == tenant_id
            and r.get("user_id") == user_id
            and str(r.get("status") or "") in statuses
        ]
        out.sort(key=lambda r: r.get("updated_at") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        return out

    async def execute(self, query: str, *args):
        if "INSERT INTO actionable_candidates" in query:
            return self._insert(*args)
        if "status = 'superseded'" in query:
            return self._supersede(*args)
        if "SET\n                  status = $4" in query and "candidate_id = $3" in query:
            return self._cancel(*args)
        if "SET\n                  session_id = $4" in query and "candidate_id = $3" in query:
            return self._merge(*args)
        return "OK"

    def _insert(self, *args):
        promoted_action_item_id = None
        if len(args) == 25:
            (
                tenant_id,
                user_id,
                session_id,
                run_id,
                candidate_key,
                record_type,
                title,
                summary,
                due_iso,
                candidate_subtype,
                relevant_from_iso,
                relevant_until_iso,
                waiting_on,
                needs_response,
                cadence_text,
                suggested_action,
                linked_external_id,
                linked_external_type,
                source,
                provenance,
                confidence_score,
                confidence_label,
                status,
                promoted_action_item_id,
                metadata,
            ) = args
        else:
            (
                tenant_id,
                user_id,
                session_id,
                run_id,
                candidate_key,
                record_type,
                title,
                summary,
                due_iso,
                candidate_subtype,
                relevant_from_iso,
                relevant_until_iso,
                waiting_on,
                needs_response,
                cadence_text,
                suggested_action,
                linked_external_id,
                linked_external_type,
                source,
                provenance,
                confidence_score,
                confidence_label,
                status,
                metadata,
            ) = args
        existing = next(
            (
                r
                for r in self.rows
                if r.get("tenant_id") == tenant_id
                and r.get("user_id") == user_id
                and r.get("candidate_key") == candidate_key
            ),
            None,
        )
        if existing:
            existing.update(
                {
                    "session_id": session_id,
                    "run_id": run_id,
                    "title": title,
                    "summary": summary,
                    "due_iso": due_iso or existing.get("due_iso"),
                    "candidate_subtype": candidate_subtype or existing.get("candidate_subtype"),
                    "relevant_from_iso": relevant_from_iso or existing.get("relevant_from_iso"),
                    "relevant_until_iso": relevant_until_iso or existing.get("relevant_until_iso"),
                    "waiting_on": waiting_on or existing.get("waiting_on"),
                    "needs_response": needs_response if isinstance(needs_response, bool) else existing.get("needs_response"),
                    "cadence_text": cadence_text or existing.get("cadence_text"),
                    "suggested_action": suggested_action or existing.get("suggested_action"),
                    "linked_external_id": linked_external_id or existing.get("linked_external_id"),
                    "linked_external_type": linked_external_type or existing.get("linked_external_type"),
                    "source": source,
                    "provenance": provenance,
                    "confidence_score": max(float(existing.get("confidence_score") or 0.0), float(confidence_score or 0.0)),
                    "confidence_label": confidence_label,
                    "status": existing.get("status")
                    if str(existing.get("status") or "") in {"confirmed", "dismissed", "acted_on", "superseded"}
                    else status,
                    "promoted_action_item_id": existing.get("promoted_action_item_id") or promoted_action_item_id,
                    "metadata": {**(existing.get("metadata") or {}), **(metadata or {})},
                    "updated_at": datetime.now(timezone.utc),
                }
            )
            return "UPDATE 1"
        row = {
            "candidate_id": self._next_id,
            "tenant_id": tenant_id,
            "user_id": user_id,
            "session_id": session_id,
            "run_id": run_id,
            "candidate_key": candidate_key,
            "record_type": record_type,
            "title": title,
            "summary": summary,
            "due_iso": due_iso,
            "candidate_subtype": candidate_subtype,
            "relevant_from_iso": relevant_from_iso,
            "relevant_until_iso": relevant_until_iso,
            "waiting_on": waiting_on,
            "needs_response": needs_response,
            "cadence_text": cadence_text,
            "suggested_action": suggested_action,
            "linked_external_id": linked_external_id,
            "linked_external_type": linked_external_type,
            "source": source,
            "provenance": provenance or {},
            "confidence_score": float(confidence_score or 0.0),
            "confidence_label": confidence_label,
            "status": status,
            "promoted_action_item_id": promoted_action_item_id,
            "metadata": metadata or {},
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        }
        self._next_id += 1
        self.rows.append(row)
        return "INSERT 1"

    def _supersede(self, *args):
        tenant_id, user_id, candidate_id, metadata = args
        for row in self.rows:
            if row.get("tenant_id") == tenant_id and row.get("user_id") == user_id and int(row.get("candidate_id")) == int(candidate_id):
                row["status"] = "superseded"
                row["metadata"] = {**(row.get("metadata") or {}), **(metadata or {})}
                row["updated_at"] = datetime.now(timezone.utc)
                return "UPDATE 1"
        return "UPDATE 0"

    def _cancel(self, *args):
        tenant_id, user_id, candidate_id, target_status, score, label, provenance, metadata = args
        for row in self.rows:
            if row.get("tenant_id") == tenant_id and row.get("user_id") == user_id and int(row.get("candidate_id")) == int(candidate_id):
                row["status"] = target_status
                row["confidence_score"] = max(float(row.get("confidence_score") or 0.0), float(score or 0.0))
                row["confidence_label"] = label
                row["provenance"] = provenance
                row["metadata"] = {**(row.get("metadata") or {}), **(metadata or {})}
                row["updated_at"] = datetime.now(timezone.utc)
                return "UPDATE 1"
        return "UPDATE 0"

    def _merge(self, *args):
        (
            tenant_id,
            user_id,
            candidate_id,
            session_id,
            run_id,
            title,
            summary,
            due_iso,
            candidate_subtype,
            relevant_from_iso,
            relevant_until_iso,
            waiting_on,
            needs_response,
            cadence_text,
            suggested_action,
            linked_external_id,
            linked_external_type,
            source,
            provenance,
            score,
            boost,
            next_status,
            metadata,
        ) = args
        for row in self.rows:
            if row.get("tenant_id") == tenant_id and row.get("user_id") == user_id and int(row.get("candidate_id")) == int(candidate_id):
                row.update(
                    {
                        "session_id": session_id,
                        "run_id": run_id,
                        "title": title or row.get("title"),
                        "summary": summary or row.get("summary"),
                        "due_iso": due_iso or row.get("due_iso"),
                        "candidate_subtype": candidate_subtype or row.get("candidate_subtype"),
                        "relevant_from_iso": relevant_from_iso or row.get("relevant_from_iso"),
                        "relevant_until_iso": relevant_until_iso or row.get("relevant_until_iso"),
                        "waiting_on": waiting_on or row.get("waiting_on"),
                        "needs_response": needs_response if isinstance(needs_response, bool) else row.get("needs_response"),
                        "cadence_text": cadence_text or row.get("cadence_text"),
                        "suggested_action": suggested_action or row.get("suggested_action"),
                        "linked_external_id": linked_external_id or row.get("linked_external_id"),
                        "linked_external_type": linked_external_type or row.get("linked_external_type"),
                        "source": source,
                        "provenance": provenance,
                        "confidence_score": min(1.0, max(float(row.get("confidence_score") or 0.0), float(score or 0.0)) + float(boost or 0.0)),
                        "status": row.get("status")
                        if str(row.get("status") or "") in {"dismissed", "acted_on", "superseded"}
                        else next_status,
                        "metadata": {**(row.get("metadata") or {}), **(metadata or {})},
                        "updated_at": datetime.now(timezone.utc),
                    }
                )
                return "UPDATE 1"
        return "UPDATE 0"


async def _persist_candidate(
    db: _FakeDb,
    *,
    source: str,
    record_type: str,
    title: str,
    summary: str,
    due_iso: Optional[str] = None,
    candidate_subtype: Optional[str] = None,
    waiting_on: Optional[str] = None,
    needs_response: Optional[bool] = None,
    cadence_text: Optional[str] = None,
    linked_external_id: Optional[str] = None,
    linked_external_type: Optional[str] = None,
    status: str = "detected",
):
    await _upsert_actionable_candidate(
        db=db,
        tenant_id="default",
        user_id="u1",
        session_id="s1",
        run_id=1,
        record_type=record_type,
        candidate_subtype=candidate_subtype,
        title=title,
        summary=summary,
        due_iso=due_iso,
        relevant_from_iso=due_iso,
        relevant_until_iso=due_iso,
        waiting_on=waiting_on,
        needs_response=needs_response,
        cadence_text=cadence_text,
        suggested_action="review_candidate",
        linked_external_id=linked_external_id,
        linked_external_type=linked_external_type,
        source=source,
        provenance={"message_hint": summary},
        confidence_label="medium",
        status=status,
        metadata={"test": True},
    )


def _add_action_item(
    db: _FakeDb,
    *,
    kind: str,
    title: str,
    due_at: Optional[datetime] = None,
    remind_at: Optional[datetime] = None,
    source_ref: Optional[Dict[str, Any]] = None,
):
    db.action_items.append(
        {
            "id": uuid4(),
            "tenant_id": "default",
            "user_id": "u1",
            "kind": kind,
            "title": title,
            "status": "pending",
            "due_at": due_at,
            "remind_at": remind_at,
            "source_type": "direct_user",
            "source_ref": source_ref,
            "confidence": 1.0,
            "provenance_summary": None,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
        }
    )


@pytest.mark.asyncio
async def test_live_confirmed_reminder_then_background_ingest_suppresses_duplicate_candidate():
    db = _FakeDb()
    remind_at = datetime(2026, 5, 2, 9, 0, tzinfo=timezone.utc)
    _add_action_item(
        db,
        kind="reminder",
        title="Call John",
        remind_at=remind_at,
        source_ref={"sessionId": "s1", "turnId": "turn-1"},
    )

    await _persist_candidate(
        db,
        source="chat",
        record_type="reminder_candidate",
        candidate_subtype="reminder",
        title="Call John tomorrow",
        summary="User asked Sophie to remind them to call John tomorrow.",
        due_iso="2026-05-02T09:00:00Z",
    )

    assert len(db.rows) == 1
    assert db.rows[0]["status"] == "superseded"
    assert db.rows[0]["promoted_action_item_id"] == str(db.action_items[0]["id"])
    assert db.rows[0]["metadata"]["reconciliation_action"] == "already_confirmed_action_item"


@pytest.mark.asyncio
async def test_whatsapp_candidate_clarified_by_email_merges():
    db = _FakeDb()
    due = "2026-05-01T15:00:00Z"
    await _persist_candidate(
        db,
        source="whatsapp",
        record_type="event_candidate",
        title="Dentist appointment",
        summary="Dentist appointment this Friday",
        due_iso=due,
    )
    await _persist_candidate(
        db,
        source="email",
        record_type="event_candidate",
        title="Dentist appointment",
        summary="Confirmed dentist appointment Friday at 3pm",
        due_iso=due,
    )
    assert len(db.rows) == 1
    row = db.rows[0]
    assert row["status"] in {"needs_review", "confirmed"}
    events = (row.get("provenance") or {}).get("events") or []
    assert len(events) >= 2
    assert events[0].get("source") in {"email", "calendar", "chat", "whatsapp"}


@pytest.mark.asyncio
async def test_duplicate_extracted_reminder_merges():
    db = _FakeDb()
    due = "2026-05-02T09:00:00Z"
    await _persist_candidate(
        db,
        source="chat",
        record_type="reminder_candidate",
        title="Call John tomorrow",
        summary="remind me to call John tomorrow",
        due_iso=due,
    )
    await _persist_candidate(
        db,
        source="chat",
        record_type="reminder_candidate",
        title="Call John tomorrow",
        summary="remind me to call John tomorrow",
        due_iso=due,
    )
    assert len(db.rows) == 1


@pytest.mark.asyncio
async def test_repeated_mentions_merge_evidence_and_confidence():
    db = _FakeDb()
    due = "2026-05-02T09:00:00Z"
    await _persist_candidate(
        db,
        source="chat",
        record_type="reminder_candidate",
        candidate_subtype="reminder",
        title="Call John tomorrow",
        summary="User mentioned calling John tomorrow.",
        due_iso=due,
    )
    await _persist_candidate(
        db,
        source="whatsapp",
        record_type="reminder_candidate",
        candidate_subtype="reminder",
        title="Call John tomorrow",
        summary="User again mentioned calling John tomorrow.",
        due_iso=due,
    )

    assert len(db.rows) == 1
    row = db.rows[0]
    assert row["confidence_score"] > 0.65
    events = (row.get("provenance") or {}).get("events") or []
    assert len(events) >= 2


@pytest.mark.asyncio
async def test_changed_event_time_supersedes_old_candidate():
    db = _FakeDb()
    await _persist_candidate(
        db,
        source="chat",
        record_type="event_candidate",
        title="Team sync",
        summary="we should meet Friday evening",
        due_iso="2026-05-01T15:00:00Z",
    )
    await _persist_candidate(
        db,
        source="email",
        record_type="event_candidate",
        title="Team sync",
        summary="updated to Saturday",
        due_iso="2026-05-03T15:00:00Z",
    )
    assert len(db.rows) == 2
    statuses = sorted(str(r.get("status")) for r in db.rows)
    assert "superseded" in statuses
    assert any(s in {"detected", "needs_review", "confirmed"} for s in statuses)


@pytest.mark.asyncio
async def test_cancellation_marks_candidate_stale_or_dismissed():
    db = _FakeDb()
    await _persist_candidate(
        db,
        source="chat",
        record_type="event_candidate",
        title="Dentist appointment",
        summary="I have a dentist appointment Tuesday",
        due_iso="2026-05-10T15:00:00Z",
    )
    await _persist_candidate(
        db,
        source="email",
        record_type="event_candidate",
        title="Dentist appointment cancelled",
        summary="Dentist appointment canceled",
        due_iso="2026-05-10T15:00:00Z",
    )
    assert len(db.rows) == 1
    assert db.rows[0]["status"] in {"stale", "dismissed"}


@pytest.mark.asyncio
async def test_unrelated_candidate_remains_separate():
    db = _FakeDb()
    await _persist_candidate(
        db,
        source="chat",
        record_type="task_candidate",
        title="Send deck",
        summary="I need to send that deck",
    )
    await _persist_candidate(
        db,
        source="chat",
        record_type="task_candidate",
        title="Book flights",
        summary="I need to book flights",
    )
    assert len(db.rows) == 2


@pytest.mark.asyncio
async def test_calendar_link_can_confirm_existing_candidate():
    db = _FakeDb()
    await _persist_candidate(
        db,
        source="chat",
        record_type="event_candidate",
        title="Board meeting",
        summary="we should meet Thursday at 2",
        due_iso="2026-05-07T14:00:00Z",
    )
    await _persist_candidate(
        db,
        source="calendar",
        record_type="event_candidate",
        title="Board meeting",
        summary="Calendar event present",
        due_iso="2026-05-07T14:00:00Z",
        linked_external_id="cal-evt-1",
        linked_external_type="calendar",
    )
    assert len(db.rows) == 1
    row = db.rows[0]
    assert row.get("linked_external_id") == "cal-evt-1"
    assert row.get("linked_external_type") == "calendar"
    assert row.get("status") == "confirmed"
