from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pytest
from fastapi import HTTPException

from src.invariants import InvariantManager, InvariantViolationCandidate
from src.main import list_v2_invariant_repairs, list_v2_invariant_violations


class FakeDB:
    def __init__(self):
        self.fetch_calls: List[str] = []
        self.execute_calls: List[tuple[str, tuple[Any, ...]]] = []
        self.fetchone_calls: List[str] = []
        self.fetchval_calls: List[str] = []
        self._next_violation_id = 1

    async def fetch(self, query: str, *args):
        self.fetch_calls.append(query)
        if "LEFT JOIN claim_evidence" in query and "HAVING COUNT(ce.claim_evidence_id) = 0" in query:
            return [
                {
                    "tenant_id": "default",
                    "user_id": "u1",
                    "claim_id": 10,
                    "claim_event_key": "cek1",
                    "predicate": "relationship.status",
                    "lifecycle_status": "active",
                }
            ]
        if "FROM claims" in query and "metadata->>'source_role'" in query:
            return [
                {
                    "tenant_id": "default",
                    "user_id": "u1",
                    "claim_id": 11,
                    "claim_event_key": "cek2",
                    "metadata": {"source_role": "assistant"},
                }
            ]
        if "GROUP BY status" in query:
            return [{"status": "open", "n": 2}, {"status": "review_required", "n": 1}]
        if "FROM invariant_violations" in query:
            return []
        if "FROM invariant_repair_actions" in query:
            return []
        return []

    async def fetchone(self, query: str, *args):
        self.fetchone_calls.append(query)
        if "FROM invariant_violations" in query:
            return None
        return None

    async def fetchval(self, query: str, *args):
        self.fetchval_calls.append(query)
        if "INSERT INTO invariant_violations" in query:
            value = self._next_violation_id
            self._next_violation_id += 1
            return value
        return None

    async def execute(self, query: str, *args):
        self.execute_calls.append((query, args))
        return "OK"


@pytest.mark.asyncio
async def test_t13_invariant_detection_representative_cases():
    db = FakeDB()
    manager = InvariantManager(db)

    factual = await manager._detect_factual_without_evidence(limit=10)
    assistant = await manager._detect_assistant_authored_canonical_claims(limit=10)

    assert len(factual) == 1
    assert factual[0].invariant_code == "factual_claim_missing_evidence"
    assert factual[0].requires_human_review is True
    assert len(assistant) == 1
    assert assistant[0].invariant_code == "assistant_authored_claim_without_policy"
    assert assistant[0].requires_human_review is True


@pytest.mark.asyncio
async def test_t13_auto_repair_only_runs_for_safe_classes(monkeypatch):
    db = FakeDB()
    manager = InvariantManager(db)

    async def _stub_detect_all(*, max_rows_per_invariant: int = 200):
        return [
            InvariantViolationCandidate(
                invariant_code="canonical_mutation_watermark_inconsistent",
                severity="medium",
                tenant_id="default",
                user_id=None,
                object_type="canonical_tenant_watermark",
                object_id="default",
                details={"expected_last_sequence": 7, "observed_last_sequence": 3},
                requires_human_review=False,
            ),
            InvariantViolationCandidate(
                invariant_code="factual_claim_missing_evidence",
                severity="high",
                tenant_id="default",
                user_id="u1",
                object_type="claim",
                object_id="11",
                details={},
                requires_human_review=True,
            ),
        ]

    monkeypatch.setattr(manager, "detect_all", _stub_detect_all)

    summary = await manager.run_cycle(auto_repair_enabled=True)

    assert summary.detected == 2
    assert summary.auto_repaired == 1
    assert summary.review_required == 1
    assert any("INSERT INTO canonical_tenant_watermarks" in q for q, _args in db.execute_calls)


@pytest.mark.asyncio
async def test_t13_risky_semantic_repairs_are_marked_review_and_not_auto_mutated(monkeypatch):
    db = FakeDB()
    manager = InvariantManager(db)

    async def _stub_detect_all(*, max_rows_per_invariant: int = 200):
        return [
            InvariantViolationCandidate(
                invariant_code="duplicate_active_exclusive_slot_claims",
                severity="high",
                tenant_id="default",
                user_id="u1",
                object_type="claim_slot",
                object_id="slot_abc",
                details={"active_claim_count": 2},
                requires_human_review=True,
            )
        ]

    monkeypatch.setattr(manager, "detect_all", _stub_detect_all)

    summary = await manager.run_cycle(auto_repair_enabled=True)

    assert summary.detected == 1
    assert summary.auto_repaired == 0
    assert summary.review_required == 1
    assert not any("INSERT INTO canonical_tenant_watermarks" in q for q, _args in db.execute_calls)


@pytest.mark.asyncio
async def test_t13_internal_violation_and_repair_endpoints_fail_safe_filters(monkeypatch):
    db = FakeDB()
    monkeypatch.setattr("src.main.db", db, raising=False)
    monkeypatch.setattr("src.main._require_internal_token", lambda _token: None, raising=True)

    with pytest.raises(HTTPException):
        await list_v2_invariant_violations(status="bad-status", x_internal_token="x")

    with pytest.raises(HTTPException):
        await list_v2_invariant_repairs(status="bad-status", x_internal_token="x")

    out = await list_v2_invariant_violations(status="open", limit=50, x_internal_token="x")
    assert out["status_filter"] == "open"
    assert isinstance(out["status_counts"], dict)
