from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import pytest

from src.rollout import (
    MODE_COHORT_V2,
    MODE_LEGACY_ONLY,
    MODE_V2_ALL,
    ROUTE_LEGACY,
    ROUTE_SHADOW,
    ROUTE_V2,
    RolloutController,
)
from src.main import memory_query_v2
from src.models import MemoryQueryV2Request
from fastapi import HTTPException


class FakeDB:
    def __init__(self):
        self.state = {
            "mode": MODE_COHORT_V2,
            "cohort_tenants": ["tenant-a"],
            "cohort_users": ["user-b"],
            "cohort_percentage": 0,
            "threshold_evidence_regression": 0.1,
            "threshold_active_slot_conflicts": 1,
            "threshold_replay_divergence": 1,
            "threshold_latency_regression": 0.2,
            "threshold_quality_regression": 0.2,
            "rollback_active": False,
            "rollback_reason": None,
            "updated_by": "test",
            "updated_at": "2026-04-20T00:00:00Z",
        }
        self.shadow_rows: List[Dict[str, Any]] = []
        self.invariant_rows: List[Dict[str, Any]] = []
        self.execute_calls: List[tuple[str, tuple[Any, ...]]] = []

    async def execute(self, query: str, *args):
        self.execute_calls.append((query, args))
        if "UPDATE retrieval_rollout_control" in query:
            self.state.update(
                {
                    "mode": args[0],
                    "cohort_tenants": args[1],
                    "cohort_users": args[2],
                    "cohort_percentage": args[3],
                    "threshold_evidence_regression": args[4],
                    "threshold_active_slot_conflicts": args[5],
                    "threshold_replay_divergence": args[6],
                    "threshold_latency_regression": args[7],
                    "threshold_quality_regression": args[8],
                    "rollback_active": args[9],
                    "rollback_reason": args[10],
                    "updated_by": args[11],
                    "updated_at": args[12],
                }
            )
        return "OK"

    async def fetchone(self, query: str, *args):
        if "FROM retrieval_rollout_control" in query:
            return dict(self.state)
        return None

    async def fetch(self, query: str, *args):
        if "FROM retrieval_shadow_diffs" in query:
            return list(self.shadow_rows)
        if "FROM invariant_violations" in query:
            return list(self.invariant_rows)
        return []


@pytest.mark.asyncio
async def test_t14_cohort_routing_behaves_as_configured():
    db = FakeDB()
    controller = RolloutController(db)

    route_tenant = await controller.decide_route(tenant_id="tenant-a", user_id="u1")
    route_user = await controller.decide_route(tenant_id="tenant-x", user_id="user-b")
    route_other = await controller.decide_route(tenant_id="tenant-x", user_id="user-x")

    assert route_tenant == ROUTE_V2
    assert route_user == ROUTE_V2
    assert route_other == ROUTE_SHADOW


@pytest.mark.asyncio
async def test_t14_v2_all_and_legacy_only_modes():
    db = FakeDB()
    controller = RolloutController(db)

    await controller.update_state(patch={"mode": MODE_V2_ALL}, updated_by="test")
    assert await controller.decide_route(tenant_id="t", user_id="u") == ROUTE_V2

    await controller.update_state(patch={"mode": MODE_LEGACY_ONLY}, updated_by="test")
    assert await controller.decide_route(tenant_id="t", user_id="u") == ROUTE_LEGACY


@pytest.mark.asyncio
async def test_t14_rollback_trigger_and_post_integrity_invocation(monkeypatch):
    db = FakeDB()
    controller = RolloutController(db)
    db.shadow_rows = [
        {
            "diff_payload": {
                "regression_signals": {
                    "evidence_coverage_regression": True,
                    "latency_regression": False,
                    "continuity_drift": False,
                    "possible_contradictions": [],
                }
            },
            "metrics_payload": {},
        }
        for _ in range(25)
    ]

    called = {"run_cycle": 0}

    @dataclass(frozen=True)
    class _Summary:
        detected: int = 3
        persisted: int = 3
        auto_repaired: int = 0
        review_required: int = 3
        failed_repairs: int = 0

    class _FakeInvariantManager:
        def __init__(self, _db):
            pass

        async def run_cycle(self, *, auto_repair_enabled: bool = False, max_rows_per_invariant: int = 200):
            called["run_cycle"] += 1
            return _Summary()

    monkeypatch.setattr("src.rollout.InvariantManager", _FakeInvariantManager, raising=True)

    result = await controller.evaluate_and_apply_rollback(lookback_minutes=60, min_samples=20)

    assert result.triggered is True
    assert "evidence_regression_rate_exceeded" in (result.rollback_reason or "")
    assert db.state["mode"] == MODE_LEGACY_ONLY
    assert db.state["rollback_active"] is True
    assert called["run_cycle"] == 1


@pytest.mark.asyncio
async def test_t14_rollback_not_triggered_below_threshold():
    db = FakeDB()
    controller = RolloutController(db)
    db.shadow_rows = [
        {
            "diff_payload": {
                "regression_signals": {
                    "evidence_coverage_regression": False,
                    "latency_regression": False,
                    "continuity_drift": False,
                    "possible_contradictions": [],
                }
            },
            "metrics_payload": {},
        }
        for _ in range(25)
    ]

    result = await controller.evaluate_and_apply_rollback(lookback_minutes=60, min_samples=20)

    assert result.triggered is False
    assert db.state["rollback_active"] is False


@pytest.mark.asyncio
async def test_t14_v2_endpoint_respects_cohort_gate(monkeypatch):
    async def _route_shadow(self, *, tenant_id: str, user_id: str):
        return ROUTE_SHADOW

    monkeypatch.setattr("src.main.RolloutController.decide_route", _route_shadow, raising=True)

    with pytest.raises(HTTPException) as exc:
        await memory_query_v2(
            MemoryQueryV2Request(
                tenantId="default",
                userId="u1",
                query="q",
                lane="factual",
                limit=3,
            )
        )
    assert exc.value.status_code == 409
