from __future__ import annotations

import pytest

from src.main import (
    _run_retrieval_shadow_diff,
    _safe_run_retrieval_shadow_diff,
    memory_query,
    retrieval_shadow_audit,
)
from src.models import (
    Fact,
    MemoryQueryRequest,
    MemoryQueryResponse,
    MemoryQueryV2Item,
    MemoryQueryV2Request,
    MemoryQueryV2Response,
)


@pytest.fixture(autouse=True)
def _force_rollout_legacy_route(monkeypatch):
    async def _route_legacy(self, *, tenant_id: str, user_id: str):
        return "legacy_served"

    monkeypatch.setattr("src.main.RolloutController.decide_route", _route_legacy, raising=True)


@pytest.mark.asyncio
async def test_t12b_shadow_path_runs_without_changing_served_output(monkeypatch):
    expected = MemoryQueryResponse(
        facts=["Ashley relationship status: dating"],
        factItems=[Fact(text="Ashley relationship status: dating", source="canonical_claims", relevance=0.9)],
        entities=[],
        episodes=[],
        metadata={"k": "v"},
    )
    called = {"shadow": 0}

    async def _stub_adapter(_request):
        return expected

    async def _stub_shadow(**_kwargs):
        called["shadow"] += 1
        return None

    settings = __import__("src.main", fromlist=["get_settings"]).get_settings()
    monkeypatch.setattr(settings, "retrieval_shadow_read_enabled", True, raising=False)
    monkeypatch.setattr(settings, "retrieval_shadow_read_endpoint_enabled", True, raising=False)
    monkeypatch.setattr(settings, "retrieval_shadow_read_sample_rate", 1.0, raising=False)
    monkeypatch.setattr(settings, "retrieval_shadow_read_blocking", True, raising=False)
    monkeypatch.setattr("src.main._memory_query_legacy_adapter", _stub_adapter, raising=True)
    monkeypatch.setattr("src.main._safe_run_retrieval_shadow_diff", _stub_shadow, raising=True)

    resp = await memory_query(
        MemoryQueryRequest(
            tenantId="default",
            userId="u1",
            query="who is ashley",
            memoryIntent="exact",
            limit=3,
        )
    )

    assert resp.model_dump() == expected.model_dump()
    assert called["shadow"] == 1


@pytest.mark.asyncio
async def test_t12b_shadow_feature_flag_disabled_skips_shadow(monkeypatch):
    expected = MemoryQueryResponse(facts=["x"], factItems=[Fact(text="x")], entities=[], episodes=[], metadata={})
    called = {"shadow": 0}

    async def _stub_adapter(_request):
        return expected

    async def _stub_shadow(**_kwargs):
        called["shadow"] += 1

    settings = __import__("src.main", fromlist=["get_settings"]).get_settings()
    monkeypatch.setattr(settings, "retrieval_shadow_read_enabled", False, raising=False)
    monkeypatch.setattr(settings, "retrieval_shadow_read_sample_rate", 1.0, raising=False)
    monkeypatch.setattr(settings, "retrieval_shadow_read_blocking", True, raising=False)
    monkeypatch.setattr("src.main._memory_query_legacy_adapter", _stub_adapter, raising=True)
    monkeypatch.setattr("src.main._safe_run_retrieval_shadow_diff", _stub_shadow, raising=True)

    resp = await memory_query(
        MemoryQueryRequest(tenantId="default", userId="u1", query="q", memoryIntent="exact", limit=3)
    )

    assert resp.facts == ["x"]
    assert called["shadow"] == 0


@pytest.mark.asyncio
async def test_t12b_diff_record_written(monkeypatch):
    persisted = {"payload": None}

    async def _stub_v2(_request: MemoryQueryV2Request):
        return MemoryQueryV2Response(
            lane="factual",
            items=[
                MemoryQueryV2Item(
                    lane="factual",
                    itemType="claim",
                    text="Ashley relationship status: dating",
                    evidence=[{"evidenceText": "User: Ashley and I are dating."}],
                )
            ],
            metadata={},
        )

    async def _stub_persist(_db, payload):
        persisted["payload"] = payload

    monkeypatch.setattr("src.main._memory_query_v2_service", _stub_v2, raising=True)
    monkeypatch.setattr("src.main.persist_shadow_diff", _stub_persist, raising=True)

    served = MemoryQueryResponse(
        facts=["Ashley relationship status: dating"],
        factItems=[Fact(text="Ashley relationship status: dating")],
        entities=[],
        episodes=[],
        metadata={},
    )

    await _run_retrieval_shadow_diff(
        request=MemoryQueryRequest(tenantId="default", userId="u1", query="ashley", memoryIntent="exact", limit=3),
        served_response=served,
        served_latency_ms=12.0,
    )

    payload = persisted["payload"] or {}
    assert payload.get("tenant_id") == "default"
    assert isinstance((payload.get("diff") or {}).get("factual_differences"), dict)
    assert "latency_differences" in (payload.get("diff") or {})


@pytest.mark.asyncio
async def test_t12b_malformed_diff_fails_safe(monkeypatch):
    called = {"error": 0}

    async def _raise(*_args, **_kwargs):
        raise RuntimeError("boom")

    async def _persist_error(_db, **_kwargs):
        called["error"] += 1

    monkeypatch.setattr("src.main._run_retrieval_shadow_diff", _raise, raising=True)
    monkeypatch.setattr("src.main.persist_shadow_error", _persist_error, raising=True)

    await _safe_run_retrieval_shadow_diff(
        request=MemoryQueryRequest(tenantId="default", userId="u1", query="q", memoryIntent="exact", limit=3),
        served_response=MemoryQueryResponse(facts=[], factItems=[], entities=[], episodes=[], metadata={}),
        served_latency_ms=1.0,
    )

    assert called["error"] == 1


@pytest.mark.asyncio
async def test_t12b_audit_endpoint_returns_structured_summary(monkeypatch):
    async def _stub_fetch(_query, *_args, **_kwargs):
        return [
            {
                "shadow_id": 1,
                "tenant_id": "default",
                "user_id": "u1",
                "endpoint": "/memory/query",
                "served_intent": "exact",
                "request_fingerprint": "abc",
                "status": "ok",
                "served_latency_ms": 20.0,
                "shadow_latency_ms": 33.0,
                "latency_delta_ms": 13.0,
                "diff_payload": {
                    "regression_signals": {
                        "evidence_coverage_regression": True,
                        "unsupported_factual_difference": True,
                        "latency_regression": False,
                        "continuity_drift": False,
                        "possible_contradictions": [],
                    }
                },
                "metrics_payload": {},
                "created_at": "2026-04-20T00:00:00Z",
            }
        ]

    monkeypatch.setattr("src.main.db.fetch", _stub_fetch, raising=False)
    monkeypatch.setattr("src.main._require_internal_token", lambda _token: None, raising=True)

    out = await retrieval_shadow_audit(hours=24, limit=20, tenant_id="default", x_internal_token="x")

    assert out["rows"] == 1
    assert out["regression_counts"]["evidence"] == 1
    assert out["regression_counts"]["unsupported_factual"] == 1
