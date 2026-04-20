from __future__ import annotations

import pytest

from src.main import memory_query
from src.models import MemoryQueryRequest, MemoryQueryV2Response, MemoryQueryV2Item


@pytest.fixture(autouse=True)
def _force_rollout_legacy_route(monkeypatch):
    async def _route_legacy(self, *, tenant_id: str, user_id: str):
        return "legacy_served"

    monkeypatch.setattr("src.main.RolloutController.decide_route", _route_legacy, raising=True)


@pytest.mark.asyncio
async def test_t11_legacy_query_routes_to_v2_only_and_blocks_old_path(monkeypatch):
    async def _should_not_call(**_kwargs):
        raise AssertionError("legacy mixed-authority helper invoked")

    async def _stub_memory_query_v2(_request):
        return MemoryQueryV2Response(
            lane="factual",
            items=[
                MemoryQueryV2Item(
                    lane="factual",
                    itemType="claim",
                    text="Ashley relationship status: dating",
                    relevance=0.91,
                    source="canonical_claims",
                    sourceTenant="default",
                    evidence=[{"evidenceText": "User: Ashley and I are dating."}],
                )
            ],
            metadata={"counts": {"factual": 1, "episodic": 0, "continuity": 0, "total": 1}},
        )

    monkeypatch.setattr("src.main._pg_search_facts", _should_not_call, raising=True)
    monkeypatch.setattr("src.main._pg_search_nodes", _should_not_call, raising=True)
    monkeypatch.setattr("src.main._build_episodic_recall_items", _should_not_call, raising=True)
    monkeypatch.setattr("src.main._memory_query_v2_service", _stub_memory_query_v2, raising=True)

    resp = await memory_query(
        MemoryQueryRequest(
            tenantId="default",
            userId="u1",
            query="who is ashley",
            memoryIntent="exact",
            limit=5,
        )
    )

    assert resp.facts == ["Ashley relationship status: dating"]
    assert len(resp.factItems) == 1
    assert resp.entities == []
    assert resp.episodes == []
    adapter = (resp.metadata or {}).get("adapter") or {}
    assert adapter.get("mode") == "t11_legacy_to_v2_only"
    assert adapter.get("legacyMixedAuthorityFallback") is False


@pytest.mark.asyncio
async def test_t11_legacy_hybrid_maps_v2_lanes_conservatively(monkeypatch):
    async def _stub_memory_query_v2(_request):
        return MemoryQueryV2Response(
            lane="hybrid",
            items=[
                MemoryQueryV2Item(
                    lane="factual",
                    itemType="claim",
                    text="Project focus: launch onboarding",
                    relevance=0.88,
                    source="canonical_claims",
                    sourceTenant="default",
                    evidence=[{"evidenceText": "User: I need to ship onboarding this week."}],
                ),
                MemoryQueryV2Item(
                    lane="episodic",
                    itemType="episode",
                    text="Discussion about blockers and timeline.",
                    relevance=0.73,
                    source="episodic_recall",
                    sourceTenant="default",
                    episodeId="ep1",
                    sessionId="s1",
                    referenceTime="2026-04-18T10:00:00Z",
                    evidence=[{"evidenceText": "User: We got blocked on CI."}],
                    linkedEntities=["Onboarding"],
                ),
                MemoryQueryV2Item(
                    lane="continuity",
                    itemType="continuity_fact",
                    text="Derived continuity hint.",
                    relevance=0.4,
                    source="continuity_projection",
                    sourceTenant="default",
                    derived=True,
                ),
            ],
            metadata={"counts": {"factual": 1, "episodic": 1, "continuity": 1, "total": 3}},
        )

    monkeypatch.setattr("src.main._memory_query_v2_service", _stub_memory_query_v2, raising=True)

    resp = await memory_query(
        MemoryQueryRequest(
            tenantId="default",
            userId="u1",
            query="what should i resume",
            memoryIntent="hybrid",
            includeContext=True,
            limit=5,
        )
    )

    assert "Project focus: launch onboarding" in resp.facts
    assert "Derived continuity hint." in resp.facts
    assert len(resp.episodes) == 1
    assert (resp.episodes[0].evidence or [""])[0].startswith("User:")
    assert resp.entities == []
    metadata = resp.metadata or {}
    assert metadata.get("memoryIntent") == "hybrid"
    assert metadata.get("facts") == 1
    assert metadata.get("continuityFacts") == 1
    assert "contextCompatibilityNotice" in metadata


@pytest.mark.asyncio
async def test_t11_legacy_default_intent_routes_to_factual_lane(monkeypatch):
    captured = {"lane": None}

    async def _stub_memory_query_v2(request):
        captured["lane"] = request.lane
        return MemoryQueryV2Response(lane=request.lane, items=[], metadata={})

    monkeypatch.setattr("src.main._memory_query_v2_service", _stub_memory_query_v2, raising=True)

    resp = await memory_query(
        MemoryQueryRequest(
            tenantId="default",
            userId="u1",
            query="q",
            limit=3,
        )
    )

    assert captured["lane"] == "factual"
    assert resp.facts == []
    assert resp.factItems == []
