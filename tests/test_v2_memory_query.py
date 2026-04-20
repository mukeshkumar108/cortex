from __future__ import annotations

from datetime import datetime, timezone as dt_timezone
import pytest
from pydantic import ValidationError

from src.main import memory_query_v2
from src.models import MemoryQueryV2Item, MemoryQueryV2Request


@pytest.fixture(autouse=True)
def _force_v2_route(monkeypatch):
    async def _route_v2(self, *, tenant_id: str, user_id: str):
        return "v2_served"

    monkeypatch.setattr("src.main.RolloutController.decide_route", _route_v2, raising=True)


@pytest.mark.asyncio
async def test_v2_memory_query_factual_returns_evidence_backed_claims_only(monkeypatch):
    now = datetime.now(dt_timezone.utc)

    async def _stub_fetch(_query, *_args, **_kwargs):
        return [
            {
                "tenant_id": "default",
                "claim_slot_key": "slot:1",
                "claim_event_key": "event:no_evidence",
                "lifecycle_status": "active",
                "predicate": "relationship.status",
                "subject_text": "Ashley",
                "object_payload": {"value": "dating"},
                "extraction_confidence": 0.81,
                "truth_confidence": 0.82,
                "predicate_policy_version": "v1",
                "updated_at": now,
                "subject_name": "Ashley",
                "evidence_links": [],
            },
            {
                "tenant_id": "default",
                "claim_slot_key": "slot:2",
                "claim_event_key": "event:with_evidence",
                "lifecycle_status": "active",
                "predicate": "relationship.status",
                "subject_text": "Ashley",
                "object_payload": {"value": "dating"},
                "extraction_confidence": 0.89,
                "truth_confidence": 0.91,
                "predicate_policy_version": "v1",
                "updated_at": now,
                "subject_name": "Ashley",
                "evidence_links": [{"claimEvidenceId": 1, "evidenceText": "User: Ashley and I are dating."}],
            },
        ]

    monkeypatch.setattr("src.main.db.fetch", _stub_fetch, raising=False)

    resp = await memory_query_v2(
        MemoryQueryV2Request(
            tenantId="default",
            userId="u1",
            query="who is Ashley",
            lane="factual",
            limit=10,
        )
    )

    assert resp.lane == "factual"
    assert len(resp.items) == 1
    assert resp.items[0].lane == "factual"
    assert resp.items[0].itemType == "claim"
    assert bool(resp.items[0].evidence)
    assert resp.items[0].derived is False
    assert resp.items[0].dataClassification == "canonical factual"


@pytest.mark.asyncio
async def test_v2_memory_query_continuity_lane_stays_derived_only(monkeypatch):
    async def _stub_continuity_facts(**_kwargs):
        return [
            {
                "text": "Derived continuity note.",
                "relevance": 0.55,
                "source": "continuity_projection",
                "source_type": "derived continuity/projection",
                "derived": True,
                "evidence_backed": False,
                "data_classification": "derived continuity/projection",
                "valid_at": None,
            },
            {
                "text": "Canonical should be dropped in continuity lane.",
                "relevance": 0.99,
                "source": "canonical_claims",
                "source_type": "canonical factual",
                "derived": False,
                "evidence_backed": True,
                "data_classification": "canonical factual",
                "valid_at": None,
            },
        ]

    monkeypatch.setattr("src.main._pg_search_continuity_facts", _stub_continuity_facts, raising=True)

    resp = await memory_query_v2(
        MemoryQueryV2Request(
            tenantId="default",
            userId="u1",
            query="status",
            lane="continuity",
            limit=10,
        )
    )

    assert resp.lane == "continuity"
    assert len(resp.items) == 1
    assert resp.items[0].lane == "continuity"
    assert resp.items[0].derived is True
    assert resp.items[0].dataClassification == "derived continuity/projection"


@pytest.mark.asyncio
async def test_v2_memory_query_episodic_lane_returns_episodic_only(monkeypatch):
    async def _stub_episodic_items(**_kwargs):
        return [
            MemoryQueryV2Item(
                lane="episodic",
                itemType="episode",
                text="Conversation about launch blockers.",
                source="episodic_recall",
                evidence=[{"evidenceText": "User: We were blocked on CI."}],
            )
        ]

    async def _should_not_call(**_kwargs):
        raise AssertionError("unexpected lane collector invocation")

    monkeypatch.setattr("src.main._v2_collect_episodic_items", _stub_episodic_items, raising=True)
    monkeypatch.setattr("src.main._v2_collect_factual_items", _should_not_call, raising=True)
    monkeypatch.setattr("src.main._v2_collect_continuity_items", _should_not_call, raising=True)

    resp = await memory_query_v2(
        MemoryQueryV2Request(
            tenantId="default",
            userId="u1",
            query="what did we discuss",
            lane="episodic",
            limit=5,
        )
    )

    assert resp.lane == "episodic"
    assert len(resp.items) == 1
    assert all(item.lane == "episodic" for item in resp.items)


@pytest.mark.asyncio
async def test_v2_memory_query_hybrid_preserves_lane_labels(monkeypatch):
    async def _stub_factual(**_kwargs):
        return [MemoryQueryV2Item(lane="factual", itemType="claim", text="Fact", evidence=[{"evidenceText": "e"}])]

    async def _stub_episodic(**_kwargs):
        return [MemoryQueryV2Item(lane="episodic", itemType="episode", text="Episode")]

    async def _stub_continuity(**_kwargs):
        return [MemoryQueryV2Item(lane="continuity", itemType="continuity_fact", text="Continuity", derived=True)]

    monkeypatch.setattr("src.main._v2_collect_factual_items", _stub_factual, raising=True)
    monkeypatch.setattr("src.main._v2_collect_episodic_items", _stub_episodic, raising=True)
    monkeypatch.setattr("src.main._v2_collect_continuity_items", _stub_continuity, raising=True)

    resp = await memory_query_v2(
        MemoryQueryV2Request(
            tenantId="default",
            userId="u1",
            query="combined",
            lane="hybrid",
            limit=3,
        )
    )

    assert [item.lane for item in resp.items] == ["factual", "episodic", "continuity"]
    counts = (resp.metadata or {}).get("counts") or {}
    assert counts.get("factual") == 1
    assert counts.get("episodic") == 1
    assert counts.get("continuity") == 1


def test_v2_memory_query_rejects_unsupported_lane():
    with pytest.raises(ValidationError):
        MemoryQueryV2Request(
            tenantId="default",
            userId="u1",
            query="q",
            lane="unsupported",
        )
