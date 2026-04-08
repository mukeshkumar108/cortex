from datetime import datetime, timezone

import pytest

from src.graphiti_client import GraphitiClient


class _Node:
    def __init__(self, name: str):
        self.name = name
        self.entity_type = "Entity"
        self.uuid = "node-1"
        self.labels = ["Entity"]


@pytest.mark.asyncio
async def test_search_nodes_search_branch_passes_reference_time_and_query_hint():
    gc = GraphitiClient()
    gc._initialized = True
    captured = {}

    class _FakeSearchResult:
        def __init__(self):
            self.nodes = [_Node("Kidney stones")]

    class _FakeClient:
        async def search_(self, **kwargs):
            captured.update(kwargs)
            return _FakeSearchResult()

    gc.client = _FakeClient()

    reference_time = datetime(2026, 4, 8, 9, 30, tzinfo=timezone.utc)
    rows = await gc.search_nodes(
        tenant_id="default",
        user_id="u1",
        query="hospital reason",
        reference_time=reference_time,
        query_hint="medical context",
        limit=3,
    )

    assert rows
    assert captured.get("reference_time") == reference_time
    assert captured.get("query_hint") == "medical context"


@pytest.mark.asyncio
async def test_search_nodes_fallback_search_preserves_reference_time_and_query_hint():
    gc = GraphitiClient()
    gc._initialized = True
    captured = {}

    class _FakeClient:
        async def search_(self, **_kwargs):
            raise RuntimeError("force fallback")

        async def search(self, **kwargs):
            captured.update(kwargs)
            return [_Node("Hospital visit")]

    gc.client = _FakeClient()

    reference_time = datetime(2026, 4, 8, 9, 45, tzinfo=timezone.utc)
    rows = await gc.search_nodes(
        tenant_id="default",
        user_id="u1",
        query="hospital reason",
        reference_time=reference_time,
        query_hint="medical context",
        limit=2,
    )

    assert rows
    assert captured.get("reference_time") == reference_time
    assert captured.get("query_hint") == "medical context"
