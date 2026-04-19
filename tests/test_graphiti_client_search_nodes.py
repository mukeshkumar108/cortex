from datetime import datetime, timezone

import pytest

from src.graphiti_client import GraphitiClient


class _Node:
    def __init__(self, name: str, entity_type: str = "Person", labels=None):
        self.name = name
        self.entity_type = entity_type
        self.uuid = "node-1"
        self.labels = labels or [entity_type]


@pytest.mark.asyncio
async def test_search_nodes_search_branch_passes_reference_time_and_query_hint():
    gc = GraphitiClient()
    gc._initialized = True
    captured = {}

    class _FakeSearchResult:
        def __init__(self):
            self.nodes = [_Node("Ashley")]

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
            return [_Node("Ashley")]

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


@pytest.mark.asyncio
async def test_search_nodes_filters_internal_nodes_and_dedupes_canonical_names():
    gc = GraphitiClient()
    gc._initialized = True

    class _FakeSearchResult:
        def __init__(self):
            self.nodes = [
                _Node("User", entity_type="Person", labels=["Entity"]),
                _Node("User", entity_type="Person", labels=["Entity"]),
                _Node("session_summary_abc", entity_type="SessionSummary", labels=["SessionSummary"]),
            ]

    class _FakeClient:
        async def search_(self, **_kwargs):
            return _FakeSearchResult()

    gc.client = _FakeClient()

    rows = await gc.search_nodes(
        tenant_id="default",
        user_id="u1",
        query="important people",
        limit=5,
        allowed_types=["person"],
    )

    assert len(rows) == 1
    assert rows[0]["summary"] == "User"
    assert rows[0]["type"] == "person"


@pytest.mark.asyncio
async def test_search_nodes_suppresses_generic_low_signal_nodes():
    gc = GraphitiClient()
    gc._initialized = True

    class _FakeSearchResult:
        def __init__(self):
            self.nodes = [
                _Node("update", entity_type="Event", labels=["Event"]),
                _Node("finish portfolio this week", entity_type="Goal", labels=["Goal"]),
            ]

    class _FakeClient:
        async def search_(self, **_kwargs):
            return _FakeSearchResult()

    gc.client = _FakeClient()

    rows = await gc.search_nodes(
        tenant_id="default",
        user_id="u1",
        query="goals",
        limit=5,
        allowed_types=["goal", "event"],
    )

    assert len(rows) == 1
    assert rows[0]["summary"] == "finish portfolio this week"
    assert rows[0]["type"] == "goal"
