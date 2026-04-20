from __future__ import annotations

import inspect

import pytest
from fastapi import HTTPException

from src.main import debug_graphiti_query, memory_query
from src.models import MemoryQueryRequest


def test_t15_memory_query_legacy_mixed_authority_block_removed():
    source = inspect.getsource(memory_query)
    assert "Legacy implementation retained below" not in source
    assert "_pg_search_facts(" not in source
    assert "_pg_search_nodes(" not in source


@pytest.mark.asyncio
async def test_t15_debug_graphiti_query_is_explicitly_disabled(monkeypatch):
    monkeypatch.setattr("src.main._require_internal_token", lambda _token: None, raising=True)
    with pytest.raises(HTTPException) as exc:
        await debug_graphiti_query(
            MemoryQueryRequest(
                tenantId="default",
                userId="u1",
                query="status",
                memoryIntent="hybrid",
                limit=5,
            ),
            x_internal_token="x",
        )
    assert exc.value.status_code == 410
    assert "Deprecated endpoint" in str(exc.value.detail)
