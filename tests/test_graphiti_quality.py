from datetime import datetime

import pytest

from src.main import app, graphiti_client
from src.briefing import build_briefing


@pytest.mark.asyncio
async def test_speaker_tagging_applied():
    class _FakeGraph:
        def __init__(self):
            self.bodies = []

        async def add_episode(self, **kwargs):
            self.bodies.append(kwargs.get("episode_body"))

    fake_graph = _FakeGraph()
    graphiti_client.client = fake_graph
    graphiti_client._initialized = True

    now = datetime.utcnow()
    await graphiti_client.add_episode(
        tenant_id="t",
        user_id="u",
        text="hello there",
        timestamp=now,
        role="user"
    )
    await graphiti_client.add_episode(
        tenant_id="t",
        user_id="u",
        text="hi",
        timestamp=now,
        role="assistant"
    )

    assert fake_graph.bodies[0].startswith("USER:")
    assert fake_graph.bodies[1].startswith("ASSISTANT:")


@pytest.mark.asyncio
async def test_search_uses_rerank_and_reference_time():
    class _FakeGraph:
        def __init__(self):
            self.last_kwargs = None

        async def search(self, query, group_ids, num_results, rerank=False, reference_time=None, query_hint=None):
            self.last_kwargs = {
                "query": query,
                "group_ids": group_ids,
                "num_results": num_results,
                "rerank": rerank,
                "reference_time": reference_time,
                "query_hint": query_hint
            }
            return []

    fake_graph = _FakeGraph()
    graphiti_client.client = fake_graph
    graphiti_client._initialized = True

    now = datetime.utcnow()

    async with app.router.lifespan_context(app):
        await build_briefing(
            tenant_id="tenant",
            user_id="user",
            persona_id="persona",
            session_id=None,
            query="hello world",
            now=now,
            graphiti_client=graphiti_client
        )

    assert fake_graph.last_kwargs["rerank"] is True
    assert fake_graph.last_kwargs["reference_time"] == now


@pytest.mark.asyncio
async def test_blank_query_skips_graphiti_search():
    class _FakeGraph:
        def __init__(self):
            self.last_kwargs = None

        async def search(self, **kwargs):
            self.last_kwargs = kwargs
            return []

    fake_graph = _FakeGraph()
    graphiti_client.client = fake_graph
    graphiti_client._initialized = True

    async with app.router.lifespan_context(app):
        await build_briefing(
            tenant_id="tenant",
            user_id="user",
            persona_id="persona",
            session_id=None,
            query="   ",
            now=datetime.utcnow(),
            graphiti_client=graphiti_client
        )

    assert fake_graph.last_kwargs is None


@pytest.mark.asyncio
async def test_episode_bridge_returned_for_new_session():
    async def _recent(*_args, **_kwargs):
        return [{"name": "session_summary_abc", "text": "welcome back"}]

    graphiti_client.get_recent_episodes = _recent

    async with app.router.lifespan_context(app):
        result = await build_briefing(
            tenant_id="tenant",
            user_id="user",
            persona_id="persona",
            session_id="new-session",
            query=None,
            now=datetime.utcnow(),
            graphiti_client=graphiti_client
        )

    assert result.episodeBridge == "welcome back"
