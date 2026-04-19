import asyncio
import os
from datetime import datetime
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from src.main import app, graphiti_client
from src.graphiti_client import NARRATIVE_EXTRACTION_INSTRUCTIONS
from graphiti_core.nodes import EntityNode
from src.briefing import build_briefing
from src import session as session_module
from src.config import get_settings


def _contains_banned_interpretive_phrases(text: str) -> bool:
    banned = ("feels", "feeling", "struggling", "isolating", "grounding", "vibe", "tension", "emotional")
    lower = (text or "").lower()
    return any(term in lower for term in banned)


def _contains_required_sections(text: str) -> bool:
    required = ("FACTS:", "OPEN_LOOPS:", "COMMITMENTS:", "CONTEXT_ANCHORS:")
    value = text or ""
    return all(section in value for section in required)


@pytest.mark.asyncio
async def test_focus_extraction_instruction_present():
    assert "Capture UserFocus only when explicitly stated" in NARRATIVE_EXTRACTION_INSTRUCTIONS


def _db_url() -> str:
    import os
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    password = os.getenv("POSTGRES_PASSWORD", "password")
    return f"postgresql://synapse:{password}@postgres:5432/synapse"


@pytest.mark.asyncio
async def test_brief_is_minimal_without_query():
    async with app.router.lifespan_context(app):
        result = await build_briefing(
            tenant_id="tenant",
            user_id="user",
            persona_id="persona",
            session_id=None,
            query="Ashley",
            now=datetime.utcnow(),
            graphiti_client=graphiti_client
        )
        assert result.semanticContext == []
        assert result.entities == []
        assert result.activeLoops == []


@pytest.mark.asyncio
async def test_memory_query_uses_evidence_backed_factual_rows(monkeypatch):
    async def _stub_semantic(items, **_kwargs):
        return [
            SimpleNamespace(
                domain="relationships",
                intent="share_update",
                memory_type="fact",
                domain_scores={"relationships": 0.7},
                confidence=0.8,
                classification_method="fallback",
            )
            for _ in items
        ]

    async def _stub_pg_search_facts(**_kwargs):
        return [
            {
                "text": "User likes birds",
                "relevance": 0.9,
                "source": "claim_store",
                "source_type": "canonical factual",
                "derived": False,
                "evidence_backed": True,
                "data_classification": "canonical factual",
            }
        ]

    async def _stub_pg_search_nodes(**_kwargs):
        return [{"summary": "Ashley", "type": "Entity", "labels": ["Entity"], "uuid": "u1"}]

    async def _stub_user_model_rows(**_kwargs):
        return []

    monkeypatch.setattr("src.main._pg_search_facts", _stub_pg_search_facts, raising=True)
    monkeypatch.setattr("src.main._pg_search_nodes", _stub_pg_search_nodes, raising=True)
    monkeypatch.setattr("src.main._fetch_user_model_rows_for_scope", _stub_user_model_rows, raising=True)
    monkeypatch.setattr("src.main.classify_memory_candidates_semantic", _stub_semantic, raising=True)

    from src.main import memory_query
    from src.models import MemoryQueryRequest

    req = MemoryQueryRequest(
        tenantId="t",
        userId="u",
        query="Ashley",
        limit=5,
        referenceTime=None,
        includeContext=True
    )
    resp = await memory_query(req)
    assert len(resp.facts) == 1
    assert len(resp.factItems) == 1
    assert len(resp.entities) == 1
    assert resp.recallSheet is not None
    assert _contains_required_sections(resp.recallSheet)
    assert resp.supplementalContext == resp.recallSheet
    assert len(resp.recallSheet) <= 720


@pytest.mark.asyncio
async def test_session_close_is_enqueue_only():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"
    session_id = f"session-{uuid4().hex}"

    called = {"raw_episode": 0}

    async def _fake_add_session_episode(**_kwargs):
        called["raw_episode"] += 1
        return {"success": True, "episode_uuid": "ep1"}

    graphiti_client.add_session_episode = _fake_add_session_episode

    async with app.router.lifespan_context(app):
        await session_module.add_turn(
            tenant_id=tenant,
            session_id=session_id,
            user_id=user,
            role="user",
            text="My name is Mukesh",
            timestamp=datetime.utcnow().isoformat() + "Z"
        )
        await session_module.add_turn(
            tenant_id=tenant,
            session_id=session_id,
            user_id=user,
            role="assistant",
            text="Nice to meet you",
            timestamp=datetime.utcnow().isoformat() + "Z"
        )
        await session_module.close_session(
            tenant_id=tenant,
            session_id=session_id,
            user_id=user,
            graphiti_client=graphiti_client
        )

    assert called["raw_episode"] == 0

    conn = await asyncpg.connect(_db_url())
    try:
        buffer_row = await conn.fetchrow(
            "SELECT closed_at FROM session_buffer WHERE tenant_id=$1 AND session_id=$2",
            tenant,
            session_id
        )
        raw_row = await conn.fetchrow(
            """
            SELECT job_type, status, dedupe_key
            FROM graphiti_outbox
            WHERE tenant_id = $1 AND user_id = $2 AND session_id = $3
              AND job_type = 'session_raw_episode'
            ORDER BY id DESC
            LIMIT 1
            """,
            tenant,
            user,
            session_id
        )
        assert buffer_row is not None
        assert buffer_row["closed_at"] is not None
        assert raw_row is not None
        assert raw_row["status"] == "pending"
        assert raw_row["dedupe_key"] == f"session_ingest_raw:{tenant}:{user}:{session_id}"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_add_session_summary_sets_summary_field(monkeypatch):
    from src.graphiti_client import GraphitiClient

    gc = GraphitiClient()
    gc._initialized = True

    class _FakeDriver:
        pass

    class _FakeClient:
        driver = _FakeDriver()

    gc.client = _FakeClient()

    saved = {}

    async def _fake_save(self, _driver):
        saved["summary"] = getattr(self, "summary", None)
        saved["labels"] = getattr(self, "labels", [])
        saved["name"] = getattr(self, "name", None)
        return True

    monkeypatch.setattr(EntityNode, "save", _fake_save, raising=True)
    async def _always_exists(**_kwargs):
        return True
    monkeypatch.setattr(gc, "_session_summary_uuid_exists", _always_exists, raising=True)

    resp = await gc.add_session_summary(
        tenant_id="t",
        user_id="u",
        session_id="session-1",
        summary_text="User finished portfolio updates.",
        reference_time=datetime.utcnow(),
        episode_uuid=None
    )

    assert resp["success"] is True
    assert saved["summary"] == "User finished portfolio updates."
    assert "SessionSummary" in saved["labels"]


@pytest.mark.asyncio
async def test_add_session_summary_uses_index_text_for_summary_field(monkeypatch):
    from src.graphiti_client import GraphitiClient

    gc = GraphitiClient()
    gc._initialized = True

    class _FakeDriver:
        pass

    class _FakeClient:
        driver = _FakeDriver()

    gc.client = _FakeClient()

    saved = {}

    async def _fake_save(self, _driver):
        saved["summary"] = getattr(self, "summary", None)
        saved["attributes"] = getattr(self, "attributes", {})
        return True

    monkeypatch.setattr(EntityNode, "save", _fake_save, raising=True)
    async def _always_exists(**_kwargs):
        return True
    monkeypatch.setattr(gc, "_session_summary_uuid_exists", _always_exists, raising=True)

    resp = await gc.add_session_summary(
        tenant_id="t",
        user_id="u",
        session_id="session-2",
        summary_text="Display summary only.",
        reference_time=datetime.utcnow(),
        episode_uuid=None,
        extra_attributes={"index_text": "Embedding content with decisions and loops."}
    )

    assert resp["success"] is True
    assert saved["summary"] == "Embedding content with decisions and loops."
    assert saved["attributes"]["index_text"] == "Embedding content with decisions and loops."


@pytest.mark.asyncio
async def test_session_brief_happy_path():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"

    async def _stub_recent_summaries(**_kwargs):
        return [
            {"summary": "User talked about testing and bugs", "reference_time": "2026-02-05T01:00:00Z"},
            {"summary": "User planned a demo", "reference_time": "2026-02-04T20:00:00Z"},
            {"summary": "User mentioned Ashley", "reference_time": "2026-02-04T18:00:00Z"},
        ]

    async def _stub_search_nodes(**kwargs):
        query = kwargs.get("query", "")
        if "unresolved" in query or "open loops" in query:
            return [{
                "summary": "Flaky tests",
                "type": "Tension",
                "attributes": {"description": "Flaky tests", "status": "unresolved"},
            }]
        if "named people" in query:
            return [{
                "summary": "Cafe",
                "type": "Environment",
                "attributes": {"location_type": "Cafe"},
            }]
        if "commitment" in query:
            return [{
                "summary": "I will send the demo notes tomorrow",
                "type": "Task",
            }]
        if "current focus" in query:
            return [{
                "summary": "I'm focused on stabilizing the release pipeline",
                "type": "UserFocus",
                "attributes": {"focus": "I'm focused on stabilizing the release pipeline"},
            }]
        return []

    graphiti_client.get_recent_episode_summaries = _stub_recent_summaries
    graphiti_client.search_nodes = _stub_search_nodes

    from src.main import session_brief

    resp = await session_brief(
        tenantId=tenant,
        userId=user,
        now="2026-02-05T03:00:00Z",
    )
    assert resp.timeGapDescription
    assert len(resp.narrativeSummary) >= 1
    assert resp.activeLoops[0]["status"] == "unresolved"
    assert resp.openLoops == ["Flaky tests"]
    assert resp.timeOfDayLabel == "NIGHT"
    assert _contains_required_sections(resp.briefContext or "")
    assert len(resp.briefContext or "") <= 720
    assert not _contains_banned_interpretive_phrases(resp.briefContext or "")
    assert resp.currentFocus is not None
    assert "CURRENT_FOCUS:" in (resp.briefContext or "")


@pytest.mark.asyncio
async def test_session_brief_filters_low_signal_facts_and_sheet_headings():
    async def _stub_recent_summaries(**_kwargs):
        return [
            {
                "summary": "FACTS:\n- User\n- presentation\n- User stayed up all night finishing the UI overhaul.\nOPEN_LOOPS:\n- TODO",
                "reference_time": "2026-02-05T01:00:00Z",
            }
        ]

    async def _stub_search_nodes(**kwargs):
        query = kwargs.get("query", "")
        if "unresolved" in query or "open loops" in query:
            return []
        if "named people" in query:
            return [
                {"summary": "presentation", "type": "Project"},
                {"summary": "Ashley's presentation", "type": "Project"},
            ]
        if "commitment" in query:
            return []
        if "current focus" in query:
            return []
        return []

    graphiti_client.get_recent_episode_summaries = _stub_recent_summaries
    graphiti_client.search_nodes = _stub_search_nodes

    from src.main import session_brief

    resp = await session_brief(tenantId="t", userId="u", now="2026-02-05T03:00:00Z")

    # Facts should be concrete and avoid low-signal singleton tokens/headings.
    lower_facts = [f.lower() for f in resp.facts]
    assert "user" not in lower_facts
    assert "presentation" not in lower_facts
    assert "facts:" not in lower_facts
    assert "open_loops:" not in lower_facts
    assert any("ui overhaul" in f.lower() for f in resp.facts)

    # briefContext should not echo sheet headings as fact lines.
    assert "- FACTS:" not in (resp.briefContext or "")
    assert "- OPEN_LOOPS:" not in (resp.briefContext or "")


@pytest.mark.asyncio
async def test_session_brief_narrative_summary_does_not_duplicate_facts():
    async def _stub_recent_summaries(**_kwargs):
        return [
            {"summary": "User planned a demo. User stayed up all night finishing the UI overhaul.", "reference_time": "2026-02-05T01:00:00Z"},
            {"summary": "User mentioned Ashley's presentation.", "reference_time": "2026-02-04T20:00:00Z"},
        ]

    async def _stub_search_nodes(**kwargs):
        query = kwargs.get("query", "")
        if "unresolved" in query or "open loops" in query:
            return []
        if "named people" in query:
            return [{"summary": "Ashley's presentation", "type": "Project"}]
        if "commitment" in query:
            return []
        if "current focus" in query:
            return []
        return []

    graphiti_client.get_recent_episode_summaries = _stub_recent_summaries
    graphiti_client.search_nodes = _stub_search_nodes

    from src.main import session_brief

    resp = await session_brief(tenantId="t", userId="u", now="2026-02-05T03:00:00Z")
    fact_keys = {f.lower() for f in resp.facts}
    narrative_keys = {item.get("summary", "").lower() for item in (resp.narrativeSummary or [])}
    assert fact_keys.isdisjoint(narrative_keys)


@pytest.mark.asyncio
async def test_internal_graphiti_debug_endpoints():
    os.environ["INTERNAL_TOKEN"] = "test_token"
    get_settings.cache_clear()

    async def _stub_recent_episodes(**_kwargs):
        return [
            {"name": "session_raw_x", "summary": "User talked about bugs", "reference_time": "2026-02-05T01:00:00Z"}
        ]

    async def _stub_search_facts(**_kwargs):
        return [{"text": "User is frustrated about bugs", "relevance": 0.8, "source": "graphiti"}]

    async def _stub_search_nodes(**_kwargs):
        return [{"summary": "Ashley", "type": "person", "uuid": "u1", "attributes": {"role": "girlfriend"}}]

    graphiti_client.get_recent_episodes = _stub_recent_episodes
    graphiti_client.search_facts = _stub_search_facts
    graphiti_client.search_nodes = _stub_search_nodes

    from src.main import debug_graphiti_episodes, debug_graphiti_query
    from src.models import MemoryQueryRequest

    episodes = await debug_graphiti_episodes(
        tenantId="t",
        userId="u",
        limit=2,
        x_internal_token="test_token"
    )
    assert episodes["count"] == 1

    query_resp = await debug_graphiti_query(
        request=MemoryQueryRequest(tenantId="t", userId="u", query="Ashley"),
        x_internal_token="test_token"
    )
    assert len(query_resp["facts"]) == 1
    assert len(query_resp["entities"]) == 1


@pytest.mark.asyncio
async def test_narrative_continuity_integrity():
    tenant = f"tenant-{uuid4().hex}"
    user = f"user-{uuid4().hex}"

    async def _stub_recent_summaries(**_kwargs):
        return [{"summary": "User mentioned the blue-widget-glitch", "reference_time": "2026-02-05T01:00:00Z"}]

    async def _stub_search_nodes(**kwargs):
        query = kwargs.get("query", "")
        if "unresolved" in query or "open loops" in query:
            return [{
                "summary": "blue-widget-glitch",
                "type": "Tension",
                "attributes": {"description": "blue-widget-glitch", "status": "unresolved"},
            }]
        if "named people" in query:
            return [{
                "summary": "Gym",
                "type": "Environment",
                "attributes": {"location_type": "Gym"},
            }]
        return []

    graphiti_client.get_recent_episode_summaries = _stub_recent_summaries
    graphiti_client.search_nodes = _stub_search_nodes

    from src.main import session_brief

    resp = await session_brief(
        tenantId=tenant,
        userId=user,
        now="2026-02-05T02:00:00Z",
    )

    assert any(loop["description"] == "blue-widget-glitch" for loop in resp.activeLoops)
    assert "blue-widget-glitch" in resp.openLoops
    assert "blue-widget-glitch" in " ".join(resp.facts)
    assert not _contains_banned_interpretive_phrases(resp.briefContext or "")
    assert len(resp.briefContext or "") <= 720
    assert "CURRENT_FOCUS:" not in (resp.briefContext or "")


@pytest.mark.asyncio
async def test_explicit_user_state_allowed_and_isolated():
    async def _stub_recent_summaries(**_kwargs):
        return [
            {"summary": "I feel anxious about tomorrow's launch", "reference_time": "2026-02-05T01:00:00Z"},
            {"summary": "User is struggling with launch pressure", "reference_time": "2026-02-04T20:00:00Z"},
        ]

    async def _stub_search_nodes(**kwargs):
        query = kwargs.get("query", "")
        if "unresolved" in query or "open loops" in query:
            return []
        if "named people" in query:
            return [{"summary": "Launch prep", "type": "Project"}]
        if "commitment" in query:
            return []
        return []

    graphiti_client.get_recent_episode_summaries = _stub_recent_summaries
    graphiti_client.search_nodes = _stub_search_nodes

    from src.main import session_brief

    resp = await session_brief(
        tenantId="tenant",
        userId="user",
        now="2026-02-05T02:00:00Z",
    )

    assert resp.userStatedState is not None
    assert "i feel anxious" in resp.userStatedState.lower()
    assert "i feel anxious" not in " ".join(resp.facts).lower()
    assert "i feel anxious" not in " ".join(resp.openLoops).lower()
    assert "i feel anxious" not in " ".join(resp.commitments).lower()
    assert "USER_STATED_STATE:" in (resp.briefContext or "")
    assert "struggling" not in (resp.briefContext or "").lower()


@pytest.mark.asyncio
async def test_memory_query_recall_sheet_filters_interpretive_language():
    class _FakeGraph:
        async def search_facts(self, **_kwargs):
            return [
                {"text": "User is struggling with launch pressure", "source": "graphiti"},
                {"text": "I feel anxious about tomorrow's launch", "source": "graphiti"},
                {"text": "Launch is scheduled for Friday 9 AM", "source": "graphiti"},
            ]

        async def search_nodes(self, **_kwargs):
            query = _kwargs.get("query", "")
            if "current focus" in query:
                return [{
                    "summary": "Right now I'm trying to finish the launch checklist",
                    "type": "UserFocus",
                    "attributes": {"focus": "Right now I'm trying to finish the launch checklist"},
                    "created_at": "2026-02-04T18:00:00Z",
                }]
            return [
                {
                    "summary": "launch-checklist",
                    "type": "Tension",
                    "attributes": {"description": "launch-checklist", "status": "unresolved"},
                },
                {"summary": "Sophie", "type": "Person", "uuid": "u1"},
            ]

    fake = _FakeGraph()
    graphiti_client.search_facts = fake.search_facts
    graphiti_client.search_nodes = fake.search_nodes

    from src.main import memory_query
    from src.models import MemoryQueryRequest

    resp = await memory_query(
        MemoryQueryRequest(
            tenantId="t",
            userId="u",
            query="launch",
            referenceTime="2026-02-05T01:00:00Z",
            includeContext=True
        )
    )
    assert resp.recallSheet is not None
    assert _contains_required_sections(resp.recallSheet)
    assert len(resp.recallSheet) <= 720
    assert "struggling" not in resp.recallSheet.lower()
    assert "i feel anxious" in resp.recallSheet.lower()
    assert "i feel anxious" not in " ".join(resp.facts).lower()
    assert "i feel anxious" not in " ".join(resp.openLoops).lower()
    assert "i feel anxious" not in " ".join(resp.commitments).lower()
    assert "CURRENT_FOCUS:" not in resp.recallSheet
    assert resp.currentFocus is None


@pytest.mark.asyncio
async def test_current_focus_omitted_when_stale():
    class _FakeGraph:
        async def search_facts(self, **_kwargs):
            return []

        async def search_nodes(self, **_kwargs):
            query = _kwargs.get("query", "")
            if "current focus" in query:
                return [{
                    "summary": "I'm focused on refactoring the build",
                    "type": "UserFocus",
                    "attributes": {"focus": "I'm focused on refactoring the build"},
                    "created_at": "2026-01-01T00:00:00Z",
                }]
            return []

    fake = _FakeGraph()
    graphiti_client.search_facts = fake.search_facts
    graphiti_client.search_nodes = fake.search_nodes

    from src.main import memory_query
    from src.models import MemoryQueryRequest

    resp = await memory_query(MemoryQueryRequest(tenantId="t", userId="u", query="focus", includeContext=True))
    assert resp.recallSheet is not None
    assert "CURRENT_FOCUS:" not in resp.recallSheet
    assert resp.currentFocus is None


@pytest.mark.asyncio
async def test_memory_query_focus_query_is_configurable():
    captured_queries = []

    class _FakeGraph:
        async def search_facts(self, **_kwargs):
            return []

        async def search_nodes(self, **_kwargs):
            query = _kwargs.get("query", "")
            captured_queries.append(query)
            return []

    fake = _FakeGraph()
    graphiti_client.search_facts = fake.search_facts
    graphiti_client.search_nodes = fake.search_nodes

    from src.main import memory_query
    from src.models import MemoryQueryRequest

    await memory_query(
        MemoryQueryRequest(
            tenantId="t",
            userId="u",
            query="focus",
            includeContext=True,
            focusQuery="my custom focus query terms",
        )
    )

    assert "my custom focus query terms" in captured_queries


@pytest.mark.asyncio
async def test_memory_query_blocks_session_summary_and_user_model_fact_injection(monkeypatch):
    async def _stub_semantic(items, **_kwargs):
        return [
            SimpleNamespace(
                domain="relationships",
                intent="share_update",
                memory_type="fact",
                domain_scores={"relationships": 0.7},
                confidence=0.75,
                classification_method="fallback",
            )
            for _ in items
        ]

    async def _stub_pg_search_facts(**_kwargs):
        return [
            {
                "text": "Ashley is the user's long-term girlfriend.",
                "relevance": 0.91,
                "source": "claim_store",
                "source_type": "canonical factual",
                "derived": False,
                "evidence_backed": True,
                "data_classification": "canonical factual",
            }
        ]

    async def _stub_pg_search_nodes(**_kwargs):
        return [
            {
                "summary": "session summary",
                "type": "SessionSummary",
                "labels": ["SessionSummary"],
                "attributes": {
                    "summary_facts": "Recently out of hospital with kidney stones.",
                    "reference_time": "2026-04-08T09:00:00Z",
                },
            },
            {"summary": "Ashley", "type": "person", "uuid": "p1", "labels": ["Person"]},
        ]

    async def _stub_user_model_rows(**_kwargs):
        return [
            {
                "tenant_id": "default",
                "model": {
                    "narrative_current": "Recently out of hospital with kidney stones.",
                    "recent_signals": [{"text": "Hydration helps with kidney stone recovery."}],
                },
            }
        ]

    monkeypatch.setattr("src.main._pg_search_facts", _stub_pg_search_facts, raising=True)
    monkeypatch.setattr("src.main._pg_search_nodes", _stub_pg_search_nodes, raising=True)
    monkeypatch.setattr("src.main._fetch_user_model_rows_for_scope", _stub_user_model_rows, raising=True)
    monkeypatch.setattr("src.main.classify_memory_candidates_semantic", _stub_semantic, raising=True)

    from src.main import memory_query
    from src.models import MemoryQueryRequest

    resp = await memory_query(
        MemoryQueryRequest(
            tenantId="default",
            userId="u",
            query="who is Ashley",
            includeContext=False,
            limit=5,
        )
    )

    assert resp.facts == ["Ashley is the user's long-term girlfriend."]
    sources = {item.source for item in resp.factItems}
    assert "graphiti_session_summary" not in sources
    assert "user_model" not in sources
    assert all("kidney stone" not in item.text.lower() for item in resp.factItems)
    assert not any((e.type or "").lower() == "sessionsummary" for e in resp.entities)


@pytest.mark.asyncio
async def test_memory_query_factual_output_requires_evidence_backed_rows(monkeypatch):
    async def _stub_semantic(items, **_kwargs):
        return [
            SimpleNamespace(
                domain="work",
                intent="share_update",
                memory_type="fact",
                domain_scores={"work": 0.6},
                confidence=0.7,
                classification_method="fallback",
            )
            for _ in items
        ]

    async def _stub_pg_search_facts(**_kwargs):
        return [
            {
                "text": "Unsupported derived fact.",
                "relevance": 0.9,
                "source": "derived_projection",
                "source_type": "derived continuity/projection",
                "derived": True,
                "evidence_backed": False,
                "data_classification": "derived continuity/projection",
            },
            {
                "text": "Evidence-backed canonical fact.",
                "relevance": 0.8,
                "source": "claim_store",
                "source_type": "canonical factual",
                "derived": False,
                "evidence_backed": True,
                "data_classification": "canonical factual",
            },
        ]

    async def _stub_pg_search_nodes(**_kwargs):
        return []

    async def _stub_user_model_rows(**_kwargs):
        return []

    monkeypatch.setattr("src.main._pg_search_facts", _stub_pg_search_facts, raising=True)
    monkeypatch.setattr("src.main._pg_search_nodes", _stub_pg_search_nodes, raising=True)
    monkeypatch.setattr("src.main._fetch_user_model_rows_for_scope", _stub_user_model_rows, raising=True)
    monkeypatch.setattr("src.main.classify_memory_candidates_semantic", _stub_semantic, raising=True)

    from src.main import memory_query
    from src.models import MemoryQueryRequest

    resp = await memory_query(
        MemoryQueryRequest(
            tenantId="default",
            userId="u",
            query="what do you know",
            includeContext=False,
            limit=5,
        )
    )

    assert resp.facts == ["Evidence-backed canonical fact."]
    assert all((item.source or "") != "derived_projection" for item in resp.factItems)


@pytest.mark.asyncio
async def test_memory_search_endpoint_is_disabled():
    from fastapi import HTTPException
    from src.main import memory_search

    with pytest.raises(HTTPException) as exc:
        await memory_search({"user_id": "u", "query": "q", "limit": 3})
    assert exc.value.status_code == 410


@pytest.mark.asyncio
async def test_memory_query_alias_scope_queries_canonical_and_alias(monkeypatch):
    called_tenants = []
    captured_scope = {"value": None}

    class _FakeGraph:
        async def search_facts(self, **kwargs):
            called_tenants.append(kwargs.get("tenant_id"))
            return []

        async def search_nodes(self, **_kwargs):
            return []

    async def _stub_db_fetch(query, *args, **_kwargs):
        if "FROM user_model" in query:
            captured_scope["value"] = args[0]
            return []
        return []

    fake = _FakeGraph()
    graphiti_client.search_facts = fake.search_facts
    graphiti_client.search_nodes = fake.search_nodes
    monkeypatch.setattr("src.main.db.fetch", _stub_db_fetch, raising=False)

    from src.main import memory_query
    from src.models import MemoryQueryRequest

    resp = await memory_query(
        MemoryQueryRequest(
            tenantId="sophie-prod",
            userId="u",
            query="hospital",
            includeContext=False,
            limit=3,
        )
    )

    assert set(called_tenants) >= {"default", "sophie-prod"}
    assert captured_scope["value"] and set(captured_scope["value"]) >= {"default", "sophie-prod"}
    assert (resp.metadata or {}).get("tenantCanonical") == "default"


@pytest.mark.asyncio
async def test_memory_query_people_scope_filters_non_person_nodes(monkeypatch):
    class _FakeGraph:
        async def search_facts(self, **_kwargs):
            return []

        async def search_nodes(self, **kwargs):
            allowed_types = kwargs.get("allowed_types")
            if allowed_types == ["person"]:
                return [
                    {"summary": "Ashley", "type": "person", "uuid": "p1", "labels": ["Person"]},
                    {"summary": "Bluum", "type": "project", "uuid": "pr1", "labels": ["Project"]},
                    {"summary": "session_summary_1", "type": "SessionSummary", "uuid": "s1", "labels": ["SessionSummary"]},
                ]
            return []

    async def _stub_db_fetch(query, *args, **_kwargs):
        if "FROM user_model" in query:
            return []
        return []

    fake = _FakeGraph()
    graphiti_client.search_facts = fake.search_facts
    graphiti_client.search_nodes = fake.search_nodes
    monkeypatch.setattr("src.main.db.fetch", _stub_db_fetch, raising=False)

    from src.main import memory_query
    from src.models import MemoryQueryRequest

    resp = await memory_query(
        MemoryQueryRequest(
            tenantId="default",
            userId="u",
            query="important people in this user's life",
            includeContext=False,
            limit=5,
        )
    )

    assert [e.summary for e in resp.entities] == ["Ashley"]
    assert (resp.metadata or {}).get("rankingSignals", {}).get("ontologyNodeTypes") == ["person"]


@pytest.mark.asyncio
async def test_memory_query_episodic_mode_returns_ranked_episodes(monkeypatch):
    async def _stub_db_fetch(_query, *_args, **_kwargs):
        return []

    class _FakeGraph:
        async def search_facts(self, **_kwargs):
            return [{"text": "should be skipped in episodic mode", "relevance": 0.9, "source": "graphiti"}]

        async def search_nodes(self, **_kwargs):
            return [{"summary": "Jasmine", "type": "person", "uuid": "p1", "labels": ["Person"]}]

        async def get_recent_episodes(self, **_kwargs):
            return [
                {
                    "uuid": "ep-recent",
                    "name": "session_raw_recent",
                    "summary": "Conversation about life and spirituality.",
                    "content": "User: What was I saying about God the other day?\nAssistant: You were exploring life and spirituality deeply.",
                    "reference_time": "2026-04-09T20:00:00Z",
                },
                {
                    "uuid": "ep-older",
                    "name": "session_raw_old",
                    "summary": "Work update and deployment.",
                    "content": "User: Deployed a patch.",
                    "reference_time": "2026-03-10T20:00:00Z",
                },
            ]

    fake = _FakeGraph()
    graphiti_client.search_facts = fake.search_facts
    graphiti_client.search_nodes = fake.search_nodes
    graphiti_client.get_recent_episodes = fake.get_recent_episodes
    monkeypatch.setattr("src.main.db.fetch", _stub_db_fetch, raising=False)

    from src.main import memory_query
    from src.models import MemoryQueryRequest

    resp = await memory_query(
        MemoryQueryRequest(
            tenantId="default",
            userId="u",
            query="what was I saying about God the other day?",
            memoryIntent="episodic",
            limit=3,
        )
    )

    assert resp.facts == []
    assert resp.factItems == []
    assert len(resp.episodes) >= 1
    assert resp.episodes[0].episodeId == "ep-recent"
    assert any("god" in ev.lower() for ev in (resp.episodes[0].evidence or []))
    assert (resp.metadata or {}).get("memoryIntent") == "episodic"


@pytest.mark.asyncio
async def test_memory_query_exact_mode_skips_episodic_fetch(monkeypatch):
    calls = {"episodes": 0}

    async def _stub_db_fetch(_query, *_args, **_kwargs):
        return []

    class _FakeGraph:
        async def search_facts(self, **_kwargs):
            return [{"text": "Ashley is user's girlfriend.", "relevance": 0.9, "source": "graphiti"}]

        async def search_nodes(self, **_kwargs):
            return [{"summary": "Ashley", "type": "person", "uuid": "p1", "labels": ["Person"]}]

        async def get_recent_episodes(self, **_kwargs):
            calls["episodes"] += 1
            return []

    fake = _FakeGraph()
    graphiti_client.search_facts = fake.search_facts
    graphiti_client.search_nodes = fake.search_nodes
    graphiti_client.get_recent_episodes = fake.get_recent_episodes
    monkeypatch.setattr("src.main.db.fetch", _stub_db_fetch, raising=False)

    from src.main import memory_query
    from src.models import MemoryQueryRequest

    resp = await memory_query(
        MemoryQueryRequest(
            tenantId="default",
            userId="u",
            query="who is Ashley",
            memoryIntent="exact",
            limit=3,
        )
    )

    assert len(resp.facts) >= 1
    assert resp.episodes == []
    assert calls["episodes"] == 0


@pytest.mark.asyncio
async def test_memory_query_narrow_identity_scopes_to_direct_entity_facts(monkeypatch):
    async def _stub_semantic(items, **_kwargs):
        return [
            SimpleNamespace(
                domain="relationships",
                intent="ask_help",
                memory_type="relationship",
                domain_scores={"relationships": 0.9},
                confidence=0.8,
                classification_method="fallback",
            )
            for _ in items
        ]

    async def _stub_user_model_rows(**_kwargs):
        return [
            {
                "tenant_id": "default",
                "model": {
                    "narrative_current": "Long autobiographical blob about diaspora, spirituality, and many unrelated life themes.",
                    "key_relationships": [{"name": "Ashley", "who": "girlfriend"}],
                },
            }
        ]

    class _FakeGraph:
        async def search_facts(self, **_kwargs):
            return [
                {"text": "Diaspora is important to the user.", "relevance": 0.99, "source": "graphiti"},
                {"text": "Ashley is connected to spring and general reflection.", "relevance": 0.88, "source": "graphiti"},
            ]

        async def search_nodes(self, **_kwargs):
            return [
                {"summary": "Ashley", "type": "person", "uuid": "ash-1", "labels": ["Person"]},
                {"summary": "diaspora", "type": "project", "uuid": "dia-1", "labels": ["Project"]},
            ]

        async def get_canonical_entity_nodes(self, **_kwargs):
            return [
                {
                    "summary": "Ashley",
                    "name": "Ashley",
                    "type": "person",
                    "uuid": "ash-1",
                    "labels": ["Person"],
                    "attributes": {"profile_summary": "Ashley is a central person in the user's life."},
                }
            ]

        async def get_entity_role_grounding(self, **_kwargs):
            return {"role": "girlfriend", "relationship": "girlfriend", "edge_name": "RELATED_TO_USER_AS"}

        async def get_entity_facts_exact(self, **_kwargs):
            return [
                {"text": "Ashley is the user's girlfriend.", "relevance": 1.0, "source": "graphiti_exact"},
                {"text": "Ashley is an important relationship in the user's life.", "relevance": 0.95, "source": "graphiti_exact"},
            ]

    fake = _FakeGraph()
    graphiti_client.search_facts = fake.search_facts
    graphiti_client.search_nodes = fake.search_nodes
    graphiti_client.get_canonical_entity_nodes = fake.get_canonical_entity_nodes
    graphiti_client.get_entity_role_grounding = fake.get_entity_role_grounding
    graphiti_client.get_entity_facts_exact = fake.get_entity_facts_exact
    monkeypatch.setattr("src.main._fetch_user_model_rows_for_scope", _stub_user_model_rows, raising=True)
    monkeypatch.setattr("src.main.classify_memory_candidates_semantic", _stub_semantic, raising=True)

    from src.main import memory_query
    from src.models import MemoryQueryRequest

    resp = await memory_query(
        MemoryQueryRequest(
            tenantId="default",
            userId="u",
            query="who is Ashley?",
            memoryIntent="exact",
            limit=5,
        )
    )

    texts = [item.text for item in resp.factItems]
    assert texts
    assert "Ashley is the user's girlfriend." in texts
    assert all("diaspora" not in text.lower() for text in texts)
    assert all("blob" not in text.lower() for text in texts)
    assert len(resp.entities) == 1
    assert resp.entities[0].summary == "Ashley"
    assert (resp.metadata or {}).get("rankingSignals", {}).get("exactEntityScoped") is True
    assert (resp.metadata or {}).get("rankingSignals", {}).get("exactEntityTarget") == "Ashley"


@pytest.mark.asyncio
async def test_memory_query_narrow_project_identity_prefers_direct_project_grounding(monkeypatch):
    async def _stub_semantic(items, **_kwargs):
        return [
            SimpleNamespace(
                domain="work",
                intent="ask_help",
                memory_type="fact",
                domain_scores={"work": 0.8},
                confidence=0.8,
                classification_method="fallback",
            )
            for _ in items
        ]

    async def _stub_user_model_rows(**_kwargs):
        return []

    class _FakeGraph:
        async def search_facts(self, **_kwargs):
            return [{"text": "Bluum is discussed near unrelated finance history.", "relevance": 0.9, "source": "graphiti"}]

        async def search_nodes(self, **_kwargs):
            return [{"summary": "Bluum", "type": "project", "uuid": "blu-1", "labels": ["Project"]}]

        async def get_canonical_entity_nodes(self, **_kwargs):
            return [
                {
                    "summary": "Bluum",
                    "name": "Bluum",
                    "type": "project",
                    "uuid": "blu-1",
                    "labels": ["Project"],
                    "attributes": {"profile_summary": "Bluum is a project the user is actively building."},
                }
            ]

        async def get_entity_role_grounding(self, **_kwargs):
            return {"role": "active_project", "relationship": "active_project", "edge_name": "WORKING_ON"}

        async def get_entity_facts_exact(self, **_kwargs):
            return [{"text": "Bluum is a project the user is working on.", "relevance": 1.0, "source": "graphiti_exact"}]

    fake = _FakeGraph()
    graphiti_client.search_facts = fake.search_facts
    graphiti_client.search_nodes = fake.search_nodes
    graphiti_client.get_canonical_entity_nodes = fake.get_canonical_entity_nodes
    graphiti_client.get_entity_role_grounding = fake.get_entity_role_grounding
    graphiti_client.get_entity_facts_exact = fake.get_entity_facts_exact
    monkeypatch.setattr("src.main._fetch_user_model_rows_for_scope", _stub_user_model_rows, raising=True)
    monkeypatch.setattr("src.main.classify_memory_candidates_semantic", _stub_semantic, raising=True)

    from src.main import memory_query
    from src.models import MemoryQueryRequest

    resp = await memory_query(
        MemoryQueryRequest(
            tenantId="default",
            userId="u",
            query="what is Bluum?",
            memoryIntent="exact",
            limit=4,
        )
    )

    texts = [item.text for item in resp.factItems]
    assert texts
    assert "Bluum is a project the user is working on." in texts
    assert all("finance" not in text.lower() for text in texts)
    assert len(resp.entities) == 1
    assert resp.entities[0].summary == "Bluum"


@pytest.mark.asyncio
async def test_memory_query_broader_ashley_prompt_does_not_force_narrow_identity_scope(monkeypatch):
    async def _stub_db_fetch(_query, *_args, **_kwargs):
        return []

    async def _stub_user_model_rows(**_kwargs):
        return [
            {
                "tenant_id": "default",
                "model": {
                    "key_relationships": [{"name": "Ashley", "who": "girlfriend"}],
                    "narrative_current": "Ashley came up in recent conversation context.",
                },
            }
        ]

    class _FakeGraph:
        async def search_facts(self, **_kwargs):
            return [{"text": "Ashley is the user's girlfriend.", "relevance": 0.9, "source": "graphiti"}]

        async def search_nodes(self, **_kwargs):
            return [{"summary": "Ashley", "type": "person", "uuid": "ash-1", "labels": ["Person"]}]

        async def get_recent_episodes(self, **_kwargs):
            return []

    fake = _FakeGraph()
    graphiti_client.search_facts = fake.search_facts
    graphiti_client.search_nodes = fake.search_nodes
    graphiti_client.get_recent_episodes = fake.get_recent_episodes
    monkeypatch.setattr("src.main.db.fetch", _stub_db_fetch, raising=False)
    monkeypatch.setattr("src.main._fetch_user_model_rows_for_scope", _stub_user_model_rows, raising=True)

    from src.main import memory_query
    from src.models import MemoryQueryRequest

    resp = await memory_query(
        MemoryQueryRequest(
            tenantId="default",
            userId="u",
            query="what do you remember about Ashley lately?",
            memoryIntent="exact",
            limit=4,
        )
    )

    assert (resp.metadata or {}).get("rankingSignals", {}).get("exactEntityScoped") is False


@pytest.mark.asyncio
async def test_memory_query_episodic_embedding_signal_promotes_older_candidate(monkeypatch):
    async def _stub_db_fetch(_query, *_args, **_kwargs):
        return []

    async def _stub_embedding_candidates(**_kwargs):
        return [
            {
                "tenant_id": "default",
                "session_id": "s_old",
                "episode_uuid": "",
                "unit_text": "User: We explored life meaning and spiritual direction.",
                "embedding_similarity": 0.96,
            }
        ]

    class _FakeGraph:
        async def search_facts(self, **_kwargs):
            return []

        async def search_nodes(self, **_kwargs):
            return [{"summary": "Jasmine", "type": "person", "uuid": "p1", "labels": ["Person"]}]

        async def get_recent_episodes(self, **_kwargs):
            return [
                {
                    "uuid": "ep-recent",
                    "name": "session_raw_s_recent",
                    "summary": "Quick logistics check-in.",
                    "content": "User: groceries and schedule.",
                    "reference_time": "2026-04-09T22:00:00Z",
                },
                {
                    "uuid": "ep-old",
                    "name": "session_raw_s_old",
                    "summary": "Earlier discussion.",
                    "content": "User: We were exploring deeper ideas.",
                    "reference_time": "2026-03-01T10:00:00Z",
                },
            ]

    fake = _FakeGraph()
    graphiti_client.search_facts = fake.search_facts
    graphiti_client.search_nodes = fake.search_nodes
    graphiti_client.get_recent_episodes = fake.get_recent_episodes
    monkeypatch.setattr("src.main.db.fetch", _stub_db_fetch, raising=False)
    monkeypatch.setattr("src.main.search_episode_embedding_candidates", _stub_embedding_candidates, raising=True)

    from src.main import memory_query
    from src.models import MemoryQueryRequest

    resp = await memory_query(
        MemoryQueryRequest(
            tenantId="default",
            userId="u",
            query="remember that idea we were exploring before",
            memoryIntent="episodic",
            limit=3,
        )
    )

    assert len(resp.episodes) >= 1
    assert resp.episodes[0].sessionId == "s_old"
    assert any("spiritual" in ev.lower() or "meaning" in ev.lower() for ev in (resp.episodes[0].evidence or []))


@pytest.mark.asyncio
async def test_memory_query_episodic_diversifies_sessions(monkeypatch):
    async def _stub_db_fetch(_query, *_args, **_kwargs):
        return []

    async def _stub_embedding_candidates(**_kwargs):
        return [
            {"tenant_id": "default", "session_id": "s1", "episode_uuid": "", "unit_text": "User: life purpose talk", "embedding_similarity": 0.91},
            {"tenant_id": "default", "session_id": "s2", "episode_uuid": "", "unit_text": "User: jasmine future worries", "embedding_similarity": 0.90},
        ]

    async def _stub_embedding_stats(**_kwargs):
        return {"rows": 20, "sessions": 2}

    class _FakeGraph:
        async def search_facts(self, **_kwargs):
            return []

        async def search_nodes(self, **_kwargs):
            return []

        async def get_recent_episodes(self, **_kwargs):
            return [
                {"uuid": "ep1", "name": "session_raw_s1", "summary": "Life and spirituality", "content": "User: life purpose", "reference_time": "2026-04-09T22:00:00Z"},
                {"uuid": "ep1b", "name": "session_raw_s1", "summary": "More life and spirituality", "content": "User: meaning", "reference_time": "2026-04-09T21:55:00Z"},
                {"uuid": "ep2", "name": "session_raw_s2", "summary": "Jasmine future planning", "content": "User: jasmine future", "reference_time": "2026-04-08T20:00:00Z"},
            ]

    fake = _FakeGraph()
    graphiti_client.search_facts = fake.search_facts
    graphiti_client.search_nodes = fake.search_nodes
    graphiti_client.get_recent_episodes = fake.get_recent_episodes
    monkeypatch.setattr("src.main.db.fetch", _stub_db_fetch, raising=False)
    monkeypatch.setattr("src.main.search_episode_embedding_candidates", _stub_embedding_candidates, raising=True)
    monkeypatch.setattr("src.main.get_user_episodic_embedding_stats", _stub_embedding_stats, raising=True)

    from src.main import memory_query
    from src.models import MemoryQueryRequest

    resp = await memory_query(
        MemoryQueryRequest(
            tenantId="default",
            userId="u",
            query="remember what we discussed before",
            memoryIntent="episodic",
            limit=2,
        )
    )

    assert len(resp.episodes) == 2
    session_ids = [e.sessionId for e in resp.episodes]
    assert len(set(session_ids)) == 2


@pytest.mark.asyncio
async def test_memory_query_episodic_sets_weak_recall_flag_on_low_signal(monkeypatch):
    async def _stub_db_fetch(_query, *_args, **_kwargs):
        return []

    async def _stub_embedding_candidates(**_kwargs):
        return []

    async def _stub_embedding_stats(**_kwargs):
        return {"rows": 0, "sessions": 0}

    class _FakeGraph:
        async def search_facts(self, **_kwargs):
            return []

        async def search_nodes(self, **_kwargs):
            return []

        async def get_recent_episodes(self, **_kwargs):
            return [
                {"uuid": "ep1", "name": "session_raw_s1", "summary": "Random quick update", "content": "User: hi", "reference_time": "2026-04-09T22:00:00Z"},
            ]

    fake = _FakeGraph()
    graphiti_client.search_facts = fake.search_facts
    graphiti_client.search_nodes = fake.search_nodes
    graphiti_client.get_recent_episodes = fake.get_recent_episodes
    monkeypatch.setattr("src.main.db.fetch", _stub_db_fetch, raising=False)
    monkeypatch.setattr("src.main.search_episode_embedding_candidates", _stub_embedding_candidates, raising=True)
    monkeypatch.setattr("src.main.get_user_episodic_embedding_stats", _stub_embedding_stats, raising=True)

    from src.main import memory_query
    from src.models import MemoryQueryRequest

    resp = await memory_query(
        MemoryQueryRequest(
            tenantId="default",
            userId="u",
            query="remember the deep spiritual thread",
            memoryIntent="episodic",
            limit=3,
        )
    )

    ranking = (resp.metadata or {}).get("episodicRanking") or {}
    audit = ranking.get("audit") if isinstance(ranking, dict) else {}
    assert (resp.metadata or {}).get("episodicWeakRecall") is True
    assert (audit or {}).get("weakRecall") is True


@pytest.mark.asyncio
async def test_get_user_model_reads_alias_scope(monkeypatch):
    async def _stub_db_fetch(query, *args, **_kwargs):
        if "FROM user_model" in query:
            scope = args[0]
            if set(scope) >= {"default", "sophie-prod"}:
                return [
                    {
                        "tenant_id": "default",
                        "model": {"current_focus": {"text": "Recover from kidney stones"}},
                        "version": 2,
                        "created_at": None,
                        "updated_at": None,
                        "last_source": "updater",
                        "narrative_stable": None,
                        "narrative_current": None,
                    }
                ]
        return []

    monkeypatch.setattr("src.main.db.fetch", _stub_db_fetch, raising=False)

    from src.main import get_user_model

    resp = await get_user_model(tenantId="sophie-prod", userId="u")
    assert resp.exists is True
    assert resp.tenantId == "default"
    assert (resp.metadata or {}).get("sourceTenant") == "default"
