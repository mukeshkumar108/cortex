import asyncio
import os
from datetime import datetime
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
async def test_memory_query_uses_graphiti():
    class _FakeGraph:
        async def search_facts(self, **_kwargs):
            return [{"text": "User likes birds", "relevance": 0.9, "source": "graphiti"}]

        async def search_nodes(self, **_kwargs):
            return [{"summary": "Ashley", "type": "Entity", "labels": ["Entity"], "uuid": "u1"}]

    fake = _FakeGraph()
    graphiti_client.search_facts = fake.search_facts
    graphiti_client.search_nodes = fake.search_nodes

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
async def test_memory_query_surfaces_session_summary_and_user_model_with_provenance(monkeypatch):
    class _FakeGraph:
        async def search_facts(self, **kwargs):
            tenant_id = kwargs.get("tenant_id")
            if tenant_id == "default":
                return [{"text": "User recently left hospital", "relevance": 0.55, "source": "graphiti"}]
            return []

        async def search_nodes(self, **kwargs):
            tenant_id = kwargs.get("tenant_id")
            if tenant_id != "default":
                return []
            return [
                {
                    "summary": "session summary",
                    "type": "SessionSummary",
                    "labels": ["SessionSummary"],
                    "attributes": {
                        "summary_facts": "Recently out of hospital with kidney stones.",
                        "reference_time": "2026-04-08T09:00:00Z",
                    },
                }
            ]

    async def _stub_db_fetch(query, *args, **_kwargs):
        if "FROM user_model" in query:
            return [
                {
                    "tenant_id": "default",
                    "model": {
                        "narrative_current": "Recently out of hospital with kidney stones.",
                        "recent_signals": [{"text": "Hydration helps with kidney stone recovery."}],
                    },
                    "version": 3,
                    "created_at": None,
                    "updated_at": None,
                    "last_source": "enrichment",
                    "narrative_stable": None,
                    "narrative_current": None,
                }
            ]
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
            query="why did I go to hospital",
            includeContext=False,
            limit=5,
        )
    )

    lower_facts = [f.lower() for f in resp.facts]
    assert any("kidney stones" in text for text in lower_facts)
    sources = {item.source for item in resp.factItems}
    assert "graphiti_session_summary" in sources or "user_model" in sources
    assert "provenanceCounts" in (resp.metadata or {})
    assert not any((e.type or "").lower() == "sessionsummary" for e in resp.entities)


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
