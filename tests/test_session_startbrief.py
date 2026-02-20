from datetime import datetime, timedelta, timezone

import pytest
from httpx import ASGITransport, AsyncClient

from src.main import app, graphiti_client
from src import loops as loops_module
from src.models import Loop


def _base_summary(now_iso: str, session_id: str = "s1", salience: str = "high", bridge: str = "They just agreed to continue this thread.") -> dict:
    return {
        "session_id": session_id,
        "created_at": now_iso,
        "summary_text": "They reviewed the same thread and narrowed the next focus.",
        "bridge_text": bridge,
        "attributes": {
            "session_id": session_id,
            "summary_facts": "They reviewed the same thread and narrowed the next focus.",
            "tone": "They sounded steady.",
            "moment": "A clear next step was named.",
            "decisions": ["Continue the same thread"],
            "unresolved": ["Confirm progress next session"],
            "salience": salience,
            "reference_time": now_iso,
        },
    }


def _loop(text: str, salience: int = 5, horizon: str = "today", now_iso: str = "2026-02-06T10:15:00Z") -> Loop:
    return Loop(
        id="00000000-0000-0000-0000-000000000001",
        type="thread",
        status="active",
        text=text,
        confidence=0.8,
        salience=salience,
        timeHorizon=horizon,
        sourceTurnTs=None,
        dueDate=None,
        entityRefs=[],
        tags=[],
        createdAt=now_iso,
        updatedAt=None,
        lastSeenAt=now_iso,
        metadata={},
    )


async def _make_request(client: AsyncClient, now: str) -> dict:
    resp = await client.get(
        "/session/startbrief",
        params={
            "tenantId": "t",
            "userId": "u",
            "now": now,
            "sessionId": "session-test",
            "timezone": "UTC",
        },
    )
    assert resp.status_code == 200
    return resp.json()


@pytest.mark.asyncio
async def test_startbrief_continuation_uses_bridge_within_ttl(monkeypatch):
    now_dt = datetime(2026, 2, 6, 10, 15, tzinfo=timezone.utc)
    now = now_dt.isoformat().replace("+00:00", "Z")
    summary = _base_summary((now_dt - timedelta(minutes=10)).isoformat().replace("+00:00", "Z"))

    async def _stub_latest_summary_node(*_args, **_kwargs):
        return summary

    async def _stub_recent_session_summary_nodes(*_args, **_kwargs):
        return [summary]

    async def _stub_get_top_loops(*_args, **_kwargs):
        return [_loop("Finish portfolio site today", salience=6, horizon="today", now_iso=now)]

    async def _stub_db_fetchone(query, *_args, **_kwargs):
        if "FROM session_transcript" in query:
            return {"messages": [{"role": "user", "text": "last", "timestamp": (now_dt - timedelta(minutes=10)).isoformat()}]}
        if "FROM daily_analysis" in query:
            return {"analysis_date": "2026-02-05", "themes": ["avoidance"], "steering_note": "Do not put this in prose."}
        if "FROM user_model" in query:
            return None
        return None

    async def _stub_bridge_llm(*_args, **_kwargs):
        return "They just left off confirming the same thread."

    graphiti_client.get_latest_session_summary_node = _stub_latest_summary_node
    graphiti_client.get_recent_session_summary_nodes = _stub_recent_session_summary_nodes
    async def _stub_search_nodes(*_args, **_kwargs):
        return []
    graphiti_client.search_nodes = _stub_search_nodes
    monkeypatch.setattr(loops_module, "get_top_loops_for_startbrief", _stub_get_top_loops, raising=True)
    monkeypatch.setattr("src.main.db.fetchone", _stub_db_fetchone, raising=False)
    monkeypatch.setattr("src.main._generate_startbrief_bridge_llm", _stub_bridge_llm, raising=True)

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            data = await _make_request(client, now)
            assert data["handover_depth"] == "continuation"
            assert data["resume"]["use_bridge"] is True
            assert data["resume"]["bridge_text"]
            assert len(data["handover_text"].split()) <= 35
            text = data["handover_text"].lower()
            assert "the user" in text
            assert "you last spoke" in text
            assert "conversation today" in text
            assert "tone:" not in text
            assert "user tone:" not in text
            assert "open threads:" not in text


@pytest.mark.asyncio
async def test_startbrief_today_depth_no_advice_words(monkeypatch):
    now_dt = datetime(2026, 2, 6, 10, 15, tzinfo=timezone.utc)
    now = now_dt.isoformat().replace("+00:00", "Z")
    summary = _base_summary((now_dt - timedelta(hours=2)).isoformat().replace("+00:00", "Z"), salience="medium")

    async def _stub_latest_summary_node(*_args, **_kwargs):
        return summary

    async def _stub_recent_session_summary_nodes(*_args, **_kwargs):
        return [summary]

    async def _stub_get_top_loops(*_args, **_kwargs):
        return [
            _loop("Go outside together after coffee", salience=5, horizon="today", now_iso=now),
            _loop("Finish portfolio site today", salience=6, horizon="today", now_iso=now),
        ]

    async def _stub_db_fetchone(query, *_args, **_kwargs):
        if "FROM session_transcript" in query:
            return {"messages": [{"role": "user", "text": "last", "timestamp": (now_dt - timedelta(hours=2)).isoformat()}]}
        if "FROM daily_analysis" in query:
            return {"analysis_date": "2026-02-05", "themes": ["avoidance"], "steering_note": "Do not put this in prose."}
        return None

    async def _stub_bridge_llm(*_args, **_kwargs):
        return "Earlier today they tightened the thread and kept momentum on finishing the portfolio update."

    graphiti_client.get_latest_session_summary_node = _stub_latest_summary_node
    graphiti_client.get_recent_session_summary_nodes = _stub_recent_session_summary_nodes
    async def _stub_search_nodes(*_args, **_kwargs):
        return []
    graphiti_client.search_nodes = _stub_search_nodes
    monkeypatch.setattr(loops_module, "get_top_loops_for_startbrief", _stub_get_top_loops, raising=True)
    monkeypatch.setattr("src.main.db.fetchone", _stub_db_fetchone, raising=False)
    monkeypatch.setattr("src.main._generate_startbrief_bridge_llm", _stub_bridge_llm, raising=True)

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            data = await _make_request(client, now)
            assert data["handover_depth"] == "today"
            assert all(w not in data["handover_text"].lower() for w in [" should ", " could ", " try ", " maybe "])
            assert "the user" in data["handover_text"].lower()
            assert "you seemed" not in data["handover_text"].lower()
            assert "you felt" not in data["handover_text"].lower()
            assert "tone:" not in data["handover_text"].lower()
            assert "open threads:" not in data["handover_text"].lower()
            assert "after coffee" not in " ".join(l["text"].lower() for l in data["ops_context"]["top_loops_today"])


@pytest.mark.asyncio
async def test_startbrief_multi_day_cap_and_no_hallucinated_continuity(monkeypatch):
    now_dt = datetime(2026, 2, 8, 10, 15, tzinfo=timezone.utc)
    now = now_dt.isoformat().replace("+00:00", "Z")
    summary = _base_summary((now_dt - timedelta(days=2)).isoformat().replace("+00:00", "Z"), salience="high")

    async def _stub_latest_summary_node(*_args, **_kwargs):
        return summary

    async def _stub_recent_session_summary_nodes(*_args, **_kwargs):
        return [summary]

    async def _stub_get_top_loops(*_args, **_kwargs):
        return [_loop("Finish portfolio site", salience=6, horizon="ongoing", now_iso=now)]

    async def _stub_db_fetchone(query, *_args, **_kwargs):
        if "FROM session_transcript" in query:
            return {"messages": [{"role": "user", "text": "old", "timestamp": (now_dt - timedelta(days=2)).isoformat()}]}
        if "FROM daily_analysis" in query:
            return {"analysis_date": "2026-02-07", "themes": ["avoidance"], "steering_note": "Do not put this in prose."}
        return None

    async def _stub_bridge_llm(*_args, **_kwargs):
        return "Over the last couple of days the same thread kept returning, and coffee is still ready."

    graphiti_client.get_latest_session_summary_node = _stub_latest_summary_node
    graphiti_client.get_recent_session_summary_nodes = _stub_recent_session_summary_nodes
    async def _stub_search_nodes(*_args, **_kwargs):
        return []
    graphiti_client.search_nodes = _stub_search_nodes
    monkeypatch.setattr(loops_module, "get_top_loops_for_startbrief", _stub_get_top_loops, raising=True)
    monkeypatch.setattr("src.main.db.fetchone", _stub_db_fetchone, raising=False)
    monkeypatch.setattr("src.main._generate_startbrief_bridge_llm", _stub_bridge_llm, raising=True)

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            data = await _make_request(client, now)
            assert data["handover_depth"] == "multi_day"
            assert len(data["handover_text"].split()) <= 120
            assert "coffee still ready" not in data["handover_text"].lower()
            assert "20" not in data["handover_text"]
            assert "At " not in data["handover_text"]
            assert "were reported" not in data["handover_text"].lower()
            assert "the user" in data["handover_text"].lower()


@pytest.mark.asyncio
async def test_startbrief_steering_note_stays_out_of_handover(monkeypatch):
    now_dt = datetime(2026, 2, 6, 10, 15, tzinfo=timezone.utc)
    now = now_dt.isoformat().replace("+00:00", "Z")
    summary = _base_summary((now_dt - timedelta(hours=9)).isoformat().replace("+00:00", "Z"), salience="high")

    async def _stub_latest_summary_node(*_args, **_kwargs):
        return summary

    async def _stub_recent_session_summary_nodes(*_args, **_kwargs):
        return [summary]

    async def _stub_get_top_loops(*_args, **_kwargs):
        return [_loop("Finish portfolio site", salience=6, horizon="today", now_iso=now)]

    steering = "User talked about the walk three times but did not do it."

    async def _stub_db_fetchone(query, *_args, **_kwargs):
        if "FROM session_transcript" in query:
            return {"messages": [{"role": "user", "text": "last", "timestamp": (now_dt - timedelta(hours=9)).isoformat()}]}
        if "FROM daily_analysis" in query:
            return {"analysis_date": "2026-02-05", "themes": ["avoidance"], "steering_note": steering}
        return None

    async def _stub_bridge_llm(*_args, **_kwargs):
        return "Yesterday the same thread stayed active and they returned to finishing the portfolio update."

    graphiti_client.get_latest_session_summary_node = _stub_latest_summary_node
    graphiti_client.get_recent_session_summary_nodes = _stub_recent_session_summary_nodes
    async def _stub_search_nodes(*_args, **_kwargs):
        return []
    graphiti_client.search_nodes = _stub_search_nodes
    monkeypatch.setattr(loops_module, "get_top_loops_for_startbrief", _stub_get_top_loops, raising=True)
    monkeypatch.setattr("src.main.db.fetchone", _stub_db_fetchone, raising=False)
    monkeypatch.setattr("src.main._generate_startbrief_bridge_llm", _stub_bridge_llm, raising=True)

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            data = await _make_request(client, now)
            assert steering.lower() not in data["handover_text"].lower()
            assert data["ops_context"]["steering_note"] == steering


@pytest.mark.asyncio
async def test_startbrief_advice_output_triggers_fallback(monkeypatch):
    now_dt = datetime(2026, 2, 6, 10, 15, tzinfo=timezone.utc)
    now = now_dt.isoformat().replace("+00:00", "Z")
    summary = _base_summary((now_dt - timedelta(hours=2)).isoformat().replace("+00:00", "Z"), salience="high")

    async def _stub_latest_summary_node(*_args, **_kwargs):
        return summary

    async def _stub_recent_session_summary_nodes(*_args, **_kwargs):
        return [summary]

    async def _stub_get_top_loops(*_args, **_kwargs):
        return [_loop("Finish portfolio site", salience=6, horizon="today", now_iso=now)]

    async def _stub_db_fetchone(query, *_args, **_kwargs):
        if "FROM session_transcript" in query:
            return {"messages": [{"role": "user", "text": "last", "timestamp": (now_dt - timedelta(hours=2)).isoformat()}]}
        if "FROM daily_analysis" in query:
            return {"analysis_date": "2026-02-05", "themes": [], "steering_note": None}
        return None

    calls = {"n": 0}

    async def _stub_bridge_llm(*_args, **_kwargs):
        calls["n"] += 1
        return "They should probably try to keep going."

    graphiti_client.get_latest_session_summary_node = _stub_latest_summary_node
    graphiti_client.get_recent_session_summary_nodes = _stub_recent_session_summary_nodes
    async def _stub_search_nodes(*_args, **_kwargs):
        return []
    graphiti_client.search_nodes = _stub_search_nodes
    monkeypatch.setattr(loops_module, "get_top_loops_for_startbrief", _stub_get_top_loops, raising=True)
    monkeypatch.setattr("src.main.db.fetchone", _stub_db_fetchone, raising=False)
    monkeypatch.setattr("src.main._generate_startbrief_bridge_llm", _stub_bridge_llm, raising=True)

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            data = await _make_request(client, now)
            assert "should" not in data["handover_text"].lower()
            assert "try" not in data["handover_text"].lower()
            assert "ready to proceed" not in data["handover_text"].lower()
            assert "next tasks" not in data["handover_text"].lower()
            assert "the user" in data["handover_text"].lower()
            assert "you felt" not in data["handover_text"].lower()
            assert ".with" not in data["handover_text"].lower()
            assert "tone:" not in data["handover_text"].lower()
            assert "open threads:" not in data["handover_text"].lower()
            assert calls["n"] >= 1


@pytest.mark.asyncio
async def test_startbrief_bureaucratic_output_rewritten_or_fallback(monkeypatch):
    now_dt = datetime(2026, 2, 6, 10, 15, tzinfo=timezone.utc)
    now = now_dt.isoformat().replace("+00:00", "Z")
    summary = _base_summary((now_dt - timedelta(hours=2)).isoformat().replace("+00:00", "Z"), salience="high")

    async def _stub_latest_summary_node(*_args, **_kwargs):
        return summary

    async def _stub_recent_session_summary_nodes(*_args, **_kwargs):
        return [summary]

    async def _stub_get_top_loops(*_args, **_kwargs):
        return [_loop("Finish portfolio site", salience=6, horizon="today", now_iso=now)]

    async def _stub_db_fetchone(query, *_args, **_kwargs):
        if "FROM session_transcript" in query:
            return {"messages": [{"role": "user", "text": "last", "timestamp": (now_dt - timedelta(hours=2)).isoformat()}]}
        if "FROM daily_analysis" in query:
            return {"analysis_date": "2026-02-05", "themes": [], "steering_note": None}
        return None

    calls = {"n": 0}

    async def _stub_bridge_llm(*_args, **_kwargs):
        calls["n"] += 1
        return "At 11:43, on February 20, 2026, details were reported and they are ready to proceed with next tasks."

    graphiti_client.get_latest_session_summary_node = _stub_latest_summary_node
    graphiti_client.get_recent_session_summary_nodes = _stub_recent_session_summary_nodes
    async def _stub_search_nodes(*_args, **_kwargs):
        return []
    graphiti_client.search_nodes = _stub_search_nodes
    monkeypatch.setattr(loops_module, "get_top_loops_for_startbrief", _stub_get_top_loops, raising=True)
    monkeypatch.setattr("src.main.db.fetchone", _stub_db_fetchone, raising=False)
    monkeypatch.setattr("src.main._generate_startbrief_bridge_llm", _stub_bridge_llm, raising=True)

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            data = await _make_request(client, now)
            text = data["handover_text"]
            assert "at " not in text.lower() or ":" not in text
            assert "2026" not in text
            assert "were reported" not in text.lower()
            assert "ready to proceed" not in text.lower()
            assert "next tasks" not in text.lower()
            assert "worked out just fine" not in text.lower()
            assert "big events" not in text.lower()
            assert "you seemed" not in text.lower()
            assert "you felt" not in text.lower()
            assert "the user" in text.lower()
            assert ".with" not in text.lower()
            assert calls["n"] >= 1


@pytest.mark.asyncio
async def test_startbrief_normalizes_bad_graphiti_shape_placeholders(monkeypatch):
    now_dt = datetime(2026, 2, 19, 16, 35, tzinfo=timezone.utc)
    now = now_dt.isoformat().replace("+00:00", "Z")

    bad_shape = {
        "name": "name",
        "summary": "summary",
        "summary_text": "summary_text",
        "bridge_text": "bridge_text",
        "session_id": "session_id",
        "created_at": "created_at",
        "attributes": {
            "session_id": "real-session-42",
            "summary_text": "The user felt stressed and overwhelmed after a long workday.",
            "tone": "The user sounded stressed.",
            "created_at": "2026-02-19T16:26:22.969717+00:00",
            "salience": "high",
            "reference_time": "2026-02-19T16:26:22.969717+00:00",
        },
    }

    async def _stub_latest_summary_node(*_args, **_kwargs):
        return bad_shape

    async def _stub_recent_session_summary_nodes(*_args, **_kwargs):
        return [bad_shape]

    async def _stub_get_top_loops(*_args, **_kwargs):
        return [_loop("Complete checkpoint changes", salience=6, horizon="today", now_iso=now)]

    async def _stub_db_fetchone(query, *_args, **_kwargs):
        if "count(*) AS sessions_today" in query:
            return {"sessions_today": 1}
        if "FROM session_transcript" in query:
            return {"messages": [{"role": "user", "text": "last", "timestamp": "2026-02-19T16:26:25+00:00"}]}
        if "FROM daily_analysis" in query:
            return {"analysis_date": "2026-02-18", "themes": [], "steering_note": None}
        return None

    async def _stub_bridge_llm(*_args, **kwargs):
        ingredients = kwargs.get("narrative_ingredients") or {}
        thread = ingredients.get("last_thread") or "ongoing"
        return (
            "It is Thursday this afternoon. You last spoke with the user about 2 hours ago. "
            "This is the 2nd conversation today. "
            f"The latest thread is {thread}."
        )

    graphiti_client.get_latest_session_summary_node = _stub_latest_summary_node
    graphiti_client.get_recent_session_summary_nodes = _stub_recent_session_summary_nodes
    async def _stub_search_nodes(*_args, **_kwargs):
        return []
    graphiti_client.search_nodes = _stub_search_nodes
    monkeypatch.setattr(loops_module, "get_top_loops_for_startbrief", _stub_get_top_loops, raising=True)
    monkeypatch.setattr("src.main.db.fetchone", _stub_db_fetchone, raising=False)
    monkeypatch.setattr("src.main._generate_startbrief_bridge_llm", _stub_bridge_llm, raising=True)

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            data = await _make_request(client, now)
            text = data["handover_text"].lower()
            assert "latest thread is summary_text" not in text
            assert data["evidence"]["session_summary_ids_used"] == ["real-session-42"]
            assert "the user" in text


@pytest.mark.asyncio
async def test_startbrief_evidence_ids_not_empty_when_nodes_fetched(monkeypatch):
    now_dt = datetime(2026, 2, 19, 16, 35, tzinfo=timezone.utc)
    now = now_dt.isoformat().replace("+00:00", "Z")
    empty_node = {
        "session_id": "real-empty-1",
        "created_at": "2026-02-19T16:26:22.969717+00:00",
        "summary_text": "",
        "summary": "",
        "attributes": {"session_id": "real-empty-1"},
    }

    async def _stub_latest_summary_node(*_args, **_kwargs):
        return empty_node

    async def _stub_recent_session_summary_nodes(*_args, **_kwargs):
        return [empty_node]

    async def _stub_refetch(*_args, **_kwargs):
        return []

    async def _stub_get_top_loops(*_args, **_kwargs):
        return [_loop("Complete checkpoint changes", salience=6, horizon="today", now_iso=now)]

    async def _stub_db_fetchone(query, *_args, **_kwargs):
        if "count(*) AS sessions_today" in query:
            return {"sessions_today": 1}
        if "FROM session_transcript" in query:
            return {"messages": [{"role": "user", "text": "last", "timestamp": "2026-02-19T16:26:25+00:00"}]}
        if "FROM daily_analysis" in query:
            return {"analysis_date": "2026-02-18", "themes": [], "steering_note": None}
        return None

    async def _stub_bridge_llm(*_args, **_kwargs):
        return "It is Thursday this afternoon. You last spoke with the user about 2 hours ago. This is the 2nd conversation today."

    graphiti_client.get_latest_session_summary_node = _stub_latest_summary_node
    graphiti_client.get_recent_session_summary_nodes = _stub_recent_session_summary_nodes
    graphiti_client.get_session_summary_nodes_by_ids = _stub_refetch
    async def _stub_search_nodes(*_args, **_kwargs):
        return []
    graphiti_client.search_nodes = _stub_search_nodes
    monkeypatch.setattr(loops_module, "get_top_loops_for_startbrief", _stub_get_top_loops, raising=True)
    monkeypatch.setattr("src.main.db.fetchone", _stub_db_fetchone, raising=False)
    monkeypatch.setattr("src.main._generate_startbrief_bridge_llm", _stub_bridge_llm, raising=True)

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            data = await _make_request(client, now)
            assert data["evidence"]["session_summary_ids_used"] == ["real-empty-1"]
            assert data["evidence"]["summary_content_quality"] == "empty_after_normalization"
            assert data["evidence"]["summary_fetch_count"] >= 1


@pytest.mark.asyncio
async def test_startbrief_fallback_properties_refetch_populates_content(monkeypatch):
    now_dt = datetime(2026, 2, 19, 16, 35, tzinfo=timezone.utc)
    now = now_dt.isoformat().replace("+00:00", "Z")
    empty_node = {
        "session_id": "real-fallback-1",
        "created_at": "2026-02-19T16:26:22.969717+00:00",
        "summary_text": "",
        "summary": "",
        "attributes": {"session_id": "real-fallback-1"},
    }
    refetched_node = {
        "props": {
            "session_id": "real-fallback-1",
            "created_at": "2026-02-19T16:26:22.969717+00:00",
            "summary_text": "The user was stressed after a long day and discussed checkpoint progress.",
            "attributes": {
                "session_id": "real-fallback-1",
                "summary_text": "The user was stressed after a long day and discussed checkpoint progress.",
                "tone": "The user sounded stressed.",
                "salience": "high",
                "reference_time": "2026-02-19T16:26:22.969717+00:00",
            },
        }
    }

    async def _stub_latest_summary_node(*_args, **_kwargs):
        return empty_node

    async def _stub_recent_session_summary_nodes(*_args, **_kwargs):
        return [empty_node]

    async def _stub_refetch(*_args, **_kwargs):
        return [refetched_node]

    async def _stub_get_top_loops(*_args, **_kwargs):
        return [_loop("Complete checkpoint changes", salience=6, horizon="today", now_iso=now)]

    async def _stub_db_fetchone(query, *_args, **_kwargs):
        if "count(*) AS sessions_today" in query:
            return {"sessions_today": 1}
        if "FROM session_transcript" in query:
            return {"messages": [{"role": "user", "text": "last", "timestamp": "2026-02-19T16:26:25+00:00"}]}
        if "FROM daily_analysis" in query:
            return {"analysis_date": "2026-02-18", "themes": [], "steering_note": None}
        return None

    async def _stub_bridge_llm(*_args, **kwargs):
        ingredients = kwargs.get("narrative_ingredients") or {}
        thread = ingredients.get("last_thread") or "ongoing"
        return (
            "It is Thursday this afternoon. You last spoke with the user about 2 hours ago. "
            "This is the 2nd conversation today. "
            f"The latest thread is {thread}."
        )

    graphiti_client.get_latest_session_summary_node = _stub_latest_summary_node
    graphiti_client.get_recent_session_summary_nodes = _stub_recent_session_summary_nodes
    graphiti_client.get_session_summary_nodes_by_ids = _stub_refetch
    async def _stub_search_nodes(*_args, **_kwargs):
        return []
    graphiti_client.search_nodes = _stub_search_nodes
    monkeypatch.setattr(loops_module, "get_top_loops_for_startbrief", _stub_get_top_loops, raising=True)
    monkeypatch.setattr("src.main.db.fetchone", _stub_db_fetchone, raising=False)
    monkeypatch.setattr("src.main._generate_startbrief_bridge_llm", _stub_bridge_llm, raising=True)

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            data = await _make_request(client, now)
            assert data["evidence"]["fallback_used"] is True
            assert data["evidence"]["fallback_success"] is True
            assert data["evidence"]["session_summary_ids_used"] == ["real-fallback-1"]
            assert "latest thread is the user was stressed after a long day" in data["handover_text"].lower()


@pytest.mark.asyncio
async def test_startbrief_fallback_bounded_and_soft_fail(monkeypatch):
    now_dt = datetime(2026, 2, 19, 16, 35, tzinfo=timezone.utc)
    now = now_dt.isoformat().replace("+00:00", "Z")
    empty_node = {
        "session_id": "real-fallback-fail-1",
        "created_at": "2026-02-19T16:26:22.969717+00:00",
        "summary_text": "",
        "summary": "",
        "attributes": {"session_id": "real-fallback-fail-1"},
    }

    async def _stub_latest_summary_node(*_args, **_kwargs):
        return empty_node

    async def _stub_recent_session_summary_nodes(*_args, **_kwargs):
        return [empty_node]

    async def _stub_refetch(*_args, **_kwargs):
        raise RuntimeError("fallback failed")

    async def _stub_get_top_loops(*_args, **_kwargs):
        return [_loop("Complete checkpoint changes", salience=6, horizon="today", now_iso=now)]

    async def _stub_db_fetchone(query, *_args, **_kwargs):
        if "count(*) AS sessions_today" in query:
            return {"sessions_today": 1}
        if "FROM session_transcript" in query:
            return {"messages": [{"role": "user", "text": "last", "timestamp": "2026-02-19T16:26:25+00:00"}]}
        if "FROM daily_analysis" in query:
            return {"analysis_date": "2026-02-18", "themes": [], "steering_note": None}
        return None

    async def _stub_bridge_llm(*_args, **_kwargs):
        return "It is Thursday this afternoon. You last spoke with the user about 2 hours ago. This is the 2nd conversation today."

    graphiti_client.get_latest_session_summary_node = _stub_latest_summary_node
    graphiti_client.get_recent_session_summary_nodes = _stub_recent_session_summary_nodes
    graphiti_client.get_session_summary_nodes_by_ids = _stub_refetch
    async def _stub_search_nodes(*_args, **_kwargs):
        return []
    graphiti_client.search_nodes = _stub_search_nodes
    monkeypatch.setattr(loops_module, "get_top_loops_for_startbrief", _stub_get_top_loops, raising=True)
    monkeypatch.setattr("src.main.db.fetchone", _stub_db_fetchone, raising=False)
    monkeypatch.setattr("src.main._generate_startbrief_bridge_llm", _stub_bridge_llm, raising=True)

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            data = await _make_request(client, now)
            assert data["handover_text"]
            assert data["evidence"]["fallback_used"] is True
            assert data["evidence"]["fallback_success"] is False
