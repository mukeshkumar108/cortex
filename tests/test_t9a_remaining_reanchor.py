from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src import loops as loops_module
from src.main import (
    _pg_get_entity_role_hint,
    _pg_search_nodes,
    _generate_startbrief_bridge_llm,
    _get_session_ingest_freshness,
    _log_startbrief_history,
    graphiti_client,
    session_startbrief,
)


@pytest.mark.asyncio
async def test_t9a_role_hint_falls_back_to_canonical_when_derived_missing(monkeypatch):
    async def _stub_fetchone(query, *args, **kwargs):
        if "FROM entity_profiles" in query:
            return None
        if "FROM entities" in query:
            return {"entity_id": "2ea8c5b7-c6d0-4f56-8f3e-8fe62f69f65e", "canonical_name": "Ashley", "entity_type": "person"}
        if "FROM claims" in query:
            return {"predicate": "relationship.role", "object_payload": {"value": "partner"}, "truth_confidence": 0.84}
        return None

    monkeypatch.setattr("src.main.db.fetchone", _stub_fetchone, raising=False)

    hint = await _pg_get_entity_role_hint(
        tenant_id="default",
        user_id="u1",
        name="Ashley",
        entity_id=None,
    )

    assert hint["entity_name"] == "Ashley"
    assert hint["role"] == "partner"
    assert hint["source"] == "canonical_entities_claims"
    assert hint["derived"] is False
    assert hint["evidence_backed"] is True


@pytest.mark.asyncio
async def test_t9a_search_nodes_includes_canonical_fallback(monkeypatch):
    now = datetime.now(timezone.utc)

    async def _stub_fetch(query, *args, **kwargs):
        if "FROM entity_profiles" in query:
            return []
        if "FROM open_threads" in query:
            return []
        if "FROM session_classifications" in query:
            return []
        if "FROM entities" in query and "canonical_name_normalized" in query:
            return [
                {
                    "tenant_id": "default",
                    "entity_id": "2ea8c5b7-c6d0-4f56-8f3e-8fe62f69f65e",
                    "canonical_name": "Ashley",
                    "entity_type": "person",
                    "created_at": now,
                    "updated_at": now,
                }
            ]
        if "FROM claims c" in query and "claim_event_key" in query:
            return [
                {
                    "claim_event_key": "cek_v1_test",
                    "predicate": "relationship.status",
                    "object_payload": {"value": "dating"},
                    "truth_confidence": 0.9,
                    "updated_at": now,
                    "subject_text": "Ashley",
                    "subject_name": "Ashley",
                    "evidence_count": 1,
                }
            ]
        return []

    monkeypatch.setattr("src.main.db.fetch", _stub_fetch, raising=False)

    rows = await _pg_search_nodes(
        tenant_id="default",
        user_id="u1",
        query="ashley dating",
        limit=10,
        reference_time=now,
    )

    sources = {((row.get("attributes") or {}).get("source")) for row in rows}
    assert "canonical_entities" in sources
    assert "canonical_claims" in sources


@pytest.mark.asyncio
async def test_t9a_startbrief_emits_canonical_provenance(monkeypatch):
    now_dt = datetime(2026, 2, 6, 10, 15, tzinfo=timezone.utc)
    now = now_dt.isoformat().replace("+00:00", "Z")

    async def _stub_latest_summary_node(*_args, **_kwargs):
        return None

    async def _stub_recent_session_summary_nodes(*_args, **_kwargs):
        return []

    async def _stub_recent_episode_summaries(*_args, **_kwargs):
        return []

    async def _stub_get_top_loops(*_args, **_kwargs):
        return []

    async def _stub_db_fetchone(query, *_args, **_kwargs):
        if "FROM session_transcript" in query:
            return {"messages": [{"role": "user", "text": "last", "timestamp": (now_dt).isoformat()}]}
        if "count(*) AS sessions_today" in query:
            return {"sessions_today": 0}
        if "FROM daily_analysis" in query:
            return None
        if "FROM user_model" in query:
            return None
        return None

    async def _stub_db_fetch(query, *_args, **_kwargs):
        if "FROM user_model" in query:
            return []
        if "FROM claims c" in query and "COUNT(ce.claim_evidence_id)::int AS evidence_count" in query:
            return [
                {
                    "tenant_id": "default",
                    "claim_event_key": "cek_v1_startbrief",
                    "predicate": "project.focus",
                    "subject_entity_id": "2ea8c5b7-c6d0-4f56-8f3e-8fe62f69f65e",
                    "subject_text": "Onboarding Launch",
                    "object_payload": {"value": "ship this week"},
                    "truth_confidence": 0.93,
                    "updated_at": now_dt,
                    "created_at": now_dt,
                    "subject_name": "Onboarding Launch",
                    "entity_type": "project",
                    "evidence_count": 1,
                }
            ]
        if "FROM entities" in query and "canonical_name_normalized" in query:
            return [
                {
                    "tenant_id": "default",
                    "entity_id": "2ea8c5b7-c6d0-4f56-8f3e-8fe62f69f65e",
                    "canonical_name": "Onboarding Launch",
                    "canonical_name_normalized": "onboarding launch",
                    "entity_type": "project",
                    "status": "active",
                    "created_at": now_dt,
                    "updated_at": now_dt,
                }
            ]
        if "FROM canonical_tenant_watermarks" in query:
            return [{"tenant_id": "default", "last_sequence": 22, "updated_at": now_dt}]
        return []

    async def _stub_bridge_llm(*_args, **_kwargs):
        return "The user is continuing the same core thread."

    async def _stub_ingest_freshness(*_args, **_kwargs):
        return {}

    async def _stub_log_history(*_args, **_kwargs):
        return None

    async def _stub_build_entity_candidates(*_args, **_kwargs):
        return []

    graphiti_client.get_latest_session_summary_node = _stub_latest_summary_node
    graphiti_client.get_recent_session_summary_nodes = _stub_recent_session_summary_nodes
    graphiti_client.get_recent_episode_summaries = _stub_recent_episode_summaries
    monkeypatch.setattr(loops_module, "get_top_loops_for_startbrief", _stub_get_top_loops, raising=True)
    monkeypatch.setattr("src.main.db.fetchone", _stub_db_fetchone, raising=False)
    monkeypatch.setattr("src.main.db.fetch", _stub_db_fetch, raising=False)
    monkeypatch.setattr("src.main._generate_startbrief_bridge_llm", _stub_bridge_llm, raising=True)
    monkeypatch.setattr("src.main._get_session_ingest_freshness", _stub_ingest_freshness, raising=True)
    monkeypatch.setattr("src.main._log_startbrief_history", _stub_log_history, raising=True)
    monkeypatch.setattr("src.main._build_entity_candidates", _stub_build_entity_candidates, raising=True)

    response = await session_startbrief(
        tenantId="default",
        userId="u1",
        now=now,
        sessionId="session-test",
        personaId=None,
        timezone="UTC",
    )
    payload = response.model_dump()

    provenance = ((payload.get("evidence") or {}).get("canonical_provenance") or {})
    assert provenance.get("projection_version") == "t9a.startbrief_reanchor.v1"
    assert int(provenance.get("canonical_claims_considered") or 0) >= 1
    assert int(provenance.get("canonical_entities_considered") or 0) >= 1
    watermarks = provenance.get("canonical_watermarks") or []
    assert watermarks and int(watermarks[0].get("last_sequence") or 0) == 22
