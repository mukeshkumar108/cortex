from __future__ import annotations

import json
from datetime import datetime, timezone as dt_timezone
from typing import Any, Dict, List

from .canonicalization import stable_short_hash, normalize_text
from .models import MemoryQueryResponse, MemoryQueryV2Response


SHADOW_DIFF_VERSION = "t12b.shadow_diff.v1"


def _n(value: Any) -> str:
    return normalize_text(value, casefold=False)


def _to_text_set(values: List[str]) -> set[str]:
    return {_n(v).lower() for v in values if _n(v)}


def _legacy_factual_texts(response: MemoryQueryResponse) -> List[str]:
    return [_n(item.text) for item in (response.factItems or []) if _n(item.text)]


def _legacy_continuity_texts(response: MemoryQueryResponse) -> List[str]:
    out: List[str] = []
    for item in (response.factItems or []):
        source = _n(item.source).lower()
        text = _n(item.text)
        if text and source in {"continuity_projection", "derived_continuity", "continuity"}:
            out.append(text)
    return out


def _legacy_episode_ids(response: MemoryQueryResponse) -> List[str]:
    out: List[str] = []
    for ep in (response.episodes or []):
        key = _n(ep.episodeId) or _n(ep.sessionId) or _n(ep.summary)
        if key:
            out.append(key)
    return out


def _v2_lane_texts(response: MemoryQueryV2Response, lane: str) -> List[str]:
    return [
        _n(item.text)
        for item in (response.items or [])
        if _n(item.lane).lower() == lane and _n(item.text)
    ]


def _v2_lane_ids(response: MemoryQueryV2Response, lane: str) -> List[str]:
    out: List[str] = []
    for item in (response.items or []):
        if _n(item.lane).lower() != lane:
            continue
        key = _n(item.episodeId) or _n(item.sessionId) or _n(item.text)
        if key:
            out.append(key)
    return out


def _v2_evidence_coverage(response: MemoryQueryV2Response) -> Dict[str, Any]:
    factual_items = [item for item in (response.items or []) if _n(item.lane).lower() == "factual"]
    factual_count = len(factual_items)
    evidenced = 0
    for item in factual_items:
        if isinstance(item.evidence, list) and len(item.evidence) > 0:
            evidenced += 1
    ratio = (float(evidenced) / float(factual_count)) if factual_count > 0 else 1.0
    return {
        "factual_count": factual_count,
        "factual_with_evidence": evidenced,
        "evidence_coverage_ratio": round(ratio, 4),
    }


def build_shadow_diff(
    *,
    tenant_id: str,
    user_id: str,
    endpoint: str,
    request_query: str,
    served_intent: str,
    served_response: MemoryQueryResponse,
    shadow_response: MemoryQueryV2Response,
    served_latency_ms: float,
    shadow_latency_ms: float,
) -> Dict[str, Any]:
    served_factual = _legacy_factual_texts(served_response)
    served_continuity = _legacy_continuity_texts(served_response)
    served_episodes = _legacy_episode_ids(served_response)

    shadow_factual = _v2_lane_texts(shadow_response, "factual")
    shadow_continuity = _v2_lane_texts(shadow_response, "continuity")
    shadow_episodes = _v2_lane_ids(shadow_response, "episodic")

    served_factual_set = _to_text_set(served_factual)
    shadow_factual_set = _to_text_set(shadow_factual)
    served_continuity_set = _to_text_set(served_continuity)
    shadow_continuity_set = _to_text_set(shadow_continuity)
    served_episode_set = _to_text_set(served_episodes)
    shadow_episode_set = _to_text_set(shadow_episodes)

    evidence = _v2_evidence_coverage(shadow_response)
    unsupported_in_served = sorted(list(served_factual_set - shadow_factual_set))

    # Conservative contradiction signal: negation mismatch on same normalized tail text.
    contradictions: List[Dict[str, str]] = []
    for text in served_factual_set.intersection(shadow_factual_set):
        if " not " in f" {text} ":
            continue
    for served_text in served_factual_set:
        for shadow_text in shadow_factual_set:
            if served_text == shadow_text:
                continue
            if served_text.replace(" not ", " ") == shadow_text.replace(" not ", " "):
                contradictions.append({"served": served_text, "shadow": shadow_text})

    latency_delta = float(shadow_latency_ms) - float(served_latency_ms)
    diff = {
        "version": SHADOW_DIFF_VERSION,
        "captured_at": datetime.now(dt_timezone.utc).isoformat(),
        "factual_differences": {
            "served_only": sorted(list(served_factual_set - shadow_factual_set)),
            "shadow_only": sorted(list(shadow_factual_set - served_factual_set)),
            "shared": sorted(list(served_factual_set.intersection(shadow_factual_set))),
        },
        "evidence_coverage_differences": {
            "shadow": evidence,
            "served_missing_in_shadow": unsupported_in_served,
        },
        "continuity_differences": {
            "served_only": sorted(list(served_continuity_set - shadow_continuity_set)),
            "shadow_only": sorted(list(shadow_continuity_set - served_continuity_set)),
            "shared": sorted(list(served_continuity_set.intersection(shadow_continuity_set)),),
        },
        "episodic_differences": {
            "served_only": sorted(list(served_episode_set - shadow_episode_set)),
            "shadow_only": sorted(list(shadow_episode_set - served_episode_set)),
        },
        "latency_differences": {
            "served_latency_ms": round(float(served_latency_ms), 3),
            "shadow_latency_ms": round(float(shadow_latency_ms), 3),
            "delta_ms": round(latency_delta, 3),
        },
        "regression_signals": {
            "evidence_coverage_regression": bool(len(unsupported_in_served) > 0),
            "unsupported_factual_difference": bool(len(unsupported_in_served) > 0),
            "possible_contradictions": contradictions,
            "latency_regression": bool(latency_delta > 120.0),
            "continuity_drift": bool(len(shadow_continuity_set.symmetric_difference(served_continuity_set)) > 0),
        },
    }

    request_fingerprint = stable_short_hash(
        json.dumps(
            {
                "tenant": _n(tenant_id),
                "user": _n(user_id),
                "endpoint": _n(endpoint),
                "intent": _n(served_intent),
                "query": _n(request_query),
            },
            sort_keys=True,
            ensure_ascii=True,
        )
    )

    return {
        "tenant_id": _n(tenant_id),
        "user_id": _n(user_id),
        "endpoint": _n(endpoint),
        "served_intent": _n(served_intent),
        "request_fingerprint": request_fingerprint,
        "diff": diff,
        "metrics": {
            "factual_served_count": len(served_factual_set),
            "factual_shadow_count": len(shadow_factual_set),
            "continuity_served_count": len(served_continuity_set),
            "continuity_shadow_count": len(shadow_continuity_set),
            "episodic_served_count": len(served_episode_set),
            "episodic_shadow_count": len(shadow_episode_set),
            "latency_delta_ms": round(latency_delta, 3),
            "evidence_coverage_ratio_shadow": evidence.get("evidence_coverage_ratio"),
            "unsupported_factual_count": len(unsupported_in_served),
            "possible_contradiction_count": len(contradictions),
        },
        "served_latency_ms": round(float(served_latency_ms), 3),
        "shadow_latency_ms": round(float(shadow_latency_ms), 3),
        "latency_delta_ms": round(latency_delta, 3),
    }


async def persist_shadow_diff(db: Any, payload: Dict[str, Any]) -> None:
    await db.execute(
        """
        INSERT INTO retrieval_shadow_diffs (
            tenant_id,
            user_id,
            endpoint,
            served_intent,
            request_fingerprint,
            status,
            served_latency_ms,
            shadow_latency_ms,
            latency_delta_ms,
            diff_payload,
            metrics_payload,
            metadata
        )
        VALUES (
            $1, $2, $3, $4, $5, 'ok', $6, $7, $8, $9::jsonb, $10::jsonb, $11::jsonb
        )
        """,
        payload.get("tenant_id"),
        payload.get("user_id"),
        payload.get("endpoint"),
        payload.get("served_intent"),
        payload.get("request_fingerprint"),
        payload.get("served_latency_ms"),
        payload.get("shadow_latency_ms"),
        payload.get("latency_delta_ms"),
        payload.get("diff") or {},
        payload.get("metrics") or {},
        {"version": SHADOW_DIFF_VERSION},
    )


async def persist_shadow_error(
    db: Any,
    *,
    tenant_id: str,
    user_id: str,
    endpoint: str,
    served_intent: str,
    request_query: str,
    error: str,
) -> None:
    request_fingerprint = stable_short_hash(
        json.dumps(
            {
                "tenant": _n(tenant_id),
                "user": _n(user_id),
                "endpoint": _n(endpoint),
                "intent": _n(served_intent),
                "query": _n(request_query),
            },
            sort_keys=True,
            ensure_ascii=True,
        )
    )
    await db.execute(
        """
        INSERT INTO retrieval_shadow_diffs (
            tenant_id,
            user_id,
            endpoint,
            served_intent,
            request_fingerprint,
            status,
            error_text,
            diff_payload,
            metrics_payload,
            metadata
        )
        VALUES (
            $1, $2, $3, $4, $5, 'shadow_error', $6, '{}'::jsonb, '{}'::jsonb, $7::jsonb
        )
        """,
        _n(tenant_id),
        _n(user_id),
        _n(endpoint),
        _n(served_intent),
        request_fingerprint,
        _n(error)[:500],
        {"version": SHADOW_DIFF_VERSION},
    )
