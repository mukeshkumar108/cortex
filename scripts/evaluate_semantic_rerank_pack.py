#!/usr/bin/env python3
"""
Evaluate semantic reranking quality from a curated fixture pack.

Outputs:
- JSON report with per-case before/after retrieval outputs
- Markdown summary with improvements, regressions, and calibration notes
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.memory_taxonomy import classify_memory_candidates_semantic, classify_memory_semantic_fallback


QUERY_STOPWORDS = {
    "the", "a", "an", "and", "or", "for", "to", "of", "in", "on", "at", "by",
    "with", "from", "is", "are", "was", "were", "be", "been", "being", "do",
    "did", "does", "why", "what", "when", "where", "who", "how", "i", "me",
    "my", "you", "your", "we", "our", "it", "this", "that", "these", "those",
    "remember",
}


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    return re.sub(r"\s+", " ", value).strip()


def _query_terms(query: str) -> List[str]:
    terms = re.findall(r"[A-Za-z0-9]+", _normalize_text(query).lower())
    return [term for term in terms if len(term) >= 3 and term not in QUERY_STOPWORDS]


def _query_overlap_score(text: str, query_terms: List[str]) -> float:
    normalized = _normalize_text(text).lower()
    if not normalized or not query_terms:
        return 0.0
    hits = sum(1 for term in query_terms if term in normalized)
    if hits <= 0:
        return 0.0
    return min(0.99, 0.35 + (hits * 0.2))


def _dcg_at_k(ids: List[str], expected: set[str], k: int) -> float:
    score = 0.0
    for idx, item_id in enumerate(ids[:k], start=1):
        rel = 1.0 if item_id in expected else 0.0
        if rel <= 0:
            continue
        score += rel / (1.0 if idx == 1 else (idx.bit_length()))
    return score


def _semantic_rank_bonus(
    semantic: Optional[Any],
    row_source: str,
    fallback_semantic: Optional[Any],
    query_domain_focus: set[str],
    query_intent: str,
    query_memory_type: str,
    emotion_focus_query: bool,
    finance_focus_query: bool,
    semantic_enabled: bool,
) -> float:
    if not semantic_enabled or semantic is None:
        return 0.0
    score = 0.0
    source = _normalize_text(row_source)
    domain = _normalize_text(getattr(semantic, "domain", None))
    if domain and domain in query_domain_focus:
        score += 0.10
    domain_scores = getattr(semantic, "domain_scores", None) or {}
    if isinstance(domain_scores, dict):
        for domain_key in query_domain_focus:
            score += min(0.03, float(domain_scores.get(domain_key) or 0.0) * 0.06)
    intent = _normalize_text(getattr(semantic, "intent", None))
    if query_intent and intent and intent == query_intent:
        score += 0.05
    memory_type = _normalize_text(getattr(semantic, "memory_type", None))
    if (
        query_intent not in {"make_commitment", "reflect"}
        and query_memory_type
        and memory_type
        and memory_type == query_memory_type
    ):
        score += 0.05
    if query_intent == "make_commitment":
        if intent in {"make_commitment", "reflect", "express_emotion"}:
            score += 0.06
        if memory_type in {"habit", "goal", "event", "relationship"}:
            score += 0.06
        if intent == "ask_help" and memory_type in {"relationship", "state", "habit"}:
            score += 0.03
        if intent == "share_update" and memory_type in {"state", "fact"}:
            score -= 0.05
    elif query_intent == "reflect":
        if intent in {"reflect", "express_emotion", "make_commitment"}:
            score += 0.05
        if memory_type in {"state", "habit", "relationship", "goal"}:
            score += 0.05
    if (
        source == "user_model"
        and query_intent in {"make_commitment", "reflect"}
        and intent in {"reflect", "share_update"}
        and memory_type in {"habit", "state", "fact"}
    ):
        score -= 0.05
    fallback_intent = _normalize_text(getattr(fallback_semantic, "intent", None))
    fallback_memory_type = _normalize_text(getattr(fallback_semantic, "memory_type", None))
    if (
        query_intent in {"make_commitment", "reflect", "express_emotion"}
        and intent == "share_update"
        and fallback_intent
        and fallback_intent != "share_update"
    ):
        score += 0.04
        if fallback_intent == query_intent:
            score += 0.03
        if fallback_memory_type in {"habit", "goal", "relationship", "state"}:
            score += 0.02
    confidence = float(getattr(semantic, "confidence", 0.0) or 0.0)
    score += min(0.12, max(0.0, confidence) * 0.12)

    if emotion_focus_query:
        if intent in {"express_emotion", "reflect"}:
            score += 0.11
        if memory_type in {"state", "habit", "relationship"}:
            score += 0.06
        if domain in {"worries", "wellness", "relationships", "finance"}:
            score += 0.05
        if intent in {"share_update", "ask_help"}:
            score -= 0.07
        if memory_type in {"event", "goal", "preference"}:
            score -= 0.05
        if source == "user_model" and intent == "share_update" and memory_type == "event":
            score -= 0.08

    if finance_focus_query:
        if domain in {"finance", "worries"}:
            score += 0.14
        if intent == "express_emotion":
            score += 0.08
        if memory_type in {"state", "habit", "relationship"}:
            score += 0.04
        if domain == "work" and intent == "share_update" and memory_type == "event":
            score -= 0.14
        if source == "user_model" and domain not in {"finance", "worries"} and memory_type == "event":
            score -= 0.10
    return score


def _confidence_bucket(value: float) -> str:
    if value < 0.50:
        return "lt_0_50"
    if value < 0.70:
        return "0_50_to_0_69"
    if value < 0.85:
        return "0_70_to_0_84"
    return "ge_0_85"


async def _evaluate_case(
    case: Dict[str, Any],
    limit: int,
    semantic_enabled: bool,
    embedding_enabled: bool,
    embedding_model: str,
) -> Dict[str, Any]:
    query = _normalize_text(case.get("query"))
    utterances = [row for row in (case.get("utterances") or []) if isinstance(row, dict)]
    expected_ids = {_normalize_text(x) for x in (case.get("expected_ids") or []) if _normalize_text(x)}
    query_terms = _query_terms(query)

    candidate_rows: List[Dict[str, Any]] = []
    for row in utterances:
        text = _normalize_text(row.get("text"))
        if not text:
            continue
        base = _query_overlap_score(text, query_terms) or 0.2
        candidate_rows.append(
            {
                "id": _normalize_text(row.get("id")) or f"row_{len(candidate_rows)}",
                "text": text,
                "source": _normalize_text(row.get("source")) or "graphiti_fact",
                "base_relevance": float(base),
            }
        )

    semantic_items = [{"text": row["text"], "source": row["source"]} for row in candidate_rows]
    fallback_semantics = [
        classify_memory_semantic_fallback(item.get("text"), source_hint=item.get("source"))
        for item in semantic_items
    ]
    audit_stats: Dict[str, Any] = {}
    semantics = await classify_memory_candidates_semantic(
        semantic_items,
        llm_client=None,
        semantic_enabled=semantic_enabled,
        enable_embedding_fallback=embedding_enabled,
        embedding_model=embedding_model,
        audit_stats=audit_stats,
    )

    query_semantic_rows = await classify_memory_candidates_semantic(
        [{"text": query, "source": "query"}],
        llm_client=None,
        semantic_enabled=semantic_enabled,
        enable_embedding_fallback=embedding_enabled,
        embedding_model=embedding_model,
        audit_stats=None,
    )
    query_semantic = (
        query_semantic_rows[0]
        if query_semantic_rows
        else classify_memory_semantic_fallback(query, source_hint="query")
    )
    ranked_query_domains = sorted((query_semantic.domain_scores or {}).items(), key=lambda kv: float(kv[1]), reverse=True)
    query_domain_peak = float(ranked_query_domains[0][1]) if ranked_query_domains else 0.0
    query_domain_focus: set[str] = set()
    for idx, (key, raw_score) in enumerate(ranked_query_domains):
        score = float(raw_score or 0.0)
        if idx >= 2 and score < 0.22:
            continue
        normalized = _normalize_text(key)
        if normalized:
            query_domain_focus.add(normalized)
        if len(query_domain_focus) >= 4:
            break
    if query_domain_peak < 0.20:
        query_domain_focus.clear()
    if _normalize_text(query_semantic.domain) and not query_domain_focus:
        query_domain_focus.add(_normalize_text(query_semantic.domain))
    query_intent = _normalize_text(query_semantic.intent)
    query_memory_type = _normalize_text(query_semantic.memory_type)
    query_domain_scores = query_semantic.domain_scores or {}
    emotion_signal = max(
        float(query_domain_scores.get("worries") or 0.0),
        float(query_domain_scores.get("wellness") or 0.0),
        float(query_domain_scores.get("relationships") or 0.0),
    )
    finance_signal = float(query_domain_scores.get("finance") or 0.0)
    emotion_focus_query = (
        query_intent in {"express_emotion", "reflect"}
        or emotion_signal >= 0.34
    )
    finance_focus_query = (
        _normalize_text(query_semantic.domain) == "finance"
        or finance_signal >= 0.34
        or ("finance" in query_domain_focus and query_intent in {"express_emotion", "reflect", "share_update"})
    )

    before_ranked: List[Dict[str, Any]] = []
    after_ranked: List[Dict[str, Any]] = []
    for idx, row in enumerate(candidate_rows):
        semantic = semantics[idx] if idx < len(semantics) else None
        fallback_semantic = fallback_semantics[idx] if idx < len(fallback_semantics) else None
        source_bonus = 0.03 if row["source"].startswith("graphiti") else 0.0
        before_score = row["base_relevance"] + source_bonus
        after_score = before_score + _semantic_rank_bonus(
            semantic=semantic,
            row_source=row["source"],
            fallback_semantic=fallback_semantic,
            query_domain_focus=query_domain_focus,
            query_intent=query_intent,
            query_memory_type=query_memory_type,
            emotion_focus_query=emotion_focus_query,
            finance_focus_query=finance_focus_query,
            semantic_enabled=semantic_enabled,
        )

        before_ranked.append({"score": before_score, "row": row, "semantic": semantic})
        after_ranked.append({"score": after_score, "row": row, "semantic": semantic})

    before_ranked.sort(key=lambda item: item["score"], reverse=True)
    after_ranked.sort(key=lambda item: item["score"], reverse=True)
    before_top = before_ranked[:limit]
    after_top = after_ranked[:limit]

    before_ids = [item["row"]["id"] for item in before_top]
    after_ids = [item["row"]["id"] for item in after_top]
    before_hits = sum(1 for item_id in before_ids if item_id in expected_ids)
    after_hits = sum(1 for item_id in after_ids if item_id in expected_ids)
    before_dcg = _dcg_at_k(before_ids, expected_ids, limit)
    after_dcg = _dcg_at_k(after_ids, expected_ids, limit)

    def _pack(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for item in items:
            semantic = item.get("semantic")
            row = item["row"]
            out.append(
                {
                    "id": row["id"],
                    "score": round(float(item["score"]), 4),
                    "text": row["text"],
                    "source": row["source"],
                    "domain": _normalize_text(getattr(semantic, "domain", None)) if semantic else None,
                    "intent": _normalize_text(getattr(semantic, "intent", None)) if semantic else None,
                    "memory_type": _normalize_text(getattr(semantic, "memory_type", None)) if semantic else None,
                    "confidence": float(getattr(semantic, "confidence", 0.0) or 0.0) if semantic else 0.0,
                    "classification_method": _normalize_text(getattr(semantic, "classification_method", None)) if semantic else None,
                    "expected_relevant": row["id"] in expected_ids,
                }
            )
        return out

    return {
        "id": _normalize_text(case.get("id")),
        "query": query,
        "tags": [str(t) for t in (case.get("tags") or [])],
        "expected_ids": sorted(expected_ids),
        "semantic_query": {
            "domain": query_semantic.domain,
            "intent": query_semantic.intent,
            "memory_type": query_semantic.memory_type,
            "domain_scores": query_semantic.domain_scores,
        },
        "audit": audit_stats,
        "before": {
            "top": _pack(before_top),
            "hits_at_k": before_hits,
            "dcg_at_k": round(before_dcg, 4),
        },
        "after": {
            "top": _pack(after_top),
            "hits_at_k": after_hits,
            "dcg_at_k": round(after_dcg, 4),
        },
        "delta": {
            "hits_at_k": after_hits - before_hits,
            "dcg_at_k": round(after_dcg - before_dcg, 4),
        },
        "status": "improved" if after_dcg > before_dcg else ("regressed" if after_dcg < before_dcg else "unchanged"),
    }


def _build_markdown_report(report: Dict[str, Any]) -> str:
    lines: List[str] = []
    summary = report.get("summary") or {}
    lines.append("# Semantic Rerank Eval Report")
    lines.append("")
    lines.append(f"- Cases: {summary.get('cases_total', 0)}")
    lines.append(f"- Improved: {summary.get('improved', 0)}")
    lines.append(f"- Regressed: {summary.get('regressed', 0)}")
    lines.append(f"- Unchanged: {summary.get('unchanged', 0)}")
    lines.append(f"- Embedding fallback rate: {summary.get('fallback_rate', 0.0):.3f}")
    lines.append(f"- Embedding failure rate: {summary.get('embedding_failure_rate', 0.0):.3f}")
    lines.append("")

    lines.append("## Improvements")
    improved = [case for case in report.get("cases", []) if case.get("status") == "improved"]
    if not improved:
        lines.append("- None.")
    for case in improved:
        lines.append(
            f"- `{case['id']}`: hits@k {case['before']['hits_at_k']} -> {case['after']['hits_at_k']}, "
            f"dcg {case['before']['dcg_at_k']} -> {case['after']['dcg_at_k']}"
        )
    lines.append("")

    lines.append("## Regressions")
    regressed = [case for case in report.get("cases", []) if case.get("status") == "regressed"]
    if not regressed:
        lines.append("- None.")
    for case in regressed:
        lines.append(
            f"- `{case['id']}`: hits@k {case['before']['hits_at_k']} -> {case['after']['hits_at_k']}, "
            f"dcg {case['before']['dcg_at_k']} -> {case['after']['dcg_at_k']}"
        )
    lines.append("")

    lines.append("## Confidence Calibration Snapshot")
    calib = report.get("confidence_calibration") or {}
    for bucket, row in (calib.get("buckets") or {}).items():
        lines.append(
            f"- `{bucket}`: total={row.get('total', 0)}, correct_top1={row.get('correct', 0)}, "
            f"accuracy={row.get('accuracy', 0.0):.3f}"
        )
    lines.append("")

    lines.append("## Noisy Label Patterns")
    noisy = report.get("noisy_patterns") or []
    if not noisy:
        lines.append("- None flagged.")
    for row in noisy:
        lines.append(f"- `{row.get('case_id')}`: {row.get('reason')}")
    lines.append("")

    lines.append("## Before/After Top-3 by Case")
    for case in report.get("cases", []):
        lines.append(f"### {case['id']}")
        lines.append(f"- Query: {case['query']}")
        lines.append("- Before:")
        for item in (case.get("before", {}).get("top") or [])[:3]:
            lines.append(
                f"  - {item['id']} | {item['score']:.3f} | intent={item.get('intent')} | "
                f"type={item.get('memory_type')} | conf={item.get('confidence', 0.0):.2f} | {item['text']}"
            )
        lines.append("- After:")
        for item in (case.get("after", {}).get("top") or [])[:3]:
            lines.append(
                f"  - {item['id']} | {item['score']:.3f} | intent={item.get('intent')} | "
                f"type={item.get('memory_type')} | conf={item.get('confidence', 0.0):.2f} | {item['text']}"
            )
        lines.append("")
    return "\n".join(lines).strip() + "\n"


async def _run_eval(
    pack_path: Path,
    limit: int,
    semantic_enabled: bool,
    embedding_enabled: bool,
    embedding_model: str,
) -> Dict[str, Any]:
    payload = json.loads(pack_path.read_text(encoding="utf-8"))
    cases = payload.get("cases") if isinstance(payload, dict) else None
    if not isinstance(cases, list) or not cases:
        raise ValueError("No eval cases found.")

    evaluated: List[Dict[str, Any]] = []
    method_counts: Dict[str, int] = {}
    embedding_attempted = 0
    embedding_failed = 0
    top1_calibration: Dict[str, Dict[str, int]] = {}
    noisy_patterns: List[Dict[str, Any]] = []

    for case in cases:
        if not isinstance(case, dict):
            continue
        result = await _evaluate_case(
            case=case,
            limit=limit,
            semantic_enabled=semantic_enabled,
            embedding_enabled=embedding_enabled,
            embedding_model=embedding_model,
        )
        evaluated.append(result)

        audit = result.get("audit") or {}
        if bool(audit.get("embedding_attempted")):
            embedding_attempted += 1
        if bool(audit.get("embedding_failed")):
            embedding_failed += 1
        for key, value in (audit.get("classification_method_counts") or {}).items():
            method = _normalize_text(key) or "unknown"
            method_counts[method] = int(method_counts.get(method) or 0) + int(value or 0)

        top_after = (result.get("after") or {}).get("top") or []
        top1 = top_after[0] if top_after else None
        if top1:
            bucket = _confidence_bucket(float(top1.get("confidence") or 0.0))
            row = top1_calibration.setdefault(bucket, {"total": 0, "correct": 0})
            row["total"] += 1
            if bool(top1.get("expected_relevant")):
                row["correct"] += 1

        case_tags = {str(tag) for tag in (result.get("tags") or [])}
        intents = {_normalize_text(item.get("intent")) for item in top_after if _normalize_text(item.get("intent"))}
        memory_types = {_normalize_text(item.get("memory_type")) for item in top_after if _normalize_text(item.get("memory_type"))}
        share_update_count = sum(1 for item in top_after if _normalize_text(item.get("intent")) == "share_update")
        fact_count = sum(1 for item in top_after if _normalize_text(item.get("memory_type")) == "fact")
        total_top = max(1, len(top_after))

        if "mixed_domain" in case_tags and len(intents) <= 1:
            noisy_patterns.append({"case_id": result.get("id"), "reason": "mixed-domain case collapsed to single intent"})
        if "mixed_domain" in case_tags and len(memory_types) <= 1:
            noisy_patterns.append({"case_id": result.get("id"), "reason": "mixed-domain case collapsed to single memory_type"})
        if (share_update_count / float(total_top)) >= 0.8:
            noisy_patterns.append({"case_id": result.get("id"), "reason": "intent mostly share_update (>=80%)"})
        if (fact_count / float(total_top)) >= 0.8:
            noisy_patterns.append({"case_id": result.get("id"), "reason": "memory_type mostly fact (>=80%)"})

        if top1 and not bool(top1.get("expected_relevant")):
            top1_intent = _normalize_text(top1.get("intent"))
            top1_memory = _normalize_text(top1.get("memory_type"))
            if top1_intent in {"share_update", "reflect"}:
                noisy_patterns.append(
                    {"case_id": result.get("id"), "reason": f"top1 miss with broad intent `{top1_intent}`"}
                )
            if top1_memory in {"fact", "event", "preference"}:
                noisy_patterns.append(
                    {"case_id": result.get("id"), "reason": f"top1 miss with broad memory_type `{top1_memory}`"}
                )
            if "emotional" in case_tags and top1_intent not in {"express_emotion", "reflect"}:
                noisy_patterns.append(
                    {"case_id": result.get("id"), "reason": "emotional case top1 intent not emotion-aligned"}
                )

    improved = sum(1 for case in evaluated if case.get("status") == "improved")
    regressed = sum(1 for case in evaluated if case.get("status") == "regressed")
    unchanged = sum(1 for case in evaluated if case.get("status") == "unchanged")
    total_method = max(1, sum(method_counts.values()))
    fallback_rate = float(method_counts.get("fallback", 0)) / float(total_method)
    embedding_failure_rate = float(embedding_failed) / float(max(1, embedding_attempted))

    calibration_buckets: Dict[str, Dict[str, Any]] = {}
    for bucket, row in top1_calibration.items():
        total = int(row.get("total") or 0)
        correct = int(row.get("correct") or 0)
        calibration_buckets[bucket] = {
            "total": total,
            "correct": correct,
            "accuracy": (float(correct) / float(total)) if total else 0.0,
        }

    return {
        "pack": str(pack_path),
        "settings": {
            "limit": limit,
            "semantic_enabled": semantic_enabled,
            "embedding_enabled": embedding_enabled,
            "embedding_model": embedding_model,
        },
        "summary": {
            "cases_total": len(evaluated),
            "improved": improved,
            "regressed": regressed,
            "unchanged": unchanged,
            "method_counts": method_counts,
            "fallback_rate": round(fallback_rate, 4),
            "embedding_failure_rate": round(embedding_failure_rate, 4),
        },
        "confidence_calibration": {
            "buckets": calibration_buckets,
        },
        "noisy_patterns": noisy_patterns,
        "cases": evaluated,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate semantic rerank quality from curated fixture pack.")
    parser.add_argument(
        "--pack",
        default="tests/fixtures/semantic_rerank_eval_pack.v1.json",
        help="Path to eval pack JSON",
    )
    parser.add_argument("--limit", type=int, default=5, help="Top-k limit used for before/after comparison")
    parser.add_argument("--semantic-enabled", choices=["true", "false"], default="true")
    parser.add_argument("--embedding-enabled", choices=["true", "false"], default="true")
    parser.add_argument("--embedding-model", default="text-embedding-3-small")
    parser.add_argument("--json-out", default="docs/semantic_rerank_eval_report.json")
    parser.add_argument("--md-out", default="docs/semantic_rerank_eval_report.md")
    args = parser.parse_args()

    pack_path = Path(args.pack)
    if not pack_path.exists():
        print(json.dumps({"ok": False, "error": f"Pack file not found: {pack_path}"}, indent=2))
        return 2

    report = asyncio.run(
        _run_eval(
            pack_path=pack_path,
            limit=max(1, int(args.limit)),
            semantic_enabled=(args.semantic_enabled == "true"),
            embedding_enabled=(args.embedding_enabled == "true"),
            embedding_model=str(args.embedding_model),
        )
    )
    json_out = Path(args.json_out)
    md_out = Path(args.md_out)
    json_out.write_text(json.dumps(report, indent=2, ensure_ascii=True), encoding="utf-8")
    md_out.write_text(_build_markdown_report(report), encoding="utf-8")
    print(json.dumps({"ok": True, "json_report": str(json_out), "md_report": str(md_out)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
