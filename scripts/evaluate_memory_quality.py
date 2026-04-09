#!/usr/bin/env python3
"""Fixture-driven evaluator for /memory/query recall quality.

Example:
  python3 scripts/evaluate_memory_quality.py \
    --base-url http://localhost:8000 \
    --cases tests/fixtures/memory_quality_cases.sample.json
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import request, error


@dataclass
class CaseResult:
    name: str
    passed: bool
    latency_ms: int
    details: Dict[str, Any]


def _post_json(url: str, payload: Dict[str, Any], timeout: int = 15) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _contains_any(haystack: str, needles: List[str]) -> bool:
    low = haystack.lower()
    return any(n.lower() in low for n in needles)


def _evaluate_case(base_url: str, case: Dict[str, Any], timeout: int) -> CaseResult:
    payload = {
        "tenantId": case["tenantId"],
        "userId": case["userId"],
        "query": case["query"],
        "limit": int(case.get("limit", 6)),
        "includeContext": bool(case.get("includeContext", False)),
    }
    if case.get("referenceTime"):
        payload["referenceTime"] = case["referenceTime"]

    started = time.time()
    response = _post_json(f"{base_url.rstrip('/')}/memory/query", payload, timeout=timeout)
    latency_ms = int((time.time() - started) * 1000)

    facts = response.get("facts") if isinstance(response, dict) else []
    fact_items = response.get("factItems") if isinstance(response, dict) else []
    metadata = response.get("metadata") if isinstance(response, dict) else {}
    fact_blob = "\n".join([str(x) for x in (facts or [])])

    required_terms = [str(x) for x in case.get("must_contain_any", []) if str(x).strip()]
    required_sources = [str(x) for x in case.get("must_include_sources", []) if str(x).strip()]
    max_latency_ms = int(case.get("max_latency_ms", 1500))

    contains_term = True if not required_terms else _contains_any(fact_blob, required_terms)
    sources = sorted({str((i or {}).get("source") or "") for i in (fact_items or []) if isinstance(i, dict)})
    source_ok = all(src in sources for src in required_sources)
    latency_ok = latency_ms <= max_latency_ms
    provenance_ok = isinstance(metadata.get("provenanceCounts"), dict)
    domain_ok = isinstance(metadata.get("domainBreakdown"), dict)

    passed = bool(contains_term and source_ok and latency_ok and provenance_ok and domain_ok)
    return CaseResult(
        name=str(case.get("name") or "unnamed_case"),
        passed=passed,
        latency_ms=latency_ms,
        details={
            "contains_term": contains_term,
            "source_ok": source_ok,
            "latency_ok": latency_ok,
            "provenance_ok": provenance_ok,
            "domain_ok": domain_ok,
            "sources_seen": sources,
            "facts_count": len(facts or []),
            "entities_count": len(response.get("entities") or []) if isinstance(response, dict) else 0,
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate memory/query recall quality from fixture cases")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--cases", required=True, help="Path to fixture JSON file")
    parser.add_argument("--timeout", type=int, default=20)
    args = parser.parse_args()

    cases_path = Path(args.cases)
    if not cases_path.exists():
        print(json.dumps({"ok": False, "error": f"Cases file not found: {cases_path}"}, indent=2))
        return 2

    cases_doc = json.loads(cases_path.read_text(encoding="utf-8"))
    cases = cases_doc if isinstance(cases_doc, list) else cases_doc.get("cases", [])
    if not isinstance(cases, list) or not cases:
        print(json.dumps({"ok": False, "error": "No cases found"}, indent=2))
        return 2

    results: List[CaseResult] = []
    for case in cases:
        if not isinstance(case, dict):
            continue
        try:
            results.append(_evaluate_case(args.base_url, case, timeout=args.timeout))
        except error.HTTPError as e:
            results.append(
                CaseResult(
                    name=str(case.get("name") or "unnamed_case"),
                    passed=False,
                    latency_ms=0,
                    details={"http_error": e.code, "reason": str(e)},
                )
            )
        except Exception as e:
            results.append(
                CaseResult(
                    name=str(case.get("name") or "unnamed_case"),
                    passed=False,
                    latency_ms=0,
                    details={"error": str(e)},
                )
            )

    passed = sum(1 for r in results if r.passed)
    total = len(results)
    p95_latency = 0
    if results:
        latencies = sorted(r.latency_ms for r in results)
        p95_idx = min(len(latencies) - 1, int(len(latencies) * 0.95))
        p95_latency = latencies[p95_idx]

    output = {
        "ok": passed == total,
        "summary": {
            "passed": passed,
            "total": total,
            "pass_rate": round((passed / total) if total else 0.0, 3),
            "p95_latency_ms": p95_latency,
        },
        "results": [
            {
                "name": r.name,
                "passed": r.passed,
                "latency_ms": r.latency_ms,
                "details": r.details,
            }
            for r in results
        ],
    }
    print(json.dumps(output, indent=2))
    return 0 if output["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
