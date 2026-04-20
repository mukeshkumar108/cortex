#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any, Dict, List

from src.db import Database
from src.replay_audit import OfflineReplayHarness, ReplayRunConfig, load_golden_corpus


def _parse_extract_ids(raw: str) -> List[int]:
    out: List[int] = []
    for part in str(raw or "").split(","):
        token = part.strip()
        if not token:
            continue
        out.append(int(token))
    if not out:
        raise ValueError("at least one extract result id is required")
    return out


async def _run(args: argparse.Namespace) -> Dict[str, Any]:
    db = Database()
    harness = OfflineReplayHarness(db)
    try:
        if args.golden_corpus:
            corpus = load_golden_corpus(args.golden_corpus)
            reports: List[Dict[str, Any]] = []
            for case in corpus.get("cases", []):
                config = ReplayRunConfig(
                    tenant_id=str(case.get("tenant_id")),
                    extract_result_ids=[int(v) for v in (case.get("extract_result_ids") or [])],
                    policy_version=str(case.get("policy_version")) if case.get("policy_version") else None,
                    reset_before_run=bool(case.get("reset_before_run", True)),
                    allow_assistant_authored=bool(case.get("allow_assistant_authored", False)),
                )
                result = await harness.run_replay(config)
                reports.append(
                    {
                        "case_id": case.get("case_id"),
                        "run_id": result.run_id,
                        "tenant_id": result.tenant_id,
                        "state_hash": result.state_hash,
                        "errors": result.errors,
                    }
                )
            return {"mode": "golden_corpus", "reports": reports}

        config = ReplayRunConfig(
            tenant_id=args.tenant_id,
            extract_result_ids=_parse_extract_ids(args.extract_result_ids),
            policy_version=args.policy_version,
            reset_before_run=bool(args.reset_before_run),
            allow_assistant_authored=bool(args.allow_assistant_authored),
        )
        result = await harness.run_replay(config)
        return {
            "mode": "single_run",
            "run_id": result.run_id,
            "tenant_id": result.tenant_id,
            "extract_result_ids": result.extract_result_ids,
            "policy_version": result.policy_version,
            "state_hash": result.state_hash,
            "errors": result.errors,
            "claim_results": [
                {
                    "extract_result_id": r.extract_result_id,
                    "created_claim_ids": r.created_claim_ids,
                    "reinforced_claim_ids": r.reinforced_claim_ids,
                    "superseded_claim_ids": r.superseded_claim_ids,
                    "retracted_claim_ids": r.retracted_claim_ids,
                    "rejected_candidates": [vars(x) for x in r.rejected_candidates],
                }
                for r in result.claim_results
            ],
        }
    finally:
        await db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Synapse v2 offline replay/diff audit harness (T12a).")
    parser.add_argument("--tenant-id", type=str, help="Tenant id for single run mode")
    parser.add_argument("--extract-result-ids", type=str, help="Comma-separated extract result ids for single run mode")
    parser.add_argument("--policy-version", type=str, default=None)
    parser.add_argument("--reset-before-run", action="store_true", default=False)
    parser.add_argument("--allow-assistant-authored", action="store_true", default=False)
    parser.add_argument("--golden-corpus", type=str, default=None, help="Path to golden corpus JSON file")
    args = parser.parse_args()

    if not args.golden_corpus and (not args.tenant_id or not args.extract_result_ids):
        parser.error("single run mode requires --tenant-id and --extract-result-ids")

    report = asyncio.run(_run(args))
    print(json.dumps(report, sort_keys=True, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
