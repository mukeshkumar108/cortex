#!/usr/bin/env python3
"""
Verify tenant alias consolidation state.

Usage:
  DATABASE_URL=... python3 scripts/verify_tenant_alias_consolidation.py \
    --alias sophie-prod --canonical default
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import List

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.db import Database


TABLES: List[str] = [
    "user_identity",
    "identity_cache",
    "loops",
    "session_buffer",
    "session_transcript",
    "graphiti_outbox",
    "user_model",
    "daily_analysis",
    "startbrief_history",
    "user_model_enrichment_state",
    "user_model_write_claims",
    "habit_dedupe_state",
    "checkin_tactic_log",
]


async def _run(alias: str, canonical: str, sample_limit: int) -> None:
    db = Database()
    try:
        print(f"alias={alias} canonical={canonical}")
        print("table counts:")
        for table in TABLES:
            row = await db.fetchone(
                f"""
                SELECT
                    count(*) AS alias_count,
                    (SELECT count(*) FROM {table} WHERE tenant_id = $2) AS canonical_count
                FROM {table}
                WHERE tenant_id = $1
                """,
                alias,
                canonical,
            )
            print(f"- {table}: alias={row['alias_count']} canonical={row['canonical_count']}")

        rows = await db.fetch(
            """
            WITH all_rows AS (
                SELECT user_id, tenant_id FROM user_model
                UNION ALL
                SELECT user_id, tenant_id FROM session_transcript
                UNION ALL
                SELECT user_id, tenant_id FROM loops
            )
            SELECT
                user_id,
                array_agg(DISTINCT tenant_id ORDER BY tenant_id) AS tenants,
                count(DISTINCT tenant_id) AS tenant_count
            FROM all_rows
            GROUP BY user_id
            HAVING count(DISTINCT tenant_id) > 1
            ORDER BY tenant_count DESC, user_id
            LIMIT $1
            """,
            max(1, int(sample_limit)),
        )
        print(f"users split across tenants (sample_count={len(rows)}):")
        for row in rows:
            print(f"- {row['user_id']}: {row['tenants']}")
    finally:
        await db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify tenant alias consolidation state.")
    parser.add_argument("--alias", default="sophie-prod")
    parser.add_argument("--canonical", default="default")
    parser.add_argument("--sample-limit", type=int, default=25)
    args = parser.parse_args()
    asyncio.run(_run(alias=args.alias, canonical=args.canonical, sample_limit=args.sample_limit))


if __name__ == "__main__":
    main()
