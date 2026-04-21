#!/usr/bin/env python3
"""Run Pass 3 thread updates using the shared live pass implementation."""

from __future__ import annotations

import argparse
import asyncio
import json
from typing import Dict

from src.config import get_settings
from src.db import Database
from src.derived_pipeline import run_pass3_threads


async def run_batch(*, user_id: str, tenant_id: str = "default", limit: int = 100) -> Dict[str, int]:
    db = Database()
    settings = get_settings()
    try:
        rows = await db.fetch(
            """
            SELECT sc.session_id, st.tenant_id, st.messages
            FROM session_classifications sc
            JOIN session_transcript st ON st.session_id = sc.session_id AND st.user_id = sc.user_id
            WHERE sc.user_id=$1 AND COALESCE(st.tenant_id, 'default')=$2 AND sc.run_threads_pass IS TRUE
            ORDER BY sc.processed_at ASC NULLS LAST
            LIMIT $3
            """,
            user_id,
            tenant_id,
            int(limit),
        )
        processed = 0
        for row in rows:
            await run_pass3_threads(
                db=db,
                tenant_id=row.get("tenant_id") or tenant_id,
                user_id=user_id,
                session_id=row["session_id"],
                messages=row.get("messages") if isinstance(row.get("messages"), list) else [],
                settings=settings,
            )
            processed += 1
        return {"seen": len(rows), "processed": processed}
    finally:
        await db.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--tenant-id", default="default")
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run_batch(user_id=args.user_id, tenant_id=args.tenant_id, limit=args.limit)), ensure_ascii=False))


if __name__ == "__main__":
    main()
