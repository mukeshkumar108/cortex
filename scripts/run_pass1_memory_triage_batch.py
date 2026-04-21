#!/usr/bin/env python3
"""Run Pass 1 memory triage using the shared live pass implementation."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, List

from src.config import get_settings
from src.db import Database
from src.derived_pipeline import run_pass1_triage


async def run_batch(*, user_id: str, tenant_id: str = "default", limit: int = 100, include_existing: bool = False) -> Dict[str, int]:
    db = Database()
    settings = get_settings()
    try:
        exists_filter = "" if include_existing else "AND sc.session_id IS NULL"
        rows = await db.fetch(
            f"""
            SELECT st.tenant_id, st.session_id, st.user_id, st.messages, st.created_at
            FROM session_transcript st
            LEFT JOIN session_classifications sc ON sc.session_id = st.session_id
            WHERE st.user_id = $1
              AND COALESCE(st.tenant_id, 'default') = $2
              {exists_filter}
            ORDER BY st.created_at ASC NULLS LAST
            LIMIT $3
            """,
            user_id,
            tenant_id,
            int(limit),
        )
        processed = 0
        for row in rows:
            messages = row.get("messages") if isinstance(row.get("messages"), list) else []
            await run_pass1_triage(
                db=db,
                tenant_id=row.get("tenant_id") or tenant_id,
                user_id=row.get("user_id") or user_id,
                session_id=row["session_id"],
                messages=messages,
                reference_time=row.get("created_at") or datetime.now(timezone.utc),
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
    parser.add_argument("--include-existing", action="store_true")
    args = parser.parse_args()
    result = asyncio.run(run_batch(user_id=args.user_id, tenant_id=args.tenant_id, limit=args.limit, include_existing=args.include_existing))
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
