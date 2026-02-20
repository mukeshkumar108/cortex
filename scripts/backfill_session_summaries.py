#!/usr/bin/env python3
import argparse
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

from src.db import Database
from src.graphiti_client import GraphitiClient
from src.session import SessionManager


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    value = ts.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


async def _summary_exists(driver: Any, group_id: str, session_id: str) -> bool:
    rows = await driver.execute_query(
        """
        MATCH (n:SessionSummary {group_id: $group_id})
        WHERE n.session_id = $session_id OR n.attributes.session_id = $session_id
        RETURN n.uuid AS uuid
        LIMIT 1
        """,
        group_id=group_id,
        session_id=session_id
    )
    if not rows:
        return False
    for row in rows:
        # Falkor may return header-like rows
        if isinstance(row, dict):
            value = row.get("uuid")
            if value and value != "uuid":
                return True
        elif isinstance(row, (list, tuple)) and row:
            value = row[0]
            if value and value != "uuid":
                return True
    return False


async def run(args: argparse.Namespace) -> int:
    db = Database()
    graph = GraphitiClient()
    await graph.initialize()

    if not graph.client:
        raise RuntimeError("Graphiti client unavailable")

    driver = getattr(graph.client, "driver", None)
    if not driver:
        raise RuntimeError("Graphiti driver unavailable")

    session_mgr = SessionManager(db)

    where = ["st.tenant_id = $1"]
    params: List[Any] = [args.tenant_id]
    if args.user_id:
        where.append("st.user_id = $2")
        params.append(args.user_id)
    if args.since:
        where.append(f"st.updated_at >= ${len(params)+1}")
        params.append(_parse_iso(args.since))

    limit_param = len(params) + 1
    params.append(args.limit)

    query = f"""
        SELECT st.tenant_id, st.session_id, st.user_id,
               st.messages, st.created_at, st.updated_at,
               sb.rolling_summary, sb.closed_at
        FROM session_transcript st
        LEFT JOIN session_buffer sb
          ON st.tenant_id = sb.tenant_id
         AND st.session_id = sb.session_id
        WHERE {' AND '.join(where)}
        ORDER BY st.updated_at DESC
        LIMIT ${limit_param}
    """

    rows = await db.fetch(query, *params)
    processed = 0
    skipped = 0
    created = 0

    for row in rows:
        tenant_id = row.get("tenant_id")
        user_id = row.get("user_id")
        session_id = row.get("session_id")
        messages = row.get("messages") or []
        if not session_id or not user_id or not isinstance(messages, list):
            skipped += 1
            continue

        composite_user_id = graph._make_composite_user_id(tenant_id, user_id)
        if not args.force:
            exists = await _summary_exists(driver, composite_user_id, session_id)
            if exists:
                skipped += 1
                continue

        reference_time = None
        if messages:
            reference_time = _parse_iso(messages[-1].get("timestamp"))
        if not reference_time:
            reference_time = row.get("updated_at") or row.get("created_at") or datetime.utcnow()

        recaps = await session_mgr.summarize_session_messages_with_quality(
            messages=messages,
            reference_time=reference_time
        )
        summary_text = (recaps.get("summary_text") or "").strip()
        bridge_text = (recaps.get("bridge_text") or "").strip()

        if not summary_text:
            skipped += 1
            continue

        processed += 1
        if args.dry_run:
            continue

        await graph.add_session_summary(
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            summary_text=summary_text,
            bridge_text=bridge_text,
            reference_time=reference_time,
            episode_uuid=None,
            extra_attributes={
                "summary_quality_tier": recaps.get("summary_quality_tier"),
                "summary_source": recaps.get("summary_source"),
                "backfilled_at": datetime.utcnow().isoformat(),
                "backfill_version": "session_summary_v2"
            },
            replace_existing_session=args.force
        )
        created += 1

    print(
        f"backfill: processed={processed} created={created} skipped={skipped} dry_run={args.dry_run}"
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Graphiti SessionSummary nodes.")
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--user-id")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--since", help="ISO timestamp; only sessions updated after this time")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true", help="Create even if summary exists")
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
