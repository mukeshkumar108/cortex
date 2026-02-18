#!/usr/bin/env python3
import argparse
import asyncio
from typing import Any, Dict, Optional

from src.graphiti_client import GraphitiClient
from src.session import SessionManager
from src.db import Database


async def _update_summary_node(
    driver: Any,
    uuid: str,
    summary_text: str,
    bridge_text: str
) -> None:
    await driver.execute_query(
        """
        MATCH (n {uuid: $uuid})
        SET n.summary = $summary_text,
            n.summary_text = $summary_text,
            n.bridge_text = $bridge_text
        RETURN n
        """,
        uuid=uuid,
        summary_text=summary_text,
        bridge_text=bridge_text
    )


async def run(args: argparse.Namespace) -> int:
    graph = GraphitiClient()
    await graph.initialize()
    if not graph.client:
        raise RuntimeError("Graphiti client unavailable")
    driver = getattr(graph.client, "driver", None)
    if not driver:
        raise RuntimeError("Graphiti driver unavailable")

    mgr = SessionManager(Database())
    group_id = graph._make_composite_user_id(args.tenant_id, args.user_id)

    rows = await driver.execute_query(
        """
        MATCH (n:SessionSummary {group_id: $group_id})
        RETURN properties(n) AS props
        ORDER BY n.created_at DESC
        """,
        group_id=group_id
    )

    processed = 0
    updated = 0
    for row in rows or []:
        props: Optional[Dict[str, Any]] = None
        if isinstance(row, dict):
            props = row.get("props") if isinstance(row.get("props"), dict) else None
        elif isinstance(row, (list, tuple)) and row:
            props = row[0] if isinstance(row[0], dict) else None
        if not props:
            continue

        uuid = props.get("uuid")
        summary = props.get("summary_text") or props.get("summary")
        if not uuid or not summary:
            continue

        if not args.force and not mgr._looks_like_transcript(summary):
            continue

        processed += 1
        if args.dry_run:
            continue

        new_summary = await mgr._rewrite_summary_from_text(summary)
        if not new_summary:
            continue
        if mgr._looks_like_transcript(new_summary):
            continue

        bridge_text = await mgr._summarize_session_bridge(new_summary)
        await _update_summary_node(driver, uuid, new_summary, bridge_text or "")
        updated += 1

    print(f"summary_redo: processed={processed} updated={updated} dry_run={args.dry_run}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rewrite transcript-like SessionSummary nodes into narrative summaries."
    )
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true", help="Rewrite all summaries, not just transcript-like.")
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
