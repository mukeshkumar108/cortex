#!/usr/bin/env python3
import argparse
import asyncio
from typing import Any, Dict, Optional

from src.graphiti_client import GraphitiClient
from src.session import SessionManager
from src.db import Database


async def _update_bridge_text(driver: Any, uuid: str, bridge_text: str) -> None:
    await driver.execute_query(
        """
        MATCH (n {uuid: $uuid})
        SET n.bridge_text = $bridge_text
        RETURN n
        """,
        uuid=uuid,
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
        bridge_text = props.get("bridge_text")
        if not uuid or not summary:
            continue
        if bridge_text and not args.force:
            continue
        processed += 1
        if args.dry_run:
            continue
        new_bridge = await mgr._summarize_session_bridge(summary)
        if not new_bridge:
            new_bridge = summary
        if new_bridge:
            await _update_bridge_text(driver, uuid, new_bridge)
            updated += 1

    print(f"bridge_backfill: processed={processed} updated={updated} dry_run={args.dry_run}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill bridge_text for SessionSummary nodes.")
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
