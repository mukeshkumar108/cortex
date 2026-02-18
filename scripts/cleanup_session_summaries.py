#!/usr/bin/env python3
import argparse
import asyncio
import re
from typing import Any, Dict, List, Optional

from src.graphiti_client import GraphitiClient


def _normalize_summary(text: str) -> str:
    if not text:
        return text
    # Replace Sophie (case-insensitive) with User, preserving possessive
    text = re.sub(r"\bSophie\b", "User", text)
    text = re.sub(r"\bSOPHIE\b", "User", text)
    text = re.sub(r"\bSophie['’]s\b", "User's", text)
    text = re.sub(r"\bsophie\b", "User", text)
    text = re.sub(r"\bsophie['’]s\b", "User's", text)
    return text


async def _update_node(driver: Any, uuid: str, summary: str) -> None:
    await driver.execute_query(
        """
        MATCH (n {uuid: $uuid})
        SET n.summary = $summary
        RETURN n
        """,
        uuid=uuid,
        summary=summary
    )


async def run(args: argparse.Namespace) -> int:
    graph = GraphitiClient()
    await graph.initialize()
    if not graph.client:
        raise RuntimeError("Graphiti client unavailable")
    driver = getattr(graph.client, "driver", None)
    if not driver:
        raise RuntimeError("Graphiti driver unavailable")

    params: Dict[str, Any] = {}
    if args.tenant_id and args.user_id:
        group_id = graph._make_composite_user_id(args.tenant_id, args.user_id)
        rows = await driver.execute_query(
            """
            MATCH (n:SessionSummary {group_id: $group_id})
            RETURN n.uuid AS uuid, n.summary AS summary
            """,
            group_id=group_id
        )
    else:
        rows = await driver.execute_query(
            """
            MATCH (n:SessionSummary)
            RETURN n.uuid AS uuid, n.summary AS summary
            """
        )

    total = 0
    changed = 0
    for row in rows or []:
        uuid = None
        summary = None
        if isinstance(row, dict):
            # Handle nested dict row shapes
            if any(isinstance(v, dict) for v in row.values()):
                for v in row.values():
                    if isinstance(v, dict) and v.get("uuid") and v.get("summary"):
                        uuid = v.get("uuid")
                        summary = v.get("summary")
                        break
            else:
                uuid = row.get("uuid")
                summary = row.get("summary")
        elif isinstance(row, (list, tuple)):
            # Sometimes rows are list-of-dicts
            for item in row:
                if isinstance(item, dict) and item.get("uuid") and item.get("summary"):
                    uuid = item.get("uuid")
                    summary = item.get("summary")
                    break
            if uuid is None:
                if len(row) > 0:
                    uuid = row[0]
                if len(row) > 1:
                    summary = row[1]
        if uuid in ("uuid", None) or summary in ("summary", None) or not isinstance(summary, str):
            continue
        total += 1
        new_summary = _normalize_summary(summary)
        if new_summary != summary:
            changed += 1
            if not args.dry_run:
                await _update_node(driver, uuid, new_summary)

    print(f"cleanup: total={total} changed={changed} dry_run={args.dry_run}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Cleanup SessionSummary text (replace Sophie -> User).")
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
