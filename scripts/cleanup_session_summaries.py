#!/usr/bin/env python3
import argparse
import asyncio
import re
from typing import Any, Dict, List, Optional

from src.graphiti_client import GraphitiClient
from src.falkor_utils import extract_node_dicts


def _normalize_summary(text: str) -> str:
    if not text:
        return text
    # Replace Sophie (case-insensitive) with User, preserving possessive
    text = re.sub(r"\bSophie\b", "User", text)
    text = re.sub(r"\bSOPHIE\b", "User", text)
    text = re.sub(r"\bSophie['’]s\b", "User's", text)
    text = re.sub(r"\bsophie\b", "User", text)
    text = re.sub(r"\bsophie['’]s\b", "User's", text)
    # Normalize pronouns: she/her -> he/him/his
    text = re.sub(r"\bShe\b", "He", text)
    text = re.sub(r"\bshe\b", "he", text)
    text = re.sub(r"\bHer\b", "His", text)
    text = re.sub(r"\bher\b", "his", text)
    text = re.sub(r"\bHers\b", "His", text)
    text = re.sub(r"\bhers\b", "his", text)
    return text


async def _update_node(driver: Any, uuid: str, summary: str, summary_text: Optional[str]) -> None:
    await driver.execute_query(
        """
        MATCH (n {uuid: $uuid})
        SET n.summary = $summary,
            n.summary_text = $summary_text
        RETURN n
        """,
        uuid=uuid,
        summary=summary,
        summary_text=summary_text
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
            RETURN n.uuid AS uuid, n.summary AS summary, n.summary_text AS summary_text
            """,
            group_id=group_id
        )
    else:
        rows = await driver.execute_query(
            """
            MATCH (n:SessionSummary)
            RETURN n.uuid AS uuid, n.summary AS summary, n.summary_text AS summary_text
            """
        )

    total = 0
    changed = 0
    seen = set()

    for row in rows or []:
        for node in extract_node_dicts(row, required_keys=("uuid", "summary")):
            uuid = node.get("uuid")
            summary = node.get("summary")
            summary_text = node.get("summary_text")
            if uuid in ("uuid", None) or summary in ("summary", None) or not isinstance(summary, str):
                continue
            if uuid in seen:
                continue
            seen.add(uuid)
            total += 1
            new_summary = _normalize_summary(summary)
            new_summary_text = _normalize_summary(summary_text) if isinstance(summary_text, str) else summary_text
            if new_summary != summary or (isinstance(summary_text, str) and new_summary_text != summary_text):
                changed += 1
                if not args.dry_run:
                    await _update_node(driver, uuid, new_summary, new_summary_text)

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
