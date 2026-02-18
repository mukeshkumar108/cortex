#!/usr/bin/env python3
import argparse
import asyncio

from src.db import Database
from src.graphiti_client import GraphitiClient
from src import session as session_module


async def run(args: argparse.Namespace) -> int:
    db = Database()
    graph = GraphitiClient()
    await graph.initialize()

    rows = await db.fetch(
        """
        SELECT tenant_id, user_id, session_id
        FROM session_buffer
        WHERE closed_at IS NULL
        ORDER BY updated_at DESC
        LIMIT $1
        """,
        args.limit
    )

    closed = 0
    for row in rows:
        if args.tenant_id and row["tenant_id"] != args.tenant_id:
            continue
        if args.user_id and row["user_id"] != args.user_id:
            continue
        ok = await session_module.close_session(
            tenant_id=row["tenant_id"],
            session_id=row["session_id"],
            user_id=row["user_id"],
            graphiti_client=graph,
            persona_id=args.persona_id
        )
        if ok:
            closed += 1

    print(f"force_close: found={len(rows)} closed={closed}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Force close open sessions in session_buffer.")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--tenant-id")
    parser.add_argument("--user-id")
    parser.add_argument("--persona-id")
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
