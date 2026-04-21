#!/usr/bin/env python3
"""Run Pass 4 identity synthesis using the shared live pass implementation."""

from __future__ import annotations

import argparse
import asyncio
import json

from src.config import get_settings
from src.db import Database
from src.derived_pipeline import run_pass4_identity


async def run_once(*, user_id: str, tenant_id: str = "default") -> dict:
    db = Database()
    try:
        result = await run_pass4_identity(db=db, tenant_id=tenant_id, user_id=user_id, settings=get_settings())
        return {"run_id": result.run_id, "pass_name": result.pass_name, "output_hash": result.output_hash}
    finally:
        await db.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--tenant-id", default="default")
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run_once(user_id=args.user_id, tenant_id=args.tenant_id)), ensure_ascii=False))


if __name__ == "__main__":
    main()
