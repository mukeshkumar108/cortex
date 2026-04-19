#!/usr/bin/env python3
"""Backfill and maintain session_classifications.memory_delta_embedding."""

from __future__ import annotations

import argparse
import asyncio
import re
from typing import Any, List

from src.config import get_settings
from src.db import Database
from src.episodic_memory import embed_texts


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    return re.sub(r"\s+", " ", value).strip()


def _format_embedding(vector: List[float]) -> str:
    return "[" + ",".join(str(float(x)) for x in vector) + "]"


async def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill memory_delta_embedding for memory-worthy sessions")
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args()

    settings = get_settings()
    model = _normalize_text(settings.memory_semantic_embedding_model) or "text-embedding-3-small"
    db = Database()
    embedded = 0
    skipped_empty = 0
    skipped_failed = 0
    try:
        rows = await db.fetch(
            """
            SELECT
              session_id,
              COALESCE(
                (
                  SELECT string_agg(value, ' ')
                  FROM jsonb_array_elements_text(COALESCE(raw_triage_output->'memory_deltas', '[]'::jsonb))
                ),
                one_line_summary,
                ''
              ) AS embedding_text
            FROM session_classifications
            WHERE user_id = $1
              AND is_memory_worthy = true
              AND memory_delta_embedding IS NULL
            ORDER BY session_date ASC
            LIMIT $2
            """,
            args.user_id,
            max(1, int(args.limit)),
        )

        for row in rows:
            text = _normalize_text(row.get("embedding_text"))
            if not text:
                skipped_empty += 1
                continue
            vectors = await embed_texts([text], model=model)
            if not vectors or not isinstance(vectors, list) or not vectors or not isinstance(vectors[0], list):
                skipped_failed += 1
                continue
            await db.execute(
                """
                UPDATE session_classifications
                SET memory_delta_embedding = $1::vector
                WHERE session_id = $2
                """,
                _format_embedding(vectors[0]),
                _normalize_text(row.get("session_id")),
            )
            embedded += 1

        print("Episodic Embeddings Complete")
        print("===========================")
        print(f"user_id={args.user_id}")
        print(f"model={model}")
        print(f"candidates={len(rows)}")
        print(f"embedded={embedded}")
        print(f"skipped_empty={skipped_empty}")
        print(f"skipped_failed={skipped_failed}")
        return 0
    finally:
        await db.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
