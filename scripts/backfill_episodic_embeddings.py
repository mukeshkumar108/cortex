#!/usr/bin/env python3
"""Backfill episodic transcript-window embeddings from session_transcript rows."""

from __future__ import annotations

import argparse
import asyncio
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from src.config import get_settings
from src.db import Database
from src.episodic_memory import upsert_session_episode_embeddings
from src.graphiti_client import GraphitiClient


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    return re.sub(r"\s+", " ", value).strip()


def _extract_session_id_from_episode_name(name: Any) -> str:
    clean = _normalize_text(name)
    if not clean:
        return ""
    match = re.match(r"^session_raw_(.+?)(?:_\d{9,})?$", clean)
    if not match:
        return ""
    return _normalize_text(match.group(1))


def _parse_reference_time(messages: List[Dict[str, Any]], updated_at: Optional[datetime]) -> datetime:
    for row in reversed(messages):
        if not isinstance(row, dict):
            continue
        ts = _normalize_text(row.get("timestamp"))
        if not ts:
            continue
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            continue
    return updated_at or datetime.utcnow()


async def _load_transcript_rows(
    db: Database,
    *,
    tenant_id: str,
    user_id: str,
    limit: int,
) -> List[Dict[str, Any]]:
    return await db.fetch(
        """
        SELECT tenant_id, user_id, session_id, messages, updated_at
        FROM session_transcript
        WHERE tenant_id = $1 AND user_id = $2
        ORDER BY updated_at DESC
        LIMIT $3
        """,
        tenant_id,
        user_id,
        limit,
    )


async def _load_episode_map(
    graphiti_client: GraphitiClient,
    *,
    tenant_id: str,
    user_id: str,
    limit: int,
) -> Dict[str, str]:
    episodes = await graphiti_client.get_recent_episodes(
        tenant_id=tenant_id,
        user_id=user_id,
        since=None,
        limit=limit,
    )
    out: Dict[str, str] = {}
    for row in episodes or []:
        if isinstance(row, dict):
            name = _normalize_text(row.get("name") or row.get("episode_name"))
            episode_uuid = _normalize_text(row.get("uuid"))
        else:
            name = _normalize_text(getattr(row, "name", None) or getattr(row, "episode_name", None))
            episode_uuid = _normalize_text(getattr(row, "uuid", None))
        session_id = _extract_session_id_from_episode_name(name)
        if not session_id:
            continue
        if episode_uuid and session_id not in out:
            out[session_id] = episode_uuid
    return out


async def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill episodic_memory_embeddings from transcripts")
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--limit", type=int, default=2000)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    settings = get_settings()
    graphiti_client = GraphitiClient()
    db = Database()
    await graphiti_client.initialize()
    try:
        safe_limit = max(1, min(int(args.limit), 10000))
        transcript_rows = await _load_transcript_rows(
            db,
            tenant_id=args.tenant_id,
            user_id=args.user_id,
            limit=safe_limit,
        )
        episode_map = await _load_episode_map(
            graphiti_client,
            tenant_id=args.tenant_id,
            user_id=args.user_id,
            limit=max(safe_limit, 5000),
        )
        processed = 0
        skipped_empty = 0
        indexed = 0
        attempted = 0
        errors = 0
        for row in transcript_rows or []:
            messages = row.get("messages")
            if isinstance(messages, str):
                try:
                    messages = json.loads(messages)
                except Exception:
                    messages = []
            if not isinstance(messages, list) or not messages:
                skipped_empty += 1
                continue

            processed += 1
            session_id = _normalize_text(row.get("session_id"))
            reference_time = _parse_reference_time(messages, row.get("updated_at"))
            if args.dry_run:
                continue
            try:
                result = await upsert_session_episode_embeddings(
                    db=db,
                    tenant_id=args.tenant_id,
                    user_id=args.user_id,
                    session_id=session_id,
                    episode_uuid=episode_map.get(session_id),
                    reference_time=reference_time,
                    messages=messages,
                    model=settings.episodic_embedding_model,
                    window_size=int(settings.episodic_embedding_window_size),
                    stride=int(settings.episodic_embedding_stride),
                    max_windows=int(settings.episodic_embedding_max_windows),
                    max_chars=int(settings.episodic_embedding_max_chars),
                    user_turn_centric=bool(settings.episodic_embedding_user_turn_centric),
                    include_assistant_turns=bool(settings.episodic_embedding_include_assistant_turns),
                    assistant_char_weight=float(settings.episodic_embedding_assistant_char_weight),
                )
                indexed += int(result.get("indexed") or 0)
                attempted += int(result.get("attempted") or 0)
            except Exception:
                errors += 1

        print(
            json.dumps(
                {
                    "tenant_id": args.tenant_id,
                    "user_id": args.user_id,
                    "dry_run": bool(args.dry_run),
                    "transcripts_seen": len(transcript_rows or []),
                    "sessions_with_episode_uuid": len(episode_map),
                    "processed_non_empty": processed,
                    "skipped_empty": skipped_empty,
                    "embedding_windows_indexed": indexed,
                    "embedding_windows_attempted": attempted,
                    "errors": errors,
                },
                indent=2,
            )
        )
        return 0
    finally:
        await db.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
