#!/usr/bin/env python3
"""Score entity profiles and open threads for a user."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from src.db import Database


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    return " ".join(value.split()).strip()


def _coerce_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    txt = _normalize_text(value)
    if not txt:
        return None
    try:
        parsed = datetime.fromisoformat(txt.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return None


def calculate_entity_scores(entity: Dict[str, Any]) -> Dict[str, float]:
    core_by_relationship = {
        "daughter": 0.95,
        "son": 0.95,
        "mother": 0.90,
        "father": 0.90,
        "girlfriend": 0.90,
        "boyfriend": 0.90,
        "wife": 0.95,
        "husband": 0.95,
        "active_project": 0.80,
        "ai_assistant": 0.75,
        "friend": 0.65,
        "colleague": 0.50,
        "other": 0.35,
        "none": 0.25,
    }

    relationship = _normalize_text(entity.get("relationship_to_user")).lower() or "other"
    core = core_by_relationship.get(relationship, 0.35)

    mention_count = int(entity.get("mention_count") or 0)
    if mention_count > 10:
        core = min(core + 0.10, 1.0)
    elif mention_count > 5:
        core = min(core + 0.05, 1.0)

    if bool(entity.get("has_open_threads")):
        core = min(core + 0.05, 1.0)

    last_seen_at = _coerce_datetime(entity.get("last_seen_at"))
    if last_seen_at:
        days_stale = (datetime.now(timezone.utc) - last_seen_at).days
    else:
        days_stale = 999

    if days_stale < 7:
        operational = 1.0
    elif days_stale < 30:
        operational = 0.8
    elif days_stale < 60:
        operational = 0.6
    elif days_stale < 90:
        operational = 0.4
    else:
        operational = 0.2

    importance = (core * 0.7) + (operational * 0.3)
    salience = importance
    if days_stale > 90:
        salience = max(salience - 0.3, 0.1)
    elif days_stale > 60:
        salience = max(salience - 0.2, 0.1)

    return {
        "core_importance": round(core, 3),
        "operational_priority": round(operational, 3),
        "importance_score": round(importance, 3),
        "salience_score": round(salience, 3),
    }


def calculate_thread_scores(thread: Dict[str, Any]) -> Dict[str, float]:
    now = datetime.now(timezone.utc)

    priority_base = {
        "high": 0.85,
        "medium": 0.55,
        "low": 0.30,
    }
    priority = _normalize_text(thread.get("priority")).lower() or "medium"
    base = priority_base.get(priority, 0.55)

    times_mentioned = int(thread.get("times_mentioned") or 1)
    if times_mentioned >= 5:
        base = min(base + 0.15, 1.0)
    elif times_mentioned >= 3:
        base = min(base + 0.08, 1.0)

    category = _normalize_text(thread.get("category")).lower()
    category_boosts = {
        "health": 0.10,
        "relationship": 0.08,
        "commitment": 0.05,
        "assistant_feedback": 0.0,
    }
    boost = category_boosts.get(category, 0.0)
    importance = min(base + boost, 1.0)

    salience = importance
    follow_up_after = _coerce_datetime(thread.get("follow_up_after"))
    last_mentioned_at = _coerce_datetime(thread.get("last_mentioned_at"))
    days_stale = (now - last_mentioned_at).days if last_mentioned_at else 999
    thread_type = _normalize_text(thread.get("thread_type")).lower() or "situational"

    if follow_up_after:
        days_overdue = (now - follow_up_after).days
        days_until = (follow_up_after - now).days

        if days_until > 0:
            if days_until <= 3:
                salience = min(importance + 0.2, 1.0)
            elif days_until <= 7:
                salience = min(importance + 0.1, 1.0)
            else:
                if days_stale > 60:
                    salience = max(salience - 0.3, 0.1)
                elif days_stale > 30:
                    salience = max(salience - 0.15, 0.1)
        elif days_overdue <= 7:
            salience = min(importance + 0.2, 1.0)
        elif days_overdue <= 30:
            if days_stale > 14:
                salience = max(salience - 0.2, 0.1)
        else:
            if days_stale > 30 and thread_type != "persistent_goal":
                salience = max(salience - 0.4, 0.05)
    else:
        if days_stale > 60:
            salience = max(salience - 0.3, 0.1)
        elif days_stale > 30:
            salience = max(salience - 0.15, 0.1)

    if thread_type == "persistent_goal":
        salience = max(salience, 0.3)

    should_auto_snooze = (
        thread_type != "persistent_goal"
        and days_stale > 30
        and (
            follow_up_after is None
            or (now - follow_up_after).days > 30
        )
    )

    return {
        "importance_score": round(importance, 3),
        "salience_score": round(salience, 3),
        "should_auto_snooze": should_auto_snooze,
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run scoring update for entities and threads")
    parser.add_argument("--user-id", required=True)
    args = parser.parse_args()

    db = Database()
    entities_scored = 0
    threads_scored = 0
    threads_auto_snoozed = 0
    threads_promoted = 0
    try:
        entities = await db.fetch(
            """
            SELECT
              ep.entity_id,
              ep.canonical_name,
              ep.relationship_to_user,
              ep.mention_count,
              ep.last_seen_at,
              EXISTS (
                SELECT 1
                FROM open_threads ot
                WHERE ot.user_id = ep.user_id
                  AND ot.status = 'open'
                  AND ep.canonical_name = ANY(COALESCE(ot.related_entities, ARRAY[]::text[]))
              ) AS has_open_threads
            FROM entity_profiles ep
            WHERE ep.user_id = $1
            """,
            args.user_id,
        )
        for entity in entities:
            scores = calculate_entity_scores(entity)
            await db.execute(
                """
                UPDATE entity_profiles SET
                  core_importance = $1,
                  operational_priority = $2,
                  importance_score = $3,
                  salience_score = $4
                WHERE entity_id = $5
                """,
                scores["core_importance"],
                scores["operational_priority"],
                scores["importance_score"],
                scores["salience_score"],
                entity["entity_id"],
            )
            entities_scored += 1

        threads = await db.fetch(
            """
            SELECT
              thread_id, title, priority, category,
              times_mentioned, follow_up_after, last_mentioned_at, thread_type, status
            FROM open_threads
            WHERE user_id = $1
            """,
            args.user_id,
        )
        for thread in threads:
            scores = calculate_thread_scores(thread)
            await db.execute(
                """
                UPDATE open_threads SET
                  importance_score = $1,
                  salience_score = $2
                WHERE thread_id = $3
                """,
                scores["importance_score"],
                scores["salience_score"],
                thread["thread_id"],
            )
            threads_scored += 1
            if scores.get("should_auto_snooze"):
                updated = await db.execute(
                    """
                    UPDATE open_threads SET
                      status = 'snoozed',
                      last_updated_at = now()
                    WHERE thread_id = $1
                      AND thread_type != 'persistent_goal'
                      AND status = 'open'
                    """,
                    thread["thread_id"],
                )
                if _normalize_text(updated).upper().startswith("UPDATE 1"):
                    threads_auto_snoozed += 1

        promote_rows = await db.fetch(
            """
            SELECT thread_id
            FROM open_threads
            WHERE user_id = $1
              AND thread_type = 'situational'
              AND times_mentioned >= 4
              AND category IN ('goal', 'commitment', 'health')
            """,
            args.user_id,
        )
        for row in promote_rows:
            updated = await db.execute(
                """
                UPDATE open_threads SET
                  thread_type = 'persistent_goal',
                  last_updated_at = now()
                WHERE thread_id = $1
                """,
                row["thread_id"],
            )
            if _normalize_text(updated).upper().startswith("UPDATE 1"):
                threads_promoted += 1

        top_entities = await db.fetch(
            """
            SELECT
              canonical_name,
              importance_score,
              salience_score,
              core_importance,
              operational_priority
            FROM entity_profiles
            WHERE user_id = $1
            ORDER BY salience_score DESC NULLS LAST, importance_score DESC NULLS LAST
            LIMIT 10
            """,
            args.user_id,
        )
        top_threads = await db.fetch(
            """
            SELECT
              title,
              priority,
              importance_score,
              salience_score,
              follow_up_after
            FROM open_threads
            WHERE user_id = $1
              AND status IN ('open', 'snoozed')
            ORDER BY salience_score DESC NULLS LAST, importance_score DESC NULLS LAST
            LIMIT 10
            """,
            args.user_id,
        )

        print("Scoring Update Complete")
        print("=======================")
        print(f"Entities scored: {entities_scored}")
        print(f"Threads scored: {threads_scored}")
        print(f"Threads auto-snoozed: {threads_auto_snoozed}")
        print(f"Threads promoted to persistent_goal: {threads_promoted}")
        print("")
        print("Top entities by salience:")
        for row in top_entities:
            print(
                f"{row.get('canonical_name')} | {row.get('importance_score')} | "
                f"{row.get('salience_score')} | {row.get('core_importance')} | {row.get('operational_priority')}"
            )
        print("")
        print("Top threads by salience:")
        for row in top_threads:
            print(
                f"{row.get('title')} | {row.get('priority')} | "
                f"{row.get('importance_score')} | {row.get('salience_score')} | {row.get('follow_up_after')}"
            )
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
