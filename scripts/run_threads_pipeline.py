#!/usr/bin/env python3
"""Open threads pipeline for Synapse."""

from __future__ import annotations

import argparse
import asyncio
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from src.config import get_settings
from src.db import Database


THREAD_EXTRACTION_PROMPT = """You are maintaining an open thread registry for a
personal AI assistant named Sophie.

Open threads are unresolved things a caring attentive
friend would remember and follow up on later.

TODAY'S SESSION:
Date: {session_date}
Emotional weight: {emotional_weight}
Emotional note: {emotional_note}
Thread signals from triage: {thread_signals}

USER TURNS FROM THIS SESSION (assistant turns removed):
{transcript_text}

EXISTING OPEN THREADS (so you can update not duplicate):
{existing_threads}

Your job:

For each meaningful unresolved thing in this session,
decide one of:

CREATE — new thread not in the existing list
UPDATE — this session adds new info to an existing thread
RESOLVE — this session indicates a thread is now resolved
SNOOZE — thread is still open but not actively relevant
NO_ACTION — thread still open, nothing new to add

A thread is worth creating if it is:
- A health issue, symptom, or medical situation
- A relationship tension or unresolved situation
- A commitment or intention the user stated
- A worry or fear the user expressed
- Something in progress with no clear resolution
- Something a good friend would ask about next time

A thread is NOT worth creating if it is:
- A passing comment with no ongoing significance
- A resolved fact (Ashley is back together —
  that's a memory_delta not a thread)
- Technical work on Sophie's codebase
- Generic emotional states without specific situation
- Something the user clearly resolved in this same session

CATEGORY options:
health / relationship / goal / commitment /
worry / project / other

PRIORITY:
high = time-sensitive or emotionally significant
medium = worth following up within a week or two
low = worth noting but not urgent

For follow_up_after:
- health issues: 3-7 days
- relationship situations: 5-10 days
- goals/commitments: 7-14 days
- worries: 3-7 days
- Use session_date as the base date

RULES:
1. Only use facts stated by the USER.
   Ignore assistant turns completely.
2. Be specific. "User's leg was hurting" not
   "User mentioned a health issue."
3. One thread per distinct situation.
   Don't merge unrelated things into one thread.
4. Don't create threads for things already resolved
   in the same session.
5. If uncertain whether something is a thread —
   lean toward creating it. Better to have a low-priority
   thread than to miss something important.
6. Resolution note should be specific:
   "User confirmed kidney stones resolved,
    just needs to drink more water"
   not just "resolved"

Return JSON only — no preamble, no markdown:
{{
  "actions": [
    {{
      "action": "CREATE",
      "title": "User's leg was hurting",
      "detail": "User mentioned their leg had been painful.",
      "category": "health",
      "priority": "medium",
      "related_entities": ["Ashley"],
      "follow_up_after": "2026-04-14",
      "source_session_id": "session-id-here"
    }},
    {{
      "action": "UPDATE",
      "thread_id": "existing-thread-uuid",
      "detail": "User confirmed kidney stones resolved but needs hydration.",
      "last_mentioned_at": "2026-04-09",
      "follow_up_after": "2026-04-16"
    }},
    {{
      "action": "RESOLVE",
      "thread_id": "existing-thread-uuid",
      "resolution_note": "Ashley visited England, they reconciled."
    }}
  ]
}}
"""


THREAD_AUDIT_PROMPT = """You are auditing an open thread registry.

Current open threads:
{open_threads}

YOUR PRIMARY JOB IS DEDUPLICATION.

Read every thread carefully. For each group of threads
that describe the same underlying situation, merge them
into one. Keep the richest, most detailed version.

Two threads are the SAME situation if they describe:
- The same health issue (even if worded differently)
- The same relationship tension or situation
- The same goal or commitment
- The same worry about the same thing
- The same frustration or recurring pattern

MERGE aggressively. One rich thread beats three thin ones.

SPECIAL RULE — assistant_feedback category:
Any thread about Sophie's failures, errors,
hallucinations, or behavioral problems should be:
1. Merged into a SINGLE assistant_feedback thread
2. Category set to assistant_feedback
3. NOT treated as a personal thread about the user
These threads inform Sophie's behavior but should
not dominate the user's personal handover context.

RESOLVE threads that are clearly complete:
- Technical work that is mentioned as finished
- Health issues confirmed resolved
- Situations where a later thread confirms resolution

SNOOZE threads that are stale:
- follow_up_after more than 45 days ago
- Not mentioned in last 30 days
- No active signal of ongoing relevance

CRITICAL — do NOT merge:
- Different health issues even if both are health
- Different people even if both are relationship
- A parent and their child
- Things that are related but genuinely distinct

Return JSON only:
{{
  "audit_actions": [
    {{
      "action": "MERGE",
      "keep_thread_id": "uuid",
      "absorb_thread_ids": ["uuid1", "uuid2"],
      "merged_title": "clean title for merged thread",
      "merged_detail": "combined detail from all threads",
      "merged_category": "correct category",
      "reason": "all describe same Sophie trust breakdown"
    }},
    {{
      "action": "RESOLVE",
      "thread_id": "uuid",
      "resolution_note": "specific reason"
    }},
    {{
      "action": "SNOOZE",
      "thread_id": "uuid",
      "reason": "stale"
    }},
    {{
      "action": "CATEGORY_FIX",
      "thread_id": "uuid",
      "new_category": "assistant_feedback",
      "reason": "about Sophie behavior not user's life"
    }}
  ]
}}
"""


VALID_PRIORITIES = {"high", "medium", "low"}
VALID_CATEGORIES = {"health", "relationship", "goal", "commitment", "worry", "project", "assistant_feedback", "other"}


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    return " ".join(value.split()).strip()


def _extract_json_text(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        return raw
    fenced = re.match(r"^```(?:json)?\s*(.*?)\s*```$", raw, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        return fenced.group(1).strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start != -1 and end != -1 and end > start:
        return raw[start : end + 1].strip()
    return raw


def _safe_json(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _to_text_list(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    out: List[str] = []
    for item in value:
        txt = _normalize_text(item)
        if txt:
            out.append(txt)
    return out


def _append_error(errors_file: Path, payload: Dict[str, Any]) -> None:
    with errors_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _format_user_turns(messages: List[Dict[str, Any]], created_at: Any) -> str:
    ts_label = ""
    if isinstance(created_at, datetime):
        dt = created_at.astimezone(timezone.utc)
        ts_label = dt.strftime("%Y-%m-%d %H:%M")
    elif created_at:
        ts_label = _normalize_text(created_at)

    lines: List[str] = []
    for m in messages or []:
        if not isinstance(m, dict):
            continue
        if _normalize_text(m.get("role")).lower() != "user":
            continue
        text = m.get("text")
        if text is None:
            continue
        lines.append(f"[{ts_label}]\nUser: {text}")
    return "\n".join(lines).strip()


def _coerce_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    txt = _normalize_text(value)
    if not txt:
        return None
    try:
        txt = txt.replace("Z", "+00:00")
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _default_followup(session_date: datetime, category: str) -> datetime:
    category = (category or "").lower()
    if category == "health":
        return session_date + timedelta(days=5)
    if category == "relationship":
        return session_date + timedelta(days=7)
    if category == "goal":
        return session_date + timedelta(days=10)
    if category == "commitment":
        return session_date + timedelta(days=10)
    if category == "worry":
        return session_date + timedelta(days=5)
    return session_date + timedelta(days=10)


async def _call_openrouter_json(
    *,
    api_key: str,
    model: str,
    prompt: str,
    timeout_seconds: float = 120.0,
) -> Dict[str, Any]:
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "response_format": {"type": "json_object"},
        "temperature": 0.1,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            response = await client.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=headers,
                json=payload,
            )
    except Exception as exc:
        return {
            "ok": False,
            "status_code": None,
            "raw_output": "",
            "parsed": None,
            "error": f"openrouter_request_failed: {exc}",
        }

    body: Dict[str, Any] = {}
    try:
        body = response.json()
    except Exception:
        body = {}
    content = _normalize_text((((body.get("choices") or [{}])[0]).get("message") or {}).get("content"))
    json_text = _extract_json_text(content)
    parsed = None
    parse_error = None
    if json_text:
        try:
            parsed = json.loads(json_text)
        except Exception as exc:
            parse_error = f"model_output_json_parse_failed: {exc}"
    else:
        parse_error = "empty_model_output"

    return {
        "ok": response.is_success and isinstance(parsed, dict),
        "status_code": response.status_code,
        "raw_output": content,
        "parsed": parsed if isinstance(parsed, dict) else None,
        "error": parse_error,
    }


async def _build_session_transcript(db: Database, session_id: str) -> tuple[str, Optional[datetime]]:
    rows = await db.fetch(
        """
        SELECT session_id, created_at, messages
        FROM session_transcript
        WHERE session_id = $1
        ORDER BY created_at ASC
        """,
        session_id,
    )
    blocks: List[str] = []
    last_ts: Optional[datetime] = None
    for row in rows:
        messages = row.get("messages")
        if not isinstance(messages, list):
            messages = []
        block = _format_user_turns(messages, row.get("created_at"))
        if block:
            blocks.append(block)
        c = row.get("created_at")
        if isinstance(c, datetime) and (last_ts is None or c > last_ts):
            last_ts = c
    return "\n\n".join(blocks).strip(), last_ts


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run open threads pipeline")
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--model", default="google/gemma-4-26b-a4b-it")
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    args = parser.parse_args()

    settings = get_settings()
    api_key = _normalize_text(settings.openrouter_api_key or settings.openai_api_key)
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY or OPENAI_API_KEY is required")

    now_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    errors_file = Path(f"tmp/threads_pipeline_errors_{now_stamp}.jsonl")
    errors_file.parent.mkdir(parents=True, exist_ok=True)

    db = Database()

    sessions_processed = 0
    created_count = 0
    updated_count = 0
    resolved_count = 0
    snoozed_count = 0
    audit_applied = 0
    errors_count = 0

    try:
        session_rows = await db.fetch(
            """
            SELECT
              sc.session_id,
              sc.session_date,
              sc.raw_triage_output->'thread_signals' as thread_signals,
              sc.raw_triage_output->'memory_deltas' as memory_deltas,
              sc.entity_mentions,
              sc.emotional_weight,
              sc.emotional_note
            FROM session_classifications sc
            WHERE sc.user_id = $1
              AND sc.is_memory_worthy = true
              AND sc.run_threads_pass = true
              AND sc.session_date > COALESCE(
                (
                  SELECT last_processed
                  FROM pipeline_checkpoints
                  WHERE user_id = $1
                    AND pipeline_name = 'threads_pipeline'
                ),
                '1970-01-01'::timestamptz
              )
            ORDER BY sc.session_date ASC
            """,
            args.user_id,
        )

        if not session_rows:
            print("No new thread-candidate sessions")

        for row in session_rows:
            session_id = _normalize_text(row.get("session_id"))
            session_date = row.get("session_date")
            if not session_id or not isinstance(session_date, datetime):
                continue
            sessions_processed += 1

            existing_threads = await db.fetch(
                """
                SELECT
                  thread_id,
                  title,
                  detail,
                  status,
                  category,
                  priority,
                  related_entities,
                  last_mentioned_at,
                  follow_up_after
                FROM open_threads
                WHERE user_id = $1
                  AND status IN ('open', 'snoozed')
                ORDER BY last_mentioned_at DESC NULLS LAST
                """,
                args.user_id,
            )
            existing_lines = []
            for t in existing_threads:
                existing_lines.append(
                    " | ".join(
                        [
                            _normalize_text(t.get("thread_id")),
                            _normalize_text(t.get("title")),
                            _normalize_text(t.get("status")),
                            _normalize_text(t.get("last_mentioned_at")),
                            _normalize_text(t.get("category")),
                        ]
                    )
                )
            if not existing_lines:
                existing_lines = ["(none)"]

            transcript_text, transcript_last = await _build_session_transcript(db, session_id)
            if not transcript_text:
                continue
            prompt = THREAD_EXTRACTION_PROMPT.format(
                session_date=session_date.isoformat(),
                emotional_weight=_normalize_text(row.get("emotional_weight")) or "none",
                emotional_note=_normalize_text(row.get("emotional_note")) or "(none)",
                thread_signals=json.dumps(row.get("thread_signals") or [], ensure_ascii=False),
                transcript_text=transcript_text,
                existing_threads="\n".join(existing_lines),
            )
            result = await _call_openrouter_json(
                api_key=api_key,
                model=args.model,
                prompt=prompt,
                timeout_seconds=args.timeout_seconds,
            )
            if not result.get("ok"):
                errors_count += 1
                _append_error(
                    errors_file,
                    {
                        "stage": "session_extract",
                        "session_id": session_id,
                        "status_code": result.get("status_code"),
                        "error": result.get("error"),
                        "raw_output": result.get("raw_output"),
                    },
                )
                continue

            parsed = _safe_json(result.get("parsed"))
            actions = parsed.get("actions") if isinstance(parsed.get("actions"), list) else []
            canonical_entities = await db.fetch(
                """
                SELECT canonical_name
                FROM entity_profiles
                WHERE user_id = $1
                """,
                args.user_id,
            )
            canonical_set = {
                _normalize_text(r.get("canonical_name"))
                for r in canonical_entities
                if _normalize_text(r.get("canonical_name"))
            }

            for action in actions:
                if not isinstance(action, dict):
                    continue
                kind = _normalize_text(action.get("action")).upper()
                try:
                    if kind == "CREATE":
                        title = _normalize_text(action.get("title"))
                        detail = _normalize_text(action.get("detail")) or None
                        if not title:
                            continue
                        category = _normalize_text(action.get("category")).lower() or "other"
                        if category not in VALID_CATEGORIES:
                            category = "other"
                        priority = _normalize_text(action.get("priority")).lower() or "medium"
                        if priority not in VALID_PRIORITIES:
                            priority = "medium"
                        related_entities = _to_text_list(action.get("related_entities"))
                        related_entities = [x for x in related_entities if x in canonical_set]
                        follow_up = _coerce_datetime(action.get("follow_up_after"))
                        if follow_up is None:
                            follow_up = _default_followup(session_date, category)
                        source_session_id = _normalize_text(action.get("source_session_id")) or session_id

                        existing_same = await db.fetchone(
                            """
                            SELECT thread_id
                            FROM open_threads
                            WHERE user_id = $1
                              AND lower(title) = lower($2)
                              AND status IN ('open', 'snoozed')
                            """,
                            args.user_id,
                            title,
                        )
                        if existing_same:
                            continue

                        await db.execute(
                            """
                            INSERT INTO open_threads (
                              user_id, title, detail, status, priority,
                              category, related_entities, source_session_ids,
                              first_seen_at, last_updated_at, last_mentioned_at,
                              follow_up_after, times_mentioned
                            ) VALUES (
                              $1, $2, $3, 'open', $4,
                              $5, $6::text[], $7::text[],
                              $8, now(), $8, $9, 1
                            )
                            """,
                            args.user_id,
                            title,
                            detail,
                            priority,
                            category,
                            related_entities,
                            [source_session_id],
                            session_date,
                            follow_up,
                        )
                        created_count += 1
                        continue

                    if kind == "UPDATE":
                        thread_id = _normalize_text(action.get("thread_id"))
                        if not thread_id:
                            continue
                        row_current = await db.fetchone(
                            """
                            SELECT thread_id, status
                            FROM open_threads
                            WHERE user_id = $1 AND thread_id = $2
                            """,
                            args.user_id,
                            thread_id,
                        )
                        if not row_current or _normalize_text(row_current.get("status")) not in {"open", "snoozed"}:
                            continue
                        detail = _normalize_text(action.get("detail")) or None
                        lm = _coerce_datetime(action.get("last_mentioned_at")) or transcript_last or session_date
                        follow_up = _coerce_datetime(action.get("follow_up_after")) or _default_followup(lm, "other")
                        source_session_id = _normalize_text(action.get("source_session_id")) or session_id
                        await db.execute(
                            """
                            WITH merged AS (
                              SELECT array_agg(DISTINCT s) AS session_ids
                              FROM open_threads ot, unnest(ot.source_session_ids || $4::text[]) s
                              WHERE ot.thread_id = $5
                            )
                            UPDATE open_threads SET
                              detail = COALESCE($1, detail),
                              last_updated_at = now(),
                              last_mentioned_at = $2,
                              follow_up_after = $3,
                              source_session_ids = COALESCE((SELECT session_ids FROM merged), source_session_ids),
                              times_mentioned = COALESCE(
                                cardinality((SELECT session_ids FROM merged)),
                                times_mentioned
                              ),
                              status = 'open'
                            WHERE thread_id = $5
                            """,
                            detail,
                            lm,
                            follow_up,
                            [source_session_id],
                            thread_id,
                        )
                        updated_count += 1
                        continue

                    if kind == "RESOLVE":
                        thread_id = _normalize_text(action.get("thread_id"))
                        if not thread_id:
                            continue
                        row_current = await db.fetchone(
                            """
                            SELECT status
                            FROM open_threads
                            WHERE user_id = $1 AND thread_id = $2
                            """,
                            args.user_id,
                            thread_id,
                        )
                        if not row_current or _normalize_text(row_current.get("status")) != "open":
                            continue
                        resolution_note = _normalize_text(action.get("resolution_note")) or "Resolved from latest context."
                        await db.execute(
                            """
                            UPDATE open_threads SET
                              status = 'resolved',
                              resolved_at = now(),
                              resolution_note = $1,
                              last_updated_at = now()
                            WHERE thread_id = $2
                            """,
                            resolution_note,
                            thread_id,
                        )
                        resolved_count += 1
                        continue

                    if kind == "SNOOZE":
                        thread_id = _normalize_text(action.get("thread_id"))
                        if not thread_id:
                            continue
                        row_current = await db.fetchone(
                            """
                            SELECT status
                            FROM open_threads
                            WHERE user_id = $1 AND thread_id = $2
                            """,
                            args.user_id,
                            thread_id,
                        )
                        if not row_current or _normalize_text(row_current.get("status")) == "snoozed":
                            continue
                        await db.execute(
                            """
                            UPDATE open_threads SET
                              status = 'snoozed',
                              last_updated_at = now()
                            WHERE thread_id = $1
                            """,
                            thread_id,
                        )
                        snoozed_count += 1
                        continue

                except Exception as exc:
                    errors_count += 1
                    _append_error(
                        errors_file,
                        {
                            "stage": "session_action",
                            "session_id": session_id,
                            "action": action,
                            "error": str(exc),
                        },
                    )
                    continue

            await asyncio.sleep(1.0)

        # Audit pass
        open_threads = await db.fetch(
            """
            SELECT
              thread_id, title, detail, status, category,
              priority, related_entities, follow_up_after,
              last_mentioned_at, last_updated_at, first_seen_at,
              source_session_ids, times_mentioned
            FROM open_threads
            WHERE user_id = $1
              AND status IN ('open', 'snoozed')
            ORDER BY last_updated_at DESC NULLS LAST
            """,
            args.user_id,
        )
        open_lines = []
        for t in open_threads:
            open_lines.append(
                json.dumps(
                    {
                        "thread_id": _normalize_text(t.get("thread_id")),
                        "title": _normalize_text(t.get("title")),
                        "status": _normalize_text(t.get("status")),
                        "category": _normalize_text(t.get("category")),
                        "priority": _normalize_text(t.get("priority")),
                        "follow_up_after": _normalize_text(t.get("follow_up_after")),
                        "last_mentioned_at": _normalize_text(t.get("last_mentioned_at")),
                        "times_mentioned": t.get("times_mentioned"),
                        "detail": _normalize_text(t.get("detail")),
                    },
                    ensure_ascii=False,
                )
            )
        if not open_lines:
            open_lines = ["(none)"]
        audit_prompt = THREAD_AUDIT_PROMPT.format(open_threads="\n".join(open_lines))
        audit_result = await _call_openrouter_json(
            api_key=api_key,
            model=args.model,
            prompt=audit_prompt,
            timeout_seconds=args.timeout_seconds,
        )
        if audit_result.get("ok"):
            parsed = _safe_json(audit_result.get("parsed"))
            actions = parsed.get("audit_actions") if isinstance(parsed.get("audit_actions"), list) else []
            for action in actions:
                if not isinstance(action, dict):
                    continue
                kind = _normalize_text(action.get("action")).upper()
                try:
                    if kind == "RESOLVE":
                        thread_id = _normalize_text(action.get("thread_id"))
                        if not thread_id:
                            continue
                        row_current = await db.fetchone(
                            "SELECT status FROM open_threads WHERE user_id=$1 AND thread_id=$2",
                            args.user_id,
                            thread_id,
                        )
                        if not row_current or _normalize_text(row_current.get("status")) != "open":
                            continue
                        note = _normalize_text(action.get("resolution_note")) or "Resolved by audit context."
                        await db.execute(
                            """
                            UPDATE open_threads SET
                              status = 'resolved',
                              resolved_at = now(),
                              resolution_note = $1,
                              last_updated_at = now()
                            WHERE thread_id = $2
                            """,
                            note,
                            thread_id,
                        )
                        audit_applied += 1
                        continue

                    if kind == "SNOOZE":
                        thread_id = _normalize_text(action.get("thread_id"))
                        if not thread_id:
                            continue
                        row_current = await db.fetchone(
                            "SELECT status FROM open_threads WHERE user_id=$1 AND thread_id=$2",
                            args.user_id,
                            thread_id,
                        )
                        if not row_current or _normalize_text(row_current.get("status")) == "snoozed":
                            continue
                        await db.execute(
                            "UPDATE open_threads SET status='snoozed', last_updated_at=now() WHERE thread_id=$1",
                            thread_id,
                        )
                        audit_applied += 1
                        continue

                    if kind == "MERGE":
                        keep_id = _normalize_text(action.get("keep_thread_id"))
                        absorb_ids_raw = action.get("absorb_thread_ids")
                        absorb_ids: List[str] = []
                        if isinstance(absorb_ids_raw, list):
                            absorb_ids = [_normalize_text(x) for x in absorb_ids_raw if _normalize_text(x)]
                        else:
                            single_absorb = _normalize_text(action.get("absorb_thread_id"))
                            if single_absorb:
                                absorb_ids = [single_absorb]
                        absorb_ids = [x for x in absorb_ids if x and x != keep_id]
                        if not keep_id:
                            continue
                        keep = await db.fetchone(
                            """
                            SELECT
                              thread_id, title, detail, category, source_session_ids,
                              related_entities, follow_up_after, last_mentioned_at,
                              first_seen_at, times_mentioned
                            FROM open_threads WHERE user_id=$1 AND thread_id=$2
                            """,
                            args.user_id,
                            keep_id,
                        )
                        if not keep:
                            continue

                        absorb_threads: List[Dict[str, Any]] = []
                        for absorb_id in absorb_ids:
                            absorb = await db.fetchone(
                                """
                                SELECT
                                  thread_id, title, detail, category, source_session_ids,
                                  related_entities, follow_up_after, last_mentioned_at,
                                  first_seen_at, times_mentioned
                                FROM open_threads WHERE user_id=$1 AND thread_id=$2
                                """,
                                args.user_id,
                                absorb_id,
                            )
                            if absorb:
                                absorb_threads.append(absorb)

                        if not absorb_threads:
                            continue

                        merged_title = _normalize_text(action.get("merged_title")) or _normalize_text(keep.get("title"))
                        merged_detail = _normalize_text(action.get("merged_detail")) or _normalize_text(keep.get("detail"))
                        merged_category = _normalize_text(action.get("merged_category")).lower() or _normalize_text(keep.get("category")).lower() or "other"
                        if merged_category not in VALID_CATEGORIES:
                            merged_category = "other"

                        keep_sources = keep.get("source_session_ids") if isinstance(keep.get("source_session_ids"), list) else []
                        absorb_sources = [
                            s
                            for t in absorb_threads
                            for s in (t.get("source_session_ids") if isinstance(t.get("source_session_ids"), list) else [])
                        ]
                        merged_sources = sorted({_normalize_text(s) for s in (keep_sources + absorb_sources) if _normalize_text(s)})

                        keep_entities = keep.get("related_entities") if isinstance(keep.get("related_entities"), list) else []
                        absorb_entities = [
                            e
                            for t in absorb_threads
                            for e in (t.get("related_entities") if isinstance(t.get("related_entities"), list) else [])
                        ]
                        merged_entities = sorted({_normalize_text(e) for e in (keep_entities + absorb_entities) if _normalize_text(e)})

                        first_seen_candidates = [_coerce_datetime(keep.get("first_seen_at"))] + [
                            _coerce_datetime(t.get("first_seen_at")) for t in absorb_threads
                        ]
                        first_seen_candidates = [d for d in first_seen_candidates if d is not None]
                        earliest_seen = min(first_seen_candidates) if first_seen_candidates else None

                        last_mentioned_candidates = [_coerce_datetime(keep.get("last_mentioned_at"))] + [
                            _coerce_datetime(t.get("last_mentioned_at")) for t in absorb_threads
                        ]
                        last_mentioned_candidates = [d for d in last_mentioned_candidates if d is not None]
                        latest_mentioned = max(last_mentioned_candidates) if last_mentioned_candidates else None

                        total_times = int(keep.get("times_mentioned") or 1) + sum(
                            int(t.get("times_mentioned") or 1) for t in absorb_threads
                        )

                        await db.execute(
                            """
                            UPDATE open_threads SET
                              title = $1,
                              detail = $2,
                              category = $3,
                              source_session_ids = $4::text[],
                              related_entities = $5::text[],
                              times_mentioned = $6,
                              first_seen_at = COALESCE($7, first_seen_at),
                              last_mentioned_at = COALESCE($8, last_mentioned_at),
                              status = 'open',
                              category_updated = CASE
                                WHEN lower(COALESCE(category, '')) <> lower($3) THEN true
                                ELSE category_updated
                              END,
                              last_updated_at = now()
                            WHERE thread_id = $9
                            """,
                            merged_title,
                            merged_detail or None,
                            merged_category,
                            merged_sources,
                            merged_entities,
                            total_times,
                            earliest_seen,
                            latest_mentioned,
                            keep_id,
                        )
                        for absorb_id in absorb_ids:
                            await db.execute(
                                "DELETE FROM open_threads WHERE thread_id = $1",
                                absorb_id,
                            )
                        audit_applied += 1
                        continue

                    if kind == "CATEGORY_FIX":
                        thread_id = _normalize_text(action.get("thread_id"))
                        new_category = _normalize_text(action.get("new_category")).lower()
                        if not thread_id or not new_category or new_category not in VALID_CATEGORIES:
                            continue
                        row_current = await db.fetchone(
                            "SELECT category FROM open_threads WHERE user_id=$1 AND thread_id=$2",
                            args.user_id,
                            thread_id,
                        )
                        if not row_current:
                            continue
                        old_category = _normalize_text(row_current.get("category")).lower()
                        if old_category == new_category:
                            continue
                        await db.execute(
                            """
                            UPDATE open_threads SET
                              category = $1,
                              category_updated = true,
                              last_updated_at = now()
                            WHERE thread_id = $2
                            """,
                            new_category,
                            thread_id,
                        )
                        audit_applied += 1
                        continue
                except Exception as exc:
                    errors_count += 1
                    _append_error(
                        errors_file,
                        {"stage": "audit_action", "action": action, "error": str(exc)},
                    )
        else:
            errors_count += 1
            _append_error(
                errors_file,
                {
                    "stage": "audit_call",
                    "status_code": audit_result.get("status_code"),
                    "error": audit_result.get("error"),
                    "raw_output": audit_result.get("raw_output"),
                },
            )

        await db.execute(
            """
            INSERT INTO pipeline_checkpoints (user_id, pipeline_name, last_processed)
            VALUES ($1, 'threads_pipeline', now())
            ON CONFLICT (user_id, pipeline_name)
            DO UPDATE SET last_processed = now()
            """,
            args.user_id,
        )

        print("Threads Pipeline Complete")
        print("=========================")
        print(f"Sessions processed: {sessions_processed}")
        print(f"Threads created: {created_count}")
        print(f"Threads updated: {updated_count}")
        print(f"Threads resolved: {resolved_count}")
        print(f"Threads snoozed: {snoozed_count}")
        print(f"Audit actions applied: {audit_applied}")
        print(f"Errors: {errors_count}")
        print("")
        print("Open threads summary:")

        summary_rows = await db.fetch(
            """
            SELECT
              title,
              category,
              priority,
              status,
              related_entities,
              follow_up_after,
              LEFT(detail, 200) as detail_preview,
              last_mentioned_at
            FROM open_threads
            WHERE user_id = $1
              AND status = 'open'
            ORDER BY
              CASE priority
                WHEN 'high' THEN 1
                WHEN 'medium' THEN 2
                ELSE 3
              END,
              follow_up_after ASC
            """,
            args.user_id,
        )
        print(json.dumps(summary_rows, indent=2, ensure_ascii=False, default=str))
        print(f"errors_file={errors_file}")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
