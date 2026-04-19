#!/usr/bin/env python3
"""Run pass-5 living context synthesis."""

from __future__ import annotations

import argparse
import asyncio
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

from src.config import get_settings
from src.db import Database


LIVING_CONTEXT_PROMPT = """You are synthesizing the current living context for
the user of a personal AI assistant named Sophie.

This is NOT a status report.
This is a tension map — what's happening on the surface
AND what's underneath it.

═══════════════════════════════════════════
OBSERVER EFFECT WARNING — READ THIS FIRST
═══════════════════════════════════════════
An existing living context may be shown below.
Treat it as a HYPOTHESIS TO CHALLENGE, not truth.

Your job is to re-synthesize from raw evidence.
Ask yourself: does the previous picture still hold?
What has shifted? What contradicts it?
If something has changed, say so explicitly.
Do NOT just rephrase the existing context.
Do NOT let the previous summary define the new one.
═══════════════════════════════════════════

EXISTING LIVING CONTEXT (challenge this, don't preserve it):
{existing_living_context}

WHO THIS PERSON IS (identity grounding):
{identity_grounding}

RECENT SESSIONS — oldest first:
{recent_sessions}

CURRENT OPEN THREADS (high salience):
{open_threads}

RECENTLY ACTIVE ENTITIES:
{active_entities}

YOUR TASK:

Write a living context that tells Sophie what she
needs to know to show up well for this person TODAY.

SURFACE LAYER — what's actually happening:
- What is this person focused on right now?
- What has happened in the last 2 weeks that matters?
- How are their key relationships feeling right now?
- What is the general emotional texture — the vibe?

TENSION MAP — what's underneath:
- What is the main unresolved friction right now?
  The thing that hasn't been named directly.
- What are they avoiding or circling around?
- What is this period really about beneath the surface?
  (not just tasks, but why)
- What are the deeper stakes of what's happening now?

CONTRADICTIONS — where things have shifted:
- Has something changed from the previous context?
- Are they saying different things now than before?
- Where do earlier views conflict with recent ones?
  Surface these explicitly. Don't silently overwrite.
  Note both the earlier and recent view.

SOPHIE DIRECTIVES — behavioral instructions:
- Based on this context, what should Sophie specifically
  do or avoid?
- These come from explicit user statements, not inference.

RULES:
1. Ground every statement in specific evidence.
   Do not psychologize beyond what the sessions show.
2. Be honest about uncertainty.
   "Possibly" and "appears to" are fine.
3. Do not be clinical. Write as a caring, attentive
   friend describing someone they know well.
4. The tension map sections are the most important.
   Do not skip them or make them vague.
5. Contradictions must show BOTH views — earlier AND
   recent. Never just overwrite the earlier one.
6. Sophie directives must be grounded in explicit
   user statements, not inferred preferences.

Return JSON only — no preamble, no markdown:
{{
  "current_focus": "prose",
  "recent_narrative": "prose",
  "relationship_pulse": "prose",
  "emotional_texture": "prose",
  "primary_tension": "prose",
  "what_theyre_avoiding": "prose",
  "unspoken_goal": "prose",
  "why_it_matters": "prose",
  "active_contradictions": [
    {{
      "topic": "topic",
      "earlier_view": "earlier view",
      "recent_view": "recent view",
      "first_stated": "2026-04-07",
      "last_stated": "2026-04-10",
      "resolved": false,
      "note": "holding both views"
    }}
  ],
  "sophie_directives": [
    {{
      "directive": "directive text",
      "reason": "explicit reason",
      "confidence": 0.98
    }}
  ]
}}
"""


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
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


def _as_list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


async def _call_openrouter_json(
    *,
    api_key: str,
    model: str,
    prompt: str,
    timeout_seconds: float = 180.0,
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


def _json_lines(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "(none)"
    return "\n".join(json.dumps(r, ensure_ascii=False, default=str) for r in rows)


async def _get_recent_sessions(db: Database, user_id: str, days: int) -> List[Dict[str, Any]]:
    return await db.fetch(
        """
        SELECT
          sc.session_id,
          sc.session_date,
          sc.session_kind,
          sc.emotional_weight,
          sc.emotional_note,
          sc.raw_triage_output->'memory_deltas' as memory_deltas,
          sc.raw_triage_output->'thread_signals' as thread_signals,
          sc.entity_mentions
        FROM session_classifications sc
        WHERE sc.user_id = $1
          AND sc.is_memory_worthy = true
          AND sc.session_date >= now() - ($2::text || ' days')::interval
        ORDER BY sc.session_date ASC
        """,
        user_id,
        str(days),
    )


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run living context synthesis")
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--model", default="google/gemma-4-26b-a4b-it")
    parser.add_argument("--timeout-seconds", type=float, default=180.0)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    settings = get_settings()
    api_key = _normalize_text(settings.openrouter_api_key or settings.openai_api_key)
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY or OPENAI_API_KEY is required")

    db = Database()
    try:
        checkpoint = await db.fetchone(
            """
            SELECT last_processed, sessions_since_last
            FROM pipeline_checkpoints
            WHERE user_id = $1
              AND pipeline_name = 'living_context'
            """,
            args.user_id,
        )
        last_processed: Optional[datetime] = checkpoint.get("last_processed") if checkpoint else None
        new_since_last = await db.fetchval(
            """
            SELECT COUNT(*)
            FROM session_classifications sc
            WHERE sc.user_id = $1
              AND sc.is_memory_worthy = true
              AND sc.session_date > COALESCE($2, '1970-01-01'::timestamptz)
            """,
            args.user_id,
            last_processed,
        )
        new_since_last = int(new_since_last or 0)

        # Persist current session delta count so scheduler/ops can inspect it.
        await db.execute(
            """
            INSERT INTO pipeline_checkpoints (user_id, pipeline_name, last_processed, sessions_since_last)
            VALUES ($1, 'living_context', COALESCE($2, now()), $3)
            ON CONFLICT (user_id, pipeline_name)
            DO UPDATE SET sessions_since_last = EXCLUDED.sessions_since_last
            """,
            args.user_id,
            last_processed,
            new_since_last,
        )

        existing_context = await db.fetchone(
            "SELECT * FROM living_context WHERE user_id = $1",
            args.user_id,
        )
        if not args.force and existing_context and new_since_last < 5:
            print("Living Context Skipped")
            print("======================")
            print(f"Reason: sessions_since_last={new_since_last} < 5")
            print("Use --force to run anyway.")
            return

        sessions = await _get_recent_sessions(db, args.user_id, 30)
        source_window_days = 30
        if len(sessions) < 3:
            sessions = await _get_recent_sessions(db, args.user_id, 60)
            source_window_days = 60

        open_threads = await db.fetch(
            """
            SELECT title, detail, category, priority,
                   salience_score, times_mentioned
            FROM open_threads
            WHERE user_id = $1
              AND status = 'open'
              AND salience_score >= 0.5
            ORDER BY salience_score DESC
            LIMIT 10
            """,
            args.user_id,
        )
        entities = await db.fetch(
            """
            SELECT canonical_name, type, relationship_to_user,
                   last_known_status, LEFT(profile_text, 200) AS profile_snippet
            FROM entity_profiles
            WHERE user_id = $1
              AND status = 'active'
              AND last_seen_at >= now() - interval '30 days'
            ORDER BY salience_score DESC NULLS LAST
            """,
            args.user_id,
        )
        identity = await db.fetchone(
            """
            SELECT who_they_are, what_they_want,
                   what_they_avoid, how_they_relate,
                   current_chapter
            FROM identity_profile
            WHERE user_id = $1
            """,
            args.user_id,
        )

        session_payload = []
        for s in sessions:
            session_payload.append(
                {
                    "session_id": _normalize_text(s.get("session_id")),
                    "session_date": _normalize_text(s.get("session_date")),
                    "session_kind": _normalize_text(s.get("session_kind")),
                    "emotional_weight": _normalize_text(s.get("emotional_weight")) or "none",
                    "emotional_note": _normalize_text(s.get("emotional_note")),
                    "memory_deltas": _as_list(s.get("memory_deltas")),
                    "thread_signals": _as_list(s.get("thread_signals")),
                    "entity_mentions": _as_list(s.get("entity_mentions")),
                }
            )

        identity_grounding = "(none)"
        if identity:
            identity_grounding = json.dumps(identity, ensure_ascii=False, default=str)

        prompt = LIVING_CONTEXT_PROMPT.format(
            existing_living_context=json.dumps(existing_context, ensure_ascii=False, default=str) if existing_context else "None — first synthesis",
            identity_grounding=identity_grounding,
            recent_sessions=_json_lines(session_payload),
            open_threads=_json_lines(open_threads),
            active_entities=_json_lines(entities),
        )
        result = await _call_openrouter_json(
            api_key=api_key,
            model=args.model,
            prompt=prompt,
            timeout_seconds=args.timeout_seconds,
        )
        if not result.get("ok"):
            raise RuntimeError(
                f"living_context_synthesis_failed status={result.get('status_code')} "
                f"error={result.get('error')} raw={result.get('raw_output')}"
            )
        parsed = _safe_json(result.get("parsed"))
        now = datetime.now(timezone.utc)

        await db.execute(
            """
            INSERT INTO living_context (
              user_id, current_focus, recent_narrative,
              relationship_pulse, emotional_texture,
              primary_tension, what_theyre_avoiding,
              unspoken_goal, why_it_matters,
              active_contradictions, sophie_directives,
              last_synthesized_at, sessions_since_last,
              source_session_count, synthesis_model
            ) VALUES (
              $1, $2, $3, $4, $5, $6, $7, $8, $9,
              $10::jsonb, $11::jsonb, $12, 0, $13, $14
            )
            ON CONFLICT (user_id) DO UPDATE SET
              current_focus = EXCLUDED.current_focus,
              recent_narrative = EXCLUDED.recent_narrative,
              relationship_pulse = EXCLUDED.relationship_pulse,
              emotional_texture = EXCLUDED.emotional_texture,
              primary_tension = EXCLUDED.primary_tension,
              what_theyre_avoiding = EXCLUDED.what_theyre_avoiding,
              unspoken_goal = EXCLUDED.unspoken_goal,
              why_it_matters = EXCLUDED.why_it_matters,
              active_contradictions = EXCLUDED.active_contradictions,
              sophie_directives = EXCLUDED.sophie_directives,
              last_synthesized_at = EXCLUDED.last_synthesized_at,
              sessions_since_last = 0,
              source_session_count = EXCLUDED.source_session_count,
              synthesis_model = EXCLUDED.synthesis_model,
              updated_at = now()
            """,
            args.user_id,
            _normalize_text(parsed.get("current_focus")) or None,
            _normalize_text(parsed.get("recent_narrative")) or None,
            _normalize_text(parsed.get("relationship_pulse")) or None,
            _normalize_text(parsed.get("emotional_texture")) or None,
            _normalize_text(parsed.get("primary_tension")) or None,
            _normalize_text(parsed.get("what_theyre_avoiding")) or None,
            _normalize_text(parsed.get("unspoken_goal")) or None,
            _normalize_text(parsed.get("why_it_matters")) or None,
            json.dumps(_as_list(parsed.get("active_contradictions"))),
            json.dumps(_as_list(parsed.get("sophie_directives"))),
            now,
            len(sessions),
            args.model,
        )

        await db.execute(
            """
            INSERT INTO pipeline_checkpoints
              (user_id, pipeline_name, last_processed, sessions_since_last)
            VALUES ($1, 'living_context', now(), 0)
            ON CONFLICT (user_id, pipeline_name)
            DO UPDATE SET last_processed = now(), sessions_since_last = 0
            """,
            args.user_id,
        )

        print("Living Context Complete")
        print("=======================")
        print(f"User: {args.user_id}")
        print(f"Source window (days): {source_window_days}")
        print(f"Source session count: {len(sessions)}")
        print(f"Open threads used: {len(open_threads)}")
        print(f"Active entities used: {len(entities)}")
        print(f"Model: {args.model}")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
