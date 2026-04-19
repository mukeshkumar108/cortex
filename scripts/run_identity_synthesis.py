#!/usr/bin/env python3
"""Run pass-4 identity profile synthesis."""

from __future__ import annotations

import argparse
import asyncio
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List

import httpx

from src.config import get_settings
from src.db import Database


IDENTITY_SYNTHESIS_PROMPT = """You are synthesizing an identity profile for the user
of a personal AI assistant named Sophie.

This is NOT a status report. This is a deep reading
of who this person is — their values, patterns, fears,
and the story they are living.

OBSERVER EFFECT WARNING:
If an existing profile is shown below, treat it as
a hypothesis to test against the evidence — not as
truth to preserve. Re-synthesize from the raw data.
Do not just reword the existing profile.

EXISTING PROFILE (hypothesis only, may be outdated):
{existing_profile}

IDENTITY-RELEVANT SESSIONS — memory deltas and signals,
chronological order:
{session_evidence}

PERSISTENT GOALS (stated repeatedly as core):
{persistent_goals}

Synthesize a complete identity profile.

For each section, ground your answer in specific
evidence from the sessions. Note uncertainty where
the evidence is thin. Do not invent or psychologize
beyond what is clearly present.

INWARD-FACING SECTIONS ARE CRITICAL:
Most AI systems only capture what a person is doing.
Your job is to also capture who they are when they
are not performing. What are they afraid of? What do
they want for themselves, not just for their projects?
What patterns keep showing up that they may not even
name directly?

Return JSON only — no preamble, no markdown:
{{
  "who_they_are": "synthesized prose — values, beliefs,
                   worldview, how they see themselves.
                   Warm, specific, grounded in evidence.
                   2-3 paragraphs.",
  "core_values": [
    {{
      "value": "integrity",
      "evidence": "explicit evidence from sessions",
      "confidence": 0.95
    }}
  ],
  "recurring_patterns": [
    {{
      "pattern": "recurrent pattern",
      "evidence": "specific evidence",
      "first_seen": "2026-02-07",
      "frequency": "high"
    }}
  ],
  "family_history": "prose — key biographical facts",
  "faith_and_beliefs": "prose — spiritual/philosophical worldview",
  "what_they_want": "prose — deeper aspirations",
  "recurring_fears": [
    {{
      "fear": "fear",
      "evidence": "specific evidence",
      "confidence": 0.8
    }}
  ],
  "what_they_avoid": "prose — avoidance patterns",
  "how_they_relate": "prose — relational patterns",
  "persistent_goals": [
    {{
      "goal": "daily morning walks",
      "stated_times": 4,
      "first_stated": "2026-02-07",
      "evidence": "repeatedly stated evidence"
    }}
  ],
  "current_chapter": "one paragraph describing current life season"
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


def _as_json_array(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run identity profile synthesis")
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--model", default="google/gemma-4-26b-a4b-it")
    parser.add_argument("--timeout-seconds", type=float, default=180.0)
    args = parser.parse_args()

    settings = get_settings()
    api_key = _normalize_text(settings.openrouter_api_key or settings.openai_api_key)
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY or OPENAI_API_KEY is required")

    db = Database()
    try:
        session_rows = await db.fetch(
            """
            SELECT
              sc.session_id,
              sc.session_date,
              sc.raw_triage_output->'memory_deltas' as memory_deltas,
              sc.raw_triage_output->'identity_signals' as identity_signals,
              sc.emotional_weight,
              sc.emotional_note
            FROM session_classifications sc
            WHERE sc.user_id = $1
              AND sc.identity_relevant = true
            ORDER BY sc.session_date ASC
            """,
            args.user_id,
        )
        persistent_goal_rows = await db.fetch(
            """
            SELECT title, detail, times_mentioned, first_seen_at
            FROM open_threads
            WHERE user_id = $1
              AND thread_type = 'persistent_goal'
              AND status = 'open'
            ORDER BY times_mentioned DESC, first_seen_at ASC NULLS LAST
            """,
            args.user_id,
        )
        existing = await db.fetchone(
            """
            SELECT *
            FROM identity_profile
            WHERE user_id = $1
            """,
            args.user_id,
        )

        session_lines: List[str] = []
        for row in session_rows:
            session_lines.append(
                json.dumps(
                    {
                        "session_id": _normalize_text(row.get("session_id")),
                        "session_date": _normalize_text(row.get("session_date")),
                        "memory_deltas": _as_json_array(row.get("memory_deltas")),
                        "identity_signals": _as_json_array(row.get("identity_signals")),
                        "emotional_weight": _normalize_text(row.get("emotional_weight")) or "none",
                        "emotional_note": _normalize_text(row.get("emotional_note")),
                    },
                    ensure_ascii=False,
                )
            )
        if not session_lines:
            session_lines = ["(none)"]

        persistent_goal_lines: List[str] = []
        for row in persistent_goal_rows:
            persistent_goal_lines.append(
                json.dumps(
                    {
                        "title": _normalize_text(row.get("title")),
                        "detail": _normalize_text(row.get("detail")),
                        "times_mentioned": row.get("times_mentioned"),
                        "first_seen_at": _normalize_text(row.get("first_seen_at")),
                    },
                    ensure_ascii=False,
                )
            )
        if not persistent_goal_lines:
            persistent_goal_lines = ["(none)"]

        existing_profile_text = json.dumps(existing, ensure_ascii=False, default=str) if existing else "None — first synthesis"
        prompt = IDENTITY_SYNTHESIS_PROMPT.format(
            existing_profile=existing_profile_text,
            session_evidence="\n".join(session_lines),
            persistent_goals="\n".join(persistent_goal_lines),
        )
        result = await _call_openrouter_json(
            api_key=api_key,
            model=args.model,
            prompt=prompt,
            timeout_seconds=args.timeout_seconds,
        )
        if not result.get("ok"):
            raise RuntimeError(
                f"identity_synthesis_failed status={result.get('status_code')} error={result.get('error')} "
                f"raw={result.get('raw_output')}"
            )

        parsed = _safe_json(result.get("parsed"))
        now = datetime.now(timezone.utc)
        await db.execute(
            """
            INSERT INTO identity_profile (
              user_id, who_they_are, core_values, recurring_patterns,
              family_history, faith_and_beliefs, what_they_want,
              recurring_fears, what_they_avoid, how_they_relate,
              persistent_goals, current_chapter,
              last_synthesized_at, source_session_count, synthesis_model
            ) VALUES (
              $1, $2, $3::jsonb, $4::jsonb, $5, $6, $7,
              $8::jsonb, $9, $10, $11::jsonb, $12, $13, $14, $15
            )
            ON CONFLICT (user_id) DO UPDATE SET
              who_they_are = EXCLUDED.who_they_are,
              core_values = EXCLUDED.core_values,
              recurring_patterns = EXCLUDED.recurring_patterns,
              family_history = EXCLUDED.family_history,
              faith_and_beliefs = EXCLUDED.faith_and_beliefs,
              what_they_want = EXCLUDED.what_they_want,
              recurring_fears = EXCLUDED.recurring_fears,
              what_they_avoid = EXCLUDED.what_they_avoid,
              how_they_relate = EXCLUDED.how_they_relate,
              persistent_goals = EXCLUDED.persistent_goals,
              current_chapter = EXCLUDED.current_chapter,
              last_synthesized_at = EXCLUDED.last_synthesized_at,
              source_session_count = EXCLUDED.source_session_count,
              synthesis_model = EXCLUDED.synthesis_model,
              updated_at = now()
            """,
            args.user_id,
            _normalize_text(parsed.get("who_they_are")) or None,
            json.dumps(_as_json_array(parsed.get("core_values"))),
            json.dumps(_as_json_array(parsed.get("recurring_patterns"))),
            _normalize_text(parsed.get("family_history")) or None,
            _normalize_text(parsed.get("faith_and_beliefs")) or None,
            _normalize_text(parsed.get("what_they_want")) or None,
            json.dumps(_as_json_array(parsed.get("recurring_fears"))),
            _normalize_text(parsed.get("what_they_avoid")) or None,
            _normalize_text(parsed.get("how_they_relate")) or None,
            json.dumps(_as_json_array(parsed.get("persistent_goals"))),
            _normalize_text(parsed.get("current_chapter")) or None,
            now,
            len(session_rows),
            args.model,
        )

        print("Identity Synthesis Complete")
        print("===========================")
        print(f"User: {args.user_id}")
        print(f"Source sessions: {len(session_rows)}")
        print(f"Persistent goal threads: {len(persistent_goal_rows)}")
        print(f"Model: {args.model}")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
