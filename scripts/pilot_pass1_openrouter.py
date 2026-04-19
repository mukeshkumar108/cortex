#!/usr/bin/env python3
"""Run a Pass-1 classification prompt on selected session transcripts via OpenRouter."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import httpx

from src.config import get_settings
from src.db import Database


PASS1_PROMPT = """You are the first-pass memory triage layer for a personal AI assistant.

Your job is NOT to write a nice summary.
Your job is to decide whether this session contains durable memory value,
and if so, what kind.

Return JSON only in this exact shape:

{
  "is_memory_worthy": true,
  "session_kind": "technical|personal|mixed|transient",
  "memory_deltas": [
    "short specific statement 1",
    "short specific statement 2"
  ],
  "entity_mentions": ["name1", "name2"],
  "identity_signals": [
    "short specific statement"
  ],
  "thread_signals": [
    "short specific unresolved or follow-up-worthy item"
  ],
  "run_entity_pass": true,
  "run_threads_pass": true,
  "identity_relevant": true,
  "emotional_weight": "none|low|medium|high",
  "emotional_note": "one short sentence if medium or high, otherwise null",
  "ignore_reasons": [
    "optional short reason for what should be ignored"
  ]
}

Definitions:

- is_memory_worthy:
  true only if this session contains something a caring friend would
  remember later because it changes understanding of the user's world,
  reveals something important about the user, updates a key relationship,
  introduces a meaningful event, or creates/reinforces an unresolved thread.

- session_kind:
  technical = mainly system/code/testing/infrastructure talk
  personal = mainly life, relationships, health, beliefs, identity
  mixed = both technical and personal matter meaningfully
  transient = brief check-in, logistics, or low-value chatter with no durable memory value

- memory_deltas:
  the most important specific changes or updates from this session.
  These should be concrete and worth storing.
  Do NOT write generic summaries like "user discussed relationship updates."
  Prefer specific deltas like:
  - "Ashley and the user are back together."
  - "Ashley visited England and spent two weeks with the user."
  - "User sent Jasmine an Easter message without expecting a reply."

- entity_mentions:
  only include people, projects, places, or named things that matter to durable memory.
  Do NOT include generic nouns or low-value transient mentions.

- identity_signals:
  only include signals about who the user is at a deeper level:
  values, beliefs, enduring patterns, important biography, meaningful worldview,
  recurring ways of relating, or self-understanding.
  Do NOT include generic mood or filler.
  If none, return [].

- thread_signals:
  unresolved things a caring assistant should remember or follow up on later:
  health issues, relationship tensions, active hopes, commitments, worries,
  unfinished decisions, emotionally loaded situations.
  Do NOT create a thread for every mention.
  If none, return [].

- run_entity_pass:
  true if this session contains new, changed, corrected, or reinforced information
  about a meaningful person, project, place, or named thing.

- run_threads_pass:
  true if this session contains a new unresolved thread, reinforces an existing one,
  or resolves one.

- identity_relevant:
  true only if the session reveals or updates something meaningful about the user's
  values, beliefs, enduring relationships, history, or character.

- emotional_weight:
  none = no meaningful emotion
  low = mild feeling, not especially important
  medium = clearly emotional, vulnerable, or relationally meaningful
  high = intense distress, rupture, grief, fear, anger, shame, or major emotional significance

- emotional_note:
  one short sentence describing the emotionally meaningful part only if medium or high.
  Otherwise null.

- ignore_reasons:
  use this to note what should NOT be treated as durable memory, such as:
  technical chatter, system testing, weather, greetings, filler, repeated context,
  or assistant mistakes that do not matter long-term.
  If nothing notable to ignore, return [].

Rules:

1. Be specific, not vague.
2. Prefer 1-4 high-signal memory_deltas, not many weak ones.
3. Do not restate the whole session.
4. Do not invent facts.
5. Do not treat assistant small talk, weather, pasta bake, or generic check-ins as durable memory unless they clearly matter.
6. Do not confuse "topic discussed" with "memory worth storing."
7. If a session is mostly technical but contains one important personal update, mark it memory-worthy and session_kind="mixed".
8. If the session contains corrections to prior memory, treat those as high-value memory_deltas.
9. If a topic is interesting but not relevant to the user's ongoing world, do not elevate it into durable memory by default.

Transcript:
{{transcript}}
"""


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    return " ".join(value.split()).strip()


def _iso_utc(value: Any) -> str:
    if isinstance(value, datetime):
        dt = value
    else:
        raw = _normalize_text(value)
        if not raw:
            return ""
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            return raw
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _format_messages(messages: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for row in messages or []:
        if not isinstance(row, dict):
            continue
        role = _normalize_text(row.get("role")).lower()
        text = row.get("text")
        if role not in {"user", "assistant"}:
            continue
        if text is None:
            text = ""
        speaker = "User" if role == "user" else "Assistant"
        lines.append(f"{speaker}: {text}")
    return "\n".join(lines).strip()


async def _call_openrouter(
    *,
    api_key: str,
    model: str,
    transcript_text: str,
    timeout_seconds: float,
) -> Dict[str, Any]:
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": PASS1_PROMPT},
            {"role": "user", "content": f"TRANSCRIPT:\n{transcript_text}"},
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.1,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        response = await client.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers=headers,
            json=payload,
        )
    raw_text = ""
    body: Dict[str, Any] = {}
    parsed: Dict[str, Any] | None = None
    parse_error = ""
    try:
        body = response.json()
        raw_text = _normalize_text((((body.get("choices") or [{}])[0]).get("message") or {}).get("content"))
    except Exception as exc:
        parse_error = f"response_json_parse_failed: {exc}"
    if raw_text:
        try:
            parsed = json.loads(raw_text)
        except Exception as exc:
            parse_error = f"model_output_json_parse_failed: {exc}"
    return {
        "ok": response.is_success and parsed is not None,
        "status_code": response.status_code,
        "raw_output_text": raw_text,
        "parsed_output": parsed,
        "response_body": body,
        "parse_error": parse_error or None,
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Pilot Pass-1 session classification with OpenRouter")
    parser.add_argument("--tenant-id", default="default")
    parser.add_argument("--session-prefixes", nargs="+", required=True)
    parser.add_argument("--model", default="amazon/nova-micro-v1")
    parser.add_argument("--timeout-seconds", type=float, default=40.0)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    settings = get_settings()
    api_key = _normalize_text(settings.openrouter_api_key or settings.openai_api_key)
    if not api_key:
        raise RuntimeError("Missing OPENROUTER_API_KEY or OPENAI_API_KEY")

    db = Database()
    try:
        rows = await db.fetch(
            """
            SELECT session_id, tenant_id, user_id, created_at, updated_at, messages
            FROM session_transcript
            WHERE tenant_id = $1
              AND (
                FALSE
                OR session_id LIKE $2
                OR session_id LIKE $3
                OR session_id LIKE $4
                OR session_id LIKE $5
                OR session_id LIKE $6
              )
            ORDER BY updated_at ASC, session_id ASC
            """,
            args.tenant_id,
            f"{args.session_prefixes[0]}%",
            f"{args.session_prefixes[1]}%",
            f"{args.session_prefixes[2]}%",
            f"{args.session_prefixes[3]}%",
            f"{args.session_prefixes[4]}%",
        )
    finally:
        await db.close()

    results: List[Dict[str, Any]] = []
    for row in rows:
        messages = row.get("messages")
        if not isinstance(messages, list):
            messages = []
        transcript_text = _format_messages(messages)
        model_result = await _call_openrouter(
            api_key=api_key,
            model=args.model,
            transcript_text=transcript_text,
            timeout_seconds=args.timeout_seconds,
        )
        results.append(
            {
                "session_id": _normalize_text(row.get("session_id")),
                "tenant_id": _normalize_text(row.get("tenant_id")),
                "user_id": _normalize_text(row.get("user_id")),
                "created_at": _iso_utc(row.get("created_at")),
                "updated_at": _iso_utc(row.get("updated_at")),
                "message_count": len(messages),
                "transcript_text": transcript_text,
                "model_result": model_result,
            }
        )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output = {
        "model": args.model,
        "tenant_id": args.tenant_id,
        "session_prefixes": args.session_prefixes,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "sessions_found": len(results),
        "results": results,
    }
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    print(f"Wrote {len(results)} results to {output_path}")


if __name__ == "__main__":
    asyncio.run(main())
