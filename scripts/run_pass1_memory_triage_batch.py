#!/usr/bin/env python3
"""Run Pass 1 memory triage across processable sessions for one user."""

from __future__ import annotations

import argparse
import asyncio
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from src.config import get_settings
from src.db import Database
from src.episodic_memory import embed_texts


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
  "tension_signal": "one sentence or null. If this session contains a hint of something unspoken, avoided, or underneath the surface, name it briefly. Examples: 'User deflected from Ashley topic back to work' | 'User expressed doubt then immediately pivoted to optimism' | 'User mentioned Jasmine briefly then moved on quickly'. Only populate if clearly present in USER turns. Null if nothing notable.",
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
10. Only extract facts stated or confirmed by the USER. Ignore assistant turns for fact extraction. The assistant may be wrong. The user is the source of truth.

Transcript:
{{transcript}}
"""


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    return " ".join(value.split()).strip()


def _extract_json_text(text: str) -> str:
    raw = text.strip()
    if not raw:
        return raw
    fenced = re.match(r"^```(?:json)?\s*(.*?)\s*```$", raw, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        return fenced.group(1).strip()
    start_obj = raw.find("{")
    end_obj = raw.rfind("}")
    if start_obj != -1 and end_obj != -1 and end_obj > start_obj:
        return raw[start_obj : end_obj + 1].strip()
    return raw


def _format_messages(messages: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for row in messages or []:
        if not isinstance(row, dict):
            continue
        role = _normalize_text(row.get("role")).lower()
        text = row.get("text")
        if role not in {"user", "assistant"}:
            continue
        speaker = "User" if role == "user" else "Assistant"
        lines.append(f"{speaker}: {'' if text is None else text}")
    return "\n".join(lines).strip()


def _to_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"true", "t", "1", "yes"}:
            return True
        if v in {"false", "f", "0", "no"}:
            return False
    return None


def _to_text_list(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    out: List[str] = []
    for item in value:
        txt = _normalize_text(item)
        if txt:
            out.append(txt)
    return out


def _format_embedding(vector: List[float]) -> str:
    return "[" + ",".join(str(float(x)) for x in vector) + "]"


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
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            response = await client.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=headers,
                json=payload,
            )
    except Exception as exc:
        return {
            "status_code": None,
            "ok": False,
            "raw_output_text": "",
            "parsed_output": None,
            "parse_error": f"openrouter_request_failed: {exc}",
            "response_body": {},
        }
    parsed_response: Dict[str, Any] = {}
    try:
        parsed_response = response.json()
    except Exception:
        parsed_response = {}
    content = _normalize_text((((parsed_response.get("choices") or [{}])[0]).get("message") or {}).get("content"))
    json_text = _extract_json_text(content)
    parsed_output = None
    parse_error = None
    if json_text:
        try:
            parsed_output = json.loads(json_text)
        except Exception as exc:
            parse_error = f"model_output_json_parse_failed: {exc}"
    else:
        parse_error = "empty_model_output"
    return {
        "status_code": response.status_code,
        "ok": response.is_success and isinstance(parsed_output, dict),
        "raw_output_text": content,
        "parsed_output": parsed_output if isinstance(parsed_output, dict) else None,
        "parse_error": parse_error,
        "response_body": parsed_response,
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run pass1 memory triage batch")
    parser.add_argument("--tenant-id", default="default")
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--model", default="google/gemma-4-26b-a4b-it")
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--batch-delay-seconds", type=float, default=2.0)
    parser.add_argument("--timeout-seconds", type=float, default=80.0)
    parser.add_argument("--errors-file", default="")
    args = parser.parse_args()

    settings = get_settings()
    api_key = _normalize_text(settings.openrouter_api_key or settings.openai_api_key)
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY or OPENAI_API_KEY is required")

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    errors_path = Path(args.errors_file) if _normalize_text(args.errors_file) else Path(f"tmp/pass1_triage_errors_{ts}.jsonl")
    errors_path.parent.mkdir(parents=True, exist_ok=True)

    db = Database()
    total_source = 0
    total_processed = 0
    total_skipped = 0
    total_errors = 0
    memory_worthy_count = 0
    kind_counts: Counter[str] = Counter()

    try:
        source_rows = await db.fetch(
            """
            SELECT session_id, user_id, created_at, messages
            FROM processable_sessions
            WHERE user_id = $1
            ORDER BY created_at ASC, session_id ASC
            """,
            args.user_id,
        )
        total_source = len(source_rows)
        existing_rows = await db.fetch(
            "SELECT session_id FROM session_classifications WHERE user_id = $1",
            args.user_id,
        )
        existing_ids = {_normalize_text(row.get("session_id")) for row in existing_rows if _normalize_text(row.get("session_id"))}

        for idx in range(0, len(source_rows), max(1, args.batch_size)):
            batch = source_rows[idx : idx + max(1, args.batch_size)]
            for batch_pos, row in enumerate(batch):
                session_id = _normalize_text(row.get("session_id"))
                if not session_id:
                    continue
                seen = idx + batch_pos + 1
                if session_id in existing_ids:
                    total_skipped += 1
                    if seen % 10 == 0:
                        print(
                            f"progress seen={seen}/{total_source} processed={total_processed} skipped={total_skipped} errors={total_errors}"
                        )
                    continue

                messages = row.get("messages")
                if not isinstance(messages, list):
                    messages = []
                transcript_text = _format_messages(messages)
                result = await _call_openrouter(
                    api_key=api_key,
                    model=args.model,
                    transcript_text=transcript_text,
                    timeout_seconds=args.timeout_seconds,
                )

                parsed = result.get("parsed_output") if isinstance(result.get("parsed_output"), dict) else None
                if not result.get("ok") or parsed is None:
                    total_errors += 1
                    error_row = {
                        "session_id": session_id,
                        "user_id": _normalize_text(row.get("user_id")),
                        "created_at": str(row.get("created_at")),
                        "status_code": result.get("status_code"),
                        "parse_error": result.get("parse_error"),
                        "raw_output_text": result.get("raw_output_text"),
                    }
                    with errors_path.open("a", encoding="utf-8") as f:
                        f.write(json.dumps(error_row, ensure_ascii=False) + "\n")
                    if seen % 10 == 0:
                        print(
                            f"progress seen={seen}/{total_source} processed={total_processed} skipped={total_skipped} errors={total_errors}"
                        )
                    continue

                memory_deltas = _to_text_list(parsed.get("memory_deltas"))
                entity_mentions = _to_text_list(parsed.get("entity_mentions"))
                is_memory_worthy = _to_bool(parsed.get("is_memory_worthy"))
                run_entity_pass = _to_bool(parsed.get("run_entity_pass"))
                run_threads_pass = _to_bool(parsed.get("run_threads_pass"))
                identity_relevant = _to_bool(parsed.get("identity_relevant"))
                session_kind = _normalize_text(parsed.get("session_kind")) or None
                emotional_weight = _normalize_text(parsed.get("emotional_weight")) or None
                emotional_note_raw = parsed.get("emotional_note")
                emotional_note = _normalize_text(emotional_note_raw) if emotional_note_raw is not None else None
                tension_signal_raw = parsed.get("tension_signal")
                tension_signal = _normalize_text(tension_signal_raw) if tension_signal_raw is not None else None
                one_line_summary = memory_deltas[0] if memory_deltas else None

                memory_delta_embedding: Optional[str] = None
                embedding_input = _normalize_text(" ".join(memory_deltas))
                if embedding_input:
                    vectors = await embed_texts(
                        [embedding_input],
                        model=str(settings.memory_semantic_embedding_model or "text-embedding-3-small"),
                    )
                    if vectors and isinstance(vectors, list) and vectors and isinstance(vectors[0], list):
                        memory_delta_embedding = _format_embedding(vectors[0])

                await db.execute(
                    """
                    INSERT INTO session_classifications (
                        session_id,
                        user_id,
                        session_date,
                        is_memory_worthy,
                        session_kind,
                        one_line_summary,
                        entity_mentions,
                        run_entity_pass,
                        run_threads_pass,
                        identity_relevant,
                        emotional_weight,
                        emotional_note,
                        tension_signal,
                        memory_delta_embedding,
                        processed_at,
                        model_used,
                        raw_triage_output
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7::text[],$8,$9,$10,$11,$12,$13,$14::vector,NOW(),$15,$16::jsonb
                    )
                    ON CONFLICT (session_id) DO NOTHING
                    """,
                    session_id,
                    _normalize_text(row.get("user_id")) or args.user_id,
                    row.get("created_at"),
                    is_memory_worthy,
                    session_kind,
                    one_line_summary,
                    entity_mentions,
                    run_entity_pass,
                    run_threads_pass,
                    identity_relevant,
                    emotional_weight,
                    emotional_note,
                    tension_signal,
                    memory_delta_embedding,
                    args.model,
                    parsed,
                )
                existing_ids.add(session_id)
                total_processed += 1
                if is_memory_worthy is True:
                    memory_worthy_count += 1
                if session_kind:
                    kind_counts[session_kind] += 1

                if seen % 10 == 0:
                    print(
                        f"progress seen={seen}/{total_source} processed={total_processed} skipped={total_skipped} errors={total_errors}"
                    )

            if idx + max(1, args.batch_size) < len(source_rows):
                await asyncio.sleep(max(0.0, args.batch_delay_seconds))
    finally:
        await db.close()

    print("\nRun summary")
    print(f"total_source={total_source}")
    print(f"processed={total_processed}")
    print(f"skipped_existing={total_skipped}")
    print(f"memory_worthy_count={memory_worthy_count}")
    print("session_kind_breakdown=" + json.dumps(dict(kind_counts), ensure_ascii=False))
    print(f"errors={total_errors}")
    print(f"errors_file={errors_path}")


if __name__ == "__main__":
    asyncio.run(main())
