#!/usr/bin/env python3
"""Compare raw-transcript entity extraction/profile synthesis across OpenRouter models vs Synapse."""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from src.config import get_settings
from src.db import Database


DEFAULT_MODELS = [
    "anthropic/claude-3-haiku",
    "openai/gpt-4o-mini",
    "amazon/nova-lite-v1",
    "google/gemma-4-26b-a4b-it",
]

PROMPTS: Dict[str, str] = {
    "A": """You are given raw conversation transcripts between a user and an AI assistant.
The transcripts are messy and unstructured.

Extract all distinct entities mentioned: people, projects, places, organizations.

For each entity return:
- name
- type (person / project / place / other)
- every distinct fact stated about them
- confidence: high / medium / low
- any contradictions or uncertainties you noticed
- evidence: a list of supporting items with session_id, approximate timestamp, and short snippet

Return as structured JSON only.""",
    "B": """You are given raw conversation transcripts between a user and an AI assistant.
The transcripts are messy and unstructured.

Build the most complete profile you can of the person named Ashley based only on what is in these transcripts.

Include:
- who she is
- her relationship to the user
- where she lives / background
- key facts about her
- timeline of key events involving her
- people connected to her
- anything you are uncertain about or that seems incomplete
- evidence: supporting items with session_id, approximate timestamp, and short snippet

Be honest about gaps. Do not invent anything not in the transcripts.

Return as structured JSON only.""",
    "C": """You are given raw conversation transcripts between a user and an AI assistant.
The transcripts are messy and unstructured.

Build a chronological timeline of the relationship between the user and the person named Ashley, based only on what is in these transcripts.

For each event include:
- approximate date or relative time if no exact date
- what happened
- source confidence: high / medium / low
- evidence_snippet
- session_id when known

Order oldest to newest. Note any gaps in the timeline.

Return as structured JSON only.""",
}


@dataclass
class TranscriptCorpus:
    sessions: List[Dict[str, Any]]
    raw_text: str
    total_chars: int


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
        lines.append(f"{'User' if role == 'user' else 'Assistant'}: {text}")
    return "\n".join(lines).strip()


def _extract_json_text(output_text: str) -> str:
    text = output_text.strip()
    if not text:
        return text

    fenced = re.match(r"^```(?:json)?\s*(.*?)\s*```$", text, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        return fenced.group(1).strip()

    start_obj = text.find("{")
    start_arr = text.find("[")
    starts = [idx for idx in (start_obj, start_arr) if idx != -1]
    if not starts:
        return text
    start = min(starts)

    end_obj = text.rfind("}")
    end_arr = text.rfind("]")
    ends = [idx for idx in (end_obj, end_arr) if idx != -1]
    if not ends:
        return text
    end = max(ends)

    if end >= start:
        candidate = text[start : end + 1].strip()
        if candidate:
            return candidate
    return text


async def _load_transcripts(
    db: Database,
    *,
    tenant_id: str,
    user_id: str,
    since: datetime,
) -> TranscriptCorpus:
    rows = await db.fetch(
        """
        SELECT session_id, messages, updated_at
        FROM session_transcript
        WHERE tenant_id = $1
          AND user_id = $2
          AND updated_at >= $3
        ORDER BY updated_at ASC, session_id ASC
        """,
        tenant_id,
        user_id,
        since,
    )

    sessions: List[Dict[str, Any]] = []
    blocks: List[str] = []
    for row in rows or []:
        messages = row.get("messages")
        if isinstance(messages, str):
            try:
                messages = json.loads(messages)
            except Exception:
                messages = []
        if not isinstance(messages, list):
            messages = []
        session_id = _normalize_text(row.get("session_id"))
        updated_at = _iso_utc(row.get("updated_at"))
        transcript_text = _format_messages(messages)
        session_payload = {
            "session_id": session_id,
            "timestamp": updated_at,
            "messages": messages,
            "raw_text": transcript_text,
        }
        sessions.append(session_payload)
        blocks.append(
            "\n".join(
                [
                    f"SESSION_ID: {session_id}",
                    f"TIMESTAMP: {updated_at}",
                    transcript_text,
                ]
            ).strip()
        )

    raw_text = "\n\n".join([block for block in blocks if block]).strip()
    return TranscriptCorpus(
        sessions=sessions,
        raw_text=raw_text,
        total_chars=len(raw_text),
    )


async def _call_openrouter(
    *,
    api_key: str,
    model: str,
    prompt_id: str,
    transcript_text: str,
    timeout_seconds: float,
) -> Dict[str, Any]:
    system_prompt = PROMPTS[prompt_id]
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": (
                    "Raw transcript corpus follows.\n\n"
                    f"{transcript_text}\n\n"
                    "Return JSON only."
                ),
            },
        ],
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    started = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            response = await client.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=payload)
            latency_ms = round((time.perf_counter() - started) * 1000, 2)
            raw_body = response.text
            parsed_body: Optional[Dict[str, Any]] = None
            output_text = ""
            parse_error: Optional[str] = None

            try:
                parsed_body = response.json()
            except Exception as e:
                parse_error = f"response_json_parse_failed: {e}"

            if response.is_success and isinstance(parsed_body, dict):
                try:
                    output_text = (
                        (((parsed_body.get("choices") or [])[0] or {}).get("message") or {}).get("content")
                        or ""
                    )
                except Exception:
                    output_text = ""

            output_json = None
            if output_text:
                try:
                    output_json = json.loads(_extract_json_text(output_text))
                except Exception as e:
                    parse_error = f"model_output_json_parse_failed: {e}"

            return {
                "model": model,
                "prompt": prompt_id,
                "latency_ms": latency_ms,
                "http_status": response.status_code,
                "ok": response.is_success,
                "output_text": output_text,
                "output_json": output_json,
                "response_body": parsed_body if isinstance(parsed_body, dict) else raw_body,
                "parse_error": parse_error,
            }
    except Exception as e:
        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        return {
            "model": model,
            "prompt": prompt_id,
            "latency_ms": latency_ms,
            "http_status": None,
            "ok": False,
            "output_text": "",
            "output_json": None,
            "response_body": None,
            "parse_error": f"request_failed: {type(e).__name__}: {e}",
        }


async def _post_synapse(
    *,
    api_base: str,
    path: str,
    payload: Dict[str, Any],
    timeout_seconds: float,
) -> Dict[str, Any]:
    started = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            response = await client.post(f"{api_base.rstrip('/')}{path}", json=payload)
            latency_ms = round((time.perf_counter() - started) * 1000, 2)
            try:
                body = response.json()
            except Exception:
                body = {"raw_text": response.text}
            return {
                "latency_ms": latency_ms,
                "http_status": response.status_code,
                "ok": response.is_success,
                "body": body,
            }
    except Exception as e:
        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        return {
            "latency_ms": latency_ms,
            "http_status": None,
            "ok": False,
            "body": {"error": f"request_failed: {type(e).__name__}: {e}"},
        }


def _default_output_path() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path("/opt/synapse/tmp") / f"entity_reader_eval_{stamp}.json"


async def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate raw-transcript entity reading across OpenRouter models")
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--api-base", default="http://127.0.0.1:8000")
    parser.add_argument("--query", default="who is Ashley?")
    parser.add_argument("--profile-name", default="Ashley")
    parser.add_argument("--output", default=str(_default_output_path()))
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    parser.add_argument("--models", nargs="*", default=DEFAULT_MODELS)
    args = parser.parse_args()

    settings = get_settings()
    api_key = _normalize_text(settings.openrouter_api_key or settings.openai_api_key)
    if not api_key:
        raise SystemExit("Missing OPENROUTER_API_KEY / OPENAI_API_KEY")

    since = datetime.now(timezone.utc) - timedelta(days=max(1, int(args.days)))
    db = Database()
    try:
        corpus = await _load_transcripts(
            db,
            tenant_id=args.tenant_id,
            user_id=args.user_id,
            since=since,
        )
    finally:
        await db.close()

    results: List[Dict[str, Any]] = []
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    def _write_snapshot() -> None:
        snapshot = {
            "run_date": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "tenant_id": args.tenant_id,
            "user_id": args.user_id,
            "days": int(args.days),
            "query": args.query,
            "profile_name": args.profile_name,
            "models": list(args.models),
            "transcript_sessions": len(corpus.sessions),
            "transcript_total_chars": corpus.total_chars,
            "transcript_corpus": {
                "sessions": [
                    {
                        "session_id": row["session_id"],
                        "timestamp": row["timestamp"],
                        "raw_text": row["raw_text"],
                    }
                    for row in corpus.sessions
                ],
                "raw_text": corpus.raw_text,
            },
            "results": results,
        }
        output_path.write_text(json.dumps(snapshot, indent=2, ensure_ascii=True), encoding="utf-8")

    print(
        f"[entity-reader-eval] loaded {len(corpus.sessions)} sessions, {corpus.total_chars} chars",
        file=sys.stderr,
        flush=True,
    )
    _write_snapshot()

    for model in args.models:
        for prompt_id in ("A", "B", "C"):
            print(
                f"[entity-reader-eval] running model={model} prompt={prompt_id}",
                file=sys.stderr,
                flush=True,
            )
            results.append(
                await _call_openrouter(
                    api_key=api_key,
                    model=model,
                    prompt_id=prompt_id,
                    transcript_text=corpus.raw_text,
                    timeout_seconds=float(args.timeout_seconds),
                )
            )
            _write_snapshot()
            print(
                f"[entity-reader-eval] finished model={model} prompt={prompt_id} ok={results[-1].get('ok')} latency_ms={results[-1].get('latency_ms')}",
                file=sys.stderr,
                flush=True,
            )

    print("[entity-reader-eval] querying synapse exact", file=sys.stderr, flush=True)
    synapse_exact = await _post_synapse(
        api_base=args.api_base,
        path="/memory/query",
        payload={
            "tenantId": args.tenant_id,
            "userId": args.user_id,
            "query": args.query,
            "memoryIntent": "exact",
            "limit": 10,
        },
        timeout_seconds=float(args.timeout_seconds),
    )
    _write_snapshot()
    print("[entity-reader-eval] querying synapse hybrid", file=sys.stderr, flush=True)
    synapse_hybrid = await _post_synapse(
        api_base=args.api_base,
        path="/memory/query",
        payload={
            "tenantId": args.tenant_id,
            "userId": args.user_id,
            "query": args.query,
            "memoryIntent": "hybrid",
            "limit": 10,
        },
        timeout_seconds=float(args.timeout_seconds),
    )
    _write_snapshot()
    print("[entity-reader-eval] querying synapse entities/profile", file=sys.stderr, flush=True)
    synapse_entity_profile = await _post_synapse(
        api_base=args.api_base,
        path="/entities/profile",
        payload={
            "tenantId": args.tenant_id,
            "userId": args.user_id,
            "name": args.profile_name,
            "includeOpenLoops": True,
            "factsLimit": 8,
            "loopsLimit": 3,
        },
        timeout_seconds=float(args.timeout_seconds),
    )

    output_payload = {
        "run_date": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "tenant_id": args.tenant_id,
        "user_id": args.user_id,
        "days": int(args.days),
        "query": args.query,
        "profile_name": args.profile_name,
        "models": list(args.models),
        "transcript_sessions": len(corpus.sessions),
        "transcript_total_chars": corpus.total_chars,
        "transcript_corpus": {
            "sessions": [
                {
                    "session_id": row["session_id"],
                    "timestamp": row["timestamp"],
                    "raw_text": row["raw_text"],
                }
                for row in corpus.sessions
            ],
            "raw_text": corpus.raw_text,
        },
        "results": results,
        "synapse_exact": synapse_exact,
        "synapse_hybrid": synapse_hybrid,
        "synapse_entities_profile": synapse_entity_profile,
    }

    output_path.write_text(json.dumps(output_payload, indent=2, ensure_ascii=True), encoding="utf-8")
    print(str(output_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
