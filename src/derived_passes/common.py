from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

from src.canonicalization import normalize_text
from src.openrouter_client import get_llm_client


def clean_text(value: Any) -> str:
    return normalize_text(value, casefold=False)


def extract_json_text(text: str) -> str:
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


def safe_json(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(extract_json_text(value))
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


def as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
        return [value] if value.strip() else []
    return [value]


def text_list(value: Any, *, limit: int = 20) -> List[str]:
    out: List[str] = []
    for item in as_list(value):
        if isinstance(item, dict):
            raw = item.get("text") or item.get("name") or item.get("statement") or item.get("delta")
        else:
            raw = item
        text = clean_text(raw)
        if text and text not in out:
            out.append(text)
        if len(out) >= limit:
            break
    return out


def format_user_turns(messages: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for row in messages or []:
        if not isinstance(row, dict):
            continue
        role = clean_text(row.get("role")).lower()
        if role != "user":
            continue
        text = clean_text(row.get("text") or row.get("content"))
        if text:
            lines.append(f"User: {text}")
    return "\n".join(lines).strip()


async def call_json_llm(*, prompt: str, model: str, max_tokens: int = 1600, temperature: float = 0.1) -> Optional[Dict[str, Any]]:
    client = get_llm_client()
    original_model = client.model
    try:
        client.model = model or original_model
        raw = await client._call_llm(
            prompt=prompt,
            max_tokens=max_tokens,
            temperature=temperature,
            task="generic",
        )
    finally:
        client.model = original_model
    parsed = safe_json(raw)
    return parsed or None
