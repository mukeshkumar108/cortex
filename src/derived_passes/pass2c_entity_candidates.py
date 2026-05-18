from __future__ import annotations

from typing import Any, Dict, List, Optional

from .common import call_json_llm, clean_text, format_user_turns

PASS2C_ENTITY_CANDIDATES_PROMPT = """You are Pass 2c for entity candidate extraction.

Your job is to identify named entities from a user transcript that are worth downstream resolution.

Include only named things with durable or ongoing relevance:
- people
- projects/products/companies
- places with ongoing importance
- other named entities that matter to the user's world

Skip:
- generic nouns
- transient tools/frameworks/libraries/model names unless clearly central
- media titles mentioned casually
- vague references like "someone" or "they"

Return JSON only in this exact shape:
{
  "entity_candidates": [
    {
      "name": "Riley",
      "candidate_type": "person|project|place|other",
      "summary": "short reason this entity matters",
      "source": "chat",
      "provenance": {
        "message_hint": "short quote from user text"
      },
      "confidence": "low|medium|high",
      "status": "detected|needs_review"
    }
  ]
}

Rules:
1. Only extract from USER turns.
2. Prefer precision over quantity.
3. If no durable entity candidates exist, return {"entity_candidates": []}.
4. Ambiguous candidates should usually be `needs_review`.

Transcript:
{{transcript}}
"""


def _normalize_entity_candidates_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    allowed_types = {"person", "project", "place", "other"}
    allowed_conf = {"low", "medium", "high"}
    allowed_status = {"detected", "needs_review"}
    out: List[Dict[str, Any]] = []
    seen = set()
    rows = payload.get("entity_candidates") if isinstance(payload.get("entity_candidates"), list) else []
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = clean_text(row.get("name"))[:160]
        if not name:
            continue
        candidate_type = clean_text(row.get("candidate_type")).lower() or "other"
        if candidate_type not in allowed_types:
            candidate_type = "other"
        summary = clean_text(row.get("summary"))[:240] or None
        source = clean_text(row.get("source")).lower() or "chat"
        provenance = row.get("provenance") if isinstance(row.get("provenance"), dict) else {}
        confidence = clean_text(row.get("confidence")).lower() or "medium"
        if confidence not in allowed_conf:
            confidence = "medium"
        status = clean_text(row.get("status")).lower() or "detected"
        if status not in allowed_status:
            status = "detected"
        key = (name.casefold(), candidate_type)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "name": name,
                "candidate_type": candidate_type,
                "summary": summary,
                "source": source,
                "provenance": {**provenance, "message_hint": clean_text(provenance.get("message_hint"))[:240] or name},
                "confidence": confidence,
                "status": status,
            }
        )
        if len(out) >= 24:
            break
    return out


async def run_pass2c_entity_candidates_llm(*, messages: List[Dict[str, Any]], model: str) -> Optional[List[Dict[str, Any]]]:
    transcript = format_user_turns(messages)
    if not transcript:
        return []
    prompt = PASS2C_ENTITY_CANDIDATES_PROMPT.replace("{{transcript}}", transcript)
    parsed = await call_json_llm(prompt=prompt, model=model, max_tokens=1600, temperature=0.1)
    if not parsed:
        return None
    return _normalize_entity_candidates_payload(parsed)
