from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from .common import call_json_llm, clean_text


_ACTIONABLE_AUDIT_PROMPT = """You are auditing action/tool candidate records for a personal AI system.

Act like a chief of staff reviewing extracted possible actions with bounded source evidence.

Your job is not to rescue bad extraction. Your job is to decide whether each candidate was classified, rewritten, merged, prioritized, or dismissed correctly.

Use the temporal reference context in the candidate packet when interpreting relative date language.
Only treat due/start/end timing as resolved when it is explicit or safely resolvable from that reference context.
Never invent dates.

For each candidate decide one of:
- KEEP: valid candidate, keep as-is
- REWRITE: valid candidate, but rewrite it into a better stand-alone item
- MERGE: duplicate/covered by another candidate
- SUPERSEDE: older candidate should be superseded by this one or this one should supersede another
- NEEDS_REVIEW: a meaningful real candidate exists here, but it needs human confirmation
- NEEDS_CONTEXT: the packet is too thin to judge safely; do not guess
- DISMISS: junk, conversational noise, placeholder text, duplicate residue, or not actually actionable
- STALE: previously valid but no longer useful/current

Dismiss candidates that are:
- vague placeholders
- missing a concrete referent/target that you cannot confidently resolve from the evidence
- pure conversational chatter
- obvious transcript artifacts
- identity statements
- factual/session-change updates rather than actionables
- low-value ephemeral plans with no durable follow-up value

Questions to answer:
- Is this a real useful item?
- Can it be understood without guessing?
- Can the referent be resolved from the evidence?
- Is it duplicate or already covered by another item?
- Should the title be rewritten into a stand-alone form?
- Should it be typed as todo, reminder, calendar_event, habit, follow_up, waiting_on, or nudge?
- Should it be hidden from the daily or review lenses even if kept?

Important distinctions:
- `needs_review` is for meaningful candidates that need confirmation, not for meaningless fragments.
- If the extractor should never have emitted this because it does not stand alone, dismiss it rather than rescuing it.
- Only use REWRITE when the underlying candidate is already real and supported, and you are improving phrasing or subtype clarity.
- If the referent/object still cannot be stated clearly after reviewing the evidence, prefer DISMISS over NEEDS_REVIEW.
- Use NEEDS_CONTEXT when the packet is insufficient and forcing a judgment would require guessing.

Return JSON only:
{
  "actions": [
    {
      "candidate_id": 123,
      "action": "KEEP|REWRITE|MERGE|SUPERSEDE|NEEDS_REVIEW|NEEDS_CONTEXT|DISMISS|STALE",
      "reason": "short reason",
      "confidence": 0.0,
      "normalized_title": "optional rewritten stand-alone title or null",
      "normalized_summary": "optional rewritten summary or null",
      "candidate_subtype": "todo|reminder|calendar_event|habit|follow_up|waiting_on|nudge|null",
      "waiting_on": "optional waiting-on target or null",
      "needs_response": true,
      "cadence_text": "optional cadence text or null",
      "resolved_object": "optional resolved object/referent or null",
      "related_entities": ["optional entity names"],
      "target_candidate_id": 456,
      "hide_from_daily": true,
      "hide_from_review": false
    }
  ]
}

Candidates:
{{candidates}}
"""


_SESSION_CHANGE_AUDIT_PROMPT = """You are auditing session change records for a personal AI system.

Your job is cleanup, not extraction.

For each candidate decide one of:
- KEEP
- NEEDS_REVIEW
- DISMISS
- STALE

Keep only concrete factual/contextual updates that change what the system should believe going forward.
Dismiss discussion summaries, vague project chatter, stable identity statements, and actionables that belong elsewhere.

Return JSON only:
{
  "actions": [
    {
      "change_id": 123,
      "action": "KEEP|NEEDS_REVIEW|DISMISS|STALE",
      "reason": "short reason",
      "confidence": 0.0
    }
  ]
}

Candidates:
{{candidates}}
"""


_ENTITY_CANDIDATE_AUDIT_PROMPT = """You are auditing entity candidate records for a personal AI system.

Your job is cleanup, not extraction.

For each candidate decide one of:
- KEEP
- NEEDS_REVIEW
- DISMISS
- STALE

Dismiss candidates that are generic, low-signal, non-durable, obvious transcript artifacts, or not truly named entities worth tracking.

Return JSON only:
{
  "actions": [
    {
      "candidate_id": 123,
      "action": "KEEP|NEEDS_REVIEW|DISMISS|STALE",
      "reason": "short reason",
      "confidence": 0.0
    }
  ]
}

Candidates:
{{candidates}}
"""


_ACTIONABLE_RECONCILIATION_PROMPT = """You are reconciling a newly extracted action/tool candidate against existing open candidates for the same user.

Your job is to decide whether the new candidate is:
- SEPARATE: a new distinct item
- MERGE: the same underlying item as an existing candidate
- SUPERSEDE: a changed version of an existing candidate that should replace/supersede it
- DISMISS: not useful enough to keep
- STALE_EXISTING: a new candidate indicates an existing candidate is stale/cancelled

Be conservative. Only merge when clearly the same underlying thing. Use the evidence, not token overlap.

Return JSON only:
{
  "decision": {
    "action": "SEPARATE|MERGE|SUPERSEDE|DISMISS|STALE_EXISTING",
    "target_candidate_id": 123,
    "reason": "short reason",
    "confidence": 0.0
  }
}

New candidate:
{{new_candidate}}

Existing candidates:
{{existing_candidates}}
"""


def _normalize_audit_actions(payload: Dict[str, Any], *, id_field: str) -> List[Dict[str, Any]]:
    rows = payload.get("actions") if isinstance(payload.get("actions"), list) else []
    out: List[Dict[str, Any]] = []
    seen: set[int] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            item_id = int(row.get(id_field))
        except Exception:
            continue
        if item_id in seen:
            continue
        action = clean_text(row.get("action")).upper()
        if action not in {"KEEP", "REWRITE", "MERGE", "SUPERSEDE", "NEEDS_REVIEW", "NEEDS_CONTEXT", "DISMISS", "STALE"}:
            continue
        try:
            confidence = float(row.get("confidence"))
        except Exception:
            confidence = 0.0
        seen.add(item_id)
        out.append(
            {
                id_field: item_id,
                "action": action,
                "reason": clean_text(row.get("reason"))[:240] or None,
                "confidence": max(0.0, min(1.0, confidence)),
                "normalized_title": clean_text(row.get("normalized_title"))[:180] or None,
                "normalized_summary": clean_text(row.get("normalized_summary"))[:320] or None,
                "candidate_subtype": clean_text(row.get("candidate_subtype")).lower() or None,
                "waiting_on": clean_text(row.get("waiting_on"))[:180] or None,
                "needs_response": row.get("needs_response") if isinstance(row.get("needs_response"), bool) else None,
                "cadence_text": clean_text(row.get("cadence_text"))[:120] or None,
                "resolved_object": clean_text(row.get("resolved_object"))[:180] or None,
                "related_entities": [
                    clean_text(item)[:120]
                    for item in (row.get("related_entities") if isinstance(row.get("related_entities"), list) else [])
                    if clean_text(item)
                ][:8],
                "target_candidate_id": int(row.get("target_candidate_id")) if str(row.get("target_candidate_id") or "").isdigit() else None,
                "hide_from_daily": row.get("hide_from_daily") if isinstance(row.get("hide_from_daily"), bool) else None,
                "hide_from_review": row.get("hide_from_review") if isinstance(row.get("hide_from_review"), bool) else None,
            }
        )
    return out


async def audit_actionable_candidates(*, candidates: List[Dict[str, Any]], model: str) -> Optional[List[Dict[str, Any]]]:
    prompt = _ACTIONABLE_AUDIT_PROMPT.replace("{{candidates}}", json.dumps(candidates, ensure_ascii=True))
    parsed = await call_json_llm(prompt=prompt, model=model, max_tokens=2200, temperature=0.0)
    if not parsed:
        return None
    return _normalize_audit_actions(parsed, id_field="candidate_id")


async def audit_session_changes(*, candidates: List[Dict[str, Any]], model: str) -> Optional[List[Dict[str, Any]]]:
    prompt = _SESSION_CHANGE_AUDIT_PROMPT.replace("{{candidates}}", json.dumps(candidates, ensure_ascii=True))
    parsed = await call_json_llm(prompt=prompt, model=model, max_tokens=2200, temperature=0.0)
    if not parsed:
        return None
    return _normalize_audit_actions(parsed, id_field="change_id")


async def audit_entity_candidates(*, candidates: List[Dict[str, Any]], model: str) -> Optional[List[Dict[str, Any]]]:
    prompt = _ENTITY_CANDIDATE_AUDIT_PROMPT.replace("{{candidates}}", json.dumps(candidates, ensure_ascii=True))
    parsed = await call_json_llm(prompt=prompt, model=model, max_tokens=2200, temperature=0.0)
    if not parsed:
        return None
    return _normalize_audit_actions(parsed, id_field="candidate_id")


async def reconcile_actionable_candidate(*, new_candidate: Dict[str, Any], existing_candidates: List[Dict[str, Any]], model: str) -> Optional[Dict[str, Any]]:
    prompt = (
        _ACTIONABLE_RECONCILIATION_PROMPT
        .replace("{{new_candidate}}", json.dumps(new_candidate, ensure_ascii=True))
        .replace("{{existing_candidates}}", json.dumps(existing_candidates, ensure_ascii=True))
    )
    parsed = await call_json_llm(prompt=prompt, model=model, max_tokens=1200, temperature=0.0)
    if not parsed:
        return None
    row = parsed.get("decision") if isinstance(parsed.get("decision"), dict) else {}
    action = clean_text(row.get("action")).upper()
    if action not in {"SEPARATE", "MERGE", "SUPERSEDE", "DISMISS", "STALE_EXISTING"}:
        return None
    try:
        confidence = float(row.get("confidence"))
    except Exception:
        confidence = 0.0
    target_candidate_id = row.get("target_candidate_id")
    if not str(target_candidate_id or "").isdigit():
        target_candidate_id = None
    else:
        target_candidate_id = int(target_candidate_id)
    return {
        "action": action,
        "target_candidate_id": target_candidate_id,
        "reason": clean_text(row.get("reason"))[:240] or None,
        "confidence": max(0.0, min(1.0, confidence)),
    }
