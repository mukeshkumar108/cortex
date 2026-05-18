from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from .common import call_json_llm, clean_text, format_temporal_context_block, format_user_turns

PASS2B_SESSION_CHANGES_PROMPT = """You are Pass 2b for session change extraction.

Your job is to identify factual or contextual changes from a user transcript that update understanding of the user's world.

Extract only concrete changes that change what the system should believe going forward, such as:
- changed plans or dates
- changed priorities or focus
- changed relationship state
- changed project/product direction
- changed decisions, commitments, or living situation

Do not extract:
- summaries of discussion
- general opinions unless they are decisions, preferences, priorities, or changed stances
- identity/personality interpretations
- open threads by themselves
- actionable tasks/reminders/events unless the important thing is the factual change itself
- vague summaries with no concrete update
- stable identity traits or preferences that are not new/changed
- vague project chatter with no explicit decision or directional change

Return JSON only in this exact shape:
{
  "session_changes": [
    {
      "kind": "factual_update|state_change|decision_change|schedule_change|focus_change",
      "title": "short change title",
      "summary": "short summary of what changed",
      "effective_iso": "ISO-8601 datetime in UTC or null",
      "time_text": "raw time/date phrase such as 'Friday', 'tomorrow', 'later', or null",
      "source": "chat",
      "provenance": {
        "message_hint": "short quote from user text",
        "date_resolution_basis": "explicit_date|relative_date|inferred_from_context|unresolved"
      },
      "confidence": "low|medium|high",
      "status": "detected|needs_review"
    }
  ]
}

Rules:
1. Only emit a session_change if the transcript contains a concrete factual/contextual update that changes what the system should believe going forward.
2. Do not emit summaries of discussion.
3. Do not emit actionables; those belong in actionable_candidates.
4. Do not emit stable identity traits; those belong in identity_profile.
5. Prefer fewer, higher-confidence changes.
6. Only produce effective_iso when the time/date is explicitly stated or safely resolvable from the temporal reference context below.
7. If the user says "tomorrow", "Friday", or "tonight", resolve using the temporal reference context.
8. If the user says vague phrases like "soon", "later", "next week", or "sometime", keep effective_iso null and preserve the raw phrase in time_text with date_resolution_basis=unresolved.
9. Proposed or ambiguous changes should usually be `needs_review`.
10. If there is no clear material change, return {"session_changes": []}.
11. Never emit confirmed/acted_on/dismissed statuses.

{{temporal_context}}

Transcript:
{{transcript}}
"""

def _normalize_session_changes_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    allowed_kinds = {"factual_update", "state_change", "decision_change", "schedule_change", "focus_change"}
    allowed_conf = {"low", "medium", "high"}
    allowed_status = {"detected", "needs_review"}
    out: List[Dict[str, Any]] = []
    seen = set()
    rows = payload.get("session_changes") if isinstance(payload.get("session_changes"), list) else []
    for row in rows:
        if not isinstance(row, dict):
            continue
        kind = clean_text(row.get("kind")).lower()
        if kind not in allowed_kinds:
            continue
        title = clean_text(row.get("title"))[:180]
        if not title:
            continue
        summary = clean_text(row.get("summary"))[:320] or None
        effective_iso = clean_text(row.get("effective_iso")) or None
        time_text = clean_text(row.get("time_text"))[:120] or None
        source = clean_text(row.get("source")).lower() or "chat"
        provenance = row.get("provenance") if isinstance(row.get("provenance"), dict) else {}
        confidence = clean_text(row.get("confidence")).lower() or "medium"
        if confidence not in allowed_conf:
            confidence = "medium"
        status = clean_text(row.get("status")).lower() or "detected"
        if status not in allowed_status:
            status = "detected"
        normalized_provenance = {
            **provenance,
            "message_hint": clean_text(provenance.get("message_hint"))[:240] or title,
            "date_resolution_basis": clean_text(provenance.get("date_resolution_basis") or row.get("date_resolution_basis")).lower() or None,
            "time_text": time_text,
        }
        if normalized_provenance.get("date_resolution_basis") == "unresolved":
            effective_iso = None
        dedupe_key = (kind, title.casefold(), effective_iso or "")
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        out.append(
            {
                "kind": kind,
                "title": title,
                "summary": summary,
                "effective_iso": effective_iso,
                "source": source,
                "provenance": normalized_provenance,
                "confidence": confidence,
                "status": status,
            }
        )
        if len(out) >= 24:
            break
    return out


async def run_pass2b_session_changes_llm(
    *,
    messages: List[Dict[str, Any]],
    model: str,
    reference_time: Optional[datetime] = None,
    timezone_name: Optional[str] = None,
) -> Optional[List[Dict[str, Any]]]:
    transcript = format_user_turns(messages)
    if not transcript:
        return []
    prompt = (
        PASS2B_SESSION_CHANGES_PROMPT
        .replace("{{temporal_context}}", format_temporal_context_block(reference_time=reference_time, timezone_name=timezone_name))
        .replace("{{transcript}}", transcript)
    )
    parsed = await call_json_llm(prompt=prompt, model=model, max_tokens=1800, temperature=0.1)
    if not parsed:
        return None
    return _normalize_session_changes_payload(parsed)
