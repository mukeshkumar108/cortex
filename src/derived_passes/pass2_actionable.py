from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from .common import call_json_llm, clean_text, format_temporal_context_block, format_user_turns

PASS2_ACTIONABLE_PROMPT = """You are Pass 2a for action/tool candidate extraction.

Act like an excellent chief of staff or executive assistant reading messy human conversation.

Your job is to identify real-world things Sophie may need to track, confirm, schedule, revisit, or surface later:
- tasks the user needs to do
- events/meetings/appointments that are scheduled or proposed
- reminder asks
- commitments/promises/intentions
- recurring habits/routines
- follow-ups, waiting-on states, or nudges Sophie may later surface

Do not merely copy a local phrase. Understand the surrounding context, resolve what pronouns refer to, and only emit candidates that can stand alone as useful items that Sophie or the user could read later without reopening the transcript.

This pass is extraction and interpretation only:
- no personality analysis
- no narrative summary
- no auto-confirmation
- no execution

Return JSON only in this exact shape:
{
  "actionable_candidates": [
    {
      "record_type": "event_candidate|task_candidate|reminder_candidate|commitment",
      "candidate_subtype": "todo|reminder|calendar_event|habit|follow_up|waiting_on|nudge",
      "title": "short actionable title",
      "summary": "short summary",
      "provenance_summary": "one-line provenance summary",
      "confidence": 0.0,
      "risk_tags": ["private_source|financial|deletion|cancellation|irreversible|..."],
      "has_direct_user_expression": true,
      "requires_confirmation": true,
      "suggested_action": "create_action_item|ask_user|ignore",
      "due_iso": "ISO-8601 datetime in UTC or null",
      "due_text": "raw due/date phrase such as 'tomorrow', 'Friday', 'soon', or null",
      "time_text": "raw time phrase such as 'at 3', 'tonight', or null",
      "proposed_due_at": "ISO-8601 datetime in UTC or null",
      "proposed_remind_at": "ISO-8601 datetime in UTC or null",
      "relevant_from_iso": "ISO-8601 datetime in UTC or null",
      "relevant_until_iso": "ISO-8601 datetime in UTC or null",
      "waiting_on": "who/what the user is waiting on, or null",
      "needs_response": true,
      "cadence_text": "recurrence/cadence wording like 'daily' or 'every weekday', or null",
      "linked_external_id": null,
      "linked_external_type": null,
      "source": "chat",
      "provenance": {
        "message_hint": "short quote from user text",
        "resolved_object": "the concrete object/referent this is about, or null",
        "related_entities": ["entity or project names if relevant"],
        "resolution_confidence": "low|medium|high",
        "resolution_basis": "explicit|inferred",
        "date_resolution_basis": "explicit_date|relative_date|inferred_from_context|unresolved"
      },
      "status": "detected|needs_review"
    }
  ]
}

Rules:
1. Extract signal from noisy language; do not rely on exact phrasing.
2. Understand what the candidate is actually about before writing it down.
3. Only produce due_iso/relevant_from_iso/relevant_until_iso when the time/date is explicitly stated or safely resolvable from the temporal reference context below.
4. Prefer fewer, high-quality candidates over many weak ones.
5. Never emit confirmed/acted_on/dismissed statuses.
6. Keep titles concise, concrete, and stand-alone.
7. You are not extracting phrases. You are creating useful action candidates.
8. Every emitted candidate must answer:
   - what exactly needs doing?
   - who or what is involved?
   - by when, if known?
   - why it matters, if known?
   - can this stand alone without rereading the transcript?
9. Only extract a candidate if it can be written as a stand-alone useful item.
10. Do not emit fragments, pronoun-only items, or decontextualized phrases such as "send it", "check that", "sort this", or "do the thing".
11. If the candidate depends on "it/this/that/the thing" or similar wording, resolve the referent from context and write the resolved object in the title/summary/provenance.
12. If you cannot resolve the object/referent confidently enough to make a useful stand-alone candidate, do not emit it.
13. `needs_review` is only for real, meaningful candidates that need human confirmation. It is not for meaningless fragments.
14. Use `candidate_subtype` to classify the tool/action shape:
   - `todo`: one-off thing the user needs to do
   - `reminder`: explicit reminder ask
   - `calendar_event`: scheduled or proposed event/meeting/appointment
   - `habit`: recurring routine or cadence-based practice
   - `follow_up`: check back in, revisit, or reach out later
   - `waiting_on`: something blocked on another person/system
   - `nudge`: a proactive prompt Sophie might surface later; never an automatic action
15. Keep `record_type` coarse and `candidate_subtype` specific.
16. For `waiting_on`, populate `waiting_on` and `needs_response` where the transcript supports it.
17. For `habit`, populate `cadence_text` when recurrence wording is available.
18. Set `resolution_basis=explicit` when the referent/object is directly stated; use `inferred` only when you had to infer from surrounding context.
19. For time/date resolution:
   - use `explicit_date` when the exact date is stated
   - use `relative_date` when resolving phrases like tomorrow, Friday, or tonight from the temporal reference context
   - use `inferred_from_context` only when the date can be safely inferred from surrounding context
   - use `unresolved` when the phrase is vague or cannot be safely resolved
20. If the user says vague phrases like "soon", "later", "next week", or "sometime", keep ISO fields null and preserve the raw phrase in due_text and/or time_text.
21. Never invent dates.
22. If no actionable items exist, return {"actionable_candidates": []}.
23. Confidence must be numeric in [0.0, 1.0].
24. High-confidence direct commands may set: has_direct_user_expression=true, requires_confirmation=false, suggested_action=create_action_item.
25. Ambiguous or implicit items must set: requires_confirmation=true, suggested_action=ask_user.
26. Private-source-only signals must never be auto-actionable: has_direct_user_expression=false, requires_confirmation=true, and risk_tags includes "private_source".
27. Calendar events must not auto-create: candidate_subtype=calendar_event and requires_confirmation=true.

{{temporal_context}}

Transcript:
{{transcript}}
"""


def _strip_speaker_prefix(text: Any) -> str:
    value = clean_text(text)
    return re.sub(r"^(?:user|assistant|system)\s*:\s*", "", value, flags=re.I).strip()


def _normalize_actionable_text(value: Any, *, limit: int) -> str:
    return _strip_speaker_prefix(value)[:limit]


def _normalize_actionable_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    allowed_types = {"event_candidate", "task_candidate", "reminder_candidate", "commitment"}
    allowed_subtypes = {"todo", "reminder", "calendar_event", "habit", "follow_up", "waiting_on", "nudge"}
    allowed_status = {"detected", "needs_review"}
    allowed_suggested_actions = {"create_action_item", "ask_user", "ignore"}
    out: List[Dict[str, Any]] = []
    seen_keys = set()
    rows = payload.get("actionable_candidates") if isinstance(payload.get("actionable_candidates"), list) else []
    for row in rows:
        if not isinstance(row, dict):
            continue
        record_type = clean_text(row.get("record_type")).lower()
        if record_type not in allowed_types:
            continue
        candidate_subtype = clean_text(row.get("candidate_subtype")).lower() or None
        if candidate_subtype and candidate_subtype not in allowed_subtypes:
            candidate_subtype = None
        title = _normalize_actionable_text(row.get("title"), limit=180)
        if not title:
            continue

        summary = _normalize_actionable_text(row.get("summary"), limit=320) or None
        provenance_summary = _normalize_actionable_text(row.get("provenance_summary"), limit=240) or None
        due_iso = _normalize_actionable_text(row.get("due_iso"), limit=64) or None
        due_text = _normalize_actionable_text(row.get("due_text"), limit=120) or None
        time_text = _normalize_actionable_text(row.get("time_text"), limit=120) or None
        proposed_due_at = _normalize_actionable_text(row.get("proposed_due_at"), limit=64) or None
        proposed_remind_at = _normalize_actionable_text(row.get("proposed_remind_at"), limit=64) or None
        relevant_from_iso = _normalize_actionable_text(row.get("relevant_from_iso"), limit=64) or None
        relevant_until_iso = _normalize_actionable_text(row.get("relevant_until_iso"), limit=64) or None
        waiting_on = _normalize_actionable_text(row.get("waiting_on"), limit=180) or None

        needs_response = row.get("needs_response")
        if isinstance(needs_response, str):
            lowered = clean_text(needs_response).lower()
            if lowered in {"true", "yes", "1"}:
                needs_response = True
            elif lowered in {"false", "no", "0"}:
                needs_response = False
            else:
                needs_response = None
        elif not isinstance(needs_response, bool):
            needs_response = None

        cadence_text = _normalize_actionable_text(row.get("cadence_text"), limit=120) or None
        suggested_action = _normalize_actionable_text(row.get("suggested_action"), limit=64).lower() or "ask_user"
        if suggested_action not in allowed_suggested_actions:
            suggested_action = "ask_user"

        has_direct_user_expression = row.get("has_direct_user_expression")
        if isinstance(has_direct_user_expression, str):
            has_direct_user_expression = clean_text(has_direct_user_expression).lower() in {"true", "yes", "1"}
        elif not isinstance(has_direct_user_expression, bool):
            has_direct_user_expression = False

        requires_confirmation = row.get("requires_confirmation")
        if isinstance(requires_confirmation, str):
            requires_confirmation = clean_text(requires_confirmation).lower() in {"true", "yes", "1"}
        elif not isinstance(requires_confirmation, bool):
            requires_confirmation = True

        risk_tags = [
            _normalize_actionable_text(tag, limit=64).lower()
            for tag in (row.get("risk_tags") if isinstance(row.get("risk_tags"), list) else [])
            if _normalize_actionable_text(tag, limit=64)
        ][:12]

        linked_external_id = _normalize_actionable_text(row.get("linked_external_id"), limit=120) or None
        linked_external_type = _normalize_actionable_text(row.get("linked_external_type"), limit=64) or None
        source = _normalize_actionable_text(row.get("source"), limit=64).lower() or "chat"
        provenance = row.get("provenance") if isinstance(row.get("provenance"), dict) else {}

        confidence_value = row.get("confidence")
        try:
            confidence = float(confidence_value)
        except Exception:
            confidence = 0.6
        confidence = max(0.0, min(1.0, confidence))

        status = clean_text(row.get("status")).lower() or "detected"
        if status not in allowed_status:
            status = "detected"

        message_hint = _normalize_actionable_text(provenance.get("message_hint"), limit=240) or title
        normalized_provenance = {
            **provenance,
            "message_hint": message_hint,
            "resolved_object": _normalize_actionable_text(provenance.get("resolved_object"), limit=180) or None,
            "related_entities": [
                _normalize_actionable_text(item, limit=120)
                for item in (provenance.get("related_entities") if isinstance(provenance.get("related_entities"), list) else [])
                if _normalize_actionable_text(item, limit=120)
            ][:8],
            "resolution_confidence": clean_text(provenance.get("resolution_confidence")).lower() or None,
            "resolution_basis": clean_text(provenance.get("resolution_basis")).lower() or None,
            "date_resolution_basis": clean_text(provenance.get("date_resolution_basis") or row.get("date_resolution_basis")).lower() or None,
            "due_text": due_text,
            "time_text": time_text,
            "risk_tags": risk_tags,
            "has_direct_user_expression": has_direct_user_expression,
            "requires_confirmation": requires_confirmation,
            "provenance_summary": provenance_summary,
            "suggested_action": suggested_action,
        }
        if normalized_provenance.get("date_resolution_basis") == "unresolved":
            due_iso = None
            relevant_from_iso = None
            relevant_until_iso = None

        dedupe_key = (record_type, candidate_subtype or "", title.casefold(), due_iso or "", status)
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)

        out.append(
            {
                "record_type": record_type,
                "candidate_subtype": candidate_subtype,
                "title": title,
                "summary": summary,
                "provenance_summary": provenance_summary,
                "confidence": confidence,
                "risk_tags": risk_tags,
                "has_direct_user_expression": has_direct_user_expression,
                "requires_confirmation": requires_confirmation,
                "due_iso": due_iso,
                "proposed_due_at": proposed_due_at or due_iso,
                "proposed_remind_at": proposed_remind_at,
                "relevant_from_iso": relevant_from_iso,
                "relevant_until_iso": relevant_until_iso,
                "waiting_on": waiting_on,
                "needs_response": needs_response,
                "cadence_text": cadence_text,
                "suggested_action": suggested_action,
                "linked_external_id": linked_external_id,
                "linked_external_type": linked_external_type,
                "source": source,
                "provenance": normalized_provenance,
                "status": status,
            }
        )
        if len(out) >= 24:
            break
    return out


async def run_pass2_actionable_llm(
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
        PASS2_ACTIONABLE_PROMPT
        .replace("{{temporal_context}}", format_temporal_context_block(reference_time=reference_time, timezone_name=timezone_name))
        .replace("{{transcript}}", transcript)
    )
    parsed = await call_json_llm(prompt=prompt, model=model, max_tokens=1800, temperature=0.1)
    if not parsed:
        return None
    return _normalize_actionable_payload(parsed)
