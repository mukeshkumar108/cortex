from __future__ import annotations

from typing import Any, Dict, List, Optional

from .common import as_list, call_json_llm, clean_text, format_user_turns, safe_json, text_list

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



def _json_lines(rows: List[Dict[str, Any]]) -> str:
    import json
    return "\n".join(json.dumps(r, ensure_ascii=False, default=str) for r in rows)


async def synthesize_living_context(
    *,
    existing_context: Dict[str, Any] | None,
    identity_grounding: Dict[str, Any] | None,
    recent_sessions: List[Dict[str, Any]],
    open_threads: List[Dict[str, Any]],
    active_entities: List[Dict[str, Any]],
    model: str,
) -> Optional[Dict[str, Any]]:
    import json
    session_payload = []
    for row in recent_sessions:
        raw = row.get("raw_triage_output") if isinstance(row.get("raw_triage_output"), dict) else {}
        session_payload.append({
            "session_id": row.get("session_id"),
            "session_date": row.get("session_date"),
            "memory_deltas": as_list(raw.get("memory_deltas")),
            "thread_signals": as_list(raw.get("thread_signals")),
            "emotional_note": row.get("emotional_note"),
            "tension_signal": row.get("tension_signal"),
        })
    prompt = LIVING_CONTEXT_PROMPT.format(
        existing_living_context=json.dumps(existing_context, ensure_ascii=False, default=str) if existing_context else "None — first synthesis",
        identity_grounding=json.dumps(identity_grounding, ensure_ascii=False, default=str) if identity_grounding else "None",
        recent_sessions=_json_lines(session_payload),
        open_threads=_json_lines(open_threads),
        active_entities=_json_lines(active_entities),
    )
    parsed = await call_json_llm(prompt=prompt, model=model, max_tokens=2600, temperature=0.2)
    return parsed or None


def normalize_living_context_output(parsed: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "current_focus": clean_text(parsed.get("current_focus")) or None,
        "recent_narrative": clean_text(parsed.get("recent_narrative")) or None,
        "relationship_pulse": clean_text(parsed.get("relationship_pulse")) or None,
        "emotional_texture": clean_text(parsed.get("emotional_texture")) or None,
        "primary_tension": clean_text(parsed.get("primary_tension")) or None,
        "what_theyre_avoiding": clean_text(parsed.get("what_theyre_avoiding")) or None,
        "unspoken_goal": clean_text(parsed.get("unspoken_goal")) or None,
        "why_it_matters": clean_text(parsed.get("why_it_matters")) or None,
        "active_contradictions": as_list(parsed.get("active_contradictions")),
        "sophie_directives": as_list(parsed.get("sophie_directives")),
    }
