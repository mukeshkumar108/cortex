from __future__ import annotations

from typing import Any, Dict, List, Optional

from .common import as_list, call_json_llm, clean_text, format_user_turns, safe_json, text_list

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



def _json_lines(rows: List[Dict[str, Any]]) -> str:
    import json
    return "\n".join(json.dumps(r, ensure_ascii=False, default=str) for r in rows)


async def synthesize_identity_profile(
    *,
    existing_profile: Dict[str, Any] | None,
    session_rows: List[Dict[str, Any]],
    persistent_goals: List[Dict[str, Any]],
    model: str,
) -> Optional[Dict[str, Any]]:
    existing_profile_text = "None — first synthesis" if not existing_profile else __import__('json').dumps(existing_profile, ensure_ascii=False, default=str)
    session_lines = []
    for row in session_rows:
        raw = row.get("raw_triage_output") if isinstance(row.get("raw_triage_output"), dict) else {}
        session_lines.append(__import__('json').dumps({
            "session_id": row.get("session_id"),
            "session_date": row.get("session_date"),
            "memory_deltas": as_list(raw.get("memory_deltas")),
            "identity_signals": as_list(raw.get("identity_signals")),
            "emotional_note": row.get("emotional_note"),
            "tension_signal": row.get("tension_signal"),
        }, ensure_ascii=False, default=str))
    prompt = IDENTITY_SYNTHESIS_PROMPT.format(
        existing_profile=existing_profile_text,
        session_evidence="\n".join(session_lines),
        persistent_goals=_json_lines(persistent_goals),
    )
    parsed = await call_json_llm(prompt=prompt, model=model, max_tokens=2800, temperature=0.2)
    return parsed or None


def normalize_identity_output(parsed: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "who_they_are": clean_text(parsed.get("who_they_are")) or None,
        "core_values": as_list(parsed.get("core_values")),
        "recurring_patterns": as_list(parsed.get("recurring_patterns")),
        "family_history": clean_text(parsed.get("family_history")) or None,
        "faith_and_beliefs": clean_text(parsed.get("faith_and_beliefs")) or None,
        "what_they_want": clean_text(parsed.get("what_they_want")) or None,
        "recurring_fears": as_list(parsed.get("recurring_fears")),
        "what_they_avoid": clean_text(parsed.get("what_they_avoid")) or None,
        "how_they_relate": clean_text(parsed.get("how_they_relate")) or None,
        "persistent_goals": as_list(parsed.get("persistent_goals")),
        "current_chapter": clean_text(parsed.get("current_chapter")) or None,
    }
