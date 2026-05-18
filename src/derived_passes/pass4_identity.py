from __future__ import annotations

from typing import Any, Dict, List, Optional

from .common import as_list, call_json_llm, clean_text, format_user_turns, safe_json, text_list
from .synthesis_quality import conservative_rewrite_text, sanitize_list_of_dicts

IDENTITY_SYNTHESIS_PROMPT = """You are synthesizing an identity profile for the user
of a personal AI assistant named Sophie.

Your job is to identify this person, not interpret them.

This is NOT a status report, NOT a therapy note, NOT a character study,
and NOT a worldview synthesis.
Capture only stable, useful memory that helps Sophie respond better:
durable roles, what they are building, explicit commitments, important
people, plain stated aims, repeated preferences, known constraints,
and uncertainty.

OBSERVER EFFECT WARNING:
If an existing profile is shown below, treat it as
a hypothesis to test against the evidence — not as
truth to preserve. Re-synthesize from the raw data.
Do not just reword the existing profile.

EXISTING PROFILE (hypothesis only, may be outdated):
{existing_profile}

DECLARED PROFILE TRUTH (highest authority; use when present):
{declared_profile_truth}

DURABLE PROFILE FACTS (structured facts with provenance and confidence):
{durable_profile_facts}

IDENTITY-RELEVANT SESSION EVIDENCE — chronological order:
{session_evidence}

PERSISTENT GOALS (stated repeatedly as core):
{persistent_goals}

Answer these questions only:
- What are their durable roles? (founder, parent, writer, believer, operator, builder)
- What are they building or working on?
- What beliefs or commitments are explicitly and repeatedly stated?
- Who matters to them and why?
- What do they want, stated plainly and not inferred?
- What is active in the current chapter, kept clearly separate from enduring identity?

For each section, ground your answer in specific evidence from the raw user excerpts.
Routing hints may appear alongside excerpts, but they are only hints for where to look.
If a routing hint is more interpretive than the raw excerpt supports, ignore the hint.
Authority order:
1. declared profile truth
2. repeated explicit session facts
3. durable derived facts
4. prose synthesis
Higher-authority facts fill and constrain the profile.
Do not let lower-authority synthesis overwrite higher-authority facts.
Note uncertainty where the evidence is thin. Do not invent or psychologize
beyond what is clearly present.

QUALITY CONSTRAINTS:
- Do not synthesize a worldview.
- Do not produce a philosophy.
- Do not explain what their work means about them.
- Do not explain what their relationships mean about them.
- Do not produce a polished personal brand statement.
- Preserve concrete relationship-state terms exactly when they are
  explicit in the source or declared truth.
- Do not soften, euphemize, or editorially improve relationship
  descriptions.
- "Estranged" means estranged. "Long distance" means long distance.
- The runtime model needs accurate relationship facts, not diplomatic
  summaries.
- If declared profile truth gives a clear, concrete description
  of a role, project, public work, or health consideration,
  preserve that concrete meaning instead of abstracting it into
  a broader category.
- Prefer the concrete project phrasing over generic labels.
  Example: if the declared truth says "Bluum — neuroplasticity wellness app",
  do not rewrite it as "wellness application".
- If the user uses internal shorthand that is not broadly clear,
  translate it into plain language. But if the declared truth already
  uses plain, concrete wording, keep it close to that wording.
- Do not use the user's own internal jargon or slogans when plain descriptive
  language would be clearer.
- Translate jargon into plain language a stranger could understand.
- Do not write a personality essay.
- Do not write personality verdicts, global character claims,
  or phrases like "the user is someone who...".
- Do not dramatize the user or frame them as a tragic,
  heroic, broken, complex, or literary character.
- Do not narrate an inner emotional landscape unless the user
  explicitly and repeatedly makes it part of their durable identity.
- Do not use generic literary phrases like "deep complexity",
  "navigating a life shaped by", "beneath it all", "trying to prove",
  "driven by", "weight of", "defined by", "rollercoaster", or similar.
- Do not infer global character from a single session.
- Do not infer hidden motives or internal motives, fears, shame,
  guilt, avoidance, or aspirations unless they are explicit or
  repeatedly evidenced.
- Do not causally connect facts unless the connection is
  explicit, repeated, or represented as uncertainty.
- Prefer concrete stable patterns, durable anchors,
  explicit preferences, known constraints, and uncertainty.
- Frame stable patterns as observed behavior, not identity essence.
- Prefer roles, commitments, beliefs, projects, repeated choices,
  and stated standards over inferred feelings.
- If the user is a builder, writer, parent, believer, operator,
  founder, or caretaker, say that only when strongly supported.
- Intellectual, work, or creative specificity is valuable when it is
  strongly evidenced and helps Sophie identify the person plainly.
- If support is thin, use uncertainty language or leave the field sparse.
- Every identity claim must help Sophie respond better.
  If it is merely clever, flattering, dramatic, or poetic,
  leave it out.

CRITICAL DISTINCTION:
- enduring identity = who this person is across time
- current chapter = what is active right now

Do not let the current chapter overwrite the enduring person.
Do not freeze a hard season into identity.

When in doubt:
- choose plain identification over synthesis
- choose concrete roles and commitments over inner-state narration
- choose omission over overreach

Return JSON only — no preamble, no markdown:
{{
  "who_they_are": "compact prose — enduring identity only.
                   Identify the person plainly: durable roles,
                   what they build or do, explicit commitments,
                   and who they are across time. 1 short paragraph.",
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
  "family_history": "prose — key biographical facts only if durable and useful",
  "faith_and_beliefs": "prose — explicit faith or belief commitments only if clearly stated or strongly durable",
  "what_they_want": "prose — plain stated aims and durable wants, not hidden motives",
  "recurring_fears": [
    {{
      "fear": "fear",
      "evidence": "specific evidence",
      "confidence": 0.8
    }}
  ],
  "what_they_avoid": "prose — only explicit or repeatedly evidenced avoidance patterns",
  "how_they_relate": "prose — optional plain note on important people or relationship stance only when clearly supported",
  "persistent_goals": [
    {{
      "goal": "daily morning walks",
      "stated_times": 4,
      "first_stated": "2026-02-07",
      "evidence": "repeatedly stated evidence"
    }}
  ],
  "current_chapter": "one short paragraph describing what is active now in plain, operational terms without turning it into identity"
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
    declared_profile_truth: Dict[str, Any],
    declared_truth_facts: List[Dict[str, Any]],
    durable_profile_facts: List[Dict[str, Any]],
    model: str,
) -> Optional[Dict[str, Any]]:
    existing_profile_text = "None — first synthesis" if not existing_profile else __import__('json').dumps(existing_profile, ensure_ascii=False, default=str)
    session_lines = []
    for row in session_rows:
        session_lines.append(__import__('json').dumps({
            "session_id": row.get("session_id"),
            "session_date": row.get("session_date"),
            "user_excerpt": row.get("user_excerpt"),
            "routing_hints": row.get("routing_hints") or {
                "identity_relevant": bool(row.get("identity_relevant")),
                "run_entity_pass": bool(row.get("run_entity_pass")),
                "run_threads_pass": bool(row.get("run_threads_pass")),
                "session_kind": clean_text(row.get("session_kind")) or None,
                "emotional_weight": clean_text(row.get("emotional_weight")) or None,
                "tension_signal": clean_text(row.get("tension_signal")) or None,
            },
        }, ensure_ascii=False, default=str))
    prompt = IDENTITY_SYNTHESIS_PROMPT.format(
        existing_profile=existing_profile_text,
        declared_profile_truth=__import__('json').dumps(declared_profile_truth or {}, ensure_ascii=False, default=str),
        durable_profile_facts=_json_lines([*(declared_truth_facts or []), *(durable_profile_facts or [])]),
        session_evidence="\n".join(session_lines),
        persistent_goals=_json_lines(persistent_goals),
    )
    parsed = await call_json_llm(prompt=prompt, model=model, max_tokens=2800, temperature=0.1)
    return parsed or None


def normalize_identity_output(parsed: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "who_they_are": conservative_rewrite_text(parsed.get("who_they_are")) or None,
        "core_values": sanitize_list_of_dicts(parsed.get("core_values")),
        "recurring_patterns": sanitize_list_of_dicts(parsed.get("recurring_patterns")),
        "family_history": conservative_rewrite_text(parsed.get("family_history")) or None,
        "faith_and_beliefs": conservative_rewrite_text(parsed.get("faith_and_beliefs")) or None,
        "what_they_want": conservative_rewrite_text(parsed.get("what_they_want")) or None,
        "recurring_fears": sanitize_list_of_dicts(parsed.get("recurring_fears")),
        "what_they_avoid": conservative_rewrite_text(parsed.get("what_they_avoid")) or None,
        "how_they_relate": conservative_rewrite_text(parsed.get("how_they_relate")) or None,
        "persistent_goals": sanitize_list_of_dicts(parsed.get("persistent_goals")),
        "current_chapter": conservative_rewrite_text(parsed.get("current_chapter")) or None,
    }
