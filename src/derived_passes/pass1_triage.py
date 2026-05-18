from __future__ import annotations

from typing import Any, Dict, List, Optional

from .common import call_json_llm, clean_text, format_user_turns, text_list

PASS1_PROMPT = """You are the first-pass memory triage layer for a personal AI assistant.

Your job is NOT to write a nice summary and NOT to explain what the session means.
Your job is triage and routing:
- decide whether this session contains durable memory value
- identify what kind of follow-on processing it may need
- output only routing flags and lightweight triage metadata

Pass 1 is not the place for personality reading, emotional interpretation,
or deeper synthesis. Later passes will go back to the raw transcript.

Return JSON only in this exact shape:

{
  "is_memory_worthy": true,
  "session_kind": "technical|personal|mixed|transient",
  "run_actionable_pass": true,
  "run_session_changes_pass": true,
  "run_entity_pass": true,
  "run_threads_pass": true,
  "identity_relevant": true,
  "context_relevant": true,
  "emotional_weight": "none|low|medium|high",
  "emotional_note": "one short sentence if medium or high, otherwise null",
  "tension_signal": "one sentence or null. If this session contains a hint of something unspoken, avoided, or underneath the surface, name it briefly. Examples: 'User deflected from Riley topic back to work' | 'User expressed doubt then immediately pivoted to optimism' | 'User mentioned Jordan briefly then moved on quickly'. Only populate if clearly present in USER turns. Null if nothing notable.",
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

- run_actionable_pass:
  true when this session likely contains attention-worthy actions/events/obligations
  that should be processed by a dedicated downstream actionable extraction pass.
  Do not extract those items in Pass 1.

- run_session_changes_pass:
  true when this session likely contains factual or contextual changes that should
  be processed by a dedicated downstream session-changes lane.

- run_entity_pass:
  true if this session contains new, changed, corrected, or reinforced information
  about a meaningful person, project, place, or named thing.

- run_threads_pass:
  true if this session contains a new unresolved thread, reinforces an existing one,
  or resolves one.

- identity_relevant:
  true only if the session reveals or updates something meaningful about the user's
  values, beliefs, enduring relationships, history, or character.

- context_relevant:
  true if the session contains current-situation signal that should contribute to
  living context synthesis. This is a routing flag, not extracted content.

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
2. Return routing flags, not extracted payloads.
3. Do not restate the whole session.
4. Do not invent facts.
5. Do not treat assistant small talk, weather, pasta bake, or generic check-ins as durable memory unless they clearly matter.
6. Do not confuse "topic discussed" with "memory worth storing."
7. If a session is mostly technical but contains one important personal update, mark it memory-worthy and session_kind="mixed".
8. If the session contains corrections to prior memory, mark the relevant routing flags true.
9. If a topic is interesting but not relevant to the user's ongoing world, do not elevate it into durable memory by default.
10. Only extract facts stated or confirmed by the USER. Ignore assistant turns for fact extraction. The assistant may be wrong. The user is the source of truth.
11. Do not infer motives, inner states, coping strategies, attachment patterns, or personality traits.
12. If you are tempted to write "user is/struggles/tends/tries/wants to prove", you are probably doing later-pass work too early.
13. Do NOT output entity_mentions, thread_signals, actionable_candidates, memory_deltas, or identity_signals.

Transcript:
{{transcript}}
"""



def to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return clean_text(value).lower() in {"true", "yes", "1", "y"}


def normalize_pass1_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    tension_signal = clean_text(payload.get("tension_signal")) or None
    context_relevant = to_bool(payload.get("context_relevant")) or bool(tension_signal)
    return {
        "is_memory_worthy": to_bool(payload.get("is_memory_worthy")),
        "session_kind": clean_text(payload.get("session_kind")).lower() or "transient",
        "run_actionable_pass": to_bool(payload.get("run_actionable_pass")),
        "run_session_changes_pass": to_bool(payload.get("run_session_changes_pass")),
        "emotional_weight": clean_text(payload.get("emotional_weight")).lower() or "none",
        "emotional_note": clean_text(payload.get("emotional_note")) or None,
        "tension_signal": tension_signal,
        "context_relevant": context_relevant,
        "run_entity_pass": to_bool(payload.get("run_entity_pass")),
        "run_threads_pass": to_bool(payload.get("run_threads_pass")),
        "identity_relevant": to_bool(payload.get("identity_relevant")),
        "ignore_reasons": text_list(payload.get("ignore_reasons"), limit=8),
    }


async def run_rich_pass1_llm(*, messages: List[Dict[str, Any]], model: str) -> Optional[Dict[str, Any]]:
    transcript = format_user_turns(messages)
    if not transcript:
        return None
    prompt = PASS1_PROMPT.replace("{{transcript}}", transcript)
    parsed = await call_json_llm(prompt=prompt, model=model, max_tokens=1600, temperature=0.1)
    if not parsed:
        return None
    return normalize_pass1_payload(parsed)
