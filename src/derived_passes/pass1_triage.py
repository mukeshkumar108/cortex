from __future__ import annotations

from typing import Any, Dict, List, Optional

from .common import call_json_llm, clean_text, format_user_turns, text_list

PASS1_PROMPT = """You are the first-pass memory triage layer for a personal AI assistant.

Your job is NOT to write a nice summary and NOT to explain what the session means.
Your job is triage and routing:
- decide whether this session contains durable memory value
- identify what kind of follow-on processing it may need
- extract only concrete, user-stated updates worth routing forward

Pass 1 is not the place for personality reading, emotional interpretation,
or deeper synthesis. Later passes will go back to the raw transcript.

Return JSON only in this exact shape:

{
  "is_memory_worthy": true,
  "session_kind": "technical|personal|mixed|transient",
  "memory_deltas": [
    "short specific statement 1",
    "short specific statement 2"
  ],
  "entity_mentions": ["name1", "name2"],
  "identity_signals": [
    "short specific statement"
  ],
  "thread_signals": [
    "short specific unresolved or follow-up-worthy item"
  ],
  "run_entity_pass": true,
  "run_threads_pass": true,
  "identity_relevant": true,
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

- memory_deltas:
  the most important specific changes or updates from this session.
  These should be concrete and worth storing.
  Do NOT write generic summaries like "user discussed relationship updates."
  Prefer specific deltas like:
  - "Riley and the user are back together."
  - "Riley visited England and spent two weeks with the user."
  - "User sent Jordan an holiday message without expecting a reply."
  Do NOT explain what these facts reveal about the user's psychology.

- entity_mentions:
  only include people, projects, places, or named things that matter to durable memory.
  Do NOT include generic nouns or low-value transient mentions.

- identity_signals:
  only include concrete, evidence-grade observations from USER turns that may be useful
  for later identity review:
  values explicitly stated, beliefs explicitly stated, major biographical facts,
  durable roles, or repeated standards/preferences stated plainly.
  Keep them close to the user's wording.
  Good:
  - "User asked to be called out for approval-seeking behavior."
  - "User said integrity matters more than validation."
  - "User said Jasmine is his daughter."
  Bad:
  - "User struggles with focus and feels overwhelmed by unfinished projects."
  - "User is trying to prove his worth through work."
  - "User tends to overthink social interactions."
  Do NOT include generic mood or filler.
  If none, return [].

- thread_signals:
  unresolved situations a caring assistant should remember or follow up on later:
  health issues, relationship states, active commitments, unfinished decisions,
  waiting-for-reply situations, concrete worries with future relevance.
  If the user expresses a feeling about a situation, name the situation,
  not the feeling.
  Good:
  - "User needs reminders to stay hydrated after kidney stones."
  - "User has not heard back from Jasmine after sending a birthday message."
  - "User and his mother are currently not speaking."
  Bad:
  - "User is carrying deep grief about estrangement."
  - "User is navigating guilt and shame about Jasmine."
  Do NOT create a thread for every mention.
  If none, return [].

- run_entity_pass:
  true if this session contains new, changed, corrected, or reinforced information
  about a meaningful person, project, place, or named thing.

- run_threads_pass:
  true if this session contains a new unresolved thread, reinforces an existing one,
  or resolves one.

- identity_relevant:
  true only if the session reveals or updates something meaningful about the user's
  values, beliefs, enduring relationships, history, or character.

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
2. Prefer 1-4 high-signal memory_deltas, not many weak ones.
3. Do not restate the whole session.
4. Do not invent facts.
5. Do not treat assistant small talk, weather, pasta bake, or generic check-ins as durable memory unless they clearly matter.
6. Do not confuse "topic discussed" with "memory worth storing."
7. If a session is mostly technical but contains one important personal update, mark it memory-worthy and session_kind="mixed".
8. If the session contains corrections to prior memory, treat those as high-value memory_deltas.
9. If a topic is interesting but not relevant to the user's ongoing world, do not elevate it into durable memory by default.
10. Only extract facts stated or confirmed by the USER. Ignore assistant turns for fact extraction. The assistant may be wrong. The user is the source of truth.
11. Do not infer motives, inner states, coping strategies, attachment patterns, or personality traits.
12. If you are tempted to write "user is/struggles/tends/tries/wants to prove", you are probably doing later-pass work too early.
13. For thread_signals, prefer the external situation, state change, or follow-up need.
    Internal emotion by itself is not a thread.

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
    memory_deltas = text_list(payload.get("memory_deltas"), limit=6)
    entity_mentions = text_list(payload.get("entity_mentions"), limit=20)
    thread_signals = text_list(payload.get("thread_signals"), limit=8)
    identity_signals = text_list(payload.get("identity_signals"), limit=8)
    tension_signal = clean_text(payload.get("tension_signal")) or None
    context_relevant = to_bool(payload.get("context_relevant")) or bool(memory_deltas or thread_signals or tension_signal)
    return {
        "is_memory_worthy": to_bool(payload.get("is_memory_worthy")) or bool(memory_deltas or entity_mentions or thread_signals or identity_signals),
        "session_kind": clean_text(payload.get("session_kind")).lower() or "transient",
        "memory_deltas": memory_deltas,
        "entity_mentions": entity_mentions,
        "thread_signals": thread_signals,
        "identity_signals": identity_signals,
        "emotional_weight": clean_text(payload.get("emotional_weight")).lower() or "none",
        "emotional_note": clean_text(payload.get("emotional_note")) or None,
        "tension_signal": tension_signal,
        "context_relevant": context_relevant,
        "run_entity_pass": to_bool(payload.get("run_entity_pass")) or bool(entity_mentions),
        "run_threads_pass": to_bool(payload.get("run_threads_pass")) or bool(thread_signals),
        "identity_relevant": to_bool(payload.get("identity_relevant")) or bool(identity_signals),
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
