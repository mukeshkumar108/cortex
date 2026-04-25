from __future__ import annotations

from typing import Any, Dict, List, Optional

from .common import as_list, call_json_llm, clean_text, format_user_turns, safe_json, text_list

THREAD_EXTRACTION_PROMPT = """You are maintaining an open thread registry for a
personal AI assistant named Sophie.

Open threads are unresolved situations a caring attentive
friend would remember and follow up on later.

TODAY'S SESSION:
Date: {session_date}
Emotional weight: {emotional_weight}
Emotional note: {emotional_note}
Thread routing hints from triage (routing only, not authority): {thread_signals}

USER TURNS FROM THIS SESSION (assistant turns removed):
{transcript_text}

EXISTING OPEN THREADS (so you can update not duplicate):
{existing_threads}

Your job:

For each meaningful unresolved thing in this session,
decide one of:

CREATE — new thread not in the existing list
UPDATE — this session adds new info to an existing thread
RESOLVE — this session indicates a thread is now resolved
SNOOZE — thread is still open but not actively relevant
NO_ACTION — thread still open, nothing new to add

A thread is worth creating if it is:
- A health issue, symptom, or medical situation
- A relationship tension or unresolved situation
- A commitment or intention the user stated that has real future follow-up value
- A concrete worry, risk, or waiting situation with future follow-up value
- Something in progress with no clear resolution
- Something a good friend would ask about next time

A thread is NOT worth creating if it is:
- A passing comment with no ongoing significance
- A resolved fact (Riley is back together —
  that's a memory_delta not a thread)
- Technical work on Sophie's codebase
- Generic emotional states without specific situation
- Something the user clearly resolved in this same session
- Casual one-off plans with no follow-up value

CATEGORY options:
health / relationship / goal / commitment /
worry / project / other

PRIORITY:
high = time-sensitive or emotionally significant
medium = worth following up within a week or two
low = worth noting but not urgent

For follow_up_after:
- health issues: 3-7 days
- relationship situations: 5-10 days
- goals/commitments: 7-14 days
- worries: 3-7 days
- Use session_date as the base date

RULES:
1. Only use facts stated by the USER.
   Ignore assistant turns completely.
2. Be specific. "User's leg was hurting" not
   "User mentioned a health issue."
3. Keep detail concrete and situational.
   Do not add emotional narration, motive inference,
   psychologizing, or "deeper meaning."
   Name the underlying situation, not the user's emotional reaction to it.
   Bad:
   - "User is carrying intense grief and shame..."
   - "User is using work to avoid the pain..."
   Good:
   - "User has not spoken to Jasmine for six years and is sending low-pressure messages."
   - "User wants hydration reminders after a kidney stone issue."
   Better title/detail pairs:
   - title: "Reconnecting with Jasmine"
     detail: "User sent Jasmine a birthday message and has not heard back yet."
   - title: "Relationship with mother is currently severed"
     detail: "User said he and his mother are not speaking right now."
4. One thread per distinct situation.
   Don't merge unrelated things into one thread.
5. Don't create threads for things already resolved
   in the same session.
6. If uncertain whether something is a thread, use NO_ACTION
   unless you can explain why it matters later.
7. Resolution note should be specific:
   "User confirmed kidney stones resolved,
    just needs to drink more water"
   not just "resolved"
8. Treat triage thread hints as routing hints only.
   If a hint is not directly supported by USER turns,
   do not use it.
9. Relationship threads should usually be about the relationship situation
   with a specific person, not about the user's grief, shame, anger, or fear
   about that relationship.
10. Thread titles should be neutral situation labels, not character or emotion summaries.
11. For every CREATE/UPDATE/RESOLVE/SNOOZE include:
   unresolvedness: open / resolved / unclear
   follow_up_value: low / medium / high
   evidence_strength: weak / medium / strong
   why_this_matters_later: one concrete sentence, or empty
   if the action is RESOLVE and no follow-up is needed

Return JSON only — no preamble, no markdown:
{{
  "actions": [
    {{
      "action": "CREATE",
      "title": "User's leg was hurting",
      "detail": "User mentioned their leg had been painful.",
      "category": "health",
      "priority": "medium",
      "unresolvedness": "open",
      "follow_up_value": "medium",
      "evidence_strength": "medium",
      "why_this_matters_later": "Pain may still affect the user's plans and is worth checking on.",
      "related_entities": ["Riley"],
      "follow_up_after": "2026-04-14",
      "source_session_id": "session-id-here"
    }},
    {{
      "action": "UPDATE",
      "thread_id": "existing-thread-uuid",
      "detail": "User confirmed kidney stones resolved but needs hydration.",
      "unresolvedness": "open",
      "follow_up_value": "medium",
      "evidence_strength": "strong",
      "why_this_matters_later": "Hydration remains an active prevention goal.",
      "last_mentioned_at": "2026-04-09",
      "follow_up_after": "2026-04-16"
    }},
    {{
      "action": "RESOLVE",
      "thread_id": "existing-thread-uuid",
      "unresolvedness": "resolved",
      "follow_up_value": "low",
      "evidence_strength": "strong",
      "why_this_matters_later": "",
      "resolution_note": "Riley visited England, they reconciled."
    }}
  ]
}}
"""


THREAD_AUDIT_PROMPT = """You are conservatively auditing an open thread registry.

Current open threads:
{open_threads}

Your primary job is hygiene, not rewriting meaning.

Read every thread carefully. For each group of threads
that describe the same underlying situation, merge them
into one. Keep the richest, most detailed version.

Two threads are the SAME situation if they describe:
- The same health issue (even if worded differently)
- The same relationship tension or situation
- The same goal or commitment
- The same worry about the same thing
- The same frustration or recurring pattern

Only propose MERGE when the duplicate is clear.
Ambiguous actions must be FLAG_REVIEW, not forced.

SPECIAL RULE — assistant_feedback category:
Any thread about Sophie's failures, errors,
hallucinations, or behavioral problems should be:
1. Merged into a SINGLE assistant_feedback thread
2. Category set to assistant_feedback
3. NOT treated as a personal thread about the user
These threads inform Sophie's behavior but should
not dominate the user's personal handover context.

RESOLVE threads that are clearly complete:
- Technical work that is mentioned as finished
- Health issues confirmed resolved
- Situations where a later thread confirms resolution

SNOOZE threads that are stale:
- follow_up_after more than 45 days ago
- Not mentioned in last 30 days
- No active signal of ongoing relevance

FLAG_REVIEW anything that may be wrong but is not safe
to mutate automatically.

CRITICAL — do NOT merge:
- Different health issues even if both are health
- Different people even if both are relationship
- A parent and their child
- Things that are related but genuinely distinct

Return JSON only:
{{
  "audit_actions": [
    {{
      "action": "MERGE",
      "keep_thread_id": "uuid",
      "absorb_thread_ids": ["uuid1", "uuid2"],
      "merged_title": "clean title for merged thread",
      "merged_detail": "combined detail from all threads",
      "merged_category": "correct category",
      "reason": "all describe same Sophie trust breakdown",
      "confidence": 0.92
    }},
    {{
      "action": "RESOLVE",
      "thread_id": "uuid",
      "resolution_note": "specific reason",
      "confidence": 0.95
    }},
    {{
      "action": "SNOOZE",
      "thread_id": "uuid",
      "reason": "stale",
      "confidence": 0.9
    }},
    {{
      "action": "CATEGORY_FIX",
      "thread_id": "uuid",
      "new_category": "assistant_feedback",
      "reason": "about Sophie behavior not user's life",
      "confidence": 0.95
    }},
    {{
      "action": "FLAG_REVIEW",
      "thread_id": "uuid",
      "reason": "possible duplicate but not safe to merge",
      "confidence": 0.6
    }}
  ]
}}
"""


VALID_PRIORITIES = {"high", "medium", "low"}
VALID_CATEGORIES = {"health", "relationship", "goal", "commitment", "worry", "project", "assistant_feedback", "other"}


VALID_CATEGORIES = {"health", "relationship", "goal", "commitment", "worry", "project", "other", "assistant_feedback"}
VALID_PRIORITIES = {"high", "medium", "low"}


async def extract_thread_actions(
    *,
    messages: List[Dict[str, Any]],
    session_date: Any,
    emotional_weight: str,
    emotional_note: str | None,
    thread_signals: List[str],
    existing_threads: List[Dict[str, Any]],
    model: str,
) -> Optional[List[Dict[str, Any]]]:
    existing_lines = []
    for thread in existing_threads:
        existing_lines.append(
            f"- id={thread.get('thread_id')} status={thread.get('status')} title={thread.get('title')} detail={thread.get('detail')}"
        )
    prompt = THREAD_EXTRACTION_PROMPT.format(
        session_date=session_date.isoformat() if hasattr(session_date, 'isoformat') else str(session_date),
        emotional_weight=emotional_weight or "none",
        emotional_note=emotional_note or "(none)",
        thread_signals=__import__('json').dumps(thread_signals or [], ensure_ascii=False),
        transcript_text=format_user_turns(messages),
        existing_threads="\n".join(existing_lines),
    )
    parsed = await call_json_llm(prompt=prompt, model=model, max_tokens=1800, temperature=0.1)
    if not parsed:
        return None
    actions = parsed.get("actions") if isinstance(parsed.get("actions"), list) else []
    return [a for a in actions if isinstance(a, dict)]


async def audit_thread_registry(
    *,
    open_threads: List[Dict[str, Any]],
    model: str,
) -> Optional[List[Dict[str, Any]]]:
    lines = []
    for thread in open_threads:
        lines.append(__import__("json").dumps(thread, ensure_ascii=False, default=str))
    prompt = THREAD_AUDIT_PROMPT.format(open_threads="\n".join(lines) or "(none)")
    parsed = await call_json_llm(prompt=prompt, model=model, max_tokens=2200, temperature=0.1)
    if not parsed:
        return None
    actions = parsed.get("audit_actions") if isinstance(parsed.get("audit_actions"), list) else []
    return [a for a in actions if isinstance(a, dict)]
