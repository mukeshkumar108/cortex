from __future__ import annotations

from typing import Any, Dict, List, Optional

from .common import as_list, call_json_llm, clean_text, format_user_turns, safe_json, text_list

RESOLVE_PROMPT_TEMPLATE = """You are maintaining an entity registry for a personal
AI assistant. Your job is to decide what to do with
each new entity mention.

EXISTING ENTITIES IN REGISTRY:
{existing_entities}

NEW ENTITY MENTIONS WITH CONTEXT:
{new_mentions}

For each new mention decide one of:

MATCH — this refers to an existing entity
- Use context to confirm, not just name matching
- "User's girlfriend", "my girlfriend", "she" in
  romantic context -> try to match to existing
  partner entity
- If same name but context clearly suggests different
  person -> do NOT match, create NEW instead
- Provide matched_entity_id and confidence 0.0-1.0

NEW — this is a genuinely new entity worth tracking
Classify type as one of:
  person = real human being in the user's life
  project = something the user is building or working on
  place = location user lives in or frequently mentions
  other = anything else worth tracking

TENTATIVE — this may matter later, but current evidence is too weak
to create a confident profile or surface as a real relationship yet.

Decide status:
  tentative = low signal, not enough to profile or surface yet
  active = enough signal to build a full profile now

Mark as ACTIVE if ANY of these are true:
  - Person with 3+ mentions
  - Person who is clearly family or romantic partner
  - Project the user is actively building
  - Person mentioned with high emotional significance
    even if only 1-2 mentions

SKIP — not worth tracking
  - Tools and technologies (SDKs, frameworks, APIs,
    libraries, model names)
  - Films, shows, books, music, media
  - Places mentioned in passing, even if repeated,
    unless the user explicitly lives there or the
    place has deep personal significance
  - Vague references: "someone", "a friend", "they"
  - One-off low-significance mentions

For every MATCH, NEW, or TENTATIVE decision include:
  evidence_strength: weak / medium / strong
  memory_relevance: low / medium / high
  relationship_confidence: 0.0-1.0
  why_this_matters: one concrete sentence grounded in USER text

RULES:
1. Do not invent relationship meaning. If evidence is weak, use TENTATIVE or SKIP.
2. Prefer MATCH over NEW when context supports it.
3. Prefer SKIP over NEW for anything technical or media.
4. A NEW active entity requires medium/strong evidence and clear memory relevance.
5. Keep relationship_to_user coarse:
   girlfriend / daughter / son / friend / colleague /
   active_project / mother / father / other
   NOT long prose descriptions.
6. Only extract facts from USER turns. Ignore assistant.
7. Do not connect facts across entities. Each entity decision must stand on its own evidence.

Return JSON only — no preamble, no markdown fences:
{{
  "resolutions": [
    {{
      "mention": "Riley",
      "decision": "MATCH",
      "matched_entity_id": "existing-uuid",
      "confidence": 0.95,
      "evidence_strength": "strong",
      "memory_relevance": "high",
      "relationship_confidence": 0.95,
      "why_this_matters": "The user is discussing Riley as a romantic partner.",
      "reason": "same romantic partner context"
    }},
    {{
      "mention": "Bluum",
      "decision": "NEW",
      "canonical_name": "Bluum",
      "canonical_name_normalized": "bluum",
      "type": "project",
      "status": "active",
      "relationship_to_user": "active_project",
      "aliases": ["Bloom"],
      "confidence": 0.9,
      "evidence_strength": "strong",
      "memory_relevance": "high",
      "relationship_confidence": 0.0,
      "why_this_matters": "The user is actively building this product.",
      "reason": "user's product, multiple mentions"
    }},
    {{
      "mention": "Jordan",
      "decision": "TENTATIVE",
      "canonical_name": "Jordan",
      "canonical_name_normalized": "jordan",
      "type": "person",
      "status": "tentative",
      "relationship_to_user": "other",
      "aliases": ["Jordan"],
      "confidence": 0.45,
      "evidence_strength": "weak",
      "memory_relevance": "medium",
      "relationship_confidence": 0.2,
      "why_this_matters": "The user mentioned Jordan, but the relationship is not clear in this session.",
      "reason": "potentially meaningful person, insufficient relationship evidence"
    }},
    {{
      "mention": "cell SDK",
      "decision": "SKIP",
      "reason": "technology tool"
    }}
  ]
}}
"""

PROFILE_PROMPT_TEMPLATE = """You are building a memory profile for an entity that
matters to the user of a personal AI assistant named Sophie.

Entity name: {canonical_name}
Type: {entity_type}
Relationship to user: {relationship_to_user}

{profile_mode_block}

USER TURNS FROM RELEVANT CONVERSATIONS
(oldest first, assistant turns removed):
{transcript_text}

RULES:
1. Only use facts stated by the USER.
   The assistant turns have been removed.
   If any assistant text appears, ignore it completely.
2. Be specific and grounded. No invented facts.
3. Note uncertainty rather than guessing.
4. Do not stitch facts into a causal story unless the user explicitly said the connection
   or the same connection is repeated in the provided evidence.
5. Do not import facts from another entity or relationship.
6. If the evidence is too thin for a useful profile, return profile_text as null
   and put the uncertainty in open_questions.
7. Write profile_text as Sophie's internal knowledge —
   warm and natural, as if a close friend is describing
   this person. Not clinical. Not a report.
8. Keep key_facts discrete and specific — one fact
   per entry, not compound sentences.
9. relationship_to_user should stay coarse:
   girlfriend / daughter / active_project / friend etc.

Return JSON only — no preamble, no markdown fences:
{{
  "profile_text": "natural prose paragraph describing who this person/entity is, their relationship to the user, key facts, current status, recent developments",
  "key_facts": [
    {{
      "fact": "Riley is the user's long-term long-distance girlfriend",
      "confidence": 0.98,
      "first_mentioned": "2026-02-09",
      "last_confirmed": "2026-04-09"
    }}
  ],
  "open_questions": [
    "Where exactly does Riley live?"
  ],
  "last_known_status": "Back together as of April 2026 after brief breakup"
}}
"""

ENTITY_DISCOVERY_PROMPT = """You are identifying durable entity mentions from a user transcript for a personal AI assistant.

Your job is only to identify named entities worth sending to a downstream entity-resolution pass.

Include only named things that materially matter to the user's life or work:
- people
- projects/products/companies
- places with ongoing relevance
- other named things with durable memory value

Skip:
- generic nouns
- one-off tools/frameworks/libraries/model names unless clearly central to the user's world
- media titles mentioned casually
- vague references like "someone" or "they"

Only use USER turns.

Return JSON only:
{{
  "entity_mentions": ["Riley", "Bluum"]
}}

If none, return:
{{
  "entity_mentions": []
}}

Transcript:
{transcript_text}
"""


async def discover_entity_mentions(
    *,
    messages: List[Dict[str, Any]],
    model: str,
) -> Optional[List[str]]:
    transcript_text = format_user_turns(messages)
    if not transcript_text:
        return None
    prompt = ENTITY_DISCOVERY_PROMPT.format(transcript_text=transcript_text)
    parsed = await call_json_llm(prompt=prompt, model=model, max_tokens=1000, temperature=0.1)
    if not parsed:
        return None
    return text_list(parsed.get("entity_mentions"), limit=20)




async def resolve_entity_mentions(
    *,
    existing_entities: List[Dict[str, Any]],
    mentions: List[Dict[str, Any]],
    model: str,
) -> Optional[List[Dict[str, Any]]]:
    existing_lines = []
    for entity in existing_entities:
        existing_lines.append(
            f"- id={entity.get('entity_id')} name={entity.get('canonical_name')} type={entity.get('type')} relationship={entity.get('relationship_to_user')} aliases={entity.get('aliases')}"
        )
    mention_lines = []
    for mention in mentions:
        mention_lines.append(__import__('json').dumps(mention, ensure_ascii=False, default=str))
    prompt = RESOLVE_PROMPT_TEMPLATE.format(
        existing_entities="\n".join(existing_lines) or "(none)",
        new_mentions="\n".join(mention_lines) or "(none)",
    )
    parsed = await call_json_llm(prompt=prompt, model=model, max_tokens=2000, temperature=0.1)
    if not parsed:
        return None
    resolutions = parsed.get("resolutions") if isinstance(parsed.get("resolutions"), list) else []
    return [r for r in resolutions if isinstance(r, dict)]


async def build_entity_profile(
    *,
    canonical_name: str,
    entity_type: str,
    relationship_to_user: str,
    messages: List[Dict[str, Any]],
    existing_profile_text: str | None,
    model: str,
) -> Optional[Dict[str, Any]]:
    transcript_text = format_user_turns(messages)
    if not transcript_text:
        return None
    existing = clean_text(existing_profile_text)
    if existing:
        profile_mode_block = (
            "EXISTING PROFILE:\n"
            f"{existing}\n\n"
            "This is an update. The transcripts below are NEW sessions only. "
            "Preserve accurate existing facts. Add new information. Update anything "
            "that has changed. Note contradictions explicitly rather than silently overwriting."
        )
    else:
        profile_mode_block = (
            "This is a first build. Build from scratch using only what the user "
            "has stated in the transcripts below."
        )
    prompt = PROFILE_PROMPT_TEMPLATE.format(
        canonical_name=clean_text(canonical_name),
        entity_type=clean_text(entity_type) or "other",
        relationship_to_user=clean_text(relationship_to_user) or "other",
        profile_mode_block=profile_mode_block,
        transcript_text=transcript_text,
    )
    parsed = await call_json_llm(prompt=prompt, model=model, max_tokens=1800, temperature=0.1)
    if not parsed:
        return None
    return {
        "profile_text": clean_text(parsed.get("profile_text")) or None,
        "key_facts": as_list(parsed.get("key_facts")),
        "open_questions": text_list(parsed.get("open_questions"), limit=12),
        "last_known_status": clean_text(parsed.get("last_known_status")) or None,
    }
