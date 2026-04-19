#!/usr/bin/env python3
"""Entity registry pipeline for Synapse."""

from __future__ import annotations

import argparse
import asyncio
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from src.config import get_settings
from src.db import Database


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

Decide status:
  tentative = low signal, not enough to profile yet
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

RULES:
1. Make your best guess. More evidence will arrive
   later and the system will refine over time.
2. Prefer MATCH over NEW when in doubt.
3. Prefer SKIP over NEW for anything technical or media.
4. Keep relationship_to_user coarse:
   girlfriend / daughter / son / friend / colleague /
   active_project / mother / father / other
   NOT long prose descriptions.
5. Only extract facts from USER turns. Ignore assistant.

Return JSON only — no preamble, no markdown fences:
{{
  "resolutions": [
    {{
      "mention": "Ashley",
      "decision": "MATCH",
      "matched_entity_id": "existing-uuid",
      "confidence": 0.95,
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
      "reason": "user's product, multiple mentions"
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
4. Write profile_text as Sophie's internal knowledge —
   warm and natural, as if a close friend is describing
   this person. Not clinical. Not a report.
5. Keep key_facts discrete and specific — one fact
   per entry, not compound sentences.
6. relationship_to_user should stay coarse:
   girlfriend / daughter / active_project / friend etc.

Return JSON only — no preamble, no markdown fences:
{{
  "profile_text": "natural prose paragraph describing
                   who this person/entity is, their
                   relationship to the user, key facts,
                   current status, recent developments",
  "key_facts": [
    {{
      "fact": "Ashley is the user's long-term
               long-distance girlfriend",
      "confidence": 0.98,
      "first_mentioned": "2026-02-09",
      "last_confirmed": "2026-04-09"
    }}
  ],
  "open_questions": [
    "Where exactly does Ashley live?"
  ],
  "last_known_status": "Back together as of April 2026
                        after brief breakup"
}}
"""

AUDIT_PROMPT_TEMPLATE = """You are auditing an entity registry for a personal AI
assistant. You have full visibility of all entities
for this user.

Your job is to find and fix four types of problems:

1. DUPLICATES — different entries that refer to the
   same real-world entity

2. MISCLASSIFICATIONS — wrong type or relationship_to_user

3. FRAGMENTATION — a person with many mentions is split
   across multiple partial entries

4. ORPHANED MENTIONS — entity mentions that appear
   frequently in conversations but have no profile.
   These were missed during resolution.

Here are ALL current entities for this user:

{entities_block}

Orphaned mentions (seen 2+ times, no profile):
{orphaned_mentions_block}

Common patterns to look for:

DUPLICATES:
- "User's Girlfriend" + "Ashley" -> same person if
  context matches romantic partner
- "Jasmine" + "Jasmine Sophia Kumar" + "Jasmine's Mother"
  -> are any of these the same person?
- Spelling variants: "Bluum" vs "Bloom"
- Possessive variants: "Jasmine's mum" as separate entity
  when Jasmine's mother is already tracked

MISCLASSIFICATIONS:
- An AI assistant (Sophie) classified as type=other
  with relationship=friend -> should be type=project
  or type=other with relationship=ai_assistant
- A product classified as a person
- A place classified as a person

FRAGMENTATION:
- Same person appearing under full name AND first name
  AND nickname as separate entries
- Mentions split across variants when one canonical
  entry should hold all of them

For each problem found, return one of these actions:

MERGE — combine two entities into one
  - keep_entity_id: the canonical one to keep
  - absorb_entity_id: the one to merge in and delete
  - canonical_name: what the merged entity should be called
  - canonical_name_normalized: lowercase version
  - relationship_to_user: correct value
  - type: correct value
  - reason: why these are the same

FIX — correct a field on an existing entity
  - entity_id: which entity
  - field: which field to fix
  - old_value: what it currently says
  - new_value: what it should say
  - reason: why

CREATE — create a missing entity from orphaned mentions
  - mention: the orphaned mention text
  - canonical_name: canonical profile name
  - canonical_name_normalized: lowercase version
  - type: person / project / place / other
  - status: active / tentative
  - relationship_to_user: coarse relationship
  - aliases: optional aliases list
  - confidence: 0.0-1.0
  - session_ids: optional explicit session ids
  - reason: why this should exist

SKIP — do not create an orphaned mention profile
  - mention: orphaned mention text
  - reason: why it should be skipped

NO_ACTION — if everything looks correct

Rules:
1. When merging, the entity with more mentions and
   richer profile_text is usually the canonical one
2. Sophie is an AI assistant product, not a friend
3. "User's girlfriend/partner/she" in romantic context
   should merge with the named partner if one exists
4. A mother/father mentioned only in relation to another
   entity does not need their own profile unless they
   appear independently with their own facts
5. Be conservative — only merge when confident
   If uncertain, leave as separate and add a note

CRITICAL RULES FOR MERGING — do NOT merge if:
- One entity is a PARENT of another entity
  e.g. "Jasmine's Mother" is NOT the same as "Jasmine"
  e.g. "Ashley's daughter" is NOT the same as "Ashley"
  Possessive relationships indicate DIFFERENT people
- One entity is a PLACE and another is a PERSON
- relationship_to_user values are clearly incompatible
  e.g. daughter vs mother
- You are uncertain. Conservative merging is better
  than wrong merging.

Return JSON only:
{{
  "audit_actions": [
    {{
      "action": "MERGE",
      "keep_entity_id": "uuid-of-ashley",
      "absorb_entity_id": "uuid-of-users-girlfriend",
      "canonical_name": "Ashley",
      "canonical_name_normalized": "ashley",
      "type": "person",
      "relationship_to_user": "girlfriend",
      "reason": "same romantic partner context"
    }},
    {{
      "action": "FIX",
      "entity_id": "uuid-of-sophie",
      "field": "relationship_to_user",
      "old_value": "friend",
      "new_value": "ai_assistant",
      "reason": "Sophie is an AI assistant product"
    }},
    {{
      "action": "CREATE",
      "mention": "Jasmine",
      "canonical_name": "Jasmine",
      "canonical_name_normalized": "jasmine",
      "type": "person",
      "status": "active",
      "relationship_to_user": "daughter",
      "aliases": ["Jasmine Sophia Kumar"],
      "confidence": 0.9,
      "reason": "Frequent mention with strong relational context"
    }}
  ]
}}
"""


@dataclass
class EntityCandidate:
    name: str
    key: str
    mention_count: int
    session_ids: List[str]
    context_snippets: List[str]
    aliases: List[str]


def normalize_name(name: str) -> str:
    return (name or "").strip().title()


def normalize_key(name: str) -> str:
    return (name or "").strip().lower()


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    return " ".join(value.split()).strip()


def _extract_json_text(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        return raw
    fenced = re.match(r"^```(?:json)?\s*(.*?)\s*```$", raw, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        return fenced.group(1).strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start != -1 and end != -1 and end > start:
        return raw[start : end + 1].strip()
    return raw


def _to_text_list(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    out: List[str] = []
    for item in value:
        txt = _normalize_text(item)
        if txt:
            out.append(txt)
    return out


def _safe_json(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


async def _call_openrouter_json(
    *,
    api_key: str,
    model: str,
    prompt: str,
    timeout_seconds: float = 120.0,
) -> Dict[str, Any]:
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "response_format": {"type": "json_object"},
        "temperature": 0.1,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            response = await client.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=headers,
                json=payload,
            )
    except Exception as exc:
        return {
            "ok": False,
            "status_code": None,
            "raw_output": "",
            "parsed": None,
            "error": f"openrouter_request_failed: {exc}",
        }
    body: Dict[str, Any] = {}
    try:
        body = response.json()
    except Exception:
        body = {}
    content = _normalize_text((((body.get("choices") or [{}])[0]).get("message") or {}).get("content"))
    json_text = _extract_json_text(content)
    parsed = None
    parse_error = None
    if json_text:
        try:
            parsed = json.loads(json_text)
        except Exception as exc:
            parse_error = f"model_output_json_parse_failed: {exc}"
    else:
        parse_error = "empty_model_output"
    return {
        "ok": response.is_success and isinstance(parsed, dict),
        "status_code": response.status_code,
        "raw_output": content,
        "parsed": parsed if isinstance(parsed, dict) else None,
        "error": parse_error,
    }


def _format_user_turns(messages: List[Dict[str, Any]], created_at: Any) -> str:
    ts_label = ""
    if isinstance(created_at, datetime):
        dt = created_at.astimezone(timezone.utc)
        ts_label = dt.strftime("%Y-%m-%d %H:%M")
    elif created_at:
        ts_label = _normalize_text(created_at)

    lines: List[str] = []
    for m in messages or []:
        if not isinstance(m, dict):
            continue
        role = _normalize_text(m.get("role")).lower()
        if role != "user":
            continue
        text = m.get("text")
        if text is None:
            continue
        lines.append(f"[{ts_label}]\nUser: {text}")
    return "\n".join(lines).strip()


def _append_error(errors_file: Path, payload: Dict[str, Any]) -> None:
    with errors_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _looks_possessive_relative(name: str) -> bool:
    n = normalize_key(name)
    return bool(
        re.search(r"\b.+['’]s\s+(mother|mum|father|dad|daughter|son)\b", n)
        or re.search(r"\b(mother|mum|father|dad)\s+of\b", n)
    )


def _merge_is_incompatible(keep: Dict[str, Any], absorb: Dict[str, Any]) -> Optional[str]:
    keep_type = normalize_key(_normalize_text(keep.get("type")))
    absorb_type = normalize_key(_normalize_text(absorb.get("type")))
    if {keep_type, absorb_type} == {"place", "person"}:
        return "type_place_vs_person"

    keep_rel = normalize_key(_normalize_text(keep.get("relationship_to_user")))
    absorb_rel = normalize_key(_normalize_text(absorb.get("relationship_to_user")))
    incompatible_rel = {
        ("daughter", "mother"),
        ("daughter", "father"),
        ("son", "mother"),
        ("son", "father"),
        ("mother", "daughter"),
        ("mother", "son"),
        ("father", "daughter"),
        ("father", "son"),
    }
    if (keep_rel, absorb_rel) in incompatible_rel:
        return f"relationship_incompatible:{keep_rel}:{absorb_rel}"

    keep_name = _normalize_text(keep.get("canonical_name"))
    absorb_name = _normalize_text(absorb.get("canonical_name"))
    if _looks_possessive_relative(keep_name) != _looks_possessive_relative(absorb_name):
        return "possessive_relative_vs_non_relative"

    return None


async def _collect_user_transcript_blocks(
    *,
    db: Database,
    session_ids: List[str],
) -> tuple[str, Optional[datetime]]:
    if not session_ids:
        return "", None
    transcript_rows = await db.fetch(
        """
        SELECT session_id, created_at, messages
        FROM session_transcript
        WHERE session_id = ANY($1::text[])
        ORDER BY created_at ASC
        """,
        session_ids,
    )
    user_turn_blocks: List[str] = []
    latest_session_date: Optional[datetime] = None
    for tr in transcript_rows:
        messages = tr.get("messages")
        if not isinstance(messages, list):
            messages = []
        block = _format_user_turns(messages, tr.get("created_at"))
        if block:
            user_turn_blocks.append(block)
        created = tr.get("created_at")
        if isinstance(created, datetime) and (latest_session_date is None or created > latest_session_date):
            latest_session_date = created
    return "\n\n".join(user_turn_blocks).strip(), latest_session_date


async def _build_profile_from_sessions(
    *,
    db: Database,
    api_key: str,
    model: str,
    timeout_seconds: float,
    canonical_name: str,
    entity_type: str,
    relationship_to_user: str,
    session_ids: List[str],
    profile_text_existing: Optional[str],
) -> tuple[Optional[Dict[str, Any]], Optional[datetime], Optional[str], Optional[int], Optional[str]]:
    transcript_text, latest_session_date = await _collect_user_transcript_blocks(db=db, session_ids=session_ids)
    if not transcript_text:
        return None, latest_session_date, None, None, "no_user_transcript_text"

    if not _normalize_text(profile_text_existing):
        profile_mode_block = "This is a first build. Build from scratch using only\nwhat the user has stated in the transcripts below."
    else:
        profile_mode_block = (
            "EXISTING PROFILE:\n"
            f"{_normalize_text(profile_text_existing)}\n\n"
            "This is an update. The transcripts below are NEW sessions\n"
            "only — ones that occurred after the existing profile was\n"
            "last built. Preserve all accurate existing facts. Add new\n"
            "information. Update anything that has changed. Note\n"
            "contradictions explicitly rather than silently overwriting."
        )

    profile_prompt = PROFILE_PROMPT_TEMPLATE.format(
        canonical_name=_normalize_text(canonical_name),
        entity_type=_normalize_text(entity_type),
        relationship_to_user=_normalize_text(relationship_to_user) or "other",
        profile_mode_block=profile_mode_block,
        transcript_text=transcript_text,
    )
    profile_result = await _call_openrouter_json(
        api_key=api_key,
        model=model,
        prompt=profile_prompt,
        timeout_seconds=timeout_seconds,
    )
    if not profile_result.get("ok"):
        return None, latest_session_date, profile_result.get("raw_output"), profile_result.get("status_code"), profile_result.get("error")
    return _safe_json(profile_result.get("parsed")), latest_session_date, None, None, None


async def find_orphaned_mentions(*, user_id: str, db: Database) -> List[Dict[str, Any]]:
    mention_rows = await db.fetch(
        """
        SELECT
          m.mention AS mention,
          COUNT(*)::int AS times,
          array_agg(DISTINCT sc.session_id) AS session_ids
        FROM session_classifications sc,
             unnest(sc.entity_mentions) AS m(mention)
        WHERE sc.user_id = $1
          AND sc.is_memory_worthy = true
        GROUP BY m.mention
        HAVING COUNT(*) >= 2
        ORDER BY COUNT(*) DESC
        """,
        user_id,
    )

    existing_rows = await db.fetch(
        """
        SELECT canonical_name_normalized, aliases
        FROM entity_profiles
        WHERE user_id = $1
        """,
        user_id,
    )
    existing_names: set[str] = set()
    for row in existing_rows:
        canonical = normalize_key(_normalize_text(row.get("canonical_name_normalized")))
        if canonical:
            existing_names.add(canonical)
        aliases = row.get("aliases")
        if isinstance(aliases, list):
            for alias in aliases:
                key = normalize_key(alias)
                if key:
                    existing_names.add(key)

    orphaned: List[Dict[str, Any]] = []
    for row in mention_rows:
        mention = _normalize_text(row.get("mention"))
        key = normalize_key(mention)
        if not key or key in existing_names:
            continue
        session_ids = row.get("session_ids") if isinstance(row.get("session_ids"), list) else []
        orphaned.append(
            {
                "mention": mention,
                "times": int(row.get("times") or 0),
                "session_ids": [_normalize_text(s) for s in session_ids if _normalize_text(s)],
            }
        )
    return orphaned


async def run_entity_audit(
    *,
    user_id: str,
    db: Database,
    api_key: str,
    model: str,
    timeout_seconds: float,
    errors_file: Path,
) -> Dict[str, int]:
    stats = {"merge_actions": 0, "fix_actions": 0, "create_actions": 0, "orphaned_checked": 0, "errors": 0}

    entities = await db.fetch(
        """
        SELECT
          entity_id,
          canonical_name,
          canonical_name_normalized,
          type,
          status,
          relationship_to_user,
          mention_count,
          profile_text,
          aliases,
          source_session_ids
        FROM entity_profiles
        WHERE user_id = $1
        ORDER BY mention_count DESC, canonical_name ASC
        """,
        user_id,
    )
    if not entities:
        return stats

    entity_lines: List[str] = []
    for e in entities:
        entity_lines.append(
            " | ".join(
                [
                    _normalize_text(e.get("entity_id")),
                    _normalize_text(e.get("canonical_name")),
                    _normalize_text(e.get("type")),
                    _normalize_text(e.get("status")),
                    _normalize_text(e.get("relationship_to_user")),
                    str(e.get("mention_count") or 0),
                    _normalize_text((e.get("profile_text") or "")[:200]),
                    json.dumps(e.get("aliases") or [], ensure_ascii=False),
                ]
            )
        )
    orphaned_mentions = await find_orphaned_mentions(user_id=user_id, db=db)
    stats["orphaned_checked"] = len(orphaned_mentions)
    orphaned_mentions_block = "\n".join(
        json.dumps(item, ensure_ascii=False) for item in orphaned_mentions
    ) or "(none)"

    prompt = AUDIT_PROMPT_TEMPLATE.format(
        entities_block="\n".join(entity_lines),
        orphaned_mentions_block=orphaned_mentions_block,
    )
    audit_result = await _call_openrouter_json(
        api_key=api_key,
        model=model,
        prompt=prompt,
        timeout_seconds=timeout_seconds,
    )
    if not audit_result.get("ok"):
        stats["errors"] += 1
        _append_error(
            errors_file,
            {
                "stage": "audit_resolve",
                "status_code": audit_result.get("status_code"),
                "error": audit_result.get("error"),
                "raw_output": audit_result.get("raw_output"),
            },
        )
        return stats

    parsed = _safe_json(audit_result.get("parsed"))
    actions = parsed.get("audit_actions")
    if not isinstance(actions, list):
        return stats

    orphaned_by_key: Dict[str, Dict[str, Any]] = {
        normalize_key(_normalize_text(o.get("mention"))): o for o in orphaned_mentions if normalize_key(_normalize_text(o.get("mention")))
    }
    allowed_fix_fields = {
        "type",
        "relationship_to_user",
        "canonical_name",
        "canonical_name_normalized",
        "status",
        "last_known_status",
    }

    for action in actions:
        if not isinstance(action, dict):
            continue
        kind = _normalize_text(action.get("action")).upper()
        if kind == "NO_ACTION":
            continue

        if kind == "MERGE":
            keep_id = _normalize_text(action.get("keep_entity_id"))
            absorb_id = _normalize_text(action.get("absorb_entity_id"))
            keep = await db.fetchone(
                """
                SELECT entity_id, canonical_name, type, relationship_to_user, aliases, mention_count, source_session_ids
                FROM entity_profiles
                WHERE user_id = $1 AND entity_id = $2
                """,
                user_id,
                keep_id,
            )
            absorb = await db.fetchone(
                """
                SELECT entity_id, canonical_name, type, relationship_to_user, aliases, mention_count, source_session_ids
                FROM entity_profiles
                WHERE user_id = $1 AND entity_id = $2
                """,
                user_id,
                absorb_id,
            )
            if not keep or not absorb or keep_id == absorb_id:
                _append_error(
                    errors_file,
                    {
                        "stage": "audit_merge_skipped",
                        "error": "invalid_merge_entities_nonfatal",
                        "action": action,
                    },
                )
                continue

            incompat_reason = _merge_is_incompatible(keep, absorb)
            if incompat_reason:
                _append_error(
                    errors_file,
                    {
                        "stage": "audit_merge_skipped",
                        "error": f"incompatible_merge_nonfatal:{incompat_reason}",
                        "action": action,
                    },
                )
                continue

            keep_source_ids = keep.get("source_session_ids") if isinstance(keep.get("source_session_ids"), list) else []
            absorb_source_ids = absorb.get("source_session_ids") if isinstance(absorb.get("source_session_ids"), list) else []
            merged_session_ids = sorted(
                {
                    _normalize_text(s)
                    for s in (keep_source_ids + absorb_source_ids)
                    if _normalize_text(s)
                }
            )
            keep_aliases = keep.get("aliases") if isinstance(keep.get("aliases"), list) else []
            absorb_aliases = absorb.get("aliases") if isinstance(absorb.get("aliases"), list) else []
            merged_aliases = sorted(
                {
                    normalize_name(a)
                    for a in (keep_aliases + absorb_aliases + [_normalize_text(absorb.get("canonical_name"))])
                    if _normalize_text(a)
                }
            )
            canonical_name = normalize_name(_normalize_text(action.get("canonical_name")) or _normalize_text(keep.get("canonical_name")))
            canonical_name_normalized = normalize_key(
                _normalize_text(action.get("canonical_name_normalized")) or canonical_name
            )
            merged_type = _normalize_text(action.get("type")).lower() or _normalize_text(keep.get("type")).lower() or "other"
            merged_relationship = _normalize_text(action.get("relationship_to_user")) or _normalize_text(keep.get("relationship_to_user")) or "other"

            profile_json, latest_session_date, raw_output, status_code, profile_error = await _build_profile_from_sessions(
                db=db,
                api_key=api_key,
                model=model,
                timeout_seconds=timeout_seconds,
                canonical_name=canonical_name,
                entity_type=merged_type,
                relationship_to_user=merged_relationship,
                session_ids=merged_session_ids,
                profile_text_existing=None,
            )
            if not profile_json:
                stats["errors"] += 1
                _append_error(
                    errors_file,
                    {
                        "stage": "audit_merge_profile_rebuild",
                        "keep_entity_id": keep_id,
                        "absorb_entity_id": absorb_id,
                        "status_code": status_code,
                        "error": profile_error,
                        "raw_output": raw_output,
                    },
                )
                continue

            merged_key_facts = profile_json.get("key_facts")
            merged_open_questions = profile_json.get("open_questions")
            if not isinstance(merged_key_facts, list):
                merged_key_facts = []
            if not isinstance(merged_open_questions, list):
                merged_open_questions = []

            keep_count = int(keep.get("mention_count") or 0)
            absorb_count = int(absorb.get("mention_count") or 0)
            merged_count = keep_count + absorb_count

            await db.execute(
                """
                UPDATE entity_profiles SET
                  canonical_name = $1,
                  canonical_name_normalized = $2,
                  type = $3,
                  relationship_to_user = $4,
                  aliases = $5::text[],
                  mention_count = $6,
                  source_session_ids = $7::text[],
                  profile_text = $8,
                  key_facts = $9::jsonb,
                  open_questions = $10::jsonb,
                  last_known_status = $11,
                  status = 'active',
                  last_updated_at = now(),
                  last_processed_session_date = $12
                WHERE entity_id = $13
                """,
                canonical_name,
                canonical_name_normalized,
                merged_type,
                merged_relationship,
                merged_aliases,
                merged_count,
                merged_session_ids,
                _normalize_text(profile_json.get("profile_text")) or None,
                json.dumps(merged_key_facts),
                json.dumps(merged_open_questions),
                _normalize_text(profile_json.get("last_known_status")) or None,
                latest_session_date,
                keep_id,
            )
            await db.execute("DELETE FROM entity_profiles WHERE entity_id = $1", absorb_id)
            stats["merge_actions"] += 1
            continue

        if kind == "FIX":
            entity_id = _normalize_text(action.get("entity_id"))
            field = _normalize_text(action.get("field"))
            entity = await db.fetchone(
                f"SELECT {field} AS field_value FROM entity_profiles WHERE user_id = $1 AND entity_id = $2"
                if field in allowed_fix_fields
                else "SELECT NULL::text AS field_value WHERE false",
                user_id,
                entity_id,
            ) if field in allowed_fix_fields else None
            if not entity or field not in allowed_fix_fields:
                stats["errors"] += 1
                _append_error(
                    errors_file,
                    {
                        "stage": "audit_fix",
                        "error": "invalid_fix_target_or_field",
                        "action": action,
                    },
                )
                continue
            new_value = action.get("new_value")
            if field in {"canonical_name", "canonical_name_normalized", "type", "relationship_to_user", "status", "last_known_status"}:
                new_value = _normalize_text(new_value)
            if field == "canonical_name":
                new_value = normalize_name(str(new_value))
            if field == "canonical_name_normalized":
                new_value = normalize_key(str(new_value))
            old_value = action.get("old_value")
            current_value = entity.get("field_value")
            if _normalize_text(current_value) == _normalize_text(new_value):
                continue
            if old_value is not None and _normalize_text(current_value) != _normalize_text(old_value):
                _append_error(
                    errors_file,
                    {
                        "stage": "audit_fix_skipped",
                        "error": "old_value_mismatch_nonfatal",
                        "action": action,
                        "current_value": current_value,
                    },
                )
                continue
            await db.execute(
                f"UPDATE entity_profiles SET {field} = $1, last_updated_at = now() WHERE entity_id = $2",
                new_value,
                entity_id,
            )
            stats["fix_actions"] += 1
            continue

        if kind == "CREATE":
            mention = _normalize_text(action.get("mention"))
            mention_key = normalize_key(mention)
            canonical_name = normalize_name(_normalize_text(action.get("canonical_name")) or mention)
            canonical_name_normalized = normalize_key(
                _normalize_text(action.get("canonical_name_normalized")) or canonical_name
            )
            existing = await db.fetchone(
                """
                SELECT entity_id
                FROM entity_profiles
                WHERE user_id = $1 AND canonical_name_normalized = $2
                """,
                user_id,
                canonical_name_normalized,
            )
            if existing:
                continue
            entity_type = _normalize_text(action.get("type")).lower() or "other"
            if entity_type == "place":
                _append_error(
                    errors_file,
                    {
                        "stage": "audit_create_skipped",
                        "error": "place_create_disabled_nonfatal",
                        "action": action,
                    },
                )
                continue
            status = _normalize_text(action.get("status")).lower() or "tentative"
            relationship_to_user = _normalize_text(action.get("relationship_to_user")) or None
            confidence = action.get("confidence")
            if not isinstance(confidence, (int, float)):
                confidence = 0.6
            aliases = action.get("aliases") if isinstance(action.get("aliases"), list) else []
            aliases = [normalize_name(a) for a in aliases if _normalize_text(a)]
            if mention:
                aliases.append(normalize_name(mention))
            aliases = sorted(set(aliases))

            session_ids = action.get("session_ids") if isinstance(action.get("session_ids"), list) else []
            cleaned_session_ids = [_normalize_text(s) for s in session_ids if _normalize_text(s)]
            if not cleaned_session_ids and mention_key in orphaned_by_key:
                cleaned_session_ids = orphaned_by_key[mention_key].get("session_ids") or []
            cleaned_session_ids = sorted(set(cleaned_session_ids))

            if not cleaned_session_ids:
                stats["errors"] += 1
                _append_error(
                    errors_file,
                    {
                        "stage": "audit_create",
                        "error": "no_source_sessions_for_create",
                        "action": action,
                    },
                )
                continue

            seen = await db.fetchone(
                """
                SELECT MIN(session_date) AS first_seen_at, MAX(session_date) AS last_seen_at
                FROM session_classifications
                WHERE user_id = $1
                  AND session_id = ANY($2::text[])
                """,
                user_id,
                cleaned_session_ids,
            )
            first_seen_at = (seen or {}).get("first_seen_at")
            last_seen_at = (seen or {}).get("last_seen_at")
            upserted = await db.fetchone(
                """
                INSERT INTO entity_profiles (
                  user_id,
                  canonical_name,
                  canonical_name_normalized,
                  type,
                  aliases,
                  status,
                  relationship_to_user,
                  confidence,
                  mention_count,
                  first_seen_at,
                  last_seen_at,
                  source_session_ids
                ) VALUES (
                  $1,$2,$3,$4,$5::text[],$6,$7,$8,$9,$10,$11,$12::text[]
                )
                ON CONFLICT (user_id, canonical_name_normalized)
                DO UPDATE SET
                  type = EXCLUDED.type,
                  status = EXCLUDED.status,
                  relationship_to_user = EXCLUDED.relationship_to_user,
                  confidence = GREATEST(entity_profiles.confidence, EXCLUDED.confidence),
                  mention_count = GREATEST(entity_profiles.mention_count, EXCLUDED.mention_count),
                  first_seen_at = LEAST(entity_profiles.first_seen_at, EXCLUDED.first_seen_at),
                  last_seen_at = GREATEST(entity_profiles.last_seen_at, EXCLUDED.last_seen_at),
                  aliases = (
                    SELECT array_agg(DISTINCT a)
                    FROM unnest(entity_profiles.aliases || EXCLUDED.aliases) a
                  ),
                  source_session_ids = (
                    SELECT array_agg(DISTINCT s)
                    FROM unnest(entity_profiles.source_session_ids || EXCLUDED.source_session_ids) s
                  ),
                  last_updated_at = now(),
                  last_processed_session_date = NULL
                RETURNING entity_id, source_session_ids
                """,
                user_id,
                canonical_name,
                canonical_name_normalized,
                entity_type,
                aliases,
                status,
                relationship_to_user,
                float(confidence),
                int(orphaned_by_key.get(mention_key, {}).get("times") or len(cleaned_session_ids)),
                first_seen_at,
                last_seen_at,
                cleaned_session_ids,
            )
            created_entity_id = _normalize_text((upserted or {}).get("entity_id"))
            entity_session_ids = (upserted or {}).get("source_session_ids") if isinstance((upserted or {}).get("source_session_ids"), list) else cleaned_session_ids
            if created_entity_id and status == "active":
                profile_json, latest_session_date, raw_output, status_code, profile_error = await _build_profile_from_sessions(
                    db=db,
                    api_key=api_key,
                    model=model,
                    timeout_seconds=timeout_seconds,
                    canonical_name=canonical_name,
                    entity_type=entity_type,
                    relationship_to_user=relationship_to_user or "other",
                    session_ids=[_normalize_text(s) for s in entity_session_ids if _normalize_text(s)],
                    profile_text_existing=None,
                )
                if not profile_json:
                    stats["errors"] += 1
                    _append_error(
                        errors_file,
                        {
                            "stage": "audit_create_profile_build",
                            "entity_id": created_entity_id,
                            "status_code": status_code,
                            "error": profile_error,
                            "raw_output": raw_output,
                            "action": action,
                        },
                    )
                    continue
                key_facts = profile_json.get("key_facts")
                open_questions = profile_json.get("open_questions")
                if not isinstance(key_facts, list):
                    key_facts = []
                if not isinstance(open_questions, list):
                    open_questions = []
                await db.execute(
                    """
                    UPDATE entity_profiles SET
                      profile_text = $1,
                      key_facts = $2::jsonb,
                      open_questions = $3::jsonb,
                      last_known_status = $4,
                      status = 'active',
                      last_updated_at = now(),
                      last_processed_session_date = $5
                    WHERE entity_id = $6
                    """,
                    _normalize_text(profile_json.get("profile_text")) or None,
                    json.dumps(key_facts),
                    json.dumps(open_questions),
                    _normalize_text(profile_json.get("last_known_status")) or None,
                    latest_session_date,
                    created_entity_id,
                )
            stats["create_actions"] += 1
            continue

    return stats


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run entity registry pipeline")
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--model", default="google/gemma-4-26b-a4b-it")
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    args = parser.parse_args()

    settings = get_settings()
    api_key = _normalize_text(settings.openrouter_api_key or settings.openai_api_key)
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY or OPENAI_API_KEY is required")

    now_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    errors_file = Path(f"tmp/entity_pipeline_errors_{now_stamp}.jsonl")
    errors_file.parent.mkdir(parents=True, exist_ok=True)

    db = Database()
    sessions_scanned = 0
    candidates_found = 0
    match_count = 0
    new_active_count = 0
    new_tentative_count = 0
    skip_count = 0
    profile_builds = 0
    profile_updates = 0
    error_count = 0
    audit_merge_actions = 0
    audit_fix_actions = 0
    audit_create_actions = 0
    audit_orphaned_checked = 0
    resolutions: List[Dict[str, Any]] = []

    try:
        rows = await db.fetch(
            """
            SELECT
              sc.session_id,
              sc.session_date,
              sc.entity_mentions,
              sc.raw_triage_output->'memory_deltas' as memory_deltas,
              sc.raw_triage_output->'thread_signals' as thread_signals
            FROM session_classifications sc
            WHERE sc.user_id = $1
              AND sc.is_memory_worthy = true
              AND sc.entity_mentions != '{}'
              AND sc.session_date > COALESCE(
                (
                  SELECT last_processed
                  FROM pipeline_checkpoints
                  WHERE user_id = $1
                    AND pipeline_name = 'entity_pipeline'
                ),
                '1970-01-01'::timestamptz
              )
            ORDER BY sc.session_date ASC
            """,
            args.user_id,
        )
        sessions_scanned = len(rows)

        candidates_map: Dict[str, EntityCandidate] = {}
        for row in rows:
            session_id = _normalize_text(row.get("session_id"))
            deltas = _to_text_list(row.get("memory_deltas"))
            mentions = row.get("entity_mentions") or []
            if not isinstance(mentions, list):
                mentions = []
            for raw_entity in mentions:
                raw_text = _normalize_text(raw_entity)
                if not raw_text:
                    continue
                name = normalize_name(raw_text)
                key = normalize_key(raw_text)
                if key not in candidates_map:
                    candidates_map[key] = EntityCandidate(
                        name=name,
                        key=key,
                        mention_count=0,
                        session_ids=[],
                        context_snippets=[],
                        aliases=[],
                    )
                c = candidates_map[key]
                c.mention_count += 1
                if session_id and session_id not in c.session_ids:
                    c.session_ids.append(session_id)
                if raw_text not in c.aliases:
                    c.aliases.append(raw_text)
                relevant_deltas = [d for d in deltas if (name.lower() in d.lower() or key in d.lower())][:2]
                c.context_snippets.extend(relevant_deltas)

        candidates = list(candidates_map.values())
        candidates_found = len(candidates)
        if not candidates:
            print("No new entity mentions found")

        existing_entities = await db.fetch(
            """
            SELECT
              entity_id,
              canonical_name,
              canonical_name_normalized,
              type,
              aliases,
              status,
              relationship_to_user,
              confidence,
              mention_count,
              profile_text,
              source_session_ids,
              last_processed_session_date
            FROM entity_profiles
            WHERE user_id = $1
              AND status != 'archived'
            ORDER BY mention_count DESC
            """,
            args.user_id,
        )
        existing_by_id = {_normalize_text(e.get("entity_id")): e for e in existing_entities if _normalize_text(e.get("entity_id"))}

        existing_lines: List[str] = []
        for e in existing_entities:
            existing_lines.append(
                " | ".join(
                    [
                        _normalize_text(e.get("entity_id")),
                        _normalize_text(e.get("canonical_name")),
                        _normalize_text(e.get("type")),
                        _normalize_text(e.get("relationship_to_user")),
                        json.dumps(e.get("aliases") or []),
                        str(e.get("confidence")),
                    ]
                )
            )
        if not existing_lines:
            existing_lines = ["(none)"]

        mention_lines: List[str] = []
        for c in candidates:
            mention_lines.append(
                json.dumps(
                    {
                        "name": c.name,
                        "mention_count": c.mention_count,
                        "context_snippets": c.context_snippets[:6],
                    },
                    ensure_ascii=False,
                )
            )

        resolve_prompt = RESOLVE_PROMPT_TEMPLATE.format(
            existing_entities="\n".join(existing_lines),
            new_mentions="\n".join(mention_lines),
        )

        resolve_result = await _call_openrouter_json(
            api_key=api_key,
            model=args.model,
            prompt=resolve_prompt,
            timeout_seconds=args.timeout_seconds,
        )
        if not resolve_result.get("ok"):
            _append_error(
                errors_file,
                {
                    "stage": "resolve",
                    "status_code": resolve_result.get("status_code"),
                    "error": resolve_result.get("error"),
                    "raw_output": resolve_result.get("raw_output"),
                },
            )
            raise RuntimeError(f"Entity resolve call failed: {resolve_result.get('error')}")

        parsed = _safe_json(resolve_result.get("parsed"))
        resolutions = parsed.get("resolutions") if isinstance(parsed.get("resolutions"), list) else []

        # Apply MATCH/NEW/SKIP
        for item in resolutions:
            if not isinstance(item, dict):
                continue
            mention = normalize_name(_normalize_text(item.get("mention")))
            key = normalize_key(mention)
            candidate = candidates_map.get(key)
            if not candidate:
                continue
            decision = _normalize_text(item.get("decision")).upper()

            if decision == "MATCH":
                matched_id = _normalize_text(item.get("matched_entity_id"))
                target = existing_by_id.get(matched_id)
                if not target:
                    skip_count += 1
                    continue
                new_aliases = list({normalize_name(a) for a in (candidate.aliases + [candidate.name]) if _normalize_text(a)})
                latest_row = await db.fetchone(
                    """
                    SELECT MAX(session_date) AS latest_session_date
                    FROM session_classifications
                    WHERE user_id = $1
                      AND session_id = ANY($2::text[])
                    """,
                    args.user_id,
                    candidate.session_ids,
                )
                latest_session_date = (latest_row or {}).get("latest_session_date")
                await db.execute(
                    """
                    UPDATE entity_profiles SET
                      mention_count = mention_count + $1,
                      aliases = (
                        SELECT array_agg(DISTINCT a)
                        FROM unnest(aliases || $2::text[]) a
                      ),
                      last_seen_at = COALESCE($3, last_seen_at),
                      source_session_ids = (
                        SELECT array_agg(DISTINCT s)
                        FROM unnest(source_session_ids || $4::text[]) s
                      ),
                      confidence = LEAST(confidence + 0.05, 1.0)
                    WHERE entity_id = $5
                    """,
                    candidate.mention_count,
                    new_aliases,
                    latest_session_date,
                    candidate.session_ids,
                    matched_id,
                )
                match_count += 1
                continue

            if decision == "NEW":
                canonical_name = normalize_name(_normalize_text(item.get("canonical_name")) or candidate.name)
                canonical_name_normalized = normalize_key(
                    _normalize_text(item.get("canonical_name_normalized")) or canonical_name
                )
                entity_type = _normalize_text(item.get("type")).lower() or "other"
                status = _normalize_text(item.get("status")).lower() or "tentative"
                relationship_to_user = _normalize_text(item.get("relationship_to_user")) or None
                confidence = item.get("confidence")
                if not isinstance(confidence, (int, float)):
                    confidence = 0.4
                aliases = _to_text_list(item.get("aliases")) or [candidate.name]
                aliases = list({normalize_name(a) for a in aliases if _normalize_text(a)})
                first_seen = await db.fetchone(
                    """
                    SELECT MIN(session_date) AS first_seen_at, MAX(session_date) AS last_seen_at
                    FROM session_classifications
                    WHERE user_id = $1
                      AND session_id = ANY($2::text[])
                    """,
                    args.user_id,
                    candidate.session_ids,
                )
                first_seen_at = (first_seen or {}).get("first_seen_at")
                last_seen_at = (first_seen or {}).get("last_seen_at")
                await db.execute(
                    """
                    INSERT INTO entity_profiles (
                      user_id,
                      canonical_name,
                      canonical_name_normalized,
                      type,
                      aliases,
                      status,
                      relationship_to_user,
                      confidence,
                      mention_count,
                      first_seen_at,
                      last_seen_at,
                      source_session_ids
                    ) VALUES (
                      $1,$2,$3,$4,$5::text[],$6,$7,$8,$9,$10,$11,$12::text[]
                    )
                    ON CONFLICT (user_id, canonical_name_normalized)
                    DO UPDATE SET
                      mention_count = entity_profiles.mention_count + 1,
                      last_seen_at = EXCLUDED.last_seen_at,
                      confidence = LEAST(entity_profiles.confidence + 0.05, 1.0),
                      aliases = (
                        SELECT array_agg(DISTINCT a)
                        FROM unnest(entity_profiles.aliases || EXCLUDED.aliases) a
                      ),
                      source_session_ids = (
                        SELECT array_agg(DISTINCT s)
                        FROM unnest(entity_profiles.source_session_ids || EXCLUDED.source_session_ids) s
                      )
                    """,
                    args.user_id,
                    canonical_name,
                    canonical_name_normalized,
                    entity_type,
                    aliases,
                    status,
                    relationship_to_user,
                    float(confidence),
                    int(candidate.mention_count),
                    first_seen_at,
                    last_seen_at,
                    candidate.session_ids,
                )
                if status == "active":
                    new_active_count += 1
                else:
                    new_tentative_count += 1
                continue

            # SKIP default
            skip_count += 1

        # Refresh entities after upserts for profile building
        entities_for_profile = await db.fetch(
            """
            SELECT
              entity_id,
              canonical_name,
              type,
              relationship_to_user,
              status,
              profile_text,
              source_session_ids,
              last_processed_session_date
            FROM entity_profiles
            WHERE user_id = $1
              AND status = 'active'
            ORDER BY mention_count DESC, canonical_name ASC
            """,
            args.user_id,
        )

        # Build a map of session dates for all known source session ids
        all_source_ids: List[str] = []
        for e in entities_for_profile:
            ids = e.get("source_session_ids") or []
            if isinstance(ids, list):
                all_source_ids.extend([_normalize_text(x) for x in ids if _normalize_text(x)])
        all_source_ids = sorted(set(all_source_ids))
        session_date_map: Dict[str, datetime] = {}
        if all_source_ids:
            session_rows = await db.fetch(
                """
                SELECT session_id, created_at
                FROM session_transcript
                WHERE session_id = ANY($1::text[])
                """,
                all_source_ids,
            )
            for sr in session_rows:
                sid = _normalize_text(sr.get("session_id"))
                created = sr.get("created_at")
                if sid and isinstance(created, datetime):
                    session_date_map[sid] = created

        for entity in entities_for_profile:
            entity_id = _normalize_text(entity.get("entity_id"))
            source_ids = [sid for sid in (entity.get("source_session_ids") or []) if _normalize_text(sid)]
            source_ids = [_normalize_text(s) for s in source_ids]
            source_ids = sorted(set(source_ids))
            if not source_ids:
                continue

            profile_text_existing = _normalize_text(entity.get("profile_text"))
            last_processed = entity.get("last_processed_session_date")
            if profile_text_existing:
                build_ids = [
                    sid
                    for sid in source_ids
                    if isinstance(session_date_map.get(sid), datetime)
                    and (
                        not isinstance(last_processed, datetime)
                        or session_date_map[sid] > last_processed
                    )
                ]
            else:
                build_ids = source_ids
            if not build_ids:
                continue

            transcript_rows = await db.fetch(
                """
                SELECT session_id, created_at, messages
                FROM session_transcript
                WHERE session_id = ANY($1::text[])
                ORDER BY created_at ASC
                """,
                build_ids,
            )
            user_turn_blocks: List[str] = []
            latest_session_date: Optional[datetime] = None
            for tr in transcript_rows:
                messages = tr.get("messages")
                if not isinstance(messages, list):
                    messages = []
                block = _format_user_turns(messages, tr.get("created_at"))
                if block:
                    user_turn_blocks.append(block)
                created = tr.get("created_at")
                if isinstance(created, datetime) and (latest_session_date is None or created > latest_session_date):
                    latest_session_date = created
            transcript_text = "\n\n".join(user_turn_blocks).strip()
            if not transcript_text:
                continue

            if not profile_text_existing:
                profile_mode_block = "This is a first build. Build from scratch using only\nwhat the user has stated in the transcripts below."
            else:
                profile_mode_block = (
                    "EXISTING PROFILE:\n"
                    f"{profile_text_existing}\n\n"
                    "This is an update. The transcripts below are NEW sessions\n"
                    "only — ones that occurred after the existing profile was\n"
                    "last built. Preserve all accurate existing facts. Add new\n"
                    "information. Update anything that has changed. Note\n"
                    "contradictions explicitly rather than silently overwriting."
                )
            profile_prompt = PROFILE_PROMPT_TEMPLATE.format(
                canonical_name=_normalize_text(entity.get("canonical_name")),
                entity_type=_normalize_text(entity.get("type")),
                relationship_to_user=_normalize_text(entity.get("relationship_to_user")) or "other",
                profile_mode_block=profile_mode_block,
                transcript_text=transcript_text,
            )
            profile_result = await _call_openrouter_json(
                api_key=api_key,
                model=args.model,
                prompt=profile_prompt,
                timeout_seconds=args.timeout_seconds,
            )
            if not profile_result.get("ok"):
                error_count += 1
                _append_error(
                    errors_file,
                    {
                        "stage": "profile_build",
                        "entity_id": entity_id,
                        "entity_name": _normalize_text(entity.get("canonical_name")),
                        "status_code": profile_result.get("status_code"),
                        "error": profile_result.get("error"),
                        "raw_output": profile_result.get("raw_output"),
                    },
                )
                continue

            profile_json = _safe_json(profile_result.get("parsed"))
            profile_text = _normalize_text(profile_json.get("profile_text"))
            key_facts = profile_json.get("key_facts")
            open_questions = profile_json.get("open_questions")
            last_known_status = _normalize_text(profile_json.get("last_known_status")) or None
            if not isinstance(key_facts, list):
                key_facts = []
            if not isinstance(open_questions, list):
                open_questions = []

            try:
                await db.execute(
                    """
                    UPDATE entity_profiles SET
                      profile_text = $1,
                      key_facts = $2::jsonb,
                      open_questions = $3::jsonb,
                      last_known_status = $4,
                      status = 'active',
                      last_updated_at = now(),
                      last_processed_session_date = $5
                    WHERE entity_id = $6
                    """,
                    profile_text or None,
                    json.dumps(key_facts),
                    json.dumps(open_questions),
                    last_known_status,
                    latest_session_date,
                    entity_id,
                )
            except Exception:
                raise

            if profile_text_existing:
                profile_updates += 1
            else:
                profile_builds += 1
            await asyncio.sleep(1.0)

        print("Running post-pipeline audit...")
        audit_stats = await run_entity_audit(
            user_id=args.user_id,
            db=db,
            api_key=api_key,
            model=args.model,
            timeout_seconds=args.timeout_seconds,
            errors_file=errors_file,
        )
        audit_merge_actions = int(audit_stats.get("merge_actions", 0))
        audit_fix_actions = int(audit_stats.get("fix_actions", 0))
        audit_create_actions = int(audit_stats.get("create_actions", 0))
        audit_orphaned_checked = int(audit_stats.get("orphaned_checked", 0))
        error_count += int(audit_stats.get("errors", 0))
        print("Audit complete.")

        # checkpoint
        await db.execute(
            """
            INSERT INTO pipeline_checkpoints (user_id, pipeline_name, last_processed)
            VALUES ($1, 'entity_pipeline', now())
            ON CONFLICT (user_id, pipeline_name)
            DO UPDATE SET last_processed = now()
            """,
            args.user_id,
        )

        print("Entity Pipeline Complete")
        print("========================")
        print(f"Sessions scanned: {sessions_scanned}")
        print(f"Candidates found: {candidates_found}")
        print(f"  MATCH: {match_count}")
        print(f"  NEW (active): {new_active_count}")
        print(f"  NEW (tentative): {new_tentative_count}")
        print(f"  SKIP: {skip_count}")
        print(f"Profile builds completed: {profile_builds}")
        print(f"Profile updates completed: {profile_updates}")
        print(f"Audit merges applied: {audit_merge_actions}")
        print(f"Audit fixes applied: {audit_fix_actions}")
        print(f"Audit creates applied: {audit_create_actions}")
        print(f"Orphaned mentions checked: {audit_orphaned_checked}")
        print(f"Errors: {error_count}")
        print("")
        print("Running final report...")

        report_rows = await db.fetch(
            """
            SELECT
              canonical_name,
              type,
              status,
              relationship_to_user,
              mention_count,
              ROUND(confidence::numeric, 2) as confidence,
              last_known_status,
              LEFT(profile_text, 300) as profile_preview,
              open_questions
            FROM entity_profiles
            WHERE user_id = $1
            ORDER BY
              CASE status
                WHEN 'active' THEN 1
                WHEN 'tentative' THEN 2
                ELSE 3
              END,
              mention_count DESC
            """,
            args.user_id,
        )
        print(json.dumps(report_rows, indent=2, ensure_ascii=False, default=str))
        print(f"errors_file={errors_file}")

    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
