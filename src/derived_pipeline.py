"""
Async hardening helpers for the Gemma/Postgres derived synthesis pipeline.

This module deliberately keeps the six-pass pipeline as the serving synthesis
layer. Canonical v2 remains separate governance/audit infrastructure.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import logging
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .canonicalization import normalize_text, stable_short_hash
from .config import Settings, get_settings
from .db import Database
from .openrouter_client import get_llm_client
from .derived_passes.pass1_triage import run_rich_pass1_llm
from .derived_passes.pass15_entities import build_entity_profile, resolve_entity_mentions
from .derived_passes.pass3_threads import extract_thread_actions, VALID_CATEGORIES, VALID_PRIORITIES
from .derived_passes.pass4_identity import normalize_identity_output, synthesize_identity_profile
from .derived_passes.pass5_living_context import normalize_living_context_output, synthesize_living_context

logger = logging.getLogger(__name__)

PASS1_TRIAGE = "pass1_triage"
PASS1_5_ENTITIES = "pass1_5_entities"
PASS3_THREADS = "pass3_threads"
PASS4_IDENTITY = "pass4_identity"
PASS5_LIVING_CONTEXT = "pass5_living_context"

QUARANTINE_PARSE_FAILURE = "parse_failure"
QUARANTINE_MISSING_EVIDENCE_REFS = "missing_evidence_refs"
QUARANTINE_INVALID_LIFECYCLE_TRANSITION = "invalid_lifecycle_transition"

HIGH_SIGNAL_SURFACES = {
    "memory_delta",
    "identity_signal",
    "thread_signal",
    "thread_update",
    "identity_trait",
    "living_context_statement",
}

EXCLUSIVE_SLOTS = {
    "relationship.status",
    "user.location",
    "preference.durable_correction",
}


@dataclass(frozen=True)
class DerivedPassResult:
    run_id: int
    pass_name: str
    output_hash: str
    run_entity_pass: bool = False
    run_threads_pass: bool = False
    should_run_identity: bool = False
    should_run_living_context: bool = False


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass
        return [value] if value.strip() else []
    return [value]


def _text_list(value: Any, *, limit: int = 12) -> List[str]:
    out: List[str] = []
    for item in _as_list(value):
        if isinstance(item, dict):
            text = item.get("text") or item.get("name") or item.get("statement") or item.get("delta")
        else:
            text = item
        normalized = normalize_text(text, casefold=False)
        if normalized and normalized not in out:
            out.append(normalized)
        if len(out) >= limit:
            break
    return out


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return normalize_text(value) in {"true", "yes", "1", "y"}


def _messages_text(messages: Sequence[Dict[str, Any]], *, user_only: bool = True) -> str:
    lines: List[str] = []
    for msg in messages:
        role = normalize_text(msg.get("role"))
        if user_only and role != "user":
            continue
        text = normalize_text(msg.get("text") or msg.get("content"), casefold=False)
        if text:
            lines.append(f"{role or 'unknown'}: {text}")
    return "\n".join(lines)


def _source_turn_refs(
    *,
    session_id: str,
    messages: Sequence[Dict[str, Any]],
    text_hint: Optional[str] = None,
    max_refs: int = 8,
) -> List[Dict[str, Any]]:
    hint = normalize_text(text_hint or "")
    refs: List[Dict[str, Any]] = []
    for idx, msg in enumerate(messages):
        role = normalize_text(msg.get("role"))
        if role != "user":
            continue
        text = normalize_text(msg.get("text") or msg.get("content"), casefold=False)
        if not text:
            continue
        if hint:
            hint_tokens = {t for t in re.findall(r"[a-z0-9']{4,}", hint)[:8]}
            text_norm = normalize_text(text)
            if hint_tokens and not any(token in text_norm for token in hint_tokens):
                continue
        refs.append(
            {
                "session_id": session_id,
                "turn_index": idx,
                "role": role,
                "timestamp": msg.get("timestamp"),
                "text": text[:500],
            }
        )
        if len(refs) >= max_refs:
            break
    if not refs and not hint:
        for idx, msg in enumerate(messages):
            role = normalize_text(msg.get("role"))
            text = normalize_text(msg.get("text") or msg.get("content"), casefold=False)
            if role == "user" and text:
                refs.append(
                    {
                        "session_id": session_id,
                        "turn_index": idx,
                        "role": role,
                        "timestamp": msg.get("timestamp"),
                        "text": text[:500],
                    }
                )
                if len(refs) >= max_refs:
                    break
    return refs


def _semantic_category(statement: str, surface: str) -> str:
    text = normalize_text(f"{surface} {statement}")
    if any(word in text for word in ("daughter", "girlfriend", "partner", "mother", "father", "family", "relationship")):
        return "relationship"
    if any(word in text for word in ("health", "hospital", "pain", "kidney", "sleep", "hydration", "walk", "walking")):
        return "health"
    if any(word in text for word in ("goal", "want", "trying to", "building", "project", "persistent")):
        return "persistent_goal"
    if surface.startswith("identity"):
        return "core_identity"
    if "preference" in text or "like" in text or "hate" in text:
        return "preference"
    return "session_observation"


def _memory_layer_and_floor(category: str) -> tuple[str, float]:
    if category in {"relationship", "health", "persistent_goal", "core_identity"}:
        return "LML", 0.65
    if category == "preference":
        return "LML", 0.45
    return "SML", 0.0


def _input_hash(
    *,
    tenant_id: str,
    user_id: str,
    session_id: Optional[str],
    pass_name: str,
    messages: Optional[Sequence[Dict[str, Any]]] = None,
    extra: Optional[Dict[str, Any]] = None,
    settings: Optional[Settings] = None,
) -> str:
    s = settings or get_settings()
    return stable_short_hash(
        {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "session_id": session_id,
            "pass_name": pass_name,
            "messages": messages or [],
            "extra": extra or {},
            "model_version": s.derived_pipeline_model_version,
            "prompt_version": s.derived_pipeline_prompt_version,
            "policy_version": s.derived_pipeline_policy_version,
        },
        length=24,
    )


async def _start_run(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    session_id: Optional[str],
    pass_name: str,
    input_hash: str,
    input_watermark: Optional[str] = None,
    settings: Optional[Settings] = None,
) -> int:
    s = settings or get_settings()
    await db.execute(
        """
        INSERT INTO pipeline_runs (
            tenant_id, user_id, session_id, pass_name,
            model_version, prompt_version, policy_version,
            input_watermark, input_hash, status, attempt_count, started_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'running',1,NOW())
        ON CONFLICT (
            tenant_id, user_id, pass_name, input_hash, model_version, prompt_version, COALESCE(policy_version, '')
        )
        DO UPDATE SET
            status = CASE
                WHEN pipeline_runs.status = 'succeeded' THEN pipeline_runs.status
                ELSE 'running'
            END,
            attempt_count = CASE
                WHEN pipeline_runs.status = 'succeeded' THEN pipeline_runs.attempt_count
                ELSE pipeline_runs.attempt_count + 1
            END,
            started_at = CASE
                WHEN pipeline_runs.status = 'succeeded' THEN pipeline_runs.started_at
                ELSE NOW()
            END,
            error_code = NULL,
            error_message = NULL
        """,
        tenant_id,
        user_id,
        session_id,
        pass_name,
        s.derived_pipeline_model_version,
        s.derived_pipeline_prompt_version,
        s.derived_pipeline_policy_version,
        input_watermark,
        input_hash,
    )
    row = await db.fetchone(
        """
        SELECT run_id
        FROM pipeline_runs
        WHERE tenant_id=$1
          AND user_id=$2
          AND pass_name=$3
          AND input_hash=$4
          AND model_version=$5
          AND prompt_version=$6
          AND COALESCE(policy_version, '')=COALESCE($7, '')
        ORDER BY run_id DESC
        LIMIT 1
        """,
        tenant_id,
        user_id,
        pass_name,
        input_hash,
        s.derived_pipeline_model_version,
        s.derived_pipeline_prompt_version,
        s.derived_pipeline_policy_version,
    )
    if not row:
        raise RuntimeError("failed to create pipeline run")
    return int(row["run_id"])


async def _complete_run(db: Database, run_id: int, output_hash: str) -> None:
    await db.execute(
        """
        UPDATE pipeline_runs
        SET status='succeeded', output_hash=$2, completed_at=NOW()
        WHERE run_id=$1
        """,
        run_id,
        output_hash,
    )


async def _fail_run(db: Database, run_id: int, code: str, message: str) -> None:
    await db.execute(
        """
        UPDATE pipeline_runs
        SET status='failed', error_code=$2, error_message=$3, completed_at=NOW()
        WHERE run_id=$1
        """,
        run_id,
        code[:120],
        message[:2000],
    )


async def _succeeded_run(db: Database, run_id: int) -> Optional[Dict[str, Any]]:
    row = await db.fetchone(
        """
        SELECT status, output_hash
        FROM pipeline_runs
        WHERE run_id=$1
        """,
        run_id,
    )
    if row and row.get("status") == "succeeded" and row.get("output_hash"):
        return row
    return None


async def _quarantine(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    pass_name: str,
    session_id: Optional[str],
    reason_code: str,
    payload: Dict[str, Any],
    run_id: Optional[int] = None,
) -> None:
    await db.execute(
        """
        INSERT INTO derived_quarantine (
            tenant_id, user_id, pass_name, session_id, reason_code, payload, run_id
        )
        VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7)
        """,
        tenant_id,
        user_id,
        pass_name,
        session_id,
        reason_code,
        payload,
        run_id,
    )


async def _write_assertion(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    pass_name: str,
    surface: str,
    statement_text: str,
    run_id: int,
    source_session_ids: Sequence[str],
    source_turn_refs: Sequence[Dict[str, Any]],
    slot_key: Optional[str] = None,
    salience: Optional[float] = None,
    importance: Optional[float] = None,
    confidence_extraction: Optional[float] = None,
    confidence_validity: Optional[float] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[int]:
    statement = normalize_text(statement_text, casefold=False)
    if not statement:
        return None
    if surface in HIGH_SIGNAL_SURFACES and (not source_session_ids or not source_turn_refs):
        await _quarantine(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            pass_name=pass_name,
            session_id=source_session_ids[0] if source_session_ids else None,
            reason_code=QUARANTINE_MISSING_EVIDENCE_REFS,
            payload={"surface": surface, "statement_text": statement, "slot_key": slot_key},
            run_id=run_id,
        )
        return None
    category = _semantic_category(statement, surface)
    memory_layer, retention_floor = _memory_layer_and_floor(category)
    row = await db.fetchone(
        """
        INSERT INTO derived_assertions (
            tenant_id, user_id, pass_name, surface, slot_key, statement_text,
            lifecycle_state, salience, importance, confidence_extraction,
            confidence_validity, source_session_ids, source_turn_refs,
            run_id, metadata, memory_layer, semantic_category, retention_floor
        )
        VALUES (
            $1,$2,$3,$4,$5,$6,'active',$7,$8,$9,$10,$11::text[],$12::jsonb,
            $13,$14::jsonb,$15,$16,$17
        )
        RETURNING assertion_id
        """,
        tenant_id,
        user_id,
        pass_name,
        surface,
        slot_key,
        statement,
        salience,
        importance,
        confidence_extraction,
        confidence_validity,
        list(source_session_ids),
        list(source_turn_refs),
        run_id,
        metadata or {},
        memory_layer,
        category,
        retention_floor,
    )
    return int(row["assertion_id"]) if row else None


async def _update_checkpoint(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    pipeline_name: str,
    run_id: int,
    input_watermark: Optional[str],
    output_hash: str,
    increment_identity: int = 0,
    increment_context: int = 0,
    reset_identity: bool = False,
    reset_context: bool = False,
) -> None:
    await db.execute(
        """
        INSERT INTO pipeline_checkpoints (
            user_id, pipeline_name, tenant_id, last_processed,
            last_input_watermark, last_success_run_id, last_output_hash,
            identity_signal_count_since_last, context_delta_count_since_last, updated_at
        )
        VALUES (
            $2,$3,$1,NOW(),$5,$4,$6,
            CASE WHEN $9 THEN 0 ELSE $7 END,
            CASE WHEN $10 THEN 0 ELSE $8 END,
            NOW()
        )
        ON CONFLICT (user_id, pipeline_name)
        DO UPDATE SET
            tenant_id=EXCLUDED.tenant_id,
            last_processed=NOW(),
            last_input_watermark=EXCLUDED.last_input_watermark,
            last_success_run_id=EXCLUDED.last_success_run_id,
            last_output_hash=EXCLUDED.last_output_hash,
            identity_signal_count_since_last=CASE
                WHEN $9 THEN 0
                ELSE COALESCE(pipeline_checkpoints.identity_signal_count_since_last, 0) + $7
            END,
            context_delta_count_since_last=CASE
                WHEN $10 THEN 0
                ELSE COALESCE(pipeline_checkpoints.context_delta_count_since_last, 0) + $8
            END,
            updated_at=NOW()
        """,
        tenant_id,
        user_id,
        pipeline_name,
        run_id,
        input_watermark,
        output_hash,
        int(increment_identity),
        int(increment_context),
        bool(reset_identity),
        bool(reset_context),
    )


async def _checkpoint(db: Database, user_id: str, pipeline_name: str) -> Optional[Dict[str, Any]]:
    return await db.fetchone(
        """
        SELECT *
        FROM pipeline_checkpoints
        WHERE user_id=$1 AND pipeline_name=$2
        """,
        user_id,
        pipeline_name,
    )


def _extract_json_object(raw: str) -> Optional[Dict[str, Any]]:
    text = (raw or "").strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        pass
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            parsed = json.loads(text[start : end + 1])
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None
    return None


def _heuristic_pass1(messages: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    user_text = _messages_text(messages, user_only=True)
    text_norm = normalize_text(user_text)
    sentences = [
        normalize_text(s, casefold=False)
        for s in re.split(r"(?<=[.!?])\s+|\n+", user_text)
        if normalize_text(s)
    ]
    signal_terms = (
        "remember",
        "don't forget",
        "do not forget",
        "need to",
        "i want",
        "i am",
        "i'm",
        "my ",
        "ashley",
        "jasmine",
        "health",
        "pain",
        "hospital",
        "relationship",
        "sophie",
        "synapse",
    )
    memory_deltas = [s for s in sentences if any(term in normalize_text(s) for term in signal_terms)][:4]
    entity_mentions: List[str] = []
    for match in re.findall(r"\b[A-Z][a-zA-Z]{2,}(?:\s+[A-Z][a-zA-Z]{2,}){0,2}\b", user_text):
        name = normalize_text(match, casefold=False)
        if name in {"I", "The", "This", "That", "Pass", "Model", "JSON"}:
            continue
        if name not in entity_mentions:
            entity_mentions.append(name)
    identity_signals = [
        s for s in sentences
        if any(term in normalize_text(s) for term in ("i am", "i'm", "i want", "i believe", "i feel", "my fear", "my goal"))
    ][:4]
    thread_signals = [
        s for s in sentences
        if any(term in normalize_text(s) for term in ("need to", "follow up", "remind", "worried", "waiting", "unresolved", "goal"))
    ][:4]
    context_relevant = bool(memory_deltas or thread_signals or "tension" in text_norm or "stressed" in text_norm)
    emotional_weight = "medium" if any(t in text_norm for t in ("stressed", "upset", "worried", "scared", "angry")) else "low"
    return {
        "is_memory_worthy": bool(memory_deltas or entity_mentions or identity_signals or thread_signals),
        "session_kind": "mixed" if memory_deltas else "transient",
        "memory_deltas": memory_deltas,
        "entity_mentions": entity_mentions[:12],
        "thread_signals": thread_signals,
        "identity_signals": identity_signals,
        "emotional_weight": emotional_weight,
        "emotional_note": None,
        "tension_signal": None,
        "context_relevant": context_relevant,
        "run_entity_pass": bool(entity_mentions),
        "run_threads_pass": bool(thread_signals),
        "identity_relevant": bool(identity_signals),
    }


async def _call_pass1_llm(messages: Sequence[Dict[str, Any]], settings: Settings) -> Optional[Dict[str, Any]]:
    if not bool(settings.derived_pipeline_llm_enabled):
        return None
    return await run_rich_pass1_llm(
        messages=list(messages),
        model=settings.derived_pipeline_model_version,
    )


def _normalize_pass1_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    memory_deltas = _text_list(payload.get("memory_deltas"), limit=6)
    entity_mentions = _text_list(payload.get("entity_mentions"), limit=20)
    thread_signals = _text_list(payload.get("thread_signals"), limit=8)
    identity_signals = _text_list(payload.get("identity_signals"), limit=8)
    context_relevant = _to_bool(payload.get("context_relevant")) or bool(memory_deltas or thread_signals or payload.get("tension_signal"))
    return {
        "is_memory_worthy": _to_bool(payload.get("is_memory_worthy")) or bool(memory_deltas or entity_mentions or thread_signals or identity_signals),
        "session_kind": normalize_text(payload.get("session_kind")) or "transient",
        "memory_deltas": memory_deltas,
        "entity_mentions": entity_mentions,
        "thread_signals": thread_signals,
        "identity_signals": identity_signals,
        "emotional_weight": normalize_text(payload.get("emotional_weight")) or "none",
        "emotional_note": normalize_text(payload.get("emotional_note"), casefold=False) or None,
        "tension_signal": normalize_text(payload.get("tension_signal"), casefold=False) or None,
        "context_relevant": context_relevant,
        "run_entity_pass": _to_bool(payload.get("run_entity_pass")) or bool(entity_mentions),
        "run_threads_pass": _to_bool(payload.get("run_threads_pass")) or bool(thread_signals),
        "identity_relevant": _to_bool(payload.get("identity_relevant")) or bool(identity_signals),
    }


async def run_pass1_triage(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    session_id: str,
    messages: Sequence[Dict[str, Any]],
    reference_time: Optional[datetime] = None,
    settings: Optional[Settings] = None,
) -> DerivedPassResult:
    s = settings or get_settings()
    input_hash = _input_hash(
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=session_id,
        pass_name=PASS1_TRIAGE,
        messages=messages,
        settings=s,
    )
    run_id = await _start_run(
        db=db,
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=session_id,
        pass_name=PASS1_TRIAGE,
        input_hash=input_hash,
        input_watermark=session_id,
        settings=s,
    )
    existing = await _succeeded_run(db, run_id)
    if existing:
        classification = await db.fetchone(
            """
            SELECT run_entity_pass, run_threads_pass
            FROM session_classifications
            WHERE session_id=$1 AND user_id=$2
            """,
            session_id,
            user_id,
        ) or {}
        return DerivedPassResult(
            run_id=run_id,
            pass_name=PASS1_TRIAGE,
            output_hash=str(existing["output_hash"]),
            run_entity_pass=bool(classification.get("run_entity_pass")),
            run_threads_pass=bool(classification.get("run_threads_pass")),
            should_run_identity=await should_run_pass4_identity(db=db, user_id=user_id, settings=s),
            should_run_living_context=await should_run_pass5_living_context(db=db, user_id=user_id, settings=s),
        )
    try:
        parsed = await _call_pass1_llm(messages, s)
        source = "llm"
        if not parsed:
            if bool(s.derived_pipeline_llm_enabled):
                await _quarantine(
                    db=db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    pass_name=PASS1_TRIAGE,
                    session_id=session_id,
                    reason_code=QUARANTINE_PARSE_FAILURE,
                    payload={
                        "message": "Pass 1 LLM output was missing or not valid JSON; deterministic fallback used.",
                        "fallback": "heuristic",
                    },
                    run_id=run_id,
                )
            parsed = _heuristic_pass1(messages)
            source = "heuristic_fallback"
        payload = _normalize_pass1_payload(parsed)
        payload["source"] = source
        session_date = reference_time or _utcnow()
        output_hash = stable_short_hash(payload, length=24)
        await db.execute(
            """
            INSERT INTO session_classifications (
                session_id, user_id, session_date, is_memory_worthy,
                session_kind, one_line_summary, entity_mentions, run_entity_pass,
                run_threads_pass, identity_relevant, emotional_weight,
                emotional_note, tension_signal, processed_at, model_used,
                raw_triage_output, context_relevant
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7::text[],$8,$9,$10,$11,$12,$13,NOW(),$14,$15::jsonb,$16)
            ON CONFLICT (session_id)
            DO UPDATE SET
                user_id=EXCLUDED.user_id,
                session_date=EXCLUDED.session_date,
                is_memory_worthy=EXCLUDED.is_memory_worthy,
                session_kind=EXCLUDED.session_kind,
                one_line_summary=EXCLUDED.one_line_summary,
                entity_mentions=EXCLUDED.entity_mentions,
                run_entity_pass=EXCLUDED.run_entity_pass,
                run_threads_pass=EXCLUDED.run_threads_pass,
                identity_relevant=EXCLUDED.identity_relevant,
                emotional_weight=EXCLUDED.emotional_weight,
                emotional_note=EXCLUDED.emotional_note,
                tension_signal=EXCLUDED.tension_signal,
                processed_at=NOW(),
                model_used=EXCLUDED.model_used,
                raw_triage_output=EXCLUDED.raw_triage_output,
                context_relevant=EXCLUDED.context_relevant
            """,
            session_id,
            user_id,
            session_date,
            payload["is_memory_worthy"],
            payload["session_kind"],
            payload["memory_deltas"][0] if payload["memory_deltas"] else None,
            payload["entity_mentions"],
            payload["run_entity_pass"],
            payload["run_threads_pass"],
            payload["identity_relevant"],
            payload["emotional_weight"],
            payload["emotional_note"],
            payload["tension_signal"],
            s.derived_pipeline_model_version if source == "llm" else f"{s.derived_pipeline_model_version}:fallback",
            payload,
            payload["context_relevant"],
        )
        assertions: List[int] = []
        for statement in payload["memory_deltas"]:
            assertion_id = await _write_assertion(
                db=db,
                tenant_id=tenant_id,
                user_id=user_id,
                pass_name=PASS1_TRIAGE,
                surface="memory_delta",
                statement_text=statement,
                run_id=run_id,
                source_session_ids=[session_id],
                source_turn_refs=_source_turn_refs(session_id=session_id, messages=messages, text_hint=statement),
                salience=0.55,
                importance=0.55,
                confidence_extraction=0.72 if source == "llm" else 0.45,
                confidence_validity=0.65 if source == "llm" else 0.45,
            )
            if assertion_id:
                assertions.append(assertion_id)
        for statement in payload["identity_signals"]:
            await _write_assertion(
                db=db,
                tenant_id=tenant_id,
                user_id=user_id,
                pass_name=PASS1_TRIAGE,
                surface="identity_signal",
                statement_text=statement,
                slot_key=None,
                run_id=run_id,
                source_session_ids=[session_id],
                source_turn_refs=_source_turn_refs(session_id=session_id, messages=messages, text_hint=statement),
                salience=0.7,
                importance=0.75,
                confidence_extraction=0.72 if source == "llm" else 0.45,
                confidence_validity=0.62 if source == "llm" else 0.42,
            )
        for statement in payload["thread_signals"]:
            await _write_assertion(
                db=db,
                tenant_id=tenant_id,
                user_id=user_id,
                pass_name=PASS1_TRIAGE,
                surface="thread_signal",
                statement_text=statement,
                run_id=run_id,
                source_session_ids=[session_id],
                source_turn_refs=_source_turn_refs(session_id=session_id, messages=messages, text_hint=statement),
                salience=0.65,
                importance=0.65,
                confidence_extraction=0.72 if source == "llm" else 0.45,
                confidence_validity=0.58 if source == "llm" else 0.4,
            )
        if payload.get("tension_signal"):
            await _write_assertion(
                db=db,
                tenant_id=tenant_id,
                user_id=user_id,
                pass_name=PASS1_TRIAGE,
                surface="living_context_statement",
                statement_text=payload["tension_signal"],
                run_id=run_id,
                source_session_ids=[session_id],
                source_turn_refs=_source_turn_refs(session_id=session_id, messages=messages, text_hint=payload["tension_signal"]),
                salience=0.7,
                importance=0.65,
                confidence_extraction=0.72 if source == "llm" else 0.45,
                confidence_validity=0.58 if source == "llm" else 0.4,
                metadata={"kind": "tension_signal"},
            )

        await _complete_run(db, run_id, output_hash)
        await _update_checkpoint(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            pipeline_name=PASS1_TRIAGE,
            run_id=run_id,
            input_watermark=session_id,
            output_hash=output_hash,
            increment_identity=1 if payload["identity_relevant"] else 0,
            increment_context=1 if payload["context_relevant"] else 0,
        )
        should_identity = await should_run_pass4_identity(db=db, user_id=user_id, settings=s)
        should_context = await should_run_pass5_living_context(db=db, user_id=user_id, settings=s)
        return DerivedPassResult(
            run_id=run_id,
            pass_name=PASS1_TRIAGE,
            output_hash=output_hash,
            run_entity_pass=payload["run_entity_pass"],
            run_threads_pass=payload["run_threads_pass"],
            should_run_identity=should_identity,
            should_run_living_context=should_context,
        )
    except Exception as exc:
        await _fail_run(db, run_id, "pass1_failed", str(exc))
        raise


async def should_run_pass4_identity(*, db: Database, user_id: str, settings: Optional[Settings] = None) -> bool:
    s = settings or get_settings()
    pass1 = await _checkpoint(db, user_id, PASS1_TRIAGE)
    if pass1 and int(pass1.get("identity_signal_count_since_last") or 0) >= int(s.derived_pipeline_identity_signal_threshold):
        return True
    previous = await _checkpoint(db, user_id, PASS4_IDENTITY)
    last = previous.get("last_processed") if previous else None
    if isinstance(last, datetime):
        return (_utcnow() - last.astimezone(timezone.utc)) >= timedelta(days=int(s.derived_pipeline_identity_ceiling_days))
    return bool(pass1 and int(pass1.get("identity_signal_count_since_last") or 0) > 0)


async def should_run_pass5_living_context(*, db: Database, user_id: str, settings: Optional[Settings] = None) -> bool:
    s = settings or get_settings()
    pass1 = await _checkpoint(db, user_id, PASS1_TRIAGE)
    if pass1 and int(pass1.get("context_delta_count_since_last") or 0) >= int(s.derived_pipeline_context_delta_threshold):
        return True
    previous = await _checkpoint(db, user_id, PASS5_LIVING_CONTEXT)
    last = previous.get("last_processed") if previous else None
    if isinstance(last, datetime):
        return (_utcnow() - last.astimezone(timezone.utc)) >= timedelta(days=int(s.derived_pipeline_context_ceiling_days))
    return bool(pass1 and int(pass1.get("context_delta_count_since_last") or 0) > 0)


def _canonical_name(name: str) -> str:
    return normalize_text(name, casefold=False).strip()


def _canonical_name_norm(name: str) -> str:
    return normalize_text(name)


def _entity_type(name: str) -> str:
    lowered = normalize_text(name)
    if lowered in {"sophie", "synapse", "bluum"}:
        return "project" if lowered != "sophie" else "other"
    return "person"


def _valid_thread_transition(current_status: str, current_lifecycle: str, action: str) -> bool:
    action_norm = normalize_text(action).upper()
    status = normalize_text(current_status)
    lifecycle = normalize_text(current_lifecycle)
    if lifecycle in {"resolved", "superseded"} and action_norm in {"UPDATE", "SNOOZE"}:
        return False
    if status == "resolved" and action_norm in {"UPDATE", "SNOOZE"}:
        return False
    if action_norm == "RESOLVE" and lifecycle in {"resolved", "superseded"}:
        return False
    return True


async def run_pass1_5_entities(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    session_id: str,
    messages: Sequence[Dict[str, Any]],
    settings: Optional[Settings] = None,
) -> DerivedPassResult:
    s = settings or get_settings()
    classification = await db.fetchone(
        "SELECT entity_mentions, raw_triage_output FROM session_classifications WHERE session_id=$1 AND user_id=$2",
        session_id,
        user_id,
    )
    mentions = _text_list((classification or {}).get("entity_mentions"), limit=20)
    input_hash = _input_hash(
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=session_id,
        pass_name=PASS1_5_ENTITIES,
        messages=messages,
        extra={"entity_mentions": mentions},
        settings=s,
    )
    run_id = await _start_run(
        db=db,
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=session_id,
        pass_name=PASS1_5_ENTITIES,
        input_hash=input_hash,
        input_watermark=session_id,
        settings=s,
    )
    existing = await _succeeded_run(db, run_id)
    if existing:
        return DerivedPassResult(run_id=run_id, pass_name=PASS1_5_ENTITIES, output_hash=str(existing["output_hash"]))
    try:
        resolved_items: Optional[List[Dict[str, Any]]] = None
        if bool(s.derived_pipeline_llm_enabled) and mentions:
            existing_entities = await db.fetch(
                """
                SELECT entity_id, canonical_name, canonical_name_normalized, type,
                       aliases, relationship_to_user, status, confidence
                FROM entity_profiles
                WHERE user_id=$1
                ORDER BY last_seen_at DESC NULLS LAST, created_at DESC
                LIMIT 80
                """,
                user_id,
            )
            mention_payload = [
                {
                    "mention": name,
                    "session_ids": [session_id],
                    "context_snippets": [ref.get("text") for ref in _source_turn_refs(session_id=session_id, messages=messages, text_hint=name)[:3]],
                }
                for name in mentions
            ]
            resolved_items = await resolve_entity_mentions(
                existing_entities=existing_entities,
                mentions=mention_payload,
                model=s.derived_pipeline_model_version,
            )
        if resolved_items:
            names_to_write: List[Dict[str, Any]] = []
            existing_by_id = {
                _canonical_name_norm(row.get("entity_id")): row
                for row in await db.fetch("SELECT * FROM entity_profiles WHERE user_id=$1", user_id)
            }
            for item in resolved_items:
                decision = normalize_text(item.get("decision")).upper()
                mention = _canonical_name(item.get("mention"))
                if decision == "SKIP":
                    continue
                if decision == "MATCH":
                    matched_id = normalize_text(item.get("matched_entity_id"))
                    target = existing_by_id.get(matched_id)
                    if target:
                        await db.execute(
                            """
                            UPDATE entity_profiles
                            SET aliases = (
                                  SELECT ARRAY(
                                    SELECT DISTINCT x
                                    FROM unnest(COALESCE(aliases, '{}') || $2::text[]) AS x
                                    WHERE x IS NOT NULL AND x <> ''
                                  )
                                ),
                                mention_count = COALESCE(mention_count, 0) + 1,
                                last_seen_at = NOW(),
                                last_updated_at = NOW(),
                                source_session_ids = (
                                  SELECT ARRAY(
                                    SELECT DISTINCT x
                                    FROM unnest(COALESCE(source_session_ids, '{}') || $3::text[]) AS x
                                    WHERE x IS NOT NULL AND x <> ''
                                  )
                                ),
                                confidence = LEAST(COALESCE(confidence, 0.4) + 0.05, 1.0)
                            WHERE user_id=$1 AND entity_id=$4
                            """,
                            user_id,
                            [mention] if mention else [],
                            [session_id],
                            matched_id,
                        )
                        names_to_write.append({"name": target.get("canonical_name") or mention, "matched": True})
                    continue
                if decision == "NEW":
                    names_to_write.append(
                        {
                            "name": _canonical_name(item.get("canonical_name")) or mention,
                            "type": normalize_text(item.get("type")) or None,
                            "status": normalize_text(item.get("status")) or None,
                            "relationship_to_user": normalize_text(item.get("relationship_to_user")) or None,
                            "confidence": item.get("confidence"),
                            "aliases": _text_list(item.get("aliases")) or ([mention] if mention else []),
                        }
                    )
            write_items = names_to_write
        else:
            write_items = [{"name": name} for name in mentions]

        for item in write_items:
            name = item.get("name") if isinstance(item, dict) else item
            canonical = _canonical_name(name)
            canonical_norm = _canonical_name_norm(canonical)
            if not canonical_norm:
                continue
            category = _semantic_category(canonical, "entity_mention")
            memory_layer, _floor = _memory_layer_and_floor(category)
            entity_status = normalize_text((item or {}).get("status") if isinstance(item, dict) else None) or "tentative"
            entity_type = normalize_text((item or {}).get("type") if isinstance(item, dict) else None) or _entity_type(canonical)
            relationship_to_user = normalize_text((item or {}).get("relationship_to_user") if isinstance(item, dict) else None) or None
            confidence_value = (item or {}).get("confidence") if isinstance(item, dict) else None
            if not isinstance(confidence_value, (int, float)):
                confidence_value = 0.45
            aliases_value = _text_list((item or {}).get("aliases") if isinstance(item, dict) else None) or [canonical]
            await db.execute(
                """
                INSERT INTO entity_profiles (
                    user_id, canonical_name, canonical_name_normalized, type,
                    aliases, status, relationship_to_user, confidence, mention_count, first_seen_at,
                    last_seen_at, last_updated_at, last_processed_session_date,
                    source_session_ids, memory_layer
                )
                VALUES (
                    $1,$2,$3,$4,$5::text[],$6,$7,$8,1,NOW(),NOW(),NOW(),NOW(),$9::text[],$10
                )
                ON CONFLICT (user_id, canonical_name_normalized)
                DO UPDATE SET
                    aliases = (
                        SELECT ARRAY(
                            SELECT DISTINCT x
                            FROM unnest(COALESCE(entity_profiles.aliases, '{}') || EXCLUDED.aliases) AS x
                            WHERE x IS NOT NULL AND x <> ''
                        )
                    ),
                    mention_count = COALESCE(entity_profiles.mention_count, 0) + 1,
                    last_seen_at = NOW(),
                    last_updated_at = NOW(),
                    last_processed_session_date = NOW(),
                    source_session_ids = (
                        SELECT ARRAY(
                            SELECT DISTINCT x
                            FROM unnest(COALESCE(entity_profiles.source_session_ids, '{}') || EXCLUDED.source_session_ids) AS x
                            WHERE x IS NOT NULL AND x <> ''
                        )
                    ),
                    status = CASE
                        WHEN COALESCE(entity_profiles.mention_count, 0) + 1 >= 3 THEN 'active'
                        ELSE entity_profiles.status
                    END,
                    memory_layer = COALESCE(entity_profiles.memory_layer, EXCLUDED.memory_layer)
                """,
                user_id,
                canonical,
                canonical_norm,
                entity_type,
                aliases_value,
                entity_status,
                relationship_to_user,
                float(confidence_value),
                [session_id],
                memory_layer,
            )
            if bool(s.derived_pipeline_llm_enabled):
                current_profile = await db.fetchone(
                    """
                    SELECT entity_id, profile_text
                    FROM entity_profiles
                    WHERE user_id=$1 AND canonical_name_normalized=$2
                    """,
                    user_id,
                    canonical_norm,
                )
                profile_payload = await build_entity_profile(
                    canonical_name=canonical,
                    entity_type=entity_type,
                    relationship_to_user=relationship_to_user or "other",
                    messages=list(messages),
                    existing_profile_text=(current_profile or {}).get("profile_text"),
                    model=s.derived_pipeline_model_version,
                )
                if profile_payload:
                    await db.execute(
                        """
                        UPDATE entity_profiles
                        SET profile_text=COALESCE($3, profile_text),
                            key_facts=$4::jsonb,
                            open_questions=$5::jsonb,
                            last_known_status=COALESCE($6, last_known_status),
                            last_updated_at=NOW()
                        WHERE user_id=$1 AND canonical_name_normalized=$2
                        """,
                        user_id,
                        canonical_norm,
                        profile_payload.get("profile_text"),
                        profile_payload.get("key_facts") or [],
                        profile_payload.get("open_questions") or [],
                        profile_payload.get("last_known_status"),
                    )
            await _write_assertion(
                db=db,
                tenant_id=tenant_id,
                user_id=user_id,
                pass_name=PASS1_5_ENTITIES,
                surface="entity_mention",
                statement_text=f"{canonical} was mentioned in the session.",
                run_id=run_id,
                source_session_ids=[session_id],
                source_turn_refs=_source_turn_refs(session_id=session_id, messages=messages, text_hint=canonical),
                salience=0.45,
                importance=0.45,
                confidence_extraction=0.7,
                confidence_validity=0.55,
                metadata={"entity_name": canonical},
            )
        output_hash = stable_short_hash({"mentions": mentions}, length=24)
        await _complete_run(db, run_id, output_hash)
        await _update_checkpoint(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            pipeline_name=PASS1_5_ENTITIES,
            run_id=run_id,
            input_watermark=session_id,
            output_hash=output_hash,
        )
        return DerivedPassResult(run_id=run_id, pass_name=PASS1_5_ENTITIES, output_hash=output_hash)
    except Exception as exc:
        await _fail_run(db, run_id, "pass1_5_failed", str(exc))
        raise


async def run_pass3_threads(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    session_id: str,
    messages: Sequence[Dict[str, Any]],
    settings: Optional[Settings] = None,
) -> DerivedPassResult:
    s = settings or get_settings()
    classification = await db.fetchone(
        "SELECT raw_triage_output FROM session_classifications WHERE session_id=$1 AND user_id=$2",
        session_id,
        user_id,
    )
    raw = (classification or {}).get("raw_triage_output") if classification else {}
    thread_signals = _text_list((raw or {}).get("thread_signals"), limit=8) if isinstance(raw, dict) else []
    input_hash = _input_hash(
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=session_id,
        pass_name=PASS3_THREADS,
        messages=messages,
        extra={"thread_signals": thread_signals},
        settings=s,
    )
    run_id = await _start_run(
        db=db,
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=session_id,
        pass_name=PASS3_THREADS,
        input_hash=input_hash,
        input_watermark=session_id,
        settings=s,
    )
    existing = await _succeeded_run(db, run_id)
    if existing:
        return DerivedPassResult(run_id=run_id, pass_name=PASS3_THREADS, output_hash=str(existing["output_hash"]))
    try:
        classification_meta = raw if isinstance(raw, dict) else {}
        actions: Optional[List[Dict[str, Any]]] = None
        if bool(s.derived_pipeline_llm_enabled):
            existing_threads = await db.fetch(
                """
                SELECT thread_id, title, detail, status, priority, category,
                       lifecycle_state, follow_up_after, source_session_ids
                FROM open_threads
                WHERE user_id=$1 AND status IN ('open','snoozed')
                ORDER BY last_updated_at DESC NULLS LAST, created_at DESC
                LIMIT 50
                """,
                user_id,
            )
            actions = await extract_thread_actions(
                messages=list(messages),
                session_date=_utcnow(),
                emotional_weight=normalize_text(classification_meta.get("emotional_weight")) or "none",
                emotional_note=normalize_text(classification_meta.get("emotional_note")) or None,
                thread_signals=thread_signals,
                existing_threads=existing_threads,
                model=s.derived_pipeline_model_version,
            )
        if not actions:
            actions = [
                {
                    "action": "CREATE",
                    "title": signal[:120],
                    "detail": signal,
                    "category": _semantic_category(signal, "thread_update"),
                    "priority": "medium",
                    "source_session_id": session_id,
                }
                for signal in thread_signals
            ]

        for action in actions:
            kind = normalize_text(action.get("action")).upper()
            if kind in {"NO_ACTION", ""}:
                continue
            if kind in {"UPDATE", "RESOLVE", "SNOOZE"}:
                thread_id_existing = normalize_text(action.get("thread_id"))
                current = await db.fetchone(
                    """
                    SELECT status, lifecycle_state
                    FROM open_threads
                    WHERE user_id=$1 AND thread_id=$2
                    """,
                    user_id,
                    thread_id_existing,
                )
                if not current or not _valid_thread_transition(
                    current.get("status"),
                    current.get("lifecycle_state"),
                    kind,
                ):
                    await _quarantine(
                        db=db,
                        tenant_id=tenant_id,
                        user_id=user_id,
                        pass_name=PASS3_THREADS,
                        session_id=session_id,
                        reason_code=QUARANTINE_INVALID_LIFECYCLE_TRANSITION,
                        payload={"action": action, "current": current},
                        run_id=run_id,
                    )
                    continue
                if kind == "RESOLVE":
                    await db.execute(
                        """
                        UPDATE open_threads
                        SET status='resolved',
                            lifecycle_state='resolved',
                            resolved_at=NOW(),
                            resolution_note=$3,
                            last_updated_at=NOW()
                        WHERE user_id=$1 AND thread_id=$2
                        """,
                        user_id,
                        thread_id_existing,
                        normalize_text(action.get("resolution_note")) or "Resolved from latest context.",
                    )
                    continue
                if kind == "SNOOZE":
                    await db.execute(
                        """
                        UPDATE open_threads
                        SET status='snoozed',
                            lifecycle_state='snoozed',
                            last_updated_at=NOW()
                        WHERE user_id=$1 AND thread_id=$2
                        """,
                        user_id,
                        thread_id_existing,
                    )
                    continue

            signal = normalize_text(action.get("detail")) or normalize_text(action.get("title"))
            title = normalize_text(action.get("title"))[:120] or signal[:120]
            category = _semantic_category(signal, "thread_update")
            if normalize_text(action.get("category")) in VALID_CATEGORIES:
                category = normalize_text(action.get("category"))
            priority = normalize_text(action.get("priority")) or "medium"
            if priority not in VALID_PRIORITIES:
                priority = "medium"
            memory_layer, retention_floor = _memory_layer_and_floor(category)
            evidence_refs = _source_turn_refs(session_id=session_id, messages=messages, text_hint=signal)
            if not evidence_refs:
                await _quarantine(
                    db=db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    pass_name=PASS3_THREADS,
                    session_id=session_id,
                    reason_code=QUARANTINE_MISSING_EVIDENCE_REFS,
                    payload={"thread_signal": signal},
                    run_id=run_id,
                )
                continue
            thread_id = stable_short_hash({"user_id": user_id, "thread": normalize_text(title)}, length=20)
            await db.execute(
                """
                INSERT INTO open_threads (
                    thread_id, user_id, title, detail, status, priority, category,
                    source_session_ids, first_seen_at, last_updated_at, last_mentioned_at,
                    lifecycle_state, evidence_turn_refs, confidence_extraction,
                    confidence_validity, memory_layer, semantic_category, retention_floor
                )
                VALUES (
                    $1,$2,$3,$4,'open',$11,$5,$6::text[],NOW(),NOW(),NOW(),
                    'active',$7::jsonb,0.7,0.55,$8,$9,$10
                )
                ON CONFLICT (thread_id)
                DO UPDATE SET
                    detail=COALESCE(open_threads.detail, EXCLUDED.detail),
                    status=CASE WHEN open_threads.status='resolved' THEN open_threads.status ELSE 'open' END,
                    source_session_ids=(
                        SELECT ARRAY(
                            SELECT DISTINCT x
                            FROM unnest(COALESCE(open_threads.source_session_ids, '{}') || EXCLUDED.source_session_ids) AS x
                            WHERE x IS NOT NULL AND x <> ''
                        )
                    ),
                    last_updated_at=NOW(),
                    last_mentioned_at=NOW(),
                    lifecycle_state=CASE
                        WHEN open_threads.lifecycle_state='resolved' THEN open_threads.lifecycle_state
                        ELSE 'active'
                    END,
                    evidence_turn_refs=COALESCE(open_threads.evidence_turn_refs, '[]'::jsonb) || EXCLUDED.evidence_turn_refs,
                    confidence_extraction=GREATEST(COALESCE(open_threads.confidence_extraction, 0), EXCLUDED.confidence_extraction),
                    confidence_validity=GREATEST(COALESCE(open_threads.confidence_validity, 0), EXCLUDED.confidence_validity),
                    memory_layer=COALESCE(open_threads.memory_layer, EXCLUDED.memory_layer),
                    semantic_category=COALESCE(open_threads.semantic_category, EXCLUDED.semantic_category),
                    retention_floor=GREATEST(COALESCE(open_threads.retention_floor, 0), EXCLUDED.retention_floor)
                """,
                thread_id,
                user_id,
                title,
                signal,
                category,
                [session_id],
                evidence_refs,
                memory_layer,
                category,
                retention_floor,
                priority,
            )
            await _write_assertion(
                db=db,
                tenant_id=tenant_id,
                user_id=user_id,
                pass_name=PASS3_THREADS,
                surface="thread_update",
                statement_text=signal,
                run_id=run_id,
                source_session_ids=[session_id],
                source_turn_refs=evidence_refs,
                salience=0.65,
                importance=0.65,
                confidence_extraction=0.7,
                confidence_validity=0.55,
                metadata={"thread_id": thread_id},
            )
        output_hash = stable_short_hash({"thread_signals": thread_signals}, length=24)
        await _complete_run(db, run_id, output_hash)
        await _update_checkpoint(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            pipeline_name=PASS3_THREADS,
            run_id=run_id,
            input_watermark=session_id,
            output_hash=output_hash,
        )
        return DerivedPassResult(run_id=run_id, pass_name=PASS3_THREADS, output_hash=output_hash)
    except Exception as exc:
        await _fail_run(db, run_id, "pass3_failed", str(exc))
        raise


async def _recent_classifications(
    db: Database,
    *,
    user_id: str,
    identity_only: bool = False,
    context_only: bool = False,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    clauses = ["user_id=$1"]
    if identity_only:
        clauses.append("identity_relevant IS TRUE")
    if context_only:
        clauses.append("context_relevant IS TRUE")
    where = " AND ".join(clauses)
    return await db.fetch(
        f"""
        SELECT session_id, session_date, raw_triage_output, tension_signal
        FROM session_classifications
        WHERE {where}
        ORDER BY session_date DESC NULLS LAST, processed_at DESC NULLS LAST
        LIMIT {int(limit)}
        """,
        user_id,
    )


async def run_pass4_identity(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    settings: Optional[Settings] = None,
) -> DerivedPassResult:
    s = settings or get_settings()
    rows = await _recent_classifications(db, user_id=user_id, identity_only=True, limit=80)
    input_hash = _input_hash(
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=None,
        pass_name=PASS4_IDENTITY,
        extra={"sessions": [r.get("session_id") for r in rows]},
        settings=s,
    )
    run_id = await _start_run(
        db=db,
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=None,
        pass_name=PASS4_IDENTITY,
        input_hash=input_hash,
        input_watermark=rows[0].get("session_id") if rows else None,
        settings=s,
    )
    existing = await _succeeded_run(db, run_id)
    if existing:
        return DerivedPassResult(run_id=run_id, pass_name=PASS4_IDENTITY, output_hash=str(existing["output_hash"]))
    try:
        assertions: List[Dict[str, Any]] = []
        for row in rows:
            raw = row.get("raw_triage_output") if isinstance(row.get("raw_triage_output"), dict) else {}
            for signal in _text_list(raw.get("identity_signals"), limit=8):
                assertion_id = await _write_assertion(
                    db=db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    pass_name=PASS4_IDENTITY,
                    surface="identity_trait",
                    statement_text=signal,
                    run_id=run_id,
                    source_session_ids=[row["session_id"]],
                    source_turn_refs=[{"session_id": row["session_id"], "source": "session_classifications.identity_signals"}],
                    salience=0.75,
                    importance=0.8,
                    confidence_extraction=0.72,
                    confidence_validity=0.6,
                )
                if assertion_id:
                    assertions.append(
                        {
                            "assertion_id": assertion_id,
                            "statement_text": signal,
                            "source_session_ids": [row["session_id"]],
                        }
                    )
        rich_output: Optional[Dict[str, Any]] = None
        if bool(s.derived_pipeline_llm_enabled) and rows:
            existing_profile = await db.fetchone(
                "SELECT * FROM identity_profile WHERE user_id=$1",
                user_id,
            )
            persistent_goals = await db.fetch(
                """
                SELECT thread_id, title, detail, source_session_ids, first_seen_at, last_mentioned_at
                FROM open_threads
                WHERE user_id=$1
                  AND (thread_type='persistent_goal' OR category='goal')
                ORDER BY last_mentioned_at DESC NULLS LAST, created_at DESC
                LIMIT 20
                """,
                user_id,
            )
            parsed = await synthesize_identity_profile(
                existing_profile=existing_profile,
                session_rows=list(reversed(rows)),
                persistent_goals=persistent_goals,
                model=s.derived_pipeline_model_version,
            )
            if parsed:
                rich_output = normalize_identity_output(parsed)
        output_hash = stable_short_hash({"assertions": assertions, "rich": rich_output or {}}, length=24)
        if rich_output:
            await db.execute(
                """
                INSERT INTO identity_profile (
                  user_id, who_they_are, core_values, recurring_patterns,
                  family_history, faith_and_beliefs, what_they_want,
                  recurring_fears, what_they_avoid, how_they_relate,
                  persistent_goals, current_chapter, assertions,
                  last_synthesized_at, source_session_count, synthesis_model,
                  created_at, updated_at
                ) VALUES (
                  $1,$2,$3::jsonb,$4::jsonb,$5,$6,$7,
                  $8::jsonb,$9,$10,$11::jsonb,$12,$13::jsonb,
                  NOW(),$14,$15,NOW(),NOW()
                )
                ON CONFLICT (user_id) DO UPDATE SET
                  who_they_are=EXCLUDED.who_they_are,
                  core_values=EXCLUDED.core_values,
                  recurring_patterns=EXCLUDED.recurring_patterns,
                  family_history=EXCLUDED.family_history,
                  faith_and_beliefs=EXCLUDED.faith_and_beliefs,
                  what_they_want=EXCLUDED.what_they_want,
                  recurring_fears=EXCLUDED.recurring_fears,
                  what_they_avoid=EXCLUDED.what_they_avoid,
                  how_they_relate=EXCLUDED.how_they_relate,
                  persistent_goals=EXCLUDED.persistent_goals,
                  current_chapter=EXCLUDED.current_chapter,
                  assertions=EXCLUDED.assertions,
                  last_synthesized_at=NOW(),
                  source_session_count=EXCLUDED.source_session_count,
                  synthesis_model=EXCLUDED.synthesis_model,
                  updated_at=NOW()
                """,
                user_id,
                rich_output["who_they_are"],
                rich_output["core_values"],
                rich_output["recurring_patterns"],
                rich_output["family_history"],
                rich_output["faith_and_beliefs"],
                rich_output["what_they_want"],
                rich_output["recurring_fears"],
                rich_output["what_they_avoid"],
                rich_output["how_they_relate"],
                rich_output["persistent_goals"],
                rich_output["current_chapter"],
                assertions,
                len(rows),
                s.derived_pipeline_model_version,
            )
        else:
            await db.execute(
                """
                INSERT INTO identity_profile (
                    user_id, assertions, last_synthesized_at, source_session_count,
                    synthesis_model, created_at, updated_at
                )
                VALUES ($1,$2::jsonb,NOW(),$3,$4,NOW(),NOW())
                ON CONFLICT (user_id)
                DO UPDATE SET
                    assertions=$2::jsonb,
                    last_synthesized_at=NOW(),
                    source_session_count=$3,
                    synthesis_model=$4,
                    updated_at=NOW()
                """,
                user_id,
                assertions,
                len(rows),
                s.derived_pipeline_model_version,
            )
        await _complete_run(db, run_id, output_hash)
        await _update_checkpoint(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            pipeline_name=PASS4_IDENTITY,
            run_id=run_id,
            input_watermark=rows[0].get("session_id") if rows else None,
            output_hash=output_hash,
            reset_identity=True,
        )
        return DerivedPassResult(run_id=run_id, pass_name=PASS4_IDENTITY, output_hash=output_hash)
    except Exception as exc:
        await _fail_run(db, run_id, "pass4_failed", str(exc))
        raise


async def run_pass5_living_context(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    settings: Optional[Settings] = None,
) -> DerivedPassResult:
    s = settings or get_settings()
    rows = await _recent_classifications(db, user_id=user_id, context_only=True, limit=40)
    input_hash = _input_hash(
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=None,
        pass_name=PASS5_LIVING_CONTEXT,
        extra={"sessions": [r.get("session_id") for r in rows]},
        settings=s,
    )
    run_id = await _start_run(
        db=db,
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=None,
        pass_name=PASS5_LIVING_CONTEXT,
        input_hash=input_hash,
        input_watermark=rows[0].get("session_id") if rows else None,
        settings=s,
    )
    existing = await _succeeded_run(db, run_id)
    if existing:
        return DerivedPassResult(run_id=run_id, pass_name=PASS5_LIVING_CONTEXT, output_hash=str(existing["output_hash"]))
    try:
        assertions: List[Dict[str, Any]] = []
        for row in rows:
            raw = row.get("raw_triage_output") if isinstance(row.get("raw_triage_output"), dict) else {}
            statements = _text_list(raw.get("memory_deltas"), limit=4)
            statements.extend(_text_list(raw.get("thread_signals"), limit=4))
            tension = normalize_text(row.get("tension_signal"), casefold=False)
            if tension:
                statements.append(tension)
            for statement in statements[:10]:
                assertion_id = await _write_assertion(
                    db=db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    pass_name=PASS5_LIVING_CONTEXT,
                    surface="living_context_statement",
                    statement_text=statement,
                    run_id=run_id,
                    source_session_ids=[row["session_id"]],
                    source_turn_refs=[{"session_id": row["session_id"], "source": "session_classifications.context"}],
                    salience=0.7,
                    importance=0.65,
                    confidence_extraction=0.72,
                    confidence_validity=0.58,
                )
                if assertion_id:
                    assertions.append(
                        {
                            "assertion_id": assertion_id,
                            "statement_text": statement,
                            "source_session_ids": [row["session_id"]],
                        }
                    )
        rich_output: Optional[Dict[str, Any]] = None
        if bool(s.derived_pipeline_llm_enabled) and rows:
            existing_context = await db.fetchone("SELECT * FROM living_context WHERE user_id=$1", user_id)
            identity_grounding = await db.fetchone("SELECT * FROM identity_profile WHERE user_id=$1", user_id)
            open_threads = await db.fetch(
                """
                SELECT thread_id, title, detail, priority, category, thread_type,
                       source_session_ids, last_mentioned_at, follow_up_after
                FROM open_threads
                WHERE user_id=$1 AND status='open'
                ORDER BY salience_score DESC NULLS LAST, last_mentioned_at DESC NULLS LAST
                LIMIT 12
                """,
                user_id,
            )
            active_entities = await db.fetch(
                """
                SELECT canonical_name, type, relationship_to_user, profile_text,
                       last_known_status, salience_score, source_session_ids
                FROM entity_profiles
                WHERE user_id=$1 AND status IN ('active','tentative')
                ORDER BY salience_score DESC NULLS LAST, last_seen_at DESC NULLS LAST
                LIMIT 12
                """,
                user_id,
            )
            parsed = await synthesize_living_context(
                existing_context=existing_context,
                identity_grounding=identity_grounding,
                recent_sessions=list(reversed(rows)),
                open_threads=open_threads,
                active_entities=active_entities,
                model=s.derived_pipeline_model_version,
            )
            if parsed:
                rich_output = normalize_living_context_output(parsed)
        output_hash = stable_short_hash({"assertions": assertions, "rich": rich_output or {}}, length=24)
        if rich_output:
            await db.execute(
                """
                INSERT INTO living_context (
                  user_id, current_focus, recent_narrative,
                  relationship_pulse, emotional_texture,
                  primary_tension, what_theyre_avoiding,
                  unspoken_goal, why_it_matters,
                  active_contradictions, sophie_directives,
                  assertions, last_synthesized_at, sessions_since_last,
                  source_session_count, synthesis_model, created_at, updated_at
                ) VALUES (
                  $1,$2,$3,$4,$5,$6,$7,$8,$9,
                  $10::jsonb,$11::jsonb,$12::jsonb,NOW(),0,$13,$14,NOW(),NOW()
                )
                ON CONFLICT (user_id) DO UPDATE SET
                  current_focus=EXCLUDED.current_focus,
                  recent_narrative=EXCLUDED.recent_narrative,
                  relationship_pulse=EXCLUDED.relationship_pulse,
                  emotional_texture=EXCLUDED.emotional_texture,
                  primary_tension=EXCLUDED.primary_tension,
                  what_theyre_avoiding=EXCLUDED.what_theyre_avoiding,
                  unspoken_goal=EXCLUDED.unspoken_goal,
                  why_it_matters=EXCLUDED.why_it_matters,
                  active_contradictions=EXCLUDED.active_contradictions,
                  sophie_directives=EXCLUDED.sophie_directives,
                  assertions=EXCLUDED.assertions,
                  last_synthesized_at=NOW(),
                  sessions_since_last=0,
                  source_session_count=EXCLUDED.source_session_count,
                  synthesis_model=EXCLUDED.synthesis_model,
                  updated_at=NOW()
                """,
                user_id,
                rich_output["current_focus"],
                rich_output["recent_narrative"],
                rich_output["relationship_pulse"],
                rich_output["emotional_texture"],
                rich_output["primary_tension"],
                rich_output["what_theyre_avoiding"],
                rich_output["unspoken_goal"],
                rich_output["why_it_matters"],
                rich_output["active_contradictions"],
                rich_output["sophie_directives"],
                assertions,
                len(rows),
                s.derived_pipeline_model_version,
            )
        else:
            await db.execute(
                """
                INSERT INTO living_context (
                    user_id, assertions, last_synthesized_at, sessions_since_last,
                    source_session_count, synthesis_model, created_at, updated_at
                )
                VALUES ($1,$2::jsonb,NOW(),0,$3,$4,NOW(),NOW())
                ON CONFLICT (user_id)
                DO UPDATE SET
                    assertions=$2::jsonb,
                    last_synthesized_at=NOW(),
                    sessions_since_last=0,
                    source_session_count=$3,
                    synthesis_model=$4,
                    updated_at=NOW()
                """,
                user_id,
                assertions,
                len(rows),
                s.derived_pipeline_model_version,
            )
        await _complete_run(db, run_id, output_hash)
        await _update_checkpoint(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            pipeline_name=PASS5_LIVING_CONTEXT,
            run_id=run_id,
            input_watermark=rows[0].get("session_id") if rows else None,
            output_hash=output_hash,
            reset_context=True,
        )
        return DerivedPassResult(run_id=run_id, pass_name=PASS5_LIVING_CONTEXT, output_hash=output_hash)
    except Exception as exc:
        await _fail_run(db, run_id, "pass5_failed", str(exc))
        raise


async def bump_memory_access(
    *,
    db: Database,
    user_id: str,
    tenant_id: str = "default",
    surfaces: Optional[Iterable[str]] = None,
) -> None:
    surface_list = list(surfaces or [])
    if surface_list:
        await db.execute(
            """
            UPDATE derived_assertions
            SET last_accessed_at=NOW(), access_count=access_count + 1, updated_at=NOW()
            WHERE tenant_id=$1 AND user_id=$2 AND surface = ANY($3::text[])
              AND lifecycle_state='active'
            """,
            tenant_id,
            user_id,
            surface_list,
        )
    await db.execute(
        """
        UPDATE open_threads
        SET last_accessed_at=NOW(), access_count=access_count + 1
        WHERE user_id=$1 AND status='open'
        """,
        user_id,
    )
    await db.execute(
        """
        UPDATE entity_profiles
        SET last_accessed_at=NOW(), access_count=access_count + 1
        WHERE user_id=$1 AND status IN ('active','tentative')
        """,
        user_id,
    )


async def run_reinforcement_decay(*, db: Database, tenant_id: str = "default") -> int:
    result = await db.execute(
        """
        UPDATE derived_assertions
        SET salience = GREATEST(
                COALESCE(retention_floor, 0.0),
                COALESCE(salience, 0.5) *
                CASE WHEN memory_layer='LML' THEN 0.98 ELSE 0.90 END
            ),
            updated_at=NOW()
        WHERE tenant_id=$1
          AND lifecycle_state='active'
          AND COALESCE(last_accessed_at, updated_at, created_at) < NOW() - INTERVAL '7 days'
        """,
        tenant_id,
    )
    try:
        return int(result.split()[-1])
    except Exception:
        return 0


async def run_staleness_review(*, db: Database, tenant_id: str = "default") -> int:
    result = await db.execute(
        """
        UPDATE derived_assertions
        SET staleness_status='review_needed',
            staleness_review_at=NOW(),
            updated_at=NOW()
        WHERE tenant_id=$1
          AND lifecycle_state='active'
          AND COALESCE(salience, 0) >= 0.75
          AND staleness_status = 'fresh'
          AND COALESCE(last_accessed_at, updated_at, created_at) < NOW() - INTERVAL '90 days'
        """,
        tenant_id,
    )
    try:
        return int(result.split()[-1])
    except Exception:
        return 0


async def run_explicit_consolidation(*, db: Database, tenant_id: str = "default") -> int:
    rows = await db.fetch(
        """
        SELECT user_id,
               surface,
               lower(statement_text) AS normalized_statement,
               array_agg(assertion_id ORDER BY assertion_id) AS assertion_ids,
               array_agg(DISTINCT unnest_session) AS session_ids,
               jsonb_agg(source_turn_refs) AS turn_refs,
               COUNT(*) AS n,
               MAX(salience) AS salience,
               MAX(importance) AS importance,
               MAX(confidence_validity) AS confidence_validity
        FROM (
            SELECT da.*, unnest(da.source_session_ids) AS unnest_session
            FROM derived_assertions da
            WHERE da.tenant_id=$1
              AND da.lifecycle_state='active'
              AND da.surface IN ('thread_signal','identity_signal','memory_delta','living_context_statement')
        ) x
        GROUP BY user_id, surface, lower(statement_text)
        HAVING COUNT(*) >= 3
        LIMIT 100
        """,
        tenant_id,
    )
    inserted = 0
    for row in rows:
        statement = row.get("normalized_statement")
        if not statement:
            continue
        await db.execute(
            """
            INSERT INTO consolidated_insights (
                tenant_id, user_id, insight_type, statement_text, lifecycle_state,
                source_assertion_ids, source_session_ids, source_turn_refs,
                salience, importance, confidence_validity, promoted_to
            )
            VALUES ($1,$2,$3,$4,'active',$5::bigint[],$6::text[],$7::jsonb,$8,$9,$10,$11::text[])
            """,
            tenant_id,
            row["user_id"],
            row["surface"],
            statement,
            row.get("assertion_ids") or [],
            row.get("session_ids") or [],
            row.get("turn_refs") or [],
            row.get("salience"),
            row.get("importance"),
            row.get("confidence_validity"),
            ["identity_profile" if row["surface"] == "identity_signal" else "living_context"],
        )
        inserted += 1
    return inserted
