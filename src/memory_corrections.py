from __future__ import annotations

from datetime import datetime, timezone as dt_timezone
from typing import Any, Dict, List, Optional

from .action_state import markActionItemDone
from .attention_outcomes import record_attention_outcome
from .timeline_events import (
    ALLOWED_CORRECTION_COMMANDS,
    build_object_refs,
    correction_command_to_defaults,
    record_timeline_event,
)


DEFAULT_COMPANION_ID = "sophie"


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _normalize_list(value: Any) -> List[Any]:
    return list(value) if isinstance(value, list) else []


def _title_for_command(command: str, note: Optional[str], target_type: Optional[str], target_id: Optional[str]) -> str:
    normalized = _normalize_text(command).lower()
    target_label = _normalize_text(target_type) or _normalize_text(target_id) or "memory"
    if normalized == "forget_that":
        return f"User asked to forget {target_label}"
    if normalized == "dont_bring_that_up_again":
        return f"User asked not to bring up {target_label} again"
    if normalized == "thats_wrong":
        return f"User corrected {target_label}"
    if normalized == "thats_done":
        return f"User marked {target_label} done"
    if normalized == "this_is_important":
        return f"User marked {target_label} important"
    if normalized == "remember_this":
        return f"User asked to remember {target_label}"
    return _normalize_text(note) or "User memory correction"


async def record_memory_correction(
    db: Any,
    *,
    tenant_id: str,
    user_id: str,
    companion_id: str = DEFAULT_COMPANION_ID,
    command: str,
    target_type: Optional[str] = None,
    target_id: Optional[str] = None,
    source_table: Optional[str] = None,
    source_id: Optional[str] = None,
    note: Optional[str] = None,
    evidence_refs: Optional[List[Dict[str, Any]]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    normalized_command = _normalize_text(command).lower()
    if normalized_command not in ALLOWED_CORRECTION_COMMANDS:
        raise ValueError(f"Unsupported command: {command}")
    defaults = correction_command_to_defaults(normalized_command)
    normalized_metadata = _normalize_dict(metadata)
    object_refs = build_object_refs(
        target_type=target_type,
        target_id=target_id,
        source_table=source_table,
        source_id=source_id,
        metadata=normalized_metadata,
    )
    recorded = await record_timeline_event(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        timeline_type=defaults["timeline_type"],
        event_type=defaults["event_type"],
        domain=_normalize_text(target_type) or normalized_metadata.get("domain") or "state",
        title=_title_for_command(normalized_command, note, target_type, target_id),
        summary=_normalize_text(note) or None,
        occurred_at=datetime.now(dt_timezone.utc),
        status="confirmed" if normalized_command in {"this_is_important", "remember_this"} else "active",
        actor="user",
        subject=_normalize_text(target_id) or _normalize_text(source_id) or None,
        object_refs=object_refs,
        source_table=_normalize_text(source_table) or None,
        source_id=_normalize_text(source_id) or _normalize_text(target_id) or None,
        evidence_refs=[item for item in _normalize_list(evidence_refs) if isinstance(item, dict)],
        user_corrected=normalized_command in {"forget_that", "dont_bring_that_up_again", "thats_wrong", "thats_done"},
        user_visible=True,
        effect=defaults["effect"],
        metadata={
            **normalized_metadata,
            "command": normalized_command,
            "targetType": _normalize_text(target_type) or None,
            "targetId": _normalize_text(target_id) or None,
            "sourceTable": _normalize_text(source_table) or None,
            "sourceId": _normalize_text(source_id) or None,
            "companionId": _normalize_text(companion_id) or DEFAULT_COMPANION_ID,
        },
    )

    applied_side_effects: List[Dict[str, Any]] = []
    gaps: List[str] = []

    if normalized_command == "dont_bring_that_up_again" and (_normalize_text(source_table) and _normalize_text(source_id) or _normalize_text(target_id)):
        attention_item_id = f"{_normalize_text(source_table)}:{_normalize_text(source_id)}" if _normalize_text(source_table) and _normalize_text(source_id) else f"memory-correction:{recorded['event_id']}"
        outcome = await record_attention_outcome(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            companion_id=_normalize_text(companion_id) or DEFAULT_COMPANION_ID,
            attention_item_id=attention_item_id,
            source_table=_normalize_text(source_table) or None,
            source_id=_normalize_text(source_id) or _normalize_text(target_id) or None,
            outcome_type="dont_bring_up_again",
            outcome_reason=_normalize_text(note) or "User explicitly asked not to bring this up again.",
            metadata={
                "source_object_ids": [_normalize_text(target_id)] if _normalize_text(target_id) else [],
                "evidence_refs": [item for item in _normalize_list(evidence_refs) if isinstance(item, dict)],
                "timeline_event_id": recorded["event_id"],
            },
        )
        applied_side_effects.append({"type": "attention_outcome", "outcome": outcome})

    if normalized_command == "thats_done":
        explicit_action_id = None
        if _normalize_text(source_table) == "action_items" and _normalize_text(source_id):
            explicit_action_id = _normalize_text(source_id)
        elif _normalize_text(target_type) == "action_item" and _normalize_text(target_id):
            explicit_action_id = _normalize_text(target_id)
        if explicit_action_id:
            try:
                updated = await markActionItemDone(
                    db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    action_item_id=explicit_action_id,
                    reason=_normalize_text(note) or "User marked this done through memory correction.",
                )
                applied_side_effects.append({"type": "action_item_mark_done", "action_item": updated})
            except Exception:
                gaps.append("action_item_update_not_applied")
        else:
            gaps.append("no_safe_action_item_target_for_mark_done")

    if normalized_command in {"thats_wrong", "forget_that"}:
        gaps.append("deletion_or_archival_not_performed_in_v1")

    return {
        "recorded": recorded,
        "applied_side_effects": applied_side_effects,
        "gaps": sorted(set(filter(None, gaps))),
        "readOnly": False,
    }
