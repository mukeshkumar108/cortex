from __future__ import annotations

import json
import re
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import UUID
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from .canonicalization import normalize_text
from .db import Database

ACTION_ITEM_STATUSES = {"pending", "done", "cancelled", "dismissed", "archived"}
ACTION_ITEM_KINDS = {"todo", "reminder", "habit"}
ACTION_ITEM_SOURCE_TYPES = {"direct_user", "candidate_promotion", "external_import", "system"}
ACTION_CANDIDATE_STATUSES = {"detected", "confirmed", "dismissed", "expired"}
ACTION_UPDATE_STATUSES = {"proposed", "applied", "rejected", "expired"}
ACTION_CANDIDATE_PENDING_STATUSES = {"detected", "needs_review"}

ACTION_ITEM_ALLOWED_TRANSITIONS = {
    "pending": {"done", "cancelled", "dismissed", "archived"},
    "done": {"archived"},
    "cancelled": {"archived"},
    "dismissed": {"archived"},
    "archived": set(),
}

ACTION_CANDIDATE_ALLOWED_TRANSITIONS = {
    "detected": {"confirmed", "dismissed", "expired"},
    "needs_review": {"confirmed", "dismissed", "expired"},
    "confirmed": set(),
    "dismissed": set(),
    "expired": set(),
}

ACTION_UPDATE_ALLOWED_TRANSITIONS = {
    "proposed": {"applied", "rejected", "expired"},
    "applied": set(),
    "rejected": set(),
    "expired": set(),
}

ACTION_STATE_STOPWORDS = {
    "a",
    "an",
    "and",
    "at",
    "for",
    "from",
    "in",
    "me",
    "my",
    "of",
    "on",
    "the",
    "to",
}


class ActionStateError(HTTPException):
    def __init__(self, status_code: int, detail: str):
        super().__init__(status_code=status_code, detail=detail)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def _json_safe(value: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    return json.loads(json.dumps(value, default=_json_default))


def _ensure_tz(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _title_tokens(value: Any) -> set[str]:
    text = normalize_text(value)
    if not text:
        return set()

    return {
        token
        for token in re.findall(r"[a-z0-9']{3,}", text)
        if token not in ACTION_STATE_STOPWORDS
    }


def _title_overlap(a: Any, b: Any) -> float:
    left = _title_tokens(a)
    right = _title_tokens(b)
    if not left or not right:
        return 0.0
    return len(left.intersection(right)) / len(left.union(right))


def _day_bucket(value: Any) -> Optional[str]:
    dt = _ensure_tz(value) if isinstance(value, datetime) else None
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).date().isoformat()


def _candidate_kind(candidate: Dict[str, Any]) -> str:
    subtype = normalize_text(candidate.get("candidate_subtype")).lower()
    record_type = normalize_text(candidate.get("record_type")).lower()
    if subtype in {"todo", "reminder", "habit"}:
        return subtype
    if subtype in {"follow_up", "waiting_on", "nudge"}:
        return "todo"
    if record_type == "reminder_candidate":
        return "reminder"
    return "todo"


def _candidate_date_bucket(candidate: Dict[str, Any], kind: str) -> Optional[str]:
    primary = candidate.get("proposed_remind_at") if kind == "reminder" else candidate.get("proposed_due_at")
    if primary is None:
        primary = candidate.get("due_iso") or candidate.get("proposed_due_at") or candidate.get("proposed_remind_at")
    parsed = _ensure_tz(primary) if isinstance(primary, datetime) else None
    return _day_bucket(parsed)


async def _link_matching_candidate_to_action_item(conn: Any, *, item: Dict[str, Any]) -> None:
    rows = await conn.fetch(
        """
        SELECT candidate_id, record_type, candidate_subtype, title, due_iso, proposed_due_at, proposed_remind_at,
               status, confidence_score, summary, provenance_summary, metadata, updated_at
        FROM actionable_candidates
        WHERE tenant_id=$1
          AND user_id=$2
          AND status = ANY($3::text[])
        ORDER BY updated_at DESC
        LIMIT 120
        """,
        item.get("tenant_id"),
        item.get("user_id"),
        ["detected", "needs_review"],
    )
    item_kind = normalize_text(item.get("kind")).lower()
    item_bucket = _day_bucket(item.get("remind_at") if item_kind == "reminder" else item.get("due_at"))
    best: Optional[Dict[str, Any]] = None
    best_score = 0.0
    best_date_or_undated = False
    for row in [dict(r) for r in (rows or []) if isinstance(r, dict)]:
        if _candidate_kind(row) != item_kind:
            continue
        candidate_bucket = _candidate_date_bucket(row, item_kind)
        date_or_undated = bool(candidate_bucket and item_bucket and candidate_bucket == item_bucket) or (
            candidate_bucket is None and item_bucket is None
        )
        score = _title_overlap(item.get("title"), row.get("title")) + (0.18 if date_or_undated else 0.0)
        if score > best_score:
            best = row
            best_score = score
            best_date_or_undated = date_or_undated
    if not best:
        return
    threshold = 0.62 if best_date_or_undated else 0.78
    if best_score < threshold:
        return
    await conn.execute(
        """
        UPDATE actionable_candidates
        SET status='confirmed',
            promoted_action_item_id=$4::uuid,
            confidence=COALESCE(confidence, confidence_score),
            provenance_summary=COALESCE(provenance_summary, summary),
            metadata=COALESCE(metadata, '{}'::jsonb) || $5::jsonb,
            updated_at=NOW()
        WHERE tenant_id=$1 AND user_id=$2 AND candidate_id=$3
        """,
        item.get("tenant_id"),
        item.get("user_id"),
        int(best.get("candidate_id")),
        str(item.get("id")),
        {
            "reconciled": True,
            "reconciliation_action": "confirmed_by_action_item",
            "matched_action_item_id": str(item.get("id")),
        },
    )


def _row_to_action_item(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": str(row.get("id")),
        "tenantId": row.get("tenant_id"),
        "userId": row.get("user_id"),
        "kind": row.get("kind"),
        "title": row.get("title"),
        "notes": row.get("notes"),
        "status": row.get("status"),
        "dueAt": row.get("due_at").isoformat() if row.get("due_at") else None,
        "remindAt": row.get("remind_at").isoformat() if row.get("remind_at") else None,
        "recurrenceRule": row.get("recurrence_rule"),
        "sourceType": row.get("source_type"),
        "sourceRef": row.get("source_ref"),
        "confidence": float(row.get("confidence")) if isinstance(row.get("confidence"), (float, int, Decimal)) else None,
        "provenanceSummary": row.get("provenance_summary"),
        "createdAt": row.get("created_at").isoformat() if row.get("created_at") else None,
        "updatedAt": row.get("updated_at").isoformat() if row.get("updated_at") else None,
        "completedAt": row.get("completed_at").isoformat() if row.get("completed_at") else None,
        "dismissedAt": row.get("dismissed_at").isoformat() if row.get("dismissed_at") else None,
        "cancelledAt": row.get("cancelled_at").isoformat() if row.get("cancelled_at") else None,
    }


def _row_to_candidate(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(row.get("candidate_id") or 0),
        "title": row.get("title"),
        "status": row.get("status"),
        "candidateSubtype": row.get("candidate_subtype"),
        "proposedDueAt": row.get("proposed_due_at").isoformat() if row.get("proposed_due_at") else None,
        "proposedRemindAt": row.get("proposed_remind_at").isoformat() if row.get("proposed_remind_at") else None,
        "provenanceSummary": getCandidateProvenance(row),
        "confidence": getCandidateConfidence(row),
    }


def getCandidateConfidence(candidate: Dict[str, Any]) -> Optional[float]:
    primary = candidate.get("confidence")
    if isinstance(primary, (float, int, Decimal)):
        return float(primary)
    fallback = candidate.get("confidence_score")
    if isinstance(fallback, (float, int, Decimal)):
        return float(fallback)
    return None


def getCandidateProvenance(candidate: Dict[str, Any]) -> Optional[str]:
    primary = candidate.get("provenance_summary")
    if isinstance(primary, str) and primary.strip():
        return primary.strip()
    fallback = candidate.get("provenance")
    if isinstance(fallback, dict):
        text = fallback.get("summary") or fallback.get("provenance_summary") or fallback.get("message_hint")
        if isinstance(text, str) and text.strip():
            return text.strip()
    return None


def _candidate_has_direct_user_expression(candidate: Dict[str, Any]) -> bool:
    provenance = candidate.get("provenance") if isinstance(candidate.get("provenance"), dict) else {}
    metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
    # Guardrail: do not add keyword-based interpretation in service layer.
    # This function only reads structured extraction fields produced upstream.
    return bool(
        metadata.get("has_direct_user_expression")
        or provenance.get("has_direct_user_expression")
        or metadata.get("source_has_direct_user_expression")
        or provenance.get("source_has_direct_user_expression")
    )


def _candidate_has_high_risk_flags(candidate: Dict[str, Any]) -> bool:
    provenance = candidate.get("provenance") if isinstance(candidate.get("provenance"), dict) else {}
    metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
    risk_tags = metadata.get("risk_tags") or provenance.get("risk_tags") or []
    if not isinstance(risk_tags, list):
        risk_tags = []
    normalized = {str(tag).strip().lower() for tag in risk_tags}
    blocked_tag = bool(normalized.intersection({"financial", "deletion", "cancellation", "irreversible"}))
    blocked_flags = any(
        bool(container.get(key))
        for container in (metadata, provenance)
        for key in (
            "has_financial_action",
            "has_deletion_action",
            "has_cancellation_action",
            "has_irreversible_action",
            "is_high_risk_action",
        )
    )
    return blocked_tag or blocked_flags


def shouldAutoPromoteCandidate(candidate: Dict[str, Any]) -> bool:
    # Guardrail: no semantic/keyword inference here. Only structured fields from extraction/LLM lanes.
    confidence = getCandidateConfidence(candidate) or 0.0
    subtype = (candidate.get("candidate_subtype") or "").strip().lower()
    if confidence < 0.85:
        return False
    if subtype not in {"todo", "reminder"}:
        return False
    if subtype == "calendar_event":
        return False
    if _candidate_has_high_risk_flags(candidate):
        return False
    if not _candidate_has_direct_user_expression(candidate):
        return False
    return True


async def _write_audit(
    conn,
    *,
    tenant_id: str,
    user_id: str,
    object_type: str,
    object_id: str,
    action: str,
    old_value: Optional[Dict[str, Any]],
    new_value: Optional[Dict[str, Any]],
    actor: str,
    reason: Optional[str] = None,
    provenance_summary: Optional[str] = None,
) -> None:
    old_value_safe = _json_safe(old_value)
    new_value_safe = _json_safe(new_value)
    await conn.execute(
        """
        INSERT INTO action_audit_log (
          tenant_id, user_id, object_type, object_id, action, old_value, new_value, actor, reason, provenance_summary
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10)
        """,
        tenant_id,
        user_id,
        object_type,
        object_id,
        action,
        old_value_safe,
        new_value_safe,
        actor,
        reason,
        provenance_summary,
    )


async def createActionItem(db: Database, input_data: Dict[str, Any]) -> Dict[str, Any]:
    tenant_id = input_data.get("tenantId") or "default"
    user_id = input_data.get("userId")
    if not user_id:
        raise ActionStateError(400, "userId is required")

    kind = (input_data.get("kind") or "").strip().lower()
    if kind not in ACTION_ITEM_KINDS:
        raise ActionStateError(400, f"Invalid action item kind: {kind}")

    title = (input_data.get("title") or "").strip()
    if not title:
        raise ActionStateError(400, "title is required")

    status = (input_data.get("status") or "pending").strip().lower()
    if status not in ACTION_ITEM_STATUSES:
        raise ActionStateError(400, f"Invalid action item status: {status}")

    source_type = (input_data.get("sourceType") or "direct_user").strip().lower()
    if source_type not in ACTION_ITEM_SOURCE_TYPES:
        raise ActionStateError(400, f"Invalid sourceType: {source_type}")

    due_at = _ensure_tz(input_data.get("dueAt"))
    remind_at = _ensure_tz(input_data.get("remindAt"))

    pool = await db.get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                INSERT INTO action_items (
                  tenant_id, user_id, kind, title, notes, status, due_at, remind_at,
                  recurrence_rule, source_type, source_ref, confidence, provenance_summary
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13)
                RETURNING *
                """,
                tenant_id,
                user_id,
                kind,
                title,
                input_data.get("notes"),
                status,
                due_at,
                remind_at,
                input_data.get("recurrenceRule"),
                source_type,
                input_data.get("sourceRef"),
                input_data.get("confidence"),
                input_data.get("provenanceSummary"),
            )
            if not row:
                raise ActionStateError(500, "Failed to create action item")
            item = dict(row)
            await _write_audit(
                conn,
                tenant_id=tenant_id,
                user_id=user_id,
                object_type="action_item",
                object_id=str(item["id"]),
                action="create",
                old_value=None,
                new_value=item,
                actor="user" if source_type == "direct_user" else "sophie",
                provenance_summary=item.get("provenance_summary"),
            )
            await _link_matching_candidate_to_action_item(conn, item=item)
    return _row_to_action_item(item)


async def listActionItems(db: Database, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    tenant_id = filters.get("tenantId") or "default"
    user_id = filters.get("userId")
    if not user_id:
        raise ActionStateError(400, "userId is required")

    where = ["tenant_id=$1", "user_id=$2"]
    params: List[Any] = [tenant_id, user_id]

    status = (filters.get("status") or "").strip().lower() or None
    if status == "active":
        status = "pending"
    if status:
        if status not in ACTION_ITEM_STATUSES:
            raise ActionStateError(400, f"Invalid action item status: {status}")
        where.append(f"status=${len(params) + 1}")
        params.append(status)
    else:
        include_done = bool(filters.get("include_done"))
        if not include_done:
            where.append("status='pending'")

    kind = (filters.get("kind") or "").strip().lower() or None
    if kind:
        if kind not in ACTION_ITEM_KINDS:
            raise ActionStateError(400, f"Invalid action item kind: {kind}")
        where.append(f"kind=${len(params) + 1}")
        params.append(kind)

    title = (filters.get("title") or "").strip() or None
    if title:
        where.append(f"title ILIKE ${len(params) + 1}")
        params.append(f"%{title}%")

    date_from = _ensure_tz(filters.get("date_from"))
    if date_from:
        where.append(f"(due_at >= ${len(params) + 1} OR remind_at >= ${len(params) + 1})")
        params.append(date_from)

    date_to = _ensure_tz(filters.get("date_to"))
    if date_to:
        where.append(f"(due_at <= ${len(params) + 1} OR remind_at <= ${len(params) + 1})")
        params.append(date_to)

    limit = int(filters.get("limit") or 100)
    offset = int(filters.get("offset") or 0)
    limit = max(1, min(limit, 500))
    offset = max(0, offset)

    rows = await db.fetch(
        f"""
        SELECT *
        FROM action_items
        WHERE {' AND '.join(where)}
        ORDER BY COALESCE(due_at, remind_at, created_at) ASC, created_at DESC
        LIMIT ${len(params) + 1}
        OFFSET ${len(params) + 2}
        """,
        *params,
        limit,
        offset,
    )
    return [_row_to_action_item(r) for r in rows]


async def updateActionItem(
    db: Database,
    *,
    tenant_id: str,
    user_id: str,
    action_item_id: str,
    patch: Dict[str, Any],
    actor: str = "user",
    reason: Optional[str] = None,
) -> Dict[str, Any]:
    allowed_actors = {"user", "sophie"}
    if actor not in allowed_actors:
        raise ActionStateError(400, f"Invalid actor: {actor}")

    editable_fields = {
        "title": "title",
        "notes": "notes",
        "kind": "kind",
        "dueAt": "due_at",
        "remindAt": "remind_at",
        "recurrenceRule": "recurrence_rule",
        "status": "status",
    }
    incoming = {k: v for k, v in (patch or {}).items() if k in editable_fields}
    if not incoming:
        raise ActionStateError(400, "No editable fields provided")

    pool = await db.get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            current = await conn.fetchrow(
                "SELECT * FROM action_items WHERE tenant_id=$1 AND user_id=$2 AND id=$3::uuid",
                tenant_id,
                user_id,
                action_item_id,
            )
            if not current:
                raise ActionStateError(404, "Action item not found")
            current_row = dict(current)
            next_row = dict(current_row)

            if "title" in incoming:
                title = (incoming.get("title") or "").strip()
                if not title:
                    raise ActionStateError(400, "title cannot be empty")
                next_row["title"] = title
            if "notes" in incoming:
                next_row["notes"] = incoming.get("notes")
            if "kind" in incoming:
                kind = (incoming.get("kind") or "").strip().lower()
                if kind not in ACTION_ITEM_KINDS:
                    raise ActionStateError(400, f"Invalid action item kind: {kind}")
                next_row["kind"] = kind
            if "dueAt" in incoming:
                next_row["due_at"] = _ensure_tz(incoming.get("dueAt"))
            if "remindAt" in incoming:
                next_row["remind_at"] = _ensure_tz(incoming.get("remindAt"))
            if "recurrenceRule" in incoming:
                next_row["recurrence_rule"] = incoming.get("recurrenceRule")

            current_status = (current_row.get("status") or "").strip().lower()
            next_status = current_status
            if "status" in incoming and incoming.get("status") is not None:
                next_status = str(incoming.get("status")).strip().lower()
                if next_status not in ACTION_ITEM_STATUSES:
                    raise ActionStateError(400, f"Invalid action item status: {next_status}")
                if next_status != current_status:
                    allowed = ACTION_ITEM_ALLOWED_TRANSITIONS.get(current_status, set())
                    if next_status not in allowed:
                        raise ActionStateError(409, f"Invalid action item status transition: {current_status} -> {next_status}")

            completed_at = current_row.get("completed_at")
            cancelled_at = current_row.get("cancelled_at")
            dismissed_at = current_row.get("dismissed_at")
            if next_status != current_status:
                now = datetime.now(timezone.utc)
                if next_status == "done":
                    completed_at = now
                elif next_status == "cancelled":
                    cancelled_at = now
                elif next_status == "dismissed":
                    dismissed_at = now

            updated = await conn.fetchrow(
                """
                UPDATE action_items
                SET title=$4,
                    notes=$5,
                    kind=$6,
                    due_at=$7,
                    remind_at=$8,
                    recurrence_rule=$9,
                    status=$10,
                    completed_at=$11,
                    cancelled_at=$12,
                    dismissed_at=$13,
                    updated_at=NOW()
                WHERE tenant_id=$1 AND user_id=$2 AND id=$3::uuid
                RETURNING *
                """,
                tenant_id,
                user_id,
                action_item_id,
                next_row.get("title"),
                next_row.get("notes"),
                next_row.get("kind"),
                next_row.get("due_at"),
                next_row.get("remind_at"),
                next_row.get("recurrence_rule"),
                next_status,
                completed_at,
                cancelled_at,
                dismissed_at,
            )
            updated_row = dict(updated)
            action_name = "update"
            if next_status != current_status:
                action_name = f"status_change:{current_status}->{next_status}"
            await _write_audit(
                conn,
                tenant_id=tenant_id,
                user_id=user_id,
                object_type="action_item",
                object_id=str(action_item_id),
                action=action_name,
                old_value=current_row,
                new_value=updated_row,
                actor=actor,
                reason=reason,
                provenance_summary=updated_row.get("provenance_summary"),
            )
    return _row_to_action_item(updated_row)


async def updateActionItemStatus(
    db: Database,
    *,
    tenant_id: str,
    user_id: str,
    action_item_id: str,
    status: str,
    reason: Optional[str] = None,
    actor: str = "sophie",
) -> Dict[str, Any]:
    next_status = (status or "").strip().lower()
    if next_status not in ACTION_ITEM_STATUSES:
        raise ActionStateError(400, f"Invalid action item status: {next_status}")

    pool = await db.get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            current = await conn.fetchrow(
                "SELECT * FROM action_items WHERE tenant_id=$1 AND user_id=$2 AND id=$3::uuid",
                tenant_id,
                user_id,
                action_item_id,
            )
            if not current:
                raise ActionStateError(404, "Action item not found")
            current_row = dict(current)
            current_status = current_row.get("status")
            if next_status == current_status:
                return _row_to_action_item(current_row)
            allowed = ACTION_ITEM_ALLOWED_TRANSITIONS.get(current_status, set())
            if next_status not in allowed:
                raise ActionStateError(409, f"Invalid action item status transition: {current_status} -> {next_status}")

            completed_at = current_row.get("completed_at")
            cancelled_at = current_row.get("cancelled_at")
            dismissed_at = current_row.get("dismissed_at")
            now = datetime.now(timezone.utc)
            if next_status == "done":
                completed_at = now
            elif next_status == "cancelled":
                cancelled_at = now
            elif next_status == "dismissed":
                dismissed_at = now

            updated = await conn.fetchrow(
                """
                UPDATE action_items
                SET status=$4, updated_at=NOW(), completed_at=$5, cancelled_at=$6, dismissed_at=$7
                WHERE tenant_id=$1 AND user_id=$2 AND id=$3::uuid
                RETURNING *
                """,
                tenant_id,
                user_id,
                action_item_id,
                next_status,
                completed_at,
                cancelled_at,
                dismissed_at,
            )
            updated_row = dict(updated)
            await _write_audit(
                conn,
                tenant_id=tenant_id,
                user_id=user_id,
                object_type="action_item",
                object_id=str(action_item_id),
                action=f"status_change:{current_status}->{next_status}",
                old_value=current_row,
                new_value=updated_row,
                actor=actor,
                reason=reason,
                provenance_summary=updated_row.get("provenance_summary"),
            )
    return _row_to_action_item(updated_row)


async def markActionItemDone(db: Database, *, tenant_id: str, user_id: str, action_item_id: str, reason: Optional[str] = None) -> Dict[str, Any]:
    return await updateActionItemStatus(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        action_item_id=action_item_id,
        status="done",
        reason=reason,
        actor="sophie",
    )


async def dismissActionItem(db: Database, *, tenant_id: str, user_id: str, action_item_id: str, reason: Optional[str] = None) -> Dict[str, Any]:
    return await updateActionItemStatus(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        action_item_id=action_item_id,
        status="dismissed",
        reason=reason,
        actor="sophie",
    )


async def cancelActionItem(db: Database, *, tenant_id: str, user_id: str, action_item_id: str, reason: Optional[str] = None) -> Dict[str, Any]:
    return await updateActionItemStatus(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        action_item_id=action_item_id,
        status="cancelled",
        reason=reason,
        actor="sophie",
    )


async def archiveActionItem(db: Database, *, tenant_id: str, user_id: str, action_item_id: str, reason: Optional[str] = None, actor: str = "sophie") -> Dict[str, Any]:
    return await updateActionItemStatus(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        action_item_id=action_item_id,
        status="archived",
        reason=reason,
        actor=actor,
    )


def _candidate_to_kind(candidate_subtype: str, override_kind: Optional[str]) -> str:
    if override_kind:
        value = override_kind.strip().lower()
        if value not in ACTION_ITEM_KINDS:
            raise ActionStateError(400, f"Invalid overrides.kind: {value}")
        return value

    subtype = (candidate_subtype or "").strip().lower()
    if subtype in {"todo", "reminder", "habit"}:
        return subtype
    if subtype == "calendar_event":
        raise ActionStateError(409, "calendar_event candidate requires overrides.kind until calendar integration is implemented")
    if subtype in {"follow_up", "waiting_on", "nudge"}:
        return "todo"
    return "todo"


async def promoteCandidateToActionItem(
    db: Database,
    *,
    tenant_id: str,
    user_id: str,
    candidate_id: int,
    overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    overrides = overrides or {}

    pool = await db.get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            candidate = await conn.fetchrow(
                """
                SELECT *
                FROM actionable_candidates
                WHERE tenant_id=$1 AND user_id=$2 AND candidate_id=$3
                """,
                tenant_id,
                user_id,
                candidate_id,
            )
            if not candidate:
                raise ActionStateError(404, "Action candidate not found")
            candidate_row = dict(candidate)

            current_status = (candidate_row.get("status") or "").strip().lower()
            if current_status not in ACTION_CANDIDATE_ALLOWED_TRANSITIONS:
                raise ActionStateError(409, f"Candidate status {current_status} is not promotable")
            if "confirmed" not in ACTION_CANDIDATE_ALLOWED_TRANSITIONS.get(current_status, set()):
                raise ActionStateError(409, f"Invalid candidate status transition: {current_status} -> confirmed")

            kind = _candidate_to_kind(candidate_row.get("candidate_subtype") or "", overrides.get("kind"))
            title = (overrides.get("title") or candidate_row.get("title") or "").strip()
            if not title:
                raise ActionStateError(400, "Promotion requires title")

            due_at = _ensure_tz(overrides.get("dueAt")) or candidate_row.get("proposed_due_at") or candidate_row.get("due_iso")
            remind_at = _ensure_tz(overrides.get("remindAt")) or candidate_row.get("proposed_remind_at")
            source_ref = {
                "candidateId": candidate_id,
                "candidateKey": candidate_row.get("candidate_key"),
            }

            created = await conn.fetchrow(
                """
                INSERT INTO action_items (
                  tenant_id, user_id, kind, title, notes, status, due_at, remind_at,
                  recurrence_rule, source_type, source_ref, confidence, provenance_summary
                ) VALUES ($1,$2,$3,$4,$5,'pending',$6,$7,$8,'candidate_promotion',$9::jsonb,$10,$11)
                RETURNING *
                """,
                tenant_id,
                user_id,
                kind,
                title,
                overrides.get("notes") or candidate_row.get("summary"),
                due_at,
                remind_at,
                overrides.get("recurrenceRule") or candidate_row.get("cadence_text"),
                source_ref,
                overrides.get("confidence") if overrides.get("confidence") is not None else getCandidateConfidence(candidate_row),
                overrides.get("provenanceSummary") or getCandidateProvenance(candidate_row),
            )
            created_row = dict(created)

            updated_candidate = await conn.fetchrow(
                """
                UPDATE actionable_candidates
                SET status='confirmed',
                    promoted_action_item_id=$4::uuid,
                    updated_at=NOW(),
                    confidence=COALESCE(confidence, confidence_score),
                    provenance_summary=COALESCE(provenance_summary, summary)
                WHERE tenant_id=$1 AND user_id=$2 AND candidate_id=$3
                RETURNING *
                """,
                tenant_id,
                user_id,
                candidate_id,
                str(created_row["id"]),
            )
            updated_candidate_row = dict(updated_candidate)

            await _write_audit(
                conn,
                tenant_id=tenant_id,
                user_id=user_id,
                object_type="action_item",
                object_id=str(created_row["id"]),
                action="promoted_from_candidate",
                old_value=None,
                new_value=created_row,
                actor="sophie",
                provenance_summary=created_row.get("provenance_summary"),
            )
            await _write_audit(
                conn,
                tenant_id=tenant_id,
                user_id=user_id,
                object_type="action_candidate",
                object_id=str(candidate_id),
                action=f"status_change:{current_status}->confirmed",
                old_value=candidate_row,
                new_value=updated_candidate_row,
                actor="sophie",
                reason="promoted_to_action_item",
                provenance_summary=updated_candidate_row.get("provenance_summary"),
            )

    return {
        "actionItem": _row_to_action_item(created_row),
        "candidate": _row_to_candidate(updated_candidate_row),
    }


async def maybeAutoPromoteCandidate(
    db: Database,
    *,
    tenant_id: str,
    user_id: str,
    candidate_id: int,
) -> Dict[str, Any]:
    pool = await db.get_pool()
    async with pool.acquire() as conn:
        candidate = await conn.fetchrow(
            """
            SELECT *
            FROM actionable_candidates
            WHERE tenant_id=$1 AND user_id=$2 AND candidate_id=$3
            """,
            tenant_id,
            user_id,
            candidate_id,
        )
    if not candidate:
        raise ActionStateError(404, "Action candidate not found")
    candidate_row = dict(candidate)
    if (candidate_row.get("status") or "").strip().lower() not in ACTION_CANDIDATE_PENDING_STATUSES:
        return {"autoPromoted": False, "reason": "status_not_pending", "candidateId": candidate_id}
    if not shouldAutoPromoteCandidate(candidate_row):
        return {"autoPromoted": False, "reason": "policy_not_met", "candidateId": candidate_id}
    result = await promoteCandidateToActionItem(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        candidate_id=candidate_id,
        overrides=None,
    )
    pool = await db.get_pool()
    async with pool.acquire() as conn:
        await _write_audit(
            conn,
            tenant_id=tenant_id,
            user_id=user_id,
            object_type="action_candidate",
            object_id=str(candidate_id),
            action="auto_promote",
            old_value=None,
            new_value={"promotedActionItemId": result["actionItem"]["id"]},
            actor="system_auto_promote",
            reason="high_confidence_low_risk",
            provenance_summary=getCandidateProvenance(candidate_row),
        )
    return {"autoPromoted": True, "reason": "high_confidence_low_risk", **result}


async def listDailyAgenda(
    db: Database,
    *,
    tenant_id: str,
    user_id: str,
    day: str,
    timezone_name: str,
) -> Dict[str, Any]:
    tz = ZoneInfo(timezone_name)
    target_date = _parse_date(day)
    start_local = datetime.combine(target_date, time.min).replace(tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(timezone.utc)
    end_utc = end_local.astimezone(timezone.utc)

    item_rows = await db.fetch(
        """
        SELECT *
        FROM action_items
        WHERE tenant_id=$1 AND user_id=$2 AND status='pending'
        """,
        tenant_id,
        user_id,
    )

    due_today: List[Dict[str, Any]] = []
    reminders_today: List[Dict[str, Any]] = []
    overdue: List[Dict[str, Any]] = []

    for row in item_rows:
        due_at = row.get("due_at")
        remind_at = row.get("remind_at")
        kind = (row.get("kind") or "").lower()

        if kind in {"todo", "habit"} and isinstance(due_at, datetime) and start_utc <= due_at < end_utc:
            due_today.append(_row_to_action_item(row))

        if kind in {"reminder", "todo"} and isinstance(remind_at, datetime) and start_utc <= remind_at < end_utc:
            reminders_today.append(_row_to_action_item(row))

        if isinstance(due_at, datetime) and due_at < start_utc:
            overdue.append(_row_to_action_item(row))

    candidate_rows = await db.fetch(
        """
        SELECT *
        FROM actionable_candidates
        WHERE tenant_id=$1
          AND user_id=$2
          AND status = ANY($3::text[])
          AND candidate_subtype IN ('todo','reminder','calendar_event','habit','follow_up','waiting_on','nudge')
        ORDER BY COALESCE(proposed_due_at, due_iso, updated_at) ASC, candidate_id ASC
        """,
        tenant_id,
        user_id,
        list(ACTION_CANDIDATE_PENDING_STATUSES),
    )

    pending_candidates = [_row_to_candidate(r) for r in candidate_rows]

    return {
        "date": target_date.isoformat(),
        "timezone": timezone_name,
        "dueToday": due_today,
        "remindersToday": reminders_today,
        "overdue": overdue,
        "pendingCandidates": pending_candidates,
        "counts": {
            "dueToday": len(due_today),
            "remindersToday": len(reminders_today),
            "overdue": len(overdue),
            "pendingCandidates": len(pending_candidates),
        },
        "suggestedActions": _derive_suggested_actions(
            due_today=due_today,
            reminders_today=reminders_today,
            overdue=overdue,
            pending_candidates=candidate_rows,
        ),
    }


def _derive_suggested_actions(
    *,
    due_today: List[Dict[str, Any]],
    reminders_today: List[Dict[str, Any]],
    overdue: List[Dict[str, Any]],
    pending_candidates: List[Dict[str, Any]],
) -> List[str]:
    suggestions: List[str] = []
    if any((row.get("kind") or "").lower() == "todo" for row in overdue):
        suggestions.append("follow_up")
    if any((getCandidateConfidence(row) or 0.0) >= 0.85 for row in pending_candidates):
        suggestions.append("confirm_or_auto")
    if not due_today and not reminders_today and not overdue:
        suggestions.append("light_planning_prompt")
    return suggestions
