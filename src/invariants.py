from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone as dt_timezone
import json
from typing import Any, Dict, List, Optional

from .canonicalization import normalize_text, stable_short_hash
from .db import Database

INVARIANT_RUNNER_VERSION = "t13.invariants.v1"


@dataclass(frozen=True)
class InvariantViolationCandidate:
    invariant_code: str
    severity: str
    tenant_id: str
    user_id: Optional[str]
    object_type: str
    object_id: str
    details: Dict[str, Any]
    requires_human_review: bool


@dataclass(frozen=True)
class InvariantRunSummary:
    detected: int
    persisted: int
    auto_repaired: int
    review_required: int
    failed_repairs: int


def _utc_now() -> datetime:
    return datetime.utcnow().replace(tzinfo=dt_timezone.utc)


def _n(value: Any) -> str:
    return normalize_text(value, casefold=False)


def _fingerprint(*, invariant_code: str, tenant_id: str, object_type: str, object_id: str) -> str:
    payload = json.dumps(
        {
            "invariant_code": _n(invariant_code),
            "tenant_id": _n(tenant_id),
            "object_type": _n(object_type),
            "object_id": _n(object_id),
        },
        sort_keys=True,
        ensure_ascii=True,
    )
    return stable_short_hash(payload)


class InvariantManager:
    def __init__(self, db: Database):
        self.db = db

    async def run_cycle(self, *, auto_repair_enabled: bool = True, max_rows_per_invariant: int = 200) -> InvariantRunSummary:
        candidates = await self.detect_all(max_rows_per_invariant=max_rows_per_invariant)
        persisted = 0
        auto_repaired = 0
        review_required = 0
        failed_repairs = 0
        for candidate in candidates:
            violation_id, status = await self._persist_violation(candidate)
            persisted += 1
            if status == "review_required":
                review_required += 1
                continue
            if auto_repair_enabled and self._is_auto_repairable(candidate.invariant_code):
                repair = await self._apply_safe_auto_repair(candidate=candidate, violation_id=violation_id)
                if repair == "applied":
                    auto_repaired += 1
                elif repair == "failed":
                    failed_repairs += 1
            elif candidate.requires_human_review:
                review_required += 1

        return InvariantRunSummary(
            detected=len(candidates),
            persisted=persisted,
            auto_repaired=auto_repaired,
            review_required=review_required,
            failed_repairs=failed_repairs,
        )

    async def detect_all(self, *, max_rows_per_invariant: int = 200) -> List[InvariantViolationCandidate]:
        out: List[InvariantViolationCandidate] = []
        out.extend(await self._detect_factual_without_evidence(limit=max_rows_per_invariant))
        out.extend(await self._detect_invalid_lifecycle_state(limit=max_rows_per_invariant))
        out.extend(await self._detect_duplicate_active_exclusive_slot(limit=max_rows_per_invariant))
        out.extend(await self._detect_tenant_user_session_integrity(limit=max_rows_per_invariant))
        out.extend(await self._detect_canonical_mutation_watermark_consistency(limit=max_rows_per_invariant))
        out.extend(await self._detect_assistant_authored_canonical_claims(limit=max_rows_per_invariant))
        out.extend(await self._detect_invalid_merge_lineage(limit=max_rows_per_invariant))
        return out

    def _is_auto_repairable(self, invariant_code: str) -> bool:
        # Auto-repair is intentionally restricted to unambiguous structural consistency classes.
        return _n(invariant_code) in {"canonical_mutation_watermark_inconsistent"}

    async def _persist_violation(self, candidate: InvariantViolationCandidate) -> tuple[int, str]:
        now = _utc_now()
        fp = _fingerprint(
            invariant_code=candidate.invariant_code,
            tenant_id=candidate.tenant_id,
            object_type=candidate.object_type,
            object_id=candidate.object_id,
        )
        existing = await self.db.fetchone(
            """
            SELECT violation_id, status
            FROM invariant_violations
            WHERE fingerprint = $1
              AND status IN ('open', 'review_required')
            ORDER BY violation_id DESC
            LIMIT 1
            """,
            fp,
        )
        if existing:
            await self.db.execute(
                """
                UPDATE invariant_violations
                SET
                  severity = $2,
                  details = $3::jsonb,
                  last_detected_at = $4,
                  occurrence_count = occurrence_count + 1,
                  updated_at = $4
                WHERE violation_id = $1
                """,
                int(existing["violation_id"]),
                candidate.severity,
                candidate.details,
                now,
            )
            return int(existing["violation_id"]), _n(existing.get("status")) or "open"

        status = "review_required" if candidate.requires_human_review else "open"
        violation_id = await self.db.fetchval(
            """
            INSERT INTO invariant_violations (
                invariant_code,
                severity,
                tenant_id,
                user_id,
                object_type,
                object_id,
                details,
                status,
                requires_human_review,
                first_detected_at,
                last_detected_at,
                occurrence_count,
                fingerprint,
                detected_by,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $10, 1, $11, $12, $10
            )
            RETURNING violation_id
            """,
            candidate.invariant_code,
            candidate.severity,
            candidate.tenant_id,
            candidate.user_id,
            candidate.object_type,
            candidate.object_id,
            candidate.details,
            status,
            bool(candidate.requires_human_review),
            now,
            fp,
            INVARIANT_RUNNER_VERSION,
        )
        return int(violation_id), status

    async def _apply_safe_auto_repair(self, *, candidate: InvariantViolationCandidate, violation_id: int) -> str:
        if _n(candidate.invariant_code) != "canonical_mutation_watermark_inconsistent":
            await self._log_repair_action(
                violation_id=violation_id,
                tenant_id=candidate.tenant_id,
                action_code="auto_repair_not_supported",
                action_mode="auto",
                status="blocked",
                details={"invariant_code": candidate.invariant_code},
            )
            return "blocked"

        expected = candidate.details.get("expected_last_sequence")
        try:
            expected_seq = int(expected)
        except Exception:
            await self._log_repair_action(
                violation_id=violation_id,
                tenant_id=candidate.tenant_id,
                action_code="repair_watermark_to_max_mutation_sequence",
                action_mode="auto",
                status="failed",
                details={"error": "missing expected_last_sequence"},
            )
            return "failed"

        now = _utc_now()
        try:
            await self.db.execute(
                """
                INSERT INTO canonical_tenant_watermarks (tenant_id, last_sequence, updated_at)
                VALUES ($1, $2, $3)
                ON CONFLICT (tenant_id)
                DO UPDATE SET
                  last_sequence = EXCLUDED.last_sequence,
                  updated_at = EXCLUDED.updated_at
                """,
                candidate.tenant_id,
                expected_seq,
                now,
            )
            await self.db.execute(
                """
                UPDATE invariant_violations
                SET status = 'auto_repaired', repaired_at = $2, updated_at = $2
                WHERE violation_id = $1
                """,
                violation_id,
                now,
            )
            await self._log_repair_action(
                violation_id=violation_id,
                tenant_id=candidate.tenant_id,
                action_code="repair_watermark_to_max_mutation_sequence",
                action_mode="auto",
                status="applied",
                details={"set_last_sequence": expected_seq},
            )
            return "applied"
        except Exception as e:
            await self._log_repair_action(
                violation_id=violation_id,
                tenant_id=candidate.tenant_id,
                action_code="repair_watermark_to_max_mutation_sequence",
                action_mode="auto",
                status="failed",
                details={"error": str(e)},
            )
            return "failed"

    async def _log_repair_action(
        self,
        *,
        violation_id: int,
        tenant_id: str,
        action_code: str,
        action_mode: str,
        status: str,
        details: Dict[str, Any],
    ) -> None:
        now = _utc_now()
        applied_at = now if status == "applied" else None
        await self.db.execute(
            """
            INSERT INTO invariant_repair_actions (
                violation_id,
                tenant_id,
                action_code,
                action_mode,
                status,
                details,
                created_at,
                applied_at
            )
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
            """,
            violation_id,
            tenant_id,
            action_code,
            action_mode,
            status,
            details,
            now,
            applied_at,
        )

    async def _detect_factual_without_evidence(self, *, limit: int) -> List[InvariantViolationCandidate]:
        rows = await self.db.fetch(
            """
            SELECT
              c.tenant_id,
              c.user_id,
              c.claim_id,
              c.claim_event_key,
              c.predicate,
              c.lifecycle_status
            FROM claims c
            LEFT JOIN claim_evidence ce
              ON ce.tenant_id = c.tenant_id
             AND ce.claim_id = c.claim_id
            WHERE c.lifecycle_status = 'active'
            GROUP BY c.tenant_id, c.user_id, c.claim_id, c.claim_event_key, c.predicate, c.lifecycle_status
            HAVING COUNT(ce.claim_evidence_id) = 0
            ORDER BY c.claim_id ASC
            LIMIT $1
            """,
            max(1, int(limit or 200)),
        )
        out: List[InvariantViolationCandidate] = []
        for row in rows or []:
            out.append(
                InvariantViolationCandidate(
                    invariant_code="factual_claim_missing_evidence",
                    severity="high",
                    tenant_id=_n(row.get("tenant_id")),
                    user_id=_n(row.get("user_id")) or None,
                    object_type="claim",
                    object_id=str(int(row.get("claim_id"))),
                    details={
                        "claim_event_key": _n(row.get("claim_event_key")) or None,
                        "predicate": _n(row.get("predicate")) or None,
                        "lifecycle_status": _n(row.get("lifecycle_status")) or None,
                    },
                    requires_human_review=True,
                )
            )
        return out

    async def _detect_invalid_lifecycle_state(self, *, limit: int) -> List[InvariantViolationCandidate]:
        rows = await self.db.fetch(
            """
            SELECT
              tenant_id,
              user_id,
              claim_id,
              claim_event_key,
              lifecycle_status,
              superseded_by_claim_id,
              retracted_at
            FROM claims
            WHERE
              (lifecycle_status = 'active' AND (superseded_by_claim_id IS NOT NULL OR retracted_at IS NOT NULL))
              OR (lifecycle_status = 'superseded' AND superseded_by_claim_id IS NULL)
              OR (lifecycle_status = 'retracted' AND retracted_at IS NULL)
            ORDER BY claim_id ASC
            LIMIT $1
            """,
            max(1, int(limit or 200)),
        )
        out: List[InvariantViolationCandidate] = []
        for row in rows or []:
            out.append(
                InvariantViolationCandidate(
                    invariant_code="invalid_claim_lifecycle_transition",
                    severity="high",
                    tenant_id=_n(row.get("tenant_id")),
                    user_id=_n(row.get("user_id")) or None,
                    object_type="claim",
                    object_id=str(int(row.get("claim_id"))),
                    details={
                        "claim_event_key": _n(row.get("claim_event_key")) or None,
                        "lifecycle_status": _n(row.get("lifecycle_status")) or None,
                        "superseded_by_claim_id": row.get("superseded_by_claim_id"),
                        "retracted_at": _n(row.get("retracted_at")) or None,
                    },
                    requires_human_review=True,
                )
            )
        return out

    async def _detect_duplicate_active_exclusive_slot(self, *, limit: int) -> List[InvariantViolationCandidate]:
        rows = await self.db.fetch(
            """
            SELECT
              c.tenant_id,
              c.user_id,
              c.claim_slot_key,
              c.predicate,
              COUNT(*)::int AS active_claim_count,
              ARRAY_AGG(c.claim_id ORDER BY c.claim_id ASC) AS claim_ids
            FROM claims c
            JOIN predicate_policy p
              ON p.policy_version = c.predicate_policy_version
             AND p.predicate = c.predicate
            WHERE c.lifecycle_status = 'active'
              AND lower(p.cardinality) = 'one'
            GROUP BY c.tenant_id, c.user_id, c.claim_slot_key, c.predicate
            HAVING COUNT(*) > 1
            ORDER BY active_claim_count DESC, c.claim_slot_key ASC
            LIMIT $1
            """,
            max(1, int(limit or 200)),
        )
        out: List[InvariantViolationCandidate] = []
        for row in rows or []:
            slot_key = _n(row.get("claim_slot_key"))
            out.append(
                InvariantViolationCandidate(
                    invariant_code="duplicate_active_exclusive_slot_claims",
                    severity="high",
                    tenant_id=_n(row.get("tenant_id")),
                    user_id=_n(row.get("user_id")) or None,
                    object_type="claim_slot",
                    object_id=slot_key,
                    details={
                        "predicate": _n(row.get("predicate")) or None,
                        "active_claim_count": int(row.get("active_claim_count") or 0),
                        "claim_ids": row.get("claim_ids") if isinstance(row.get("claim_ids"), list) else [],
                    },
                    requires_human_review=True,
                )
            )
        return out

    async def _detect_tenant_user_session_integrity(self, *, limit: int) -> List[InvariantViolationCandidate]:
        rows = await self.db.fetch(
            """
            SELECT
              ce.tenant_id,
              c.user_id,
              ce.claim_evidence_id,
              ce.claim_id,
              ce.session_id,
              ce.turn_id,
              t.turn_id AS matched_turn_id,
              t.user_id AS turn_user_id,
              t.tenant_id AS turn_tenant_id
            FROM claim_evidence ce
            JOIN claims c
              ON c.tenant_id = ce.tenant_id
             AND c.claim_id = ce.claim_id
            LEFT JOIN turns_v2 t
              ON t.tenant_id = ce.tenant_id
             AND t.session_id = ce.session_id
             AND t.turn_id = ce.turn_id
            WHERE t.turn_id IS NULL
               OR t.user_id <> c.user_id
               OR t.tenant_id <> ce.tenant_id
            ORDER BY ce.claim_evidence_id ASC
            LIMIT $1
            """,
            max(1, int(limit or 200)),
        )
        out: List[InvariantViolationCandidate] = []
        for row in rows or []:
            out.append(
                InvariantViolationCandidate(
                    invariant_code="tenant_user_session_integrity_mismatch",
                    severity="high",
                    tenant_id=_n(row.get("tenant_id")),
                    user_id=_n(row.get("user_id")) or None,
                    object_type="claim_evidence",
                    object_id=str(int(row.get("claim_evidence_id"))),
                    details={
                        "claim_id": int(row.get("claim_id") or 0),
                        "session_id": _n(row.get("session_id")) or None,
                        "turn_id": row.get("turn_id"),
                        "matched_turn_id": row.get("matched_turn_id"),
                        "turn_user_id": _n(row.get("turn_user_id")) or None,
                    },
                    requires_human_review=False,
                )
            )
        return out

    async def _detect_canonical_mutation_watermark_consistency(self, *, limit: int) -> List[InvariantViolationCandidate]:
        rows = await self.db.fetch(
            """
            WITH mutation_stats AS (
              SELECT
                tenant_id,
                MAX(tenant_sequence)::bigint AS max_sequence,
                MIN(tenant_sequence)::bigint AS min_sequence,
                COUNT(*)::bigint AS seq_count
              FROM canonical_mutations
              GROUP BY tenant_id
            )
            SELECT
              ms.tenant_id,
              ms.max_sequence,
              ms.min_sequence,
              ms.seq_count,
              w.last_sequence
            FROM mutation_stats ms
            LEFT JOIN canonical_tenant_watermarks w
              ON w.tenant_id = ms.tenant_id
            WHERE
              w.last_sequence IS NULL
              OR w.last_sequence <> ms.max_sequence
              OR (ms.min_sequence IS NOT NULL AND ms.seq_count <> (ms.max_sequence - ms.min_sequence + 1))
            ORDER BY ms.tenant_id ASC
            LIMIT $1
            """,
            max(1, int(limit or 200)),
        )
        out: List[InvariantViolationCandidate] = []
        for row in rows or []:
            tenant_id = _n(row.get("tenant_id"))
            out.append(
                InvariantViolationCandidate(
                    invariant_code="canonical_mutation_watermark_inconsistent",
                    severity="medium",
                    tenant_id=tenant_id,
                    user_id=None,
                    object_type="canonical_tenant_watermark",
                    object_id=tenant_id,
                    details={
                        "expected_last_sequence": int(row.get("max_sequence") or 0),
                        "observed_last_sequence": (
                            int(row.get("last_sequence"))
                            if row.get("last_sequence") is not None
                            else None
                        ),
                        "sequence_min": int(row.get("min_sequence") or 0),
                        "sequence_count": int(row.get("seq_count") or 0),
                    },
                    requires_human_review=False,
                )
            )
        return out

    async def _detect_assistant_authored_canonical_claims(self, *, limit: int) -> List[InvariantViolationCandidate]:
        rows = await self.db.fetch(
            """
            SELECT
              tenant_id,
              user_id,
              claim_id,
              claim_event_key,
              metadata
            FROM claims
            WHERE lower(COALESCE(metadata->>'source_role', metadata->>'author_role', '')) = 'assistant'
              AND lower(COALESCE(metadata->>'assistant_allowed', 'false')) NOT IN ('true', '1', 'yes')
            ORDER BY claim_id ASC
            LIMIT $1
            """,
            max(1, int(limit or 200)),
        )
        out: List[InvariantViolationCandidate] = []
        for row in rows or []:
            metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            out.append(
                InvariantViolationCandidate(
                    invariant_code="assistant_authored_claim_without_policy",
                    severity="high",
                    tenant_id=_n(row.get("tenant_id")),
                    user_id=_n(row.get("user_id")) or None,
                    object_type="claim",
                    object_id=str(int(row.get("claim_id"))),
                    details={
                        "claim_event_key": _n(row.get("claim_event_key")) or None,
                        "source_role": _n(metadata.get("source_role") or metadata.get("author_role")) or "assistant",
                    },
                    requires_human_review=True,
                )
            )
        return out

    async def _detect_invalid_merge_lineage(self, *, limit: int) -> List[InvariantViolationCandidate]:
        rows = await self.db.fetch(
            """
            SELECT
              e.tenant_id,
              e.user_id,
              e.entity_id::text AS entity_id,
              e.status,
              e.merged_into_entity_id::text AS merged_into_entity_id,
              target.status AS target_status,
              target.entity_id::text AS target_entity_id
            FROM entities e
            LEFT JOIN entities target
              ON target.tenant_id = e.tenant_id
             AND target.entity_id = e.merged_into_entity_id
            WHERE
              (e.status = 'merged' AND e.merged_into_entity_id IS NULL)
              OR (e.status <> 'merged' AND e.merged_into_entity_id IS NOT NULL)
              OR (e.merged_into_entity_id IS NOT NULL AND target.entity_id IS NULL)
              OR (e.merged_into_entity_id IS NOT NULL AND target.status <> 'active')
            ORDER BY e.updated_at DESC NULLS LAST, e.entity_id ASC
            LIMIT $1
            """,
            max(1, int(limit or 200)),
        )
        out: List[InvariantViolationCandidate] = []
        for row in rows or []:
            out.append(
                InvariantViolationCandidate(
                    invariant_code="invalid_entity_merge_lineage",
                    severity="high",
                    tenant_id=_n(row.get("tenant_id")),
                    user_id=_n(row.get("user_id")) or None,
                    object_type="entity",
                    object_id=_n(row.get("entity_id")),
                    details={
                        "status": _n(row.get("status")) or None,
                        "merged_into_entity_id": _n(row.get("merged_into_entity_id")) or None,
                        "target_entity_id": _n(row.get("target_entity_id")) or None,
                        "target_status": _n(row.get("target_status")) or None,
                    },
                    requires_human_review=True,
                )
            )
        return out
