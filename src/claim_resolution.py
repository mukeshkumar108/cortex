from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone as dt_timezone
from typing import Any, Dict, List, Optional, Tuple
import uuid

from .canonicalization import (
    generate_claim_event_key,
    generate_claim_slot_key,
    normalize_text,
    normalize_timestamp,
)
from .canonical_mutation_log import CanonicalMutationLogger
from .db import Database
from .predicate_policy import (
    MissingPolicyVersionError,
    PolicyVersionNotFoundError,
    PredicateNotFoundError,
    PredicatePolicy,
    PredicatePolicyService,
)


CLAIM_RESOLVER_VERSION = "t7.claim_resolver.v1"
CLAIM_STATUS_ACTIVE = "active"
CLAIM_STATUS_SUPERSEDED = "superseded"
CLAIM_STATUS_RETRACTED = "retracted"


class ClaimResolutionError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class ClaimCandidateReject:
    candidate_index: int
    reason_code: str
    reason: str


@dataclass(frozen=True)
class ClaimResolutionResult:
    extract_result_id: int
    tenant_id: str
    user_id: str
    session_id: str
    predicate_policy_version: str
    resolver_version: str
    created_claim_ids: List[int]
    reinforced_claim_ids: List[int]
    superseded_claim_ids: List[int]
    retracted_claim_ids: List[int]
    rejected_candidates: List[ClaimCandidateReject]


def _utc_now() -> datetime:
    return datetime.utcnow().replace(tzinfo=dt_timezone.utc)


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def _clamp_confidence(value: Optional[float], *, default: float) -> float:
    if value is None:
        return float(default)
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return float(value)


def _extract_confidence(candidate: Dict[str, Any], default: float = 0.75) -> float:
    conf = _to_float(candidate.get("extraction_confidence"))
    if conf is None:
        conf = _to_float(candidate.get("confidence"))
    return _clamp_confidence(conf, default=default)


def _extract_truth_confidence(candidate: Dict[str, Any], extraction_confidence: float) -> float:
    conf = _to_float(candidate.get("truth_confidence"))
    if conf is None:
        conf = extraction_confidence
    return _clamp_confidence(conf, default=extraction_confidence)


def _candidate_action(candidate: Dict[str, Any]) -> str:
    status = normalize_text(candidate.get("lifecycle_status"), casefold=True)
    if status == CLAIM_STATUS_RETRACTED:
        return "retract"
    action = normalize_text(candidate.get("action"), casefold=True)
    if action in {"retract", "retracted"}:
        return "retract"
    return "upsert"


def _extract_object_payload(candidate: Dict[str, Any]) -> Dict[str, Any]:
    payload = candidate.get("object_payload")
    if isinstance(payload, dict):
        return payload
    if payload is not None:
        return {"value": payload}
    if "object" in candidate:
        return {"value": candidate.get("object")}
    if "value" in candidate:
        return {"value": candidate.get("value")}
    return {}


def _extract_subject_text(candidate: Dict[str, Any]) -> str:
    subject_text = candidate.get("subject_text")
    if subject_text is None:
        subject_text = candidate.get("subject")
    return normalize_text(subject_text, casefold=False)


def _extract_subject_entity_id(candidate: Dict[str, Any]) -> Optional[str]:
    raw = str(candidate.get("subject_entity_id") or "").strip()
    if not raw:
        return None
    try:
        return str(uuid.UUID(raw))
    except Exception:
        return None


def _extract_occurred_at(candidate: Dict[str, Any]) -> Optional[str]:
    for key in ("occurred_at", "timestamp", "event_time"):
        normalized = normalize_timestamp(candidate.get(key))
        if normalized:
            return normalized
    return None


def _extract_evidence_spans(candidate: Dict[str, Any]) -> List[Dict[str, Any]]:
    spans = candidate.get("evidence_spans")
    if not isinstance(spans, list):
        return []
    out: List[Dict[str, Any]] = []
    for span in spans:
        if not isinstance(span, dict):
            continue
        try:
            turn_index = int(span.get("turn_index"))
        except Exception:
            continue
        start = span.get("start")
        end = span.get("end")
        evidence_text = span.get("text")
        out.append(
            {
                "turn_index": turn_index,
                "start": int(start) if isinstance(start, (int, float)) else None,
                "end": int(end) if isinstance(end, (int, float)) else None,
                "text": str(evidence_text) if evidence_text is not None else None,
            }
        )
    return out


class ClaimResolver:
    def __init__(self, db: Database):
        self.db = db
        self.policy_service = PredicatePolicyService(db)
        self.mutation_logger = CanonicalMutationLogger(db)

    async def resolve_extract_result_claims(
        self,
        *,
        tenant_id: str,
        extract_result_id: int,
        allow_assistant_authored: bool = False,
        policy_version_override: Optional[str] = None,
        resolver_version: str = CLAIM_RESOLVER_VERSION,
    ) -> ClaimResolutionResult:
        extract_row = await self.db.fetchone(
            """
            SELECT
              extract_result_id,
              tenant_id,
              user_id,
              session_id,
              predicate_policy_version,
              candidates
            FROM extract_results
            WHERE tenant_id = $1
              AND extract_result_id = $2
            LIMIT 1
            """,
            tenant_id,
            int(extract_result_id),
        )
        if not extract_row:
            raise ClaimResolutionError("CLAIM_RESOLUTION_EXTRACT_NOT_FOUND", "extract result not found")

        effective_policy_version = normalize_text(policy_version_override, casefold=False) or normalize_text(
            extract_row.get("predicate_policy_version"),
            casefold=False,
        )
        if not effective_policy_version:
            raise ClaimResolutionError("CLAIM_RESOLUTION_MISSING_POLICY_VERSION", "predicate policy version is required")
        version_exists = await self.db.fetchval(
            """
            SELECT 1
            FROM predicate_policy_versions
            WHERE policy_version = $1
            LIMIT 1
            """,
            effective_policy_version,
        )
        if not version_exists:
            raise ClaimResolutionError(
                "CLAIM_RESOLUTION_UNKNOWN_POLICY_VERSION",
                f"unknown predicate policy version: {effective_policy_version}",
            )

        candidates_payload = extract_row.get("candidates") if isinstance(extract_row.get("candidates"), dict) else {}
        raw_candidates = candidates_payload.get("candidates") if isinstance(candidates_payload, dict) else None
        candidate_list = [c for c in (raw_candidates or []) if isinstance(c, dict)]

        created: List[int] = []
        reinforced: List[int] = []
        superseded: List[int] = []
        retracted: List[int] = []
        rejected: List[ClaimCandidateReject] = []

        # Deterministic iteration by candidate list order.
        for idx, candidate in enumerate(candidate_list):
            result = await self._apply_candidate(
                tenant_id=str(extract_row["tenant_id"]),
                user_id=str(extract_row["user_id"]),
                session_id=str(extract_row["session_id"]),
                extract_result_id=int(extract_row["extract_result_id"]),
                candidate_index=idx,
                candidate=candidate,
                policy_version=effective_policy_version,
                allow_assistant_authored=allow_assistant_authored,
                resolver_version=resolver_version,
            )
            if result.get("reject"):
                rejected.append(result["reject"])
            created.extend(result.get("created", []))
            reinforced.extend(result.get("reinforced", []))
            superseded.extend(result.get("superseded", []))
            retracted.extend(result.get("retracted", []))

        return ClaimResolutionResult(
            extract_result_id=int(extract_row["extract_result_id"]),
            tenant_id=str(extract_row["tenant_id"]),
            user_id=str(extract_row["user_id"]),
            session_id=str(extract_row["session_id"]),
            predicate_policy_version=effective_policy_version,
            resolver_version=resolver_version,
            created_claim_ids=created,
            reinforced_claim_ids=reinforced,
            superseded_claim_ids=superseded,
            retracted_claim_ids=retracted,
            rejected_candidates=rejected,
        )

    async def _apply_candidate(
        self,
        *,
        tenant_id: str,
        user_id: str,
        session_id: str,
        extract_result_id: int,
        candidate_index: int,
        candidate: Dict[str, Any],
        policy_version: str,
        allow_assistant_authored: bool,
        resolver_version: str,
    ) -> Dict[str, Any]:
        out: Dict[str, Any] = {"created": [], "reinforced": [], "superseded": [], "retracted": []}
        candidate_type = normalize_text(candidate.get("type"), casefold=True)
        if candidate_type not in {"claim_candidate", "claim", ""} and "predicate" not in candidate:
            return out

        source_role = normalize_text(candidate.get("source_role"), casefold=True) or normalize_text(
            candidate.get("author_role"),
            casefold=True,
        )
        if source_role == "assistant" and not allow_assistant_authored:
            out["reject"] = ClaimCandidateReject(
                candidate_index=candidate_index,
                reason_code="assistant_authored_disallowed",
                reason="assistant-authored candidate rejected by default policy",
            )
            return out

        predicate = normalize_text(candidate.get("predicate"), casefold=True)
        if not predicate:
            out["reject"] = ClaimCandidateReject(
                candidate_index=candidate_index,
                reason_code="missing_predicate",
                reason="candidate predicate is required",
            )
            return out

        try:
            policy = await self.policy_service.get_policy(predicate=predicate, policy_version=policy_version)
        except MissingPolicyVersionError as e:
            raise ClaimResolutionError("CLAIM_RESOLUTION_MISSING_POLICY_VERSION", str(e))
        except PolicyVersionNotFoundError as e:
            raise ClaimResolutionError("CLAIM_RESOLUTION_UNKNOWN_POLICY_VERSION", str(e))
        except PredicateNotFoundError:
            out["reject"] = ClaimCandidateReject(
                candidate_index=candidate_index,
                reason_code="unsupported_predicate",
                reason=f"unsupported predicate '{predicate}' for policy '{policy_version}'",
            )
            return out

        subject_resolution_status = normalize_text(candidate.get("subject_resolution_status"), casefold=True)
        if subject_resolution_status in {"ambiguous", "unresolved"}:
            out["reject"] = ClaimCandidateReject(
                candidate_index=candidate_index,
                reason_code="ambiguous_subject",
                reason="subject entity resolution is ambiguous/unresolved",
            )
            return out

        subject_entity_id = _extract_subject_entity_id(candidate)
        subject_text = _extract_subject_text(candidate)
        if policy.expected_subject_kind == "entity":
            if not subject_entity_id:
                out["reject"] = ClaimCandidateReject(
                    candidate_index=candidate_index,
                    reason_code="missing_subject_entity",
                    reason="predicate requires resolved subject entity",
                )
                return out
            subject_entity_exists = await self.db.fetchval(
                """
                SELECT 1
                FROM entities
                WHERE tenant_id = $1
                  AND user_id = $2
                  AND entity_id = $3::uuid
                  AND status = 'active'
                LIMIT 1
                """,
                tenant_id,
                user_id,
                subject_entity_id,
            )
            if not subject_entity_exists:
                out["reject"] = ClaimCandidateReject(
                    candidate_index=candidate_index,
                    reason_code="unknown_subject_entity",
                    reason="resolved subject entity is not active in tenant/user scope",
                )
                return out

        object_payload = _extract_object_payload(candidate)
        evidence_spans = _extract_evidence_spans(candidate)
        if not evidence_spans:
            out["reject"] = ClaimCandidateReject(
                candidate_index=candidate_index,
                reason_code="missing_evidence",
                reason="candidate has no valid evidence spans",
            )
            return out

        occurred_at = _extract_occurred_at(candidate)
        subject_for_key = subject_entity_id or subject_text
        slot_key = generate_claim_slot_key(
            tenant_id=tenant_id,
            subject=subject_for_key,
            predicate=predicate,
            slot_scope={},
        )
        event_key = generate_claim_event_key(
            tenant_id=tenant_id,
            subject=subject_for_key,
            predicate=predicate,
            obj=object_payload,
            occurred_at=occurred_at,
            event_scope={},
        )

        extraction_conf = _extract_confidence(candidate)
        truth_conf = _extract_truth_confidence(candidate, extraction_conf)
        action = _candidate_action(candidate)
        now = _utc_now()

        active_rows = await self.db.fetch(
            """
            SELECT claim_id, object_payload
            FROM claims
            WHERE tenant_id = $1
              AND user_id = $2
              AND claim_slot_key = $3
              AND lifecycle_status = 'active'
            ORDER BY claim_id ASC
            """,
            tenant_id,
            user_id,
            slot_key,
        )

        equivalent_active = self._find_equivalent_active(policy=policy, active_rows=active_rows, object_payload=object_payload)

        if action == "retract":
            retracted_ids = await self._retract_active_claims(
                tenant_id=tenant_id,
                user_id=user_id,
                session_id=session_id,
                extract_result_id=extract_result_id,
                resolver_version=resolver_version,
                policy=policy,
                active_rows=active_rows,
                object_payload=object_payload,
            )
            out["retracted"].extend(retracted_ids)
            return out

        if equivalent_active is not None:
            claim_id = int(equivalent_active[0])
            await self.db.execute(
                """
                UPDATE claims
                SET extraction_confidence = GREATEST(COALESCE(extraction_confidence, 0.0), $3),
                    truth_confidence = GREATEST(COALESCE(truth_confidence, 0.0), $4),
                    updated_at = $5,
                    metadata = COALESCE(metadata, '{}'::jsonb) || $6::jsonb
                WHERE tenant_id = $1 AND claim_id = $2
                """,
                tenant_id,
                claim_id,
                extraction_conf,
                truth_conf,
                now,
                {
                    "resolver_version": resolver_version,
                    "reinforced_from_extract_result_id": extract_result_id,
                    "candidate_index": candidate_index,
                },
            )
            await self._ensure_claim_evidence(
                tenant_id=tenant_id,
                claim_id=claim_id,
                session_id=session_id,
                evidence_spans=evidence_spans,
            )
            await self._record_mutation(
                tenant_id=tenant_id,
                user_id=user_id,
                mutation_type="claim_reinforced",
                claim_id=claim_id,
                extract_result_id=extract_result_id,
                resolver_version=resolver_version,
                payload={
                    "claim_id": claim_id,
                    "claim_slot_key": slot_key,
                    "claim_event_key": event_key,
                },
            )
            out["reinforced"].append(claim_id)
            return out

        existing_same_event = await self.db.fetchone(
            """
            SELECT claim_id
            FROM claims
            WHERE tenant_id = $1 AND claim_event_key = $2
            LIMIT 1
            """,
            tenant_id,
            event_key,
        )
        if existing_same_event:
            claim_id = int(existing_same_event["claim_id"])
            await self._ensure_claim_evidence(
                tenant_id=tenant_id,
                claim_id=claim_id,
                session_id=session_id,
                evidence_spans=evidence_spans,
            )
            out["reinforced"].append(claim_id)
            return out

        created_claim_id = await self.db.fetchval(
            """
            INSERT INTO claims (
                tenant_id,
                user_id,
                claim_slot_key,
                claim_event_key,
                predicate,
                subject_entity_id,
                subject_text,
                object_payload,
                lifecycle_status,
                extraction_confidence,
                truth_confidence,
                predicate_policy_version,
                source_extract_result_id,
                occurred_at,
                valid_from,
                metadata,
                created_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6::uuid, $7, $8::jsonb, 'active', $9, $10, $11, $12, $13, $14, $15::jsonb, $16, $16
            )
            RETURNING claim_id
            """,
            tenant_id,
            user_id,
            slot_key,
            event_key,
            predicate,
            subject_entity_id,
            subject_text or None,
            object_payload,
            extraction_conf,
            truth_conf,
            policy_version,
            extract_result_id,
            occurred_at,
            now,
            {
                "resolver_version": resolver_version,
                "candidate_index": candidate_index,
                "source_role": source_role or None,
            },
            now,
        )
        claim_id = int(created_claim_id)
        out["created"].append(claim_id)

        await self._ensure_claim_evidence(
            tenant_id=tenant_id,
            claim_id=claim_id,
            session_id=session_id,
            evidence_spans=evidence_spans,
        )
        await self._record_mutation(
            tenant_id=tenant_id,
            user_id=user_id,
            mutation_type="claim_created",
            claim_id=claim_id,
            extract_result_id=extract_result_id,
            resolver_version=resolver_version,
            payload={
                "claim_id": claim_id,
                "claim_slot_key": slot_key,
                "claim_event_key": event_key,
                "predicate": predicate,
            },
        )

        should_exclusive_supersede = policy.cardinality == "one" or policy.conflict_mode == "supersede"
        if should_exclusive_supersede and active_rows:
            superseded_ids = [int(r["claim_id"]) for r in active_rows]
            if superseded_ids:
                await self.db.execute(
                    """
                    UPDATE claims
                    SET lifecycle_status = 'superseded',
                        superseded_by_claim_id = $3,
                        valid_to = $4,
                        updated_at = $4
                    WHERE tenant_id = $1
                      AND claim_id = ANY($2::bigint[])
                      AND lifecycle_status = 'active'
                    """,
                    tenant_id,
                    superseded_ids,
                    claim_id,
                    now,
                )
                out["superseded"].extend(superseded_ids)
                for old_claim_id in superseded_ids:
                    await self._record_mutation(
                        tenant_id=tenant_id,
                        user_id=user_id,
                        mutation_type="claim_superseded",
                        claim_id=old_claim_id,
                        extract_result_id=extract_result_id,
                        resolver_version=resolver_version,
                        payload={
                            "superseded_claim_id": old_claim_id,
                            "superseded_by_claim_id": claim_id,
                            "claim_slot_key": slot_key,
                        },
                    )

        return out

    def _find_equivalent_active(
        self,
        *,
        policy: PredicatePolicy,
        active_rows: List[Dict[str, Any]],
        object_payload: Dict[str, Any],
    ) -> Optional[Tuple[int, Any]]:
        eq = policy.object_equivalence_hook()
        for row in active_rows:
            existing_payload = row.get("object_payload")
            if eq(existing_payload, object_payload):
                return int(row["claim_id"]), existing_payload
        return None

    async def _ensure_claim_evidence(
        self,
        *,
        tenant_id: str,
        claim_id: int,
        session_id: str,
        evidence_spans: List[Dict[str, Any]],
    ) -> None:
        for span in evidence_spans:
            turn_index = span.get("turn_index")
            turn_id = await self.db.fetchval(
                """
                SELECT turn_id
                FROM turns_v2
                WHERE tenant_id = $1
                  AND session_id = $2
                  AND turn_index = $3
                LIMIT 1
                """,
                tenant_id,
                session_id,
                int(turn_index),
            )
            evidence_text = span.get("text")
            evidence_start = span.get("start")
            evidence_end = span.get("end")
            exists = await self.db.fetchval(
                """
                SELECT 1
                FROM claim_evidence
                WHERE tenant_id = $1
                  AND claim_id = $2
                  AND session_id = $3
                  AND COALESCE(turn_id, -1) = COALESCE($4::bigint, -1)
                  AND COALESCE(evidence_start_char, -1) = COALESCE($5::int, -1)
                  AND COALESCE(evidence_end_char, -1) = COALESCE($6::int, -1)
                LIMIT 1
                """,
                tenant_id,
                claim_id,
                session_id,
                turn_id,
                evidence_start,
                evidence_end,
            )
            if exists:
                continue
            await self.db.execute(
                """
                INSERT INTO claim_evidence (
                    tenant_id,
                    claim_id,
                    session_id,
                    turn_id,
                    evidence_kind,
                    evidence_text,
                    evidence_start_char,
                    evidence_end_char,
                    metadata,
                    created_at
                )
                VALUES (
                    $1, $2, $3, $4, 'span', $5, $6, $7, $8::jsonb, $9
                )
                """,
                tenant_id,
                claim_id,
                session_id,
                turn_id,
                evidence_text,
                evidence_start,
                evidence_end,
                {"resolver_version": CLAIM_RESOLVER_VERSION, "turn_index": turn_index},
                _utc_now(),
            )

    async def _retract_active_claims(
        self,
        *,
        tenant_id: str,
        user_id: str,
        session_id: str,
        extract_result_id: int,
        resolver_version: str,
        policy: PredicatePolicy,
        active_rows: List[Dict[str, Any]],
        object_payload: Dict[str, Any],
    ) -> List[int]:
        eq = policy.object_equivalence_hook()
        retract_ids: List[int] = []
        for row in active_rows:
            row_id = int(row["claim_id"])
            if object_payload and not eq(row.get("object_payload"), object_payload):
                continue
            retract_ids.append(row_id)
        if not retract_ids:
            return []
        now = _utc_now()
        await self.db.execute(
            """
            UPDATE claims
            SET lifecycle_status = 'retracted',
                retracted_at = $3,
                valid_to = $3,
                updated_at = $3
            WHERE tenant_id = $1
              AND claim_id = ANY($2::bigint[])
              AND user_id = $4
            """,
            tenant_id,
            retract_ids,
            now,
            user_id,
        )
        for claim_id in retract_ids:
            await self._record_mutation(
                tenant_id=tenant_id,
                user_id=user_id,
                mutation_type="claim_retracted",
                claim_id=claim_id,
                extract_result_id=extract_result_id,
                resolver_version=resolver_version,
                payload={
                    "claim_id": claim_id,
                    "session_id": session_id,
                },
            )
        return retract_ids

    async def _record_mutation(
        self,
        *,
        tenant_id: str,
        user_id: str,
        mutation_type: str,
        claim_id: int,
        extract_result_id: int,
        resolver_version: str,
        payload: Dict[str, Any],
    ) -> None:
        await self.mutation_logger.append_mutation(
            tenant_id=tenant_id,
            user_id=user_id,
            mutation_type=mutation_type,
            object_type="claim",
            object_id=str(int(claim_id)),
            resolver_version=resolver_version,
            claim_id=int(claim_id),
            source_extract_result_id=int(extract_result_id),
            source_run_id=str(int(extract_result_id)),
            committed_by="t7.claim_resolver",
            payload=payload if isinstance(payload, dict) else {},
            metadata={"resolver_version": resolver_version},
        )
