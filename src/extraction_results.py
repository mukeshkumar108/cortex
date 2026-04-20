from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone as dt_timezone
import uuid
from typing import Any, Dict, List, Optional, Tuple

from .canonicalization import stable_short_hash
from .db import Database
from .predicate_policy import (
    PolicyVersionNotFoundError,
    PredicateNotFoundError,
    PredicatePolicyService,
)


class ExtractionContractError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class ExtractionPersistResult:
    extract_result_id: int
    extract_run_id: str
    status: str
    deduped: bool


@dataclass(frozen=True)
class QuarantineDecision:
    reason_code: str
    reason: str
    confidence: Optional[float]
    grounding_score: Optional[float]


_LOW_CONFIDENCE_THRESHOLD = 0.60
_CLAIM_CANDIDATE_TYPES = {"claim_candidate", "claim"}


def _utc_now() -> datetime:
    return datetime.utcnow().replace(tzinfo=dt_timezone.utc)


def _normalize_timestamp(value: Any) -> Optional[str]:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value or "").strip()
        if not raw:
            return None
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=dt_timezone.utc)
    return dt.astimezone(dt_timezone.utc).isoformat().replace("+00:00", "Z")


def _build_default_candidate_payload(messages: List[Dict[str, Any]]) -> Dict[str, Any]:
    candidates: List[Dict[str, Any]] = []
    for idx, msg in enumerate(messages or []):
        if not isinstance(msg, dict):
            continue
        role = str(msg.get("role") or "").strip().lower()
        text = str(msg.get("text") or "")
        if not role or not text.strip():
            continue
        candidates.append(
            {
                "candidate_id": f"msg-{idx}",
                "type": "utterance_signal",
                "role": role,
                "text": text,
                "timestamp": _normalize_timestamp(msg.get("timestamp")),
            }
        )
    return {
        "schema_version": "t4.extract_results.v1",
        "candidates": candidates,
    }


def _validate_candidate_payload(candidate_payload: Any) -> Optional[str]:
    if not isinstance(candidate_payload, dict):
        return "candidate_payload must be an object"
    schema_version = str(candidate_payload.get("schema_version") or "").strip()
    if not schema_version:
        return "candidate_payload.schema_version is required"
    candidates = candidate_payload.get("candidates")
    if not isinstance(candidates, list):
        return "candidate_payload.candidates must be a list"
    for idx, candidate in enumerate(candidates):
        if not isinstance(candidate, dict):
            return f"candidate_payload.candidates[{idx}] must be an object"
    return None


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
    except ValueError:
        return None


def _extract_confidence(candidate: Dict[str, Any]) -> Optional[float]:
    for key in ("confidence", "extraction_confidence", "score"):
        conf = _to_float(candidate.get(key))
        if conf is not None:
            return conf
    return None


def _extract_grounding_score(candidate: Dict[str, Any]) -> Optional[float]:
    score = _to_float(candidate.get("grounding_score"))
    if score is not None:
        return score
    evidence_spans = candidate.get("evidence_spans")
    if isinstance(evidence_spans, list):
        return 1.0 if len(evidence_spans) > 0 else 0.0
    return None


def _is_claim_candidate(candidate: Dict[str, Any]) -> bool:
    candidate_type = str(candidate.get("type") or "").strip().lower()
    if candidate_type in _CLAIM_CANDIDATE_TYPES:
        return True
    claim_hint_keys = (
        "predicate",
        "subject",
        "subject_text",
        "subject_entity_id",
        "object",
        "object_payload",
        "evidence_spans",
        "confidence",
        "extraction_confidence",
    )
    return any(key in candidate for key in claim_hint_keys)


async def _evaluate_quarantine(
    *,
    policy_service: PredicatePolicyService,
    policy_version: str,
    candidate: Dict[str, Any],
) -> Optional[QuarantineDecision]:
    if not _is_claim_candidate(candidate):
        return None

    declared_policy_version = str(candidate.get("predicate_policy_version") or "").strip()
    if declared_policy_version and declared_policy_version != policy_version:
        return QuarantineDecision(
            reason_code="policy_mismatch",
            reason=(
                "candidate policy version does not match extract run policy version "
                f"({declared_policy_version} != {policy_version})"
            ),
            confidence=_extract_confidence(candidate),
            grounding_score=_extract_grounding_score(candidate),
        )

    predicate = str(candidate.get("predicate") or "").strip().lower()
    if not predicate:
        return QuarantineDecision(
            reason_code="malformed_candidate",
            reason="claim candidate is missing required predicate",
            confidence=_extract_confidence(candidate),
            grounding_score=_extract_grounding_score(candidate),
        )

    has_subject = bool(
        str(candidate.get("subject_text") or "").strip()
        or str(candidate.get("subject_entity_id") or "").strip()
        or str(candidate.get("subject") or "").strip()
    )
    if not has_subject:
        return QuarantineDecision(
            reason_code="malformed_candidate",
            reason="claim candidate is missing subject",
            confidence=_extract_confidence(candidate),
            grounding_score=_extract_grounding_score(candidate),
        )

    has_object = (
        "object" in candidate
        or "object_payload" in candidate
        or "object_text" in candidate
    )
    if not has_object:
        return QuarantineDecision(
            reason_code="malformed_candidate",
            reason="claim candidate is missing object payload",
            confidence=_extract_confidence(candidate),
            grounding_score=_extract_grounding_score(candidate),
        )

    try:
        await policy_service.get_policy(predicate=predicate, policy_version=policy_version)
    except PredicateNotFoundError:
        return QuarantineDecision(
            reason_code="unsupported_predicate",
            reason=f"unsupported predicate '{predicate}' for policy version '{policy_version}'",
            confidence=_extract_confidence(candidate),
            grounding_score=_extract_grounding_score(candidate),
        )

    confidence = _extract_confidence(candidate)
    if confidence is not None and confidence < _LOW_CONFIDENCE_THRESHOLD:
        return QuarantineDecision(
            reason_code="low_confidence",
            reason=(
                f"candidate confidence {confidence:.3f} below threshold "
                f"{_LOW_CONFIDENCE_THRESHOLD:.2f}"
            ),
            confidence=confidence,
            grounding_score=_extract_grounding_score(candidate),
        )

    evidence_spans = candidate.get("evidence_spans")
    if not isinstance(evidence_spans, list) or len(evidence_spans) == 0:
        return QuarantineDecision(
            reason_code="weak_grounding",
            reason="candidate has no evidence_spans grounding",
            confidence=confidence,
            grounding_score=_extract_grounding_score(candidate),
        )

    return None


async def _partition_candidates(
    *,
    policy_service: PredicatePolicyService,
    policy_version: str,
    candidates: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    accepted: List[Dict[str, Any]] = []
    quarantined: List[Dict[str, Any]] = []
    for idx, candidate in enumerate(candidates):
        decision = await _evaluate_quarantine(
            policy_service=policy_service,
            policy_version=policy_version,
            candidate=candidate,
        )
        if decision is None:
            accepted.append(candidate)
            continue
        quarantined.append(
            {
                "candidate_index": idx,
                "candidate_payload": candidate,
                "reason_code": decision.reason_code,
                "reason": decision.reason,
                "confidence": decision.confidence,
                "grounding_score": decision.grounding_score,
            }
        )
    return accepted, quarantined


def _build_extract_run_id(
    *,
    tenant_id: str,
    user_id: str,
    session_id: str,
    policy_version: str,
    extractor_model_version: str,
    prompt_version: str,
    reference_time: str,
    candidate_payload: Dict[str, Any],
) -> str:
    digest = stable_short_hash(
        {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "session_id": session_id,
            "policy_version": policy_version,
            "extractor_model_version": extractor_model_version,
            "prompt_version": prompt_version,
            "reference_time": reference_time,
            "candidate_payload": candidate_payload,
        },
        version="t4.extract_run_id.v1",
        length=32,
    )
    return str(uuid.UUID(hex=digest))


async def _persist_quarantine_rows(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    session_id: str,
    extract_result_id: int,
    extract_run_id: str,
    rows: List[Dict[str, Any]],
    created_at: datetime,
) -> None:
    if not rows:
        return
    for row in rows:
        candidate_payload = row.get("candidate_payload")
        reason = str(row.get("reason") or "").strip() or "quarantined candidate"
        reason_code = str(row.get("reason_code") or "").strip() or "unknown"
        confidence = _to_float(row.get("confidence"))
        grounding_score = _to_float(row.get("grounding_score"))
        metadata = {
            "candidate_index": row.get("candidate_index"),
            "ticket": "T4b",
        }
        await db.execute(
            """
            INSERT INTO claims_quarantine (
                tenant_id,
                user_id,
                session_id,
                extract_result_id,
                extract_run_id,
                candidate_payload,
                reason,
                reason_code,
                confidence,
                grounding_score,
                quarantine_status,
                metadata,
                created_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5::uuid, $6::jsonb, $7, $8, $9, $10, 'pending', $11::jsonb, $12, $12
            )
            """,
            tenant_id,
            user_id,
            session_id,
            int(extract_result_id),
            extract_run_id,
            candidate_payload,
            reason,
            reason_code,
            confidence,
            grounding_score,
            metadata,
            created_at,
        )


async def persist_extract_result(
    *,
    db: Database,
    tenant_id: str,
    user_id: str,
    session_id: str,
    messages: List[Dict[str, Any]],
    extractor_model_version: str,
    prompt_version: str,
    policy_version: Optional[str],
    reference_time: Optional[datetime] = None,
    candidate_payload: Optional[Any] = None,
    raw_output: Optional[Any] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> ExtractionPersistResult:
    policy_service = PredicatePolicyService(db)
    effective_policy_version = str(policy_version or "").strip()
    if not effective_policy_version:
        try:
            effective_policy_version = await policy_service.get_current_policy_version()
        except PolicyVersionNotFoundError as e:
            raise ExtractionContractError("EXTRACT_MISSING_POLICY_VERSION", str(e))
    version_exists = await db.fetchval(
        """
        SELECT 1
        FROM predicate_policy_versions
        WHERE policy_version = $1
        LIMIT 1
        """,
        effective_policy_version,
    )
    if not version_exists:
        raise ExtractionContractError(
            "EXTRACT_UNKNOWN_POLICY_VERSION",
            f"unknown predicate policy version: {effective_policy_version}",
        )

    ref_dt = reference_time if isinstance(reference_time, datetime) else _utc_now()
    if ref_dt.tzinfo is None:
        ref_dt = ref_dt.replace(tzinfo=dt_timezone.utc)
    ref_dt = ref_dt.astimezone(dt_timezone.utc)
    reference_time_iso = ref_dt.isoformat()

    existing_session = await db.fetchone(
        """
        SELECT user_id
        FROM sessions_v2
        WHERE tenant_id = $1 AND session_id = $2
        LIMIT 1
        """,
        tenant_id,
        session_id,
    )
    if existing_session:
        if str(existing_session.get("user_id") or "") != user_id:
            raise ExtractionContractError(
                "EXTRACT_SESSION_USER_MISMATCH",
                f"session ownership mismatch for session_id={session_id}",
            )
    else:
        await db.execute(
            """
            INSERT INTO sessions_v2 (
                tenant_id, session_id, user_id, started_at, ended_at, status, source, metadata
            )
            VALUES ($1, $2, $3, $4, $5, 'open', $6, $7::jsonb)
            ON CONFLICT (tenant_id, session_id) DO NOTHING
            """,
            tenant_id,
            session_id,
            user_id,
            ref_dt,
            ref_dt,
            "extract_results_hook",
            {"created_by": "t4.extract_results"},
        )

    payload = candidate_payload if candidate_payload is not None else _build_default_candidate_payload(messages)
    validation_error = _validate_candidate_payload(payload)
    status = "succeeded"
    error_text = None
    persisted_candidates = payload
    persisted_raw_output = raw_output if isinstance(raw_output, dict) else {}
    quarantine_rows: List[Dict[str, Any]] = []
    metadata_payload = metadata if isinstance(metadata, dict) else {}
    if metadata_payload:
        persisted_raw_output = {
            **persisted_raw_output,
            "extract_contract_metadata": metadata_payload,
        }
    if validation_error:
        status = "failed"
        error_text = f"validation error: {validation_error}"
        persisted_candidates = {"schema_version": "t4.extract_results.v1", "candidates": []}
        persisted_raw_output = {
            "invalid_candidate_payload": payload,
            "error": validation_error,
        }
    else:
        candidate_list = payload.get("candidates")
        accepted_candidates, quarantined_candidates = await _partition_candidates(
            policy_service=policy_service,
            policy_version=effective_policy_version,
            candidates=list(candidate_list or []),
        )
        quarantine_rows = quarantined_candidates
        persisted_candidates = {
            "schema_version": str(payload.get("schema_version")),
            "candidates": accepted_candidates,
        }
        if quarantine_rows:
            persisted_raw_output = {
                **persisted_raw_output,
                "quarantine": {
                    "applied": True,
                    "quarantined_count": len(quarantine_rows),
                    "accepted_count": len(accepted_candidates),
                    "thresholds": {
                        "confidence_min": _LOW_CONFIDENCE_THRESHOLD,
                    },
                    "reason_codes": sorted(
                        {str(r.get("reason_code") or "unknown") for r in quarantine_rows}
                    ),
                },
            }
            status = "partial" if accepted_candidates else "quarantined"
            error_text = (
                f"quarantined {len(quarantine_rows)} candidate(s); "
                f"accepted {len(accepted_candidates)} candidate(s)"
            )

    extract_run_id = _build_extract_run_id(
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=session_id,
        policy_version=effective_policy_version,
        extractor_model_version=str(extractor_model_version or "").strip(),
        prompt_version=str(prompt_version or "").strip(),
        reference_time=reference_time_iso,
        candidate_payload=persisted_candidates,
    )

    existing = await db.fetchone(
        """
        SELECT extract_result_id, extract_run_id, status
        FROM extract_results
        WHERE tenant_id = $1
          AND extract_run_id = $2::uuid
        LIMIT 1
        """,
        tenant_id,
        extract_run_id,
    )
    if existing:
        return ExtractionPersistResult(
            extract_result_id=int(existing["extract_result_id"]),
            extract_run_id=str(existing["extract_run_id"]),
            status=str(existing["status"] or "succeeded"),
            deduped=True,
        )

    row = await db.fetchone(
        """
        INSERT INTO extract_results (
            tenant_id,
            user_id,
            session_id,
            extract_run_id,
            model_version,
            prompt_version,
            predicate_policy_version,
            status,
            raw_output,
            candidates,
            error_text,
            started_at,
            completed_at
        )
        VALUES (
            $1, $2, $3, $4::uuid, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11, $12, $13
        )
        RETURNING extract_result_id, extract_run_id, status
        """,
        tenant_id,
        user_id,
        session_id,
        extract_run_id,
        str(extractor_model_version or "").strip() or "t4-extractor-v1",
        str(prompt_version or "").strip() or "t4-prompt-v1",
        effective_policy_version,
        status,
        persisted_raw_output,
        persisted_candidates,
        error_text,
        ref_dt,
        _utc_now(),
    )
    await _persist_quarantine_rows(
        db=db,
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=session_id,
        extract_result_id=int(row["extract_result_id"]),
        extract_run_id=str(row["extract_run_id"]),
        rows=quarantine_rows,
        created_at=_utc_now(),
    )
    return ExtractionPersistResult(
        extract_result_id=int(row["extract_result_id"]),
        extract_run_id=str(row["extract_run_id"]),
        status=str(row["status"] or status),
        deduped=False,
    )
