from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone as dt_timezone
import uuid
from typing import Any, Dict, List, Optional

from .canonicalization import stable_short_hash
from .db import Database
from .predicate_policy import PredicatePolicyService, PolicyVersionNotFoundError


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
    return ExtractionPersistResult(
        extract_result_id=int(row["extract_result_id"]),
        extract_run_id=str(row["extract_run_id"]),
        status=str(row["status"] or status),
        deduped=False,
    )
