from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any, Dict, List, Optional, Sequence

from .canonicalization import normalize_text, stable_short_hash
from .claim_resolution import CLAIM_RESOLVER_VERSION, ClaimResolutionResult, ClaimResolver
from .db import Database
from .entity_resolution import (
    ENTITY_RESOLVER_VERSION,
    RESOLUTION_STATUS_AMBIGUOUS,
    EntityResolver,
)
from .predicate_policy import PredicatePolicyService


@dataclass(frozen=True)
class ReplayRunConfig:
    tenant_id: str
    extract_result_ids: List[int]
    policy_version: Optional[str] = None
    entity_resolver_version: str = ENTITY_RESOLVER_VERSION
    claim_resolver_version: str = CLAIM_RESOLVER_VERSION
    allow_assistant_authored: bool = False
    reset_before_run: bool = False


@dataclass(frozen=True)
class ReplayRunResult:
    run_id: str
    tenant_id: str
    extract_result_ids: List[int]
    policy_version: str
    claim_results: List[ClaimResolutionResult]
    errors: List[Dict[str, Any]]
    snapshot: Dict[str, Any]
    state_hash: str


class OfflineReplayHarness:
    """
    T12a offline replay harness.
    Determinism contract: same inputs + same versions + same reset semantics => same snapshot/state_hash.
    """

    def __init__(self, db: Database):
        self.db = db
        self.entity_resolver = EntityResolver(db)
        self.claim_resolver = ClaimResolver(db)
        self.policy_service = PredicatePolicyService(db)

    async def run_replay(self, config: ReplayRunConfig) -> ReplayRunResult:
        tenant_id = normalize_text(config.tenant_id, casefold=False)
        extract_ids = sorted({int(x) for x in (config.extract_result_ids or [])})
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if not extract_ids:
            raise ValueError("extract_result_ids is required")

        run_id = stable_short_hash(
            {
                "tenant_id": tenant_id,
                "extract_result_ids": extract_ids,
                "policy_version": config.policy_version,
                "entity_resolver_version": config.entity_resolver_version,
                "claim_resolver_version": config.claim_resolver_version,
                "allow_assistant_authored": bool(config.allow_assistant_authored),
                "reset_before_run": bool(config.reset_before_run),
            },
            version="t12a.replay_run.v1",
            length=32,
        )

        if config.reset_before_run:
            await self.clear_tenant_canonical_state(tenant_id=tenant_id)

        claim_results: List[ClaimResolutionResult] = []
        errors: List[Dict[str, Any]] = []

        policy_version = normalize_text(config.policy_version, casefold=False)
        if not policy_version:
            policy_version = await self.policy_service.get_current_policy_version()

        for extract_result_id in extract_ids:
            row = await self.db.fetchone(
                """
                SELECT tenant_id, user_id, session_id, candidates, predicate_policy_version
                FROM extract_results
                WHERE tenant_id = $1
                  AND extract_result_id = $2
                LIMIT 1
                """,
                tenant_id,
                extract_result_id,
            )
            if not row:
                errors.append(
                    {
                        "extract_result_id": extract_result_id,
                        "code": "EXTRACT_RESULT_NOT_FOUND",
                        "message": "extract result not found for tenant",
                    }
                )
                continue
            user_id = str(row.get("user_id") or "")
            session_id = str(row.get("session_id") or "")
            payload = row.get("candidates") if isinstance(row.get("candidates"), dict) else {}
            raw_candidates = payload.get("candidates") if isinstance(payload, dict) else []
            candidate_list = [dict(c) for c in (raw_candidates or []) if isinstance(c, dict)]
            effective_policy = (
                policy_version
                or normalize_text(row.get("predicate_policy_version"), casefold=False)
                or await self.policy_service.get_current_policy_version()
            )

            processed_candidates: List[Dict[str, Any]] = []
            for candidate_index, candidate in enumerate(candidate_list):
                processed = dict(candidate)
                try:
                    await self._resolve_candidate_subject_entity(
                        tenant_id=tenant_id,
                        user_id=user_id,
                        candidate=processed,
                        run_id=run_id,
                    )
                except Exception as e:
                    errors.append(
                        {
                            "extract_result_id": extract_result_id,
                            "candidate_index": candidate_index,
                            "code": "ENTITY_RESOLUTION_ERROR",
                            "message": str(e),
                        }
                    )
                processed_candidates.append(processed)

            try:
                claim_result = await self.claim_resolver.resolve_candidates_for_scope(
                    tenant_id=tenant_id,
                    user_id=user_id,
                    session_id=session_id,
                    extract_result_id=extract_result_id,
                    policy_version=effective_policy,
                    candidate_list=processed_candidates,
                    allow_assistant_authored=bool(config.allow_assistant_authored),
                    resolver_version=config.claim_resolver_version,
                )
                claim_results.append(claim_result)
            except Exception as e:
                errors.append(
                    {
                        "extract_result_id": extract_result_id,
                        "code": "CLAIM_RESOLUTION_ERROR",
                        "message": str(e),
                    }
                )

        snapshot = await self.capture_snapshot(tenant_id=tenant_id)
        state_hash = self.compute_state_hash(snapshot)
        return ReplayRunResult(
            run_id=run_id,
            tenant_id=tenant_id,
            extract_result_ids=extract_ids,
            policy_version=policy_version,
            claim_results=claim_results,
            errors=errors,
            snapshot=snapshot,
            state_hash=state_hash,
        )

    async def _resolve_candidate_subject_entity(
        self,
        *,
        tenant_id: str,
        user_id: str,
        candidate: Dict[str, Any],
        run_id: str,
    ) -> None:
        existing_subject_entity = str(candidate.get("subject_entity_id") or "").strip()
        if existing_subject_entity:
            return
        subject_status = normalize_text(candidate.get("subject_resolution_status"), casefold=True)
        if subject_status in {"ambiguous", "unresolved"}:
            return
        subject_text = normalize_text(
            candidate.get("subject_text") if candidate.get("subject_text") is not None else candidate.get("subject"),
            casefold=False,
        )
        if not subject_text:
            return
        entity_type = normalize_text(candidate.get("subject_entity_type"), casefold=True) or "person"
        resolved = await self.entity_resolver.resolve_entity_mention(
            tenant_id=tenant_id,
            user_id=user_id,
            mention_text=subject_text,
            entity_type=entity_type,
            allow_create=True,
            metadata={
                "source_run_id": run_id,
                "replay_mode": "offline_t12a",
            },
        )
        if resolved.resolution_status == RESOLUTION_STATUS_AMBIGUOUS:
            candidate["subject_resolution_status"] = "ambiguous"
            return
        if resolved.resolved_entity_id:
            candidate["subject_entity_id"] = resolved.resolved_entity_id
            candidate["subject_resolution_status"] = "resolved"

    async def clear_tenant_canonical_state(self, *, tenant_id: str) -> None:
        # Ordered deletes for FK safety. Used only in offline replay environments.
        await self.db.execute("DELETE FROM canonical_mutations WHERE tenant_id = $1", tenant_id)
        await self.db.execute("DELETE FROM claim_evidence WHERE tenant_id = $1", tenant_id)
        await self.db.execute("DELETE FROM claims WHERE tenant_id = $1", tenant_id)
        await self.db.execute("DELETE FROM entity_aliases WHERE tenant_id = $1", tenant_id)
        await self.db.execute("DELETE FROM entities WHERE tenant_id = $1", tenant_id)
        await self.db.execute("DELETE FROM canonical_tenant_watermarks WHERE tenant_id = $1", tenant_id)

    async def capture_snapshot(self, *, tenant_id: str) -> Dict[str, Any]:
        claims = await self.db.fetch(
            """
            SELECT
              claims.user_id,
              claims.claim_slot_key,
              claims.claim_event_key,
              claims.predicate,
              claims.subject_entity_id::text AS subject_entity_id,
              claims.subject_text,
              claims.object_payload,
              claims.lifecycle_status,
              claims.extraction_confidence,
              claims.truth_confidence,
              claims.predicate_policy_version,
              claims.source_extract_result_id,
              sb.claim_event_key AS superseded_by_claim_event_key,
              claims.retracted_at
            FROM claims
            LEFT JOIN claims sb
              ON sb.tenant_id = claims.tenant_id
             AND sb.claim_id = claims.superseded_by_claim_id
            WHERE claims.tenant_id = $1
            ORDER BY claims.claim_event_key ASC, claims.claim_id ASC
            """,
            tenant_id,
        )
        evidence = await self.db.fetch(
            """
            SELECT
              c.claim_event_key,
              ce.session_id,
              ce.turn_id,
              ce.evidence_start_char,
              ce.evidence_end_char,
              ce.evidence_text
            FROM claim_evidence ce
            JOIN claims c
              ON c.tenant_id = ce.tenant_id
             AND c.claim_id = ce.claim_id
            WHERE ce.tenant_id = $1
            ORDER BY c.claim_event_key ASC, ce.session_id ASC, ce.turn_id ASC, ce.evidence_start_char ASC NULLS FIRST
            """,
            tenant_id,
        )
        mutations = await self.db.fetch(
            """
            SELECT
              tenant_sequence,
              mutation_type,
              object_type,
              object_id,
              canonical_mutations.claim_id,
              c.claim_event_key AS claim_event_key,
              source_run_id,
              resolver_version,
              commit_status
            FROM canonical_mutations
            LEFT JOIN claims c
              ON c.tenant_id = canonical_mutations.tenant_id
             AND c.claim_id = canonical_mutations.claim_id
            WHERE canonical_mutations.tenant_id = $1
            ORDER BY tenant_sequence ASC
            """,
            tenant_id,
        )
        normalized_mutations = []
        for row in (mutations or []):
            object_type = str(row.get("object_type") or "")
            normalized_object = str(row.get("object_id") or "")
            if object_type == "claim":
                # Claim IDs are DB-surrogate values; canonical replay identity is claim_event_key.
                normalized_object = str(row.get("claim_event_key") or normalized_object)
            normalized_mutations.append(
                {
                    "tenant_sequence": int(row.get("tenant_sequence") or 0),
                    "mutation_type": str(row.get("mutation_type") or ""),
                    "object_type": object_type,
                    "object_id": normalized_object,
                    "source_run_id": str(row.get("source_run_id") or ""),
                    "resolver_version": str(row.get("resolver_version") or ""),
                    "commit_status": str(row.get("commit_status") or ""),
                }
            )
        return {
            "tenant_id": tenant_id,
            "claims": claims or [],
            "claim_evidence": evidence or [],
            "mutations": normalized_mutations,
        }

    def compute_state_hash(self, snapshot: Dict[str, Any]) -> str:
        encoded = json.dumps(snapshot or {}, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def build_diff_report(self, *, baseline: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
        baseline_claims = {str(r.get("claim_event_key")): r for r in (baseline.get("claims") or [])}
        candidate_claims = {str(r.get("claim_event_key")): r for r in (candidate.get("claims") or [])}
        baseline_keys = set(baseline_claims.keys())
        candidate_keys = set(candidate_claims.keys())

        claim_creation = sorted(list(candidate_keys - baseline_keys))

        claim_supersession: List[Dict[str, Any]] = []
        claim_retraction: List[Dict[str, Any]] = []
        for key in sorted(baseline_keys & candidate_keys):
            old = baseline_claims[key]
            new = candidate_claims[key]
            if str(old.get("lifecycle_status")) != str(new.get("lifecycle_status")):
                if str(new.get("lifecycle_status")) == "superseded":
                    claim_supersession.append(
                        {
                            "claim_event_key": key,
                            "before": str(old.get("lifecycle_status")),
                            "after": str(new.get("lifecycle_status")),
                            "superseded_by_claim_id": new.get("superseded_by_claim_id"),
                        }
                    )
                if str(new.get("lifecycle_status")) == "retracted":
                    claim_retraction.append(
                        {
                            "claim_event_key": key,
                            "before": str(old.get("lifecycle_status")),
                            "after": str(new.get("lifecycle_status")),
                            "retracted_at": str(new.get("retracted_at") or ""),
                        }
                    )

        baseline_evidence = {
            json.dumps(
                [
                    r.get("claim_event_key"),
                    r.get("session_id"),
                    r.get("turn_id"),
                    r.get("evidence_start_char"),
                    r.get("evidence_end_char"),
                    r.get("evidence_text"),
                ],
                sort_keys=False,
                separators=(",", ":"),
                ensure_ascii=True,
            )
            for r in (baseline.get("claim_evidence") or [])
        }
        candidate_evidence = {
            json.dumps(
                [
                    r.get("claim_event_key"),
                    r.get("session_id"),
                    r.get("turn_id"),
                    r.get("evidence_start_char"),
                    r.get("evidence_end_char"),
                    r.get("evidence_text"),
                ],
                sort_keys=False,
                separators=(",", ":"),
                ensure_ascii=True,
            )
            for r in (candidate.get("claim_evidence") or [])
        }
        evidence_linkage_differences = {
            "added": sorted(list(candidate_evidence - baseline_evidence)),
            "removed": sorted(list(baseline_evidence - candidate_evidence)),
        }

        baseline_mut = [
            (
                int(r.get("tenant_sequence") or 0),
                str(r.get("mutation_type") or ""),
                str(r.get("object_type") or ""),
                str(r.get("object_id") or ""),
            )
            for r in (baseline.get("mutations") or [])
        ]
        candidate_mut = [
            (
                int(r.get("tenant_sequence") or 0),
                str(r.get("mutation_type") or ""),
                str(r.get("object_type") or ""),
                str(r.get("object_id") or ""),
            )
            for r in (candidate.get("mutations") or [])
        ]
        mismatch_indices: List[int] = []
        for i in range(min(len(baseline_mut), len(candidate_mut))):
            if baseline_mut[i] != candidate_mut[i]:
                mismatch_indices.append(i)

        mutation_ordering_differences = {
            "mismatch_indices": mismatch_indices,
            "added": [
                {"tenant_sequence": seq, "mutation_type": mt, "object_type": ot, "object_id": oid}
                for (seq, mt, ot, oid) in candidate_mut
                if (seq, mt, ot, oid) not in baseline_mut
            ],
            "removed": [
                {"tenant_sequence": seq, "mutation_type": mt, "object_type": ot, "object_id": oid}
                for (seq, mt, ot, oid) in baseline_mut
                if (seq, mt, ot, oid) not in candidate_mut
            ],
        }

        return {
            "claim_creation": claim_creation,
            "claim_supersession": claim_supersession,
            "claim_retraction": claim_retraction,
            "evidence_linkage_differences": evidence_linkage_differences,
            "mutation_ordering_differences": mutation_ordering_differences,
            "baseline_state_hash": self.compute_state_hash(baseline),
            "candidate_state_hash": self.compute_state_hash(candidate),
            "states_equal": self.compute_state_hash(baseline) == self.compute_state_hash(candidate),
        }


def load_golden_corpus(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError("golden corpus must be a JSON object")
    version = str(payload.get("schema_version") or "").strip()
    if not version:
        raise ValueError("golden corpus schema_version is required")
    cases = payload.get("cases")
    if not isinstance(cases, list):
        raise ValueError("golden corpus cases must be a list")
    for idx, case in enumerate(cases):
        if not isinstance(case, dict):
            raise ValueError(f"golden corpus case[{idx}] must be an object")
        if not str(case.get("tenant_id") or "").strip():
            raise ValueError(f"golden corpus case[{idx}] missing tenant_id")
        if not isinstance(case.get("extract_result_ids"), list) or not case.get("extract_result_ids"):
            raise ValueError(f"golden corpus case[{idx}] missing extract_result_ids")
    return payload
