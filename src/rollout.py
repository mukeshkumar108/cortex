from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone as dt_timezone
import json
from typing import Any, Dict, List, Optional

from .canonicalization import normalize_text, stable_short_hash
from .db import Database
from .invariants import InvariantManager

ROLLOUT_CONTROLLER_VERSION = "t14.rollout.v1"

MODE_LEGACY_ONLY = "legacy_only"
MODE_SHADOW_ONLY = "shadow_only"
MODE_COHORT_V2 = "cohort_v2"
MODE_V2_ALL = "v2_all"

ROUTE_LEGACY = "legacy_served"
ROUTE_SHADOW = "shadow_only"
ROUTE_V2 = "v2_served"


@dataclass(frozen=True)
class RolloutEvaluationResult:
    samples: int
    triggered: bool
    rollback_reason: Optional[str]
    metrics: Dict[str, Any]


def _n(value: Any) -> str:
    return normalize_text(value, casefold=False)


def _now() -> datetime:
    return datetime.utcnow().replace(tzinfo=dt_timezone.utc)


def _cohort_bucket(*, tenant_id: str, user_id: str) -> int:
    digest = stable_short_hash(
        json.dumps(
            {"tenant_id": _n(tenant_id), "user_id": _n(user_id)},
            sort_keys=True,
            ensure_ascii=True,
        )
    )
    raw = digest.encode("utf-8")
    seed = int.from_bytes((raw[:4] if len(raw) >= 4 else raw.ljust(4, b"0")), byteorder="big")
    return seed % 100


class RolloutController:
    def __init__(self, db: Database):
        self.db = db

    async def ensure_state_row(self) -> None:
        await self.db.execute(
            """
            INSERT INTO retrieval_rollout_control (
                control_id,
                mode,
                cohort_tenants,
                cohort_users,
                cohort_percentage,
                threshold_evidence_regression,
                threshold_active_slot_conflicts,
                threshold_replay_divergence,
                threshold_latency_regression,
                threshold_quality_regression,
                rollback_active,
                rollback_reason,
                updated_by,
                updated_at
            )
            VALUES (
                TRUE,
                'shadow_only',
                '[]'::jsonb,
                '[]'::jsonb,
                0,
                0.08,
                1,
                1,
                0.20,
                0.20,
                FALSE,
                NULL,
                'bootstrap',
                NOW()
            )
            ON CONFLICT (control_id) DO NOTHING
            """
        )

    async def get_state(self) -> Dict[str, Any]:
        await self.ensure_state_row()
        row = await self.db.fetchone(
            """
            SELECT
              mode,
              cohort_tenants,
              cohort_users,
              cohort_percentage,
              threshold_evidence_regression,
              threshold_active_slot_conflicts,
              threshold_replay_divergence,
              threshold_latency_regression,
              threshold_quality_regression,
              rollback_active,
              rollback_reason,
              updated_by,
              updated_at
            FROM retrieval_rollout_control
            WHERE control_id = TRUE
            LIMIT 1
            """
        )
        return row or {}

    async def update_state(self, *, patch: Dict[str, Any], updated_by: str) -> Dict[str, Any]:
        await self.ensure_state_row()
        state = await self.get_state()

        mode = _n(patch.get("mode") or state.get("mode") or MODE_SHADOW_ONLY).lower()
        if mode not in {MODE_LEGACY_ONLY, MODE_SHADOW_ONLY, MODE_COHORT_V2, MODE_V2_ALL}:
            raise ValueError("invalid rollout mode")

        cohort_tenants = patch.get("cohort_tenants") if isinstance(patch.get("cohort_tenants"), list) else state.get("cohort_tenants")
        cohort_users = patch.get("cohort_users") if isinstance(patch.get("cohort_users"), list) else state.get("cohort_users")
        cohort_percentage = patch.get("cohort_percentage", state.get("cohort_percentage"))
        cohort_percentage = int(cohort_percentage or 0)
        cohort_percentage = max(0, min(100, cohort_percentage))

        threshold_evidence_regression = float(patch.get("threshold_evidence_regression", state.get("threshold_evidence_regression") or 0.08))
        threshold_active_slot_conflicts = int(patch.get("threshold_active_slot_conflicts", state.get("threshold_active_slot_conflicts") or 1))
        threshold_replay_divergence = int(patch.get("threshold_replay_divergence", state.get("threshold_replay_divergence") or 1))
        threshold_latency_regression = float(patch.get("threshold_latency_regression", state.get("threshold_latency_regression") or 0.20))
        threshold_quality_regression = float(patch.get("threshold_quality_regression", state.get("threshold_quality_regression") or 0.20))

        rollback_active = bool(patch.get("rollback_active", state.get("rollback_active") or False))
        rollback_reason = _n(patch.get("rollback_reason") or state.get("rollback_reason")) or None

        now = _now()
        await self.db.execute(
            """
            UPDATE retrieval_rollout_control
            SET
              mode = $1,
              cohort_tenants = $2::jsonb,
              cohort_users = $3::jsonb,
              cohort_percentage = $4,
              threshold_evidence_regression = $5,
              threshold_active_slot_conflicts = $6,
              threshold_replay_divergence = $7,
              threshold_latency_regression = $8,
              threshold_quality_regression = $9,
              rollback_active = $10,
              rollback_reason = $11,
              updated_by = $12,
              updated_at = $13
            WHERE control_id = TRUE
            """,
            mode,
            cohort_tenants or [],
            cohort_users or [],
            cohort_percentage,
            threshold_evidence_regression,
            threshold_active_slot_conflicts,
            threshold_replay_divergence,
            threshold_latency_regression,
            threshold_quality_regression,
            rollback_active,
            rollback_reason,
            _n(updated_by) or "unknown",
            now,
        )
        await self._log_event(
            event_type="state_update",
            details={
                "mode": mode,
                "rollback_active": rollback_active,
                "rollback_reason": rollback_reason,
                "cohort_percentage": cohort_percentage,
            },
            updated_by=updated_by,
        )
        return await self.get_state()

    async def decide_route(self, *, tenant_id: str, user_id: str) -> str:
        state = await self.get_state()
        if bool(state.get("rollback_active")):
            return ROUTE_LEGACY

        mode = _n(state.get("mode")).lower()
        if mode == MODE_LEGACY_ONLY:
            return ROUTE_LEGACY
        if mode == MODE_SHADOW_ONLY:
            return ROUTE_SHADOW
        if mode == MODE_V2_ALL:
            return ROUTE_V2

        tenants = {
            _n(x).lower()
            for x in (state.get("cohort_tenants") or [])
            if _n(x)
        }
        users = {
            _n(x).lower()
            for x in (state.get("cohort_users") or [])
            if _n(x)
        }
        t = _n(tenant_id).lower()
        u = _n(user_id).lower()
        if t in tenants or u in users:
            return ROUTE_V2

        percentage = int(state.get("cohort_percentage") or 0)
        if percentage > 0 and _cohort_bucket(tenant_id=tenant_id, user_id=user_id) < percentage:
            return ROUTE_V2

        return ROUTE_SHADOW if mode == MODE_COHORT_V2 else ROUTE_LEGACY

    async def evaluate_and_apply_rollback(
        self,
        *,
        lookback_minutes: int = 60,
        min_samples: int = 20,
        updated_by: str = "t14.rollout.evaluator",
    ) -> RolloutEvaluationResult:
        state = await self.get_state()
        since = _now() - timedelta(minutes=max(1, int(lookback_minutes or 60)))
        rows = await self.db.fetch(
            """
            SELECT diff_payload, metrics_payload
            FROM retrieval_shadow_diffs
            WHERE status = 'ok'
              AND created_at >= $1
            ORDER BY created_at DESC
            LIMIT 1000
            """,
            since,
        )
        samples = len(rows or [])
        evidence_regressions = 0
        latency_regressions = 0
        quality_drift = 0
        for row in rows or []:
            diff = row.get("diff_payload") if isinstance(row.get("diff_payload"), dict) else {}
            signals = diff.get("regression_signals") if isinstance(diff.get("regression_signals"), dict) else {}
            if bool(signals.get("evidence_coverage_regression")):
                evidence_regressions += 1
            if bool(signals.get("latency_regression")):
                latency_regressions += 1
            if bool(signals.get("continuity_drift")) or bool(signals.get("possible_contradictions")):
                quality_drift += 1

        inv_rows = await self.db.fetch(
            """
            SELECT invariant_code, COUNT(*)::int AS n
            FROM invariant_violations
            WHERE status IN ('open', 'review_required')
              AND invariant_code IN (
                'duplicate_active_exclusive_slot_claims',
                'replay_state_divergence',
                'replay_divergence_detected'
              )
            GROUP BY invariant_code
            """
        )
        conflict_count = 0
        replay_divergence_count = 0
        for row in inv_rows or []:
            code = _n(row.get("invariant_code"))
            n = int(row.get("n") or 0)
            if code == "duplicate_active_exclusive_slot_claims":
                conflict_count += n
            if code in {"replay_state_divergence", "replay_divergence_detected"}:
                replay_divergence_count += n

        metrics = {
            "samples": samples,
            "evidence_regression_rate": (float(evidence_regressions) / float(samples)) if samples > 0 else 0.0,
            "latency_regression_rate": (float(latency_regressions) / float(samples)) if samples > 0 else 0.0,
            "quality_regression_rate": (float(quality_drift) / float(samples)) if samples > 0 else 0.0,
            "active_slot_conflicts": conflict_count,
            "replay_divergence_count": replay_divergence_count,
        }

        triggered = False
        reason = None
        if samples >= max(1, int(min_samples or 20)):
            if metrics["evidence_regression_rate"] > float(state.get("threshold_evidence_regression") or 0.08):
                triggered = True
                reason = f"evidence_regression_rate_exceeded:{metrics['evidence_regression_rate']:.4f}"
            elif metrics["active_slot_conflicts"] > int(state.get("threshold_active_slot_conflicts") or 1):
                triggered = True
                reason = f"active_slot_conflicts_exceeded:{metrics['active_slot_conflicts']}"
            elif metrics["replay_divergence_count"] > int(state.get("threshold_replay_divergence") or 1):
                triggered = True
                reason = f"replay_divergence_exceeded:{metrics['replay_divergence_count']}"
            elif metrics["latency_regression_rate"] > float(state.get("threshold_latency_regression") or 0.20):
                triggered = True
                reason = f"latency_regression_rate_exceeded:{metrics['latency_regression_rate']:.4f}"
            elif metrics["quality_regression_rate"] > float(state.get("threshold_quality_regression") or 0.20):
                triggered = True
                reason = f"quality_regression_rate_exceeded:{metrics['quality_regression_rate']:.4f}"

        if triggered:
            await self.update_state(
                patch={
                    "mode": MODE_LEGACY_ONLY,
                    "rollback_active": True,
                    "rollback_reason": reason,
                },
                updated_by=updated_by,
            )
            integrity_summary = await InvariantManager(self.db).run_cycle(auto_repair_enabled=False)
            await self._log_event(
                event_type="rollback_triggered",
                details={
                    "reason": reason,
                    "metrics": metrics,
                    "post_rollback_integrity": {
                        "detected": integrity_summary.detected,
                        "persisted": integrity_summary.persisted,
                        "auto_repaired": integrity_summary.auto_repaired,
                        "review_required": integrity_summary.review_required,
                        "failed_repairs": integrity_summary.failed_repairs,
                    },
                },
                updated_by=updated_by,
            )
        else:
            await self._log_event(
                event_type="evaluation",
                details={"triggered": False, "metrics": metrics},
                updated_by=updated_by,
            )

        return RolloutEvaluationResult(
            samples=samples,
            triggered=triggered,
            rollback_reason=reason,
            metrics=metrics,
        )

    async def _log_event(self, *, event_type: str, details: Dict[str, Any], updated_by: str) -> None:
        await self.db.execute(
            """
            INSERT INTO retrieval_rollout_events (
                event_type,
                details,
                updated_by,
                created_at
            )
            VALUES ($1, $2::jsonb, $3, NOW())
            """,
            _n(event_type),
            details or {},
            _n(updated_by) or "unknown",
        )
