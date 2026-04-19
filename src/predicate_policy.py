from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable

from .canonicalization import normalize_text
from .db import Database


class PredicatePolicyError(ValueError):
    """Base error for predicate policy lookup failures."""


class MissingPolicyVersionError(PredicatePolicyError):
    """Raised when callers do not provide a required policy version."""


class PolicyVersionNotFoundError(PredicatePolicyError):
    """Raised when a policy version is not registered."""


class PredicateNotFoundError(PredicatePolicyError):
    """Raised when a predicate is unknown for a known policy version."""


def _equivalent_strict(left: Any, right: Any) -> bool:
    return left == right


def _equivalent_normalized_text(left: Any, right: Any) -> bool:
    return normalize_text(left, casefold=True) == normalize_text(right, casefold=True)


def _equivalent_canonical_json(left: Any, right: Any) -> bool:
    return json.dumps(left, sort_keys=True, separators=(",", ":"), ensure_ascii=False) == json.dumps(
        right,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


OBJECT_EQUIVALENCE_RULE_HOOKS: dict[str, Callable[[Any, Any], bool]] = {
    "strict": _equivalent_strict,
    "normalized_text": _equivalent_normalized_text,
    "canonical_json": _equivalent_canonical_json,
}


@dataclass(frozen=True)
class PredicatePolicy:
    policy_version: str
    predicate: str
    cardinality: str
    conflict_mode: str
    expected_subject_kind: str
    expected_object_kind: str
    object_equivalence_rule: str

    def object_equivalence_hook(self) -> Callable[[Any, Any], bool]:
        return OBJECT_EQUIVALENCE_RULE_HOOKS.get(self.object_equivalence_rule, _equivalent_strict)


class PredicatePolicyService:
    def __init__(self, db: Database):
        self.db = db

    async def get_current_policy_version(self) -> str:
        row = await self.db.fetchone(
            """
            SELECT policy_version
            FROM predicate_policy_versions
            WHERE status = 'active'
            LIMIT 1
            """
        )
        if not row or not row.get("policy_version"):
            raise PolicyVersionNotFoundError("no active predicate policy version is registered")
        return str(row["policy_version"])

    async def get_policy(self, *, predicate: str, policy_version: str) -> PredicatePolicy:
        normalized_predicate = normalize_text(predicate, casefold=True)
        normalized_version = normalize_text(policy_version, casefold=False)
        if not normalized_version:
            raise MissingPolicyVersionError("policy_version is required")
        if not normalized_predicate:
            raise PredicateNotFoundError("predicate is required")

        row = await self.db.fetchone(
            """
            SELECT
              p.policy_version,
              p.predicate,
              p.cardinality,
              p.conflict_mode,
              p.expected_subject_kind,
              p.expected_object_kind,
              p.object_equivalence_rule
            FROM predicate_policy p
            WHERE p.policy_version = $1
              AND p.predicate = $2
            LIMIT 1
            """,
            normalized_version,
            normalized_predicate,
        )
        if row:
            return PredicatePolicy(
                policy_version=str(row["policy_version"]),
                predicate=str(row["predicate"]),
                cardinality=str(row["cardinality"]),
                conflict_mode=str(row["conflict_mode"]),
                expected_subject_kind=str(row["expected_subject_kind"]),
                expected_object_kind=str(row["expected_object_kind"]),
                object_equivalence_rule=str(row["object_equivalence_rule"]),
            )

        version_exists = await self.db.fetchval(
            """
            SELECT 1
            FROM predicate_policy_versions
            WHERE policy_version = $1
            LIMIT 1
            """,
            normalized_version,
        )
        if not version_exists:
            raise PolicyVersionNotFoundError(
                f"unknown predicate policy version: {normalized_version}"
            )

        raise PredicateNotFoundError(
            f"unknown predicate '{normalized_predicate}' for policy version '{normalized_version}'"
        )
