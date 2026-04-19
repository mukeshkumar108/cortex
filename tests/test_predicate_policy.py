import pytest
import pytest_asyncio

from src.db import Database
from src.main import app
from src.predicate_policy import (
    PolicyVersionNotFoundError,
    PredicateNotFoundError,
    PredicatePolicyService,
)


@pytest_asyncio.fixture
async def predicate_policy_service():
    async with app.router.lifespan_context(app):
        pass
    database = Database()
    service = PredicatePolicyService(database)
    try:
        yield service
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_predicate_policy_known_predicate_lookup(predicate_policy_service: PredicatePolicyService):
    policy = await predicate_policy_service.get_policy(
        predicate="relationship.status",
        policy_version="v2.p1",
    )
    assert policy.policy_version == "v2.p1"
    assert policy.predicate == "relationship.status"
    assert policy.cardinality == "one"
    assert policy.conflict_mode == "supersede"
    assert policy.expected_subject_kind == "entity"
    assert policy.expected_object_kind == "entity"
    assert policy.object_equivalence_rule == "normalized_text"


@pytest.mark.asyncio
async def test_predicate_policy_unknown_predicate_failure(predicate_policy_service: PredicatePolicyService):
    with pytest.raises(PredicateNotFoundError):
        await predicate_policy_service.get_policy(
            predicate="unknown.predicate",
            policy_version="v2.p1",
        )


@pytest.mark.asyncio
async def test_predicate_policy_unknown_version_failure(predicate_policy_service: PredicatePolicyService):
    with pytest.raises(PolicyVersionNotFoundError):
        await predicate_policy_service.get_policy(
            predicate="relationship.status",
            policy_version="v2.missing",
        )


@pytest.mark.asyncio
async def test_predicate_policy_versioned_behavior_lookup(predicate_policy_service: PredicatePolicyService):
    v1 = await predicate_policy_service.get_policy(
        predicate="relationship.status",
        policy_version="v2.p1",
    )
    v2 = await predicate_policy_service.get_policy(
        predicate="relationship.status",
        policy_version="v2.p2",
    )
    assert v1.cardinality == "one"
    assert v1.conflict_mode == "supersede"
    assert v2.cardinality == "many"
    assert v2.conflict_mode == "coexist"


@pytest.mark.asyncio
async def test_predicate_policy_current_version_contract(predicate_policy_service: PredicatePolicyService):
    current_version = await predicate_policy_service.get_current_policy_version()
    assert isinstance(current_version, str)
    assert current_version
