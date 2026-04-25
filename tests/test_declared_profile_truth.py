import pytest
from httpx import ASGITransport, AsyncClient

from src.main import app, db
from src.derived_pipeline import build_pass4_identity_packet
import uuid


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


@pytest.mark.asyncio
async def test_profile_truth_patch_get_and_history_round_trip():
    tenant_id = "default"
    user_id = _unique("declared-truth-user")

    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            patch_resp = await client.patch(
                "/profile/truth",
                json={
                    "tenantId": tenant_id,
                    "userId": user_id,
                    "sourceSurface": "settings",
                    "updatedBy": "user",
                    "reason": "initial setup",
                    "profile": {
                        "preferred_name": "Kaiser",
                        "location": "Cambridge",
                        "faith": "LDS",
                        "roles": ["founder", "product lead"],
                        "projects": ["Sophie", "Bluum"],
                        "important_people": [
                            {
                                "name": "Jasmine",
                                "relationship": "daughter",
                                "situation": "six year estrangement, early reconciliation",
                                "contact": "light Instagram contact, no expectation of reply",
                                "directive": "handle with extreme care, never probe, let him lead",
                            }
                        ],
                        "notes_for_sophie": "Do not assume feelings in the moment.",
                    },
                },
            )
            assert patch_resp.status_code == 200
            patch_payload = patch_resp.json()
            assert patch_payload["profile"]["preferred_name"] == "Kaiser"
            assert "founder" in patch_payload["profile"]["roles"]
            assert patch_payload["metadata"]["auditCount"] == 1

            get_resp = await client.get(
                "/profile/truth",
                params={"tenantId": tenant_id, "userId": user_id},
            )
            assert get_resp.status_code == 200
            get_payload = get_resp.json()
            assert get_payload["exists"] is True
            assert get_payload["profile"]["projects"] == ["Sophie", "Bluum"]
            person = get_payload["profile"]["important_people"][0]
            assert person["name"] == "Jasmine"
            assert "six year estrangement" in person["situation"]
            assert "never probe" in person["directive"]

            history_resp = await client.get(
                "/profile/truth/history",
                params={"tenantId": tenant_id, "userId": user_id},
            )
            assert history_resp.status_code == 200
            history_payload = history_resp.json()
            assert history_payload["items"][0]["sourceSurface"] == "settings"
            assert "preferred_name" in history_payload["items"][0]["changeSummary"]["added"]


@pytest.mark.asyncio
async def test_pass4_packet_reads_declared_profile_truth_lane():
    tenant_id = "default"
    user_id = _unique("declared-truth-pass4-user")
    async with app.router.lifespan_context(app):
        await db.execute(
            """
            INSERT INTO user_identity (tenant_id, user_id, data, updated_at)
            VALUES ($1,$2,$3::jsonb,NOW())
            ON CONFLICT (tenant_id, user_id)
            DO UPDATE SET data=EXCLUDED.data, updated_at=NOW()
            """,
            tenant_id,
            user_id,
            {
                "declared_profile_truth": {
                    "preferred_name": "Kaiser",
                    "roles": ["founder", "architecture lead"],
                    "projects": ["Sophie", "Bluum"],
                    "writing_or_public_work": ["Substack on theology and psychology"],
                    "important_people": [
                        {
                            "name": "Jasmine",
                            "relationship": "daughter",
                            "situation": "active reconciliation",
                            "directive": "let him lead",
                        }
                    ],
                }
            },
        )
        packet = await build_pass4_identity_packet(
            db=db,
            tenant_id=tenant_id,
            user_id=user_id,
            rows=[],
        )

        assert packet["declared_profile_truth"]["preferred_name"] == "Kaiser"
        assert any(
            fact["fact_type"] == "role" and fact["fact_value"] == "founder"
            for fact in packet["declared_truth_facts"]
        )
        assert any(
            fact["fact_type"] == "work_or_project" and fact["fact_value"] == "Sophie"
            for fact in packet["declared_truth_facts"]
        )
        assert any(
            fact["fact_type"] == "important_person" and "Jasmine" in fact["fact_value"]
            for fact in packet["declared_truth_facts"]
        )
        jasmine_fact = next(
            fact for fact in packet["declared_truth_facts"]
            if fact["fact_type"] == "important_person" and "Jasmine" in fact["fact_value"]
        )
        assert jasmine_fact["metadata"]["directive"] == "let him lead"
