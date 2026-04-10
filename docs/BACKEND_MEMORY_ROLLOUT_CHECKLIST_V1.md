# Backend Memory Rollout Checklist v1

Scope: production rollout checklist for current memory stack (`exact|episodic|hybrid`) with no ontology/API changes.

## 1) Preconditions (Must Pass)

1. `/memory/query` returns stable `metadata.memoryIntent`, `metadata.episodicWeakRecall`, and episodic audit fields.
2. Episodic path operational for pilot users:
   - `metadata.episodes > 0` on recall-like prompts.
   - `metadata.episodicRanking.audit.embeddingCoverage.rows > 0`.
3. Startup path stable:
   - `GET /session/startbrief` available and latency within SLO.
4. Writeback path stable:
   - `POST /session/ingest` succeeds and outbox drain health is green.

## 2) Feature Flags (Recommended)

1. `memory_router_enabled`:
   - enables backend intent routing (`exact|episodic|hybrid`).
2. `memory_hybrid_default_enabled`:
   - defaults broad recall prompts to `hybrid`.
3. `memory_weak_recall_softening_enabled`:
   - softens assistant phrasing when `episodicWeakRecall=true`.
4. `memory_episodic_evidence_enabled`:
   - injects episodic evidence snippets into model context.

Rollout sequence:
1. Enable router only (no default change).
2. Enable weak-recall softening.
3. Enable episodic evidence injection.
4. Enable hybrid default for broad recall prompts.

## 3) SLOs and Alert Thresholds

Set initial thresholds and tighten after 1-2 weeks.

Availability/latency:
1. `/memory/query` p95 < 900ms for `exact`, < 1300ms for `hybrid`.
2. non-2xx rate < 0.5%.

Quality guardrails:
1. `episodicWeakRecall` overall rate:
   - warn > 35%
   - critical > 50%
2. Embedding coverage health (`rows` and `sessions`):
   - warn if median active-user coverage drops by >30% day-over-day
   - critical if near-zero for active cohort
3. Episodic candidate health:
   - warn if median `metadata.episodes` on recall prompts < 2
4. Correction signal:
   - warn if user correction/contradiction rate increases >15% vs baseline.

## 4) Go/No-Go Criteria

Go:
1. Error/latency SLOs green for 48h.
2. Embedding coverage non-zero and stable for pilot cohort.
3. Weak-recall behavior visible and assistant overconfidence trend reduced.
4. No major regression in relationship-anchored prompts.

No-Go:
1. Episodic candidates frequently collapse to zero.
2. Coverage drops to near-zero for active users.
3. Overconfident wrong-memory incidents increase materially.
4. p95 latency or error rates breach thresholds for >4h.

## 5) Oncall Playbook (Fast Triage)

If weak-recall spikes:
1. Check embedding coverage stats in `metadata.episodicRanking.audit.embeddingCoverage`.
2. Check candidate counts (`metadata.episodes`).
3. Verify recent Graphiti episode retrieval health for affected tenant/user.
4. If needed, temporarily route recall-heavy prompts to `exact`/`hybrid` with softening enabled.

If episodic candidates drop to zero:
1. Verify Graphiti recent episode retrieval path.
2. Verify embedding lookup path and table freshness.
3. Run backfill for impacted users/sessions.
4. Keep assistant on fallback behavior (ask clarifying follow-up; avoid confident memory claims).

If latency regresses:
1. Reduce `limit` for memory queries.
2. Lower episodic candidate fanout in backend configuration.
3. Keep hybrid default only for clearly recall-like prompts until latency recovers.

## 6) Logging Contract (Minimum)

Per `/memory/query` call, log:
1. `memoryIntent`
2. `queryProfile` (if present)
3. `episodes` count
4. `episodicWeakRecall` and reason
5. top episodic score and score components (if present)
6. embedding coverage rows/sessions
7. downstream user correction flag (if available)

## 7) Weekly Review Template

1. Prompt classes:
   - relationship
   - reflective
   - continuation
   - default
2. For each class:
   - weak-recall rate
   - top-1 usefulness sample review
   - correction rate
   - latency
3. Decide one narrow next action only:
   - ranking tweak
   - span/window tweak
   - or model bakeoff (only if retrieval substrate is healthy and gains plateau)

## 8) Rollback Plan

1. Disable `memory_hybrid_default_enabled`.
2. Keep routing but bias to `exact` for safety-critical traffic.
3. Keep `memory_weak_recall_softening_enabled` on.
4. Continue ingest/writeback so retrieval quality can recover without data loss.

Reference:
- `docs/BACKEND_MEMORY_INTEGRATION_GUIDE_V1.md`
