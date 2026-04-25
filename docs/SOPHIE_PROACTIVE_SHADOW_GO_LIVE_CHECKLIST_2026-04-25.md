# Sophie Proactive Shadow Go-Live Checklist - 2026-04-25

## Purpose

This is a strict go/no-go checklist for moving from baseline hardening into proactive queue behavior.

Rule:
- No "mostly done."
- Every gate is binary pass/fail with evidence.
- Ship only when all required gates pass.

## Scope

Shadow mode only for proactive surfacing:
- `follow_up_candidates`
- `clarification_candidates`
- `recent_change_candidates`

Shadow mode means candidates are generated, scored, and logged, but not user-delivered.

## One-Week Plan (Parallel Tracks)

Track A: remaining conservative baseline gates  
Track B: proactive candidate shadow validation

Day 1-2:
- Enable/verify shadow candidate generation and logging.
- Run targeted baseline tests and gather first shadow outputs.

Day 3-4:
- Fix false positives/priority mistakes from shadow review.
- Complete remaining retrospective/temporal reinforcement tasks queued in tracker.

Day 5:
- Re-run checklist gates and capture artifacts.

Day 6:
- Final pass/fail review against this checklist.

Day 7:
- Go/No-Go decision.

## Required Go-Live Gates

### Gate 1: Pass 4 output is plain identification, not interpretation

Pass condition:
- Pass 4 outputs avoid motive inference / personality verdict phrasing.
- Identity statements are evidence-grounded and role/commitment oriented.

Evidence required:
- Test pass for prompt/validator constraints.
- At least 10 sampled pass4 outputs reviewed with no disallowed style regressions.

Status: [ ]

### Gate 2: Relationship tiers exclude contextual entities from packets unless promoted

Pass condition:
- Tier 3 contextual entities are excluded unless promotion criteria are met.
- Packet people list is sorted/filtered by tier + importance logic.

Evidence required:
- Automated test pass for contextual filter behavior.
- Manual sample review of packet people sections across at least 10 users/sessions.

Status: [ ]

### Gate 3: Declared truth overrides derived state

Pass condition:
- When declared truth conflicts with inferred/derived data, declared truth wins in packet and pass4 grounding.

Evidence required:
- Automated tests for declared truth lane + packet precedence pass.
- Manual conflict case review (at least 5 cases) with expected precedence.

Status: [ ]

### Gate 4: Always-on packet is compact and runtime-useful

Pass condition:
- Packet text remains under target envelope (roughly <=500 tokens).
- Sections remain operationally useful and stable.

Evidence required:
- Automated checks for packet build path and persistence.
- Sampled token-length checks on at least 20 generated packets.
- Manual quality spot-check notes attached.

Status: [ ]

### Gate 5: Proactive candidate views populate correctly in shadow

Pass condition:
- All three candidate views populate with expected rows.
- Scores/ranking are present and interpretable.
- No user-facing proactive emissions occur from shadow-only runs.

Evidence required:
- SQL/output snapshots for:
  - `follow_up_candidates`
  - `clarification_candidates`
  - `recent_change_candidates`
- At least 1 week shadow log review with daily summary.

Status: [ ]

### Gate 6: Relationship-state facts are not softened

Pass condition:
- Concrete terms like "estranged" / "long distance" are preserved where present.
- No euphemizing or diplomatic rewrite drift in pass4/pass5/packet outputs.

Evidence required:
- Prompt-constraint tests pass.
- Manual review of at least 20 outputs containing relationship-state terms.

Status: [ ]

## Baseline Exit Gates (Must Also Be Queued/Tracked)

These are already identified in implementation tracker and must be explicitly queued before proactive rollout:
- Broader tentative-entity reinterpretation.
- Broader contradiction/transition reinterpretation.
- Thread-level reinterpretation for weak mistaken premises.
- Temporal reinforcement completion:
  - high-impact single-event anchors,
  - anti-recency dominance,
  - anti-stale-frequency dominance.

Reference:
- `docs/SOPHIE_MEMORY_IMPLEMENTATION_TRACKER.md`

## Go/No-Go Decision Record

Date:
- 

Decision:
- GO / NO-GO

Failed gates (if any):
- 

Follow-up actions:
- 
