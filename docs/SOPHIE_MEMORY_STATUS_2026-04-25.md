# Sophie Memory Status - 2026-04-25

## Snapshot

This status note captures the work completed after the 2026-04-22 checkpoint.

As of 2026-04-25:

- `main` has the latest memory pipeline changes pushed to GitHub.
- The recent focus areas were pass quality hardening, relationship-tier handling, declared-profile truth lane, always-on packet building, and schema/migration alignment.

## What Changed Since 2026-04-22

### 1) Pass quality and evidence precedence

- Pass 1, Pass 3, Pass 4, and Pass 5 prompts were tightened to reduce interpretation drift.
- Routing hints are now explicitly treated as hints, not authority, in synthesis prompts.
- Pass 4 identity synthesis now consumes:
  - declared profile truth (highest authority lane),
  - durable profile facts,
  - evidence-grounded session excerpts.

### 2) Relationship tier model

- Introduced `src/relationship_tiers.py` with tiered relationship classification.
- Tier rank is now used in ranking/filtering logic for packet/synthesis entity selection.
- Tier constraints are enforced for contextual entities before inclusion.

### 3) Pass 1.5 relationship-tier wiring

- Tier signal now influences Pass 1.5 entity persistence behavior:
  - score seeding floors for salience/importance by tier,
  - guarded promotion from tentative to active for higher-confidence meaningful/core roles.
- Added test coverage validating tier-driven scoring/promotion behavior.

### 4) Declared profile truth lane

- Added normalized declared-profile-truth extraction and merge helpers:
  - `src/declared_profile_truth.py`
- Added truth-history audit storage migration:
  - `migrations/049_declared_profile_truth_events.sql`
- Added tests:
  - `tests/test_declared_profile_truth.py`

### 5) Always-on memory packet

- Added always-on packet table migration:
  - `migrations/047_always_on_memory_packets.sql`
- Added packet build/rewrite/persist flow and debug endpoint in `src/main.py`.
- Packet section ordering and behavior align with current design:
  - enduring identity,
  - important people,
  - work and building,
  - current chapter,
  - handle carefully.

### 6) Identity anchors and durable profile facts

- Added durable profile facts table migration:
  - `migrations/048_pass4_identity_anchors.sql`
- Added packet and retrospective wiring to use durable anchors/facts in pass flows.

### 7) Schema consistency

- Synced `schema.sql` with new additive tables:
  - `always_on_memory_packets`
  - `durable_profile_facts`
  - `declared_profile_truth_events`

### 8) Documentation updates

- Updated:
  - `docs/SOPHIE_MEMORY_IMPLEMENTATION_TRACKER.md`
  - `docs/SOPHIE_MEMORY_SYSTEM_SIMPLE.md`
- Added:
  - `docs/SOPHIE_ALWAYS_ON_MEMORY_PACKET_SPEC.md`

## Validation Notes

Recent targeted validations passed after final fixes:

- `tests/test_declared_profile_truth.py`
- targeted hardening tests for:
  - packet compiler behavior,
  - always-on packet filters/shape,
  - pass1.5 tiered relationship promotion/scoring.

Also fixed a test bug in `tests/test_derived_pipeline_hardening.py` (`tenant_id` NameError in packet compiler test setup).

## Current Known State

- Push status: up to date on GitHub as of 2026-04-25.
- Working tree: clean after push.
- Design direction: continue conservative, evidence-backed serving with clear lane precedence and compact always-on runtime orientation.

## Next Work (Short Horizon)

1. Continue retrospective conservative reinterpretation extensions (remaining tracker items).
2. Keep packet quality stable while widening real-data coverage.
3. Keep a dated status doc per major checkpoint to avoid losing cross-day context.
