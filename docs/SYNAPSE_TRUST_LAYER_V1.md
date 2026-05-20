# Synapse Trust Layer V1

**Status:** Implemented V1  
**Date:** 2026-05-20

## Purpose

Trust Layer V1 exists to stop Sophie from treating stale state as current truth and to give direct user correction precedence over inferred memory.

This is intentionally bounded:

- no Sophie runtime changes
- no outreach delivery
- no prepared-agents implementation
- no full memory deletion system

## What V1 adds

### 1. Unified `timeline_events`

Synapse now has one minimal canonical event table:

- physical table: `timeline_events`
- logical separation: `timeline_type`

Current allowed `timeline_type` values:

- `interaction`
- `user_life`
- `calendar_event`
- `relationship`
- `system`

Why one table now:

- user corrections and state suppressions need one canonical write path
- a single event substrate is faster to ship and inspect than four partially-overlapping tables
- read models can stay disciplined by filtering on `timeline_type`

Why separate tables may still come later:

- higher write volume
- domain-specific retention rules
- tighter constraints per domain

V1 does not need that complexity yet.

### 2. State Decay Guardrails

New endpoint:

- `GET /internal/debug/state-decay`

Rules in V1:

- single-session state is provisional by default
- under 24h may be current if recently supported
- 24-48h is provisional/recent, not strong truth
- over 48h becomes stale unless reinforced
- over 2 weeks becomes historical unless reinforced
- sensitive state should usually become tone guidance, not proactive copy
- handover `recent_state_note` is short-lived continuity only
- `living_context` is a snapshot, not guaranteed truth

The output explicitly includes:

- current notes
- provisional notes
- stale warnings
- historical notes
- suppressions
- tone modifiers
- unsafe-to-surface items
- evidence gaps

Required suppression language:

- `Do not present stale emotional state as current.`

### 3. User Memory Correction Commands

New endpoint:

- `POST /internal/debug/memory/correction`

Supported commands:

- `forget_that`
- `dont_bring_that_up_again`
- `thats_wrong`
- `thats_done`
- `this_is_important`
- `remember_this`

Every correction writes a canonical `timeline_events` row.

Best-effort side effects in V1:

- `dont_bring_that_up_again` can write an `attention_outcomes` suppression
- `thats_done` can mark a matching `action_items` row done when the target is explicit
- `thats_wrong` and `forget_that` do not delete data yet; they record precedence and suppression

## Read-side impact

### Timeline Read Model

`/internal/debug/timeline` now includes `timeline_events` as first-class rows.

It also supports filtering by `timelineType` so mixed event classes do not get inspected blindly.

### Attention Preview

Attention Preview now checks direct user correction/suppression events and suppresses matching items when the match is explicit:

- same `source_table` + `source_id`
- explicit `targetId`
- direct `object_refs` overlap

This is intentionally conservative. If the match is unclear, V1 prefers gaps over broad suppression.

### Daily Overview

Daily Overview now includes `stateDecay` guidance.

Stale emotional state is no longer allowed to become the primary focus for the overview. It appears in warnings/suppressions instead.

## Why this protects Prepared Intelligence later

Prepared Intelligence agents are only safe if the substrate can say:

- what is current
- what is provisional
- what is stale
- what the user explicitly corrected

Trust Layer V1 creates that minimum substrate before agentic preparation expands.

## Current limitations

- no archival/delete propagation for `forget_that`
- no deep semantic suppression propagation
- no mutation of Graphiti/FalkorDB memory from corrections
- no user-facing UI yet
- no separate timeline tables yet
- state decay remains heuristic, but explicit and inspectable
