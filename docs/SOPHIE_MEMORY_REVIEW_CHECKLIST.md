# Sophie Memory Live Review Checklist

Use this when reviewing live-model outputs from `scripts/inspect_derived_pipeline_fixture.py`.

## Core Questions

- Does this feel specific to this person or session window?
- Does it preserve contradiction rather than flatten it?
- Does it notice silence/reactivation correctly where relevant?
- Does `startbrief` feel like Sophie knows the person?
- Does `handover` surface what actually matters, not just what is recent?
- Does anything feel generic, clinical, surveillance-like, or interchangeable with another user?

## Output Areas To Inspect

- Pass 1: memory deltas, entity mentions, thread signals, identity signals, emotional/tension fields.
- Entity profiles: relationship/status, profile prose, key facts, open questions.
- Threads: title/detail, lifecycle state, whether unresolved motion is preserved.
- Identity profile: `who_they_are`, `core_values`, `recurring_patterns`, `current_chapter`.
- Living context: `current_focus`, `primary_tension`, `emotional_texture`, `relationship_pulse`.
- Startbrief: compactness, specificity, correct entity hints, no canonical-serving leakage.
- Handover: unresolved threads, active contradictions, Sophie directives, provenance.
- Evidence refs: high-signal assertions should have source sessions/turn refs.

## Review Flags Are Prompts, Not Failures

The live review script flags generic phrasing, missing fixture terms, low entity/thread specificity, and near-identical living-context prose across fixtures. Treat these as prompts for human inspection, not automatic CI failures.
