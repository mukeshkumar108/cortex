# Synapse Problem Report (2026-04-09)

This report summarizes the issues raised today in plain language, with concrete evidence and likely causes from the current implementation.

## 1) High-level problem statement

The startup brief quality is unreliable because multiple upstream components can fail or drift, and the final handover generation still allows repetitive, stale, and low-value text to pass through.

## 2) What was wrong in the startup brief output

### 2.1 Handover text was verbose, repetitive, and low-signal

Observed behavior:
- The handover included repeated phrases and duplicated context blocks.
- Several lines added little actionable value (presence/readiness boilerplate).
- The same idea appeared multiple times in slightly different wording.

Impact:
- High cognitive noise for orchestrator and user.
- Important context gets buried.

Likely cause:
- Handover is assembled from many ingredients (`last_thread`, loops, daily analysis, narrative), then rewritten by LLM.
- Current cleanup (`_compress_handover_text`, `_validate_handover_text`) is not strict enough on semantic duplication/redundancy.

### 2.2 "Yesterday themes" and steering note did not reflect yesterday

Observed behavior:
- Returned `yesterday_themes` and `steering_note` were inconsistent with actual April 8 conversations.
- Example output theme/steering was about planning deferral, but transcript content was largely tool/memory/web checks.

Impact:
- Feels like gaslighting: system claims patterns the user did not show yesterday.

Likely causes:
- Daily analysis can fall back to regex/keyword logic (`_fallback_daily_analysis`) with fixed steering text.
- If LLM generation/rejection fails, fallback output can be generic and misleading.

## 3) What we verified for April 8 (expected vs actual)

Investigation date: 2026-04-09.

### 3.1 Actual session evidence (expected input)

- April 8 had 9 session transcripts for the user (UTC day window).
- Raw user messages were dominated by:
  - tool-calling/memory-access checks,
  - weather/headline/current-event checks,
  - explicit probing of whether memory retrieval works,
  - relationship factual correction checks.

Expected daily-analysis style from this evidence:
- Themes around verification/trust/system capability checks.
- Steering around evidence-grounded recall and consistency, not planning deferral.

### 3.2 What system captured

`daily_analysis` row for `analysis_date=2026-04-08` was present, but:
- `source = fallback`
- `confidence = 0.45`
- metadata contained:
  - `input_mode = session_summaries`
  - `session_count = 8`
  - `turn_count = 44`
  - `quality_flag = needs_review`

Mismatch detail:
- 9 sessions existed, but only 8 were included in analysis input.
- One session was missing due to outbox failure (`session_raw_episode` validation error: missing transcript messages), so downstream summary-based analysis lost evidence.

## 4) Stale and corrupted user-model hints

### 4.1 Stale hints were surfaced as if current

Observed stale hints in startbrief context included older items (months old), e.g. old work-context and old relationship framing.

Impact:
- Startup packet looks temporally wrong.
- Assistant behavior can anchor to outdated context.

Likely causes:
- `user_model` fields persisted from older runs and not replaced/invalidated promptly.
- Hint extraction prefers high-confidence entries; old high-confidence items can survive too long.

### 4.2 Relationship field corruption ("was (dad), currently active")

Observed behavior:
- Relationship hint appeared as `Relationship: was (dad), currently active`.

Why this is invalid:
- `was` is not a person entity.

Likely cause:
- Regex-based relationship extraction in the fast updater path can mis-parse free text into `{name, who}` tuples.
- Once written into `user_model.key_relationships`, it can propagate into hints and startbrief output.

## 5) Name handling failure (assistant calling user "User")

Observed behavior:
- Assistant responded with "your name is User" in chat, despite using the actual name earlier.

Likely causes from current design:
- User model has `preferred_name` field in schema but no robust pipeline that reliably extracts, verifies, refreshes, and injects it into runtime prompts.
- Session summary prompts explicitly instruct summarizers not to use names (good for privacy, but removes a reinforcement path).
- When no trusted name is available at runtime, clients can fall back to generic labels like `User`.

Impact:
- Personalization breaks unpredictably across sessions.
- User trust drops sharply because name recall is a basic expectation.

## 6) Entity package quality is inconsistent

Observed behavior:
- Entity hints are sometimes noisy/unclear and not clearly tied to durable relationship/project structure.

Likely causes:
- Entity hints are currently built by a hybrid runtime method:
  - user_model relationships,
  - graph node search,
  - summary text recurrence fallback.
- This is not the same as a dedicated curated entity graph pass over broader history.

Impact:
- Missing/ambiguous roles.
- Non-actionable entities and unclear provenance.

## 7) Core design issues behind these failures

1. Too many fallback paths produce plausible text rather than explicit "insufficient evidence".
2. Some critical fields rely on regex heuristics (relationship/name extraction) instead of grounded evidence validation.
3. Freshness metadata exists in some places but not enforced strongly enough in ranking/output.
4. Pipeline health dependency is brittle: one ingest failure can silently remove sessions from downstream analysis.
5. Final handover generation prioritizes fluency over strict anti-duplication and evidence precision.

## 8) User-visible symptoms to track

- Repetitive/boilerplate handover sentences.
- Yesterday themes that do not match yesterday transcript reality.
- Generic steering note repeated across days.
- Old hints resurfacing without clear freshness warning.
- Relationship entities with malformed names/roles.
- Name recall flips between correct name and "User".

## 9) Severity assessment

- Trust/credibility: high impact.
- Personalization quality: high impact.
- Operational debuggability: medium-high impact.
- Safety risk: medium (false autobiographical claims / stale relational claims).

## 10) Bottom line

The current issue is not one bad sentence. It is a pipeline reliability problem: ingest gaps, fallback-heavy analysis, stale model fields, and weak final de-duplication combine into low-trust startup briefs.

