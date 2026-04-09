# Semantic Rerank Diagnostics

## Scope
- No architecture changes in this pass.
- Focus: diagnostic review + evaluation pressure.
- Inputs reviewed:
  - `docs/semantic_rerank_eval_report.json` (v1, 10 cases)
  - `docs/semantic_rerank_eval_report.v2.json` (expanded, 18 cases)

## 1) Regression Diagnosis + Resolution
- Previously failing cases:
  - `c7_goal_vs_state`
  - `c17_mixed_language_commitment_vs_fear` (expanded pack)
- Root causes identified:
  - Query typing drift on meta-queries (`make_commitment` with weak/ambiguous domain + `state` typing) over-boosted generic `share_update/state`.
  - Dense query-domain focus (too many low-probability domains) introduced noisy bonus.
  - User-model abstract patterns could edge out concrete episode evidence on close scores.
- Fixes applied (semantic-only):
  - Query semantic interpretation is now primary for query routing.
  - Sparse query-domain focus with low-certainty domain damping.
  - Intent-aware compatibility scoring for `make_commitment` / `reflect`.
  - Soft fallback tie-break only when semantic intent is generic (`share_update`) and fallback carries stronger intent signal.
  - Slightly stronger graphiti source prior over user-model for close calls.
- Current status:
  - `v1` regressions: `0/10`
  - `v2` regressions: `0/18`

## 2) Confidence Calibration Review

### v1 (10-case pack)
- Top-1 accuracy: `1.00` (10/10)
- Mean top-1 confidence: `0.695`
- Brier score (top-1 correctness vs confidence): `0.101`
- Distribution:
  - `0.50–0.69`: 4 cases
  - `0.70–0.85`: 6 cases

### v2 (18-case expanded pack)
- Top-1 accuracy: `1.00` (18/18)
- Mean top-1 confidence: `0.698`
- Brier score: `0.097`
- Distribution:
  - `0.50–0.69`: 10 cases
  - `0.70–0.85`: 8 cases

### Interpretation
- Confidence compression improved materially (confidence no longer concentrated in one narrow bucket).
- Still mildly underconfident vs observed top-1 accuracy; additional stress data is needed before further calibration tuning.

## 3) Expanded Eval Pack
- New fixture: `tests/fixtures/semantic_rerank_eval_pack.v2.json`
- Size: 18 cases (10 base + 8 new stress cases)
- Added stress categories:
  - indirect phrasing
  - mixed-language/code-switching
  - shame/self-image
  - overlapping-domain interactions
- New reports:
  - `docs/semantic_rerank_eval_report.v2.json`
  - `docs/semantic_rerank_eval_report.v2.md`

## 4) Known Failure Modes (Current)
- Mixed-language intent ambiguity still appears in candidate-level classification tails (example: romanized phrases classified as `ask_help` or generic `share_update`).
- Abstract query-domain assignments can still be noisy; sparsification reduced but did not eliminate this class.
- Confidence is improved but still optimistic diagnostics are limited by synthetic-case scale and perfect top-1 in current pack.

## Current Direction
- Keep iterating through eval pressure and diagnostics.
- Avoid lexical patching as primary ranking logic.
