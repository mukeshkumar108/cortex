# Memory Quality Scorecard

Use this scorecard to evaluate long-term memory quality for premium voice.

## 1) Recall Precision
- Definition: returned facts that are relevant to the query intent.
- Metric: `relevant_fact_items / total_fact_items`.
- Target: `>= 0.85`.

## 2) Exact Fact Hit Rate
- Definition: expected critical fact appears in top-K facts.
- Metric: `% test cases with exact/near-exact fact in top-K`.
- Target: `>= 0.90` for K=5 on curated production cases.

## 3) Provenance Coverage
- Definition: critical facts include explicit source provenance.
- Metric: `% factItems with non-empty source + sourceTenant`.
- Target: `100%`.

## 4) Cross-Surface Coherence
- Definition: no contradiction between top recall facts and user_model/session summary.
- Metric: contradiction count per 100 eval runs.
- Target: `< 2/100`.

## 5) Freshness Lag
- Definition: delta between latest transcript event and retrievable semantic evidence.
- Metric: p50/p95 lag seconds.
- Target: p50 `< 60s`, p95 `< 300s`.

## 6) Latency
- Definition: `/memory/query` end-to-end response time.
- Metric: p50/p95 ms.
- Target: p50 `< 400ms`, p95 `< 1200ms` (production network included).

## 7) Domain Tag Quality
- Definition: correctness of `factItems[].domain`.
- Metric: macro-F1 on labeled eval set.
- Target: `>= 0.80` initially, improve with labeled data.

## 8) Tenant Scope Reliability
- Definition: alias/canonical split does not hide expected facts.
- Metric: alias test pass rate.
- Target: `100%`.

## Operational cadence
- Run fixture-driven evaluator daily and before deploy.
- Keep a rolling dashboard and alert on target misses.
