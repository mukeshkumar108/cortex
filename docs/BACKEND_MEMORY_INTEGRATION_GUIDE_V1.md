# Backend Memory Integration Guide v1

Purpose: productize the current memory stack without changing ontology, endpoints, or architecture.

Applies to:
- `GET /session/startbrief`
- `POST /memory/query` (`memoryIntent=exact|episodic|hybrid`)
- `POST /entities/profile`
- `GET /memory/loops`
- `POST /session/ingest`

## 1) Intent Routing Rules (Backend)

Use a lightweight router. No large classifier required.

1. Route to `exact` when the user asks for identity/factual grounding.
2. Route to `episodic` when the user asks to recall a past conversation/thread/moment.
3. Route to `hybrid` when the prompt mixes person/project anchors with conversational recall.

Practical prompt mapping:
- `exact`
  - "who is Ashley?"
  - "what is Bluum?"
  - "what are my open goals?"
- `episodic`
  - "remember that conversation we had about God?"
  - "let's continue that thread from before"
  - "remember that idea I had?"
- `hybrid`
  - "what was I saying about Jasmine and the future?"
  - "what do you remember about Ashley lately?"
  - "what did we decide about memory as a moat?"

Minimal backend heuristic:
- If query contains continuation/recall phrases (`remember`, `other day`, `that thread`, `continue`, `we were exploring`) and has no clear entity anchor -> `episodic`.
- If query is mostly identity/fact style (`who is`, `what is`, `status of`, `relationship to`) -> `exact`.
- Otherwise, default to `hybrid` for broad recall prompts.

## 2) Recommended Product Defaults

1. Default memory intent for broad recall: `hybrid`.
2. Prefer `exact` for short direct fact lookups.
3. Prefer `episodic` for explicit continuation prompts with weak entity anchors.
4. Treat `metadata.episodicWeakRecall=true` as a guardrail signal, not an error.
5. If weak recall and exact facts are also sparse, fall back to normal chat behavior and ask a clarifying question.

Recommended runtime policy:
- If `memoryIntent=hybrid` and `episodicWeakRecall=true`:
  - keep exact facts/entities if present
  - do not force "I remember clearly..." style language
  - prefer "I may be missing details; can you remind me which part?"

## 3) Runtime Response Handling

Use `/memory/query` response as structured retrieval context, not raw prompt dump.

Injection priorities:
1. Exact grounding:
   - take top `factItems` (2-6), prefer higher confidence/relevance and stronger provenance.
2. Episodic grounding:
   - take top `episodes` (1-3), include `summary` + 1-2 `evidence` lines.
3. Entity grounding:
   - include top linked entities from episode candidates and top response entities (up to ~6 total names).
4. Procedural grounding:
   - if user asks about commitments/next steps, call `/memory/loops` and include top 1-3 loops.

Prompt budget guidance:
- Keep memory payload compact (~250-600 tokens total).
- Avoid injecting full transcripts.
- Use evidence lines as citation anchors, not as narrative replacements.

When `weakRecall=true`:
- keep tone confident about what is known, uncertain about what is weak.
- do not fabricate a specific prior conversation.
- ask for one disambiguating detail if needed (timeframe/topic/person).

## 4) Failure Behavior (Trustworthy)

Exact weak:
- Behavior: "I have limited exact memory on that. I can still help if you restate the key fact."
- Action: optionally rerun with `hybrid` if not already used.

Episodic weak:
- Behavior: "I can’t confidently locate that prior thread yet."
- Action: use available exact facts/entities and ask a narrow follow-up.

Hybrid partial:
- Behavior: return what is strongly grounded, label uncertain parts as tentative.
- Action: prefer linked entity facts + top episodic evidence snippet; avoid over-claiming.

Low-confidence memory hit:
- Behavior: present as possible, not certain.
- Action: ask for confirmation before making consequential follow-on claims.

## 5) Rollout Guardrails and Monitoring

Track by endpoint + prompt class (`relationship`, `reflective`, `continuation`, `default` from metadata when available).

Core metrics:
1. `episodicWeakRecall` rate (overall and by prompt class).
2. Episodic candidate count (`metadata.episodes`) distribution.
3. Embedding coverage (`metadata.episodicRanking.audit.embeddingCoverage.rows/sessions`).
4. Top episodic score distribution (`metadata.episodicRanking.audit.topScores`).
5. Query profile mix (`metadata.episodicRanking.queryProfile`).
6. Memory intent success by route (`exact|episodic|hybrid`) and downstream user correction rate.

Operational guardrails:
1. Alert if embedding coverage drops toward zero for active users.
2. Alert if weak-recall spikes for one prompt class.
3. Alert if episodic candidates collapse to zero while Graphiti episodes still exist.
4. Sample logs for false-positive confident recall (quality review queue).

Recommended rollout:
1. Start with `hybrid` default for broad recall prompts behind a flag.
2. Compare user correction rate vs previous behavior.
3. Gradually raise traffic after weak-recall and coverage metrics are stable.

## 6) Minimal Backend Contract (Production)

Session start:
1. Call `GET /session/startbrief`.
2. Inject `handover_text`, `narrative`, `entity_hints`, and compact `ops_context`.

Profile deep-dive (on demand):
1. Call `POST /entities/profile` for entity cards only when needed for reasoning.

Recall:
1. Call `POST /memory/query` with routed `memoryIntent`.
2. Read:
   - exact: `factItems`, `entities`
   - episodic: `episodes`, `metadata.episodicWeakRecall`, `metadata.episodicRanking`
   - hybrid: all above

Loops:
1. Call `GET /memory/loops` when task/commitment continuity matters.

Writeback:
1. Call `POST /session/ingest` with full transcript at session end (or canonical ingest policy).

Example recall request:
```json
{
  "tenantId": "default",
  "userId": "user_123",
  "query": "what was I saying about Jasmine and the future?",
  "memoryIntent": "hybrid",
  "limit": 8
}
```

Example handling summary:
- If `episodes > 0` and `episodicWeakRecall=false`: include top episodic summary + evidence lines.
- If `episodicWeakRecall=true` but exact facts exist: answer from exact facts and mark episodic uncertainty.
- If both exact and episodic are weak: ask a short clarifying follow-up instead of inventing continuity.

## 7) Small Internal Helpers (No New Public Endpoints)

Recommended internal-only helpers:
1. `route_memory_intent(query) -> exact|episodic|hybrid`
2. `build_memory_context(memory_response, token_budget)`
3. `should_soften_memory_claims(memory_response)` based on weak-recall + score bands
4. `log_memory_audit(memory_response, prompt_class, outcome)` for rollout dashboards

These helpers stay inside backend orchestration; public API remains unchanged.

## 8) Rollout checklist

Use the companion launch checklist for flags, thresholds, go/no-go criteria, and oncall triage:
- `docs/BACKEND_MEMORY_ROLLOUT_CHECKLIST_V1.md`
