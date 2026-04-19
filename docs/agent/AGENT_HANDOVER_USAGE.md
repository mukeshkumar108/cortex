# Agent Handover Usage

Purpose: show exactly how an agent should use `GET /session/handover`.

## Request

`GET /session/handover?user_id=<USER_ID>`

Fallback accepted: `userId`.

## Minimal Read Order

1. `living_context`
2. `sophie_directives`
3. `active_contradictions`
4. `open_threads` (top 5)
5. `people`
6. `projects`
7. `identity`
8. `episodic_recall` (for targeted recall)

## Response Use Pattern

1. Set response stance from:
- `living_context.emotional_texture`
- `living_context.primary_tension`
- `living_context.relationship_pulse`

2. Apply directive constraints:
- Treat every `sophie_directives[*].directive` as binding.

3. Select continuity targets:
- Prioritize `open_threads` where `thread_type='persistent_goal'` or highest `salience`.

4. Ground references:
- Pull names/status from `people` and `projects`.
- Use `active_contradictions` to avoid one-sided summaries.

5. If user asks “what do you remember about X”:
- Call `POST /memory/search` with X-focused query.
- Cite concise evidence from returned `memory_deltas`.

## Output Behavior Rules

- Be specific, brief, and evidence-linked.
- Avoid generic motivational filler.
- If confidence is low, state uncertainty in one sentence and ask a focused follow-up.
