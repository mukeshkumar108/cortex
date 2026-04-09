# Memory Surfaces And Call Rules

Backend-facing rule of thumb: one canonical startup packet, then call targeted surfaces on demand.

## Canonical call map
1. startup -> `GET /session/startbrief`
2. recall -> `POST /memory/query`
3. loops -> `GET /memory/loops`
4. user picture -> `GET /user/model`
5. entity deep-dive -> `POST /entities/profile`
6. write-back -> `POST /session/ingest`

## Endpoint roles
- `GET /session/startbrief`: canonical startup packet for stateless continuity and runtime steering.
- `POST /memory/query`: semantic recall for specific natural-language questions.
- `GET /memory/loops`: prioritized procedural memory (commitments/threads/habits/frictions/decisions).
- `GET /user/model`: synthesized durable user profile and completeness/staleness metadata.
- `POST /entities/profile`: compact entity identity card (person/project/company/place/other) with facts, optional relevant loops, and provenance.
- `POST /session/ingest`: canonical durable transcript write-back and enqueue path.

## Non-canonical startup surfaces
- `POST /brief`: internal minimal/fallback startup mode only.
- `GET /session/brief`: legacy/internal compatibility mode only.

## Recommended runtime usage
- Call `/session/startbrief` once at session start.
- Call `/memory/query` only when the assistant needs recall for a concrete question.
- Call `/memory/loops` when procedural follow-through or commitments are relevant.
- Call `/user/model` for durable user picture at startup (or cache per session and refresh as needed).
- Use `/session/startbrief.entity_hints` for ambient grounding, then call `/entities/profile` only for entities you need to reason about deeply.
- Call `/session/ingest` as canonical write-back/finalization for full transcript persistence.
