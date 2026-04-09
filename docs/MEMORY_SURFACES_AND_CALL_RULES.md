# Memory Surfaces And Call Rules

Backend-facing rule of thumb: one canonical startup packet, then call targeted surfaces on demand.

## Canonical call map
1. startup -> `GET /session/startbrief`
2. recall -> `POST /memory/query`
3. loops -> `GET /memory/loops`
4. user picture -> `GET /user/model`
5. write-back -> `POST /session/ingest`

## Endpoint roles
- `GET /session/startbrief`: canonical startup packet for stateless continuity and runtime steering.
- `POST /memory/query`: semantic recall for specific natural-language questions.
- `GET /memory/loops`: prioritized procedural memory (commitments/threads/habits/frictions/decisions).
- `GET /user/model`: synthesized durable user profile and completeness/staleness metadata.
- `POST /session/ingest`: canonical durable transcript write-back and enqueue path.

## Non-canonical startup surfaces
- `POST /brief`: internal minimal/fallback startup mode only.
- `GET /session/brief`: legacy/internal compatibility mode only.

## Recommended runtime usage
- Call `/session/startbrief` once at session start.
- Call `/memory/query` only when the assistant needs recall for a concrete question.
- Call `/memory/loops` when procedural follow-through or commitments are relevant.
- Call `/user/model` for durable user picture at startup (or cache per session and refresh as needed).
- Call `/session/ingest` as canonical write-back/finalization for full transcript persistence.
