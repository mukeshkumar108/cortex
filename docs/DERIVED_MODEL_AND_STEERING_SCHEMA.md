# Derived Model + Steering Schema (Draft)

Status: draft-only, not required by runtime paths.

## Purpose
- Provide a compact derived layer for runtime steering experiments.
- Keep retrieval/reranking independent until semantic stability is stronger.

## Models
- `DerivedUserModel`:
  - `schemaVersion`
  - `generatedAt`
  - `userId`, `tenantId`
  - `focusDomains[]`
  - `dominantIntents[]`
  - `activeSignals[]` (`DerivedSignal`)
  - `confidence`
  - `provenance`, `metadata`

- `RuntimeSteeringPacket`:
  - `schemaVersion`
  - `generatedAt`
  - `userId`, `tenantId`
  - `query`, `queryDomain`, `queryIntent`, `queryMemoryType`
  - `queryDomainFocus[]`
  - `retrievalConfidence`
  - `riskFlags[]`
  - `steeringHints[]`
  - `constraints`, `metadata`

## Contract Notes
- Schemas are defined in `src/models.py` only.
- No endpoint hard dependency yet.
- Intended use for offline evaluation or optional telemetry side-channel first.
