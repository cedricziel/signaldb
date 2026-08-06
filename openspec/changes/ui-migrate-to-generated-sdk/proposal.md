## Why

`admin-management-api-contract` already requires the web UI to reach
tenant-management operations "through the generated TypeScript client...
issuing no raw HTTP request" — but five modules in `src/ui/src/api`
(`tempo.ts`, `loki.ts`, `prom.ts`, `pyroscope.ts`, `session.ts`) still call
`fetch()` directly against their respective compat/session endpoints, because
either the generated client already covered their endpoints and nobody
switched (`tempo.ts`), or the endpoints weren't in the spec at all until
`spec-cover-compat-endpoints` closed that gap. This change finishes what that
existing requirement already asked for, extending it explicitly to the
query-compat and session surfaces.

## What Changes

- Swap `tempo.ts`, `loki.ts`, `prom.ts`, `pyroscope.ts`, and `session.ts` from
  hand-written `fetch()` calls onto the generated SDK functions in
  `src/api/gen/sdk.gen.ts` (depends on `spec-cover-compat-endpoints` for
  `loki`/`prom` metadata, `pyroscope`, and `session`; `tempo.ts` can migrate
  independently since its operations are already generated).
- Each file's exported functions keep their current signatures and
  domain-shaped return types (`TempoTrace`, `LogRow`, `PromSeries`,
  `Flamebearer`, `WhoamiResponse`, etc.) — callers elsewhere in the UI are
  unaffected.
- Hand-written request/response types that only mirrored the generated
  operation's shape (renames, subsetting) are deleted in favor of the
  generated types; hand-written types encoding real computation
  (`flattenAttrs`/`rootSpan` in `tempo.ts`, ms↔ns conversion and stream
  merge/sort in `loki.ts`, flamebearer delta-decoding in `pyroscope.ts`)
  survive as a thin adapter layer over the generated call.
- `ApiError` construction moves from each file's bespoke
  `<protocol>Fetch()` helper to reading the generated client's error
  response via its interceptor/error shape, preserving the existing
  `ApiError`/`isAuthError` contract other UI code depends on.

Not breaking: no user-visible behavior changes — same requests, same
responses, same call signatures from the rest of the UI's point of view.

## Capabilities

### New Capabilities

- `ui-generated-client-only`: the web UI reaches every SignalDB HTTP
  capability — tenant/dataset management, query-compat (Tempo/Loki/
  Prometheus/Pyroscope), and session/whoami — exclusively through the
  generated TypeScript client, with no hand-written `fetch()` against a
  SignalDB endpoint anywhere in application code.

### Modified Capabilities

(none — `admin-management-api-contract`'s existing generated-client
requirement for tenant-management operations is unchanged; this change
satisfies a sibling requirement for the surfaces it didn't cover.)

## Impact

- **src/ui**: `src/api/tempo.ts`, `src/api/loki.ts`, `src/api/prom.ts`,
  `src/api/pyroscope.ts`, `src/api/session.ts`, and their test files
  (`loki.test.ts`, `prom.test.ts`, `session.test.ts`, `tempo.test.ts`) —
  tests move from mocking `global.fetch` to mocking the generated client's
  underlying fetch (or the `client` object directly).
- No backend or SDK changes; depends on `spec-cover-compat-endpoints` having
  already regenerated `src/ui/src/api/gen` with real operations/types for
  loki/prom metadata, pyroscope, and session.
