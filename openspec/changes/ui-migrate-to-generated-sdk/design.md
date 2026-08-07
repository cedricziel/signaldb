## Context

`src/api/client.ts` already configures the shared generated client
(`src/api/gen/client.gen.ts`): same-origin base URL and a request interceptor
that attaches `X-Tenant-ID`/`X-Dataset-ID` via `tenantHeaders()`. Most of the
UI's tenant/dataset/API-key/query-IR traffic already flows through
`src/api/gen/sdk.gen.ts`. The five remaining hand-written clients
(`tempo.ts`, `loki.ts`, `prom.ts`, `pyroscope.ts`, `session.ts`) each define
their own `<protocol>Fetch()` helper that builds a URL, attaches
`tenantHeaders()`, and throws `ApiError` on a non-OK response — that pattern
is what the generated client's interceptor already replaces.

`@hey-api/client-fetch`'s default `fetch()` credentials mode is
`same-origin`; since the UI is served same-origin from the router, cookie
auth (from `spec-cover-compat-endpoints`) should reach the generated client
with no extra configuration. This needs an empirical check rather than an
assumption — see Open Questions.

See `proposal.md` for the motivating requirement and Impact for the exact
file list.

## Goals / Non-Goals

**Goals:**

- Each of the five files' exported functions keeps its current signature and
  domain-shaped return type; nothing outside `src/api/*` changes.
- Every remaining raw `fetch()` call against a SignalDB endpoint is removed.
- Hand-written types that add no value over the generated type are deleted,
  per `ui-generated-client-only`'s adapter requirement.

**Non-Goals:**

- Adding the lint gate that prevents regression — `ui-enforce-sdk-only-http`.
- Changing any request/response wire behavior.
- Touching `client.ts`'s interceptor setup beyond what's needed for error
  handling parity (see Decisions).

## Decisions

**Migrate file-by-file, tempo.ts first.** `tempo.ts`'s operations
(`search`, `search/tags`, `search/tag/{name}/values`, `traces/{id}`) are
already generated with real types — no dependency on
`spec-cover-compat-endpoints`. Migrating it first validates the adapter
pattern (keep `flattenAttrs`/`rootSpan`/`toSpan`, replace `tempoFetch`) on
the smallest-risk file before repeating it for loki/prom/pyroscope/session,
which do depend on that change having landed.

**Preserve `ApiError`/`isAuthError` by wrapping the generated client's
thrown error, not by replacing it.** `@hey-api/client-fetch` throws (or
returns, depending on `throwOnError` config) an error shape derived from the
operation's declared error responses, not a SignalDB `ApiError`. Each
migrated function catches that and re-throws `ApiError` with the same
`message`/`status` shape existing callers already switch on (`isAuthError`
checks `status === 401`), so no caller elsewhere in the UI needs to change.

**Keep the five files as the domain boundary, not the generated client
directly.** Components already import `lokiQueryLogs`, `tempoGetTrace`, etc.
— not raw SDK functions — because those names encode intent (`lokiQueryLogs`
vs. a generic `getLokiApiV1QueryRange`) and because the adapter logic
(`flattenAttrs`, stream merge/sort) has to live somewhere. Renaming call
sites throughout the UI is out of scope; only the _implementation_ inside
these five files changes.

**Delete hand-written types with no computation over the generated type;
keep the rest.** Concretely, expected to disappear: `SessionResult`,
`WhoamiResponse` and its nested types (`session.ts`), `PromMatrixResult`/
`PromResponse` (`prom.ts`), `LokiStreamResult`/`LokiMatrixResult`/
`LokiResponse` (`loki.ts`) — all near-exact mirrors of what the generated
types already provide. Expected to survive: `TempoSpan`/`TraceSummary`/
`TempoTrace` and the `WireSpan`/`WireTrace`-flattening functions in
`tempo.ts` (OTLP attribute flattening, root-span selection), `LogRow`/
`HistogramSeries` in `loki.ts` (ns→ms conversion, cross-stream sort),
`Flamebearer`/`RenderResponse` handling in `pyroscope.ts` if the generated
schema for the delta-encoded `levels` field turns out too weak to consume
directly (depends on how `spec-cover-compat-endpoints` schemas it — see that
change's design.md Risk).

## Risks / Trade-offs

- **Generated operation names may not read as cleanly as `lokiQueryLogs`.**
  Mitigation: this change keeps the existing exported function names as the
  UI's stable API; only their internal implementation calls the generated
  operation.
- **Losing test coverage during the mock-target swap.** Existing tests
  (`loki.test.ts`, `prom.test.ts`, `session.test.ts`, `tempo.test.ts`) mock
  `global.fetch`; migrating means either mocking the generated client's
  `client.interceptors`/underlying fetch or mocking at the SDK-function
  level. Mitigation: migrate tests alongside their implementation file in
  the same commit, keep the same test _cases_ (inputs/expected outputs), and
  run them before/after to confirm equivalent coverage, not just equivalent
  line count.
- **Pyroscope's response typing depends entirely on how
  `spec-cover-compat-endpoints` schemas `Flamebearer`.** If that lands with
  a weak `array of array of integer` schema (its stated risk), this change's
  pyroscope.ts migration may keep more hand-written shape-narrowing than
  loki/prom needed. Not a blocker — the adapter-survives-when-there's-real-
  computation rule in `ui-generated-client-only` already covers this case.

## Open Questions

- Does the generated `client-fetch` in practice send the `signaldb_session`
  cookie on same-origin requests without additional config, once
  `spec-cover-compat-endpoints` adds `cookieAuth`? Expected yes (default
  `same-origin` credentials mode), but worth a quick manual check against a
  running dev server before or during `session.ts`'s migration — doesn't
  change the approach or task breakdown either way, only confirms it.
