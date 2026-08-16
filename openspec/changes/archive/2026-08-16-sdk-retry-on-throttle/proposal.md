## Why

Once SignalDB says _when_ to retry (`query-throttle-signalling`), no client should surface a `429` to a human or an agent on the first hit: the UI's Explore page, the CLI, and the MCP server all sit on generated clients that today fail the request immediately, so a brief burst turns into a red error panel, a non-zero exit, or an agent that gives up. Retry belongs in the shared client layer — the Rust SDK (consumed by CLI and MCP) and the TypeScript client (consumed by the UI) — so every surface backs off the same way and nobody hand-rolls it per call site.

## What Changes

- The Rust SDK gains a single, default-on retry policy applied to every generated operation: on `429` (any method) and on `502/503/504`, connection failures, and timeouts (idempotent methods only), it waits — honouring `Retry-After` when present, otherwise exponential backoff with full jitter — and retries up to a bounded number of attempts and total wait; when the wait SignalDB asks for exceeds the policy's ceiling it fails fast rather than hanging. The policy is configurable and can be disabled. The SDK also exposes the server-stated wait on the final error so callers can report it.
- The CLI and the MCP server construct their SDK clients through one SDK-provided builder (no more ad-hoc `reqwest::Client::builder()` per call site) so they inherit the policy; the CLI gains `--no-retry` / `SIGNALDB_NO_RETRY` for scripting that wants fail-fast, and reports "rate limited (server asked to retry in Ns)" with a distinct exit code when retries are exhausted; the MCP server maps an exhausted `429` to a distinct throttled error that carries the wait.
- The TypeScript client gains the same policy through a single retrying `fetch` installed on the generated client (and reused by the few remaining raw-fetch callers until they migrate), respecting the caller's `AbortSignal` so leaving a page cancels waits; the UI's shared error surface tells the user a request is being retried after throttling rather than flashing an error.
- Parity is asserted: a test in each consumer proves it obtains its client through the SDK builder, and the surface-parity check gains a "throttling is retried on every surface" assertion.

No **BREAKING** changes: default-on retry only changes failure paths (a request that would have failed with 429 now succeeds later or fails later with the same status).

## Capabilities

### New Capabilities

- `client-retry-on-throttle`: the shared retry contract of SignalDB's generated clients (Rust SDK, TypeScript client): what is retried, how waits are chosen, bounds, cancellation, opt-out, and how exhaustion is reported by each surface.

### Modified Capabilities

- `client-surface-parity`: the "SDK is the sole client access path" requirement extends to client _construction_ — the CLI and MCP obtain their HTTP client from the SDK's builder so cross-cutting policy (retry, timeouts, headers) cannot drift per consumer.
- `mcp-server`: the "Rate-limited call is reported as retryable" scenario is refined — the server retries within the SDK policy first and only then reports a throttled error carrying the server-stated wait.
- `cli-command-surface`: "Deterministic exit codes" gains a distinct exit code for "throttled after retries" and a `--no-retry` fail-fast switch.

## Impact

- **signaldb-sdk**: new `retry` module (policy, backoff, `Retry-After` parsing), a `ClientBuilder` wrapper that installs the policy, and an xtask post-processing step so the generated `ClientHooks` impl routes execution through the policy (`xtask/src/main.rs`, `src/signaldb-sdk/src/generated.rs` regenerated). New dev-dependency for an HTTP mock server in SDK tests.
- **signaldb-cli**: the six client construction sites collapse onto the SDK builder; new flag/env; error rendering + exit code.
- **mcp-server**: `sdk_client_for` uses the SDK builder; `map_sdk_err` 429 arm carries the wait and a throttled error kind.
- **src/ui**: `src/api/client.ts` installs `retryingFetch`; `src/api/http.ts` hosts it; `session.ts`/`tempo.ts`/`prom.ts`/`pyroscope.ts` call it until `ui-migrate-to-generated-sdk` removes their raw fetches; shared error banner copy.
- **tests-integration**: parity assertion; end-to-end test that a throttled router request succeeds through the SDK after backoff.
- **docs/skills**: SDK/CLI docs (retry semantics, `--no-retry`, exit code), MCP user docs (throttled error), `dev-workflow`/`tempo-api` skills if they describe client behavior.
- **Depends on** `query-throttle-signalling` (needs `Retry-After`).
