## 1. Shared contract fixture

- [x] 1.1 Add `api/retry-cases.json`: table of `(status, method, retry_after_header, attempt) → (retry: bool, wait_ms_min, wait_ms_max | fail_fast)` covering 429 any method, 502/503/504 idempotent-only, 4xx never, missing Retry-After backoff bounds, Retry-After above the per-attempt cap → fail fast

## 2. Rust SDK retry (signaldb-sdk + xtask)

- [x] 2.1 Failing tests (`cargo test -p signaldb-sdk`) using a local mock HTTP server: sequences of 429→200, 503 on GET → retry, 503 on POST → no retry, 400 → no retry, `Retry-After: 2` respected (measured ≥ 2 s with a paused tokio clock), `Retry-After` beyond cap → immediate return, max attempts honoured, `RetryPolicy::disabled()` never retries, `retry_after(&Error)` reads the header; the fixture from 1.1 is replayed
- [x] 2.2 Implement `signaldb_sdk::retry`: `RetryPolicy` (defaults per design D2), `execute(client, policy, request, info)`, `Retry-After` parsing (seconds + HTTP-date), jittered backoff, one `tracing::debug!` per retry, `retry_after()` helper
- [x] 2.3 xtask: `with_inner_type(crate::retry::RetryPolicy)` and the asserted rewrite of the generated `impl ClientHooks … for &Client {}` into an `exec` override; regenerate `src/signaldb-sdk/src/generated.rs`; add a test that the generated file contains the override (drift guard); document the shim at the top of `generated.rs` and in `xtask`
- [x] 2.4 Failing tests then implement `signaldb_sdk::ClientBuilder` (base URL, bearer, tenant, dataset, timeouts, retry policy) → `Client`; existing `Client::new`/`new_with_client` keep working with the default policy
- [x] 2.5 Failing test then implement W3C trace-context injection in the exec path: an outbound request made inside an OTel-backed span carries `traceparent`/`tracestate`; with no OTel layer no header is added (`signaldb-sdk/tracing` feature, default on; uses the workspace `opentelemetry` + `tracing-opentelemetry`)
- [x] 2.6 Open an upstream progenitor issue for a `with_client_hooks` setting; link it from the xtask comment — not needed: the override uses progenitor's documented auto-ref specialization (`impl ClientHooks<RetryPolicy> for Client` in `retry.rs`), so no xtask rewrite and no upstream setting are required (see design D1 implementation note)

## 3. CLI

- [x] 3.1 Failing test: a source-scan test asserts `signaldb-cli` contains no `reqwest::Client::builder()` / `reqwest::ClientBuilder::new()`; then replace the six construction sites with `signaldb_sdk::ClientBuilder`
- [x] 3.2 Failing tests then implement `--no-retry` / `SIGNALDB_NO_RETRY=1` → `RetryPolicy::disabled()`; throttled-after-retries → stderr "rate limited; server asked to retry in Ns" and exit code `4`; per-retry stderr note only when stderr is a terminal (SDK debug event → CLI subscriber)
- [x] 3.3 CLI integration test against a router with `max_query_requests_per_sec = 1, burst_seconds = 1`: three back-to-back `query` invocations all exit 0; with `--no-retry` the second exits `4`

## 4. MCP server

- [x] 4.1 Failing test: source-scan asserts `mcp-server` has no bare `reqwest::Client::builder()`; then `sdk_client_for` uses `signaldb_sdk::ClientBuilder` (headers, timeouts, default retry policy)
- [x] 4.2 Failing tests then implement the 429 arm of `map_sdk_err`: message prefix `throttled:` naming the server-stated wait, `data: {"retryAfterMs": N}`; existing "rate limited, retry shortly" test updated
- [x] 4.3 Integration test (mock router): 429→200 sequence yields a successful tool result; all-429 sequence yields the throttled error with `retryAfterMs`

## 5. UI

- [x] 5.1 Failing tests (`vitest`) for `retryingFetch` replaying `api/retry-cases.json` with a fake timer and a stub `fetch`: waits, caps, `AbortSignal` cancels a pending wait, disabled policy
- [x] 5.2 Implement `retryingFetch` and `throttleState` in `src/ui/src/api/http.ts`; `ApiError.retryAfterMs` + throttling message; install via `client.setConfig({ fetch: retryingFetch })` in `client.ts`; switch the raw `fetch` in `session.ts`, `tempo.ts`, `prom.ts`, `pyroscope.ts` to `retryingFetch`
- [x] 5.3 Shell banner "Some requests are being retried after throttling…" bound to `throttleState`; test that it appears while a retry is pending and disappears after
- [x] 5.4 Test that a panel rendering `error.message` shows the throttling message with the wait for an exhausted 429

## 6. Parity + end-to-end

- [x] 6.1 Extend the surface-parity check: CLI/MCP no bare client construction (3.1/4.1 tests referenced), UI `client.ts` installs `retryingFetch`
- [x] 6.2 tests-integration: router at `max_query_requests_per_sec = 1, burst_seconds = 1`; SDK default policy → 3 sequential requests succeed; `RetryPolicy::disabled()` → second request is `Error::ErrorResponse` with status 429 and `retry_after()` = Some

## 7. Docs, skills, hygiene

- [x] 7.1 Docs (route via the docs skill): SDK README/`docs/users/` client section (retry semantics, policy knobs, `retry_after`), CLI reference (`--no-retry`, exit code 4), MCP user docs (throttled error shape), UI note in the explore docs if one exists
- [x] 7.2 Update skills that describe client behaviour (`tempo-api`, `dev-workflow` CLI section, `multi-tenancy` limits) for retry semantics
- [ ] 7.3 `cargo fmt`, `cargo clippy --workspace --all-targets --all-features`, `cargo machete --with-metadata`; `pnpm --filter signaldb-ui lint && test`; `openspec validate sdk-retry-on-throttle --type change --strict`
