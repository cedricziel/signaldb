---
audience: user
type: reference
status: living
sources:
  - src/signaldb-sdk/src/retry.rs
  - src/signaldb-sdk/src/builder.rs
  - src/signaldb-cli/src/retry.rs
  - src/signaldb-cli/src/main.rs
  - src/mcp-server/src/lib.rs
  - src/ui/src/api/http.ts
  - src/ui/src/api/client.ts
  - src/ui/src/features/shell/ThrottleBanner.tsx
  - src/router/src/endpoints/api_error.rs
  - api/retry-cases.json
---

# Client retry on throttling

SignalDB's clients — the Rust SDK (`signaldb-sdk`, behind the CLI and the MCP
server) and the web UI's TypeScript client — retry throttled and transiently
failing requests before they surface a failure. Retries absorb transient
throttling while the retry budget lasts, so a brief burst past a tenant's
query budget usually never shows up as a red panel, a non-zero exit, or an
agent giving up. But the budget is bounded (four attempts, a 10 s per-attempt
cap, a 30 s total cap — see below): a sustained burst, or a wait beyond a
cap, still surfaces an error in the UI, CLI, or MCP server. Every surface
backs off the same way; the shared table `api/retry-cases.json` is the
executable form of the rules below and both test suites replay it.

## What is retried

| Outcome                                          | Retried?                                                       |
| ------------------------------------------------ | -------------------------------------------------------------- |
| `429 Too Many Requests`                          | Yes, on **any** method — a throttled request was not processed |
| `502`, `503`, `504`, connection failure, timeout | Only for idempotent methods (`GET`, `HEAD`, `PUT`, `DELETE`)   |
| Any other `4xx`, `500`, malformed response       | No — reported immediately                                      |

`POST` (Query IR, API-key creation, registry uploads) is retried on `429`
because SignalDB's limiter rejects before any handler runs; it is not retried
on `503` or a connection reset because the server may have acted on it.

## How long it waits

- When the rejection carries `Retry-After` (seconds or an HTTP-date), the
  client waits at least that long. Most SignalDB `429`s do — a request
  rejected by the rate limiter (see the
  [Tempo API reference](tempo-api-reference.md)) — but not all: a `429`
  produced from an upstream `ResourceExhausted` status (for example, Flight
  backpressure from the querier, mapped by `ApiError::new` rather than
  `ApiError::rate_limited`) carries no `Retry-After`.
- Otherwise — no `Retry-After`, including that upstream-`ResourceExhausted`
  case — it waits a fully jittered exponential backoff: uniform in
  `[0, min(cap, 250 ms · 2ⁿ)]` for the n-th retry.
- Every wait is capped at **10 s**; the sum of waits per call at **30 s**.
  If the server asks for more than the per-attempt cap, the client **fails
  fast** with the throttling error instead of blocking, so callers that
  cannot afford the wait learn at once.
- At most **4 attempts** per call (the first request plus three retries). The
  last failure is what the caller sees.

## Per surface

### Rust SDK

Construct clients through `signaldb_sdk::ClientBuilder`; the retry policy is
installed on every generated operation:

```rust
use signaldb_sdk::{ClientBuilder, RetryPolicy};

let client = ClientBuilder::new("http://localhost:3000")
    .bearer("sk-acme-key")
    .tenant("acme")
    .dataset("production")
    .timeout(std::time::Duration::from_secs(60))
    .retry(RetryPolicy::default())          // or RetryPolicy::disabled()
    .build()?;
```

`RetryPolicy { max_attempts, base, per_attempt_cap, total_cap,
retry_transient_idempotent, on_retry }` is public; `RetryPolicy::disabled()`
fails fast on the first failure. After retries are exhausted the final
`signaldb_sdk::Error` still carries the server's `Retry-After`:
`signaldb_sdk::retry::retry_after(&err) -> Option<Duration>` reads it, and
`retry::throttle_of(&dyn Error)` recognises a throttled SDK error inside an
`anyhow` chain. Each retry emits one `debug` event on the
`signaldb_sdk::retry` target (`attempt`, `wait_ms`, `status`) and calls the
policy's optional `on_retry` observer. With the default-on `tracing`
feature the SDK also injects W3C `traceparent`/`tracestate` from the current
span, so an SDK request made inside an OpenTelemetry-backed span continues
the caller's trace on the router.

Retry applies to the HTTP client only; `QueryClient` (SQL over Arrow Flight)
has gRPC's own status semantics and is not retried.

### CLI

`signaldb-cli` builds every client through the SDK builder, so all commands
retry by default. To fail fast for scripting, pass `--no-retry` (a global
flag, valid before or after the subcommand) or set `SIGNALDB_NO_RETRY=1`.
When a command is still throttled after the retry budget — or immediately,
under `--no-retry` — stderr reads

```text
Error: rate limited; server asked to retry in 5s
```

and the process exits with code **4** (generic failures keep `1`, usage errors
`2`), so scripts can back off and re-run. When stderr is a terminal, each
retry prints one short note (`rate limited; retrying in 1s (attempt 2)`) so an
interactive user knows the command is waiting, not hung; stdout is untouched.

### MCP server

Tool calls go through the same SDK client, so a brief throttle is invisible
to the agent. Once retries are exhausted the tool returns a distinct throttled
error: the message starts with `throttled:` and names the wait
(`throttled: search_logs was rate limited; the server asked to retry in 30s`),
and the error `data` carries `retryAfterMs` (milliseconds, `null` when the
server stated no wait) plus `http_status: 429`. See [MCP](mcp.md).

### Web UI

The generated TypeScript client and the remaining hand-written callers share
one `retryingFetch` (`src/ui/src/api/http.ts`). While a request is waiting to
be retried, the panel stays in its loading state and the shell shows an
unobtrusive banner ("Some requests are being retried after throttling…").
Leaving a page or superseding a query aborts the request's `AbortSignal`,
which cancels a pending wait. When retries are exhausted the panel's error
reads `Rate limited — server asked to retry in 5 s` instead of a generic
failure; `ApiError.retryAfterMs` carries the wait for code that wants it.

Callers that consume the generated SDK's `RequestResult` (which returns an
`error` rather than throwing) unwrap it through `unwrapSdkResult`, which
re-throws as the same `ApiError` — preserving the HTTP status and
`retryAfterMs` — so a `429` surfaced from a management or session call is
still recognised and backed off exactly like a raw `retryingFetch` rejection.
