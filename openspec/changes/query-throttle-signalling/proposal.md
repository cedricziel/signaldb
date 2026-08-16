## Why

The router's per-tenant query rate limit answers `429` with a plain-text body and no `Retry-After`, so no client — the UI, the CLI, the MCP server, or Grafana — can back off intelligently; they either fail the user's request or guess. The default burst allowance (2 seconds of budget) is also too tight for the way real clients behave: an Explore page or an agent investigation fires a fan-out of 10–30 requests in one instant, so a tenant with a perfectly reasonable sustained rate trips the limiter on every page load. This change makes throttling _legible_ (structured 429 with `Retry-After`) and the _default budget generous enough_ for bursty interactive clients, so the follow-up SDK retry work (`sdk-retry-on-throttle`) has a signal to act on.

## What Changes

- Every rate-limit rejection issued by SignalDB (router query surfaces, admin quotas, acceptor OTLP/HTTP and Prometheus `remote_write`) SHALL carry a `Retry-After` header (integer seconds, ≥ 1) computed from the token bucket's actual refill time, plus `X-RateLimit-Limit` (the tenant's per-second budget for the rejected dimension) and `X-RateLimit-Burst` (the burst allowance in requests/bytes).
- The router's query 429 becomes a structured JSON error in the existing `ApiError` envelope (`{"status":"error","errorType":"rate_limited","error":"…","retryAfterMs":N}`) instead of plain text, so every HTTP client sees the same shape as other errors on that surface.
- The default burst allowance rises from 2 s to 10 s of budget (`[auth.default_limits].burst_seconds` default 2.0 → 10.0) so interactive fan-outs succeed against a sustained-rate limit; the shipped example limits in `signaldb.dist.toml` are raised to reflect the intended "generous by default" posture (query 50 → 100 rps default, 200 → 500 rps in the per-tenant override example). Deployments that set explicit values are unaffected except for the burst default.
- Rate-limit rejections and the computed wait are observable: the router's warn log carries `retry_after_ms`, and a `signaldb_rate_limit_rejections_total{surface,kind}` counter increments per rejection.
- The OpenAPI document declares the 429 response (headers + body schema) on the query surfaces, so the generated Rust SDK and TypeScript client can read `Retry-After` and `retryAfterMs` typed.

**BREAKING**: the router's query 429 body changes from plain text to the JSON `ApiError` envelope. Clients that string-matched the old text must switch to the `errorType` field or the status code. (Tempo/Loki/Prometheus compat clients only look at the status code, and Grafana already tolerates the JSON envelope on those surfaces.)

## Capabilities

### New Capabilities

- `query-rate-limiting`: per-tenant rate limiting of the router's HTTP query and management surfaces — which surfaces are limited, the shape of the 429 (headers + body), the burst/budget semantics, and observability of rejections.

### Modified Capabilities

- `ingest-rate-limiting-quotas`: the "Request exceeding the rate is rejected as retryable" scenario gains the `Retry-After` header on OTLP/HTTP and Prometheus 429s (gRPC keeps `RESOURCE_EXHAUSTED`, unchanged).

## Impact

- **common**: `ratelimit.rs` (`TokenBucket` learns "time until `cost` tokens are available"; `RateLimitExceeded` carries `retry_after` and the limit/burst), `config/mod.rs` (`burst_seconds` default), self-monitoring metrics registration.
- **router**: `lib.rs` query-rate middleware (structured 429 + headers), `endpoints/api_error.rs` (optional `retryAfterMs`, header support), `endpoints/admin.rs` quota 429s (headers), OpenAPI annotations → `api/signaldb-api.json` regenerated.
- **acceptor**: OTLP/HTTP and Prometheus `remote_write` 429 sites gain `Retry-After`.
- **signaldb-sdk / src/ui/src/api/gen**: regenerated from the updated OpenAPI document (no hand-written changes here; consumption of the header is `sdk-retry-on-throttle`).
- **docs**: `docs/operations/` limits reference and the Tempo/LogQL/profiles API references (429 shape), `configuration` skill (`burst_seconds` default), `multi-tenancy` skill (limits).
- **Dependent change**: `sdk-retry-on-throttle` consumes `Retry-After`; it must land after this change.
