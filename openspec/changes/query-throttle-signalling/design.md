## Context

See proposal.md — Why. Current state: `common::ratelimit::TokenBucket` answers only admit/deny; `RateLimitExceeded` carries tenant + kind, no timing. The router's `query_rate_layer` (`src/router/src/lib.rs`) returns `(429, e.to_string())` as plain text; `ApiError` (`endpoints/api_error.rs`) already maps 429 → `errorType: "rate_limited"` but is only used for querier `ResourceExhausted`. Admin quotas (`endpoints/admin.rs`) return JSON `quota_exceeded` without headers. Acceptor OTLP/HTTP and Prometheus 429s are plain text via `otlp_http_error` / `PrometheusError`. No `Retry-After` anywhere. `burst_seconds` defaults to 2.0.

Constraints: the limiter is on the hot path (one DashMap shard + short mutex, no allocation) and must stay that way; the OpenAPI document is code-first (utoipa) and both SDKs are regenerated from it by `cargo xtask generate`; the UI is instrumented and self-monitoring metrics go through `common::self_monitoring`.

## Goals / Non-Goals

**Goals:**

- One retry rule for all of SignalDB: every HTTP 429 carries `Retry-After` computed from real bucket state, and the router's 429 body is the standard envelope.
- Defaults that don't punish bursty interactive clients.
- Rejections are visible in logs and metrics.

**Non-Goals:**

- Per-API-key or per-session buckets (stays per tenant).
- Rate-limit headers on _successful_ responses (`X-RateLimit-Remaining` etc.) — not needed for backoff; can be added later without breaking anything.
- Client-side retry (that is `sdk-retry-on-throttle`).
- Changing gRPC ingest signalling (`RESOURCE_EXHAUSTED` stays; a `retry-after` trailer is not part of the OTLP contract).

## Decisions

**D1 — Compute the wait inside the bucket, return it in the error.** `TokenBucket::try_acquire` becomes `try_acquire(cost, now) -> Result<(), Duration>` where the `Err` is the time until `cost` tokens will be available: `max(0, (cost - tokens) / rate)`. If `cost > burst` (a single request larger than the whole burst, only possible for the bytes dimension) the wait is reported as the time to fill the whole burst — the request can never be admitted, but we still answer with a finite number; the message says so. `RateLimitExceeded` gains `retry_after: Duration`, `limit: f64` (per-second budget) and `burst: f64`. Cheap: no extra locking, one division. _Alternative:_ estimate `Retry-After` in the middleware as `1 / rate` — rejected, wrong for large costs and after long overruns.

**D2 — Router 429 = `ApiError` with headers.** Add `ApiError::rate_limited(err: &RateLimitExceeded)` producing status 429, `errorType: "rate_limited"`, `retryAfterMs`, and the three headers in `IntoResponse`. `ApiError` grows an optional `retry_after: Option<Duration>` (+ limit/burst) so `IntoResponse` can attach headers; existing constructors are unchanged. `query_rate_layer` uses it. Admin quota sites switch to the same helper with kind `Quota` — no bucket there, so `Retry-After` is a fixed 1 s (the quota does not refill by time; the header still tells the client this is a wait-and-retry, and `X-RateLimit-Limit` carries the quota). _Alternative:_ a separate `RateLimitResponse` type — rejected, one envelope per surface is the point.

**D3 — Acceptor reuses the same header helper.** A small `common::ratelimit::retry_headers(&RateLimitExceeded) -> [(HeaderName, HeaderValue); 3]` (http crate types, which the acceptor and router both use) so the OTLP/HTTP and Prometheus paths attach identical headers without depending on the router. Bodies stay what they are today on those surfaces (OTLP-HTTP has its own protobuf/JSON error shape; Prometheus expects text).

**D4 — Metrics via `common::self_monitoring` counter, labelled `{surface, kind}`.** Surface ∈ `query | admin | otlp_http | otlp_grpc | prometheus`; kind ∈ `query_requests | requests | bytes | quota`. Bounded cardinality. Logged at `warn` (already the case) with `retry_after_ms` added.

**D5 — Defaults: `burst_seconds` 2.0 → 10.0; dist examples query 50 → 100 default / 200 → 500 override.** Rationale: the UI's Explore page fires ~10–30 requests on load (facets, volume, list, tooltips) and an MCP investigation runs several tools back-to-back; a 2 s burst at 50 rps is 100 tokens, which is fine for one page but two tabs or a collector sharing the tenant trip it. 10 s of budget admits a burst of 1000 at 100 rps and refills at the sustained rate, so it protects capacity (the sustained rate is unchanged) while not punishing burstiness. Ingest examples are left as they are; ingest clients batch and retry by design. _Alternative:_ raise the sustained rate instead — rejected, that weakens the actual protection.

**D6 — OpenAPI: declare 429 once, apply everywhere.** utoipa `#[utoipa::path(responses(...))]` on each rate-limited operation gets `(status = 429, description = ..., body = ApiErrorBody, headers(("Retry-After" = i64, ...), ...))`; the header trio is a shared helper macro to keep the annotations from drifting. `cargo xtask generate` regenerates `api/signaldb-api.json`, `signaldb-sdk/src/generated.rs`, and `src/ui/src/api/gen`. The doc-freshness/OpenAPI golden test enforces the regeneration.

**D7 — Middleware ordering unchanged.** The query limiter still runs after auth (it needs `TenantContext`), before the handler, on the same route groups as today; the change is only what it answers.

## Risks / Trade-offs

- [`Retry-After` is per tenant, not per client] → a burst from one client can make another client of the same tenant wait; acceptable, that is what a tenant budget means. Documented.
- [Raising the burst default changes behaviour for deployments that rely on the implicit 2.0] → called out as BREAKING-adjacent in the proposal and CHANGELOG; operators who want the old behaviour set `burst_seconds = 2.0`.
- [Body change from text to JSON on the query 429] → BREAKING for text-matching clients; the compat surfaces (Grafana) only look at status; the SDKs are regenerated in the same change.
- [OpenAPI annotation sprawl] → shared helper for the header trio; the golden test catches drift.

## Migration Plan

Deploy router + acceptor together (they only add headers/body fields; old clients keep working off the status code). Rollback: revert the image; no data or config migration. Operators who want the previous burst set `burst_seconds = 2.0` explicitly.

## Open Questions

- Should the router also emit `X-RateLimit-Remaining` on 2xx query responses so the UI can pre-emptively pace itself? Deferrable — additive, non-breaking; revisit after `sdk-retry-on-throttle` shows whether reactive backoff is enough.
