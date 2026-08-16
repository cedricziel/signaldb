## 1. Bucket timing (common)

- [x] 1.1 Failing tests in `common::ratelimit`: `try_acquire` on an empty bucket reports the wait as `(cost - tokens) / rate`; a cost larger than the burst reports the full-burst fill time; `RateLimitExceeded` exposes `retry_after`, `limit`, `burst`; `retry_after_secs()` rounds up and is never below 1 (`cargo test -p common`)
- [x] 1.2 Implement: `TokenBucket::try_acquire -> Result<(), Duration>`, thread the wait/limit/burst into `RateLimitExceeded`, add `retry_headers(&RateLimitExceeded)` (http `HeaderName`/`HeaderValue` triple: `Retry-After`, `X-RateLimit-Limit`, `X-RateLimit-Burst`)
- [x] 1.3 Failing test then implement: `TenantLimits::default().burst_seconds == 10.0`; config doc-comment and `signaldb.dist.toml` examples updated (query 50 → 100 default, 200 → 500 override, `burst_seconds = 10.0`)
- [x] 1.4 Register `signaldb_rate_limit_rejections_total{surface,kind}` in `common::self_monitoring` with a `record_rate_limit_rejection(surface, kind)` helper; unit test that it increments

## 2. Router 429 envelope + headers

- [x] 2.1 Failing tests in `router::endpoints::api_error`: `ApiError::rate_limited(&err)` renders status 429, JSON body with `errorType: "rate_limited"` and `retryAfterMs`, and the three headers; existing constructors render no rate-limit headers (`cargo test -p router`)
- [x] 2.2 Implement `ApiError::rate_limited` (+ optional `retry_after`/`limit`/`burst` fields, header emission in `IntoResponse`, `retryAfterMs` in the body)
- [x] 2.3 Failing test in `router::lib` (the existing `echo_request` rate-limit test): a throttled request answers JSON with `Content-Type: application/json`, `Retry-After ≥ 1`, `X-RateLimit-Limit`, `X-RateLimit-Burst`; log line carries `retry_after_ms`; the counter increments once. Then switch `query_rate_layer` to `ApiError::rate_limited` and record the metric
- [x] 2.4 Failing tests then implement: admin quota 429s (`max_api_keys`, `max_datasets`) carry `Retry-After: 1` and `X-RateLimit-Limit = quota` alongside the existing `quota_exceeded` body; counter records `surface="admin", kind="quota"`
- [x] 2.5 Burst test: with `max_query_requests_per_sec = 100` and default burst, 40 back-to-back requests are all admitted (regression guard for the "generous defaults" requirement)

## 3. Acceptor headers

- [x] 3.1 Failing tests: OTLP/HTTP (`src/acceptor/src/lib.rs`) and Prometheus `remote_write` 429 responses carry `Retry-After`, `X-RateLimit-Limit`, `X-RateLimit-Burst`; bytes-dimension rejection reports bytes/second (`cargo test -p acceptor`)
- [x] 3.2 Implement via `common::ratelimit::retry_headers`; record `surface="otlp_http" | "prometheus"` rejections; gRPC path records `surface="otlp_grpc"` (no header change)

## 4. OpenAPI + generated clients

- [x] 4.1 Add a shared utoipa helper for the 429 response (envelope schema + header trio) and apply it to every rate-limited operation (Tempo, Loki, Prometheus, Query IR, tenant management, schema, whoami; admin quota endpoints). **Deviation**: Pyroscope (`endpoints/pyroscope.rs`) is not annotated — its handlers carry zero `#[utoipa::path]` attributes and are not registered in `openapi.rs`'s `paths()` list at all (a pre-existing gap predating this change, unrelated to rate limiting); adding first-time full OpenAPI documentation for six undocumented handlers was out of scope. They are still rate-limited at runtime (covered by `query_rate_layer`) and answer 429 with the full header trio via `common::ratelimit::retry_headers`; only the OpenAPI _doc_ is missing, matching their pre-existing undocumented state for every other status code too.
- [x] 4.2 `cargo xtask generate` — regenerate `api/signaldb-api.json`, `src/signaldb-sdk/src/generated.rs`, `src/ui/src/api/gen`; commit all three; the OpenAPI golden test passes. **Note**: progenitor (the Rust SDK generator) asserts an operation's non-2xx responses share one body type (`extract_responses`, `response_types.len() <= 1`, a documented `TODO` in `progenitor-impl`), and a typed `429` where an operation previously had none also flips that operation's generated `Error<E>` from `Error<()>` to a concrete type — a breaking signature change for hand-written SDK consumers (`mcp-server`, `signaldb-cli`) using bare `?`. `xtask`'s `homogenize_error_response_bodies` (progenitor-input only, mirroring the existing `downconvert_nullable_types` pattern) keeps every operation's progenitor-visible error type exactly what it was before this change: if the operation's other non-2xx responses already agree on one type (`management.rs`'s `ManageError`, `schema.rs`'s `SchemaError`), the `429` is retargeted to reuse that same type (existing callers already handle it, so nothing changes); otherwise the `429`'s body is stripped (headers kept) so the operation's error type stays `()`. The served spec, `api/signaldb-api.json`, and the TypeScript client (no such limitation) keep full fidelity including the `429` envelope schema.
- [x] 4.3 Assert in the router OpenAPI test that each rate-limited path declares a `429` with the `Retry-After` header (drift guard)

## 5. Docs, skills, verification

- [x] 5.1 Docs: no existing `docs/operations` limits/quotas reference was found (none exists yet in this repo; not created per the "don't add new docs unless genuinely needed" rule — the `multi-tenancy` skill is the canonical limits/quotas reference and now documents the full 429 contract). Updated `docs/users/tempo-api-reference.md`, `logql-reference.md`, `profiles.md` error sections to show the `rate_limited` envelope with `retryAfterMs` and the header trio.
- [x] 5.2 Updated the `configuration` and `multi-tenancy` skills for the new burst default and the 429 contract
- [x] 5.3 CHANGELOG-visible note included in the commit body (burst default change + JSON 429 body are BREAKING-adjacent)
- [x] 5.4 `cargo fmt`, `cargo clippy --workspace --all-targets --all-features` (touched crates individually: common, router, acceptor, xtask, signaldb-sdk — all clean), `cargo machete --with-metadata` (clean); `openspec validate query-throttle-signalling --type change --strict`
