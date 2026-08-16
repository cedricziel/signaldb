## 1. Bucket timing (common)

- [ ] 1.1 Failing tests in `common::ratelimit`: `try_acquire` on an empty bucket reports the wait as `(cost - tokens) / rate`; a cost larger than the burst reports the full-burst fill time; `RateLimitExceeded` exposes `retry_after`, `limit`, `burst`; `retry_after_secs()` rounds up and is never below 1 (`cargo test -p common`)
- [ ] 1.2 Implement: `TokenBucket::try_acquire -> Result<(), Duration>`, thread the wait/limit/burst into `RateLimitExceeded`, add `retry_headers(&RateLimitExceeded)` (http `HeaderName`/`HeaderValue` triple: `Retry-After`, `X-RateLimit-Limit`, `X-RateLimit-Burst`)
- [ ] 1.3 Failing test then implement: `TenantLimits::default().burst_seconds == 10.0`; config doc-comment and `signaldb.dist.toml` examples updated (query 50 → 100 default, 200 → 500 override, `burst_seconds = 10.0`)
- [ ] 1.4 Register `signaldb_rate_limit_rejections_total{surface,kind}` in `common::self_monitoring` with a `record_rate_limit_rejection(surface, kind)` helper; unit test that it increments

## 2. Router 429 envelope + headers

- [ ] 2.1 Failing tests in `router::endpoints::api_error`: `ApiError::rate_limited(&err)` renders status 429, JSON body with `errorType: "rate_limited"` and `retryAfterMs`, and the three headers; existing constructors render no rate-limit headers (`cargo test -p router`)
- [ ] 2.2 Implement `ApiError::rate_limited` (+ optional `retry_after`/`limit`/`burst` fields, header emission in `IntoResponse`, `retryAfterMs` in the body)
- [ ] 2.3 Failing test in `router::lib` (the existing `echo_request` rate-limit test): a throttled request answers JSON with `Content-Type: application/json`, `Retry-After ≥ 1`, `X-RateLimit-Limit`, `X-RateLimit-Burst`; log line carries `retry_after_ms`; the counter increments once. Then switch `query_rate_layer` to `ApiError::rate_limited` and record the metric
- [ ] 2.4 Failing tests then implement: admin quota 429s (`max_api_keys`, `max_datasets`) carry `Retry-After: 1` and `X-RateLimit-Limit = quota` alongside the existing `quota_exceeded` body; counter records `surface="admin", kind="quota"`
- [ ] 2.5 Burst test: with `max_query_requests_per_sec = 100` and default burst, 40 back-to-back requests are all admitted (regression guard for the "generous defaults" requirement)

## 3. Acceptor headers

- [ ] 3.1 Failing tests: OTLP/HTTP (`src/acceptor/src/lib.rs`) and Prometheus `remote_write` 429 responses carry `Retry-After`, `X-RateLimit-Limit`, `X-RateLimit-Burst`; bytes-dimension rejection reports bytes/second (`cargo test -p acceptor`)
- [ ] 3.2 Implement via `common::ratelimit::retry_headers`; record `surface="otlp_http" | "prometheus"` rejections; gRPC path records `surface="otlp_grpc"` (no header change)

## 4. OpenAPI + generated clients

- [ ] 4.1 Add a shared utoipa helper for the 429 response (envelope schema + header trio) and apply it to every rate-limited operation (Tempo, Loki, Prometheus, Pyroscope/profiles, Query IR, tenant management, schema, whoami; admin quota endpoints)
- [ ] 4.2 `cargo xtask generate` — regenerate `api/signaldb-api.json`, `src/signaldb-sdk/src/generated.rs`, `src/ui/src/api/gen`; commit all three; the OpenAPI golden test passes
- [ ] 4.3 Assert in the router OpenAPI test that each rate-limited path declares a `429` with the `Retry-After` header (drift guard)

## 5. Docs, skills, verification

- [ ] 5.1 Docs (route via the docs skill): operations limits/quotas reference — 429 shape, headers, `burst_seconds` semantics and new default, sizing guidance; update `docs/users/tempo-api-reference.md`, `logql-reference.md`, `profiles.md` error sections to show the `rate_limited` envelope with `retryAfterMs`
- [ ] 5.2 Update the `configuration` and `multi-tenancy` skills for the new burst default and the 429 contract
- [ ] 5.3 CHANGELOG-visible note (conventional commit body) on the burst default change and the JSON 429 body
- [ ] 5.4 `cargo fmt`, `cargo clippy --workspace --all-targets --all-features`, `cargo machete --with-metadata`; `openspec validate query-throttle-signalling --type change --strict`
