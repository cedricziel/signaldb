# Tasks

Depends on `metric-convention-gate`: every instrument added below is declared
in `otel/registry/signaldb.yaml` and pinned against the semconv metric
constants as part of the task that adds it.

## 1. Merged HTTP server layer

- [ ] 1.1 Failing test (`cargo test -p common`): the server metrics for a
      request carry `http.route`, `error.type` on failure,
      `network.protocol.version`, `server.address`/`server.port`, and a
      `url.scheme` derived from the request (an `https` request must not
      report `http`).
- [ ] 1.2 Failing test (`cargo test -p common`): for one request, the span
      and the metric report identical `http.route`, `url.scheme`,
      `http.response.status_code`, and `error.type`.
- [ ] 1.3 Implement `common::self_monitoring::http` — one layer deriving the
      attribute set once, emitting the SERVER span and the four server
      metrics, preserving the `_system` anti-loop bypass and the
      before-first-enter parent adoption.
- [ ] 1.4 Extend `common/tests/http_span_semconv.rs` and
      `http_response_trace_context.rs` to run against the merged layer,
      proving span behavior is unchanged.
- [ ] 1.5 Declare the four server instruments and their attributes in the
      registry; pin their names to
      `opentelemetry_semantic_conventions::metric::*`.

## 2. Structural coverage

- [ ] 2.1 Add `common::self_monitoring::http::serve()` applying the layer and
      running `axum::serve`.
- [ ] 2.2 Migrate the 12 attachment sites (acceptor ×4, router, mcp-server)
      from the middleware pair to the merged layer via `serve()`.
- [ ] 2.3 Failing test (`cargo test -p compactor`): a request to `/metrics`,
      `/status`, and `/health` produces a SERVER span and a duration
      measurement; then route the compactor through `serve()`.
- [ ] 2.4 Include the MCP server's `.well-known` discovery document in the
      instrumented router; assert it is measured
      (`cargo test -p mcp-server`).
- [ ] 2.5 Add the CI guard rejecting `axum::serve` outside `common`, next to
      the existing span-construction guards; verify it fires on a
      deliberately direct call, then revert.

## 3. Object-storage HTTP client

- [ ] 3.1 Failing test (`cargo test -p common`): an S3-backed store issuing a
      request records `http.client.request.duration` with
      `http.request.method`, `server.address`, `server.port`, and status,
      and a CLIENT span for the same request.
- [ ] 3.2 Failing test: a retried storage request produces one measurement
      per attempt, with failing attempts carrying `error.type`.
- [ ] 3.3 Implement the instrumented `HttpConnector`/`HttpService` wrapper in
      `common::self_monitoring`; wire it in
      `common::storage::create_s3_builder_from_dsn`, leaving filesystem and
      in-memory stores untouched.
- [ ] 3.4 Failing test, then implement URL sanitization for presigned
      credentials/signatures, alongside the existing query-text sanitizer;
      assert no signature or credential value reaches `url.full`.
- [ ] 3.5 Declare the client instruments and attributes in the registry;
      pin the names.
- [ ] 3.6 Confirm filesystem/in-memory stores emit no HTTP client telemetry
      (`cargo test -p common`).

## 4. SDK HTTP client

- [ ] 4.1 Failing test (`cargo test -p signaldb-sdk`): a request records
      `http.client.request.duration` with `url.template` from the operation
      id and no path parameters in any attribute.
- [ ] 4.2 Failing test: a throttled-and-retried operation records
      `http.request.resend_count` greater than zero.
- [ ] 4.3 Failing test: with no meter provider installed, the SDK records
      nothing and does not panic.
- [ ] 4.4 Implement the instrument holder and recording in `retry::execute`
      under the existing `tracing` feature; add
      `opentelemetry-semantic-conventions` as an optional dependency and pin
      the names against its constants.
- [ ] 4.5 Add the SDK's CLIENT span, propagating trace context (the existing
      injection already covers propagation) and asserting the server SERVER
      span becomes its child in `tests-integration`.

## 5. Verification

- [ ] 5.1 `cargo test -p common -p acceptor -p router -p compactor -p mcp-server -p signaldb-sdk`,
      plus the cross-service cases in `tests-integration`.
- [ ] 5.2 `cargo fmt`, `cargo clippy --workspace --all-targets --all-features`,
      `cargo machete --with-metadata`.
- [ ] 5.3 Measure server-metric series count before and after on a running
      deployment; confirm it lands in the expected order of magnitude and
      that no series carries tenant identity.
- [ ] 5.4 Run the Weaver live-check workflow; confirm the new HTTP client and
      server metrics validate.

## 6. Documentation

- [ ] 6.1 Extend the self-monitoring metrics reference with the HTTP server
      and client inventory, and document that scrape/health endpoints are
      measured (route per the docs skill).
- [ ] 6.2 Document the browser-metrics exclusion and its rationale where the
      frontend instrumentation guidance lives.
- [ ] 6.3 Document the object-storage client telemetry as the way to separate
      storage latency from query latency, in the query/operations docs.
- [ ] 6.4 Update any skill whose described behavior changed (HTTP
      instrumentation guidance, frontend instrumentation).
- [ ] 6.5 Run the docs-freshness gate after committing, and again after any
      follow-up fix.
