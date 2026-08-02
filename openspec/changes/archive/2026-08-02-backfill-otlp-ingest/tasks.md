<!--
This is a backfill of existing behavior: no code is written. Tasks verify
that each drafted requirement is already covered by shipping code and tests,
and flag any requirement that lacks direct coverage as a follow-up. "Done"
means the requirement was confirmed against code/tests, not implemented.
-->

## 1. Verify shared ingest capabilities against code and tests

- [ ] 1.1 `ingest-auth-tenancy`: confirm Bearer + `x-tenant-id` (required) /
      `x-dataset-id` (optional) resolution and rejection paths against
      `src/acceptor/src/middleware/grpc_auth.rs` and `middleware/auth.rs`
- [ ] 1.2 `ingest-auth-tenancy`: confirm `<signal>:write` scope enforcement
      and legacy-unscoped-key behavior against
      `src/common/src/auth/mod.rs` (`can_ingest`, `scoped_authorization_tests`)
- [ ] 1.3 `ingest-auth-tenancy`: confirm `_system` self-monitoring
      suppression/non-counting against `src/common/src/self_monitoring/`
- [ ] 1.4 `ingest-durability`: confirm WAL-flush-before-ack and
      at-least-once forward/retry against
      `src/acceptor/src/handler/otlp_grpc.rs`, `handler/forward.rs`,
      `handler/wal_retry.rs`
- [ ] 1.5 `ingest-rate-limiting-quotas`: confirm `429` / `RESOURCE_EXHAUSTED`
      on rate-limit and quota against the service guards in
      `src/acceptor/src/services/otlp_trace_service.rs` and the HTTP path in
      `src/acceptor/src/lib.rs` (`handle_otlp_http_export`)

## 2. Verify per-signal ingest capabilities against code and tests

- [ ] 2.1 `otlp-traces-ingestion`: endpoints, encodings, and span
      events/exceptions preservation against `src/acceptor/tests/otlp_http_traces.rs`,
      `src/common/tests/span_exception.rs`, `src/common/tests/http_span_semconv.rs`
- [ ] 2.2 `otlp-logs-ingestion`: endpoints, encodings, correlation against
      `src/acceptor/tests/otlp_http_logs.rs` and
      `src/common/src/flight/conversion/conversion_logs.rs`
- [ ] 2.3 `otlp-metrics-ingestion`: endpoints, encodings, and all five metric
      types against `src/acceptor/tests/otlp_http_metrics.rs` and
      `src/common/src/flight/conversion/conversion_metrics.rs`
- [ ] 2.4 `otlp-profiles-ingestion`: `v1development` endpoints and encodings
      against `src/acceptor/src/handler/otlp_profiles_handler.rs` and
      `services/otlp_profile_service.rs`
- [ ] 2.5 `prometheus-remote-write`: `POST /api/v1/write`, v1+v2, snappy,
      empty/undecodable handling against
      `src/acceptor/src/handler/prometheus_handler.rs` and
      `src/common/src/flight/conversion/conversion_prometheus/`

## 3. Reconcile and archive

- [ ] 3.1 Record any requirement lacking direct test coverage as a
      follow-up change (do not add coverage under this backfill)
- [ ] 3.2 `openspec validate backfill-otlp-ingest --strict` passes
- [ ] 3.3 `openspec archive backfill-otlp-ingest` to sync deltas into
      `openspec/specs/`
