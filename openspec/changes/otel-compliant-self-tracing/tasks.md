# Tasks — otel-compliant-self-tracing

Sequenced as a stack of small PRs (design.md Migration Plan); each group is
independently shippable and TDD-ordered within.

## 1. Foundations: semconv pin, resource, sampler

- [x] 1.1 Add `opentelemetry-semantic-conventions` (features
      `semconv_experimental`) to workspace deps; add a single
      `SEMCONV_SCHEMA_URL` constant in `common::self_monitoring`
- [x] 1.2 Write failing test in `common` asserting exported resource carries
      `service.namespace=signaldb`, per-process `service.instance.id`,
      `deployment.environment.name`, schema_url — and NOT the deprecated
      `deployment.environment`
- [x] 1.3 Implement resource changes in `init_telemetry` (config-sourced
      environment name, UUID instance id, schema_url on resource + scope)
- [x] 1.4 Write failing test: unrecognized `OTEL_TRACES_SAMPLER` value falls
      back to ParentBased sampling
- [x] 1.5 Fix `resolve_trace_sampler` unrecognized-name arm to ParentBased

## 2. Span factories and conformance pins (common)

- [x] 2.1 Write failing InMemorySpanExporter conformance tests for
      `rpc_server_span` / `rpc_client_span` (name incl. ticket verb, kind,
      `rpc.system.name`, `rpc.method`, `rpc.response.status_code`,
      server/client error asymmetry per spec)
- [x] 2.2 Write failing conformance tests for `db_client_span` and
      `job_span` (incl. link-count behavior via `add_link_from_fields`)
- [x] 2.3 Implement `common::self_monitoring::spans` factories; refactor
      `http_trace_context_middleware` onto `http_server_span`, adding
      `error.type` (status-as-string on 5xx), `server.port`,
      `client.address`; extend `http_span_semconv.rs` accordingly

## 3. Acceptor becomes a trace boundary

- [ ] 3.1 Write failing integration test (tests-integration): OTLP/HTTP
      `POST /v1/traces` with `traceparent` yields a child SERVER span named
      `POST /v1/traces`; 4xx response leaves span status unset
- [ ] 3.2 Mount `http_trace_context_middleware` on acceptor OTLP/HTTP and
      remote-write routers (verify `_system` bypass still suppresses)
- [ ] 3.3 Write failing integration test: OTLP gRPC export call yields a
      SERVER span with `rpc.*` attributes, joined to the caller's context
- [ ] 3.4 Implement tonic tower layer applying `rpc_server_span` +
      `set_parent_from_request` across all four OTLP services

## 4. Flight server spans (querier, writer, compactor)

- [ ] 4.1 Write failing test (querier): `do_get` exports SERVER span
      `arrow.flight.protocol.FlightService/DoGet <ticket_verb>` with
      `rpc.*` attrs; `NOT_FOUND` completion does not set status Error
- [ ] 4.2 Replace `flight_do_get` span in `querier/src/flight.rs` with
      `rpc_server_span`, preserving suppression scope, parent-before-enter,
      and exception recording
- [ ] 4.3 Same for writer `do_put` (`flight_iceberg.rs`) and the compactor
      Flight service, with failing tests first per crate

## 5. Flight client spans (router, acceptor)

- [ ] 5.1 Write failing integration test: router→querier query produces
      CLIENT span parenting the querier SERVER span; non-OK status marks
      the CLIENT span Error
- [ ] 5.2 Wrap the 8 router Flight call sites
      (`endpoints/{flight,logql,promql,pyroscope,query,tempo}.rs`) in
      `rpc_client_span` so injection reads the client span's context
- [ ] 5.3 Wrap acceptor→writer `do_put` call sites in `rpc_client_span`

## 6. Catalog DB client spans

- [ ] 6.1 Write failing test: a catalog operation under a traced request
      exports a CLIENT span with `db.system.name`, `db.operation.name`,
      `db.namespace`
- [ ] 6.2 Instrument catalog/discovery sqlx call paths in
      `common/src/catalog.rs` + `service_bootstrap.rs` via `db_client_span`

## 7. Query execution stage spans (querier)

- [ ] 7.1 Write failing test: an executed query decomposes into
      plan/scan/execute/encode child spans with `signaldb.*` row/byte
      counts under the Flight SERVER span
- [ ] 7.2 Implement stage spans in `querier/src/query/`
- [ ] 7.3 Write failing test: recorded query text has literals replaced by
      placeholders; implement sanitize-before-record helper in `common`

## 8. Compactor lifecycle job spans

- [ ] 8.1 Write failing tests: retention enforcement, snapshot expiration,
      and orphan cleanup runs each export a root span with tenant/dataset/
      table and affected-object counts
- [ ] 8.2 Implement via `job_span` in compactor retention/lifecycle modules;
      align existing `compaction_job` span fields to `signaldb.*` names

## 9. Hygiene sweep and construction guard

- [ ] 9.1 Convert all bare `#[tracing::instrument]` sites (router
      `tempo.rs`, `tenant.rs`, querier `services/tempo.rs`) to
      `skip_all` + explicit bounded fields
- [ ] 9.2 Rename SignalDB-local span fields to registry names
      (`signaldb.tenant.id`, `signaldb.dataset.id`, `signaldb.wal.*`)
      across acceptor/router/writer/querier/compactor
- [ ] 9.3 Spike clippy `disallowed-macros` for raw span macros /
      `#[instrument]` in boundary modules; wire whichever works (clippy or
      grep-based lint job step) into CI; document the rule in
      `docs/contributing/rust.md`
- [ ] 9.4 Add writer test pinning WAL link semantics (3 ingest traces → 3
      links, no parent) — pins existing behavior against regression

## 10. Weaver registry and static gates

- [ ] 10.1 Author `otel/registry/` (manifest.yaml format 2.0 depending on
      semconv v1.43.0 + `signaldb.*` attribute/span groups); CI step
      `weaver registry check` with pinned Weaver version
- [ ] 10.2 Generate `signaldb.*` Rust constants from the registry
      (opentelemetry-rust template set), switch factories to them, add
      `git diff --exit-code` drift gate + `registry diff
--baseline-registry` evolution gate

## 11. Live-check CI harness

- [ ] 11.1 Add CI job: boot monolithic signaldb with self-monitoring
      pointed at `weaver registry live-check` OTLP listener, drive
      signal-producer ingest + HTTP API queries, `--fail-on violation`
      (non-blocking during bake-in), report `registry_coverage`
- [ ] 11.2 Add `.weaver.toml` finding filters for known-acceptable noise;
      flip job to blocking once stable

## 12. Docs and skills

- [ ] 12.1 Docs (route via docs skill): operations page describing the
      emitted trace model, the span/attribute rename table
      (`flight_do_get` → RPC names, `tenant_id` → `signaldb.tenant.id`),
      and the new config key for `deployment.environment.name`
- [ ] 12.2 Update `configuration` skill (new self_monitoring config key)
      and `architecture`/`dev-workflow` skills where they describe
      self-monitoring spans or CI jobs
