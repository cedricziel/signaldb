## 1. Span factory

- [ ] 1.1 Write a failing unit test in `src/common/src/self_monitoring/spans.rs` asserting `db_client_span` accepts an optional `query_text: Option<&str>` and, when `Some`, sets `db.query.text` on the span (extend the existing catalog-span test coverage rather than duplicating it).
- [ ] 1.2 Extend `db_client_span`'s signature with the `query_text` parameter and set `db.query.text` when present; update the existing catalog call site (`Catalog::db_span`, `src/common/src/catalog.rs:52`) to pass `None`.
- [ ] 1.3 Add `assert_eq!("db.query.text", attribute::DB_QUERY_TEXT)` to `literal_field_names_match_semconv_constants` (`spans.rs:246`).

## 2. Registry and weaver coverage

- [ ] 2.1 Add the DataFusion query span/attribute group to `otel/registry/signaldb.yaml` (span name `db.client`, `db.system.name = "datafusion"`, `db.operation.name` enum of the five query-surface operation values, `db.namespace`, `db.query.text`).
- [ ] 2.2 Run the weaver live-check locally against a manually exercised querier instance and fix any reported gaps before relying on CI's `weaver-live-check.yml`.

## 3. Raw SQL path

- [ ] 3.1 Write a failing test asserting `execute_query` (`src/querier/src/flight.rs:1243`) produces a `db.client` CLIENT span with `db.system.name="datafusion"`, `db.operation.name` set to the parsed SQL verb (or `"query"` fallback), `db.namespace` equal to the tenant/dataset passed in, and `db.query.text` equal to the sanitized SQL.
- [ ] 3.2 Thread tenant/dataset into `execute_query`/`execute_distributed_query` as explicit parameters (sourced from the existing call site's ticket/request context) and wrap the existing `signaldb.query.plan`/`signaldb.query.execute` block in the new `db_client_span(...)`, reparenting them as its children.
- [ ] 3.3 Verify the reparented stage spans still carry their existing `signaldb.query.rows`/`signaldb.query.batches`/`signaldb.query.text` attributes unchanged.

## 4. Query-IR path

- [ ] 4.1 Write a failing test asserting `IrService::query` (`src/querier/src/query/ir_planner.rs:502`) produces a `db.client` span with `db.operation.name="query_ir"` and `db.namespace` set from the service's tenant/dataset fields.
- [ ] 4.2 Wrap `IrService::query`'s existing `signaldb.query.plan`/`signaldb.query.execute` spans in the new CLIENT span.

## 5. PromQL path

- [ ] 5.1 Write a failing test asserting `MetricsService::query_metric` (`src/querier/src/query/metrics.rs:170`) produces a `db.client` span with `db.operation.name="promql_query"`, `db.namespace`, and `db.query.text` set to the (sanitized, if applicable) PromQL query text.
- [ ] 5.2 Instrument `MetricsService::query_metric`/`execute_plan`'s `.collect()` call with the new CLIENT span (this path currently has no span coverage at all).

## 6. LogQL path

- [ ] 6.1 Write a failing test asserting `LogsService::query_logs` (`src/querier/src/query/logs.rs:140`) produces a `db.client` span with `db.operation.name="logql_query"`, `db.namespace`, and `db.query.text`.
- [ ] 6.2 Instrument `LogsService::query_logs`/`execute_plan`'s `.collect()` call with the new CLIENT span.

## 7. TraceQL path

- [ ] 7.1 Write a failing test asserting the trace find/search functions (`src/querier/src/query/trace.rs:183,294`) produce a `db.client` span with `db.operation.name="traceql_query"` and `db.namespace`.
- [ ] 7.2 Instrument both `.collect()` call sites in `trace.rs` with the new CLIENT span.

## 8. Sanitization review

- [ ] 8.1 Review whether PromQL/LogQL/TraceQL query text can carry free-text literals (label values, matchers) that `sanitize_query_text` wasn't built to scrub; if so, extend or scope sanitization for those surfaces before recording `db.query.text` (see design.md Risks).

## 9. Metrics correlation

- [ ] 9.1 Add `db.system.name`, `db.operation.name`, and `db.namespace` as additional attributes (alongside the existing `query_type`) on the `signaldb.query.duration`, `signaldb.query.errors`, and `signaldb.query.rows_returned` recordings in `do_get` (`src/querier/src/flight.rs` ~2118-2136).

## 10. Regression tests

- [ ] 10.1 Add a pin test analogous to `src/common/tests/db_catalog_span_semconv.rs` for the new query-execution CLIENT span shape.
- [ ] 10.2 Run `cargo test -p querier` and `cargo test -p common` for the full new/changed test suite.
- [ ] 10.3 Add or extend an integration test in `tests-integration` exercising one query per surface (SQL, PromQL, LogQL, TraceQL, query-IR) and asserting a `db.client` span with the expected `db.operation.name` appears in the exported trace.

## 11. Docs

- [ ] 11.1 Update `docs/operations/self-monitoring-traces.md` (declared source: `src/common/src/self_monitoring/**`, `otel/registry/**`) to document the new DataFusion query CLIENT span, its attributes, and the reparented stage-span hierarchy.

## 12. Validation

- [ ] 12.1 Run `cargo fmt && cargo clippy --workspace --all-targets --all-features` and `cargo machete --with-metadata`.
- [ ] 12.2 Run `openspec validate datafusion-query-db-spans --strict` and fix any reported issues.
