## 1. Read-path guard: absent table reads as empty (querier)

Independent of reconciliation and closes issue #972's symptom on its own. Ship
first.

- [ ] 1.1 Write failing tests in `src/querier` for the logs metadata paths: with a registered tenant catalog whose dataset has no `logs` table, `get_labels` and `get_label_values` return empty results instead of an error (`cargo test -p querier`)
- [ ] 1.2 Write failing tests in `src/querier` for the metrics and profiles metadata/search paths under the same missing-table condition
- [ ] 1.3 Write failing tests in `src/querier` asserting the negative cases still error: an unknown tenant, an invalid query, and a planning failure against a table that _does_ exist must not be reported as empty
- [ ] 1.4 Add a shared table-resolution helper in the querier that reports absence via `SessionContext::table_exist` on the resolved table reference (never by matching DataFusion error text) and returns `None` for an absent table
- [ ] 1.5 Route the logs, metrics, and profiles metadata/label/search paths through the helper, yielding empty results on absence; leave data-plane errors from existing tables propagating unchanged
- [ ] 1.6 Write a failing test pinning the per-signal error wrapper, then fix the Flight status mapping in `src/querier/src/flight.rs` so a logs failure is not reported as `"Profile query failed"`
- [ ] 1.7 Upgrade the KNOWN-ISSUE pin in `src/querier/tests/lazy_tenant_registration.rs` from "any error except catalog-resolution" to asserting an empty label result, and drop the #972 marker

## 2. Reconcile entry point (common)

- [ ] 2.1 Write a failing test in `src/common` asserting `ensure_dataset_tables` creates exactly the tables enabled by `[schema.default_schemas]` for a fresh tenant/dataset (`cargo test -p common`)
- [ ] 2.2 Write a failing test asserting disabled signal types (metrics off, profiles off) produce no tables while enabled ones are still created
- [ ] 2.3 Write a failing test asserting a second `ensure_dataset_tables` pass over an already-provisioned dataset commits no new table version, snapshot, or data file
- [ ] 2.4 Write a failing test asserting concurrent `ensure_dataset_tables` calls for the same dataset both succeed and yield one table per signal
- [ ] 2.5 Implement `CatalogManager::ensure_dataset_tables(tenant_id, dataset_id)` over `TableSchema::all_from_config`, delegating per table to the existing `ensure_table`, and returning a report of created / already-present / failed tables
- [ ] 2.6 Write a failing test asserting one table's failure does not abort the remaining tables in the same dataset, then implement per-table error isolation in the report
- [ ] 2.7 Delete the dead `SchemaRegistry::create_default_tables_for_tenant` stub, its `TenantApi::create_default_tables` wrapper, and the test asserting the stub's placeholder behavior

## 3. Configuration

- [ ] 3.1 Write a failing test in `src/common` for the new `[schema]` reconcile-interval key: default value, explicit override, and the disable value parsing to "no periodic passes" (`cargo test -p common`)
- [ ] 3.2 Add the key to the schema config struct with its default, and add it to `signaldb.dist.toml` with a comment

## 4. Reconcile loop (writer)

- [ ] 4.1 Write a failing test in `src/writer` asserting a startup pass provisions every dataset returned by the tenant registry (`cargo test -p writer`)
- [ ] 4.2 Write a failing test asserting a dataset that appears in the registry after startup is provisioned by a later pass, with no restart
- [ ] 4.3 Write a failing test asserting a failing pass (catalog unreachable) neither fails startup nor aborts the remaining tenants, and that the next pass retries
- [ ] 4.4 Implement the reconciler: startup pass over `list_active_tenants()` × datasets, then a periodic task on the configured interval, wired into `src/writer/src/main.rs` alongside the existing background WAL processing
- [ ] 4.5 Write a failing test asserting a converged deployment issues no catalog calls on subsequent passes, then implement the process-local already-ensured `(tenant, dataset, table)` set
- [ ] 4.6 Add tracing (tenant/dataset/table fields, warn on failure, info on creation) and a counter metric for tables created and provisioning failures
- [ ] 4.7 Verify monolithic mode picks the reconciler up through the shared writer startup path (`cargo run --bin signaldb`), adding a task-spawn assertion if the wiring differs from the standalone binary

## 5. Cross-service integration coverage

- [ ] 5.1 Add a `tests-integration` test: create a tenant through the admin API, ingest nothing, and assert log/trace/metric label queries return empty successfully (`cargo test -p tests-integration`)
- [ ] 5.2 Add a `tests-integration` test asserting a reconciled dataset accepts a subsequent OTLP write into the pre-created tables — no duplicate table, no schema conflict, data queryable afterwards
- [ ] 5.3 Add a `tests-integration` test asserting a dataset created after startup converges on a later pass and is then queryable

## 6. Documentation

No new user-facing API, CLI, or UI surface is introduced — the admin API,
OpenAPI document, SDK, and UI client are unchanged (design.md, Decision 3), so
no surface-parity or client-regeneration work applies.

- [ ] 6.1 Document the reconcile-interval configuration key and the provisioning behavior, routing placement through the docs skill (operations audience)
- [ ] 6.2 Update the `configuration` skill with the new `[schema]` key
- [ ] 6.3 Update the `multi-tenancy` and `storage-layout` skills where they describe tables as appearing on first write
- [ ] 6.4 Update `CLAUDE.md` if the tenant/table lifecycle description it carries becomes inaccurate

## 7. Ship

- [ ] 7.1 Run `cargo fmt`, `cargo clippy --workspace --all-targets --all-features`, and `cargo machete --with-metadata`
- [ ] 7.2 Run the full workspace test suite and confirm the #972 KNOWN-ISSUE pin now passes in its upgraded form
- [ ] 7.3 Split into the three semantic commits / PRs the design describes (read guard, reconcile entry point + config, writer loop), each independently revertible
