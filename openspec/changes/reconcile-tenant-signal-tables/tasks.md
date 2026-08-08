## 1. Read-path guard: absent table reads as empty (querier)

Independent of reconciliation and closes issue #972's symptom on its own. Ship
first. Note that no read path has such a guard today — including traces, contrary
to #972's description.

- [x] 1.1 Write failing tests in `src/querier` for the logs metadata paths: with a registered tenant catalog whose dataset has no `logs` table, `get_labels` and `get_label_values` return empty results instead of an error (`cargo test -p querier`)
- [x] 1.2 Write failing tests in `src/querier` for the traces paths (`TraceService` query and search) under the same missing-table condition
- [x] 1.3 Write failing tests in `src/querier` for the metrics, profiles, and Query-IR (`ir_planner`) paths under the same condition
- [x] 1.4 Write failing tests in `src/querier` asserting the negative cases still error: an unknown tenant, an invalid query, and a planning failure against a table that _does_ exist must not be reported as empty
- [x] 1.5 Add a shared table-resolution helper in the querier keyed on the async lookup (`schema_for_ref(...)?.table(name).await` → `Ok(None)` means absent), never on `SessionContext::table_exist` (hardcoded `true` via `LiveIcebergSchema`, `src/querier/src/flight.rs:98-100`) and never on DataFusion error text
- [x] 1.6 Route the traces, logs, metrics, profiles, and Query-IR metadata/label/search paths through the helper, yielding empty results on absence; leave data-plane errors from existing tables propagating unchanged
- [x] 1.7 Tighten the metrics paths that currently swallow _all_ errors as empty (`src/querier/src/query/metrics.rs:1072`, `:1231`, `:1438`) so absence returns empty but real failures surface — a deliberate behavior change, covered by the 1.4 negative tests
- [x] 1.8 Write a failing test pinning per-signal error wrappers, then split the shared `querier_error_to_status` mapper (`src/querier/src/flight.rs:2252-2261`) so a logs or trace failure is not reported as `"Profile query failed"`
- [x] 1.9 Upgrade the KNOWN-ISSUE pin in `src/querier/tests/lazy_tenant_registration.rs` from "any error except catalog-resolution" to asserting an empty label result, and drop the #972 marker

## 2. Reconcile entry point (common)

- [x] 2.1 Write a failing test in `src/common` asserting `ensure_dataset_tables` creates exactly the tables enabled for the tenant on a fresh tenant/dataset (`cargo test -p common`)
- [x] 2.2 Write a failing test asserting a per-tenant schema override that disables a signal narrows the set for that tenant while another tenant still gets it
- [x] 2.3 Write a failing test asserting globally disabled signal types produce no tables, and that `TableSchema::Custom` entries from `custom_schemas` are skipped rather than attempted (`ensure_table` rejects them by name, `src/common/src/iceberg/table_manager.rs:117-127`)
- [x] 2.4 Write a failing test asserting a second pass over an already-provisioned, already-property-backfilled dataset commits no new table version, snapshot, or data file
- [x] 2.5 Write a failing test asserting concurrent `ensure_dataset_tables` calls for the same dataset both succeed and yield one table per signal
- [x] 2.6 Implement `CatalogManager::ensure_dataset_tables(tenant_id, dataset_id)`: resolve the enabled set via the tenant's schema config, skip `Custom`, delegate per table to `ensure_table`, return a report of created / already-present / failed tables
- [x] 2.7 Write a failing test asserting one table's failure does not abort its siblings, then implement per-table error isolation in the report

## 3. Configuration

- [x] 3.1 Write a failing test in `src/common` for the new `[writer]` reconcile-interval key: default value, explicit override, and the disable value parsing to "no periodic passes" (`cargo test -p common`). It belongs under `[writer]`, not `[schema]` — `SchemaConfig` is tenant-overridable wholesale, which would make a per-tenant reconcile interval meaningless
- [x] 3.2 Add the key to the writer config struct with its default, and add it to `signaldb.dist.toml` with a comment

## 4. Reconcile loop (writer)

- [x] 4.1 Write a failing test in `src/writer` asserting the reconciler sees database-created tenants, not only config-defined ones (`cargo test -p writer`)
- [x] 4.2 Attach a tenant source to the writer's `CatalogManager` (`src/writer/src/main.rs:138`), cloning `bootstrap.catalog()` before `ServiceBootstrap` is moved into `InMemoryFlightTransport` at `:118`. Without this `list_active_tenants` returns config tenants only (`src/common/src/catalog_manager.rs:329-331`)
- [x] 4.3 Write a failing test asserting a tenant whose `default_dataset` has no dataset row still gets that dataset provisioned
- [x] 4.4 Write a failing test asserting a dataset that appears in the registry after startup is provisioned by a later pass, with no restart
- [x] 4.5 Write a failing test asserting a failing pass (catalog unreachable) neither fails startup nor aborts the remaining tenants, and that the next pass retries
- [x] 4.6 Implement the reconciler as `start_table_reconciler()` on `IcebergWriterFlightService`, mirroring `start_background_processing()`: startup pass over the registry (datasets plus each tenant's unrecorded `default_dataset`), then a periodic task on the configured interval
- [x] 4.7 Call `start_table_reconciler()` from both `src/writer/src/main.rs` and `src/signaldb-bin/src/main.rs` — they are independent wirings with no shared startup path, so monolithic mode does not inherit it automatically
- [x] 4.8 Write a failing test asserting a converged deployment issues no catalog calls on subsequent passes, then implement the process-local already-ensured `(tenant, dataset, table)` set
- [x] 4.9 Add tracing (tenant/dataset/table fields, warn on failure, info on creation) and counter metrics for tables created and provisioning failures

## 5. Admin endpoint and compactor fix

- [x] 5.1 Write a failing test asserting `POST /tenants/{tenant_id}/tables/create` actually creates the tenant's tables rather than returning `201` having created nothing (`cargo test -p router`)
- [x] 5.2 Reimplement `SchemaRegistry::create_default_tables_for_tenant` (`src/common/src/schema/mod.rs:394-420`) on top of `ensure_dataset_tables`, keeping the `TenantApi::create_default_tables` wrapper and the endpoint's route, request shape, and `201` response contract intact
- [x] 5.3 Write a failing test in `src/compactor` asserting a table with no current snapshot yields zero compaction candidates without warning (`cargo test -p compactor`), then fix `group_files_by_partition` (`src/compactor/src/planner.rs:306-310`) so newly provisioned empty tables do not warn every cycle

## 6. Cross-service integration coverage

- [x] 6.1 Add a `tests-integration` test: create a tenant through the admin API, ingest nothing, and assert log, trace, metric, and profile label queries return empty successfully (`cargo test -p tests-integration`)
- [x] 6.2 Add a `tests-integration` test asserting a reconciled dataset accepts a subsequent OTLP write into the pre-created tables — no duplicate table, no schema conflict, data queryable afterwards
- [x] 6.3 Add a `tests-integration` test asserting a dataset created after startup converges on a later pass and is then queryable

## 7. Documentation

No new user-facing API, CLI, or UI surface is introduced. The admin endpoint
keeps its route, request shape, and response contract — only its effect changes —
so the OpenAPI document, the generated Rust SDK, and the generated TypeScript
client need no regeneration. Confirm that during 5.2 and note it in the PR.

- [x] 7.1 Document the reconcile-interval configuration key and the provisioning behavior, routing placement through the docs skill (operations audience)
- [x] 7.2 Correct `docs/users/authentication.md:143` and `.claude/skills/multi-tenancy/SKILL.md:177` where they describe the table-create endpoint, now that it does what it claims
- [x] 7.3 Update the `configuration` skill with the new `[writer]` key
- [x] 7.4 Update the `storage-layout` skill where it describes tables as appearing on first write
- [x] 7.5 Update `CLAUDE.md` if the tenant/table lifecycle description it carries becomes inaccurate

## 8. Ship

- [ ] 8.1 Run `cargo fmt`, `cargo clippy --workspace --all-targets --all-features`, and `cargo machete --with-metadata`
- [ ] 8.2 Run the full workspace test suite and confirm the #972 KNOWN-ISSUE pin now passes in its upgraded form
- [ ] 8.3 Split into semantic commits / PRs along the group boundaries (read guard; reconcile entry point + config; writer loop; endpoint + compactor fix), each independently revertible
