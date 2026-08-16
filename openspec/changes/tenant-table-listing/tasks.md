## 1. Catalog-backed listing (common)

- [ ] 1.1 Failing tests in `common` (in-memory catalog): tenant with datasets `a` and `b`; after `ensure_dataset_tables(a)`, `list_tables_for_tenant` returns exactly `a`'s signal tables tagged `dataset="a"` and nothing for `b`; after provisioning `b`, both; a tenant with no datasets returns an empty list without error; the existing `test_list_tables_for_tenant_empty` still holds (`cargo test -p common`)
- [ ] 1.2 Implement `list_tables_for_tenant` via `catalog_manager()` + `datasets_for_tenant()` + `build_namespace` + `list_tabulars` (tables only); return `(dataset, table)` pairs
- [ ] 1.3 Failing test then implement the response shape: `TableInfo.dataset`, `ListTablesResponse.datasets` grouping (flat `tables` kept); signal-type mapping includes `profiles`

## 2. Router + generated clients

- [ ] 2.1 Router handler test: `GET /api/v1/tenants/{id}/tables` returns the grouped shape from an in-memory catalog after provisioning through `POST …/tables/create` (`cargo test -p router`)
- [ ] 2.2 utoipa schema updates; `UPDATE_OPENAPI=1 cargo test -p router openapi_spec_is_up_to_date`; `cargo xtask generate`; commit generated files

## 3. Surfaces

- [ ] 3.1 CLI `tenant table list` prints `DATASET  TABLE  TYPE` (and JSON with the new fields); test
- [ ] 3.2 MCP `tenant_list_tables` returns the SDK shape (test asserts `datasets` present)
- [ ] 3.3 UI: failing component test then implement — Tables section grouped by dataset, refetch after provisioning shows the new tables (`pnpm --filter signaldb-ui test`)
- [ ] 3.4 tests-integration `tenant_table_cli.rs`: assert the CLI list shows the provisioned tables (replace the direct catalog read)

## 4. Docs, hygiene

- [ ] 4.1 Docs (route via the docs skill): `docs/operations/table-provisioning.md` — listing is catalog-backed and grouped by dataset; `docs/users/explore-ui.md` Tables section
- [ ] 4.2 `cargo fmt`, clippy on touched crates, `cargo machete --with-metadata`; UI lint/test; `openspec validate tenant-table-listing --type change --strict`
