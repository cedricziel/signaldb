## Context

See proposal.md. `TenantSchemaRegistry::list_tables_for_tenant` (common/src/schema/mod.rs:488) is a stub. Provisioning in the same file (`create_default_tables_for_tenant`) already does the right dance: `self.catalog_manager().await?` → `self.datasets_for_tenant(&manager, tenant_id).await?` → per dataset `manager.ensure_dataset_tables(...)`. `CatalogManager` exposes `build_namespace(tenant, dataset)` and `catalog().list_tabulars(&namespace)` (used in `ensure_tables_named`). `TenantApi::list_tables` maps names to `TableInfo { name, schema_type, description }` in `ListTablesResponse { tables, tenant_id }`.

## Goals / Non-Goals

**Goals:** truthful listing from the catalog, grouped by dataset; additive API; every surface shows it.
**Non-Goals:** table sizes/row counts/last-write metadata (separate observability concern); listing tables of other tenants.

## Decisions

**D1 — Reuse the provisioning path.** `list_tables_for_tenant` = `catalog_manager()` + `datasets_for_tenant()` + for each dataset `build_namespace` + `list_tabulars` → `Vec<(dataset, table_name)>`. Signal type derived from the table name as `TenantApi::list_tables` does today (`traces`, `logs`, `metrics_*`, `profiles`, else `custom`). Datasets with a namespace that does not exist yet list as empty, not as an error.

**D2 — Response shape: additive.** `TableInfo` gains `dataset: String`; `ListTablesResponse` gains `datasets: Vec<DatasetTables { dataset, tables: Vec<TableInfo> }>` while keeping the flat `tables` for existing consumers. utoipa schemas updated; regenerate.

**D3 — Surfaces.** CLI prints `DATASET  TABLE  TYPE`; MCP returns the SDK shape unchanged; UI groups by dataset (one heading per dataset) and keeps the "Provision tables" action; after a provision mutation the query is refetched.

**D4 — Tests.** Unit test in `common` with an in-memory catalog: two datasets, provision one via `ensure_dataset_tables`, list → only that dataset's tables; provision the second, list again → both. Router handler test with the in-memory setup. `tests-integration/tests/tenant_table_cli.rs` asserts through the CLI list instead of reading the catalog directly.

## Risks / Trade-offs

- [`list_tabulars` may include views/materialized views] → filter to tables (Iceberg `Tabular::Table`) or name-match the known signal tables; the type mapping labels unknowns `custom` rather than dropping them.
- [Datasets registered in the catalog but not in config] → `datasets_for_tenant` already unions both sources for provisioning; listing uses the same helper so they agree.

## Migration Plan

Additive; no rollback concerns.
