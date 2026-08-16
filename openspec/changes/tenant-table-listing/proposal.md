## Why

`GET /api/v1/tenants/{id}/tables` (and therefore `signaldb tenant table list`, the MCP `tenant_list_tables` tool, and the UI's Tables section) always answers an empty list: `TenantSchemaRegistry::list_tables_for_tenant` is a `TODO` stub. Provisioning works (`…/tables/create` goes through the catalog), but nobody can see the result through the API — the UI shows "no tables" right after a successful provision, and the `dataset-table-provisioning` spec's "Tables are listed through the SDK" scenario is only satisfied with a caveat. The listing should come from the same place provisioning writes to: the Iceberg catalog, per dataset.

## What Changes

- `list_tables_for_tenant` reads the tenant's datasets (same source provisioning uses — config and catalog-registered datasets) and lists the tables in each dataset's Iceberg namespace, returning them grouped by dataset with the signal type, and never invents entries.
- The response gains the dataset each table belongs to (`dataset` on every table entry, plus a `datasets` grouping); the existing fields stay, so the change is additive for the SDK, CLI, MCP, and UI.
- The UI Tables section groups by dataset and reflects a provision immediately; CLI output prints `dataset  table  type`; MCP returns the SDK shape.
- Tests: an in-memory catalog with two datasets, one provisioned and one empty, lists exactly the provisioned tables; provisioning then listing shows what was created; a tenant with no datasets lists nothing without erroring.

No **BREAKING** changes (additive response fields).

## Capabilities

### New Capabilities

- (none)

### Modified Capabilities

- `dataset-table-provisioning`: "On-demand provisioning is reachable from every client surface" — the listing scenario becomes unconditional (real data, grouped by dataset), and a scenario ties provisioning to listing.

## Impact

- **common**: `schema/mod.rs` (`list_tables_for_tenant` via `catalog_manager()` + `datasets_for_tenant()` + `list_tabulars` per `build_namespace(tenant, dataset)`), `tenant_api.rs` (`TableInfo.dataset`, `ListTablesResponse.datasets`), tests.
- **router**: `endpoints/tenant.rs` handler unchanged in shape; OpenAPI schema for the new fields; regenerate clients.
- **signaldb-cli**: `tenant table list` output columns.
- **src/ui**: `ManagementPanel` Tables section grouped by dataset.
- **tests-integration**: `tenant_table_cli.rs` asserts the list after provisioning instead of reading the catalog directly.
- **docs**: `docs/operations/table-provisioning.md` (listing now real), `docs/users/explore-ui.md` if it describes the section.
