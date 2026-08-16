## 1. API contract: tenant tables/schemas into OpenAPI

- [x] 1.1 Failing router test: the OpenAPI document contains operations for `GET /api/v1/tenants`, `GET /api/v1/tenants/{tenant_id}`, `GET /api/v1/tenants/{tenant_id}/tables`, `POST /api/v1/tenants/{tenant_id}/tables/create`, `GET /api/v1/tenants/{tenant_id}/schemas`, `GET /api/v1/schemas/available` (`cargo test -p router`)
- [x] 1.2 Annotate the six handlers in `endpoints/tenant.rs` (`utoipa::path`, tag `tenants`, `ToSchema` on the DTOs), register in `openapi.rs`. Deviation: `list_tenants`/`get_tenant` needed distinct operation ids (`list_tenants_self`/`get_tenant_self`) to avoid colliding with `endpoints/admin.rs`'s `list_tenants`/`get_tenant`.
- [x] 1.3 Failing test then implement the route-vs-OpenAPI drift guard (every `/api/v1/**`, `/tempo/**`, `/loki/**`, `/prometheus/**` route has an operation; explicit allowlist for `/pyroscope/**`, session/login, UI static). Deviation: axum 0.8 has no route-introspection API, so the guard extracts route literals straight out of the endpoint `router()` fn source (`known_routes_match_router_fn_source` in `router/src/openapi.rs`) and diffs them against `KNOWN_ROUTES`/`ALLOWLISTED_ROUTES`, per design's Risk-section fallback. Pre-existing Tempo v2/echo/metrics, Loki `series`/`detected_fields`, and Prometheus `label_stats`/`series` routes are allowlisted (out of this change's scope, tracked separately) rather than newly annotated.
- [x] 1.4 `cargo xtask generate` — regenerate `api/signaldb-api.json`, `src/signaldb-sdk/src/generated.rs`, `src/ui/src/api/gen`; commit

## 2. Parity manifest derived from the SDK

- [x] 2.1 xtask emits `signaldb_sdk::OPERATIONS: &[&str]` (all operation ids) alongside `generated.rs`; test that it matches the OpenAPI document's operation ids
- [ ] 2.2 Rewrite `tests-integration/tests/query_parity.rs`: iterate `OPERATIONS`, `EXCLUDED` (with reasons), `(operation → CLI path)` and `(operation → MCP tool)` maps; fail naming missing surface/operation; fail on stale exclusions/mappings; keep the language and SQL-CLI-only assertions. Run it — it must fail now, listing every gap this change fills

## 3. MCP tools

- [x] 3.1 Failing tests (`cargo test -p mcp-server`, mock router): platform-admin tools `list_tenants`, `get_tenant`, `create_tenant`, `update_tenant`, `delete_tenant`, `create_user`, `list_datasets`, `create_dataset`, `delete_dataset`, `revoke_api_key` forward the caller's credential to the admin endpoints and return the SDK-shaped JSON; destructive ones refuse without `confirm == id`; annotations present in `tools/list` (`src/mcp-server/tests/admin_tenant_tools.rs`)
- [x] 3.2 Implement the platform-admin tools (descriptions state "requires the administrative credential")
- [x] 3.3 Failing tests then implement `tenant_list_datasets`, `tenant_create_dataset`, `tenant_delete_dataset`, `tenant_list_api_keys`, `tenant_create_api_key`, `tenant_revoke_api_key`, `tenant_update_api_key`, `tenant_list_memberships`, `tenant_upsert_membership`, `tenant_remove_membership`, `tenant_get_schema`, `tenant_list_tables`, `tenant_create_tables` on the management API; key material once on create, never on list (asserted); `confirm` on delete/revoke; annotations. Deviation: also added `tenant_list_table_schemas` and `list_available_table_schemas` (for `list_tenant_schemas`/`list_available_schemas`, two tenant.rs operations the design's tool list omitted) and `get_schema_registry`/`validate_schema_registry` (pre-existing SDK operations with no MCP tool at all) — the whole-SDK parity check requires every operation covered. `query_metrics`/`search_logs` gained `start`/`end`/`step` to reach `promql_query_range`/`logql_query_range`, matching the CLI's new `--start`/`--end`/`--step`.
- [x] 3.4 Test: an unauthorized management call (router 403) yields the access-denied error and no change

## 4. CLI `tenant` group

- [x] 4.1 Failing clap-tree test: `tenant dataset {list,create,delete}`, `tenant api-key {list,create,update,revoke}`, `tenant membership {list,set,remove}`, `tenant schema get`, `tenant table {list,provision}` exist; `admin user create` exists; `whoami` decision per design (add or exclude). Deviations: `admin user create` was already reachable as top-level `user create` (no `admin` nesting existed for it pre-change; left as-is, not moved, to avoid an unrelated breaking change) — the parity manifest maps `create_user` there. `whoami` added as a top-level command (cheaper than excluding, per design's open question). `tenant table` also gained `schemas`/`available-schemas` verbs for `list_tenant_schemas`/`list_available_schemas`, two tenant.rs operations the design's tool list omitted but the whole-SDK parity check requires covered. `query` gained `--trace-id` (`query_single_trace`) and `--start`/`--end`/`--step` (`promql_query_range`/`logql_query_range`) — pre-existing operations with no CLI/MCP surface at all before this change.
- [x] 4.2 Implement the commands over the SDK (`manage_*`, tables/schemas ops). Deviation: no `--yes`/TTY prompt on destructive verbs — the existing `admin` destructive verbs (`delete_tenant`, `revoke_api_key`, `delete_dataset`) have none either, so "consistent with existing admin verbs" means immediate execution, matched here.
- [ ] 4.3 CLI integration test against a running router: `tenant dataset list` and `tenant table provision` succeed with a tenant key that has the required scopes

## 5. UI

- [ ] 5.1 Failing component test: management area shows a "Tables" section per dataset from `listTenantTables` and a "Provision tables" action calling `createTenantTables`, visible only with management rights
- [ ] 5.2 Implement using the generated client; refresh after provisioning; error state via `error.message`

## 6. Parity green + closure

- [ ] 6.1 Run the parity check from 2.2 — passes; the surface-parity assertion in `tests-integration` is green
- [ ] 6.2 Docs (route via the docs skill): `docs/users/mcp.md` tool catalogue (two families, confirmation, annotations), CLI reference (`tenant` group), table-provisioning ops doc (SDK/CLI/MCP/UI paths), admin API reference for the new tenant endpoints
- [ ] 6.3 Update skills: `multi-tenancy` (management surface parity), `tempo-api` (admin API section), `dev-workflow` if it lists CLI groups
- [ ] 6.4 `cargo fmt`, clippy, machete; `pnpm --filter signaldb-ui lint && test`; `openspec validate mcp-admin-tool-parity --type change --strict`; close #627 and #628 with the rescoping note
