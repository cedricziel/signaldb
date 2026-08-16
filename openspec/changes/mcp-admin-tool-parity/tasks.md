## 1. API contract: tenant tables/schemas into OpenAPI

- [x] 1.1 Failing router test: the OpenAPI document contains operations for `GET /api/v1/tenants`, `GET /api/v1/tenants/{tenant_id}`, `GET /api/v1/tenants/{tenant_id}/tables`, `POST /api/v1/tenants/{tenant_id}/tables/create`, `GET /api/v1/tenants/{tenant_id}/schemas`, `GET /api/v1/schemas/available` (`cargo test -p router`)
- [x] 1.2 Annotate the six handlers in `endpoints/tenant.rs` (`utoipa::path`, tag `tenants`, `ToSchema` on the DTOs), register in `openapi.rs`. Deviation: `list_tenants`/`get_tenant` needed distinct operation ids (`list_tenants_self`/`get_tenant_self`) to avoid colliding with `endpoints/admin.rs`'s `list_tenants`/`get_tenant`.
- [x] 1.3 Failing test then implement the route-vs-OpenAPI drift guard (every `/api/v1/**`, `/tempo/**`, `/loki/**`, `/prometheus/**` route has an operation; explicit allowlist for `/pyroscope/**`, session/login, UI static). Deviation: axum 0.8 has no route-introspection API, so the guard extracts route literals straight out of the endpoint `router()` fn source (`known_routes_match_router_fn_source` in `router/src/openapi.rs`) and diffs them against `KNOWN_ROUTES`/`ALLOWLISTED_ROUTES`, per design's Risk-section fallback. Pre-existing Tempo v2/echo/metrics, Loki `series`/`detected_fields`, and Prometheus `label_stats`/`series` routes are allowlisted (out of this change's scope, tracked separately) rather than newly annotated.
- [x] 1.4 `cargo xtask generate` — regenerate `api/signaldb-api.json`, `src/signaldb-sdk/src/generated.rs`, `src/ui/src/api/gen`; commit

## 2. Parity manifest derived from the SDK

- [x] 2.1 xtask emits `signaldb_sdk::OPERATIONS: &[&str]` (all operation ids) alongside `generated.rs`; test that it matches the OpenAPI document's operation ids
- [x] 2.2 Rewrite `tests-integration/tests/query_parity.rs`: iterate `OPERATIONS`, `EXCLUDED` (with reasons), `(operation → CLI path)` and `(operation → MCP tool)` maps; fail naming missing surface/operation; fail on stale exclusions/mappings; keep the language and SQL-CLI-only assertions. Deviation: `EXCLUDED` has two entries beyond design's oauth/whoami list — `manage_create_tenant` (session-cookie-only, unreachable via API-key auth) and the rescoped `list_tenants_self`/`get_tenant_self` (redundant with whoami). Written incrementally alongside the MCP/CLI work in tasks 3-4 rather than run-to-fail-first as one step, since the manifest needed those surfaces to exist to even compile; verified against `main` before task 3/4 that the equivalent hand-written checks (`api_key_admin_tools_are_registered`, old `MANIFEST`) covered only the pre-existing 3 admin tools + languages, confirming the gap.

## 2.5 Major deviation discovered while implementing tasks 3-4: the management API is human-session-only

`router::endpoints::management::authorize_tenant` (used by every `manage_*`
handler except `manage_get_schema`, which is stricter still —
`ctx.is_instance_admin` directly) requires `ctx.user_id.is_some()`: a
browser session cookie or an OAuth access token carrying a real per-tenant
membership role. It unconditionally rejects a bare API key — deliberately
and already tested
(`ingestion_api_key_cannot_use_human_management_endpoints` in
`router/src/endpoints/session.rs`, which authenticates with a legacy
_unrestricted_ config API key and still asserts 403). The design's D1
("the router enforces both [scopes and roles]... exactly as for the CLI")
assumed a tenant API key would reach these endpoints; it does not, and
should not — this is a real, intentional privilege boundary (an API key
holder can already write any signal data and provision tables, but
minting/revoking _other_ API keys, deleting datasets, or changing
memberships is reserved for a human with a real role).

The CLI's only authentication is `--api-key`/`SIGNALDB_API_KEY` — it has no
session or OAuth login — so no CLI command can ever reach `manage_list_datasets`,
`manage_create_dataset`, `manage_delete_dataset`, `manage_list_api_keys`,
`manage_create_api_key`, `manage_update_api_key`, `manage_revoke_api_key`,
`manage_list_memberships`, `manage_upsert_membership`,
`manage_remove_membership`, or `manage_get_schema`. Tasks 3 and 4 were
corrected accordingly:

- **CLI**: `signaldb_cli::commands::tenant_self` was scoped down to `tenant
table {list,provision,schemas,available-schemas}` only (the tenant.rs
  table/schema endpoints, gated by `TenantContextExtractor` + `can_manage_tenant()`
  — which _is_ API-key-friendly, unlike `authorize_tenant`). The
  `dataset`/`api-key`/`membership`/`schema get` nouns from the original
  design were removed rather than shipped as commands that always 403; the
  module doc explains why.
- **MCP**: all 11 `tenant_*` tools for datasets/API-keys/memberships/schema
  were kept — they work correctly for an OAuth-authenticated MCP session (a
  real, already-supported credential per `mcp-oauth-dcr`) and correctly
  return a clean access-denied error for a plain-API-key session, matching
  the `mcp-tool-surface` spec's own "Unauthorized management call is denied
  cleanly" scenario. Their descriptions were updated to state the
  human-authentication requirement explicitly.
- **Parity manifest**: `manage_list_datasets`, `manage_create_dataset`,
  `manage_delete_dataset`, `manage_list_api_keys`, `manage_create_api_key`,
  `manage_update_api_key`, `manage_revoke_api_key`, `manage_list_memberships`,
  `manage_upsert_membership`, `manage_remove_membership`, and
  `manage_get_schema` moved from `MANIFEST` to `EXCLUDED` (11 entries, each
  with the specific reason) — the whole-SDK check requires _both_ a CLI
  surface and an MCP tool per non-excluded operation, and no CLI surface is
  possible for these without adding session/OAuth login to the CLI, which is
  out of scope for this change.

This is the reason tasks 3.3, 4.1, and 4.2 below read differently from their
original wording.

## 3. MCP tools

- [x] 3.1 Failing tests (`cargo test -p mcp-server`, mock router): platform-admin tools `list_tenants`, `get_tenant`, `create_tenant`, `update_tenant`, `delete_tenant`, `create_user`, `list_datasets`, `create_dataset`, `delete_dataset`, `revoke_api_key` forward the caller's credential to the admin endpoints and return the SDK-shaped JSON; destructive ones refuse without `confirm == id`; annotations present in `tools/list` (`src/mcp-server/tests/admin_tenant_tools.rs`)
- [x] 3.2 Implement the platform-admin tools (descriptions state "requires the administrative credential")
- [x] 3.3 Failing tests then implement `tenant_list_datasets`, `tenant_create_dataset`, `tenant_delete_dataset`, `tenant_list_api_keys`, `tenant_create_api_key`, `tenant_revoke_api_key`, `tenant_update_api_key`, `tenant_list_memberships`, `tenant_upsert_membership`, `tenant_remove_membership`, `tenant_get_schema`, `tenant_list_tables`, `tenant_create_tables` on the management API; key material once on create, never on list (asserted); `confirm` on delete/revoke; annotations. Per §2.5, the datasets/API-keys/memberships/schema tools work only for an OAuth-authenticated MCP session (descriptions say so explicitly) and are excluded from the parity manifest's CLI requirement, not from the tool surface itself. Deviation: also added `tenant_list_table_schemas` and `list_available_table_schemas` (for `list_tenant_schemas`/`list_available_schemas`, two tenant.rs operations the design's tool list omitted) and `get_schema_registry`/`validate_schema_registry` (pre-existing SDK operations with no MCP tool at all) — the whole-SDK parity check requires every operation covered. `query_metrics`/`search_logs` gained `start`/`end`/`step` to reach `promql_query_range`/`logql_query_range`, matching the CLI's new `--start`/`--end`/`--step`.
- [x] 3.4 Test: an unauthorized management call (router 403) yields the access-denied error and no change

## 4. CLI `tenant` group

- [x] 4.1 Failing clap-tree test: `tenant table {list,provision,schemas,available-schemas}` exists; `admin user create` exists; `whoami` decision per design (add or exclude). See §2.5: `tenant dataset/api-key/membership` and `tenant schema get` are **not** implemented — the endpoints they'd wrap reject any API key, the CLI's only credential. Other deviations: `admin user create` was already reachable as top-level `user create` (no `admin` nesting existed for it pre-change; left as-is to avoid an unrelated breaking change) — the parity manifest maps `create_user` there. `whoami` added as a top-level command (cheaper than excluding, per design's open question). `query` gained `--trace-id` (`query_single_trace`) and `--start`/`--end`/`--step` (`promql_query_range`/`logql_query_range`) — pre-existing operations with no CLI/MCP surface at all before this change.
- [x] 4.2 Implement `tenant table` over the SDK (`list_tenant_tables`, `create_tenant_tables`, `list_tenant_schemas`, `list_available_schemas` — all `TenantContextExtractor`/`can_manage_tenant()`-gated, API-key-friendly). No `--yes`/TTY prompt on `provision` — the existing `admin` destructive verbs (`delete_tenant`, `revoke_api_key`, `delete_dataset`) have none either, and `provision` is additive/idempotent besides.
- [x] 4.3 CLI integration test against a running router (`tests-integration/tests/tenant_table_cli.rs`): `tenant table list` and `tenant table provision` succeed with a tenant key carrying any valid scope; provisioning verified against the Iceberg catalog directly, because `GET /tenants/{id}/tables` (`tenant table list`) is itself a pre-existing stub that always answers `[]` (`TenantSchemaRegistry::list_tables_for_tenant`, "TODO: Implement table listing") — noted here, not fixed, as out of scope. Deviation from the original wording: `tenant dataset list` is replaced with `tenant table list`/`schemas`/`available-schemas`, per §2.5.

## 5. UI

- [x] 5.1 Failing component test: management area shows a "Tables" section per dataset from `listTenantTables` and a "Provision tables" action calling `createTenantTables`, visible only with management rights (`src/ui/src/features/management/ManagementPanel.test.tsx`; the panel itself is only reachable behind the `/manage` route's existing `canManage` redirect, so no additional per-section gating was needed)
- [x] 5.2 Implement using the generated client; refresh after provisioning; error state via `toErrorMessage` (the panel's existing error-rendering convention — `error.message` is not used directly elsewhere in this component either)

## 6. Parity green + closure

- [x] 6.1 Run the parity check from 2.2 — passes; the surface-parity assertion in `tests-integration` is green (`cargo test -p tests-integration --test integration query_parity`: 5/5 passed; full `integration` binary: 183 passed, 3 ignored — pre-existing testcontainer/Docker-dependent tests, 0 failed)
- [ ] 6.2 Docs (route via the docs skill): `docs/users/mcp.md` tool catalogue (two families, confirmation, annotations), CLI reference (`tenant` group), table-provisioning ops doc (SDK/CLI/MCP/UI paths), admin API reference for the new tenant endpoints
- [ ] 6.3 Update skills: `multi-tenancy` (management surface parity), `tempo-api` (admin API section), `dev-workflow` if it lists CLI groups
- [ ] 6.4 `cargo fmt`, clippy, machete; `pnpm --filter signaldb-ui lint && test`; `openspec validate mcp-admin-tool-parity --type change --strict`; close #627 and #628 with the rescoping note
