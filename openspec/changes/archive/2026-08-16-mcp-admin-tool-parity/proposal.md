## Why

The `mcp-tool-surface` spec already requires an MCP tool for every SDK-backed capability the CLI exposes — including tenant, API-key, and dataset management — but the MCP server ships only `create_api_key`, `list_api_keys`, and `update_api_key_scopes`; the parity check that is supposed to catch this only covers query languages and compaction, so the gap is invisible. Issues #627 (platform-admin toolset) and #628 (tenant-admin toolset) describe the missing tools but assume a per-key _role_ model that was replaced by API-key scopes and membership roles (#1217), so they need rescoping rather than implementing as written. Two further parity defects surfaced while scoping: the tenant-scoped `tables`/`schemas` endpoints (`/api/v1/tenants/{id}/tables[/create]`, `/schemas`, `/schemas/available`) are outside the OpenAPI document, hence absent from the SDK, CLI, MCP, and the UI's generated client; and neither the CLI nor MCP exposes the tenant self-management (`manage_*`) operations the UI uses (datasets, keys, memberships, schema).

## What Changes

- **Parity check covers the whole SDK.** The surface-parity test derives its manifest from the SDK's operation list (every generated operation) instead of a hand-written query/ops list, with an explicit, reviewed exclusion list (OAuth consent endpoints, session/login, `whoami` → `server_info`, and Flight SQL). Every non-excluded operation must have a CLI command and an MCP tool; the check names what is missing.
- **Tenant table/schema endpoints join the API contract.** `list_tenant_tables`, `create_tenant_tables`, `list_tenant_schemas`, `list_available_schemas` (and the tenant-scoped `list_tenants`/`get_tenant`) get OpenAPI annotations; the Rust SDK and TypeScript client are regenerated.
- **MCP platform-admin tools (admin API, admin key forwarded):** `list_tenants`, `get_tenant`, `create_tenant`, `update_tenant`, `delete_tenant`, `create_user`, `list_datasets`, `create_dataset`, `delete_dataset`, `revoke_api_key` alongside the existing three. Destructive tools (`delete_tenant`, `delete_dataset`, `revoke_api_key`) carry MCP `destructiveHint` annotations and require `confirm: "<id>"`; read-only tools carry `readOnlyHint`. Visibility is not role-filtered (scopes are enforced by the router; a denied call is a clean access-denied error), matching the shipped MCP model.
- **MCP tenant self-management tools (management API, caller identity):** `tenant_list_datasets`, `tenant_create_dataset`, `tenant_delete_dataset`, `tenant_list_api_keys`, `tenant_create_api_key`, `tenant_revoke_api_key`, `tenant_update_api_key`, `tenant_list_memberships`, `tenant_upsert_membership`, `tenant_remove_membership`, `tenant_get_schema`, `tenant_list_tables`, `tenant_create_tables`. Key material is returned exactly once by the create tools and never by list tools.
- **CLI parity:** a `tenant <noun> <verb>` group for the caller-identity management surface (datasets, api-keys, memberships, schema, tables), mirroring the MCP `tenant_*` tools; `admin` gains `user create` if missing; `admin dataset`/`api-key` are unchanged.
- **UI parity:** the management area lists a dataset's signal tables and offers "provision tables" (the manual trigger from the table-provisioning docs), consuming the regenerated client. Platform-admin operations remain out of the UI by design (the admin key is not a browser identity) — scoped out explicitly.
- Issues #627/#628 are closed by this change with a note on the rescoping (scopes/memberships instead of roles; visibility not filtered).

**BREAKING**: none for existing tools (their names and behavior are unchanged; they keep wrapping the admin API).

## Capabilities

### New Capabilities

- (none)

### Modified Capabilities

- `client-surface-parity`: "SDK covers the full API surface" gains the automated whole-SDK parity check with a reviewed exclusion list; "Query-surface parity is enforced" is generalized to all SDK operations.
- `mcp-tool-surface`: the "MCP tools cover the full client capability set" requirement is made concrete for management (platform-admin vs tenant self-management tool families, destructive-tool confirmation and annotations, key-material handling).
- `cli-command-surface`: "Command taxonomy" gains the `tenant <noun> <verb>` group for caller-identity management.
- `dataset-table-provisioning`: the manual provisioning trigger is reachable through the SDK, CLI, MCP, and UI, not only via raw HTTP.

## Impact

- **router**: utoipa annotations on `endpoints/tenant.rs`; `openapi.rs` path list; `api/signaldb-api.json` regenerated.
- **signaldb-sdk**, **src/ui/src/api/gen**: regenerated.
- **mcp-server**: ~23 new tools with descriptions and annotations; confirmation parameters; tests.
- **signaldb-cli**: new `tenant` command group; parity manifest.
- **src/ui**: management → tables list + provision action.
- **tests-integration**: `query_parity.rs` → whole-SDK manifest with exclusion list.
- **docs/skills**: `docs/users/mcp.md` tool catalogue, CLI reference, `multi-tenancy` + `tempo-api` (admin API) skills, table-provisioning ops doc.
- Closes #627, #628.
