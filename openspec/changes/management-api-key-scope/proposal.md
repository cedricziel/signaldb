## Why

The tenant management API (`/api/v1/manage/*`: datasets, API keys, memberships, schema) rejects every API key — only a browser session or an OAuth token with a membership role may call it. That made the CLI's `tenant` group table-only and left eleven `manage_*` operations on the client-parity exclusion list (#1261). Automation (CI provisioning datasets, rotating keys, bootstrapping a tenant) has no first-class credential for these actions today; the workaround is the platform admin key, which is far more privileged than the task needs. A well-scoped API key should be able to manage its own tenant.

## What Changes

- New API-key scope **`tenant:manage`**: a key carrying it may call the tenant management API for its own tenant (list/create/delete datasets, list/create/update/revoke API keys, list/upsert/remove memberships, read the tenant schema). It is never OAuth-grantable (like `schema:write`), is explicit only — a legacy unscoped key does NOT gain management (the privilege boundary stays opt-in) — and is selectable on every key-management surface (UI, admin/management API, SDK, CLI, MCP).
- `authorize_tenant` accepts either a human principal with the tenant-admin role / instance-admin, or an API key with `tenant:manage` bound to that tenant; `manage_get_schema` follows the same rule (tenant admin or `tenant:manage`) instead of instance-admin only. Cross-tenant use is still forbidden; the key can only manage the tenant it belongs to.
- CLI: `tenant dataset {list,create,delete}`, `tenant api-key {list,create,update,revoke}`, `tenant membership {list,set,remove}`, `tenant schema get`, `tenant show` (the caller's own tenant view) — all working with an API key carrying `tenant:manage`.
- MCP: `tenant_*` tool descriptions drop the "human session required" caveat; new `tenant_info` tool for the self view; `tools/list` unchanged otherwise.
- Parity: `manage_list_datasets`, `manage_create_dataset`, `manage_delete_dataset`, `manage_list_api_keys`, `manage_create_api_key`, `manage_update_api_key`, `manage_revoke_api_key`, `manage_list_memberships`, `manage_upsert_membership`, `manage_remove_membership`, `manage_get_schema`, `list_tenants_self`, `get_tenant_self` leave the EXCLUDED list. Remaining exclusions: the two browser OAuth-consent endpoints and `manage_create_tenant` (human self-serve signup by an instance administrator; API keys create tenants through `admin tenant create`).
- Docs and skills updated (authentication, multi-tenancy, MCP tool catalogue, CLI usage).

No **BREAKING** changes: existing keys and sessions keep their behaviour; the new scope is opt-in.

## Capabilities

### New Capabilities

- (none)

### Modified Capabilities

- `api-key-management`: the scope vocabulary gains `tenant:manage` ("Scopes are selectable on every key-management surface"), and a new requirement defines what the scope grants and how it is bounded.
- `cli-command-surface`: "Command taxonomy" — the `tenant` group covers datasets, API keys, memberships, schema, tables, and `show`, authenticated by an API key with `tenant:manage`.
- `client-surface-parity`: the exclusion list is reduced to the inherently browser/human operations; a MODIFIED "Query-surface parity is enforced" names them.
- `mcp-tool-surface`: tenant tools work for API-key sessions carrying `tenant:manage` as well as human sessions.

## Impact

- **common**: `auth/mod.rs` scope constants (`API_KEY_SCOPES`, `validate_scopes`, OAuth-grantable set), `TenantContext::can_manage_tenant`/new `can_manage_via_key`.
- **router**: `endpoints/management.rs` (`authorize_tenant`, `get_schema` gate), `endpoints/session.rs` test `ingestion_api_key_cannot_use_human_management_endpoints` (kept — an ingest-only key still cannot) + new positive tests; OpenAPI descriptions; regenerate clients.
- **signaldb-cli**: `commands/tenant_self.rs` grows the nouns; `admin api-key create` scope help text.
- **mcp-server**: tool descriptions, `tenant_info`, scope validation text.
- **src/ui**: API-key creation form offers `tenant:manage`.
- **tests-integration**: `query_parity.rs` EXCLUDED shrink; e2e for a `tenant:manage` key managing datasets/keys via CLI and MCP; negative e2e for an ingest-only key.
- **docs/skills**: `docs/users/authentication.md`, `docs/users/mcp.md`, `.claude/skills/multi-tenancy`.
