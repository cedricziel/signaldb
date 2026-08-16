## 1. Scope + authorization (common, router)

- [x] 1.1 Failing tests in `common::auth`: `tenant:manage` validates; it is in `API_KEY_SCOPES` but not in the OAuth-grantable/read set; `can_manage_via_key()` is true only for a key whose explicit scopes contain it (false for unscoped legacy keys and for human sessions) (`cargo test -p common`)
- [x] 1.2 Implement the scope constant, validation, and `TenantContext::can_manage_via_key`
- [x] 1.3 Failing router tests (`cargo test -p router`): a `tenant:manage` key lists/creates a dataset, lists/creates/updates/revokes keys, lists/upserts/removes memberships, reads `manage_get_schema`; an ingest-only key gets 403 (keep `ingestion_api_key_cannot_use_human_management_endpoints`); an unscoped legacy key gets 403; a `tenant:manage` key for `acme` targeting `globex` gets 403; the OAuth consent context does not offer `tenant:manage`
- [x] 1.4 Implement `authorize_tenant` (tenant match; human admin OR key with `tenant:manage`) and switch `get_schema` to the same helper; update utoipa descriptions; `UPDATE_OPENAPI=1 cargo test -p router openapi_spec_is_up_to_date` + `cargo xtask generate`

## 2. CLI

- [x] 2.1 Failing clap-tree test: `tenant dataset {list,create,delete}`, `tenant api-key {list,create,update,revoke}`, `tenant membership {list,set,remove}`, `tenant schema get`, `tenant show` exist; `admin api-key create --scope tenant:manage` accepted
- [x] 2.2 Implement over the SDK `manage_*` / `get_tenant_self` operations; destructive verbs with `--yes`/TTY confirm; scope help text lists `tenant:manage`
- [ ] 2.3 CLI integration test against a router: with a `tenant:manage` key, `tenant dataset create staging` then `tenant dataset list` shows it, `tenant api-key create --name ci --scope traces:write` prints the key once, `tenant show` returns the tenant; with a `traces:write`-only key `tenant dataset create` exits non-zero with the access-denied message

## 3. MCP + UI

- [x] 3.1 Failing tests then implement: `tenant_*` tool descriptions drop the human-session caveat and name `tenant:manage`; new `tenant_info` tool (wraps `get_tenant_self`); a `tenant:manage` API-key session's `tenant_create_dataset` succeeds against a mock router that enforces the scope; scope validation text in the key tools includes `tenant:manage`
- [x] 3.2 UI: failing test then implement — the API-key creation scope picker offers `tenant:manage` with a description; listing shows it (`pnpm --filter signaldb-ui test`)

## 4. Parity

- [x] 4.1 Shrink `tests-integration/tests/query_parity.rs` EXCLUDED to `oauth_consent_context`, `oauth_consent_decision`, `manage_create_tenant` (with the reason from design D5); map the 13 un-excluded operations to their CLI paths and MCP tools; run the check — green
- [ ] 4.2 tests-integration e2e: a `tenant:manage` key manages datasets and keys through the SDK/CLI and through an MCP tool call; an ingest-only key is denied on both

## 5. Docs, skills, hygiene

- [x] 5.1 Docs (route via the docs skill): `docs/users/authentication.md` (scope table + the explicit-only rule + tenant self-service table now reachable by key), `docs/users/mcp.md` (tenant tools credential note), CLI usage in the feature docs
- [x] 5.2 Update the `multi-tenancy` skill (management API reachable by `tenant:manage` keys; remove the "human-session-only" boundary text; note the legacy-key exception) and `tempo-api` if it repeats the boundary
- [ ] 5.3 `cargo fmt`, clippy on touched crates, `cargo machete --with-metadata`; `pnpm --filter signaldb-ui lint && test`; `openspec validate management-api-key-scope --type change --strict`
