## Context

See proposal.md — Why. Current state: `router::endpoints::management::authorize_tenant` (management.rs:53) rejects `ctx.user_id.is_none()` outright, then checks tenant match and `can_manage_tenant()`; `get_schema` requires `ctx.is_instance_admin`. `TenantContext::can_manage_tenant()` returns true for instance admins, tenant `Admin` role, **or any API key** (`user_id.is_none()`) — the API-key branch exists for table provisioning (`tenant.rs`, gated by `can_manage_tenant`) and is what `authorize_tenant`'s first check deliberately shadows. Scope vocabulary lives in `common::auth` (`API_KEY_SCOPES`, `READ_SCOPES`, `SCHEMA_SCOPES`, `validate_scopes`); OAuth-grantable scopes are the read set. The parity check `tests-integration/tests/query_parity.rs` has 16 EXCLUDED entries. CLI `tenant` group is `tenant table` only (`signaldb-cli/src/commands/tenant_self.rs`). MCP `tenant_*` tools exist for all management ops (#1261). UI key-creation form lists scopes from a shared constant.

## Goals / Non-Goals

**Goals:** API keys can manage their own tenant when explicitly scoped; the privilege boundary is opt-in and tenant-bound; every management op has CLI + MCP surfaces; exclusion list minimal.

**Non-Goals:** CLI session/OAuth login; letting API keys create tenants via the management API (`admin tenant create` exists); changing what human roles may do.

## Decisions

**D1 — Scope name `tenant:manage`, explicit-only.** Fits the `<resource>:<verb>` vocabulary. Add to `API_KEY_SCOPES` (validated everywhere), NOT to `READ_SCOPES`/OAuth-grantable set. `TenantContext::can_manage_via_key() -> bool` = `user_id.is_none() && api_key_scopes.as_ref().is_some_and(|s| s.contains("tenant:manage"))` — deliberately not `has_scope_or_unrestricted`, so legacy unscoped keys stay out (the only scope with that asymmetry; documented in the doc-comment and the spec). _Alternative:_ treat unscoped legacy keys as fully privileged — rejected: they were minted before management existed for keys; silently widening them is a security surprise.

**D2 — `authorize_tenant` becomes: tenant match first, then `human admin || key with tenant:manage`.** Human path unchanged (`user_id.is_some() && can_manage_tenant()`); key path via D1. Error messages: "Tenant administrator role or tenant:manage scope required". `get_schema` uses the same helper (tenant admin or `tenant:manage`) instead of `is_instance_admin` — the schema view is per-tenant information a tenant admin may see. Keep `ingestion_api_key_cannot_use_human_management_endpoints` (an ingest-only key still 403s), add the positive/negative tests from the spec scenarios.

**D3 — Key minting is tenant-bound by construction.** `manage_create_api_key` already creates keys in `ctx.tenant_id` only; a `tenant:manage` key can mint another `tenant:manage` key for the same tenant (delegation within tenant is fine — same as a tenant admin can).

**D4 — CLI.** `tenant_self.rs` gains `dataset {list,create,delete}`, `api-key {list,create,update,revoke}`, `membership {list,set,remove}`, `schema get`, `show` (= `get_tenant_self`), reusing the `admin` command implementations' output shapes. Destructive verbs: `--yes` or TTY confirm, matching `admin`. `list_tenants_self` maps to `tenant show` too (single-item list); MCP `tenant_info` wraps `get_tenant_self`.

**D5 — Parity.** EXCLUDED shrinks to `oauth_consent_context`, `oauth_consent_decision`, `manage_create_tenant` (reason: instance-admin human self-serve; API keys use `create_tenant` on the admin API). Everything else mapped.

**D6 — UI.** Scope picker constant gains `tenant:manage` with a one-line description ("manage this tenant's datasets, keys, and members"). No other UI change (the UI already uses sessions).

**D7 — OAuth.** Consent scope list unchanged (read scopes only); test asserts `tenant:manage` is not offered.

## Risks / Trade-offs

- [A leaked `tenant:manage` key can revoke keys/delete datasets] → same blast radius as a tenant admin session; opt-in scope, listed prominently in docs; destructive tools keep `confirm`.
- [Unscoped legacy keys behave differently from "unrestricted" elsewhere] → documented as the one intentional exception; the multi-tenancy skill and authentication doc say so.
- [Regenerated clients: descriptions change only] → no schema change; regen still required for the OpenAPI golden test.

## Migration Plan

Additive. Operators grant `tenant:manage` to the keys that need it. Rollback = revert; keys with the scope simply lose management again.
