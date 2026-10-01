## 1. Shared credential parsing (common)

- [ ] 1.1 Failing tests in `common`: `parse_bearer` accepts `bearer`/`BEARER`/padded whitespace, rejects empty token, non-Bearer scheme, malformed header with one error classification
- [ ] 1.2 Implement `common::auth::parse_bearer` and an `AuthPolicy { accepted kinds, tenant_header_required }`; route the existing router middleware through it
- [ ] 1.3 Failing tests: router precedence (bearer over cookie), session without `X-Tenant-ID` → 400, OAuth token ignores `X-Tenant-ID`
- [ ] 1.4 Make the router middleware honour `AuthPolicy`; confirm 1.3 passes
- [ ] 1.5 Failing test in `tests-integration`: same malformed `Authorization` values classified identically by router, OTLP/HTTP, OTLP/gRPC, Flight (parity matrix); the matrix distinguishes absent from malformed credentials and includes Flight with `internal_service_key` unset
- [ ] 1.6 Failing tests: an API key starting with `sdb_at_` is rejected by `validate_api_key` (admin + management creation, bootstrap) and by `[[auth.tenants]]` config load with an error naming the reserved prefix; a genuine `sdb_at_` token still routes to OAuth validation
- [ ] 1.7 Implement the reserved-prefix check in `common::auth::validation` and config load; confirm 1.6 passes

## 2. Acceptor and Flight adopt the shared path

- [ ] 2.1 Failing tests in `acceptor`: existing `ingest-auth-tenancy` scenarios re-expressed against the common middleware with `AuthPolicy::api_key_only()`; cookies/OAuth tokens are rejected on ingest
- [ ] 2.2 Replace `acceptor::middleware::auth` and the parsing half of `grpc_auth` with the common middleware plus a thin path→signal `can_ingest` layer; delete the duplicated code (closes #1322)
- [ ] 2.3 Failing tests in `common::flight`: tenant key rejected by writer/compactor (`internal_only`), internal key accepted everywhere, lower-case scheme accepted, constant-time compare retained
- [ ] 2.4 Convert `FlightAuthInterceptor` to an async tower layer using the shared parser; remove `block_in_place`/`block_on`; with `internal_service_key` unset, absent metadata passes and a malformed header is still `UNAUTHENTICATED` (failing test first); keep the startup warning and assert it in a test
- [ ] 2.5 Run the parity matrix from 1.5 green; run the Flight `do_get`/`do_put` benches before/after and record the numbers in the PR

## 3. Authorization tightening

- [ ] 3.1 Failing tests in `common::auth`: `is_tenant_admin()` false for any API key; `tables/create` predicate true for `tenant:manage` key, tenant-admin session, instance admin; false for ingest-only and legacy unscoped keys; `can_write_schema` unchanged — legacy unscoped key still true, `schema:write` key true, ingest-only key false
- [ ] 3.2 Split `can_manage_tenant()` into `is_tenant_admin()` + existing `can_manage_via_key()`; update `endpoints/tenant.rs` (tables/create); leave `can_write_schema` on `has_scope_or_unrestricted`; ensure `403` bodies name the required scope
- [ ] 3.3 Failing tests in `router`: ingest-only key → 403 on `/api/v1/query`; revoked session → 401; table provisioning 403/200 per the new scenarios
- [ ] 3.3a Failing A/B test in `tests-integration`: a key restricted to dataset `staging` lists only `staging` and gets 403 provisioning `production`, while an unrestricted `tenant:manage` key lists and provisions every dataset
- [ ] 3.3b Apply `api_key_dataset_id` in `list_tenant_tables` / `create_tenant_tables` (filter the listing, scope provisioning, 403 on mismatch); confirm 3.3a passes
- [ ] 3.4 Audit every authenticated route for 401-vs-403 correctness (scope, role, tenant, dataset) and fix deviations; close #1323 by extracting the shared error mapping the audit reveals

## 4. Self-monitoring credential

- [ ] 4.1 Failing tests in `common::config`: `[self_monitoring].api_key` parsed; `_system` tenant injected with that key; `admin_api_key` is not a tenant credential for `_system` or any tenant
- [ ] 4.2 Implement config change; remove admin-key injection; wire the exporter (traces/logs/metrics/profiles) and the frontend runtime-config path to the new key
- [ ] 4.3 Failing test in `bootstrap`: self-monitoring enabled + no key + first boot → catalog key minted for `_system`, logged once
- [ ] 4.4 Implement bootstrap generation; verify restart does not re-mint
- [ ] 4.4a Failing test in `bootstrap`: self-monitoring enabled + no key + catalog write fails (read-only catalog) → startup returns an error naming `[self_monitoring].api_key` and no exporter is started; implement the fail-fast path
- [ ] 4.5 Failing test in `router`: frontend key with a read/schema/manage scope, equal to admin/self-monitoring key, belonging to a tenant other than the frontend tenant, or restricted to a dataset other than the frontend dataset, aborts startup — each with its own named reason; conforming key served with `Cache-Control: no-store`
- [ ] 4.6 Implement startup validation of `[self_monitoring.frontend].api_key`

## 5. Scope vocabulary and API contract

- [ ] 5.1 Failing test in `router`: OpenAPI document contains an `ApiKeyScope` enum with all eleven scopes (read scopes included — they are API-key scopes today, the UI's "OAuth-only" comment is the drift), used by create/patch DTOs, and `GET /api/v1/scopes` lists groups; the OAuth consent default equals the enum's read group
- [ ] 5.2 Introduce `ApiKeyScope` (`ToSchema`), `GET /api/v1/scopes`, regenerate `api/signaldb-api.json`
- [ ] 5.3 Failing test in `router`: document declares `bearerAuth`, `sessionCookie`, `oauth2` schemes; admin ops name admin bearer + instance-admin session; management ops name tenant bearer + session (+ oauth2 on reads); `/health`, openapi, `/ui/session` POST carry empty security; `/ui/session`, AS metadata, `/oauth/register|authorize|token` present in paths
- [ ] 5.4 Implement the per-operation security declarations and add the session/OAuth endpoints with DTOs to `paths(...)`; regenerate the spec
- [ ] 5.5 Regenerate the Rust SDK (`src/signaldb-sdk`) and the TypeScript client (`src/ui/src/api/gen`); fix compile fallout
- [ ] 5.6 CLI: `--scope` validates against the SDK enum (failing test first); MCP tool schemas reference the enum

## 6. UI

- [ ] 6.1 Failing UI tests: `ManagementPanel` and `ApiKeys` pickers offer all eleven scopes from the generated enum, grouped identically; `ConsentView` default scopes come from the generated client; no hardcoded scope arrays remain (lint/grep assertion)
- [ ] 6.2 Replace the three hand-maintained scope lists with the generated enum / `GET /api/v1/scopes`
- [ ] 6.3 Failing UI test: 403 renders a forbidden state in place and keeps the session; 401 still opens the login gate
- [ ] 6.4 Implement `isForbidden` + `ForbiddenState` in the HTTP client and shell

## 7. Docs, skills, release notes

- [ ] 7.1 `docs/users/authentication.md`: Bearer matching contract (now true), credential precedence, table-provisioning authority, scopes endpoint
- [ ] 7.2 `docs/operations/`: self-monitoring credential (`[self_monitoring].api_key`, bootstrap generation, rotation), frontend-key bounds, Flight mesh key warning; BREAKING note for `admin_api_key` no longer authenticating `_system`
- [ ] 7.3 Update `multi-tenancy` and `configuration` skills for the new config key and predicates; run the docs-freshness gate after committing
- [ ] 7.4 Update `signaldb.dist.toml` / `signaldb.dev.toml` with `[self_monitoring].api_key` and comments
- [ ] 7.5 Verify Definition of Done: UI + CLI + HTTP + MCP parity for scopes and provisioning authority; OpenAPI + both generated clients in sync; close #1322 and #1323 with evidence
