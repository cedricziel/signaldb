## Context

See proposal.md — Why. Current state that shapes the approach:

- Three bearer parsers exist: `common::auth::middleware` (router; literal
  `"Bearer "` prefix), `acceptor::middleware::auth` + `grpc_auth`
  (case-insensitive `parse_bearer_token` in `common::auth::validation`), and
  `common::flight::auth::FlightAuthInterceptor` (`"Bearer "`/`"bearer "` only,
  bridging async auth with `block_in_place`). Issues #1322/#1323 track the
  duplication.
- `TenantContext` is already the narrow waist; all authorization predicates
  live on it. `can_manage_tenant()` returns `true` for any API key
  (`user_id.is_none()`), which is why `tables/create` accepts ingest-only keys.
- `_system` is injected into `auth.tenants` at config load with
  `admin_api_key` as its only key. The self-monitoring exporter and the
  frontend runtime-config path read that key.
- Scope vocabulary is a Rust const array (`API_KEY_SCOPES`) mirrored by hand in
  three TypeScript files and in CLI/MCP validation.
- The OpenAPI doc is code-first (utoipa); `SecurityAddon` declares one
  `bearerAuth` scheme as a document-wide default; OAuth AS endpoints and
  `/ui/session` are served but not listed in `paths(...)`.
- FDAP alignment is unaffected (no Arrow/Parquet/Flight schema changes; the
  Flight change is authentication metadata only, not wire schema). No WAL or
  Iceberg layout change — no migration or rollback of stored data.

## Goals / Non-Goals

**Goals:**

- One bearer/identifier parsing path in `common::auth`, consumed by router,
  acceptor (HTTP + gRPC), and Flight.
- Make the credential-kind precedence, 401/403 split, and Flight mesh rules
  explicit and tested once, in `common`.
- Separate the admin credential from tenant credentials; make the published
  frontend key provably narrow.
- Scope vocabulary flows from one Rust definition through OpenAPI into every
  client.

**Non-Goals:**

- Accepting session cookies or OAuth tokens on ingest endpoints (acceptor
  keeps API-key-only; the shared parser is parameterised by accepted kinds).
- Key expiry, last-used tracking, rotation, login rate limiting, refresh-token
  family revocation, SSO — tracked separately as capability gaps.
- Changing scope semantics (legacy unscoped keys stay unrestricted for
  ingest/read/schema — including `schema:write`, as the `schema-registry`
  spec already states; `tenant:manage` stays explicit-only). Decision 4
  tightens only the management predicate.
- Touching the MCP server's no-local-authorization stance.

## Decisions

1. **Shared `CredentialParser` + `Authenticator` entry, per-surface policy.**
   `common::auth` exposes `parse_bearer(header) -> Result<Token>`
   (case-insensitive scheme, trimmed, non-empty) and a
   `Authenticator::authenticate(Credentials, Policy)` where `Policy` names the
   accepted kinds (`ApiKey | Session | OAuth | InternalService`) and whether
   `X-Tenant-ID` is mandatory. Router policy = all three user-facing kinds;
   acceptor policy = `ApiKey` only (keeps the attack surface identical to
   today, addressing the #1322 caveat); Flight policy = `InternalService |
ApiKey` or `InternalService` only. Bearer classification stays
   prefix-based, so the OAuth access-token prefix (`sdb_at_`) is reserved:
   API-key creation (admin/management API, bootstrap) and `[[auth.tenants]]`
   config load reject keys that begin with it. The two formats are disjoint
   and classification is deterministic — no lookup-order fallback needed.
   _Alternative:_ merge the acceptor middleware wholesale into the router's —
   rejected because it would silently accept cookies/OAuth on ingest.
2. **Acceptor keeps its path→signal scope map as a post-auth layer.** The
   shared middleware resolves the principal; a thin acceptor layer maps the
   route to `can_ingest(signal)`. This preserves the existing
   `ingest-auth-tenancy` scenarios unchanged.
3. **Flight interceptor becomes a tower `Layer` (async), not an `Interceptor`.**
   Removes the `block_in_place`/`block_on` bridge (same migration the acceptor
   gRPC path already did). Constant-time internal-key compare stays. The
   layer is installed unconditionally; with no `internal_service_key` it
   accepts calls that carry no authorization metadata and logs the startup
   warning (today's behaviour, now specced). A credential that _is_ supplied
   still goes through the shared parser — a malformed header is
   `UNAUTHENTICATED` on every entry point regardless of the mesh setting —
   but nothing is verified.
4. **`can_manage_tenant()` stops being true for every key.** Split into
   `is_tenant_admin()` (human admin or instance admin) and keep
   `can_manage_via_key()`; `tables/create` and any other management-grade
   route use `is_tenant_admin() || can_manage_via_key()`, the same predicate
   `management.rs` already uses. `can_write_schema` is untouched: the
   `schema-registry` spec grants a legacy unscoped key unrestricted schema
   access, and re-opening that is out of scope (see Non-Goals). The table
   routes also honour the key's dataset restriction, which they ignore
   today: a dataset-restricted key lists and provisions only its own
   dataset; naming another is `403`. This is a tightening; call it out in
   release notes.
5. **Self-monitoring key: `[self_monitoring].api_key`, bootstrap-generated.**
   Config load injects `_system` with that key instead of `admin_api_key`.
   When absent, first-boot bootstrap mints a catalog key `sk-_system-<uuid>`
   (reusing `bootstrap.rs`'s path) and logs it once; the exporter reads it
   from the resolved config. When absent and the catalog cannot persist one
   (read-only catalog, catalog error), startup fails with an error naming the
   missing credential before self-monitoring is enabled — the process never
   exports telemetry without a durable key; the operator sets
   `[self_monitoring].api_key` explicitly to run against a read-only catalog.
   `admin_api_key` is no longer added to any tenant. _Alternative:_ keep
   admin-key-as-_system-key and only document it — rejected: it makes "who can read self-monitoring"
   equal to "who is instance admin", and vice versa, with no way to hand a
   dashboard a read-only key.
6. **Frontend key validation at router startup.** The router resolves the
   configured frontend key against the authenticator (config or catalog) and
   asserts: write-only scopes, tenant/dataset match, not the admin or
   self-monitoring key. Failure aborts startup (fail-hard, like the existing
   `SIGNALDB_UI_DIR` rule) rather than silently serving without telemetry.
7. **Scope enum in OpenAPI.** `API_KEY_SCOPES` becomes a `#[derive(ToSchema)]`
   enum (`ApiKeyScope`) used by the create/patch DTOs and by a new
   `GET /api/v1/scopes` (groups + descriptions, consumed by the UI pickers).
   Progenitor and the TS generator emit the enum; `ManagementPanel`,
   `ApiKeys`, and `ConsentView` import it. CLI `--scope` validates against the
   SDK enum.
8. **Three security schemes in `SecurityAddon`, applied per operation.**
   `bearerAuth` (API key), `sessionCookie` (apiKey-in-cookie
   `signaldb_session`), `oauth2` (authorizationCode with PKCE, scopes = read
   scopes). Operations opt in via utoipa `security(...)`; public routes set
   `security(())`. OAuth AS endpoints and `/ui/session` join `paths(...)`
   with DTOs; the consent endpoints already are.
9. **UI 403 handling.** `ApiError` gains `isForbidden(403)`; the HTTP client
   surfaces it as a `ForbiddenState` the shell renders in place; only 401
   triggers the login gate.

## Risks / Trade-offs

- [Tightening `can_manage_tenant` breaks an operator script that provisions
  tables with an ingest key] → release note + `403` body names the required
  scope; `tenant:manage` keys are one CLI command away.
- [Reserving `sdb_at_` rejects an operator's existing config key with that
  prefix] → config load error names the tenant and key name; generated keys
  are `sk-…`, so a collision is a deliberate choice, not an accident.
- [Operators relying on `admin_api_key` to read `_system` in Grafana/MCP lose
  access] → BREAKING note; bootstrap prints the generated self-monitoring key
  once; the hive `signaldb-selfmon` MCP registration must be re-keyed.
- [Acceptor swap changes rejection messages/ordering for OTLP clients] →
  keep `ingest-auth-tenancy` integration tests green as the contract; add a
  parity test asserting identical status classification across router,
  OTLP/HTTP, OTLP/gRPC, Flight for the same malformed inputs.
- [Flight layer conversion regresses writer/querier latency] → benchmark
  `do_get`/`do_put` in the existing bench suite before/after; the async layer
  removes a blocking hop, expected neutral-to-better.
- [Scope enum in OpenAPI changes generated SDK types] → SDK/TS regeneration is
  part of the change; CLI and UI compile against the new types in the same
  PR stack.
- [Large change] → ship as a stack: (a) common parser + parity tests,
  (b) acceptor/Flight adoption, (c) authz tightening + frontend-key check,
  (d) self-monitoring credential, (e) OpenAPI/scope enum + client regen + UI.

## Migration Plan

1. Deploy with `[self_monitoring].api_key` set explicitly (or accept the
   bootstrap-generated key from the first-boot log); update any consumer that
   used `admin_api_key` for `_system` reads.
2. Replace ingest-only keys used for table provisioning with `tenant:manage`
   keys.
3. Rollback: previous binary reads the same config and catalog; the generated
   self-monitoring key is an ordinary catalog key and stays valid; no data
   migration to undo.

## Open Questions

- Should `GET /api/v1/scopes` also return per-scope human descriptions for the
  UI, or should those live in the UI's i18n layer? (Does not affect specs.)
