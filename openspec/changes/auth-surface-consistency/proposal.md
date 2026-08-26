## Why

An inventory of the auth surface (main @ 43422e6f) found that the behaviour of
request authentication is split across three independently written parsers
(router, acceptor, Flight), that several operator-observable auth behaviours
(session cookies, OAuth acceptance on the router, the Flight mesh secret, the
published frontend ingest key, the `_system` credential) have no spec home at
all, and that a handful of specced surfaces have drifted from their specs
(scope vocabulary hand-copied into the UI three times, OpenAPI declaring one
security scheme while the server accepts three, table provisioning reachable
by any key). Capturing these in OpenSpec now makes the structural clean-up
(#1322, #1323) a spec-driven change instead of an unanchored refactor.

## What Changes

- New `request-authentication` capability covering what today is implicit:
  - one credential-parsing contract for every entry point (HTTP router,
    OTLP/HTTP, OTLP/gRPC, Flight) — `Bearer` scheme matched
    case-insensitively everywhere, identical validators, identical rejection
    semantics;
  - the router's three credential kinds (API key, session cookie, OAuth
    access token), their precedence and tenant-header rules;
  - Flight mesh authentication via `internal_service_key`, internal-only
    services, and the unauthenticated-when-unset warning;
  - **BREAKING (operator config)**: the self-monitoring tenant authenticates
    with its own credential instead of reusing `admin_api_key`;
  - the browser-published frontend telemetry key is validated at startup to be
    a narrow write-only key for its own tenant/dataset — never the admin key,
    never a key with read or management scopes;
  - `401` vs `403` semantics the UI can act on (login gate vs. forbidden
    state).
- `api-key-management`: the scope vocabulary is served by the API contract and
  every first-party surface (both UI key pages, the consent view, CLI, MCP)
  derives its list from it — no hand-maintained copies, no surface offering a
  subset.
- `admin-management-api-contract`: the OpenAPI document declares all three
  security schemes and applies them per operation; the session and OAuth
  authorization-server endpoints are part of the published contract.
- `dataset-table-provisioning`: on-demand provisioning requires tenant
  management authority (`tenant:manage` key, tenant-admin or instance-admin
  session), not merely any valid API key.

## Capabilities

### New Capabilities

- `request-authentication`: how a request to any SignalDB entry point is
  authenticated — credential kinds, parsing contract, precedence, Flight mesh
  secret, self-monitoring and frontend credentials, and 401/403 semantics.

### Modified Capabilities

- `api-key-management`: "Scopes are selectable on every key-management
  surface" gains the requirement that the vocabulary is sourced from the
  generated contract and offered in full on every surface.
- `admin-management-api-contract`: "The published spec matches the served API"
  — the "Authentication is documented" scenario widens to all security
  schemes; a new scenario covers session/OAuth endpoints being in the document.
- `dataset-table-provisioning`: "On-demand provisioning is reachable from
  every client surface" gains an authorization clause.

## Impact

- Crates: `common` (auth module — single parser, `TenantContext` predicates,
  config validation for self-monitoring/frontend keys), `acceptor` (drops its
  private auth middleware in favour of the common one, keeping its path→signal
  scope map), `router` (middleware, OpenAPI `SecurityAddon`, scope enum,
  tables/create guard), `signaldb-bin`/`querier`/`writer`/`compactor` (Flight
  auth wiring and startup warnings), `signaldb-cli` and `signaldb-sdk`
  (regenerated from the contract), `mcp-server` (unchanged behaviour, tests).
- UI: `src/ui` — scope lists on `/manage`, `/api-keys`, consent; 403 handling
  in the HTTP client.
- Config: new `[self_monitoring].api_key` (auto-generated when absent);
  `admin_api_key` stops being a tenant credential. Documented as BREAKING in
  the release notes.
- Docs: `docs/users/authentication.md` (Bearer claim, table-provisioning
  authority), `docs/operations/` self-monitoring credential; `multi-tenancy`
  and `configuration` skills.
- Closes #1322, #1323 once implemented.
