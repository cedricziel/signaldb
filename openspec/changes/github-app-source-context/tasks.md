## 1. Installation catalog storage (common)

- [ ] 1.1 Write a failing `common` test asserting a GitHub installation record (installation id, app account, linked tenant id, linked dataset id (optional), covered repo list, created/updated timestamps) round-trips through the catalog store
- [ ] 1.2 Implement the catalog table/record and repository methods (create, get-by-tenant, list-by-tenant, delete) to make 1.1 pass; verify with `cargo test -p common`
- [ ] 1.3 Write a failing test asserting a lookup for a repo not covered by any of tenant A's installations returns "not found" even when tenant B has a covering installation, then implement the tenant-scoped resolution query to make it pass (`cargo test -p common`)

## 2. GitHub App client (common)

- [ ] 2.1 Write a failing test for app-level JWT signing (RS256, correct `iss`/`iat`/`exp` claims from a configured app id and private key) using a test keypair; implement to pass (`cargo test -p common`)
- [ ] 2.2 Write a failing test for exchanging an app JWT for an installation access token against a mocked GitHub token endpoint (wiremock or equivalent), asserting the token is never written to the catalog; implement the minting client and an in-process, expiry-aware token cache to pass (`cargo test -p common`)
- [ ] 2.3 Write a failing test for fetching a bounded line-range snippet from a mocked GitHub Contents API response (repo, ref, path, line, window size), covering the "file not found" and "ref not found" error responses mapping to an `Unavailable` result rather than a propagated error; implement the fetch client to pass (`cargo test -p common`)
- [ ] 2.4 Write a failing test asserting the snippet cache (keyed on installation id, repo, ref, file, line window) serves a second identical lookup without a second call to the mocked GitHub client; implement the cache to pass (`cargo test -p common`)

## 3. Admin/management HTTP API (router)

- [ ] 3.1 Add `POST/GET/DELETE /api/v1/manage/tenants/{id}/github-installations` to the OpenAPI spec (link, list, remove), matching `admin-management-api-contract`'s schema conventions
- [ ] 3.2 Write a failing `router` integration test asserting a tenant admin can link an installation and see it in the list, and a non-admin request is rejected; implement the handlers against the catalog methods from Task 1 to make it pass (`cargo test -p router`)
- [ ] 3.3 Write a failing test asserting removing an installation link causes an immediate-subsequent source-context fetch (Task 6) for that installation's repos to report "unavailable"; implement removal semantics to pass (`cargo test -p router`)
- [ ] 3.4 Write a failing test enforcing the app manifest declares only `contents:read`/`metadata:read` (a config-level assertion, e.g. validating the configured manifest/permissions at startup) and rejects startup if a write-capable permission is present; implement the check to pass (`cargo test -p router` or `-p common`, wherever app config is validated)

## 4. Generated clients

- [ ] 4.1 Regenerate the Rust SDK (`src/signaldb-sdk`) from the updated OpenAPI document and verify it builds (`cargo build -p signaldb-sdk`)
- [ ] 4.2 Regenerate the TypeScript client (`src/ui/src/api/gen`) from the updated OpenAPI document and verify the UI package typechecks

## 5. CLI

- [ ] 5.1 Add `signaldb tenant github-installations link|list|remove` commands consuming the regenerated Rust SDK (no hand-written HTTP calls); write a CLI test exercising link/list/remove against a test server and verify it passes

## 6. Source-context lookup endpoint

- [ ] 6.1 Write a failing `router` test for a `POST /api/v1/tenants/{id}/source-context` (or equivalent) endpoint that resolves repo+ref+file+line to a snippet using Tasks 1-2, authorized to the caller's own tenant; implement to pass (`cargo test -p router`)
- [ ] 6.2 Write a failing test asserting a request naming a repo covered only by another tenant's installation returns the same "unavailable" shape as no installation at all; implement to pass (`cargo test -p router`)
- [ ] 6.3 Add this endpoint to the OpenAPI spec and regenerate the Rust SDK and TypeScript client (repeat Task 4's regeneration for this addition)

## 7. Explore UI integration

- [ ] 7.1 Add a "view source" affordance to the trace exception detail panel that calls the generated TS client's source-context endpoint only when the exception frame carries a file path and line number, and renders nothing extra when it doesn't; add a UI test covering both cases
- [ ] 7.2 Add the equivalent affordance to the profile flame-graph frame detail panel where a frame carries file/line data; add a UI test covering both cases
- [ ] 7.3 Add a tenant-settings UI surface to link (via GitHub's install redirect), list, and remove GitHub installations, consuming the generated TS client; add a UI test covering list and remove

## 8. Cross-cutting verification

- [ ] 8.1 Add an integration test in `tests-integration` covering the end-to-end flow: link an installation (against a mocked GitHub API), fetch source context for a covered repo, remove the installation, and verify a subsequent fetch reports unavailable
- [ ] 8.2 Run `cargo fmt`, `cargo clippy --workspace --all-targets --all-features`, and `cargo machete --with-metadata`; fix any findings

## 9. Docs

- [ ] 9.1 Add a `docs/operations/` page covering GitHub App registration and per-tenant installation linking, routed per the `docs` skill
- [ ] 9.2 Add a short section to the `multi-tenancy` skill noting GitHub installations as a tenant-scoped, read-only credential, cross-referencing the new operations doc
