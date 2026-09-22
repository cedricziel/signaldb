> Delivered in two stacked changes: the installation layer (sections 1, 2.1–2.3/2.8, 3, 4, 5, 7.3, 8.2–8.3, 9), then the source-snippet lookup (2.4–2.7, 3.5, 6, 7.1–7.2, 8.1). The `stack-frame-source-context` spec's flame-graph locations and repository/ref resolution requirements were added with the second.

## 1. Installation catalog storage (common)

- [x] 1.1 Write a failing `common` test asserting a GitHub installation record (installation id, app account, linked tenant id, linked dataset id (optional), covered repo list, created/updated timestamps) round-trips through the catalog store
- [x] 1.2 Implement the catalog table/record and repository methods (create, get-by-tenant, list-by-tenant, update-repo-list, delete) to make 1.1 pass; verify with `cargo test -p common`
- [x] 1.3 Write a failing test asserting a lookup for a repo not covered by any of tenant A's installations returns "not found" even when tenant B has a covering installation, then implement the tenant-scoped resolution query to make it pass (`cargo test -p common`)
- [x] 1.4 Write a failing test asserting a link-flow state token (bound to an admin id, tenant id, expiry) can be created, consumed exactly once, and is rejected on a second consumption attempt or after expiry; implement the state-token store to make it pass (`cargo test -p common`)
- [x] 1.5 Write a failing test asserting two concurrent completions racing on the same valid, unused state token produce at most one installation record and never leave a consumed token without a corresponding record; implement the validate-consume-create sequence as one atomic (or equivalently idempotent) catalog operation to make it pass (`cargo test -p common`)

## 2. GitHub App client (common)

- [x] 2.1 Write a failing test for app-level JWT signing (RS256, correct `iss`/`iat`/`exp` claims from a configured app id and private key) using a test keypair; implement to pass (`cargo test -p common`)
- [x] 2.2 Write a failing test for exchanging an app JWT for an installation access token against a mocked GitHub token endpoint (wiremock or equivalent), asserting the minted token is never written to the catalog; implement the minting client to pass (`cargo test -p common`)
- [x] 2.3 Write a failing test asserting two mint calls for the same installation within the first token's cached validity window reuse it (only one call reaches the mocked token endpoint), and a third call after simulated expiry mints a fresh one; implement the in-process, expiry-aware token cache to pass (`cargo test -p common`)
- [x] 2.4 Write a failing test for fetching a file's content from a mocked GitHub Contents API response and slicing a bounded line-range snippet from it client-side (repo, ref, path, line, window size), covering "file not found" and "ref not found" mapping to an `Unavailable` result rather than a propagated error; implement the fetch-and-slice client to pass (`cargo test -p common`)
- [x] 2.5 Write a failing test asserting a mocked response whose base64 content decodes to bytes that are invalid UTF-8, and a mocked oversized-content response (over the fixed size cap, including an `encoding: "none"` response), both map to `Unavailable` rather than being sliced or returned; implement the base64-decode, UTF-8-validation, and size-cap checks to pass (`cargo test -p common`)
- [x] 2.6 Write a failing test asserting mocked Contents API responses for a directory (a JSON array), a directory object, an unresolved `symlink` entry, and a `submodule` entry each map to `Unavailable` before any decoding or slicing is attempted; implement the type check to pass (`cargo test -p common`)
- [x] 2.7 Write a failing test asserting the snippet cache (keyed on installation id, repo, ref, file, line window) serves a second identical lookup without a second call to the mocked GitHub client, and that exceeding a configured capacity evicts the least-recently-used entry rather than growing unbounded; implement the TTL-and-capacity-bounded, LRU-evicting cache to pass (`cargo test -p common`)
- [x] 2.8 Write a failing test asserting the callback `code`-for-user-token exchange against a mocked GitHub OAuth endpoint, and that a mocked `GET /user/installations` response is checked for the callback's `installation_id` before returning success, with a failed exchange or a non-matching installation id mapping to a rejection; implement the exchange-and-verify client to pass (`cargo test -p common`)

## 3. Admin/management HTTP API (router)

- [x] 3.1 Add the GitHub-installations endpoints to the OpenAPI spec: a state-token issuance step (returns the state token and the GitHub install-flow URL to redirect the admin to) and `GET/DELETE /api/v1/manage/tenants/{id}/github-installations`, plus the state-bound completion of the link (`POST .../github-installations` accepting the returned state token and installation id), matching `admin-management-api-contract`'s schema conventions
- [x] 3.2 Write a failing `router` integration test asserting a tenant admin can request a state token, complete the link with a matching installation id, and see it in the list; and that a non-admin request is rejected; implement the handlers against the catalog methods from Tasks 1.1-1.2 to make it pass (`cargo test -p router`)
- [x] 3.3 Write a failing test asserting a link completion with a missing, expired, mismatched-tenant, or already-consumed state token is rejected and creates no installation record, and that no outcome ever leaves a consumed state token without a corresponding installation record; implement against Task 1.4/1.5's state-token store to make it pass (`cargo test -p router`)
- [x] 3.4 Write a failing test asserting a list request triggers a live repo-list refresh (reflected in the response and persisted) and that a simulated refresh failure falls back to the last-stored repo list rather than failing the request; implement to pass (`cargo test -p router`)
- [x] 3.5 Write a failing test asserting removing an installation link causes an immediate-subsequent source-context fetch (Task 6) for that installation's repos to report "unavailable"; implement removal semantics to pass (`cargo test -p router`)
- [x] 3.6 ~~Validate the app manifest at startup~~ Replaced (see design: "Read-only scope is enforced at link time"): SignalDB holds no manifest, so the callback refuses any installation GitHub reports with a `write`/`admin` permission (`router::github::write_permissions`, tested in `endpoints::github`)
- [x] 3.7 Write a failing test asserting a link completion presenting a valid, matching state token but an `installation_id` not owned by the authorizing GitHub user (per Task 2.8's exchange-and-verify client) is rejected and creates no installation record; implement the check in the link-completion handler to make it pass (`cargo test -p router`)

## 4. Generated clients

- [x] 4.1 Regenerate the Rust SDK (`src/signaldb-sdk`) from the updated OpenAPI document and verify it builds (`cargo build -p signaldb-sdk`)
- [x] 4.2 Regenerate the TypeScript client (`src/ui/src/api/gen`) from the updated OpenAPI document and verify the UI package typechecks

## 5. CLI

- [x] 5.1 Add `signaldb tenant github-installations link|list|remove` commands consuming the regenerated Rust SDK (no hand-written HTTP calls), where `link` drives the state-token-issue-then-redirect-then-complete flow; write a CLI test exercising link/list/remove against a test server and verify it passes

## 6. Source-context lookup endpoint

- [x] 6.1 Add `POST /api/v1/tenants/{id}/source-context` to the OpenAPI spec, noting it is a runtime read available to any authenticated tenant caller and is documented outside the `admin-management-api-contract` capability's admin/management path prefixes
- [x] 6.2 Write a failing `router` test for the endpoint resolving repo+ref+file+line to a snippet using Tasks 1-2, authorized to the caller's own tenant; implement to pass (`cargo test -p router`)
- [x] 6.3 Write a failing test asserting a request naming a repo covered only by another tenant's installation returns the same "unavailable" shape as no installation at all; implement to pass (`cargo test -p router`)
- [x] 6.4 Regenerate the Rust SDK and TypeScript client for this endpoint (repeat Task 4's regeneration for this addition)

## 7. Explore UI integration

- [x] 7.1 Add a "view source" affordance to the trace exception detail panel that calls the generated TS client's source-context endpoint only when the exception frame carries a file path and line number, and renders nothing extra when it doesn't; add a UI test covering both cases
- [x] 7.2 Add the equivalent affordance to the profile flame-graph frame detail panel where a frame carries file/line data; add a UI test covering both cases
- [x] 7.3 Add a tenant-settings UI surface to link (via the state-token-issue-then-GitHub-install-redirect flow), list, and remove GitHub installations, consuming the generated TS client; add a UI test covering list and remove

## 8. Cross-cutting verification

- [x] 8.1 Add an integration test in `tests-integration` covering the end-to-end flow: issue a state token, link an installation with it (against a mocked GitHub API), fetch source context for a covered repo, remove the installation, and verify a subsequent fetch reports unavailable
- [x] 8.2 Add boundary spans, via the `common::self_monitoring::spans` factories (`skip_all` plus explicit bounded fields), to the new outbound GitHub HTTP client calls and the new router endpoints; verify no bare `#[tracing::instrument]` or `otel.kind` usage was introduced outside `common::self_monitoring`
- [x] 8.3 Run `cargo fmt`, `cargo clippy --workspace --all-targets --all-features`, and `cargo machete --with-metadata`; fix any findings

## 9. Docs

- [x] 9.1 Add a `docs/operations/` page covering GitHub App registration and per-tenant installation linking, routed per the `docs` skill
- [x] 9.2 Add a short section to the `multi-tenancy` skill noting GitHub installations as a tenant-scoped, read-only credential, cross-referencing the new operations doc
