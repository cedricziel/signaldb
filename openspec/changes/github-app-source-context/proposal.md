## Why

SignalDB can show a stack trace or a profiling frame but never the code behind it — engineers still have to jump into their own checkout to see what line actually threw. A GitHub App installation gives SignalDB read-only, per-tenant access to the repos that produced the telemetry, so it can attach the source snippet to a frame at query time. It's also the credential model SignalDB will eventually need for more autonomous, code-aware work (auto-triage, suggested fixes), so it's worth building the installation/link layer as a first-class, tenant-scoped capability now rather than bolting on a one-off integration later.

## What Changes

- Add a SignalDB GitHub App (registered once, shared across tenants) that a tenant admin installs on one or more repos/orgs and links to a tenant (and optionally a dataset), completing the link only via a single-use, admin-and-tenant-bound state token issued before redirecting to GitHub's install flow — a bare installation id is never sufficient to link.
- Store the installation (installation ID, app/installation identity, linked repo list, linked tenant) in the catalog, following the same opaque-credential pattern as `mcp-oauth`: SignalDB's app private key signs short-lived JWTs to mint per-installation access tokens, held only in a process-local cache until shortly before expiry — no long-lived or durably-stored GitHub token exists.
- Request only `contents:read` and `metadata:read` at installation. No write, PR, issues, or Actions scopes are requested in this change.
- Add an admin/management API surface to install-link, list, and remove GitHub installations for a tenant (`/api/v1/manage/tenants/{id}/github-installations`), mirroring the API-key management surface's admin/CLI/UI/SDK parity. Listing refreshes each installation's covered-repo list from GitHub, falling back to the last-known list if that refresh fails.
- Add a source-context lookup — its own endpoint documented in the main OpenAPI document, not part of `admin-management-api-contract` (that capability is scoped to admin/management operations; this is a per-request read available to any authenticated tenant caller) — that, given a repo, a commit SHA or ref, a file path, and a line number, fetches the file via the GitHub Contents API using the installation's token, slices and caches a bounded snippet of source around that line, and treats binary or oversized files as unavailable rather than returning or slicing them.
- Surface the fetched snippet next to a stack frame in the Explore UI (trace exception detail, profile flame graph frame detail) wherever the frame already carries a file path and line number; add no new Query IR fields for this change (see Design's open question on `exception.stacktrace` frame parsing).
- Document, in this change's design, that write/PR/issue scopes and any autonomous-action capability are explicitly out of scope and will require their own future proposal with separate, explicit tenant opt-in and consent, distinct from the read-only install created here.

## Capabilities

### New Capabilities

- `github-app-integration`: tenant-scoped installation of SignalDB's GitHub App (link/list/remove, credential minting, read-only scope enforcement).
- `stack-frame-source-context`: resolving a (repo, ref, file, line) tuple to a cached source snippet for display against a trace exception frame or profile flame-graph frame that already carries file/line data.

### Modified Capabilities

(none — no existing requirement changes; `query-ir-core` and `explore-ui-profiles` are extended by later, dependent changes if frame-level file/line becomes a first-class IR field, not by this one)

## Impact

- `common`: new catalog tables/records for GitHub App installations (installation id, linked tenant/dataset, linked repos, app credential metadata) and a GitHub API client module (JWT signing for app-level auth, installation-token minting, Contents API fetch, response caching).
- `router`: new admin/management HTTP endpoints for installation link/list/remove under `/api/v1/manage/tenants/{id}/github-installations`, following `admin-management-api-contract` (OpenAPI-first, regenerated SDK/TS client).
- `signaldb-bin`: no new service; endpoints live in the existing router/admin surface.
- `src/signaldb-sdk`: regenerated Rust client covering the new endpoints, consumed by the CLI.
- `src/ui`: Explore UI trace-exception and profile-frame detail panels gain a "view source" affordance calling the new source-context lookup when a linked repo and file/line are available.
- New external dependency: a GitHub REST/App client (JWT signing, e.g. an RS256 JWT crate, and an HTTP client already in the workspace) — no Flight, WAL, Iceberg, or OTLP wire-format changes; not marked BREAKING.
- Docs: `docs/operations/` gains a page for GitHub App setup/installation; `multi-tenancy` skill gains a short section once the capability lands.
