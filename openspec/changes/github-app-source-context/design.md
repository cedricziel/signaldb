## Context

See `proposal.md` for motivation. Relevant existing patterns this design reuses:

- `mcp-oauth` already stores opaque, hashed credentials (never raw secrets) with metadata in the catalog and mints/validates tokens via catalog lookup — the installation-credential storage here follows the same shape.
- `admin-management-api-contract` requires every admin/management endpoint to be OpenAPI-first with regenerated Rust SDK and TypeScript client consumers; no hand-written HTTP calls from the CLI or UI.
- `query-ir-core`'s `traces` source already exposes `exception.stacktrace` as an opaque string field (the OTel exception event's stacktrace text), and its `profiles`/`flamegraph` envelope exposes per-level frame _names_ but not a first-class structured file/line pair. This change does not add file/line as an IR field — see Open Questions.

## Goals / Non-Goals

**Goals:**

- Let a tenant link one SignalDB GitHub App across multiple repos/orgs, isolated per tenant.
- Mint GitHub tokens on demand from an app private key; never persist a usable GitHub credential.
- Serve a bounded, cached source snippet for a frame that already has a file path and line number, wherever the UI already has one.

**Non-Goals:**

- Parsing arbitrary language-specific `exception.stacktrace` text into structured `{file, line}` frames. This design assumes a caller (a later change, or a profile format that already carries file/line per frame) already has structured file/line, not a stacktrace blob it must parse itself. See Open Questions.
- Any write, comment, PR, issue, or Actions-triggering capability. Explicitly deferred to a future proposal with its own tenant opt-in.
- Multi-app-per-tenant credential brokering for third-party apps the tenant already has installed elsewhere; SignalDB has exactly one first-party App identity.
- Webhook-driven sync of installation/repo changes in this change; installations are (re)synced by re-listing via the GitHub API on demand, not pushed. Webhooks are an optimization, not a correctness requirement, and can follow later.

## Decisions

**One shared GitHub App identity, many tenant-scoped installations.**
SignalDB registers a single GitHub App (one app id, one private key, one manifest with `contents:read`+`metadata:read`). Each tenant's admin runs GitHub's installation flow against that app and links the resulting installation id to their tenant. This mirrors how most SaaS GitHub integrations work (one app, many installs) and avoids asking every tenant to register their own GitHub App. Alternative considered: a personal-access-token-per-tenant model — rejected because it puts a long-lived, broad-scoped credential in SignalDB's database per tenant and pushes scope discipline onto each user rather than the app manifest.

**Credential storage: private key at deploy time, installation id + linked repos in the catalog.**
The app's private key is operator-provided configuration (like other deploy-time secrets), not a per-tenant database record — it is one key for the whole deployment. What's tenant-scoped and catalog-stored is the installation id, the linked repo list (a cache of what GitHub reports the installation covers, refreshed on link and on list), and the tenant/dataset link. This follows `mcp-oauth`'s precedent of storing only what's needed to mint/validate, never the raw long-lived secret, and additionally means there is no tenant-scoped GitHub secret to leak — only an installation id, which is useless without the deployment's private key.

**Token minting is synchronous and per-request, with a short in-process cache bounded by GitHub's token TTL.**
GitHub installation access tokens are valid for about an hour. Rather than minting a fresh token on every source-context fetch, the design caches the minted token per installation until shortly before its expiry, then remints. This is an implementation-level cache distinct from the snippet cache in `stack-frame-source-context` and doesn't need catalog persistence — losing it on restart just costs one extra mint.

**Snippet cache is keyed on (installation id, repo, ref, file, line-window) and time-bounded, not invalidated on push.**
Source at a fixed commit SHA is immutable, so a cache keyed on a SHA never goes stale and could be cached indefinitely; a cache keyed on a mutable ref (a branch name) needs a TTL. The design uses a single bounded TTL for both cases for simplicity, favoring "occasionally re-fetches immutable content" over "two cache policies." Revisit if GitHub rate-limit pressure from SHA-keyed lookups turns out to matter in practice.

**Source-context lookup is an internal service call, not a new Query IR source.**
`query-ir-core`'s design deliberately keeps the IR to registered signal sources over stored telemetry. Fetched GitHub source is neither telemetry nor tenant data at rest — it's a live, cached lookup against a third party. Modeling it as a Query IR source would imply it's queryable/filterable/aggregatable like a signal, which it isn't. It's exposed as a plain HTTP endpoint the Explore UI calls directly (via the generated TypeScript client), parallel to how profile rendering already has non-IR, Pyroscope-compat endpoints alongside the IR path.

## Risks / Trade-offs

- [Single shared App private key is a high-value secret — its compromise affects every tenant's linked repos] → Store it the same way other deploy-time secrets are handled (operator-provided config/secret store, never in the catalog DB), and scope the app manifest to read-only so a compromised key yields read access, not write/RCE-adjacent capability.
- [GitHub API rate limits are per-installation (typically 5,000 req/hr); a popular trace with many distinct frames viewed by many users could approach that] → The snippet cache (this change) plus the token-mint cache absorb repeat views; if this proves insufficient in practice, a future change can add per-tenant request budgeting, but it's not needed to ship the read path correctly.
- [Repo's default branch or the referenced commit SHA may not exist by the time a snippet is requested (force-push, rebase, deleted branch)] → Treated as ordinary "unavailable" per the `stack-frame-source-context` spec, not a hard error; the frame simply renders without source.
- [Tenant admin links an installation, then a different admin removes the GitHub-side installation directly on GitHub without unlinking in SignalDB] → Token minting will fail against GitHub; the fetch path treats a mint failure the same as "unavailable," and the installation list surface should periodically reconcile (best-effort, not correctness-critical for this change).

## Migration Plan

No data migration: this is new, additive storage (installation records) and new endpoints. Rollout is a config change (register the App, provide its private key and app id to the deployment) plus deploying the new endpoints and UI panels. Rollback is deleting the installation records and disabling the endpoints/UI panels; no existing capability depends on this one.

## Open Questions

- How does a trace's `exception.stacktrace` (currently an opaque OTel-format string) or a profile's per-level frame data become a structured `{file, line}` pair the UI can hand to this lookup? This change assumes that mapping already exists or arrives with structured data (e.g., a profile format that carries file/line per pprof `Location`); turning `exception.stacktrace` text into structured frames per language is a separate, later effort and doesn't change this change's specs or approach.
- Should installation-to-repo resolution match on exact `owner/repo`, or also need branch/ref conventions when a trace's deploy doesn't record which commit was live? Deploy/release-to-commit correlation doesn't exist in SignalDB today; until it does, callers of this lookup must already know the ref (e.g., a `service.version` resource attribute that happens to be a SHA), and lookups without a resolvable ref simply return "unavailable."
