---
audience: operator
type: how-to
status: living
sources:
  - src/common/src/config/mod.rs
  - src/router/src/github.rs
  - src/router/src/endpoints/github.rs
  - src/router/src/source_context.rs
  - src/router/src/endpoints/source_context.rs
  - src/common/src/catalog.rs
  - signaldb.dist.toml
---

# Connecting GitHub (GitHub App)

SignalDB ships as a **GitHub App** that a tenant admin installs on the
organizations or repositories that produce their telemetry. Once linked,
SignalDB can mint short-lived, read-only tokens for that tenant's repos on
demand, without ever storing a long-lived GitHub credential. This is the
first step of SignalDB's software-delivery integration: today it establishes
the connection; showing source next to a stack frame builds on it.

This guide covers registering the App once per deployment, configuring
`[github]`, how a tenant admin connects, what SignalDB stores, and rollback.

```mermaid
sequenceDiagram
    participant A as Tenant admin (browser)
    participant S as SignalDB router
    participant G as GitHub
    A->>S: POST /api/v1/manage/tenants/{id}/github-installations/link
    S-->>A: install_url (carries a single-use state token)
    A->>G: Install the App on an org / repos
    G->>A: 302 /ui/github/callback?code&installation_id&state
    A->>S: GET /ui/github/callback (session cookie + state)
    S->>G: code → user token; GET /user/installations
    S->>G: app JWT → installation token; GET /installation/repositories
    S-->>A: 302 /integrations/github?github=linked
```

## Prerequisites

- SignalDB's catalog migration for the two GitHub tables has run (it ships
  with the binary and is additive; nothing to do beyond deploying the
  version that has this feature).
- `[public].api_url` set to the externally visible base URL of the router.
  GitHub redirects the admin's browser back to
  `https://<your-signaldb-host>/ui/github/callback`; that URL is registered
  on the App, never derived from request headers.
- A GitHub account that may create a GitHub App for your organization (or a
  personal account, for a personal App).

## Register the App (once per deployment)

1. On GitHub open **Settings → Developer settings → GitHub Apps → New GitHub
   App** (for an organization: the organization's settings page).
2. **GitHub App name**: anything; the URL slug GitHub derives from it is what
   you configure as `app_slug` below.
3. **Callback URL**: `https://<your-signaldb-host>/ui/github/callback`.
4. Tick **Request user authorization (OAuth) during installation**. SignalDB
   uses the code GitHub sends along with the installation to verify that the
   person completing the flow actually owns the installation they are
   linking (see [How linking is secured](#how-linking-is-secured)).
5. Leave **Expire user authorization tokens** on; SignalDB uses the user
   token once, during the callback, and discards it.
6. **Setup URL**: leave empty. **Webhook**: untick _Active_; no webhook is
   needed.
7. **Repository permissions**: `Contents: Read-only` and
   `Metadata: Read-only`. Request nothing else. SignalDB refuses to link an
   installation that carries any write-capable permission.
8. **Where can this GitHub App be installed?**: _Any account_ if tenants
   span several organizations, otherwise _Only on this account_.
9. Create the App, then on its settings page note the **App ID** and the
   **Client ID**, click **Generate a new client secret**, and under
   _Private keys_ click **Generate a private key** (downloads a `.pem`).

## Configure `[github]`

```toml
[github]
app_id = 12345
app_slug = "signaldb"
private_key_path = "/run/secrets/github-app.pem"   # or inline: private_key = "-----BEGIN ..."
client_id = "Iv1.0123456789abcdef"
client_secret = "<client-secret>"
```

Two optional fields bound the source cache described under
[Source context](#source-context): `snippet_cache_ttl` (default `10m`) and
`snippet_cache_capacity` (default `1000` files, least-recently-used
eviction; each entry is one fetched file of at most 512 KiB).

Every field can also come from the environment with the double-underscore
form, for example `SIGNALDB__GITHUB__APP_ID`,
`SIGNALDB__GITHUB__PRIVATE_KEY` (a multi-line PEM works),
`SIGNALDB__GITHUB__CLIENT_SECRET`. Keep the private key and the client secret
out of the TOML file in production.

For GitHub Enterprise Server set `api_url = "https://ghe.example.com/api/v3"`
and `web_url = "https://ghe.example.com"`. `link_state_ttl` (default `10m`)
bounds how long a started "Connect" flow waits for GitHub's redirect back.

A partially filled section fails startup with a message naming the setting
(`[github].app_id`, a missing key, both `private_key` and
`private_key_path` set, an unparsable URL). An unreachable GitHub does not:
the instance comes up and the connect flow reports GitHub's error when it is
used.

## Connect a tenant

Any tenant admin (or instance admin) can do this from the Explore UI: user
menu → **GitHub** → **Connect GitHub**. SignalDB redirects to GitHub's
install page; pick the organization and the repositories, and GitHub sends
the browser back to SignalDB, which records the installation and shows it
in the list with the repositories it covers.

The same surface exists on the HTTP API, the CLI, and MCP:

| Operation                       | HTTP (tenant management API)                                                      | CLI                                                   | MCP tool                            |
| ------------------------------- | --------------------------------------------------------------------------------- | ----------------------------------------------------- | ----------------------------------- |
| Start a link                    | `POST /api/v1/manage/tenants/{id}/github-installations/link` → `install_url`      | `signaldb-cli tenant github link`                     | `tenant_start_github_link`          |
| List installations              | `GET /api/v1/manage/tenants/{id}/github-installations`                            | `signaldb-cli tenant github list`                     | `tenant_list_github_installations`  |
| Remove a link                   | `DELETE /api/v1/manage/tenants/{id}/github-installations/{installation_id}`       | `signaldb-cli tenant github remove <installation_id>` | `tenant_remove_github_installation` |
| Attach an existing installation | `POST /api/v1/manage/tenants/{id}/github-installations/attach` → the installation | (not added to the CLI in this change)                 | `tenant_attach_github_installation` |

The CLI's `link` (and the MCP `tenant_start_github_link` tool) prints/returns
the install URL; open it in a browser where you are
signed in to SignalDB as an admin of that tenant, because the callback
completes against your browser session (see below). Listing refreshes each
installation's repository list from GitHub; if GitHub cannot be reached the
last known list is returned and marked `stale: true`. Removing a link takes
effect immediately: SignalDB stops minting tokens for that installation even
if it is still installed on GitHub. To also revoke GitHub's side, uninstall
the App from the organization's _Installed GitHub Apps_ page.

GitHub allows only one App installation per account. Once one tenant has
linked it, GitHub's install-flow URL for a second tenant on the same account
skips the "Install & Authorize" consent screen entirely and sends the
browser straight to GitHub's own installation-management page — no redirect
back to SignalDB, so `link`/`tenant_start_github_link` has nothing to
complete against. The attach endpoint is the way out of that dead end: it
attaches a tenant to an `installation_id` you already know (read it off
another tenant's linked-installations list, or off GitHub's own installation
settings URL) without going through the install flow again. It trusts the
caller's own `tenant:manage` grant as authorization, consistent with the
rest of the management API, and re-runs the same read-only-permission check
the install flow does, refusing an installation that carries any
write-capable permission.

Re-running the install flow for an installation that is already linked (for
example after adding repositories on GitHub) refreshes the stored record
rather than failing.

## What SignalDB stores

| Where                          | What                                                                                 | Secret?          |
| ------------------------------ | ------------------------------------------------------------------------------------ | ---------------- |
| Config / secret store          | App private key, OAuth client secret                                                 | yes, deploy-time |
| Catalog `github_installations` | installation id, account (org or user), covered repositories, who linked it          | no               |
| Catalog `github_link_states`   | SHA-256 of pending link-flow state tokens, tenant, user, expiry                      | no               |
| Process memory only            | minted installation tokens (about one hour), reused until five minutes before expiry | never persisted  |

The user-to-server token from the OAuth-on-install step is used during the
callback to verify ownership and then dropped; it is never stored.

## How linking is secured

Two independent checks bind each link, and neither alone is enough:

- **SignalDB side.** `link` mints a single-use state token bound to the
  tenant and the admin who asked, and passes it through GitHub's install
  flow as `state`. The callback accepts an installation only with an
  unexpired, unused, matching token, and consuming the token and writing the
  record happen in one catalog transaction, so two racing completions cannot
  both succeed. The callback also requires the browser's SignalDB session:
  the session's user must be the one who started the flow (or, for a flow
  started with a `tenant:manage` API key, an admin of that tenant). This is
  what defeats a crafted install link: a link someone else started cannot
  be completed in your browser.
- **GitHub side.** The callback exchanges GitHub's `code` for a user token
  and checks that the returned `installation_id` appears in that user's own
  `GET /user/installations`. A guessed or learned installation id from
  another organization is rejected and nothing is written.

An installation whose permissions include any write-capable permission is
refused at link time, so a mis-registered App cannot grant SignalDB more
than read access.

Every failure lands the browser on `/integrations/github?github=error&reason=…`
with a generic reason (`state`, `session`, `forbidden`, `github`,
`permissions`), never disclosing which check failed in detail; the router
logs the specifics.

## Source context

Once a tenant has linked its repositories, any signed-in user (or a key
that may read a signal) can ask for the source around a stack frame:

```
POST /api/v1/tenants/{id}/source-context
{ "repository": "octo-org/api", "ref": "3f2a9c1", "path": "src/handler.rs", "line": 42, "context_lines": 8 }
```

The Explore UI does this behind a **View source** control on trace exception
frames, Errors-view stacktrace frames, and profile frames whose file and
line are known. The response is always `200`: either `status: "available"`
with the snippet (the lines around `line`, the repository and ref that
served it, and a link into GitHub), or `status: "unavailable"` with a
`reason` (`not_configured`, `no_installation`, `not_found`, `not_a_file`,
`too_large`, `undecodable`, `line_out_of_range`, `github_error`,
`internal`). A frame whose source is unavailable simply shows no snippet;
it never fails the trace, error, or profile view.

Resolution rules:

- `repository` may be `owner/name` or a GitHub URL. It is resolved only
  against the caller's own tenant's installations; a repository another
  tenant linked is indistinguishable from one nobody linked.
- Omit `repository` and SignalDB probes the tenant's linked repositories
  (the first 25) for the path, serving the first hit and naming it. This is
  what makes lookups work for telemetry that carries no VCS attributes.
- Omit `ref` and the repository's default branch is read; the response then
  carries `"ref": null` and the UI labels the snippet as unpinned. The UI
  passes a ref when the telemetry carries `vcs.ref.head.revision`,
  `vcs.ref.head.name`, or a `service.version` that looks like a commit SHA.
- Only regular files up to 512 KiB of valid UTF-8 are served; directories,
  submodules, unresolved symlinks, binary and oversized files are
  `unavailable`.

Fetched files, and "unavailable" outcomes caused by the content itself,
are cached per installation, repository, ref and path for
`snippet_cache_ttl`, bounded by `snippet_cache_capacity`; every line window
in a cached file is sliced locally, so the frames of one stacktrace that
share a file cost one GitHub request. Removing a link evicts its entries at
once. GitHub's per-installation rate limit (about 5,000 requests an hour)
is therefore consumed once per distinct file per TTL, not once per view.

`GET /api/v1/tenants/{id}/source-context` answers
`{ "configured": bool, "linked": bool }` for the same callers; the Explore
UI uses it to decide whether to offer **View source** at all, since the
installation list itself is a management-only endpoint.

Outside the Explore UI, `signaldb-cli tenant source-context` and the MCP
tool `get_source_context` reach the same lookup.

## Verify

- `GET /api/v1/manage/tenants/{id}/github-installations` returns
  `configured: true` and, after connecting, the installation with its
  repositories.
- The GitHub page for the App shows the organization under _Install App_.
- The router log shows `GitHub installation linked` with the tenant and
  installation id.

## Rollback

Unset `[github]` and redeploy: the endpoints answer 404, the UI hides the
page, and nothing else changes. The `github_installations` and
`github_link_states` tables are inert to a binary without this feature and
need no cleanup; drop them if you want a clean catalog. Uninstall the App on
GitHub to revoke its access there.

## Related

- [Authentication reference](../users/authentication.md) for the tenant
  management API and who may call it.
- The `[github]` block in `signaldb.dist.toml` for the full field list.
