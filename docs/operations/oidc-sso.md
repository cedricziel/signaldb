---
audience: operator
type: how-to
status: living
sources:
  - src/common/src/config/mod.rs
  - src/router/src/oidc.rs
  - src/router/src/endpoints/oidc.rs
  - src/router/src/endpoints/session.rs
  - src/common/src/catalog.rs
  - signaldb.dist.toml
---

# Setting up SSO / OIDC login

SignalDB can act as an OIDC **relying party** against a single identity
provider, so humans sign in to the Explore UI with your IdP instead of a
SignalDB password. This guide covers provider configuration, the reverse-proxy
redirect-URL caveat, the just-in-time allowlist, group-to-role mapping,
degraded-startup behaviour, and rollback.

SSO is a UI + HTTP surface only: it authenticates browser sessions (and, for
free, the MCP OAuth consent flow that rides a browser session). API keys, the
`admin_api_key`, and the CLI stay on their own credentials — see
[Break-glass always survives](#break-glass-always-survives) below.

## Prerequisites

- SignalDB's catalog migration for the OIDC columns has run (it ships with the
  binary and is additive; nothing to do beyond deploying the version that has
  this feature).
- An OAuth/OIDC client registered with your IdP, with a **confidential** client
  secret (SignalDB authenticates to the token endpoint as a confidential
  client).
- The IdP's issuer URL, from which SignalDB resolves every endpoint via
  `.well-known/openid-configuration` — you never configure the authorization,
  token, or JWKS URLs by hand.

## Get the redirect URL right first (reverse-proxy caveat)

The callback the IdP must redirect back to is:

```
https://<your-signaldb-host>/ui/session/oidc/callback
```

SignalDB derives that URL from the incoming request's origin — it reads
`X-Forwarded-Host` / `X-Forwarded-Proto` (the reverse-proxy case) and falls
back to the `Host` header with an `https` scheme. That derivation is convenient
for a plain single-host deployment, but it **trusts the `Host` /
`X-Forwarded-Host` header**, which a client controls.

> **For anything internet-facing, set `redirect_url` explicitly.** Pin the
> exact external callback URL in `[auth.oidc].redirect_url` rather than relying
> on header derivation. This is both a correctness fix (a proxy that rewrites
> the host/scheme otherwise produces a callback URL the IdP rejects) and a
> hardening step (a spoofed `Host` header cannot steer the redirect). When
> `redirect_url` is set it is used verbatim for both the authorization request
> and the token exchange, and it must be registered as an allowed redirect URI
> in the IdP.

Register the same value as an allowed redirect/callback URI in the IdP's client
configuration.

## Configure `[auth.oidc]`

Minimal configuration:

```toml
[auth.oidc]
issuer_url = "https://idp.example.com/application/o/signaldb/"
client_id = "signaldb"
client_secret = "<client-secret>"
# Recommended for anything behind a proxy or exposed to the internet:
redirect_url = "https://signaldb.example.com/ui/session/oidc/callback"
display_name = "Example SSO"   # label on the login button; defaults to the issuer host
```

Every field can also come from the environment with the double-underscore form,
for example `SIGNALDB__AUTH__OIDC__ISSUER_URL`,
`SIGNALDB__AUTH__OIDC__CLIENT_ID`, `SIGNALDB__AUTH__OIDC__CLIENT_SECRET`,
`SIGNALDB__AUTH__OIDC__REDIRECT_URL`. Keep the secret out of the TOML file in
production and pass it via the environment. See the annotated block in
`signaldb.dist.toml` for the full field list.

SignalDB requests the `openid`, `email`, and `profile` scopes; make sure the
client is allowed to request them so the ID token carries an email and name.

### Authentik

1. Create an **OAuth2/OpenID Provider**: authorization-code flow, confidential
   client. Note the client ID and secret.
2. Set the redirect URI to
   `https://signaldb.example.com/ui/session/oidc/callback`.
3. Wrap it in an **Application** and give the users/groups that should reach
   SignalDB access to it.
4. The issuer is the provider's OIDC configuration URL without the
   `.well-known/openid-configuration` suffix, e.g.
   `https://authentik.example.com/application/o/signaldb/`.

```toml
[auth.oidc]
issuer_url = "https://authentik.example.com/application/o/signaldb/"
client_id = "signaldb"
client_secret = "<client-secret>"
redirect_url = "https://signaldb.example.com/ui/session/oidc/callback"
display_name = "Authentik"
```

### Keycloak

1. In your realm, create a **Client** of type OpenID Connect, **Client
   authentication** on (confidential), standard flow enabled.
2. Set **Valid redirect URIs** to
   `https://signaldb.example.com/ui/session/oidc/callback`.
3. Copy the client secret from the client's **Credentials** tab.
4. The issuer is `https://<keycloak-host>/realms/<realm>`.

```toml
[auth.oidc]
issuer_url = "https://keycloak.example.com/realms/observability"
client_id = "signaldb"
client_secret = "<client-secret>"
redirect_url = "https://signaldb.example.com/ui/session/oidc/callback"
display_name = "Keycloak"
```

Other standards-compliant providers (Dex, Pocket ID, hosted IdPs) work the same
way: point `issuer_url` at their discovery document and register the callback.

## Restrict who may self-provision (`allowed_email_domains`)

The first time someone signs in via SSO, SignalDB creates a passwordless user
just-in-time. Gate that with an email-domain allowlist:

```toml
[auth.oidc]
# ...
allowed_email_domains = ["example.com", "example.org"]
```

A verified email whose domain is not on the list is refused and **no user is
created** (the refusal does not disclose whether the address was known).

> **The allowlist gates JIT creation only — not linking.** If a user already
> exists in SignalDB (for example a password user, or one provisioned earlier)
> and the IdP asserts their **verified** email, that user can still link their
> OIDC identity and sign in, even when their email domain is outside
> `allowed_email_domains`. The allowlist governs who may create a *new* account
> by SSO; it does not retroactively restrict already-known accounts from
> linking. To keep an existing user out, disable that user rather than relying
> on the allowlist.

## Map IdP groups to tenant roles

SignalDB memberships stay locally managed by default. Optionally, map an IdP
group claim to tenant memberships, re-applied at every login:

```toml
[auth.oidc]
# ...
group_claim = "groups"   # the ID-token/userinfo claim carrying the user's groups

[[auth.oidc.group_mappings]]
group = "observability-admins"
tenant = "acme"
role = "admin"            # one of: admin | member | viewer

[[auth.oidc.group_mappings]]
group = "observability-viewers"
tenant = "acme"
role = "viewer"
```

How mapped memberships behave:

- **Source-keyed, coexisting with local grants.** A mapping-granted membership
  is stored separately from one an admin granted locally for the same
  tenant/user. Both rows can exist at once. The user's **effective role is the
  higher** of the two (`admin > member > viewer`).
- **Local grants are never touched by mapping.** Admin-granted memberships are
  neither modified nor removed by the login-time sync; only
  mapping-granted rows are.
- **A lost group revokes only the mapped membership.** If a user's token no
  longer carries a group that previously granted a membership, that mapped row
  is removed at the next login — any locally granted membership for the same
  tenant stays.
- **Mapping never grants instance-admin.** The instance-admin flag is never set
  or cleared by group mapping; grant it locally.
- **A rule naming a nonexistent tenant is skipped, not fatal.** If a mapping
  rule points at a tenant that doesn't exist (not yet provisioned, or a typo),
  that rule is dropped with a logged warning and the login proceeds; it does
  not block the user from signing in.

`group_mappings` requires `group_claim` to be set — configuring mappings
without it is a startup error.

## Turn off password login (optional)

Once SSO is configured you can make it the only door:

```toml
[auth.oidc]
# ...
disable_password_login = true
```

While set, the password form is refused for every user and the login page
offers only SSO. This flag is **only honoured when a provider is configured** —
setting it without a valid `[auth.oidc]` provider is a startup error, so you
cannot lock every human out by typo. It does not affect API keys, the
`admin_api_key`, or the CLI bootstrap path.

## Startup and degraded behaviour

SignalDB distinguishes an operator mistake from an IdP being down:

- **Bad configuration fails startup, naming the setting.** A malformed
  `issuer_url`, a missing `client_id`/`client_secret`,
  `disable_password_login` without a provider, or `group_mappings` without
  `group_claim` all stop the instance with a message naming the offending
  setting. The fix is local, so failing fast is correct.
- **An unreachable or invalid issuer does *not* stop the instance.** If
  discovery can't be fetched or fails validation at startup (issuer down, DNS,
  TLS, a bad discovery document), the instance still comes up with SSO marked
  **unavailable**: the login-configuration probe (`GET /ui/session/config`)
  reports `oidc: null`, the SSO start endpoint answers `503` naming the issuer,
  and an error naming the issuer is logged. A background task retries discovery
  with exponential backoff (starting at 1s, capped at 5 minutes) and flips SSO
  to available as soon as it succeeds — **no restart required**.

This is what keeps break-glass honest: even a restart during an IdP outage
still brings up password login (if enabled), API keys, and the bootstrap path.

## Break-glass always survives

Disabling password login and losing the IdP never lock operators out:

- **API keys** authenticate as before.
- **`admin_api_key`** reaches the admin API as before.
- The **CLI/config bootstrap path** can still mint an instance-admin
  credential:

  ```bash
  SIGNALDB_USER_PASSWORD='a-long-bootstrap-password' \
    signaldb-cli --config signaldb.toml user create admin@example.com \
    --tenant acme --role admin --instance-admin
  ```

None of these go through the OIDC or password-session path, so an IdP outage
cannot touch them.

## Verify

- `GET /ui/session/config` returns `oidc: { name: ... }` once discovery has
  succeeded (and `password_enabled` reflecting the switch above).
- The login page shows the SSO button; completing a login sets a
  `signaldb_session` cookie and `GET /api/v1/whoami` names the user.
- For a mapped user, `whoami` (or the membership views) shows the expected
  tenant membership, sourced from the mapping.

## Rollback

To turn SSO off, unset `[auth.oidc]` and redeploy — zero behaviour change
otherwise. SSO-only users then fall back to an admin setting them a password;
mapped memberships remain until an admin removes them.

> **Before rolling back to an *older binary*, delete the mapped membership
> rows first.** The current schema keys `tenant_memberships` on
> `(user_id, tenant_id, granted_by)`, so a local grant and a mapped grant for
> the same user/tenant coexist as separate rows. An older binary keys on
> `(user_id, tenant_id)` and must not meet duplicate pairs, so clear the mapped
> rows before downgrading:
>
> ```sql
> DELETE FROM tenant_memberships WHERE granted_by = 'oidc_mapping';
> ```
>
> The added `users` columns (`oidc_issuer`, `oidc_subject`, and the now-nullable
> `password_hash`) are inert to an older binary and need no cleanup.

## Related

- [Authentication reference](../users/authentication.md) — the user-facing view
  of SSO login, sessions, and break-glass credentials.
- Configuration field reference: the `[auth.oidc]` block in
  `signaldb.dist.toml`.
