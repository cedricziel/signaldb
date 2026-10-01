---
audience: operator
type: how-to
status: living
sources:
  - src/common/src/config/mod.rs
  - src/common/src/bootstrap.rs
  - src/router/src/demo_guard.rs
  - src/router/src/endpoints/session.rs
---

# Demo mode

`[demo]` turns on a public, read-only account for a SignalDB instance you
want to let anyone explore — a marketing demo, a showcase deployment fed by
a synthetic load generator (see [truenas.md](truenas.md#demo-instance)),
or a sandbox you hand out without issuing individual credentials.

## What it provisions

```toml
[demo]
enabled = true
tenant_id = "demo"          # required; must already exist ([[auth.tenants]])
dataset_id = "otel-demo"    # optional; the dataset the Explore UI pre-selects
username = "demo@example.com" # default shown
password = "demo"           # default shown
```

`enabled` without `tenant_id` fails config validation at startup — the
router won't come up half-configured.

When enabled, the router provisions (and reconciles on every subsequent
restart) a local user signed in as `username`/`password`:

- Not an instance admin, not disabled.
- Its password hash is reset to `password` on every start, so the
  credentials in `signaldb.toml` are always the ones that work — an admin
  who changed it by hand (or an attacker who guessed it) doesn't stick.
- Exactly one `local` tenant membership, on `tenant_id`, at the `viewer`
  role. If a previous run (or an admin) had granted it something higher,
  the next restart downgrades it back to `viewer` rather than leaving the
  elevated role in place.

Provisioning runs after config-tenant sync, so `tenant_id` must already
exist — either via `[[auth.tenants]]` or the admin API — before the demo
account can attach to it. A provisioning failure is logged and does not
block startup; the account is simply reconciled on the next restart.

## How it's enforced

Two independent layers keep the demo account read-only:

1. **Ordinary tenant-role enforcement.** A `viewer` membership already
   fails every mutating request the standard auth path checks
   (`TenantContext::can_write`) — same as any other Viewer in the system.
2. **A dedicated HTTP middleware**, as defense in depth for surfaces that
   aren't tenant-scoped at all (session/profile management, API key
   creation). It resolves the caller's session cookie and, for the demo
   user specifically, refuses every request whose method isn't
   GET/HEAD/OPTIONS with `403 {"error": "The demo account is read-only"}` —
   except a short allowlist of read-only POSTs the Explore UI itself needs:
   - `POST /ui/session` and `DELETE /ui/session` (sign in/out).
   - `POST /api/v1/query` — the Query IR is the read surface itself.
   - `POST /api/v1/tenants/{tenant_id}/source-context` — a read-only GitHub
     source-snippet lookup; persists nothing.

   Every other POST/PUT/PATCH/DELETE — creating API keys, tenants,
   datasets, processors, schema registries, OAuth consent decisions, and so
   on — is refused outright for the demo session before it reaches the
   handler, whether or not that handler would itself have enforced the
   Viewer role correctly.

The login-configuration probe (`GET /ui/session/config`) reports a `demo`
object (`{username, password}`) only when `[demo].enabled` is true, so the
Explore UI's login page can show an "Explore the demo" shortcut. Once
signed in, `GET /ui/session` reports `user.is_demo`, and the UI uses that
to show a "Demo · read-only" badge and hide mutating navigation (API keys,
GitHub integration, schema/processor editing).

## Operational notes

- The demo account's password is stored hashed like any other user's; the
  plaintext only ever appears in `signaldb.toml` and the login-config
  response.
- Rotating the password is a config change (`SIGNALDB__DEMO__PASSWORD=...`
  or edit `signaldb.toml`) plus a restart — the next reconciliation pass
  resets the stored hash.
- Disabling demo mode (`enabled = false`) stops the reconciler and the
  middleware, but leaves the previously-provisioned user and its
  membership in the catalog; remove them via the admin API if you want the
  account gone entirely.
