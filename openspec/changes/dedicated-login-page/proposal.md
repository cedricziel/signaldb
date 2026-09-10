## Why

`/login` exists (#1473) but it is the 401 gate's modal dropped onto an empty
page: a floating dialog with no branding, a hint that reads "Queries were
rejected as unauthenticated" even when nobody queried anything, and a single
hard-coded credential — the email/password form. The pending `oidc-login`
change will add a second credential (SSO), a login-configuration probe, and
a redirect-based flow that returns to the SPA without the JSON response the
password form relies on for tenant selection. Bolting SSO onto the current
modal would mean a second copy of the tenant picker and a page that still
looks like an error dialog. The login page needs to become a real
destination now, structured so `oidc-login` only has to flip a value.

## What Changes

- `/login` becomes a standalone page (brand mark, centred card, footer),
  not a `Dialog`. The gate modal and the OAuth consent screen keep their
  modal shell but render the same inner content.
- The sign-in card is split into a **credential step** and a **context
  step**. The credential step renders whichever methods the server's
  login-configuration probe offers (password form, SSO button, or both);
  the context step (tenant picker, default dataset) is shared by every
  credential, so SSO reuses it unchanged.
- The UI reads `GET /ui/session/config` through the generated client. The
  router ships the probe now, always answering
  `{"password_enabled": true, "oidc": null}`, so the endpoint, its OpenAPI
  entry, and both generated clients land ahead of `oidc-login`.
- A tenant-less session-introspection endpoint, `GET /ui/session`, returns
  the same shape as `POST /ui/session` (resolved tenant, dataset,
  memberships) plus the user. `/login` uses it instead of `whoami`, which
  is a 401 without `X-Tenant-ID` — exactly the state a browser is in after
  an SSO callback or a fresh tab.
- `/login` is where every failed redirect-based login lands and where an
  authenticated browser with no tenant context can finish signing in:
  `?redirect=` (already validated) plus `?error=<code>` rendered as one
  message per code (`sso_failed`, `no_membership` — the codes `oidc-login`
  defines). A successful SSO callback returns straight to its target; the
  shell-side tenant resolution for that case is `oidc-login`'s task and
  builds on `GET /ui/session`.
- Copy and structure fixes: no "queries were rejected" wording on the
  page; the modal keeps its context-specific hint via a prop.

Not breaking: the password form, `POST /ui/session`, cookies, and every
explore route are untouched. No OTLP, query-compat, Flight, or WAL/Iceberg
surface changes.

## Capabilities

### New Capabilities

- `login-page`: the standalone sign-in destination — layout, the
  credential-step offering derived from the login-configuration probe, the
  shared context step, landing after a redirect-based login, and error
  reporting.

### Modified Capabilities

- `explore-ui-navigation`: gains a `/login` route requirement (the route
  landed in #1473 without one) covering standalone rendering outside the
  shell, `?redirect=` validation, `?error=`, and the authenticated
  pass-through.

## Impact

- Crates: `router` (`GET /ui/session/config` returning the static
  password-only answer; `GET /ui/session` introspection; both in
  `paths(...)` with empty security), `signaldb-sdk` and the UI TypeScript
  client (regenerated).
- UI: `src/ui/src/features/shell/` — `LoginRoute` (page), new
  `LoginCard`, `LoginMethods`, `PasswordForm`, `TenantPicker`;
  `LoginPanel`/`LoginGate` and `ConsentView` compose the same pieces;
  `api/session.ts` gains `loginConfig()` and `currentSession()` on the
  generated client; `lib/useWhoami.ts`'s gate switches to `currentSession()`
  for `/login`.
- Relationship to `oidc-login`: this change owns the UI shape and the two
  read-only endpoints; `oidc-login` keeps the OIDC endpoints, the config
  section, and makes the probe report `oidc: {name}`. Three of its
  decisions need a small update to fit (see design.md, "Handshake with
  oidc-login"); a task here records them.
- Relationship to `ui-migrate-to-generated-sdk`: the new session calls go
  straight onto the generated client; the existing `createSession` /
  `deleteSession` / `whoami` migration stays that change's task.
- Docs: `docs/users/explore-ui.md` "Signing in"; `docs/users/authentication.md`
  gains the two endpoints; `tempo-api` skill (admin/UI endpoint list).
