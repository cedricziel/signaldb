## Context

See proposal.md — Why. What shapes the approach:

- Today `/login` renders `LoginPanel` — the 401 gate's `Dialog` — on a blank
  page. The panel owns three things at once: the modal shell, the password
  form, and the post-login tenant picker. `LoginGate` and `ConsentView`
  reuse it as-is, so the modal's hint copy leaks onto the page.
- `oidc-login` (pending) specifies `GET /ui/session/config` →
  `{password_enabled, oidc: {name} | null}`, `GET /ui/session/oidc/start`
  (302 to the IdP) and `/callback` (issues the standard session cookie).
  Its UI decision is one line: "SSO button does a full-page navigation to
  the start endpoint". It leaves open _where the browser lands afterwards_
  and _how the SPA picks a tenant_ when there is no JSON response.
- A session-authenticated request without `X-Tenant-ID` is a 401 by design
  (`common::auth::middleware`), so `whoami` cannot answer "who am I and
  which tenants may I enter" for a browser that has just come back from an
  IdP, or for a fresh tab with nothing in `localStorage`.
- The UI is moving onto the generated client exclusively
  (`ui-migrate-to-generated-sdk`, then a lint rule). New endpoints must be
  in OpenAPI first and consumed through `src/api/gen`.
- No FDAP surface involved: the FDAP version-alignment constraint (Arrow /
  Parquet types re-exported by DataFusion) is unaffected; no Flight v1/v2
  transform, no WAL/Iceberg migration.

## Goals / Non-Goals

**Goals:**

- A sign-in page that reads as SignalDB's front door, on desktop and phone,
  in light and dark.
- One credential-agnostic flow: _credential step → context step → target_.
  Adding SSO — or a second SSO provider — touches the credential step only.
- The page is where failed redirect-based logins land, with the
  `?redirect=` target surviving the round trip, and it can finish the tenant
  step for any browser that arrives with a session but no tenant.
- Ship the two read-only endpoints now so the UI is wired to the probe
  before any IdP code exists.

**Non-Goals:**

- The OIDC relying-party flow itself (config, discovery, callback, JIT
  provisioning) — `oidc-login`.
- Sign-up, password reset, "remember me", MFA. Passwords are created by
  admins or the bootstrap path; nothing here changes that.
- Replacing `LoginGate`'s reactive modal. It stays for mid-session expiry;
  it just renders the new inner components.
- Multiple providers. The probe stays a nullable object; the UI is shaped so
  a list is a mapping change (Decision 4), but no list is added.

## Decisions

### 1. Page, not dialog

`/login` renders a `<main class="login-page">` with three regions: brand
(the favicon path mark plus the "SignalDB" wordmark), a centred card, and a
footer (Docs link). No `Dialog`, no focus trap, no backdrop blur — the page
_is_ the destination. The card is `min(380px, 100vw - 32px)` wide, the same
width class as the consent card, so the three auth surfaces (login page,
gate modal, consent) share one visual family.

```
┌──────────────────────────────────────────────┐
│                                              │
│                 [~]  SignalDB                 │
│                                              │
│      ┌────────────────────────────────┐      │
│      │ Sign in                        │      │
│      │ Use your account to explore    │      │
│      │ logs, traces and metrics.      │      │
│      │                                │      │
│      │ ┌────────────────────────────┐ │      │  ← probe.oidc != null
│      │ │  Continue with Authentik   │ │      │
│      │ └────────────────────────────┘ │      │
│      │ ─────────── or ─────────────── │      │  ← both methods
│      │ Email                          │      │  ← probe.password_enabled
│      │ [                            ] │      │
│      │ Password                       │      │
│      │ [                            ] │      │
│      │ ⚠ Invalid email or password    │      │  ← role="alert", inline
│      │ [          Sign in           ] │      │
│      └────────────────────────────────┘      │
│                                              │
│                Docs · signaldb.dev           │
└──────────────────────────────────────────────┘
```

Below 600px the card goes full-width with 16px gutters; the brand row and
footer stay. The page uses the existing tokens only (`--bg`, `--surface`,
`--border`, `--accent`, `--accent-soft`, `--dim`, `--faint`, `--err`); dark
mode is automatic. The primary action is the top-most method: a filled
`--accent` button with white text (like `.consent-approve`). When both
methods show, the password submit uses the existing soft style so the eye
lands on SSO first — that is the deployment the operator configured SSO for.

_Alternative:_ keep the `Dialog` and add a backdrop image. Rejected — a
focus-trapped modal with nothing behind it is a lie to assistive
technology, and the "system layer" z-index exists for covering content,
which a page has none of.

### 2. Two steps, one state machine, credential-agnostic

`LoginRoute` owns a small state machine. The credential step ends when a
session cookie exists; the context step ends when a tenant is chosen.

| state        | entered when                                              | renders                       |
| ------------ | --------------------------------------------------------- | ----------------------------- |
| `checking`   | mount; `currentSession()` and `loginConfig()` in flight   | card skeleton, no form flash  |
| `credential` | no session (`currentSession()` 401)                       | `LoginMethods` from the probe |
| `context`    | session exists, `tenant == null`, ≥2 memberships          | `TenantPicker`                |
| `no-access`  | session exists, 0 memberships                             | message + "Sign out" link     |
| `done`       | tenant resolved (auto or picked); default dataset fetched | `<Navigate>` to target        |

Both credentials feed the same transition: the password form gets
`{tenant, dataset, memberships}` from `POST /ui/session`; a browser that
arrives on `/login` already holding a cookie (a bookmark, a retried SSO
attempt) gets the same shape from `GET /ui/session` (Decision 3). Neither
path knows about the other. The context step's
"default dataset" lookup (`whoami(tenant)`) and the failure fallback
(`dataset: ""`) move out of `LoginPanel` into the shared step unchanged.

Components:

- `LoginRoute` — page chrome, redirect validation (unchanged), `?error=`
  parsing, the state machine.
- `LoginCard` — heading, hint, children. Headless of layout so `LoginPanel`
  (modal) and the page both wrap it.
- `LoginMethods({config, redirect, hint?, onAuthenticated})` — renders
  `SsoButton` and/or `PasswordForm` with the "or" divider between them.
- `PasswordForm({onAuthenticated})` — today's form, extracted; unchanged
  behaviour and test coverage.
- `SsoButton({name, startUrl})` — an `<a>` styled as the primary button.
  Full-page navigation, never XHR (matches `oidc-login` Decision 8).
- `TenantPicker({memberships, onPicked})` — today's "Choose a tenant" list,
  extracted.
- `LoginPanel` (kept, for `LoginGate` and `ConsentView`) — `Dialog` +
  `LoginCard` + `LoginMethods` + `TenantPicker`, with `hint` set by the
  caller ("Your session expired" / "Sign in to authorize …") instead of
  the current hard-coded text.

### 3. Two read-only endpoints on the router, shipped now

**`GET /ui/session/config`** — exactly the `oidc-login` probe, implemented
here with a constant body `{"password_enabled": true, "oidc": null}`.
Unauthenticated, in `paths(...)`, empty security requirement. `oidc-login`
replaces the constant with its provider state; the schema does not change.
Shipping it first means the UI's four rendering states are tested against
the real generated types before an IdP exists.

**`GET /ui/session`** — session introspection, tenant-less. Authenticates
the `signaldb_session` cookie only (no API key, no `X-Tenant-ID`), and
returns:

```json
{
  "user": {"id": "…", "email": "…", "display_name": "…", "is_instance_admin": false},
  "tenant": "acme" | null,
  "dataset": "prod" | null,
  "memberships": [{"tenant_id": "acme", "name": "Acme", "role": "admin"}]
}
```

`tenant`/`dataset` follow the same auto-select rule as `POST /ui/session`
(sole membership → selected with its default dataset; otherwise `null`),
by calling the existing `list_session_memberships` and resolution helpers
— no duplicated logic. 401 without a valid session. This is the endpoint
`/login`'s "already authenticated?" gate uses, replacing `whoami()`, whose
answer depends on a tenant header the page does not have.

_Alternative:_ make `whoami` tolerate a missing tenant. Rejected — every
other caller relies on `whoami` being tenant-scoped (datasets, default
dataset, role), and the middleware's "missing tenant is 401" rule is what
keeps the gate modal honest. A separate tenant-less endpoint is smaller
than a mode switch on a shared one.

### 4. The credential step is driven only by the probe

`LoginMethods` derives its offering purely from the probe:

| `password_enabled` | `oidc`   | renders                                                                                                             |
| ------------------ | -------- | ------------------------------------------------------------------------------------------------------------------- |
| `true`             | `null`   | password form (primary style)                                                                                       |
| `true`             | `{name}` | SSO button (primary), divider, password form                                                                        |
| `false`            | `{name}` | SSO button only; hint "Password sign-in is off on this instance."                                                   |
| `false`            | `null`   | cannot happen (config error at startup); render the password form anyway so break-glass is never hidden by a UI bug |

The SSO button's `href` is `/ui/session/oidc/start?redirect=<target>`, where
`<target>` is the already-validated same-app path. Internally the button
takes a `providers: {name, startUrl}[]` prop that the route builds from the
probe (`oidc ? [oidc] : []`), so a future list-valued probe changes one
mapping line and no component.

**Probe failure** (network error, 5xx, or 404 from an older router): render
the password form and a one-line notice "Couldn't load sign-in options".
Never hide the password form on a probe failure — hiding it is the one
outcome that could lock an operator out during a partial outage, and the
server refuses password login itself when it is disabled. This is a
fallback for an unreachable probe, not inference of SSO availability from
endpoint probing; SSO is only ever offered when the probe says so.

### 5. Landing after a redirect-based login

The contract is `oidc-login`'s (its Decision 2 and 8, and its spec
"SSO login returns to where it started"); this change consumes it:

- `GET /ui/session/oidc/start?redirect=<path>` validates `redirect` with
  the same same-origin rule as the UI's `safeRedirectTarget` and carries it
  in the signed pending-login cookie.
- On success the callback sets the session cookie and answers
  `302 <path>` — the target itself, not this page. The `App` shell then
  resolves the tenant context for a browser that remembers nothing
  (`oidc-login` task 4.3), reading the session through `GET /ui/session`
  from this change because `whoami` is a 401 without a tenant header.
- On failure the callback answers `302 /login?error=<code>&redirect=<path>`
  with `sso_failed` for every validation failure (one value, so nothing
  leaks about which check tripped) and `no_membership` when a non-admin
  ends up with no tenant membership.

`LoginRoute` keeps a small code → message table ("Single sign-on failed.
Try again, or sign in with your email and password." / "Your account has no
tenant access yet. Ask a tenant admin to add you, then sign in again."),
renders the alert in the credential step, and strips `error` from the URL
after showing it (`replace`) so a reload does not re-report a stale
failure; `redirect` survives so a retry returns to the original target.
`/login` still forwards an authenticated visitor straight through, and for
one that has a session but no tenant context it runs the context step
itself — so a bookmark or a retried SSO attempt that lands here finishes
cleanly even though the callback never targets this page.

`LoginGate` (mid-session 401) passes `redirect = pathname + search` of the
page the user was on; `ConsentView` passes its own `/oauth/consent?…` URL
so the OAuth parameters survive SSO and the consent screen resumes. The
context step runs before the consent redirect; for single-tenant users it
is invisible, and consent asks its own, different question (which tenant
to _grant_), so no state is lost.

### 6. Accessibility and copy

- Page has one `<h1>` ("Sign in"); the modal keeps `<h2>` inside its
  `Dialog` as today.
- Focus lands on the first control on mount: the SSO link when present,
  else the email field. Same order for keyboard users.
- Errors are `role="alert"` paragraphs placed directly above the action
  they belong to; a page-level `?error=` alert sits at the top of the
  card.
- The SSO control is a real link (`<a href>`), so "open in new tab" and
  middle-click behave; it is styled as a button because it is the primary
  action.
- Copy: "Sign in" / "Continue with {name}" / "or" / "Invalid email or
  password" (server message, unchanged) / "Single sign-on failed …". The
  "Queries were rejected as unauthenticated" hint survives only inside
  `LoginGate`, reworded to "Your session has expired. Sign in to
  continue."

### 7. Testing

- Vitest, `LoginMethods`: the four probe rows of Decision 4, the probe-failure
  fallback, SSO link `href` carries the encoded redirect, primary-style
  assignment.
- Vitest, `LoginRoute`: existing cases stay green (password path, redirect
  validation, already-authenticated pass-through) with `currentSession()`
  stubbed instead of `whoami`; new cases — SSO landing with one membership
  (auto), several (picker), zero (`no-access`), `?error=` alerts shown
  once and stripped.
- Vitest, `LoginGate` and `ConsentView`: unchanged behaviour, hint prop
  rendered, redirect passed to the SSO link.
- Playwright (`e2e/navigation.spec.ts`): `/login` renders standalone, and
  with the probe mocked to `{password_enabled: true, oidc: {name: "Acme"}}`
  a "Continue with Acme" link exists whose `href` starts with
  `/ui/session/oidc/start`.
- Router: `GET /ui/session/config` body and OpenAPI presence with empty
  security; `GET /ui/session` 401 without a cookie, auto-selected tenant for
  one membership, `null` with several, instance admin lists every tenant.

## Handshake with oidc-login

`oidc-login` already specifies the return-target contract (start carries
`?redirect=`, the callback lands on the target, failures come back to
`/login` as `sso_failed` / `no_membership`, the `App` shell resolves the
tenant for a browser that remembers nothing). The amendments this change
makes there (task 6.1) are only the facts it supplies:

1. Decision 2 / tasks 4.1–4.2: `GET /ui/session/config` and its
   required-nullable schema already exist; the task shrinks to returning
   the provider state instead of the constant.
2. Decision 8 / tasks 4.3 and 4.5: the shell's "read memberships" step uses
   `GET /ui/session` (`currentSession()`), not `whoami`, which needs a
   tenant header the browser does not have.
3. The credential-step rendering matrix, the SSO link's `href`, and the
   `?error=` handling are tested here; `oidc-login`'s UI tests cover what
   is new to it (membership sources, the shell resolution, e2e).

## Risks / Trade-offs

- [Two pending changes touch `router::endpoints::session`] → this change
  adds two handlers and no middleware; `oidc-login` adds its own module.
  Rebase cost is a `paths(...)` merge.
- [`GET /ui/session` duplicates part of `POST`] → both call
  `list_session_memberships` and the same auto-select helper; the POST
  handler's resolution block is extracted into a function both use, no
  second copy.
- [Redirect-target length in the pending cookie] → consent URLs are a few
  hundred bytes; cap `redirect` at 2 KiB server-side and drop it (not the
  login) beyond that.
- [Instance admins with zero memberships] → `GET /ui/session` lists every
  tenant for them (as `POST` does), so `no-access` never triggers for an
  admin; a brand-new instance with no tenant shows the message with a link
  to the docs' bootstrap section.
- [Older router behind a newer UI bundle] → cannot happen in the embedded
  build (one binary); in `pnpm ui:dev` the probe 404 falls into the
  password-form fallback of Decision 4.

## Migration Plan

Purely additive. Ship the router endpoints, regenerate the clients, then the
UI in PR-sized steps (tasks.md sections 1–3 each stay under 500 lines).
Rollback of any step is a revert; no stored state changes.

## Open Questions

- Instance name on the page: the card says "SignalDB". A
  `[self_monitoring.frontend]`-style runtime value for a deployment name
  ("Sign in to Acme observability") would be nice but is a separate
  config surface — not in scope unless it already exists when implementing.
- Whether `LoginGate` should offer SSO at all, or only the password form
  with a "sign in again" link to `/login`. Leaning to offering both via the
  shared `LoginMethods`; the full-page navigation costs the unsaved explore
  state either way.
