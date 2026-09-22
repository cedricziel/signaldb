## Purpose

The standalone sign-in destination of the embedded UI: how it looks, which
credentials it offers and why, how any credential hands over to the shared
tenant step, and how a redirect-based login lands back on it.

## ADDED Requirements

### Requirement: The login page is a standalone destination

`/login` SHALL render as a full page — brand mark, a centred sign-in card,
and a footer — outside the explore shell and without a modal dialog. It
SHALL use the UI's shared design tokens so it follows the light/dark theme,
and SHALL remain usable on viewports narrower than 600px.

#### Scenario: Page renders without the shell or a dialog

- **WHEN** an unauthenticated visitor opens `/login`
- **THEN** the page shows the brand and a sign-in card with a level-one
  heading "Sign in", and no top bar, signal tabs, or `role="dialog"`
  element is present

#### Scenario: Narrow viewport

- **WHEN** the page is shown at 360px width
- **THEN** the card spans the viewport minus gutters, every control remains
  reachable, and nothing overflows horizontally

### Requirement: Credential offering follows the login-configuration probe

The sign-in card SHALL derive which credentials it offers solely from
`GET /ui/session/config`, consumed through the generated client:
`password_enabled` controls the email/password form and a non-null `oidc`
adds a "Continue with {name}" control. When both are offered, the SSO
control comes first, followed by a divider and the form. The SSO control
SHALL be a link performing a full-page navigation to the SSO start
endpoint, carrying the page's validated redirect target as `redirect`.
When the probe cannot be read, the page SHALL fall back to the password
form with a notice and SHALL NOT offer SSO.

#### Scenario: Password only

- **WHEN** the probe answers `{"password_enabled": true, "oidc": null}`
- **THEN** the card shows the email/password form and no SSO control

#### Scenario: Both credentials

- **WHEN** the probe answers `{"password_enabled": true, "oidc": {"name": "Acme SSO"}}`
- **THEN** the card shows "Continue with Acme SSO" above a divider and the
  email/password form below it

#### Scenario: SSO only

- **WHEN** the probe answers `{"password_enabled": false, "oidc": {"name": "Acme SSO"}}`
- **THEN** the card shows only "Continue with Acme SSO" and a hint that
  password sign-in is disabled

#### Scenario: SSO control carries the redirect

- **WHEN** the page was opened as `/login?redirect=%2Ftraces%3Frange%3D15m`
  and SSO is offered
- **THEN** the SSO control's `href` is
  `/ui/session/oidc/start?redirect=%2Ftraces%3Frange%3D15m`

#### Scenario: Probe unavailable

- **WHEN** the probe request fails or returns a non-2xx status
- **THEN** the card shows the email/password form, a notice that sign-in
  options could not be loaded, and no SSO control

### Requirement: The tenant step is shared by every credential

After any credential establishes a session, the page SHALL resolve the
tenant context the same way: a session whose introspection reports a
selected tenant proceeds directly; one that reports several memberships
shows the tenant picker; one that reports none shows a "no access" message
with a sign-out action. The resolved tenant's default dataset SHALL be
looked up before navigating, and a lookup failure SHALL navigate with an
empty dataset rather than block.

#### Scenario: Password login, single membership

- **WHEN** the password form succeeds and the response names a tenant
- **THEN** the page navigates to the redirect target with `tenant` and
  `dataset` query parameters

#### Scenario: Session without tenant context, several memberships

- **WHEN** the browser arrives on `/login` (a bookmark, or a retried SSO
  attempt) holding a session cookie and
  `GET /ui/session` reports `tenant: null` with two memberships
- **THEN** the tenant picker lists both, and picking one navigates to the
  redirect target with that tenant and its default dataset

#### Scenario: Session without tenant context, no memberships

- **WHEN** the browser arrives on `/login` holding a session cookie and
  `GET /ui/session` reports no memberships
- **THEN** the card explains the account has no tenant access and offers
  "Sign out"; no navigation happens

### Requirement: Failed redirect-based logins land on the page

The page SHALL accept `?error=<code>` alongside `?redirect=` and render one
message per recognised code in the credential step: `sso_failed` (every
validation failure of a redirect-based login, one generic message) and
`no_membership` (the account holds no tenant membership). Unknown codes
SHALL be ignored. The alert SHALL be shown once: the page rewrites its URL
without `error` after rendering it, so a reload does not repeat it, and
SHALL keep `redirect` so a retry returns to the original target. An
already-authenticated visitor SHALL still be forwarded without seeing the
form.

#### Scenario: Failed SSO reports generically

- **WHEN** the page opens as `/login?error=sso_failed&redirect=%2Flogs`
- **THEN** the credential step shows "Single sign-on failed. Try again, or
  sign in with your email and password.", the URL becomes
  `/login?redirect=%2Flogs`, and the redirect target is preserved for the
  next attempt

#### Scenario: Missing membership is explained

- **WHEN** the page opens as `/login?error=no_membership&redirect=%2Ftraces`
- **THEN** the credential step explains the account has no tenant access
  yet and the URL becomes `/login?redirect=%2Ftraces`

#### Scenario: Unknown error code

- **WHEN** the page opens as `/login?error=whatever`
- **THEN** no alert is shown and the page behaves as plain `/login`

### Requirement: Session introspection without tenant context

The router SHALL expose `GET /ui/session`, authenticated by the browser
session cookie alone, returning the signed-in user, the memberships the
session may enter, and the auto-selected `tenant`/`dataset` using the same
rule as `POST /ui/session` (`null` when a choice is required). It SHALL
answer 401 without a valid session and SHALL be declared in the published
OpenAPI document.

#### Scenario: Sole membership is auto-selected

- **WHEN** a session belongs to a user with exactly one membership
- **THEN** the response names that tenant and its default dataset

#### Scenario: Choice required

- **WHEN** a session belongs to a user with two memberships
- **THEN** `tenant` and `dataset` are `null` and both memberships are listed

#### Scenario: No session

- **WHEN** the request carries no valid session cookie
- **THEN** the response is 401

### Requirement: Login-configuration probe ships ahead of SSO

The router SHALL expose `GET /ui/session/config`, unauthenticated and in
the published OpenAPI document with an empty security requirement, with the
schema `{password_enabled: boolean, oidc: {name: string} | null}`. Until an
OIDC provider is implemented it SHALL answer
`{"password_enabled": true, "oidc": null}`.

#### Scenario: Probe on a password-only instance

- **WHEN** `GET /ui/session/config` is requested
- **THEN** the response is `{"password_enabled": true, "oidc": null}` and
  validates against the published schema

### Requirement: Modal sign-in surfaces reuse the page's card

The mid-session sign-in gate and the OAuth consent screen SHALL render the
same credential and tenant steps as the page inside their modal shell,
with a caller-supplied hint, and SHALL pass their own current location as
the SSO redirect target so a redirect-based login returns to where the
user was.

#### Scenario: Gate after session expiry

- **WHEN** a query fails with 401 on `/traces?range=15m` and SSO is offered
- **THEN** the gate's SSO control links to the start endpoint with
  `redirect=%2Ftraces%3Frange%3D15m` and the gate's hint reads "Your
  session has expired. Sign in to continue."

#### Scenario: Consent resumes after SSO

- **WHEN** the consent screen needs a login and the user chooses SSO
- **THEN** the redirect target is the consent URL including its OAuth
  parameters, and after landing on `/login` the browser returns to the
  consent screen
