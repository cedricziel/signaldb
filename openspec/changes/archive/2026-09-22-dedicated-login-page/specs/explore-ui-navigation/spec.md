## ADDED Requirements

### Requirement: Login screen is a standalone route

`/login` SHALL render the sign-in page on its own, outside the explore
shell (no top bar, no signal tabs) and without a modal dialog. It SHALL
accept an optional `?redirect=<path>` honoured only for a same-app relative
path (anything else falls back to `/logs`), and an optional `?error=<code>`
reported once as a generic alert. An already-authenticated visitor SHALL be
forwarded to the target without seeing the form. Signing out SHALL land
here.

#### Scenario: Login page renders standalone

- **WHEN** an unauthenticated visitor opens `/login`
- **THEN** the sign-in page renders without the explore shell's top bar or
  signal tabs and without a dialog

#### Scenario: Redirect target is honoured

- **WHEN** a visitor signs in from `/login?redirect=%2Ftraces%3Frange%3D15m`
- **THEN** the browser lands on `/traces?range=15m` with the resolved
  tenant and dataset appended

#### Scenario: Unsafe redirect falls back

- **WHEN** `redirect` is `//evil.com`, an absolute URL, or `/\evil.com`
- **THEN** the browser lands on `/logs`

#### Scenario: Already authenticated

- **WHEN** a visitor holding a valid session opens `/login?redirect=%2Ftraces`
- **THEN** they are forwarded to `/traces` without the form being shown
