## Purpose

Lets humans sign in to SignalDB with their organisation's OIDC identity
provider instead of a SignalDB password: relying-party flow, token validation,
just-in-time provisioning, group-to-role mapping, and coexistence with
password login.

## ADDED Requirements

### Requirement: Single-provider OIDC configuration with discovery

The system SHALL support one OIDC provider per instance, configured under
`[auth.oidc]` with at least `issuer_url`, `client_id`, and `client_secret`.
Provider endpoints SHALL be resolved through OIDC discovery
(`.well-known/openid-configuration`) — never configured endpoint-by-endpoint.
An invalid `[auth.oidc]` section (malformed `issuer_url`, missing
`client_id`/`client_secret`, `disable_password_login` without a provider,
unparseable mapping rules) SHALL fail startup with a message naming the
problem. A discovery document that cannot be fetched or fails validation at
startup SHALL NOT stop the instance: the instance SHALL start with SSO
unavailable, log an error naming the issuer, retry discovery in the
background with backoff, and offer SSO as soon as discovery succeeds — so
password login, API keys, and the bootstrap path stay reachable across an
IdP outage or restart. When the section is absent, no OIDC surface is
exposed and nothing else changes.

#### Scenario: Discovery resolves the provider

- **WHEN** the instance starts with a valid `[auth.oidc]` issuer
- **THEN** the authorization, token, and JWKS endpoints are taken from the
  issuer's discovery document and SSO login is offered

#### Scenario: Invalid configuration fails hard

- **WHEN** the instance starts with `[auth.oidc]` present but invalid — for
  example `disable_password_login = true` with no `issuer_url`
- **THEN** startup fails with an error naming the offending setting

#### Scenario: Unreachable issuer degrades instead of stopping

- **WHEN** the instance starts with a valid `[auth.oidc]` and the issuer's
  discovery document cannot be fetched or fails validation
- **THEN** the instance starts, the login-configuration probe reports no SSO,
  the SSO start endpoint answers 503 naming the issuer, an error naming the
  issuer is logged, and password login, API keys, and `admin_api_key` work
  as before

#### Scenario: Discovery recovers without a restart

- **WHEN** an instance started with an unreachable issuer and the issuer
  later becomes reachable
- **THEN** a subsequent background retry succeeds and SSO is offered without
  restarting the instance

#### Scenario: Absent config exposes nothing

- **WHEN** the instance starts without `[auth.oidc]`
- **THEN** the OIDC endpoints return 404 and the login page shows no SSO
  option

### Requirement: Authorization-code login flow with PKCE

Signing in via SSO SHALL use the OIDC authorization-code flow with PKCE,
`state`, and `nonce`. The callback SHALL exchange the code, validate the ID
token (signature against the provider's JWKS, issuer, audience, expiry,
nonce), and — on success — issue the same server-side session and cookie as
password login, with the same lifetime and revocation semantics. A callback
whose state does not match a pending login attempt, or whose ID token fails
any validation, SHALL be rejected without creating a session and without
revealing which check failed.

#### Scenario: Successful SSO login issues a standard session

- **WHEN** a user completes the provider's login and consent and returns to
  the callback with a valid code
- **THEN** they hold a `signaldb_session` cookie indistinguishable in
  behaviour from one issued by password login, and `whoami` names them

#### Scenario: Tampered callback is rejected

- **WHEN** the callback is invoked with a state that matches no pending
  attempt, or with a code yielding an ID token whose nonce or signature does
  not verify
- **THEN** no session is created and the user is returned to the login page
  with a generic failure message

#### Scenario: MCP OAuth consent rides the SSO session

- **WHEN** an MCP client starts the OAuth authorization flow and the user
  signs in via SSO when prompted
- **THEN** the consent and tenant-selection flow proceeds exactly as it does
  for a password-authenticated session

### Requirement: Just-in-time provisioning with an allowlist

A first SSO login SHALL resolve the user by `(issuer, subject)`; failing
that, by verified email matching an existing user, in which case the OIDC
identity is linked to that user. When neither matches, the system SHALL
create the user just-in-time — unless `[auth.oidc]` configures an allowlist
(email domains or a claim predicate), in which case a non-matching identity
SHALL be refused without creating a user. A user created or linked via OIDC
MAY have no password; such a user SHALL NOT be able to log in with a password
until one is set through an authorised path — a password login attempt
against a user with no password SHALL be refused with the same generic
failure as a wrong password, without invoking the password verifier and
without creating a session. Disabled users SHALL be refused at SSO login
exactly as at password login.

#### Scenario: First login creates the user

- **WHEN** an identity allowed by the allowlist signs in for the first time
- **THEN** a user exists afterwards carrying the OIDC subject and the
  provider-asserted email and display name, with no password set

#### Scenario: Existing password user is linked, not duplicated

- **WHEN** an SSO login's verified email equals an existing user's email and
  no user carries that OIDC subject yet
- **THEN** the OIDC identity is attached to the existing user and both login
  methods reach the same account

#### Scenario: Allowlist refuses an outside identity

- **WHEN** the allowlist names `example.com` and the provider asserts
  `mallory@evil.test`
- **THEN** login is refused, no user row is created, and the refusal does not
  disclose whether the address was known

#### Scenario: SSO-only user cannot use the password form

- **WHEN** password login is enabled and a user whose `password_hash` is null
  submits the password form with any password
- **THEN** the response is the same generic "invalid email or password"
  failure, the verifier is not invoked, and no session is created

#### Scenario: Disabled user cannot enter via SSO

- **WHEN** a user with a non-null `disabled_at` completes the provider flow
- **THEN** no session is issued

### Requirement: Group-claim mapping to memberships and roles

When `[auth.oidc]` configures a group claim and mapping rules (IdP group →
tenant + role), the system SHALL apply the mappings at every SSO login:
memberships granted by mapping are created or updated to the mapped role, and
a mapping-granted membership whose group disappears from the token SHALL be
removed at the next login. Memberships granted locally (by an admin, not by
mapping) SHALL NOT be modified or removed by mapping. Without mapping
configuration, SSO logins SHALL NOT alter memberships, and membership
management stays exactly as it is today. Mapping SHALL NOT grant or revoke
the instance-admin flag.

#### Scenario: Group grants a membership at login

- **WHEN** a mapping rule assigns group `observability-admins` to tenant
  `acme` as `admin` and a user's token carries that group
- **THEN** after login the user holds an `admin` membership in `acme`

#### Scenario: Local and mapped memberships coexist

- **WHEN** an admin has locally granted a user `viewer` in tenant `acme` and a
  mapping rule grants the same user `admin` in `acme`
- **THEN** both rows exist with their own source, the user's effective role in
  `acme` is `admin`, removing the local membership leaves the mapped one, and
  losing the group leaves the local one

#### Scenario: Lost group revokes only what mapping granted

- **WHEN** a user's token no longer carries a group that previously granted a
  mapped membership, while an admin has separately granted them a membership
  in another tenant
- **THEN** the mapped membership is removed at login and the locally granted
  membership is untouched

#### Scenario: No mapping, no membership changes

- **WHEN** no mapping rules are configured and a user with existing
  memberships signs in via SSO
- **THEN** their memberships after login are exactly their memberships before

### Requirement: Password login coexists and can be switched off

Password login SHALL remain available alongside OIDC by default. When
`[auth.oidc]` is configured, an operator MAY disable password login via
config; while disabled, password authentication SHALL be refused for every
user and the login page SHALL offer only SSO. Disabling password login SHALL
NOT disable API-key authentication, the `admin_api_key`, or the CLI/config
bootstrap path for creating an instance admin — an IdP outage never locks
operators out of break-glass access.

#### Scenario: Both doors open by default

- **WHEN** OIDC is configured and password login is not disabled
- **THEN** the login page offers both, and each issues an equivalent session

#### Scenario: Passwords disabled

- **WHEN** the operator disables password login and a user submits the
  password form directly to the session endpoint
- **THEN** the request is refused with a message that password login is
  disabled, and no session is created

#### Scenario: Break-glass survives the IdP

- **WHEN** password login is disabled and the OIDC provider is unreachable
- **THEN** API keys and `admin_api_key` continue to authenticate, and the
  documented bootstrap path can still mint an instance-admin credential

### Requirement: The SSO surface is part of the published contract

The login-configuration probe (which authentication methods this instance
offers, and the SSO display name), the SSO start endpoint, and the callback
endpoint SHALL be declared in the published OpenAPI document, and all three
SHALL be explicitly unauthenticated (empty security requirement). The probe
response SHALL have one schema in every state: `password_enabled` (boolean,
always present) and `oidc` (nullable object; `null` when OIDC is not
configured or discovery has not succeeded, otherwise `{name}`). Generated
clients SHALL be regenerated from the published schema before the UI
consumes it. The UI SHALL derive the login page's offering from the probe
via the generated client — it SHALL NOT infer SSO availability by probing
endpoints or from hardcoded configuration.

#### Scenario: Login page follows the probe

- **WHEN** the UI loads on an instance with OIDC configured and password
  login disabled
- **THEN** the login page shows only the SSO entry, sourced from the
  login-configuration probe through the generated client

#### Scenario: Probe without OIDC

- **WHEN** the probe is requested on an instance without `[auth.oidc]`
- **THEN** the response is `{"password_enabled": true, "oidc": null}` and
  validates against the published schema

#### Scenario: Contract documents the endpoints

- **WHEN** the OpenAPI document is inspected
- **THEN** the login-configuration probe, SSO start, and callback endpoints
  appear with their schemas, each with an empty security requirement
