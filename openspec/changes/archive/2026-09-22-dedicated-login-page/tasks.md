## 1. Router: probe and session introspection (one PR)

- [x] 1.1 Failing tests in `router::endpoints::session`: `GET /ui/session/config` returns `{"password_enabled": true, "oidc": null}` unauthenticated; the OpenAPI document lists it with an empty security requirement and the `{password_enabled: bool, oidc: {name} | null}` schema
- [x] 1.2 Implement the probe handler with the constant body; add to `paths(...)`
- [x] 1.3 Failing tests: `GET /ui/session` is 401 without a cookie; with a one-membership user it returns the user, that tenant, and its default dataset; with two memberships it returns `tenant: null` and both; an instance admin gets every tenant; an API key or `X-Tenant-ID` header does not substitute for the cookie
- [x] 1.4 Extract the auto-select block of `create_session` into a helper shared with the new handler; implement `GET /ui/session`; add to `paths(...)`
- [x] 1.5 Regenerate `api/signaldb-api.json`, `src/signaldb-sdk`, and `src/ui/src/api/gen`; `cargo fmt`, `cargo clippy --workspace --all-targets --all-features`, `cargo machete --with-metadata`

## 2. UI: card, methods, shared tenant step (one PR)

- [x] 2.1 `api/session.ts`: `loginConfig()` and `currentSession()` on the generated client, wrapping errors into `ApiError`; failing tests in `session.test.ts` for both, including the probe 404/5xx → `ApiError` path
- [x] 2.2 Failing tests for `LoginMethods`: the four probe rows (password only, both, SSO only, `false`/`null` degenerate), probe-failure fallback with notice, SSO `href` carries the encoded redirect, focus lands on the first control
- [x] 2.3 Extract `PasswordForm` and `TenantPicker` from `LoginPanel` (existing `LoginPanel.test.tsx` cases keep passing against the extracted components); implement `LoginCard`, `SsoButton`, `LoginMethods`
- [x] 2.4 `LoginPanel` composes the new pieces inside `Dialog` with a `hint` prop; `LoginGate` passes "Your session has expired. Sign in to continue." and `redirect = pathname + search`; `ConsentView` passes its consent hint and its own URL — failing tests first for the hint and the redirect in both
- [x] 2.5 `pnpm --filter signaldb-ui test`, `typecheck`, `lint`

## 3. UI: the page and the landing flow (one PR)

- [x] 3.1 Failing `LoginRoute` tests: existing cases pass with `currentSession()` stubbed in place of `whoami`; page has an `h1` "Sign in", brand, footer, no `role="dialog"`; SSO landing with one membership navigates, with several shows the picker, with none shows the no-access message; `?error=sso_failed` and `?error=no_membership` render their message once and the URL loses `error` while keeping `redirect`; `?error=unknown` is ignored
- [x] 3.2 Implement the `LoginRoute` state machine and page layout (`LoginPage.css`, tokens only, 600px breakpoint); `useWhoamiGate` gains a `currentSession()`-based variant for `/login` (or `/login` calls it directly — keep `/select-tenant` on `whoami`)
- [x] 3.3 Playwright: extend the `/login` standalone test; add the probe-mocked "Continue with Acme" link assertion
- [x] 3.4 `pnpm --filter signaldb-ui test`, `typecheck`, `lint`, `pnpm ui:build`

## 4. Docs and skills

- [x] 4.1 `docs/users/explore-ui.md` "Signing in": page description, the credential offering, `?error=`, the shared tenant step; refresh the login screenshot
- [x] 4.2 `docs/users/authentication.md`: document `GET /ui/session/config` and `GET /ui/session`
- [x] 4.3 Skills: the `tempo-api` skill carries no session-endpoint list, so the `multi-tenancy` skill (which does) documents both endpoints instead; run the docs-freshness gate after committing

## 5. Surface parity and Definition of Done

- [x] 5.1 Surface parity reviewed: the page is inherently UI; both endpoints are on the HTTP API and in the Rust SDK; the CLI has no browser and is scoped out (as in `oidc-login`)
- [x] 5.2 OpenAPI, Rust SDK, and TypeScript client regenerated in the same PR as the router change; the UI consumes only the generated client for the new calls

## 6. Handshake with oidc-login

- [x] 6.1 Update `openspec/changes/oidc-login/design.md` Decisions 2 and 8 and tasks 4.1–4.3/4.5 per this change's design ("Handshake with oidc-login"): the probe and its schema already exist, so the OpenAPI tasks shrink to returning provider state; the shell's post-SSO tenant resolution reads `GET /ui/session` instead of `whoami`
