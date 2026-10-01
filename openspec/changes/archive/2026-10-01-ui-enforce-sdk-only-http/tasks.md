## 1. Lint rule

- [x] 1.1 Confirm zero `fetch(` call sites remain outside `src/api/gen/**` — five survive, none of them a SignalDB API call site: the generated client's transport (`retryingFetch` in `src/api/http.ts`, two calls), the default transport `withProxyLoginRecovery` wraps (`src/lib/proxyLoginRecovery.ts`), and the service worker's app-shell and navigation fetches (`src/sw.ts`, `src/sw/navigation.ts`). Each disables the rule inline with a reason, per design.md. `session.ts`'s `createSession`/`deleteSession`/`whoami` bypass the generated client without calling `fetch` by name; that gap belongs to `ui-migrate-to-generated-sdk` (descoped there to `ui-session-endpoints-in-openapi`).
- [x] 1.2 Add the `no-restricted-syntax` rule (bare `fetch(...)`, `window.fetch(...)`, `globalThis.fetch(...)`) to `src/ui/eslint.config.js`, with a message pointing contributors at the generated client.
- [x] 1.3 `pnpm --filter ./src/ui lint` passes with zero violations.

## 2. Regression test

- [x] 2.1 Instead of a one-time manual check, `src/ui/src/api/sdkOnlyLint.test.ts` lints snippets through ESLint's API and asserts the rule fires on all three call forms and ignores an unrelated `.fetch()` method — written first and seen failing before the rule existed.

## 3. Docs

- [x] 3.1 Noted in `docs/architecture/openapi-codegen.md` ("Adding or changing an endpoint", step 4), next to the existing rule that the UI consumes endpoints through the generated client.
