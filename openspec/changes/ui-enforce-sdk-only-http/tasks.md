## 1. Lint rule

- [ ] 1.1 Confirm zero `fetch(` call sites remain outside `src/api/gen/**` (should already hold after `ui-migrate-to-generated-sdk`) — treat any survivor as that change's regression, not this one's to fix.
- [ ] 1.2 Add the `no-restricted-syntax` rule (bare `fetch(...)`, `window.fetch(...)`, `globalThis.fetch(...)`) to `src/ui/eslint.config.js`, with a message pointing contributors at the generated client.
- [ ] 1.3 `pnpm --filter signaldb-ui lint` passes with zero violations.

## 2. Regression test

- [ ] 2.1 Add a temporary `fetch(...)` call to a scratch file under `src/ui/src`, confirm `pnpm --filter signaldb-ui lint` fails on it, then remove it — a manual one-time check that the rule actually fires (no permanent test needed for an ESLint config rule).

## 3. Docs

- [ ] 3.1 Note the rule in `frontend-instrumentation` or the relevant UI-contributing doc, so new contributors know why `fetch()` is off-limits and where the generated client lives.
