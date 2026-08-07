## Why

`ui-migrate-to-generated-sdk` gets the UI to zero hand-written `fetch()`
calls against SignalDB endpoints, but nothing stops the next feature from
reaching for `fetch()` again — there's no lint rule, import restriction, or
CI check today, and the pattern the UI is moving away from (a small
per-protocol `<name>Fetch()` helper) is exactly the kind of thing a
contributor unfamiliar with the generated client would reach for by
habit. This change makes the `ui-generated-client-only` requirement
self-enforcing instead of relying on review vigilance.

## What Changes

- Add an ESLint rule (`no-restricted-syntax` matching `CallExpression[callee.name="fetch"]`, or equivalent) to `src/ui/eslint.config.js` that fails on any direct `fetch()` call in application code.
- Scope the rule to everything except `src/api/gen/**`, which is already excluded from linting entirely (it's `@hey-api/openapi-ts` output).
- No other exemption is expected: after `ui-migrate-to-generated-sdk`, no file outside `src/api/gen/**` calls `fetch()` directly, so the rule should pass with zero pre-existing violations.

Not breaking: this is a new lint rule with an expected-clean baseline, not a
behavior change.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `ui-generated-client-only`: adds an automated-enforcement requirement — an ESLint check SHALL fail the build when application code calls `fetch()` outside the generated client, so the exclusivity requirement doesn't rely on manual review alone.

## Impact

- **src/ui**: `eslint.config.js`.
- Depends on `ui-migrate-to-generated-sdk` landing first — enabling the rule
  before that change would fail lint against the five files it hasn't
  migrated yet.
