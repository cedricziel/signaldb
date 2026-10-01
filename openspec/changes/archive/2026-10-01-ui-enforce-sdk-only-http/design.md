## Context

`src/ui/eslint.config.js` is a flat ESLint config (`typescript-eslint`'s
`tseslint.config(...)`) that already excludes `src/api/gen/**` from linting
entirely (it's `@hey-api/openapi-ts` output, not hand-edited). `client.ts`
itself never calls `fetch()` directly — it only configures the generated
client's interceptors — so no additional per-file exemption is expected
beyond the existing `gen/**` ignore. After `ui-migrate-to-generated-sdk`, a
repo-wide search confirms zero `fetch(` call sites remain outside
`src/api/gen/**`.

See `proposal.md` for motivation.

## Goals / Non-Goals

**Goals:**

- A contributor adding a raw `fetch()` call against a SignalDB endpoint gets
  a fast, local lint failure, not a review comment after the fact.
- Zero pre-existing violations once `ui-migrate-to-generated-sdk` has landed.

**Non-Goals:**

- Restricting `fetch()` usage that has nothing to do with SignalDB (there is
  none today, but the rule is a blunt "no fetch() outside gen/**" rather than
  an allowlist of SignalDB hostnames — simpler, and sufficient given the
  current codebase has no legitimate non-SignalDB fetch use).
- CI wiring beyond what already runs `pnpm --filter signaldb-ui lint` (assumed
  already part of CI, unverified in this design — if it isn't, that's a gap
  larger than this change and worth flagging separately).

## Decisions

**Use `no-restricted-syntax` with an AST selector, not a custom plugin.**
`no-restricted-syntax` targeting `CallExpression[callee.name='fetch']` (and,
if needed, `CallExpression[callee.property.name='fetch']` for
`window.fetch(...)`/`globalThis.fetch(...)` call forms) is a built-in ESLint
core rule — no new dependency, and consistent with the existing
`@typescript-eslint/no-unused-vars` override already in the config.

**No per-file exemption beyond the existing `gen/**` ignore.** Confirmed
`client.ts` doesn't call `fetch()` directly; if a future legitimate
non-SignalDB fetch need arises (e.g. a static asset probe), an inline
`// eslint-disable-next-line no-restricted-syntax` with a comment explaining
why is preferable to a standing file-level exemption, which would be easy to
widen accidentally.

## Risks / Trade-offs

- **AST selector might miss a call form** (e.g. a destructured `const {
fetch: f } = window; f(...)`, or `fetch` reassigned to a variable before
  calling). Mitigation: not a realistic pattern in this codebase today: the
  rule's job is to catch the habitual, obvious case (a new
  `<protocol>Fetch()` helper reappearing), not to be adversarially airtight.
- **False positive if a future dependency's API happens to be named
  `fetch`** on an unrelated object (e.g. `cache.fetch(...)`). Mitigation:
  scope the selector to bare `fetch(...)` and `window.fetch(...)`/
  `globalThis.fetch(...)` specifically, not any `*.fetch(...)` method call.
