## 1. Query IR flame graph data layer

- [x] 1.1 Write failing tests for a flamegraph-fetch adapter over Query IR
      (`from: "profiles"`, `result: "flamegraph"`), covering service/sample-type
      selection, attribute matchers, and truncation reporting.
- [x] 1.2 Implement `api/profilesIr.ts`: `fetchFlamegraph` and
      `fetchFlamegraphById`, over the generated Query IR client — no
      hand-written `fetch`.
- [x] 1.3 Trim `api/pyroscope.ts` to discovery-only calls (profile types,
      services, label names/values); remove `render`/`render_diff` usage from
      the UI's own query path.

## 2. Single-range and compare views

- [x] 2.1 Write failing component tests for `ProfilesView`: single-range
      render, compare render (two independent ranges), and single-profile-by-id
      render.
- [x] 2.2 Implement `SingleRangeView`, `CompareView`, `SingleProfileView`, and
      the shared `useFlamegraphRender` hook in `ProfilesView.tsx`.

## 3. Trace-to-profile navigation

- [x] 3.1 Write a failing test that a span detail panel with a matching
      profile summary offers a working link into the Profiles tab at that
      profile id.
- [x] 3.2 Request `include_profiles=true` on the trace fetch and surface
      `ProfileSummaryView` on `TempoTrace` (`api/tempo.ts`); wire the link in
      the trace span detail panel.

## 4. Attribute matcher filtering

- [x] 4.1 Write failing tests for attribute-matcher query-form state
      (add/remove matchers, matchers included in the Query IR request).
- [x] 4.2 Implement the matcher dropdowns in `ProfilesView.tsx`.

## 5. Readability: frame collapsing and top-functions table

- [x] 5.1 Write failing tests for `collapseSmallFrames` (folds sub-threshold
      frames and subtrees into a synthetic "(other)" node) and
      `topFunctionsBySelf` (aggregates by frame name, sorted by self time) in
      `lib/flamebearer.ts`.
- [x] 5.2 Implement both functions; wire a collapse-threshold control
      (`COLLAPSE_PRESETS`) and a flame/top-functions view toggle into
      `FlameGraph.tsx`.
- [x] 5.3 Write failing tests for unit-aware `formatTicks` (byte units render
      as KiB/MiB/GiB); implement byte-unit humanization.
- [x] 5.4 Fix top-functions table horizontal overflow.

## 6. Symbol name simplification and hover tooltip

- [x] 6.1 Write failing tests for `simplifyFrameName` against real
      hive-observed Rust symbols: `<Type as Trait>::method::<Args>` forms,
      `{closure#N}`/`{shim:vtable#N}` compiler noise, and idempotency on
      already-short paths.
- [x] 6.2 Implement `simplifyFrameName` in `lib/flamebearer.ts`.
- [x] 6.3 Apply the simplified name to bar labels and the top-functions table;
      keep the full name on `aria-label` and a new CSS-only hover/focus
      tooltip showing full name, self, total, and percentages.

## 7. Verification and docs

- [x] 7.1 `pnpm run typecheck && pnpm run lint && pnpm vitest run` (src/ui).
- [x] 7.2 Live-verify single-range, compare, trace-link, collapse threshold,
      top-functions view, and symbol simplification against a real deployment.
- [x] 7.3 Document profiles compare, attribute filtering, trace links, the
      collapse threshold, the top-functions view, and symbol simplification in
      `docs/users/explore-ui.md`.

Surface parity: this is a UI-only change consuming Query IR's existing
`profiles` source and `flamegraph` envelope — no OpenAPI, Rust SDK, or MCP
surface changes are needed (those already exist).
