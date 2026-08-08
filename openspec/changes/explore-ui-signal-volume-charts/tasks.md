## 1. Scale and bar-height maths

- [x] 1.1 Write failing tests for a `scale.ts` module: linear and `log1p`
      mappings, a bar-height helper that floors once per bar, and a segment
      splitter whose outputs sum to the bar height. Cover the measured
      regression (525 vs 10,240 against a 373,329 maximum must differ).
- [x] 1.2 Implement `src/ui/src/features/explore/scale.ts` to make 1.1 pass.

## 2. Time-axis formatting

- [x] 2.1 Write failing tests in `src/ui/src/lib/time.test.ts` for an axis
      formatter that includes the date when a window spans more than one
      calendar day and stays time-only otherwise.
- [x] 2.2 Implement the formatter in `src/ui/src/lib/time.ts`.

## 3. Shared `SignalHistogram` component

- [x] 3.1 Write failing tests for `SignalHistogram`: generic
      `{labels, points}` series in milliseconds, caller-supplied series order
      and colours, epoch-grid zero-padding across the full window, a vertical
      axis carrying the maximum, and an empty state.
- [x] 3.2 Implement `src/ui/src/features/explore/SignalHistogram.tsx` to make
      3.1 pass, consuming `scale.ts` and the axis formatter.
- [x] 3.3 Write failing tests for interaction: a full-plot-height hit zone per
      bucket, a tooltip listing every series plus the total, hover highlight,
      and keyboard focus reaching the same detail.
- [x] 3.4 Implement the interaction layer to make 3.3 pass; remove the native
      `title` attribute.
- [x] 3.5 Add axis, tooltip, hover-highlight, and focus-ring styles to
      `src/ui/src/features/explore/explore.css`.

## 4. Scale toggle in explore state

- [x] 4.1 Write failing tests in `src/ui/src/lib/urlState.test.ts` for a
      `scale` param that round-trips, defaults to linear when absent, and
      rejects unknown values.
- [x] 4.2 Implement the `scale` field in `src/ui/src/lib/urlState.ts`.
- [x] 4.3 Write a failing test that the chart renders a scale control and that
      toggling it updates explore state; implement the control.

## 5. Logs tab on the shared component

- [x] 5.1 Update `src/ui/src/features/logs/Histogram.test.tsx` for the new
      contract, keeping the `bucketize`/`padBuckets`/`normalizeLevel`
      coverage that still applies.
- [x] 5.2 Reduce `src/ui/src/features/logs/Histogram.tsx` to a logs adapter
      over `SignalHistogram` (level ordering, level colours, Loki
      seconds→milliseconds normalisation).
- [x] 5.3 Move the row-count/limit badge out of the chart container in
      `src/ui/src/features/logs/LogsView.tsx`; update `LogsView.test.tsx`.

## 6. Traces volume chart

- [x] 6.1 Write failing tests for a traces-volume adapter: builds the Query IR
      `series` document (`from: "traces"`, `aggregate` with `by:
    ["status.code"]` and `step`), normalises `[epoch_ns, value]` points to
      milliseconds, and maps OTLP status values to `ok`/`error`/`unset`.
- [x] 6.2 Implement the adapter over the generated `queryIr` client function —
      no hand-written `fetch`.
- [x] 6.3 Write a failing test that `TracesView` renders the chart above the
      group table and that it does not depend on `state.limit`; implement the
      wiring in `src/ui/src/features/traces/TracesView.tsx`.

## 7. Verification

- [x] 7.1 `pnpm --filter ... test` (or the repo's UI test script) green; run
      `pnpm lint` and `pnpm format` per the UI toolchain. Use pnpm, never npm.
- [x] 7.2 Regression check against a deployment with a spiky window: confirm
      the body of the distribution is legible, the tooltip resolves on a
      minimum-height bar, and the 24h axis labels differ at each end.
- [x] 7.3 Confirm no Rust crate changed and no OpenAPI/SDK regeneration was
      needed.

## 8. Documentation

- [x] 8.1 Update the explore-UI user documentation for the scale control and
      the chart tooltip, routed via the `docs` skill.

## 9. Refinements from hands-on review

- [x] 9.1 Anchor the tooltip to the pointer, flipping side near the edge, with
      the focused column as the keyboard anchor.
- [x] 9.2 Give every rendered number its unit; replace `Intl` compact notation
      with a locale-stable abbreviation.
- [x] 9.3 Keep a present series visible in a stacked bar without inflating the
      bar's total height.
- [x] 9.4 Render structured metadata in the log detail panel, marked per-line
      and without stream-selector filter actions.
- [x] 9.5 Add a user-controlled bucket width, bounded per window and persisted
      in URL state.
