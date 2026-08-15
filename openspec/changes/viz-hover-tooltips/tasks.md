## 1. Shared primitive

- [x] 1.1 Failing tests for `components/VizTooltip.tsx`: renders title/rows/
      footer with swatches, `role="tooltip"`, flips left past the host midline
      and up near the bottom, `pointer-events: none` class
- [x] 1.2 Implement `VizTooltip` + `useVizPointer(hostRef)` + `lib/vizFormat.ts`
      (`formatTimestamp`, `formatValue`, `formatRange`; move `compactCount`
      from `SignalHistogram`)
- [x] 1.3 Failing test + refactor: `explore/SignalHistogram` renders its
      tooltip through `VizTooltip` with unchanged content

## 2. Panels

- [ ] 2.1 Failing test + implement: `metrics/MetricsChart` uPlot cursor
      plugin → `VizTooltip` (timestamp, every series with swatch/value/unit,
      `–` for gaps); export pure `rowsForCursorIndex`
- [ ] 2.2 Failing test + implement: `traces/TraceVolumeAreaChart` bucket
      tooltip (time range, per-series values, total); focusable bars
- [ ] 2.3 Failing test + implement: `traces/TraceVolumeHeatmap` cell tooltip
      (time bucket, value range, count, share of column); focusable cells
- [ ] 2.4 Failing test + implement: `logs/Histogram` bucket tooltip; focusable
      bars
- [ ] 2.5 Failing test + implement: `errors/ErrorSparkline` point tooltip
- [ ] 2.6 Failing test + implement: `catalog/DependencyBreakdown` bar tooltip
      (category, value, share)
- [ ] 2.7 Failing test + implement: `query/QueryView` `series` envelope →
      `MetricsChart` (adapter to `PromSeries`) so it inherits the tooltip

## 3. Docs and guidance

- [ ] 3.1 `docs/users/explore-ui.md`: one paragraph on chart tooltips (hover,
      keyboard on bars/cells, what they show)
- [ ] 3.2 Guidance for future panels: `.claude/skills/frontend-instrumentation`
      or the explore-ui skill — "every viz panel uses `VizTooltip`"; run
      `scripts/check-doc-freshness.sh` and settle any flagged doc
- [ ] 3.3 UI suite, tsc, eslint clean; verify a chart in the running app
