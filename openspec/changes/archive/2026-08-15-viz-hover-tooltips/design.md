## Context

See proposal.md — Why. Panels today (`src/ui/src/features/...`):

| Panel                           | Renderer                                                 | Tooltip today            |
| ------------------------------- | -------------------------------------------------------- | ------------------------ |
| `explore/SignalHistogram`       | SVG bars, own JS tooltip (`SignalHistogram.tsx:117–330`) | yes (bespoke)            |
| `profiles/FlameGraph`           | DOM frames, CSS-shown `FrameTooltip`                     | yes (bespoke)            |
| `metrics/MetricsChart`          | uPlot (canvas)                                           | none (uPlot cursor only) |
| `traces/TraceVolumeAreaChart`   | SVG                                                      | none                     |
| `traces/TraceVolumeHeatmap`     | SVG cells                                                | none                     |
| `logs/Histogram`                | SVG bars                                                 | none                     |
| `errors/ErrorSparkline`         | SVG                                                      | none                     |
| `catalog/DependencyBreakdown`   | DOM bars                                                 | none                     |
| `query/QueryView` `SeriesChart` | text ("N points")                                        | n/a                      |

The signal histogram already solved pointer tracking, flipping past the
midline, `pointer-events: none`, and a per-series row layout; its
`compactCount`/formatting helpers live in `explore/SignalHistogram.tsx` and
`explore/scale.ts`. Value formatting for metrics lives in `lib/promSeries.ts`
(`seriesName`, `seriesColorVar`).

## Goals / Non-Goals

**Goals:** one primitive; every panel wired; content rules from the spec;
tests per panel that assert tooltip content for a hovered point.

**Non-Goals:** crosshair syncing across panels, click-to-pin tooltips, touch
long-press behaviour, changing what the panels draw.

## Decisions

### D1 — `components/VizTooltip.tsx`: a positioned container + row model

```ts
interface VizTooltipRow {
  swatch?: string /* CSS color */;
  label: string;
  value: string;
  muted?: boolean;
}
interface VizTooltipProps {
  anchor: { x: number; y: number } /* px within host */;
  host: DOMRect | { width; height };
  title: string;
  rows: VizTooltipRow[];
  footer?: string;
  id?: string;
}
```

Renders `<div role="tooltip" class="viz-tip">` absolutely inside a
`position: relative` host, offset 12px from the pointer, flipping left past
`host.width / 2` and up past `host.height - tipHeight` (measured via a ref
after first paint; initial render uses an estimate). `pointer-events: none`.
Extracted from `SignalHistogram`'s implementation, which then consumes it.
A `useVizPointer(hostRef)` hook returns `{ x, y, inside }` from
`onPointerMove/Leave` for SVG/DOM panels.

### D2 — Per-panel data resolution stays in the panel

Each panel maps pointer → datum itself (bar index from x, heatmap cell from
x/y, nearest timestamp for lines) and hands rows to `VizTooltip`; the
primitive knows nothing about data shapes. Formatting helpers are shared:
`formatTimestamp(ms, resolutionMs)`, `formatValue(v, unit)`, `formatRange(lo,
hi, unit)` in `lib/vizFormat.ts` (moving `compactCount` there).

### D3 — uPlot: a cursor plugin, not uPlot's legend

`MetricsChart` adds a uPlot plugin that reads `u.cursor.idx` on `setCursor`,
collects `[label, value, color]` per series from `u.data` and `u.series`, and
sets React state that renders `VizTooltip` in the chart's host div. uPlot's
built-in legend stays hidden. `SeriesChart` in `QueryView` becomes a thin
adapter from `QueryIrResponse.series` to `PromSeries` and reuses
`MetricsChart`.

### D4 — Keyboard/AT

SVG bars, heatmap cells, and breakdown bars get `tabIndex={0}` and
`aria-describedby` pointing at the tooltip id while focused; focus sets the
same "hovered datum" state as pointer hover. Line charts (uPlot canvas) are
pointer-only (no focusable marks) — accepted, called out in docs.

### D5 — Tests

jsdom cannot lay out; tests assert _content_: fire `pointerMove` at
computed coordinates (bars have deterministic geometry from props) or `focus`
a bar/cell, then read `role="tooltip"` text. For uPlot, stub the plugin hook
input (`setCursor` with a fixed `idx`) via a small exported
`rowsForCursorIndex(u, idx)` pure function.

## Risks / Trade-offs

- [uPlot cursor idx is per-x-aligned data; series with gaps show `–`] →
  render muted `–` rows rather than dropping the series.
- [Tooltip measurement causes a one-frame misplacement on first show] →
  estimate size for the first render; imperceptible.
- [Heatmap cells are small; pointer jitter flickers between cells] → no
  debounce; the primitive re-renders cheaply, and cells are ≥ 6px.

## Open Questions

- Whether to expose a `pin on click` later; not needed for the rule.
