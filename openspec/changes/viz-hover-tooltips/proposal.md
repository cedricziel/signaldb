## Why

The Explore UI has grown seven data visualizations, and only two of them (the
signal histogram, the flame graph) tell you what you are pointing at. The
metrics chart, the trace-volume area chart and heatmap, the logs histogram,
the error sparkline, and the catalog dependency breakdown render points you
cannot interrogate — the value has to be guessed from an axis. Reading a
number off a chart is the most basic thing an observability UI must do; making
it a rule (every visualization panel shows a rich on-hover tooltip over its
data points) closes the gap once and stops it reopening with the next panel.

## What Changes

- **Rule**: every visualization panel in the Explore UI SHALL show a rich
  tooltip on hover/focus over its data points: the exact x (timestamp / bucket
  / category), every series value at that x with its label, unit, and color
  swatch, and — where the panel aggregates — the underlying count/total. The
  tooltip follows the pointer, flips to stay inside the panel, never blocks the
  pointed-at data, and is reachable by keyboard where the panel is focusable.
- One shared tooltip primitive (`VizTooltip`) with a common look, used by every
  panel; new panels must use it (documented in the frontend guidance).
- Retrofit the panels that lack one: metrics chart (uPlot cursor + tooltip
  plugin), trace-volume area chart, trace-volume heatmap (cell tooltip:
  bucket range, count, share), logs histogram, error sparkline, catalog
  dependency breakdown; align the signal histogram's existing tooltip to the
  primitive.
- Query IR `series` results get a chart (they currently list "N points") using
  the metrics chart, so they inherit the tooltip.

## Capabilities

### New Capabilities

- `explore-ui-viz-tooltips`: on-hover data-point tooltips on every
  visualization panel — content, positioning, keyboard access, and the shared
  primitive.

### Modified Capabilities

_None._

## Impact

- **ui** only: `src/ui/src/components/VizTooltip.tsx` (new), the seven panel
  components under `features/{metrics,traces,logs,errors,catalog,explore,
query}`, their tests, `docs/users/explore-ui.md`, and the
  `frontend-instrumentation`/`explore-ui` guidance for future panels. No API,
  backend, or spec-driven contract changes.
