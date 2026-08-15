## Purpose

Every visualization panel in the Explore UI lets the user read the exact data
under the pointer: a rich hover tooltip over data points, with one consistent
look, positioning, and keyboard behaviour across panels.

## ADDED Requirements

### Requirement: Every visualization panel has a data-point tooltip

Every panel that draws data (time-series charts, histograms, area charts,
heatmaps, sparklines, breakdown bars, flame graphs, and any panel added later)
SHALL show a tooltip while the pointer is over a data point, bucket, cell, or
bar. The tooltip SHALL name the x position exactly (an absolute timestamp with
the panel's resolution, a bucket range, or a category), and for that x SHALL
list every series present with its label, formatted value and unit, and a
color swatch matching the drawn series; where the panel aggregates, it SHALL
also show the aggregate (total, count, share). A panel with no data under the
pointer SHALL show no tooltip.

#### Scenario: Metrics chart hover lists every series at the pointed timestamp

- **WHEN** a user hovers over the metrics chart at a timestamp where three
  series have samples
- **THEN** a tooltip shows that timestamp and three rows, each with the
  series' label, color swatch, and value with the metric's unit

#### Scenario: Heatmap cell hover shows range, count, and share

- **WHEN** a user hovers over a cell of the trace-volume heatmap
- **THEN** the tooltip shows the cell's time bucket, its value bucket range
  (e.g. `1ms – 2ms`), the count in the cell, and the cell's share of the
  column total

#### Scenario: Histogram bucket hover shows the bucket and its total

- **WHEN** a user hovers over a bar of the logs histogram or the trace-volume
  area chart
- **THEN** the tooltip shows the bucket's time range, the per-series values
  with swatches, and the bucket total

#### Scenario: Empty region shows nothing

- **WHEN** the pointer is over the panel but not over any drawn data
- **THEN** no tooltip is shown

### Requirement: Tooltips share one primitive and one look

All panel tooltips SHALL be rendered through the shared `VizTooltip`
primitive so they share typography, spacing, colors (light and dark theme),
swatch style, and value formatting. A visualization that draws its own
tooltip markup instead of the primitive is a defect.

#### Scenario: Two panels render tooltips identically

- **WHEN** the metrics chart and the trace-volume area chart both show a
  tooltip
- **THEN** both use the same tooltip container, row layout, and swatch style

### Requirement: Tooltip positioning never hides the data

The tooltip SHALL follow the pointer, be offset so it does not cover the
pointed-at data, flip horizontally past the panel's midline (and vertically
near the bottom edge) so it stays inside the panel, and SHALL not intercept
pointer events.

#### Scenario: Tooltip flips near the right edge

- **WHEN** the pointer is in the right half of a chart
- **THEN** the tooltip renders to the pointer's left, fully inside the panel

#### Scenario: Tooltip does not steal the pointer

- **WHEN** the tooltip is displayed and the pointer keeps moving across data
- **THEN** the tooltip updates continuously without flicker caused by the
  pointer entering the tooltip itself

### Requirement: Keyboard access where the panel is focusable

Where a panel or its data marks are keyboard-focusable (bars, cells, frames,
series legend entries), focusing a data mark SHALL show the same tooltip
content, and it SHALL be exposed to assistive technology (`role="tooltip"`
referenced via `aria-describedby`).

#### Scenario: Focused bar shows its tooltip

- **WHEN** a user tabs to a histogram bar
- **THEN** the bar's tooltip is shown and announced

### Requirement: Query IR series results are charted

The native query view SHALL render `series` results as a time-series chart
(the same component as the metrics chart) rather than a row count, so they
carry the same tooltip.

#### Scenario: Series result renders a chart with tooltips

- **WHEN** a Query IR request returns a `series` envelope with two series
- **THEN** the query view shows a chart with two colored series and hovering
  it lists both values at the pointed timestamp
