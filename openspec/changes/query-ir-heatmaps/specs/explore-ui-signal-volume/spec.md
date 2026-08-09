## ADDED Requirements

### Requirement: Traces volume controls include a latency heatmap

The traces tab SHALL let a user switch its top-level volume visualization among
status-stacked histogram, area, and latency heatmap views for the selected
window. The latency heatmap SHALL display time on the x-axis, duration buckets
on the y-axis, and span count as cell intensity, using the native Query IR
heatmap result over the full selected window.

#### Scenario: Selecting the latency heatmap preserves the query window

- **WHEN** a user selects Heatmap in the traces volume controls
- **THEN** the UI submits a bounded Query IR heatmap for the current tenant,
  dataset, filters, range, and time step
- **AND** the displayed x-axis identifies the selected time window
- **AND** the displayed y-axis identifies duration buckets

#### Scenario: Heatmap cell intensity represents span count

- **WHEN** two time-duration cells contain different numbers of spans
- **THEN** their rendered intensities differ according to their counts
- **AND** an empty cell is visually distinct from a populated low-count cell

#### Scenario: Trace list limit does not bound the heatmap

- **WHEN** the trace list reaches its configured row limit
- **THEN** changing that limit does not change the latency heatmap cells for the
  same range, filters, and time step

#### Scenario: Heatmap values are accessible

- **WHEN** a keyboard or assistive-technology user interrogates a heatmap cell
- **THEN** its time bucket, duration bucket, and span count are available without
  relying on color alone
