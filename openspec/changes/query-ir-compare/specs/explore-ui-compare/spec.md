## Purpose

Lets an operator in the Explore UI select an anomalous region or group and see,
ranked, which attributes distinguish it from the rest — then turn any
distinguishing value into a filter or grouping on the active query without
retyping it.

## ADDED Requirements

### Requirement: A comparison can be started from a selection

The traces tab SHALL let the user draw a box on the latency heatmap and compare
the spans inside it to the rest of the current query; grouped tables on the
traces and logs tabs SHALL offer "compare to the rest" on each group row. The
comparison SHALL inherit the tab's active filters and time range as its scope,
so the baseline is the currently displayed data minus the selection.

#### Scenario: Heatmap box opens a comparison

- **WHEN** the user drags a rectangle over the latency heatmap and confirms
- **THEN** a comparison panel opens whose selection is the box's duration and
  time bounds and whose scope is the tab's active filters and range
- **AND** the panel states the selection and baseline record counts

#### Scenario: Group row opens a comparison

- **WHEN** the user picks "compare to the rest" on a grouped row for
  `service.name = payments`
- **THEN** a comparison panel opens whose selection is `service.name =
payments` and whose baseline is every other record the table counts

### Requirement: Fields render ranked, most distinguishing first

The panel SHALL list compared fields in the server's ranked order, each with its
score, per-cohort participation, and a chart: paired bars per value for
dimensions and overlaid histograms for measures, both showing the two cohorts
in the same two colours throughout the UI. A truncated value list SHALL say so.
Fields the server skipped SHALL be discoverable with their reasons, not hidden.

#### Scenario: Ranked list with charts

- **WHEN** the comparison returns
- **THEN** fields appear in score order, each with a chart of selection vs
  baseline shares and its participation ratios

#### Scenario: Truncation and skips are stated

- **WHEN** a field's values were trimmed or a field was skipped
- **THEN** the panel shows the truncation on that field and lists skipped
  fields with the server's reason

#### Scenario: Charts use the shared tooltip

- **WHEN** the user hovers a bar or bucket
- **THEN** the shared visualization tooltip shows the value or bucket, both
  cohorts' shares and counts, and the risk ratio for a dimension value

### Requirement: Comparison results refine the active query

Every value bar SHALL offer "only this value", "exclude this value", and "group
by this field"; every measure bucket SHALL offer "below this" and "above this".
Choosing one SHALL apply the corresponding filter or grouping to the tab's
active query, close or refresh the panel, and leave the new filter visible and
removable like any other.

#### Scenario: A value becomes a filter

- **WHEN** the user picks "only this value" on `http.route = /api/checkout`
- **THEN** the tab's query gains a removable `http.route = /api/checkout` filter
  and the list, chart, and facets update to it

#### Scenario: A field becomes a grouping

- **WHEN** the user picks "group by this field" on a dimension
- **THEN** the traces tab switches to the grouped view by that field with the
  active filters intact

### Requirement: A comparison is shareable

The panel's selection, scope, and field filter SHALL be carried in the URL so a
comparison can be bookmarked and shared, and reopening the URL SHALL re-run the
same comparison against the current data.

#### Scenario: Reload reproduces the comparison

- **WHEN** the user reloads a URL that encodes an open comparison
- **THEN** the panel reopens with the same selection and scope and re-executes

### Requirement: Field filter narrows the panel

The panel SHALL offer a text filter over field names so a long list can be
narrowed without re-running the comparison.

#### Scenario: Filtering by name

- **WHEN** the user types `http` in the field filter
- **THEN** only fields whose names contain `http` remain visible, in their
  original ranked order
