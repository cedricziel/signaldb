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
so the baseline is the currently displayed data minus the selection. The
scope SHALL be rebuilt from the tab's **record-level** state — active filters,
range, and any `extract` — never from the presentation pipeline: grouping,
`topk`, `order`, and `limit` stages of the grouped view SHALL NOT be part of
the comparison document, so cohorts are records, not truncated aggregate rows.

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

#### Scenario: Grouped-view presentation stages are not inherited

- **WHEN** the grouped table is showing the top 20 groups ordered by p95 with
  a row limit
- **THEN** the comparison document built from it contains the tab's filters,
  range, and extract stages plus the `compare` stage — no `aggregate`,
  `topk`, `order`, or `limit` — and the baseline count equals the total record
  count of the filtered window, not the count of the displayed groups

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
removable like any other. "Exclude this value" SHALL keep records where the
field is absent: it applies `or(not(field exists), field != v)`, not a bare
`field != v` (which, under the IR's absent semantics, would also drop
records lacking the field).

#### Scenario: A value becomes a filter

- **WHEN** the user picks "only this value" on `http.route = /api/checkout`
- **THEN** the tab's query gains a removable `http.route = /api/checkout` filter
  and the list, chart, and facets update to it

#### Scenario: Excluding a value keeps records without the field

- **WHEN** the user picks "exclude this value" on `db.system = postgresql`
- **THEN** the tab's query gains a removable filter that drops only records
  whose `db.system` is `postgresql`, and records with no `db.system` remain
  in the result

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
