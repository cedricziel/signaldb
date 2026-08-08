## Purpose

Guarantees that the traces tab can be narrowed by attribute value, and that the
counts it shows to guide that narrowing are exact for the selected window rather
than a sample of whatever rows the list happened to fetch.

## ADDED Requirements

### Requirement: Facet values carry exact counts for the window

A facet SHALL present its values with the number of matching records across the
entire selected window. Counts SHALL NOT be derived from the row-limited result
set backing the trace list.

#### Scenario: Counts are independent of the trace list's limit

- **WHEN** the trace list is truncated by its row limit
- **THEN** each facet value's count still reflects every matching record in the
  window
- **AND** changing the row limit does not change the counts

#### Scenario: Values are ordered by frequency

- **WHEN** a facet is expanded
- **THEN** its values are ordered with the most frequent first

#### Scenario: A truncated value list says so

- **WHEN** a facet holds more distinct values than the sidebar displays
- **THEN** the omission is stated rather than the list silently ending

### Requirement: Only enumerable fields are offered as facets

The sidebar SHALL offer only fields whose values the backend can enumerate
exactly. A field whose value list cannot be resolved SHALL NOT be presented as a
facet.

#### Scenario: An unresolvable field is not offered

- **WHEN** the backend cannot enumerate a field's values for the tenant
- **THEN** that field does not appear in the sidebar
- **AND** no facet is shown whose only value is a placeholder for "unresolved"

### Requirement: Selecting facet values narrows the traces shown

Selecting a facet value SHALL filter the traces to those matching it. Active
filters SHALL be visible and individually removable.

#### Scenario: A selected value filters the results

- **WHEN** a user selects a value from a facet
- **THEN** the trace list shows only traces matching that value
- **AND** the filter is displayed as a removable control

#### Scenario: Removing a filter restores the wider result

- **WHEN** a user removes an active filter
- **THEN** the traces widen back to the unfiltered set for the window

#### Scenario: Several facets combine

- **WHEN** values are selected from more than one facet
- **THEN** only traces matching every selection are shown

### Requirement: The volume chart reflects the active filters

The traces tab's volume chart SHALL describe the same set of traces the list
shows, so the chart and the table cannot disagree.

#### Scenario: Filtering narrows the chart too

- **WHEN** a filter is active
- **THEN** the volume chart counts only records matching that filter

### Requirement: A filtered trace view is shareable

Active filters SHALL be carried in the URL, so a filtered view can be
bookmarked, shared, and reached with browser history navigation.

#### Scenario: Filters survive a reload

- **WHEN** a user applies filters and reloads the page
- **THEN** the same filters are active and the same traces are shown

#### Scenario: An unparseable filter is ignored rather than fatal

- **WHEN** a URL carries a filter that cannot be parsed
- **THEN** the view loads without that filter instead of failing
