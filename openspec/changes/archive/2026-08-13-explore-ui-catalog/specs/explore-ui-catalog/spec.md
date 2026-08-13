## Purpose

Defines the Catalog tab's entity detail behavior: navigating from a matching
span into the trace waterfall, surfacing per-entity-type read-only detail
tables (e.g. top database statements), color-coding the trace waterfall by
span kind, and showing a service's time-by-dependency-category breakdown.

## ADDED Requirements

### Requirement: Matching-span rows open the trace waterfall

A "recent matching spans" row on a Catalog entity detail page SHALL, when
clicked, switch to the Traces signal and open that span's trace in the
waterfall view.

#### Scenario: Opening a matching span

- **WHEN** a user clicks a row in an entity detail page's recent matching
  spans list
- **THEN** the explore UI switches to the Traces signal and renders the
  waterfall for that span's trace id

### Requirement: Read-only top-values table for supporting entity types

An entity type MAY define a read-only top-values table, distinct from the
drillable breakdown table, that ranks distinct values of one field by
frequency within the entity's pinned identity and time window. Clicking a
row in this table SHALL NOT drill into a secondary pin or otherwise change
navigation state.

#### Scenario: Top statements for a database entity

- **WHEN** a user views a database entity's detail page
- **THEN** a "Top statements" table lists distinct `db.query.text` values
  observed for that database, ranked by frequency, and clicking a row does
  not navigate

### Requirement: Trace waterfall spans are color-coded by kind

The trace waterfall SHALL color-code each span's bar according to its
`span.kind` (SERVER, CLIENT, INTERNAL, PRODUCER, CONSUMER), display a
legend when kind data is available, and show the kind in the span detail
panel. Kind data SHALL be sourced via a dedicated Query IR query and SHALL
NOT be added to the Tempo-compatible trace response.

#### Scenario: Waterfall bars reflect span kind

- **WHEN** a trace's spans have resolvable `span.kind` values
- **THEN** each waterfall bar is colored according to its kind, a legend
  enumerating the kinds present is shown, and the span detail panel
  displays the selected span's kind

### Requirement: Service time-by-dependency-category breakdown

A service's own Catalog entity detail page SHALL show a breakdown of
summed CLIENT-span duration by dependency category — database, HTTP, RPC,
messaging — derived from the presence of `db.system.name`,
`http.request.method`, `rpc.system`, and `messaging.system` respectively,
with any remaining CLIENT-span duration not matching a known category
shown as "Other".

#### Scenario: Viewing a service's dependency breakdown

- **WHEN** a user views a service's own Catalog entity detail page and that
  service has outbound CLIENT-kind spans in the selected window
- **THEN** a proportional bar and legend show the share of total CLIENT
  duration attributable to each dependency category and to "Other"

#### Scenario: No dependency traffic

- **WHEN** a service has no outbound CLIENT-kind spans in the selected
  window
- **THEN** the breakdown section shows an explanatory empty state instead
  of an empty bar
