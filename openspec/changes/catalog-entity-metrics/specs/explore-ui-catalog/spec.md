## ADDED Requirements

### Requirement: Registry-derived metrics on an entity detail page

A Catalog entity detail page SHALL show the metrics the tenant's schema
registries associate with that entity type, discovered from the registry's
metric-to-entity associations rather than from a list maintained in the UI.
An entity type the registries associate no metric with SHALL show no metrics
section, rather than an empty one.

#### Scenario: A host's system metrics

- **WHEN** a user opens the detail page of a `host` entity, and the tenant's
  registries associate `system.*` metrics with the `host` entity
- **THEN** those metrics are the ones offered on the page, without the UI
  naming any metric itself

#### Scenario: An entity type with no associated metrics

- **WHEN** a user opens the detail page of an entity type the registries
  associate no metric with
- **THEN** the page shows no metrics section at all

#### Scenario: A tenant's own registry

- **WHEN** a tenant publishes a registry associating its own metrics with an
  entity type
- **THEN** that entity type's detail page offers those metrics on the same
  terms as bundled OpenTelemetry ones, with no change to the UI

### Requirement: Entity metric series are pinned to the entity's identity

Metric series shown on an entity detail page SHALL be filtered to the entity
being viewed, using the same identity dimensions and values that pin the
page's other measurements. A page SHALL NOT show a metric aggregated across
every entity of its type.

#### Scenario: One process among many

- **WHEN** a user opens the detail page of a process entity identified by
  `process.pid` and `host.name`
- **THEN** each metric series shown is restricted to that `process.pid` on
  that `host.name`

#### Scenario: Drilled into a breakdown row

- **WHEN** a user has drilled from an entity into one of its breakdown rows
- **THEN** the metrics shown remain those of the entity itself, pinned to the
  entity's identity

### Requirement: Only metrics observed in the window are charted

An entity detail page SHALL chart only those associated metrics that have data
in the selected time window, and SHALL NOT render an associated-but-unobserved
metric as a series of zeroes. A metric that is associated and observed SHALL be
charted over the selected window.

#### Scenario: Associated metric with no data in the window

- **WHEN** a metric is associated with the entity type but has no points for
  this entity in the selected window
- **THEN** no chart is rendered for it, and it is not shown as a flat zero
  series

#### Scenario: Window narrowed past the data

- **WHEN** a user narrows the time range until an entity's metrics have no
  points in it
- **THEN** the page reports that the entity has no metric data in this window
  rather than charting zeroes

### Requirement: Metrics-only entity rows carry a sparkline

The Catalog entity list SHALL show a sparkline for an entity type that has an
associated headline metric, so that an entity type no trace ever carried is not
presented as entirely unmeasured. An entity type with no associated headline
metric, or a row with no data for it in the window, SHALL leave the column
empty rather than draw a flat line.

#### Scenario: Listing containers

- **WHEN** a user lists an entity type whose registries associate a headline
  metric with it, and rows have data for it in the window
- **THEN** each such row carries a sparkline of that metric alongside its
  existing columns

#### Scenario: No headline metric for the entity type

- **WHEN** a user lists an entity type the registries associate no metric with
- **THEN** no sparkline column is shown

### Requirement: Metric provenance is stated on the page

An entity detail page's metrics section SHALL state that its metric selection
came from the schema registries' entity associations, in the same way the
entity list states which attributes and signals fed it.

#### Scenario: Reading where the metrics came from

- **WHEN** a user views an entity detail page's metrics section
- **THEN** the section names the registry association it was discovered
  through, so the selection is explainable without reading the code
