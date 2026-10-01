## ADDED Requirements

### Requirement: Declared metric inventory

Every metric instrument SignalDB emits about its own operation SHALL be
declared in the SignalDB convention registry (`otel/registry/signaldb.yaml`)
as a metric group carrying its metric name, instrument kind, unit, and the
attributes it may record. An instrument or metric attribute that is not
declared SHALL fail the build, in the same way an undeclared span attribute
already does.

#### Scenario: Undeclared instrument fails the build

- **WHEN** a new metric instrument is added without a matching registry
  declaration
- **THEN** the registry drift check fails and names the undeclared
  instrument

#### Scenario: Undeclared metric attribute fails the build

- **WHEN** a recording site adds an attribute to an instrument and that
  attribute is absent from the instrument's declared attribute set
- **THEN** the registry drift check fails and names the undeclared attribute

#### Scenario: Instruments are constructed in one place

- **WHEN** a metric instrument is constructed outside SignalDB's
  self-monitoring module, other than by the synthetic telemetry generator
- **THEN** CI rejects the change

### Requirement: Convention-defined metric names are pinned

A metric whose name is defined by the OpenTelemetry semantic conventions
SHALL use that name exactly as the pinned semantic-conventions version
defines it, with the unit the convention specifies, and the name SHALL be
pinned against the semantic-conventions constants so that a drift between
SignalDB's literal and the convention fails a test.

#### Scenario: Convention metric name drifts from the pinned version

- **WHEN** the pinned semantic-conventions version renames a metric
  SignalDB emits
- **THEN** the name pin fails, rather than SignalDB silently emitting a name
  the convention no longer defines

### Requirement: One tenancy vocabulary across signals

Metric attributes identifying SignalDB tenancy SHALL use the same attribute
keys the spans use — `signaldb.tenant.id`, `signaldb.dataset.id`,
`signaldb.table` — so that a metric series and the spans explaining it are
joinable on identical keys. Alternate spellings of tenancy on metrics
(`tenant`, `tenant_id`) SHALL NOT be emitted.

#### Scenario: Operator pivots from a metric series to traces

- **WHEN** an operator selects a metric series scoped to one tenant and
  queries traces filtered by the same attribute key and value
- **THEN** both queries use `signaldb.tenant.id` and return telemetry for
  that tenant

#### Scenario: Legacy tenancy labels are gone

- **WHEN** SignalDB's self-monitoring metrics are exported
- **THEN** no data point carries an attribute named `tenant` or `tenant_id`

### Requirement: SignalDB-specific metric attributes are namespaced

Every metric attribute that SignalDB defines for itself SHALL be namespaced
under `signaldb.*`. Unnamespaced attribute keys SHALL NOT be emitted, so
that SignalDB's own telemetry cannot collide with attributes a tenant's
applications emit into the same backend.

#### Scenario: Self-monitoring metrics carry no bare attribute keys

- **WHEN** SignalDB's self-monitoring metrics are exported
- **THEN** every attribute key on every data point is either defined by the
  OpenTelemetry semantic conventions or namespaced under `signaldb.`

### Requirement: Resource identity is not repeated on data points

Attributes that identify the emitting service SHALL be carried by the
telemetry resource only, and SHALL NOT be recorded as attributes on
individual metric data points.

#### Scenario: Service identity appears once

- **WHEN** any SignalDB metric data point is exported
- **THEN** it carries no `service.name` attribute, and the exported
  resource identifies the service

### Requirement: Bounded metric cardinality

A metric whose name the OpenTelemetry semantic conventions define SHALL
carry only the attributes that convention defines, and in particular SHALL
NOT carry tenant identity. Tenant identity SHALL appear only on
SignalDB-defined instruments whose purpose is per-tenant accounting.

#### Scenario: Convention metric stays portable

- **WHEN** SignalDB emits a metric the conventions define
- **THEN** its attribute set is a subset of the attributes that convention
  defines, so a dashboard built on it is not SignalDB-specific

#### Scenario: Per-tenant accounting remains available

- **WHEN** an operator needs per-tenant ingest, storage, or rate-limit
  figures
- **THEN** the SignalDB-defined instruments for those concerns carry
  `signaldb.tenant.id`

### Requirement: Self-monitoring traffic is excluded from ingestion metrics

Metrics that count ingested telemetry SHALL NOT count traffic belonging to
the reserved self-monitoring tenant, so that SignalDB's own telemetry
exports do not inflate the figures describing customer ingestion.

#### Scenario: Self-monitoring exports are not counted as ingest

- **WHEN** SignalDB exports its own telemetry into its reserved
  self-monitoring tenant
- **THEN** the ingestion counters do not increase on account of that traffic
