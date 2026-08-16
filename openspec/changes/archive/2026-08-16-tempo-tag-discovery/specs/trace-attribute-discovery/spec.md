## Purpose

Lets a tenant discover which trace attributes exist in its data and which values they take, so people and agents can build TraceQL queries and filters from what is actually there rather than from a fixed list.

## ADDED Requirements

### Requirement: Tag names reflect the tenant's data

The Tempo-compatible tag-name endpoints (`/api/search/tags` and `/api/v2/search/tags`) SHALL return the attribute keys observed in the caller's tenant and dataset within the requested time window: resource attribute keys, span attribute keys, and the intrinsic fields (`name`, `status`, `kind`, `duration`, `rootServiceName`, `rootName`). The v2 endpoint SHALL group them by scope (`resource`, `span`, `intrinsic`) and SHALL honour a `scope` filter. `start`/`end` (unix seconds) bound the window; when omitted the default lookback matches the Loki metadata endpoints. Names SHALL be OTel dotted keys as ingested, and SHALL be sorted. A tenant with no traces SHALL receive only the intrinsics, not an error.

#### Scenario: Custom attributes are discoverable

- **WHEN** a tenant has ingested spans carrying `deployment.environment.name` (resource) and `http.route` (span) in the window
- **THEN** `/api/search/tags` lists both keys alongside `service.name` and the intrinsics, and `/api/v2/search/tags` places them under `resource` and `span` respectively

#### Scenario: Scope filter narrows v2

- **WHEN** a client calls `/api/v2/search/tags?scope=span`
- **THEN** only span-scoped keys are returned

#### Scenario: Window bounds discovery

- **WHEN** an attribute appears only in spans older than the requested `start`
- **THEN** it is not listed for that window

#### Scenario: Empty tenant lists intrinsics only

- **WHEN** a tenant has no traces in the window
- **THEN** the response lists the intrinsic fields and no error is returned

### Requirement: Tag values reflect the tenant's data

The tag-value endpoints (`/api/search/tag/{tag}/values` and `/api/v2/search/tag/{scoped_tag}/values`) SHALL return the distinct values of the named attribute observed in the caller's tenant and dataset within the window, for any attribute — dedicated columns and map-stored attributes alike; the intrinsics `status` and `kind` SHALL return their enumeration values. An unknown or unobserved tag SHALL return an empty list, never `501` or `404`. Scoped names (`resource.x`, `span.x`, `.x`) SHALL resolve to the same attribute as their unscoped form. Values SHALL be sorted and the list SHALL be bounded, with the response stating truncation where the protocol allows it.

#### Scenario: Values of a map-stored attribute

- **WHEN** a tenant's spans carry `http.route` with values `/api/orders` and `/api/users` in the window
- **THEN** `/api/search/tag/http.route/values` returns both, and `/api/v2/search/tag/span.http.route/values` returns them tagged with the scoped name

#### Scenario: Unknown tag is empty, not an error

- **WHEN** a client asks for values of `no.such.attribute`
- **THEN** the response is `200` with an empty list

#### Scenario: Intrinsic enums are static

- **WHEN** a client asks for values of `status` or `kind`
- **THEN** it receives the enumeration values (`ok`, `error`, `unset`; the span kinds) regardless of data

### Requirement: Discovery is bounded and observable

Tag discovery SHALL not scan a tenant's entire history: it SHALL be limited to the requested or default window and to a bounded sample of rows, so it stays interactive on large tenants, and it SHALL run inside the same query-stage instrumentation as other querier reads so its cost is visible in self-monitoring.

#### Scenario: Large tenant stays interactive

- **WHEN** a tenant holds far more spans in the window than the sample bound
- **THEN** the tag-name request completes within the interactive budget by reading at most the bounded sample

#### Scenario: Discovery is traced

- **WHEN** a tag-name or tag-value request executes
- **THEN** a query-execution stage span for the discovery read is exported, like the label-discovery reads for logs and metrics

### Requirement: Every discovery surface sees the same names and values

The MCP `discover_attributes` tool with `signal: "traces"`, the CLI `discover` command for traces, Grafana's Tempo datasource, and the UI's traces attribute-key suggestions SHALL all be backed by these endpoints, so a key that appears in one appears in all.

#### Scenario: MCP and CLI agree with the API

- **WHEN** a tenant's spans carry `http.route` in the window
- **THEN** `discover_attributes(signal="traces")` and `signaldb discover --traces` both list `http.route`, and `discover_attributes(signal="traces", tag="http.route")` returns its values

#### Scenario: UI suggests observed keys

- **WHEN** a user types an attribute key on the traces tab (the "group by attribute" input)
- **THEN** the suggestions include the attribute keys observed in the current window, merged with registry hits as on the logs tab
