## Purpose

Defines SignalDB's native, tenant-scoped introspection surface: which signal
sources a tenant can query, which fields are queryable on each — as logical
dotted OTel-native names with canonical types, over the same namespace as
`query-ir-core` — and which values a field takes. Answers come from the schema
registry and maintained statistics rather than from scanning signal data, and
every answer states which tier produced it and what it cost.

## ADDED Requirements

### Requirement: Field discovery over the logical namespace

SignalDB SHALL expose, per signal source and tenant, the set of queryable fields
as logical dotted OTel-native names with their canonical value type, such that
any field returned by discovery is valid to reference in a `query-ir-core`
predicate. Each field SHALL carry its filterability, and its attribute level
where known. Discovery SHALL NOT expose physical column names, the
attribute-map container, or any storage detail, and SHALL NOT report whether a
field is materialized/promoted — promotion changes performance, never which
names are valid.

#### Scenario: Discovered fields are queryable

- **WHEN** a client requests the queryable fields for a signal source
- **THEN** each returned field is a logical name with a canonical type that can
  be used directly in an IR predicate, with no physical or storage reference

#### Scenario: Physical detail is never surfaced

- **WHEN** a tenant has attribute keys served from materialized columns and
  others served from the attribute map
- **THEN** both appear as ordinary logical fields, indistinguishable in the
  response, and no column name or promotion state appears in it

#### Scenario: Retrieval-only fields are marked, not hidden

- **WHEN** a source declares a field that predicates may not address
- **THEN** the field is listed with its filterability stated, rather than
  omitted or silently offered as filterable

#### Scenario: Discovery is tenant-scoped

- **WHEN** an authenticated client requests discovery
- **THEN** results cover only the authenticated tenant and dataset, using the
  same tenant-scoped request context and the same per-source read scope as the
  native query surface, and a request naming another tenant is rejected

### Requirement: Discovery is answered from registry and statistics, not scans

A discovery request SHALL be answered from declared schema, the tenant's schema
registries, and maintained statistics, without reading signal data — except on
the explicitly requested sampled path defined below. The default path SHALL NOT
dispatch a query for execution over stored signal data.

#### Scenario: A field request reads no signal data

- **WHEN** a client requests the queryable fields of a source
- **THEN** the response is produced without executing a query over the source's
  stored signal data

#### Scenario: Discovery survives a busy query path

- **WHEN** query execution capacity is exhausted
- **THEN** a default-path discovery request still returns its answer, because it
  does not depend on query execution

### Requirement: Every discovery answer states its provenance and cost

Each discovered item SHALL carry the tier that produced it — declared schema,
schema registry, maintained statistics, or a sampled read — and each response
SHALL carry a cost statement naming which tier answered it, whether the answer
is scoped to the requested time window, whether it is sampled or approximate,
and how recent the statistics behind it are. A response SHALL NOT present a
statistics-derived answer as if it were exact or window-scoped.

#### Scenario: A metadata-tier answer is labelled as not window-scoped

- **WHEN** a client requests fields for a narrow time range and the answer comes
  from maintained statistics that carry no time dimension
- **THEN** the response states that the answer is not window-scoped and reports
  how recent the statistics are, rather than implying the range narrowed it

#### Scenario: Missing statistics are reported, not hidden

- **WHEN** a tenant has no maintained statistics yet
- **THEN** the response returns the declared fields, states that no statistics
  are available, and warns the client, rather than presenting the declared set
  as the tenant's complete field set

#### Scenario: A sampled answer declares its cost

- **WHEN** a client explicitly requests the sampled path
- **THEN** the response states that it read signal data, that the answer is
  sampled and window-scoped, and how much data it read

### Requirement: Field discovery reports coverage and cardinality hints

Where statistics exist for a field, discovery SHALL report how much of the
tenant's data carries it (coverage) and an approximate distinct-value count,
marked as approximate and marked when the true count is only known to be at or
above a cap. Where no statistics exist for a field, these hints SHALL be absent
rather than defaulted to a value that could be mistaken for a measurement.

#### Scenario: A rare field is distinguishable from a ubiquitous one

- **WHEN** two fields have statistics, one present on nearly every record and
  one on a small fraction
- **THEN** their reported coverage differs accordingly, and the fields are
  ordered so that broadly present fields appear before rare ones

#### Scenario: A capped cardinality is reported as a lower bound

- **WHEN** a field's distinct-value count exceeded the statistics collector's cap
- **THEN** the response reports the count as a lower bound, not as an exact value

#### Scenario: Absent statistics yield absent hints

- **WHEN** a declared field has no statistics
- **THEN** it is still listed, with coverage and cardinality reported as unknown

### Requirement: Value suggestions prefer declared value sets

SignalDB SHALL provide value suggestions for a named field. When the field's
value set is declared — a registry-declared enumeration, or an intrinsic
enumeration such as span kind or status code — the suggestions SHALL be exactly
that set, produced without reading signal data. When maintained value statistics
cover the field, the suggestions SHALL be the bounded, counted top values from
those statistics, marked as approximate and dated. When neither applies, the
response SHALL return no values, say why, and name what would answer the
question — it SHALL NOT fall back to scanning signal data.

#### Scenario: An enumerated field answers exactly and for free

- **WHEN** a client requests values for a field whose value set is declared
- **THEN** the response contains that enumeration, marked as registry-derived,
  and no signal data was read

#### Scenario: A statistics-covered field answers from statistics

- **WHEN** maintained value statistics cover the requested field
- **THEN** the response returns the bounded top values with their counts, marked
  as approximate and carrying the age of the statistics

#### Scenario: An uncovered field is answered honestly

- **WHEN** a client requests values for a field with no declared value set and
  no maintained statistics
- **THEN** the response returns no values, states that no metadata covers the
  field, and names the request that would compute the answer by reading data —
  and no signal data is read

### Requirement: Reading data for value suggestions is opt-in and bounded

A client MAY explicitly request that value suggestions be computed by reading
signal data. Such a request SHALL be bounded by the requested time window, a
result limit, and a sampled read bound, SHALL be authorized by the same
per-source read scope as a query, and its cost SHALL be reported in the
response. Without that explicit request, no discovery request SHALL read signal
data.

#### Scenario: Opting in returns a bounded sampled answer

- **WHEN** a client requests values for a field and explicitly opts into reading
  data
- **THEN** the response contains values observed in the requested window, marked
  as sampled, bounded by the documented limits, and reporting what it read

#### Scenario: Not opting in never reads data

- **WHEN** a client requests values for an uncovered field without opting in
- **THEN** no signal data is read, whatever the field

#### Scenario: The sampled path is authorized like a query

- **WHEN** a client without the source's read scope opts into the sampled path
- **THEN** the request is rejected before any data is read

### Requirement: Signal source discovery

SignalDB SHALL expose the signal sources available to the authenticated tenant
and dataset, each with whether it is currently available to query, derived from
tenant metadata rather than from reading signal data. A registered source with
no data SHALL be reported as available and empty, never omitted or reported as
an error.

#### Scenario: Sources list the tenant's queryable signals

- **WHEN** an authenticated client requests the available sources
- **THEN** it receives the registered signal sources with their availability,
  and each returned source name is valid as an IR document's source

#### Scenario: A signal with no data is still a source

- **WHEN** a tenant has a registered signal with no ingested data
- **THEN** the source is listed as available, not omitted

### Requirement: Predicate-scoped discovery is refused, not approximated

A discovery request that attempts to scope its answer by a predicate SHALL be
rejected with an error that names the equivalent query the client can run
instead. The system SHALL NOT silently ignore the predicate, and SHALL NOT
silently answer it by scanning.

#### Scenario: A filtered discovery request is rejected with a pointer

- **WHEN** a client submits a discovery request carrying a filter predicate
- **THEN** the request is rejected with an error explaining that discovery is
  not predicate-scoped and naming the aggregation query that computes the
  scoped answer

### Requirement: Discovery results are bounded and ordered

Field and value results SHALL be bounded by documented limits, SHALL state when
a limit truncated them, and SHALL be returned in a stable, documented order so a
client renders the same list for the same data.

#### Scenario: A wide tenant is truncated, not unbounded

- **WHEN** a tenant has more discoverable fields than the response limit
- **THEN** the response returns the limit's worth of fields and marks itself
  truncated

#### Scenario: Repeated requests agree

- **WHEN** the same discovery request is issued twice against unchanged metadata
- **THEN** both responses list the same items in the same order

### Requirement: Discovery replaces the dialect metadata endpoints for first parties

The native discovery surface SHALL be the field/value introspection used by the
first-party surfaces, over the same logical dotted names as the Query IR. The
Loki, Tempo, Prometheus, and Pyroscope metadata endpoints SHALL remain unchanged
for external clients, and SHALL NOT be extended to carry this capability.

#### Scenario: First-party surfaces use the native surface

- **WHEN** the CLI or the MCP server answers "what fields can I query"
- **THEN** it does so through the native discovery surface, not through a
  compatibility dialect's metadata endpoint

#### Scenario: Compatibility endpoints keep their behaviour

- **WHEN** an external client calls a Tempo or Loki metadata endpoint
- **THEN** it receives that dialect's documented response exactly as before

### Requirement: Discovery is reachable from every shipped surface

The discovery capability SHALL be reachable over the HTTP API, the CLI, and the
MCP server, each consuming the same native surface through its generated client,
so a field discoverable on one is discoverable on all.

#### Scenario: CLI and MCP agree with the API

- **WHEN** a tenant's data carries an attribute key covered by statistics
- **THEN** the HTTP discovery response, the CLI discovery command, and the MCP
  discovery tool all list that key with the same name and type
