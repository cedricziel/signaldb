## Purpose

Defines SignalDB's native, structured query capability at its foundational
scope: a versioned Query IR with a defined type system, submitted over a native
HTTP surface and executed by lowering to a DataFusion query plan, for
single-signal (logs, traces) queries. It lets the first-party UI and CLI build
queries structurally — without formulating a LogQL/TraceQL string — and
establishes the stable contract that later changes (cross-signal correlation,
structural trace matching, metrics) and future front-ends lower into. The
compatibility dialects are unchanged; this sits alongside them.

## ADDED Requirements

### Requirement: Versioned, structured query IR

SignalDB SHALL accept a structured query as a versioned JSON document carrying a
time range, a source selection, a declared result envelope, and an ordered
pipeline of typed stages. The document SHALL carry an explicit IR version, and
the server SHALL accept a bounded range of versions and report the range it
supports. The IR schema SHALL evolve additively — new stages, operators, and
optional fields MAY be added; accepted operators SHALL be deprecated before any
removal — so that a previously valid stored query remains valid.

#### Scenario: A versioned query executes

- **WHEN** a client submits an IR document with a supported version, a logs or
  traces source, a filter, and an aggregation
- **THEN** the query executes and returns a result in the declared envelope

#### Scenario: Unsupported version is reported, not misinterpreted

- **WHEN** a client submits an IR document whose version is outside the range the
  server supports
- **THEN** the request is rejected with an error identifying the supported
  version range, rather than being parsed under a different version's rules

#### Scenario: Additive evolution preserves stored queries

- **WHEN** the IR schema gains a new optional field or stage in a later server
  version and an older stored query is replayed
- **THEN** the older query still validates and executes with unchanged meaning

### Requirement: Defined type system and evaluation semantics

The IR SHALL have a type system and evaluation semantics defined independently of
the query plan it is compiled to: each logical field has one canonical value
type owned by the attribute registry; each stage consumes and produces a typed
relation; and null/absent handling and literal coercion are defined by the IR,
not inherited from the execution engine. The result of a query SHALL be
determined by these semantics, not by engine-version-specific behaviour.

#### Scenario: Absent-field comparison is defined by the IR

- **WHEN** a query filters on a field that is absent from some records (for
  example a negated equality `not(field = x)`)
- **THEN** records where the field is absent are handled per the IR's specified
  absent-value semantics, consistently regardless of the underlying execution
  engine version

#### Scenario: Literal coercion follows the registry type

- **WHEN** a query compares a field against a literal (e.g. a duration `"500ms"`
  or a numeric `"17"`)
- **THEN** the literal is coerced to the field's registry-declared canonical
  type, and a literal that cannot be coerced is rejected at validation rather
  than silently cast at runtime

### Requirement: Structured stage operands

Stage operands (aggregation targets, ordering keys, rank expressions) SHALL be
structured values, not embedded expression strings that require parsing. A
client SHALL NOT express an operand as a mini-expression string.

#### Scenario: Rank references a structured aggregate

- **WHEN** a query ranks results by an aggregated value
- **THEN** the rank stage references the aggregate as a structured operand, and
  a request that supplies an operand as an unparsed expression string is rejected

### Requirement: Shared predicate grammar over logical field names

Filtering SHALL use one predicate grammar — comparison leaves (`{field, op,
value}`) composed with `and`/`or`/`not` — where `field` is a logical, dotted
OTel-native attribute name. A client SHALL NOT reference a physical column name,
the attribute-JSON blob, or any storage detail; such a query SHALL be rejected.
Supported operators SHALL include equality, ordered comparison, membership,
range, substring match, and existence.

#### Scenario: Predicate filters on an OTel attribute

- **WHEN** a query filters on `service.name = "checkout"` combined with
  `http.status_code >= 500` under an `and`
- **THEN** only matching records are returned, and the query contains no physical
  column or storage-layout reference

#### Scenario: Physical addressing is rejected

- **WHEN** a query attempts to filter directly on the attribute-JSON blob or a
  raw storage column name
- **THEN** the query is rejected as invalid rather than executed

### Requirement: Registry-mediated field resolution independent of promotion

Every logical field SHALL be resolved to its physical location — a promoted
column or an attribute-JSON extraction — through the attribute registry at plan
time. The result of a query SHALL NOT depend on whether a field is currently
promoted; promotion state SHALL affect only performance.

#### Scenario: Same result before and after promotion

- **WHEN** the same IR query is executed against a field served as an
  attribute-JSON extraction, and later against the same field after it has been
  promoted to a physical column
- **THEN** both executions return the same result set

### Requirement: Extensible signal-source model

The IR source SHALL reference a registered signal source rather than a fixed,
hardcoded set. Logs and traces SHALL be available as sources in this capability;
the model SHALL allow additional sources (e.g. metrics, profiles) to be added by
later changes without altering the IR document shape.

#### Scenario: Logs and traces are queryable sources

- **WHEN** a client selects `logs` or `traces` as the query source
- **THEN** the query executes against that signal

#### Scenario: Adding a source does not reshape the IR

- **WHEN** a later change registers an additional signal source
- **THEN** existing IR documents remain valid and the document shape is unchanged

### Requirement: Legal-stage enforcement by relation type

Each stage SHALL declare its input-relation constraint and output relation, and
the system SHALL infer the relation type through the pipeline and reject a stage
whose input constraint is unmet — before execution, with an error naming the
offending stage.

#### Scenario: Illegal stage is rejected pre-execution

- **WHEN** a query applies a log-only field-extraction stage to a traces source
- **THEN** the query is rejected during validation with an error naming the
  stage and source, and no plan is executed

### Requirement: Declared and validated result envelope

A query SHALL declare its result envelope (`rows`, `series`, or `table` in this
capability), and the system SHALL validate the declared envelope against the
inferred terminal relation type, rejecting a mismatch before execution. The
`rows` envelope SHALL return a curated projection of fields and SHALL NOT return
all physical columns implicitly.

#### Scenario: Envelope mismatch is rejected

- **WHEN** a query declares the `series` envelope but its terminal stage produces
  a non-time-series relation
- **THEN** the query is rejected at validation time with an envelope-mismatch
  error

#### Scenario: Row results are a curated projection

- **WHEN** a query returns the `rows` envelope
- **THEN** the response contains an explicit, bounded set of fields rather than
  every physical column of the underlying table

### Requirement: Native query surface

SignalDB SHALL expose the IR over a native HTTP endpoint (`POST /api/v1/query`)
that is authenticated and tenant-scoped consistently with the other query APIs.
The endpoint, the versioned IR request schema, and the result-envelope schemas
SHALL be described by the code-first OpenAPI specification, and the first-party
UI and CLI SHALL consume it exclusively through their generated clients.

#### Scenario: Authenticated, tenant-scoped submission

- **WHEN** an authenticated client submits an IR query with a tenant context
- **THEN** the query executes only against that tenant's datasets and returns the
  enveloped result, honouring the same auth/tenant rules as the Tempo/LogQL/
  Prometheus surfaces

#### Scenario: Surface is generated-client-only for first parties

- **WHEN** the UI or CLI issues an IR query
- **THEN** it does so through the generated TypeScript client / Rust SDK derived
  from the OpenAPI spec, not through hand-written HTTP calls

### Requirement: Additive, non-regressing surface

The IR query capability SHALL be additive: OTLP ingestion, the Tempo/LogQL/
Prometheus query surfaces, existing Flight wire schemas, and the on-disk Iceberg/
WAL layout SHALL be unchanged, and no existing query SHALL regress.

#### Scenario: Existing dialects are unaffected

- **WHEN** the IR capability is deployed
- **THEN** existing TraceQL/LogQL/PromQL queries continue to execute exactly as
  before, and no ingest, Flight schema, or on-disk layout changes
