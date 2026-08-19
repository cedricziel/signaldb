## Purpose

Defines SignalDB's compatibility query languages as standalone front-ends: what
syntax each accepts and rejects, that the accept/reject decision is a function of
the query text alone, that syntax which is valid-but-unsupported is
distinguishable from syntax which is invalid, and that a front-end never depends
on tenant, catalog, or storage state. This is the contract that lets the parsers
be compiled and reused independently of the query engine, including outside
SignalDB.

It does not define what a parsed query _does_. Translating a parsed query into a
plan over stored signals — column mapping, attribute promotion, the choice
between a materialized column and a JSON extraction — belongs to the querier and
is deliberately outside this capability.

## ADDED Requirements

### Requirement: Query-language parsing is independent of deployment state

Parsing and syntactic validation of a compatibility query language SHALL depend
only on the query text. A front-end SHALL NOT consult a tenant, dataset, schema
registry, catalog, table, attribute-promotion state, or any configuration, and
SHALL NOT be able to observe them.

#### Scenario: The same query parses identically everywhere

- **WHEN** the same query string is parsed on two deployments whose tenants,
  registered attributes, promoted columns, and stored data differ entirely
- **THEN** both produce the same accept-or-reject outcome and, on acceptance,
  the same parsed structure

#### Scenario: Promotion state cannot change parsing

- **WHEN** an attribute referenced by a query is promoted to a materialized
  column, or demoted back
- **THEN** the parse result for that query is unchanged — promotion affects how
  the query is executed, never whether it is valid

#### Scenario: Parsing requires no query engine

- **WHEN** a caller wishes to know whether a query string is valid
- **THEN** the answer is obtainable without a catalog connection, a storage
  backend, a tenant context, or a running querier

### Requirement: Unsupported syntax is distinguishable from invalid syntax

A front-end SHALL report, separately and unambiguously, whether a rejected query
is malformed with respect to the language, or is well-formed in the language but
outside the subset SignalDB implements. Callers SHALL be able to act on that
distinction without inspecting message text.

#### Scenario: Malformed input is reported as invalid

- **WHEN** a client submits a query that is not well-formed in the language
- **THEN** it is rejected as invalid input, and the compatibility API answers
  with a client-error status

#### Scenario: Well-formed but unimplemented syntax is reported as unsupported

- **WHEN** a client submits a query that is valid in the language but uses a
  construct SignalDB does not implement
- **THEN** it is rejected as unsupported, distinctly from malformed input, and
  the compatibility API answers with a not-implemented status rather than a
  client error

#### Scenario: Unsupported constructs are never silently ignored

- **WHEN** a query contains a construct outside the implemented subset that
  would narrow the result set
- **THEN** the query is rejected, and never executed with the construct dropped
  or the results returned unfiltered

### Requirement: The implemented TraceQL subset is a stated contract

SignalDB SHALL accept a documented subset of TraceQL for trace search and
SHALL reject everything outside it explicitly. The subset SHALL cover a single
spanset of `&&`-conjoined equality matchers over span-name, status, kind, and
service-name intrinsics, and over span-scoped, resource-scoped, and unscoped
attributes. An empty spanset SHALL select all traces.

#### Scenario: A conjunction of equality matchers filters traces

- **WHEN** a client searches with a single spanset conjoining equality matchers
  over intrinsics and attributes
- **THEN** the search returns only traces matching every matcher

#### Scenario: An empty spanset matches everything

- **WHEN** a client searches with an empty spanset
- **THEN** the search applies no query-derived filter and returns traces within
  the requested time range

#### Scenario: Operators outside the subset are rejected as unsupported

- **WHEN** a client uses a comparison, regex, negation, disjunction, or
  duration matcher outside the implemented subset
- **THEN** the query is rejected as unsupported, naming the construct, rather
  than being approximated or ignored

#### Scenario: Attribute scope is preserved through parsing

- **WHEN** a client scopes a matcher to span attributes, to resource
  attributes, or leaves it unscoped
- **THEN** the parsed result carries that scope distinctly, so execution can
  search the span attributes, the resource attributes, or either

### Requirement: Compatibility-API behaviour is unchanged by front-end extraction

Moving a query language into a standalone front-end SHALL NOT change what the
compatibility APIs accept, what they reject, the reason given for a rejection,
or the status code returned.

#### Scenario: An accepted query stays accepted

- **WHEN** a query that the trace-search API accepted before extraction is
  submitted after it
- **THEN** it is accepted and returns the same results

#### Scenario: A rejected query stays rejected identically

- **WHEN** a query that the trace-search API rejected before extraction is
  submitted after it
- **THEN** it is rejected for the same reason, with the same status code

### Requirement: Published front-ends carry a purity and stability contract

A compatibility query-language front-end that SignalDB publishes as a reusable
artifact SHALL depend on no SignalDB product component, and SHALL NOT require
one to build or to run. Its published form SHALL be verified automatically
rather than by review, and its public types SHALL be declared extensible so
that growing the implemented subset does not force a breaking release.

#### Scenario: A product dependency fails the build pipeline

- **WHEN** a change adds a dependency on a SignalDB product component to a
  published front-end
- **THEN** the pipeline fails before the change can merge, rather than the
  regression being caught in review or at release time

#### Scenario: Extending the accepted subset is not a breaking release

- **WHEN** the implemented subset of a language grows to accept a construct it
  previously rejected
- **THEN** consumers of the published front-end continue to compile without
  source changes

#### Scenario: Publication metadata is verified before release

- **WHEN** a published front-end is missing metadata required to publish it
- **THEN** the gap is reported by continuous integration on the change that
  introduced it, not during a release

### Requirement: A compatibility artifact carries the licence of what it re-implements

Any SignalDB component whose purpose is to re-implement another project's query
language or HTTP API SHALL be distributed under that project's licence.
Components that are SignalDB's own — its services, its shared internals, and its
native query surface — SHALL NOT be governed by this rule and keep the project's
licence.

#### Scenario: A front-end matches its upstream language

- **WHEN** a front-end implements a query language defined by a copyleft-licensed
  project
- **THEN** the front-end is distributed under that same copyleft licence, and is
  not relicensed permissively to widen adoption

#### Scenario: A first-party surface is unaffected

- **WHEN** a component is SignalDB's own design rather than a re-implementation
  of an external language or API
- **THEN** it keeps the project's licence, regardless of which compatibility
  components it sits alongside

### Requirement: Front-ends release independently of the product

A published query-language front-end SHALL be versioned and released on its own
cadence, so that a fix to it can be released without releasing the product, and
a product release does not force a version change on it.

#### Scenario: A parser fix releases on its own

- **WHEN** the only change since the last release is to a query-language
  front-end
- **THEN** a release of that front-end can be cut and published without a
  release of the services, images, or binaries

#### Scenario: A product release does not disturb the front-ends

- **WHEN** the services are released
- **THEN** the front-ends' versions are unchanged and no republication of them
  occurs
