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

Two classes, and the compatibility APIs SHALL map them to fixed statuses:
malformed input to **HTTP 400**, an unimplemented construct to **HTTP 501**. The
exact codes are part of the contract, not an implementation detail — external
clients branch on them.

#### Scenario: Malformed input is reported as invalid

- **WHEN** a client submits a query that is not well-formed in the language
- **THEN** it is rejected as invalid input, and the compatibility API answers
  with HTTP 400

#### Scenario: Well-formed but unimplemented syntax is reported as unsupported

- **WHEN** a client submits a query that is valid in the language but uses a
  construct SignalDB does not implement
- **THEN** it is rejected as unsupported, distinctly from malformed input, and
  the compatibility API answers with HTTP 501 rather than 400

#### Scenario: A construct that is valid but unlexable stays a client error

- **WHEN** a construct is well-formed in the language but the front-end's lexer
  cannot read it, and that construct was already rejected as a client error
  before the front-end was extracted
- **THEN** it continues to be reported as invalid input rather than moving to
  the unimplemented class, because no rejection may become less specific
- **AND** the front-end documents the exception on the variant that carries it,
  so the deviation from the general rule is discoverable rather than surprising

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

- **WHEN** a client uses an ordering comparison, a regex match, a negation, a
  disjunction, or a duration matcher — equality being the only comparison the
  subset implements
- **THEN** the query is rejected as unsupported, naming the construct, rather
  than being approximated or ignored

#### Scenario: Attribute scope is preserved through parsing

- **WHEN** a client scopes a matcher to span attributes, to resource
  attributes, or leaves it unscoped
- **THEN** the parsed result carries that scope distinctly, so execution can
  search the span attributes, the resource attributes, or either

### Requirement: Extraction changes only the rejection class of unparseable queries

Moving a query language into a standalone front-end SHALL NOT change which
queries the compatibility APIs accept, the results they return, or the reason
text given for a rejection. The single permitted change is that input which
cannot be parsed as the language SHALL be reclassified from not-implemented to
client-error. No rejection SHALL move in the opposite direction.

#### Scenario: An accepted query stays accepted

- **WHEN** a query that the trace-search API accepted before extraction is
  submitted after it
- **THEN** it is accepted and returns the same results

#### Scenario: A valid-but-unimplemented query keeps its status

- **WHEN** a query using a construct that is well-formed in the language but
  unimplemented is submitted before and after extraction
- **THEN** it is rejected as unsupported both times, with the same reason text
  and the same not-implemented status

#### Scenario: An unparseable query is reclassified to a client error

- **WHEN** a query that cannot be parsed as the language was previously
  rejected as not-implemented
- **THEN** it is now rejected as invalid input with a client-error status, and
  the reason text is preserved

#### Scenario: No rejection becomes less specific

- **WHEN** any query is rejected as a client error before extraction
- **THEN** it is still rejected as a client error afterwards, never
  reclassified as not-implemented

### Requirement: Published front-ends carry a purity and stability contract

A compatibility query-language front-end that SignalDB publishes as a reusable
artifact SHALL depend on no SignalDB product component and on no component of
the query engine, and SHALL NOT require either to build or to run. Its
published form SHALL be verified automatically rather than by review. Its
public types — every type a consumer matches on or constructs, not only its
enumerations — SHALL either be declared extensible or be documented as fixed,
so that growing the implemented subset does not silently force a breaking
release on consumers.

#### Scenario: A product dependency fails the build pipeline

- **WHEN** a change adds a dependency on a SignalDB product component to a
  published front-end
- **THEN** the pipeline fails before the change can merge, rather than the
  regression being caught in review or at release time

#### Scenario: A query-engine dependency also fails

- **WHEN** a change adds a dependency on the query engine's own stack to a
  published front-end, even though that dependency is independently published
  and would package successfully
- **THEN** the pipeline fails, because packaging successfully is not the same
  as remaining independent of the engine

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
