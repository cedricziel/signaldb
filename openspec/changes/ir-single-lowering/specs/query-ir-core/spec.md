## MODIFIED Requirements

### Requirement: One lowering serves every query surface

The querier SHALL lower a query onto its execution engine through a single
implementation, whichever surface the query arrived on. A query expressed as
TraceQL, as LogQL, or as an IR document SHALL resolve fields, address attribute
containers, and evaluate absent values identically, because the same code
performs all three.

Guarantees the IR already states — promotion-invariant field resolution,
three-valued absent semantics, rejection of physical names — SHALL therefore
hold for compatibility queries without being separately implemented for them.

#### Scenario: The same filter behaves identically across surfaces

- **WHEN** the same logical condition is expressed as a TraceQL matcher, as a
  LogQL label matcher, and as an IR predicate
- **THEN** each selects the same records, including for an attribute that is
  promoted to a column on one table and not on another

#### Scenario: A resolution fix reaches every surface at once

- **WHEN** field resolution is corrected — a quoting rule, a container
  fallback, an absent-value case
- **THEN** the correction applies to compatibility queries and IR documents
  together, with no second implementation left to update

#### Scenario: Physical names stay unaddressable from a compatibility surface

- **WHEN** a compatibility query names a physical column rather than a logical
  field
- **THEN** it is rejected on the same basis as an IR document naming it, rather
  than resolving because a different lowering was more permissive

### Requirement: Rerouting a compatibility surface preserves its behaviour

Moving a compatibility API onto the shared lowering SHALL NOT change which
queries it accepts, which it rejects, the reason or status of a rejection, or
the records a query returns. The change SHALL be verifiable by comparison
against the previous lowering rather than by assertion.

#### Scenario: Equivalence is demonstrated before a surface moves

- **WHEN** a compatibility surface is proposed for rerouting
- **THEN** a corpus of queries has been run through both lowerings and their
  execution plans compared, and every difference resolved, before the surface
  is switched

#### Scenario: A construct the shared lowering cannot express keeps working

- **WHEN** a query uses a construct the shared lowering does not yet cover, and
  the previous lowering did
- **THEN** the query continues to be served as before, rather than becoming a
  rejection — a working query never regresses into an error as a result of
  this rerouting

#### Scenario: A rerouted surface can be returned to the previous lowering

- **WHEN** a rerouted surface is suspected of behaving differently in
  production
- **THEN** it can be returned to the previous lowering without redeploying,
  for as long as both exist

#### Scenario: The previous lowering is removed once it is redundant

- **WHEN** a surface has run on the shared lowering with its equivalence
  evidence green
- **THEN** the superseded implementation and the mechanism for choosing between
  them are both deleted, leaving no second code path and no dormant switch
