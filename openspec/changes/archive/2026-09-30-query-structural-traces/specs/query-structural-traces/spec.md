## Purpose

Defines structural trace matching for the Query IR: a `match` stage that relates
named span-sets by hierarchy and returns matching traces. Stub scope; the
execution strategy and TraceQL-parity coverage are open pending a prototype (see
proposal). The requirements below are the headline guarantees.

## ADDED Requirements

### Requirement: Structural span-set matching

For `traces`, the IR SHALL provide a stage that matches named span-sets by
predicate and relates them by hierarchical structure (at minimum direct child,
descendant, ancestor, sibling), returning the matching traces or span-sets via a
`trace` result envelope. Descendant matching SHALL be correct without a silent
depth cutoff — any performance strategy chosen SHALL NOT drop deep descendants
from the result.

#### Scenario: Descendant relationship matches at any depth

- **WHEN** a query matches a root span-set and a second span-set required to be a
  descendant of the root, and requests the matching traces
- **THEN** every trace containing both in that relationship is returned,
  regardless of how deep the descendant sits

#### Scenario: Structural matching is trace-only

- **WHEN** a structural-match stage is used on a non-trace source
- **THEN** the query is rejected at validation
