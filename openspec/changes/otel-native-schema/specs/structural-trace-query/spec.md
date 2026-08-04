## Purpose

Defines structural trace matching over the native IR: a `match` stage that
relates named span-sets by hierarchy and returns matching traces, with
correctness guaranteed independent of the execution strategy chosen.

## ADDED Requirements

### Requirement: Structural span-set matching

For the `traces` source, the IR SHALL provide a `match` stage that defines named
span-sets by predicate and relates them by hierarchical structure — at minimum
direct child, descendant, ancestor, and sibling — returning the matching traces
or span-sets via a `trace` result envelope. Span predicates SHALL be able to
reference span-level fields including `events` and `links` from the logical
schema.

#### Scenario: Descendant relationship matches at any depth

- **WHEN** a query matches a root span-set and a second span-set required to be a
  descendant of the root, and requests the matching traces
- **THEN** every trace containing both in that relationship is returned,
  regardless of how deep the descendant sits

#### Scenario: Match references events and links

- **WHEN** a span-set predicate references a span's `events` or `links`
- **THEN** those nested fields are matchable through the logical schema

### Requirement: Descendant correctness without a silent depth cutoff

Descendant matching SHALL be correct without a silent depth cutoff: a trace
containing a matching descendant at any depth SHALL be returned, or the
incompleteness SHALL be surfaced as an explicit error — never a silently truncated
result. The execution strategy SHALL be one that can meet this under bounded
memory: a per-trace evaluator (partition by `trace_id`, build the single trace's
adjacency in memory, compute closure) or materialized ancestry (an ancestor/path
column written at ingest). A whole-relation recursive expansion (recursive CTE)
SHALL NOT be relied upon, as it materializes an unbounded working set and fails
under memory pressure rather than completing.

#### Scenario: Deep descendant is matched or the query errors, never silently dropped

- **WHEN** a trace contains a matching descendant deeper than any internal limit
- **THEN** the trace is returned, or the query fails with an explicit
  resource/incompleteness error — the answer is never silently truncated

#### Scenario: Strategy is bounded-memory-capable

- **WHEN** the `match` stage is executed
- **THEN** it runs on a per-trace evaluator or materialized ancestry, bounded by a
  single trace's span count or a precomputed column — not on a whole-relation
  recursive expansion

### Requirement: Structural matching is trace-only

The `match` stage SHALL be valid only on the `traces` source and SHALL be
rejected at validation time on any non-trace source.

#### Scenario: Structural match on a non-trace source is rejected

- **WHEN** a structural `match` stage is applied to a logs, metrics, or profiles
  source
- **THEN** the query is rejected at validation time
