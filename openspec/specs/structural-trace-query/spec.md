# structural-trace-query Specification

## Purpose

Defines structural trace matching over the native IR: a `match` stage that
relates named span-sets by hierarchy and returns matching traces, with
correctness guaranteed independent of the execution strategy chosen.

## Requirements

### Requirement: Structural span-set matching

For the `traces` source, the IR SHALL provide a `match` stage that defines named
span-sets by predicate and relates them by hierarchical structure — at minimum
direct child, descendant, ancestor, and sibling — returning the matching traces
or span-sets via a `trace` result envelope. Span predicates SHALL be able to
reference span-level fields including `events` and `links` from the logical
schema.

The stage SHALL evaluate each trace over the spans whose start time lies inside
the document's resolved `range`; spans outside it are not part of the evaluated
trace. When the stage declares at least one relation and any evaluated trace is
visibly incomplete inside the range, the response SHALL carry a
`match_incomplete_trace` warning, and the result rows SHALL be the same as
without the warning. An evaluated trace is visibly incomplete when either:

- one of its in-range spans has a non-empty, non-zero `parent_span_id` (an
  all-zero id marks a root) that no in-range
  span of the same trace carries, or
- one of its in-range spans ends after the range end.

The warning SHALL state how many matched traces may be missing witness spans and
how many evaluated, unmatched traces may have matched over a wider range. It
SHALL name up to three example trace ids. It SHALL name both possible causes:
a relative outside the range, or a span that was never ingested.

A trace that has no in-range span for some span-set cannot match and is not
evaluated. It SHALL be counted as unmatched when at least one of its in-range
spans satisfies some span-set predicate and either none of its in-range spans
has an empty or all-zero `parent_span_id`, or one of its in-range spans ends
after the range end. Counting it SHALL NOT buffer the trace, so the per-trace
bounds never fail a query on its account. Such a trace with an in-range root
and another span whose parent is missing SHALL NOT be counted. A trace whose
spans all have remote parents has no root at any range and is counted every
time. The absence of the warning SHALL NOT be presented as proof of
completeness.

#### Scenario: Descendant relationship matches at any depth

- **WHEN** a query matches a root span-set and a second span-set required to be a
  descendant of the root, and requests the matching traces
- **THEN** every trace containing both in that relationship is returned,
  regardless of how deep the descendant sits

#### Scenario: Match references events and links

- **WHEN** a span-set predicate references a span's `events` or `links`
- **THEN** those nested fields are matchable through the logical schema

#### Scenario: An ancestor before the range is reported, not silent

- **WHEN** a `match` with a `descendant` relation evaluates a trace whose root
  span starts before `range.from` while its in-range spans still reference
  that root as their parent
- **THEN** the response carries a `match_incomplete_trace` warning that counts
  the trace (as matched or unmatched, by its outcome) and names it as an
  example, and the result rows are the same as without the warning

#### Scenario: A trace whose span-set is cut off counts as unmatched

- **WHEN** a `match` with a `child` relation from a root span-set evaluates a
  range that starts after a trace's root, so the root span-set has no in-range
  span while the trace's in-range child references the root as its parent
- **THEN** the response carries a `match_incomplete_trace` warning that counts
  the trace as unmatched and names it as an example, and the result does not
  contain the trace

#### Scenario: A rooted trace missing a span-set is not counted

- **WHEN** a `match` with a relation reads a trace that lacks a span for one
  span-set, keeps its root in range, has another span whose parent is not in
  range, and has no span ending after `range.to`
- **THEN** the response carries no `match_incomplete_trace` warning for it

#### Scenario: A span open past the range end is reported

- **WHEN** a `match` with a relation evaluates a trace in which an in-range
  span ends after `range.to`
- **THEN** the response carries a `match_incomplete_trace` warning counting
  that trace

#### Scenario: A trace wholly inside the range raises no warning

- **WHEN** every evaluated trace has all of its spans' parents in range and no
  span ending after `range.to`
- **THEN** the response carries no `match_incomplete_trace` warning

#### Scenario: A relation-free match raises no warning

- **WHEN** a `match` stage declares span-sets but no relations, over traces
  that straddle the range
- **THEN** the response carries no `match_incomplete_trace` warning

#### Scenario: The warning never changes the result

- **WHEN** the same document is evaluated by a server that raises
  `match_incomplete_trace` and by one that predates it, over the same data
- **THEN** both return identical result rows; only the `warnings` array differs

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

### Requirement: A hard per-trace resource budget bounds evaluation

Because a single trace can contain an arbitrarily large number of spans — and a
single span can carry large attribute/event/link payloads — the per-trace
evaluator SHALL enforce a **mandatory finite byte budget** per evaluated trace (a
span-count cap MAY be configured additionally, but is not sufficient alone). The
byte budget SHALL be checked **before** adding a span's data to the per-trace
adjacency structure. A trace exceeding the budget SHALL produce an **explicit
outcome — a resource error that fails the query, or an explicitly-flagged partial
result — never a silent truncation or a false negative**, and SHALL identify the
offending trace. The materialized-ancestry strategy SHALL enforce the equivalent
aggregate per-trace budget **at write time**, with the same explicit-outcome rule
(no silently truncated ancestry).

#### Scenario: Oversized trace produces an explicit outcome, not OOM or a false negative

- **WHEN** a matched trace exceeds the mandatory per-trace byte budget
- **THEN** the query fails with an explicit resource error naming the trace (or
  returns an explicitly-flagged partial result) — never a silent truncation, a
  false negative, or an OOM

### Requirement: Structural matching is trace-only

The `match` stage SHALL be valid only on the `traces` source and SHALL be
rejected at validation time on any non-trace source.

#### Scenario: Structural match on a non-trace source is rejected

- **WHEN** a structural `match` stage is applied to a logs, metrics, or profiles
  source
- **THEN** the query is rejected at validation time
