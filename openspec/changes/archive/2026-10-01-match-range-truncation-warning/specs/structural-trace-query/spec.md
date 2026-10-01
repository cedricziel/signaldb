## MODIFIED Requirements

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

- one of its in-range spans has a non-empty `parent_span_id` that no in-range
  span of the same trace carries, or
- one of its in-range spans ends after the range end.

The warning SHALL state how many matched traces may be missing witness spans and
how many evaluated, unmatched traces may have matched over a wider range. It
SHALL name up to three example trace ids. It SHALL name both possible causes:
a relative outside the range, or a span that was never ingested. A trace that
was not evaluated because a span-set had no in-range span SHALL NOT be counted,
and the absence of the warning SHALL NOT be presented as proof of completeness.

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
