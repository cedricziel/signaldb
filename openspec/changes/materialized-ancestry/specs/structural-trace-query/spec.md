# structural-trace-query Delta Specification (sketch)

> Sketch only. This delta applies **if** the fast path is adopted; the
> exploration's recommendation (design.md) is not to adopt it now, in which
> case this file is dropped and the main spec is unchanged.

## MODIFIED Requirements

### Requirement: Descendant correctness without a silent depth cutoff

Descendant matching SHALL be correct without a silent depth cutoff: a trace
containing a matching descendant at any depth SHALL be returned, or the
incompleteness SHALL be surfaced as an explicit error — never a silently
truncated result. The execution strategy SHALL be one that can meet this under
bounded memory: a per-trace evaluator (partition by `trace_id`, build the
single trace's adjacency in memory, compute closure) or materialized ancestry
(an ancestor column computed ahead of the query). A whole-relation recursive
expansion (recursive CTE) SHALL NOT be relied upon, as it materializes an
unbounded working set and fails under memory pressure rather than completing.

Materialized ancestry SHALL be used only over spans whose ancestry is known
complete. A span whose ancestry is absent (not yet computed, unresolvable,
over the depth cap, or on a parent cycle) SHALL route its trace to the
per-trace evaluator. The two strategies SHALL return the same traces and the
same witness spans for the same query and data.

#### Scenario: Deep descendant is matched or the query errors, never silently dropped

- **WHEN** a trace contains a matching descendant deeper than any internal limit
- **THEN** the trace is returned, or the query fails with an explicit
  resource/incompleteness error — the answer is never silently truncated

#### Scenario: Strategy is bounded-memory-capable

- **WHEN** the `match` stage is executed
- **THEN** it runs on a per-trace evaluator or materialized ancestry, bounded by a
  single trace's span count or a precomputed column — not on a whole-relation
  recursive expansion

#### Scenario: Incomplete ancestry falls back to the evaluator

- **WHEN** a `descendant` query covers spans whose ancestry column is null
  (recent, uncompacted, or unresolvable data)
- **THEN** those spans' traces are evaluated by the per-trace evaluator and
  the result equals what the evaluator alone returns

#### Scenario: Strategies agree on duplicated spans and parent cycles

- **WHEN** a trace contains a redelivered span (two rows, one `span_id`) or a
  `parent_span_id` cycle
- **THEN** the fast path and the evaluator return the same traces and
  witnesses (a cycle's spans carry no materialized ancestry, so the evaluator
  answers for them)

### Requirement: A hard per-trace resource budget bounds evaluation

Because a single trace can contain an arbitrarily large number of spans — and a
single span can carry large attribute/event/link payloads — the per-trace
evaluator SHALL enforce a **mandatory finite byte budget** per evaluated trace (a
span-count cap MAY be configured additionally, but is not sufficient alone). The
byte budget SHALL be checked **before** adding a span's data to the per-trace
adjacency structure. A trace exceeding the budget SHALL produce an **explicit
outcome — a resource error that fails the query, or an explicitly-flagged partial
result — never a silent truncation or a false negative**, and SHALL identify the
offending trace. The materialized-ancestry strategy SHALL bound the ancestor
list per span by a configured depth cap at the time it is computed; a span
over the cap SHALL carry no ancestry (never a truncated list), so its trace
falls back to the evaluator and its budget.

#### Scenario: Oversized trace produces an explicit outcome, not OOM or a false negative

- **WHEN** a matched trace exceeds the mandatory per-trace byte budget
- **THEN** the query fails with an explicit resource error naming the trace (or
  returns an explicitly-flagged partial result) — never a silent truncation, a
  false negative, or an OOM

#### Scenario: A span deeper than the ancestry cap carries no ancestry

- **WHEN** ancestry is computed for a span whose depth exceeds the cap
- **THEN** its ancestor column is null, not a truncated prefix, and queries
  over its trace use the evaluator
