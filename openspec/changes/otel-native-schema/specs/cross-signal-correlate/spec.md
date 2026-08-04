## Purpose

Defines cross-signal correlation over the native IR: a `correlate` stage that
joins the current relation to another signal by a shared logical key — the query
no single-signal dialect can express — with bounded fan-out, time-window
scoping, and a defined join-kind taxonomy.

## ADDED Requirements

### Requirement: Cross-signal correlation stage

The IR SHALL provide a `correlate` stage that joins the current relation to
another signal by a shared logical key, producing correlated records of the
target signal. The stage SHALL account for cross-signal differences in how the
key is physically encoded, resolving both sides against the one logical key from
`otel-native-logical-schema` rather than requiring the client to reconcile
encodings.

#### Scenario: Logs for selected traces despite differing encodings

- **WHEN** a query selects a set of traces and correlates to `logs` on `trace_id`,
  where the two stores encode `trace_id` differently
- **THEN** the result contains the log records belonging to those traces, with the
  encoding difference resolved through the logical key

#### Scenario: Correlation on exemplar and resource-identity keys

- **WHEN** a query correlates metrics to traces via exemplars, or correlates two
  signals lacking `trace_id` via resource-identity (service plus resource
  attributes)
- **THEN** the correlation uses those first-class logical join keys

### Requirement: Correlation is bounded in fan-out and time, without changing join truth

Correlation SHALL be bounded so no join kind can perform unbounded work. A
**source-side bound** (a cap on source cardinality after the source pipeline) SHALL
apply to **every** join kind, including semi/anti; when the source or target bound
cannot be met, the query SHALL return an explicit resource error rather than run
unbounded. Separately, a **result fan-out cap** SHALL apply only to result-producing
join kinds (inner, left), paired with a deterministic ordering so a capped result
is reproducible, not an arbitrary subset; fan-out caps SHALL NOT be applied to
semi/anti joins (which produce at most one row per source and where a cap would
change the boolean answer — their boundedness comes from the source-side bound and
a target scan that short-circuits on first match). The target-signal scan SHALL be
bounded to a time window derived from the source rows; the window SHALL be
documented as part of the join's truth condition (absence/enrichment is _within the
window_), and for anti/left joins the operator SHALL be able to widen the window to
cover late-arriving data. Any applied bound SHALL be reported, not silent.

#### Scenario: Unbounded source is a resource error, for every join kind

- **WHEN** a correlation (of any kind, including semi/anti) has a source relation
  exceeding the source-side bound
- **THEN** the query returns an explicit resource error rather than performing
  unbounded work

#### Scenario: Enrichment fan-out cap is deterministic and reported

- **WHEN** an inner/left correlation source row matches more targets than the cap
- **THEN** the retained subset is chosen by the documented ordering and the applied
  cap is reported, not an arbitrary truncation

#### Scenario: Anti-join truth is window-scoped, not silently wrong

- **WHEN** a trace has a matching error log only outside the correlation time window
- **THEN** the anti-join result reflects the documented window semantics (the trace
  counts as "no match within window"), and the window is widenable to include
  late-arriving data rather than silently misreporting absence

#### Scenario: Wide-side pushdown only when the canonical key equals stored encoding

- **WHEN** the two signals encode the join key differently and the query
  canonicalizes it
- **THEN** wide-side scan pruning is obtained only if the canonical form equals the
  wide side's stored encoding; otherwise the correlation still executes correctly
  but without wide-side pushdown, rather than falsely claiming pruning

### Requirement: Correlation validates keys and join kinds

The `correlate` stage SHALL be rejected at validation time when the join key is
absent from either side or was dropped by a preceding aggregation, and SHALL
support at least inner, semi, anti, and left join kinds, with defined column
namespacing when both sides carry a same-named field.

#### Scenario: Missing or dropped key is rejected

- **WHEN** a query correlates on a key absent from one side, or on a key removed
  by a preceding aggregation's grouping set
- **THEN** the query is rejected at validation time

#### Scenario: Anti join finds absence

- **WHEN** a query correlates traces to error logs with an anti join
- **THEN** the result contains exactly the traces with no matching error log
