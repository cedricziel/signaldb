## Purpose

Defines cross-signal correlation for the Query IR: a `correlate` stage that joins
the current relation to another signal by a shared key, enabling queries no
single-signal dialect can express. Stub scope; the requirements below are the
headline guarantees, to be expanded (with the fan-out, time-window, key, and
join-kind semantics from the proposal) when picked up.

## ADDED Requirements

### Requirement: Cross-signal correlation stage

The IR SHALL provide a stage that correlates the current relation to another
signal by a shared key, producing correlated records of the target signal, and
accounting for cross-signal differences in how the key is encoded. Correlation
SHALL be bounded (per-side limits and a time-bounded target scan) so a
one-to-many relationship cannot produce an unbounded result, and SHALL support at
least inner, semi, anti, and left join kinds.

#### Scenario: Logs for selected traces

- **WHEN** a query selects a set of traces and correlates to `logs` on `trace_id`
- **THEN** the result contains the log records belonging to those traces, bounded
  by the documented fan-out and time-window rules, despite the trace and log
  stores encoding `trace_id` differently

#### Scenario: Correlation requires the key on both sides

- **WHEN** a query correlates two signals on a key absent from one side, or on a
  key dropped by a preceding aggregation
- **THEN** the query is rejected at validation time
