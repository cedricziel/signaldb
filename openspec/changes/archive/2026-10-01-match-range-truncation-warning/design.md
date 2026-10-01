# Design

## Context

See proposal.md (Why). The facts below come from the code:

- `structural_match::lower` runs over the **windowed** traces scan. The window
  filter is `start_time_unix_nano BETWEEN window.start_ns AND window.end_ns`,
  inclusive on both ends (`ir_planner.rs`). It prunes to candidate traces,
  meaning traces where every span-set has some in-window span, and feeds
  `StructuralMatchExec`. The exec buffers one trace at a time, ordered by
  `(trace_id, start time)`.
- `Evaluator::finish_trace` already concatenates the trace's `span_id` and
  `parent_span_id` columns, and `evaluate` builds the `span_id → node` index.
  For each node it then resolves `parent_ids_of[i]` against that index. A
  parent id that is `Some` but not in the index is exactly a **dangling
  parent**, and today that result is thrown away (`and_then(|p|
index.get(p))`).
- The span and byte bounds (`match_max_trace_spans`/`_bytes`) fail the whole
  query with 422. Range truncation must not fail the query: a range cut across
  a trace is a normal consequence of querying a time window.
- Warnings travel querier → router in a data-free Flight trailer message
  (`common::flight::correlate_report_trailer`, prefix `correlate_report:`, JSON
  body `CorrelateReport`). The router's `execute_ticket` parses it, and
  `correlate_warnings` maps it to `QueryWarning`s. Today the report
  carries `correlate` outcomes only, and `CorrelateReport` is `Copy`.
- Plan-time state that only streaming execution can set is shared through
  `Arc<Atomic*>` handles. `CorrelateOutcome.truncated` is the pattern: it is
  flipped by `CorrelateCapExec` and read after `df.collect()` in
  `IrService::query`.
- `QueryWarning` is `{code, message, field?, suggestions[]}`. The UI's
  `QueryView` and the CLI's JSON output already show every warning without
  knowing its code.

## Goals / Non-Goals

**Goals:**

- Report every evaluated trace whose structure the range visibly cut, at O(1)
  extra cost per span. Use only data the evaluator already buffers, plus one
  timestamp column.
- Keep the result exactly as it is today. The warning reports on the result
  and never changes it.
- Compatibility in either rollout order between router and querier.

**Non-Goals:**

- **Proving completeness.** A trace that never reached the evaluator, because
  one of its span-sets had no in-range span and the candidate prune dropped it,
  is not counted. Detecting that needs a second scan outside the range. The
  docs say plainly that a missing warning does not prove completeness.
- Telling "parent started before `range.from`" apart from "parent was never
  ingested" (sampling, lost export). Both break the hierarchy the evaluator
  saw, so both can change a structural answer. Separating them costs a lookup
  outside the range per dangling id, with no bound on how far back.
- Auto-widening the range, or fetching the missing spans.
- Warnings on relation-free `match` stages. With no relation, structure does
  not enter the answer: such a stage is a per-trace existence filter, and
  "only in-range spans are returned" is ordinary range semantics.

## Decisions

### D1: Fire on a dangling parent or a span open past `range.to`

**Why these two conditions:** under the window filter, a span is invisible
exactly when its start is before `range.from` or after `range.to`. With sane
clocks, a parent starts no later than its child. Each kind of invisible span
then leaves a mark inside the window:

| Hidden relative                     | Visible mark in the buffered trace                                                   |
| ----------------------------------- | ------------------------------------------------------------------------------------ |
| ancestor that started before `from` | its in-window child has a **dangling parent**                                        |
| descendant that started after `to`  | its in-window ancestor **ends after `to`**                                           |
| sibling that started after `to`     | their shared parent ends after `to`, if buffered; otherwise the dangling-parent mark |

Both checks are exact against the buffered data, and neither needs new I/O.
The dangling-parent check reuses the index `evaluate` already builds. The
end-time check needs `end_time_unix_nano`, a required physical column. The
lowering keeps it in the exec's input. The DataFrame at the `match` stage is
the full windowed scan, and the projection runs after the stage.

**Alternatives considered:**

- _"Trace start/end touches the range boundary"_ (earliest buffered start
  within ε of `from`): this is a heuristic with no correct ε. A trace whose
  root starts 1 ms after `from` is complete. A trace whose root started an hour
  earlier can have its first in-window span anywhere. Rejected as imprecise.
- _Dangling parent only:_ this misses descendants that start after `to`,
  which is the common case for a `range.to = now` query over in-flight traces.
- _A second scan for each dangling parent id_ (to separate "outside the range"
  from "never ingested"): this is a precise attribution, but the scan has no
  bound on how far back it reads, and it doubles the work of every `match`
  query. Deferred. The message names both causes instead.
- _Fail the query, like the span/byte bounds:_ that would turn every
  `now-1h..now` dashboard over long traces into an error. Rejected.

### D2: Count only traces that reached the evaluator, split by outcome

Each finished trace that meets D1 increments one of two counters:

- `matched`: the trace produced witness rows. Its witness set may be partial,
  because a relation through a hidden span went unseen.
- `unmatched`: the trace was evaluated and failed a relation. It may have
  matched over a wider range, so it is a possible false negative.

Up to three trace ids are kept as examples. Matched traces come first, in
evaluation order. Counting happens when a trace is finished, so a `limit` that
ends the stream early only counts what was actually evaluated. That count is
still a lower bound and still accurate about what was returned.

### D3: Carry the counts in the existing trailer; rename the report type

`CorrelateReport` becomes `QueryReport` (a type alias keeps the old name for
one release), with a new optional member:

```jsonc
// app_metadata: "correlate_report:" + JSON (prefix unchanged on the wire)
{
  "rowLimit": false,
  "fanoutLimit": false,
  "window": null,
  "matchIncomplete": {
    "matched": 3,
    "unmatched": 1,
    "sampleTraceIds": ["5b8e…", "a1f0…"],
  },
}
```

The prefix stays `correlate_report:` so the trailer stays readable across
versions. A new querier talking to an old router: serde ignores the unknown
member, so the warning is missing but the result is intact. An old querier
talking to a new router: the member is absent, so there is no warning. Neither
pairing fails. `QueryReport` loses `Copy` because of the `Vec`; its call sites
already pass it by value or by reference.

The counters live on `StructuralMatchExec` as `Arc<AtomicU64>` plus a
`Mutex<Vec<String>>` (at most three entries). They are created in
`lower_match` and returned through the same outcome struct as
`correlate_truncated`. They are kept out of the node's `Spec`, so the derived
`Hash`/`PartialEq` impls are unaffected. `Spec` gains `window_end_ns: i64`.

### D4: Warning shape

```jsonc
{
  "code": "match_incomplete_trace",
  "message": "3 matched traces may be missing witness spans and 1 evaluated trace may have been missed: they have spans whose parent is not in the queried range (it started before the range or was not ingested) or that end after the range (their children may start after it). Widen `range` to see whole traces. Examples: 5b8e…, a1f0…",
}
```

There is no `field` and no `suggestions`. A count of zero leaves out its
clause. The code describes the outcome ("incomplete trace") rather than the
cause, because a dangling parent may come from a lost span instead of the
range (D1).

## Risks / Trade-offs

- [Lost spans in sampled or lossy pipelines raise the warning for in-range
  traces] → The message names that cause. The warning only appears when the
  document has a relation, which is when a broken hierarchy can change the
  answer. Clients branch on `code`.
- [Clock skew: a child that starts before its parent, or a parent that ends
  before its child] → A skewed parent outside the range still leaves a
  dangling-parent mark. A skewed descendant after `to` under a parent that
  ended before `to` goes undetected. This is accepted and documented as part
  of "a missing warning does not prove completeness".
- [Extra column kept through the prune join] → `end_time_unix_nano` is 8
  bytes per span. It counts toward `match_max_trace_bytes` like every other
  buffered column, and the docs table for that bound already says "value bytes
  of one trace's buffered rows".

## Migration Plan

One PR. Additive, with no configuration. Either service can be deployed first
(D3). To roll back, revert the commit. Nothing is persisted.
