## Why

"The schema" for a signal exists in four independently hand-maintained
places today: `schemas.toml` (Iceberg physical layout), `flight/schema.rs`
(Arrow wire format for OTLP→Flight conversion), `LogicalSchema::core()`
(the query/registry layer every surface — TraceQL, query IR, MCP, HTTP —
resolves fields through), and the OTLP conversion code's own field-by-field
handling. Nothing keeps these in sync by construction, only by review
discipline, and that discipline has already failed: traces'
`dropped_attributes_count`/`dropped_events_count`/`dropped_links_count`
were registered as queryable in `LogicalSchema::core()` but never given a
physical column or a conversion-code read path, so filtering on them
silently always returned false (fixed by `iceberg-schema-evolution`); and
— bigger than originally scoped here, confirmed while implementing that
sibling change — **none of the five metrics representations (gauge, sum,
histogram, exponential histogram, summary) or profiles had a
`schemas.toml` entry backing their real physical table at all**, not just
`ExponentialHistogram`/`Summary`. `schemas.toml`'s `metrics_gauge`/
`metrics_sum`/`metrics_histogram` sections existed but were wired only to
the admin schema-introspection endpoint, disconnected from the tables
those signals actually write to.

## What Changes

**Scope corrected twice while implementing this change** (both findings
kept in `design.md`'s Decisions section for the full reasoning):

1. Generating the Flight wire schema from `schemas.toml` turned out not to
   be achievable as scoped — the wire and physical representations have
   diverged in ways a per-field rename/type string can't reconcile (traces
   needs pre-rename names and a different attribute type than physical;
   logs' wire fields don't correspond 1:1 to physical ones at all; metrics'
   wire format is one polymorphic table against five normalized physical
   tables). `flight/schema.rs` stays hand-written.
2. Generating `LogicalSchema::core()`'s physical-backed entries hits a
   smaller version of the same problem — most of its entries are
   query-ergonomics aliases (`name`/`span.name`, `duration`/`duration_nano`)
   that don't equal any real physical column name, so a name-keyed
   generator could only add parallel entries, not replace anything, for
   little safety gain beyond what the consistency check below already
   provides. `LogicalSchema::core()` stays hand-written too.

What remains, and ships in this change:

- Fold all five metrics representations (gauge, sum, histogram,
  exponential histogram, summary) and profiles into `schemas.toml`, so it
  actually is the physical source of truth for every built-in table, not
  just traces/logs. **Done — merged as #1237.**
- Add a test-level consistency check: every non-computed field a signal's
  resolved schema declares SHALL have a corresponding read and write path
  in that signal's conversion code, so a field declared but never
  populated (this change's motivating bug) fails a test instead of
  silently returning wrong query results.
- **Not implemented in this change**: operator-defined custom tables, and
  generated wire/logical schemas (see the scope corrections above) — both
  remain candidates for a future change if a concrete need or a real
  transform-primitive design emerges.

## Capabilities

### New Capabilities

- `table-schema-consistency`: guarantees that a signal's declared physical
  schema and its actual conversion-code behavior cannot silently disagree
  — a mismatch fails a test, not a silent wrong query result.

### Modified Capabilities

(none — existing signals' physical columns, wire formats, and query
results are unchanged; §1's metrics/profiles consolidation changed how
those tables' schemas are generated internally, not what they are, and
already shipped in #1237.)

## Impact

- **common**: `iceberg/schemas.rs` (all five metrics representations and
  profiles resolve from `schemas.toml` instead of hand-built `StructField`
  lists — done in #1237); new consistency tests in
  `flight::conversion::{conversion_traces,conversion_logs,conversion_metrics}`.
  `flight/schema.rs` and `schema/logical.rs` are out of scope (see the
  scope corrections above) and stay hand-written.
- **querier/router**: no change — nothing about `LogicalSchema` or the
  wire format is touched.
- Touches `schemas.toml` alongside the already-merged
  `iceberg-schema-evolution` change, but no longer shares a task with it —
  that change's evolution mechanism is scoped to traces/logs only and
  does not touch metrics/profiles.
- Not **BREAKING**: no wire format, Iceberg column, or query result
  changes for any existing signal — the metrics/profiles consolidation
  (#1237) was verified by golden-output tests; this change adds only new
  tests, no production code paths.
