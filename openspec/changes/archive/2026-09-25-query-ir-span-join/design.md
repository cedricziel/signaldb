## Context

The Query IR (`src/query-ir`) validates a versioned document into a typed relation and the querier lowers it to a DataFusion plan (`src/querier/src/query/ir_planner.rs`). IR version 7 is the current maximum. The traces table carries `trace_id`, `span_id`, `parent_span_id` (all `Utf8`), `service_name` and `span_kind` as top-level columns. There is no join in the IR today; `query-cross-signal-correlate` reserves the `correlate` stage name but is an undesigned stub. See proposal.md for motivation.

## Goals / Non-Goals

**Goals:**
- A self-join of `traces` from span to parent, usable by the `service-map` change's `graph` envelope and directly by clients.
- A stage shape that the cross-signal change can widen (new targets and keys) without breaking version 8 documents.

**Non-Goals:**
- Joins to another signal, `semi`/`anti` join kinds, joins on arbitrary keys: all left to `query-cross-signal-correlate`.
- Multi-hop ancestry (grandparent, full path). One hop is enough for call edges.
- Precomputed edge tables or ingest-time changes.

## Decisions

**Stage shape: `{"correlate": {"to": "parent", "kind": "inner"|"left"}}`.** `to` is a closed enum with one value today. The cross-signal change adds values (for example a signal target with a key) without changing what `"parent"` means. Rejected: a generic `{on: [...], target: ...}` form now, because it would force answering the cross-signal problems (encodings, fan-out) before shipping the simple case.

**Fixed `parent.` prefix for the right side.** Deterministic, no alias parameter, and it reads naturally in `aggregate.by`. The field resolver treats `parent.` as a scope that recurses into the ordinary traces field resolution, so attribute scopes and logical fields work unchanged on both sides. Rejected: user-chosen aliases (more surface, no current need) and suffixing (`service_name_1`, unreadable).

**Lowering: hash join on `(trace_id, parent_span_id) = (trace_id, span_id)`.** Both sides are scans of the same table with the same time-range filter, pushed down before the join, so predicate and bloom-filter pruning stay intact. Keys share `Utf8` encoding, so no cast wraps either side. `where` predicates that touch only one side are pushed below the join by DataFusion's optimizer; the planner does not need its own pushdown. Arrow and Parquet types come from the DataFusion re-exports, as everywhere else in the querier.

**Window semantics: both sides bounded by the outer range.** Simple and predictable; a parent outside the window is "missing". Extending the parent side's window (for example by a padding interval) was considered and rejected for now: it makes results depend on a hidden constant, and for service-to-service edges the loss is limited to calls that straddle the window start.

**Fan-out bound.** A span has at most one parent, so the join cannot multiply rows beyond the child side, except for duplicate `span_id`s (retries, bad instrumentation). The bound is a plain row limit on the join output (`[querier].correlate_max_rows`, default 5,000,000), applied with a limit node and reported through the existing warnings list.

**Versioning.** Bump `MAX_IR_VERSION` to 8 and register `correlate` as a version-8 feature in the operator registry, following how v5-v7 stages were introduced.

**No wire or storage change.** Query-time only; Flight v1/v2 schemas, WAL and Iceberg layout are untouched, so there is nothing to migrate or roll back beyond the binary.

## Risks / Trade-offs

- [Self-join over a wide window is expensive] → Time-range pushdown on both sides, the row bound, and the existing query rate limits. Service-map callers aggregate right after the join, so output stays small.
- [Parents outside the window drop edges at the window's start] → Documented; `left` join makes the loss visible as null parents.
- [Duplicate span IDs inflate counts] → The row bound caps damage; counts are documented as "joined rows", same as other span counts.
- [Stage shape constrains the cross-signal design] → `to` is an enum precisely so the cross-signal change can add targets; its stub proposal is updated to build on this.
