# traceql-parser

A **pure implementation of TraceQL**. Published to crates.io and consumed by
people who have never heard of SignalDB.

## The one rule

This crate lexes, parses, and validates syntax. Nothing else.

It must never depend on a workspace crate, on Arrow/DataFusion/Parquet, or on
anything reached over a network. `thiserror` is the only dependency, and that
is not an accident to be relaxed — it is the property that lets a caller ask
"is this valid TraceQL?" without a catalog, a tenant, or a running server.

Lowering a parsed query onto columns lives in
`src/querier/src/query/search_filter.rs`. If a change here needs to know what a
column is, it belongs there instead. Tempo's `tags` HTTP parameter deliberately
keeps its own selector vocabulary in the querier rather than calling in here:
the two agree today by coincidence, and sharing one function would let a new
TraceQL intrinsic silently redefine a frozen wire format.

`scripts/check-ql-purity.sh` enforces this in CI. `cargo publish --dry-run`
does **not** — it accepts `datafusion = "54"` happily.

## Doctests on every public item

Every `pub` function, type, and variant should carry a `///` example that
compiles and runs. These are the crate's user-facing documentation on docs.rs,
and unlike prose they fail the build when they go stale.

Current state: **0 doctests.** `tests/parse.rs` covers behaviour, but nothing
executes the examples a reader sees first.

## Coverage: 95%

Current: **95.53% regions / 95.28% lines** — at target, with no margin.

```
cargo llvm-cov -p traceql-parser --summary-only
```

`ast.rs` sits at 67% regions, which is small enough that one uncovered `Debug`
or comparison can move the total. CI measures coverage workspace-wide and
uploads to Codecov, but enforces no per-crate floor.

## The rejection contract

Two error classes, and the distinction is the point:

| Input                                                   | Variant                   | Caller maps to |
| ------------------------------------------------------- | ------------------------- | -------------- |
| not TraceQL (`notbraces`, `{ foo }`)                    | `ParseError::Syntax`      | HTTP 400       |
| valid TraceQL, unimplemented (`\|\|`, `!=`, `duration`) | `ParseError::Unsupported` | HTTP 501       |

Never widen the supported subset by silently accepting a construct — a
partially applied filter returns _more_ traces than asked for while still
looking like a successful search. Reject it and name it.

One documented exception: escaped string literals are legal TraceQL this lexer
cannot read, and stay `Syntax`/400 because they were already a client error
before extraction. Do not "fix" that to `Unsupported` without reading design D2
in the archived `publishable-ql-crates` change.

## Public API stability

Pre-1.0 and published. `Selector`, `FilterValue`, and `ParseError` are
`#[non_exhaustive]`; `Condition` is not, since a matcher is exactly a selector
and a value. Adding a `Selector` variant means the querier's `to_expr` needs a
matching arm — check `cargo build -p querier` before assuming it is additive.
