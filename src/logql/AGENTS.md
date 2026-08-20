# logql-parser

A **pure implementation of LogQL**. Published to crates.io and consumed by
people who have never heard of SignalDB.

## The one rule

This crate lexes, parses, and validates syntax. Nothing else.

It must never depend on a workspace crate, on Arrow/DataFusion/Parquet, or on
anything reached over a network. `thiserror` is the only dependency, and that
is not an accident to be relaxed — it is the property that lets a caller ask
"is this valid LogQL?" without a catalog, a tenant, or a running server.

Lowering a parsed query onto columns lives in `src/querier/src/query/logql.rs`
and `logql_metric.rs`. If a change here needs to know what a column is, it
belongs there instead.

`scripts/check-leaf-purity.sh` enforces this in CI. `cargo publish --dry-run`
does **not** — it accepts `datafusion = "54"` happily.

## Doctests on every public item

Every `pub` function, type, and variant should carry a `///` example that
compiles and runs. These are the crate's user-facing documentation on docs.rs,
and unlike prose they fail the build when they go stale.

`parse`, `parse_query`, `parse_selector`, `parse_metric_query`, and `tokenize`
carry runnable examples; `tests/crate_docs.rs` pins the crate-level ones. The
remaining `pub` types do not, which is the gap.

This matters more here than usual: the crate docs once claimed parsing was a
"later phase" and promised SQL transpilation, and stayed wrong for months
because nothing executed them.

## Coverage: 95%

Current: **94.01% regions / 97.70% lines**.

```
cargo llvm-cov -p logql-parser --summary-only
```

Lines are over target; regions are a point under. What is left is almost all
defensive branches in `parser.rs` — the "found X" and "found end of input"
halves of rejections that no realistic query reaches — plus eight `panic!` arms
inside the crate's own test helpers, which are the failure branch of an
assertion and cannot be covered at all.

Treat 95% as the line figure. Chasing the last region point buys tests of the
form "assert this unreachable branch is unreachable", which is not what the
target is for.

CI measures coverage workspace-wide and uploads to Codecov, but enforces no
per-crate floor, so this is held by whoever is editing, not by a gate.

## Public API stability

Pre-1.0 and published. The AST **enums** are `#[non_exhaustive]` so a new
construct is additive for consumers; the AST **structs** are deliberately not,
because constructing a query by hand is a legitimate use. Error types are
`#[non_exhaustive]`. Adding an enum variant means the querier's lowering needs
a matching arm — check `cargo build -p querier` before assuming it is additive.
