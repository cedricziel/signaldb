//! # Compatibility query languages → SignalDB's query IR
//!
//! The parsers (`logql`, `traceql`) recognise their languages and stop there.
//! The querier plans an IR document. This crate is the piece between: it turns
//! a parsed AST into an [`query_ir::Document`].
//!
//! ## Why it is its own crate
//!
//! How LogQL maps onto *SignalDB's* IR is not part of LogQL — Loki has no such
//! IR, and the mapping is a decision this project makes. Putting it in the
//! parser would be the same mistake as putting column names there. It is also
//! what keeps the parsers publishable: a published crate cannot depend on an
//! unpublished path dependency, and `query-ir` is deliberately unpublished.
//!
//! ## What it enables
//!
//! A caller with this crate can turn query text into an executable document
//! without a query engine — so the SDK, the CLI, or a WASM build can construct
//! and preview a query, not merely syntax-check one. Inside the querier it
//! collapses two parallel lowerings (compat → DataFusion, IR → DataFusion)
//! into one.
//!
//! ## Status: TraceQL lowers; LogQL is blocked on the IR
//!
//! TraceQL's supported subset lowers completely — it is equality matchers over
//! a single spanset, which is exactly a `where` stage.
//!
//! LogQL does not, and the gap was measured rather than assumed. The IR
//! offers `count`/`sum`/`avg`/`min`/`max`/`quantile` aggregates with a `step`
//! bucket and `by` grouping, plus `topk`/`bottomk`/`order` stages. Against
//! LogQL's surface:
//!
//! | LogQL | Count | Maps to the IR today |
//! |-------|-------|----------------------|
//! | range functions | 15 | 7 — `count`/`sum`/`avg`/`min`/`max`/`quantile`/`bytes`_over_time |
//! | vector aggregations | 11 | 9 — all but `stddev`/`stdvar` |
//!
//! The eight unmapped range functions split into two kinds, and the
//! distinction decides how much work finishing this crate is:
//!
//! **Additive.** `stddev_over_time`, `stdvar_over_time`, `first_over_time`,
//! `last_over_time`, `absent_over_time` and the `stddev`/`stdvar` vector
//! aggregations need new `AggFn` variants and nothing else. No structural
//! change; a minor IR version.
//!
//! **Structural.** `rate`, `bytes_rate` and `rate_counter` are a per-bucket
//! count divided by the window width — and **the IR has no arithmetic at
//! all**: no stage computes over another stage's output. The same absence
//! blocks binary operations between series (`a / b`) and `label_replace`.
//! Closing it means giving the IR a way to express computation over aggregate
//! results, which is a design change to SignalDB's own query surface, not a
//! lowering detail.
//!
//! `rate` is the most-used LogQL metric function, so "most of LogQL" is not a
//! useful partial state. Until the IR can express arithmetic, this crate can
//! serve TraceQL and the log-query (non-metric) half of LogQL; the metric half
//! has to wait for that decision. See design D6 in the archived
//! `publishable-ql-crates` change.

mod traceql_lower;

pub use traceql_lower::traceql_to_ir;

/// Why a query could not be lowered.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum LowerError {
    /// The text is not the language. Carries the parser's own error so the
    /// caller keeps the malformed-versus-unimplemented distinction that
    /// decides an HTTP status.
    #[error("{0}")]
    Parse(#[from] traceql::ParseError),

    /// The query parses, but says something the IR cannot express. Distinct
    /// from a parse failure: the language is fine, our target is not rich
    /// enough yet.
    #[error("{0} has no query-IR equivalent")]
    Inexpressible(String),
}
