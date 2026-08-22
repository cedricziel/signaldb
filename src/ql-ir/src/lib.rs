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
//! ## Status
//!
//! **TraceQL** — the supported subset lowers completely. It is equality
//! matchers over one spanset, which is exactly a `where` stage, and TraceQL's
//! scopes survive as the IR's container qualifiers.
//!
//! **LogQL log queries** — stream selectors and line filters lower. Parser and
//! formatter stages (`| json`, `| line_format`) reshape rather than narrow, so
//! they contribute no predicate; expressing them needs the IR's `extract`
//! stage and is separate work.
//!
//! **LogQL metric queries** — `count_over_time`, `rate`, and the
//! `sum`/`avg`/`min`/`max`/`stddev`/`stdvar`/`first`/`last` `_over_time`
//! family lower to a stepped `aggregate`, with `by` grouping from a vector
//! aggregation. `rate` is a `count` carrying a `divisor` — which is why
//! `irVersion` 5 added one.
//!
//! ### What is still inexpressible
//!
//! Each of these is refused by name rather than lowered to something narrower,
//! because a partially-applied query returns more rows than asked for while
//! looking like a success:
//!
//! - **Cross-series arithmetic** (`sum(rate(a)) / sum(rate(b))`) and
//!   `label_replace`. These need computation *between* series; the IR computes
//!   within one aggregate. This is the real remaining gap, and closing it is a
//!   design question about SignalDB's query surface rather than a lowering
//!   detail.
//! - **`without` grouping.** The IR groups by naming fields; naming the
//!   complement of a label set requires knowing the set, which is not
//!   available until the data is read.
//! - **Vector aggregations that do not collapse.** The IR aggregates once and
//!   LogQL twice, so an outer function is only accepted when applying it to
//!   partial results reproduces the inner one over their union — `sum` of
//!   counts, `min` of minima. `avg` of counts is not a count.
//! - **`topk`/`bottomk` as vector aggregations**, `ip()` filters, `irate`,
//!   `absent_over_time`.
//! - **`unwrap` in a bare log query.** It *is* supported where it supplies a
//!   range aggregate's field, which is the only place LogQL gives it a
//!   meaning this crate can carry.
//!
//! Documents claim the lowest version that carries them: a query needing no v5
//! feature declares v1 and stays executable by an older server.

mod logql_lower;
mod traceql_lower;

/// An IR range from two literal bounds.
///
/// Passed through unresolved: the router resolves relative anchors such as
/// `now-1h` to one absolute window at the ticket boundary, and resolving them
/// here too would let the two disagree.
fn ir_range(from: &str, to: &str) -> query_ir::Range {
    query_ir::Range {
        from: serde_json::Value::String(from.to_string()),
        to: serde_json::Value::String(to.to_string()),
    }
}

pub use logql_lower::logql_to_ir;
pub use traceql_lower::{traceql_condition_to_predicate, traceql_to_ir};

/// Why a query could not be lowered.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum LowerError {
    /// The text is not TraceQL. Carries the parser's own error so the caller
    /// keeps the malformed-versus-unimplemented distinction that decides an
    /// HTTP status.
    #[error("{0}")]
    Parse(#[from] traceql::ParseError),

    /// The text is not LogQL. Carries the parser's error, which reports a
    /// 1-based position.
    #[error("{0}")]
    ParseLogql(#[from] logql::ParseError),

    /// The query parses, but says something the IR cannot express. Distinct
    /// from a parse failure: the language is fine, our target is not rich
    /// enough yet.
    #[error("{0} has no query-IR equivalent")]
    Inexpressible(String),
}
