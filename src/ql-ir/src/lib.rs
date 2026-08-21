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
//! TraceQL's equality subset only. LogQL is next, and it is the one that will
//! establish whether the IR can express the compat surface at all: `rate`,
//! `irate`, `increase` and cross-series formulas have no IR equivalent today.

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
