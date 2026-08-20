//! # LogQL Front-End
//!
//! Lexing, parsing, and syntactic validation for Grafana Loki's LogQL. This
//! crate recognises a query; it does not execute one. It knows nothing about
//! columns, catalogs, tenants, or storage, and depends on nothing but
//! `thiserror` — so the same query string parses identically whatever backend
//! it is aimed at, and validity can be checked without one at all.
//!
//! Published as **`logql-parser`**; the library is imported as `logql`.
//!
//! ## The two query forms
//!
//! A **log query** starts from a stream selector (`{service_name="api"}`),
//! optionally followed by a pipeline of line filters, label filters, parser
//! stages, and formatters:
//!
//! ```text
//! {service_name="api"} |= "error" | json | status >= 500
//! ```
//!
//! A **metric query** wraps one in a range or vector aggregation:
//!
//! ```text
//! sum by (service_name) (rate({service_name="api"} |= "error" [5m]))
//! ```
//!
//! [`parse`] accepts either and returns an [`Expr`] saying which it got;
//! [`parse_query`] and [`parse_selector`] are the narrower entry points.
//!
//! ## Errors carry positions
//!
//! [`LexError`] and [`ParseError`] both report 1-based line and column, so a
//! caller can underline the offending token rather than restating the query.
//!
//! ## Modules
//!
//! - [`token`]: token type produced by the lexer
//! - [`lexer`]: hand-rolled tokenizer with line/column error reporting
//! - [`ast`]: abstract syntax tree for log queries
//! - [`metric`]: abstract syntax tree for metric queries
//! - [`parser`]: recursive-descent parser over the token stream

pub mod ast;
pub mod lexer;
pub mod metric;
pub mod parser;
pub mod token;

pub use ast::{
    FilterOp, FilterValue, Grouping, LabelExtraction, LabelFilterExpr, LabelFilterPred,
    LabelFormat, LabelFormatValue, LabelMatcher, LabelPredicate, LineFilter, LineFilterOp,
    LogQuery, LogfmtStage, MatchOp, PipelineStage, StreamSelector, Unwrap,
};
pub use lexer::{LexError, tokenize};
pub use metric::{
    AggregationFunction, BinOp, BinaryExpr, LabelReplace, MetricQuery, RangeAggregation,
    RangeFunction, VectorAggregation, VectorMatch,
};
pub use parser::{Expr, ParseError, parse, parse_query, parse_selector};
pub use token::{SpannedToken, Token};
