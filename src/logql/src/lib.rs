//! # LogQL Front-End
//!
//! Lexing (and, in later phases, parsing and SQL transpilation) for
//! Loki's LogQL query language. LogQL queries start from a stream
//! selector (`{service_name="api"}`), optionally followed by a pipeline
//! of line filters and parser stages, or wrapped in range/vector
//! aggregations for metric queries.
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
    RangeFunction, VectorAggregation,
};
pub use parser::{Expr, ParseError, parse, parse_query, parse_selector};
pub use token::{SpannedToken, Token};
