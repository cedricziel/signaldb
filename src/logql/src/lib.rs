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

pub mod lexer;
pub mod token;

pub use lexer::{LexError, tokenize};
pub use token::{SpannedToken, Token};
