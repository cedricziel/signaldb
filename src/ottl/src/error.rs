//! Error types shared by the parser, compiler, and evaluator.

use thiserror::Error;

/// A syntax error produced by [`crate::parse`].
#[derive(Debug, Clone, Error, PartialEq)]
#[error("parse error at column {column}: {message} (near `{token}`)")]
pub struct ParseError {
    pub message: String,
    pub column: usize,
    pub token: String,
}

impl ParseError {
    pub(crate) fn from_pest(src: &str, err: pest::error::Error<crate::parser::Rule>) -> Self {
        let column = match err.line_col {
            pest::error::LineColLocation::Pos((_, col)) => col,
            pest::error::LineColLocation::Span((_, col), _) => col,
        };
        let token = match err.location {
            pest::error::InputLocation::Pos(pos) => {
                src.get(pos..).unwrap_or("").chars().take(20).collect()
            }
            pest::error::InputLocation::Span((start, end)) => {
                src.get(start..end).unwrap_or("").to_string()
            }
        };
        ParseError {
            message: err.variant.message().to_string(),
            column,
            token,
        }
    }
}

/// A single compile-time error, positioned to a statement (and, where known, a column).
#[derive(Debug, Clone, Error, PartialEq)]
#[error("statement {statement}: {message}")]
pub struct CompileError {
    pub statement: usize,
    pub column: Option<usize>,
    pub message: String,
}

/// A runtime error surfaced when [`crate::ErrorMode::Propagate`] is in effect.
#[derive(Debug, Clone, Error, PartialEq)]
#[error("statement {statement}: {message}")]
pub struct ApplyError {
    pub statement: usize,
    pub message: String,
}
