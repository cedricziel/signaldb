//! PromQL-specific error types

// Error variants are designed for the translator and evaluator (Issues 4-7)
#![allow(dead_code)]

use std::fmt;

/// Errors that can occur during PromQL parsing and evaluation
#[derive(Debug)]
pub enum PromQLError {
    /// Error parsing the PromQL query syntax
    ParseError(String),
    /// Error during query evaluation/translation
    EvaluationError(String),
    /// Invalid time range specification
    InvalidTimeRange {
        start: i64,
        end: i64,
        reason: String,
    },
    /// Unsupported PromQL feature
    UnsupportedFeature(String),
    /// Invalid label matcher
    InvalidMatcher(String),
    /// Function not found or invalid arguments
    FunctionError { name: String, reason: String },
}

impl std::error::Error for PromQLError {}

impl fmt::Display for PromQLError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ParseError(msg) => write!(f, "PromQL parse error: {msg}"),
            Self::EvaluationError(msg) => write!(f, "PromQL evaluation error: {msg}"),
            Self::InvalidTimeRange { start, end, reason } => {
                write!(f, "Invalid time range [{start}, {end}]: {reason}")
            }
            Self::UnsupportedFeature(feature) => {
                write!(f, "Unsupported PromQL feature: {feature}")
            }
            Self::InvalidMatcher(msg) => write!(f, "Invalid label matcher: {msg}"),
            Self::FunctionError { name, reason } => {
                write!(f, "Function '{name}' error: {reason}")
            }
        }
    }
}

impl From<PromQLError> for super::super::error::QuerierError {
    fn from(err: PromQLError) -> Self {
        super::super::error::QuerierError::InvalidInput(err.to_string())
    }
}
