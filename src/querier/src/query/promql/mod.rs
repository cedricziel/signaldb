//! PromQL query support for SignalDB
//!
//! This module provides PromQL (Prometheus Query Language) parsing and
//! evaluation capabilities, enabling Prometheus-compatible queries against
//! metrics stored in SignalDB.
//!
//! # Architecture
//!
//! The PromQL implementation follows this flow:
//!
//! ```text
//! PromQL String → Parser → AST (Expr) → Translator → DataFusion LogicalPlan → Execution
//! ```
//!
//! # Modules
//!
//! - [`parser`] - PromQL parsing using the promql-parser crate
//! - [`types`] - PromQL-specific types for query parameters and results
//! - [`error`] - Error types for PromQL operations
//!
//! # Example
//!
//! ```ignore
//! use querier::query::promql::{parser, types::InstantQueryParams};
//!
//! // Parse a PromQL query
//! let expr = parser::parse("rate(http_requests_total[5m])")?;
//!
//! // Extract information from the AST
//! let metric_names = parser::extract_metric_names(&expr);
//! let has_range = parser::has_range_vector(&expr);
//! ```
//!
//! # Supported Features
//!
//! ## Selectors
//! - Instant vector selectors: `metric_name{label="value"}`
//! - Range vector selectors: `metric_name[5m]`
//! - Label matchers: `=`, `!=`, `=~`, `!~`
//! - Offset modifier: `metric offset 5m`
//! - @ modifier: `metric @ 1609459200`
//!
//! ## Aggregations (planned)
//! - `sum`, `avg`, `min`, `max`, `count`
//! - `stddev`, `stdvar`
//! - `topk`, `bottomk`
//! - `count_values`, `quantile`
//! - Grouping with `by` and `without`
//!
//! ## Functions (planned)
//! - Rate functions: `rate`, `irate`, `increase`, `delta`, `idelta`
//! - Aggregation over time: `avg_over_time`, `sum_over_time`, etc.
//! - Math functions: `abs`, `ceil`, `floor`, `round`, `sqrt`, etc.
//! - Histogram: `histogram_quantile`
//!
//! # Compatibility
//!
//! This implementation targets compatibility with Prometheus 3.x, including
//! support for UTF-8 metric and label names as introduced in Prometheus 3.0.

pub mod error;
pub mod parser;
pub mod types;
