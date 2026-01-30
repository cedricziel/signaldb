//! PromQL types and conversions for SignalDB
//!
//! This module provides types for representing PromQL query results
//! and intermediate representations during query translation.

// These types are designed for the translator (Issue 4) and HTTP API (Issues 8-10)
#![allow(dead_code)]

use std::collections::HashMap;
use std::time::Duration;

/// Result of a PromQL instant query (vector)
#[derive(Debug, Clone)]
pub struct InstantVector {
    /// The samples in this vector
    pub samples: Vec<Sample>,
}

/// Result of a PromQL range query (matrix)
#[derive(Debug, Clone)]
pub struct RangeVector {
    /// The time series in this matrix
    pub series: Vec<Series>,
}

/// A single sample (metric + value at a point in time)
#[derive(Debug, Clone)]
pub struct Sample {
    /// Metric labels (including __name__)
    pub metric: Metric,
    /// The sample value
    pub value: f64,
    /// Timestamp in milliseconds since epoch
    pub timestamp: i64,
}

/// A time series (metric + multiple values over time)
#[derive(Debug, Clone)]
pub struct Series {
    /// Metric labels (including __name__)
    pub metric: Metric,
    /// Values with timestamps
    pub values: Vec<(i64, f64)>,
}

/// Metric labels (the label set identifying a time series)
#[derive(Debug, Clone, Default)]
pub struct Metric {
    /// Label name-value pairs
    pub labels: HashMap<String, String>,
}

impl Metric {
    /// Create a new metric with the given name
    pub fn new(name: &str) -> Self {
        let mut labels = HashMap::new();
        labels.insert("__name__".to_string(), name.to_string());
        Self { labels }
    }

    /// Add a label to this metric
    pub fn with_label(mut self, name: &str, value: &str) -> Self {
        self.labels.insert(name.to_string(), value.to_string());
        self
    }

    /// Get the metric name (__name__ label)
    pub fn name(&self) -> Option<&str> {
        self.labels.get("__name__").map(|s| s.as_str())
    }

    /// Get a label value by name
    pub fn get(&self, name: &str) -> Option<&str> {
        self.labels.get(name).map(|s| s.as_str())
    }
}

/// Parameters for an instant query
#[derive(Debug, Clone)]
pub struct InstantQueryParams {
    /// The PromQL query string
    pub query: String,
    /// Evaluation timestamp (milliseconds since epoch)
    /// If None, uses current time
    pub time: Option<i64>,
    /// Query timeout
    pub timeout: Option<Duration>,
    /// Tenant context for multi-tenancy
    pub tenant_slug: String,
    /// Dataset context
    pub dataset_slug: String,
}

/// Parameters for a range query
#[derive(Debug, Clone)]
pub struct RangeQueryParams {
    /// The PromQL query string
    pub query: String,
    /// Start timestamp (milliseconds since epoch)
    pub start: i64,
    /// End timestamp (milliseconds since epoch)
    pub end: i64,
    /// Query resolution step (milliseconds)
    pub step: i64,
    /// Query timeout
    pub timeout: Option<Duration>,
    /// Tenant context for multi-tenancy
    pub tenant_slug: String,
    /// Dataset context
    pub dataset_slug: String,
}

/// Label matcher types matching Prometheus semantics
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatcherOp {
    /// Exact string match (=)
    Equal,
    /// Not equal (!=)
    NotEqual,
    /// Regex match (=~)
    RegexMatch,
    /// Regex not match (!~)
    RegexNotMatch,
}

impl std::fmt::Display for MatcherOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Equal => write!(f, "="),
            Self::NotEqual => write!(f, "!="),
            Self::RegexMatch => write!(f, "=~"),
            Self::RegexNotMatch => write!(f, "!~"),
        }
    }
}

/// A single label matcher
#[derive(Debug, Clone)]
pub struct LabelMatcher {
    /// Label name
    pub name: String,
    /// Match operation
    pub op: MatcherOp,
    /// Value to match against
    pub value: String,
}

impl LabelMatcher {
    /// Create a new equality matcher
    pub fn equal(name: &str, value: &str) -> Self {
        Self {
            name: name.to_string(),
            op: MatcherOp::Equal,
            value: value.to_string(),
        }
    }

    /// Create a new not-equal matcher
    pub fn not_equal(name: &str, value: &str) -> Self {
        Self {
            name: name.to_string(),
            op: MatcherOp::NotEqual,
            value: value.to_string(),
        }
    }

    /// Create a new regex matcher
    pub fn regex_match(name: &str, pattern: &str) -> Self {
        Self {
            name: name.to_string(),
            op: MatcherOp::RegexMatch,
            value: pattern.to_string(),
        }
    }

    /// Create a new regex not-match matcher
    pub fn regex_not_match(name: &str, pattern: &str) -> Self {
        Self {
            name: name.to_string(),
            op: MatcherOp::RegexNotMatch,
            value: pattern.to_string(),
        }
    }
}

/// Aggregation operators supported by PromQL
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Stddev,
    Stdvar,
    TopK,
    BottomK,
    CountValues,
    Quantile,
    Group,
}

impl std::fmt::Display for AggregationOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sum => write!(f, "sum"),
            Self::Avg => write!(f, "avg"),
            Self::Min => write!(f, "min"),
            Self::Max => write!(f, "max"),
            Self::Count => write!(f, "count"),
            Self::Stddev => write!(f, "stddev"),
            Self::Stdvar => write!(f, "stdvar"),
            Self::TopK => write!(f, "topk"),
            Self::BottomK => write!(f, "bottomk"),
            Self::CountValues => write!(f, "count_values"),
            Self::Quantile => write!(f, "quantile"),
            Self::Group => write!(f, "group"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_creation() {
        let metric = Metric::new("http_requests_total")
            .with_label("job", "api")
            .with_label("method", "GET");

        assert_eq!(metric.name(), Some("http_requests_total"));
        assert_eq!(metric.get("job"), Some("api"));
        assert_eq!(metric.get("method"), Some("GET"));
        assert_eq!(metric.get("nonexistent"), None);
    }

    #[test]
    fn test_label_matcher_display() {
        assert_eq!(format!("{}", MatcherOp::Equal), "=");
        assert_eq!(format!("{}", MatcherOp::NotEqual), "!=");
        assert_eq!(format!("{}", MatcherOp::RegexMatch), "=~");
        assert_eq!(format!("{}", MatcherOp::RegexNotMatch), "!~");
    }

    #[test]
    fn test_aggregation_op_display() {
        assert_eq!(format!("{}", AggregationOp::Sum), "sum");
        assert_eq!(format!("{}", AggregationOp::TopK), "topk");
    }
}
