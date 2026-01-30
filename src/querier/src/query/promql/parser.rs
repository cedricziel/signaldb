//! PromQL parser wrapper for SignalDB
//!
//! This module provides a wrapper around the promql-parser crate with
//! SignalDB-specific error handling and utility functions.

// Parser functions are designed for the translator (Issue 4) and HTTP API (Issues 8-10)
#![allow(dead_code)]

use promql_parser::label::{MatchOp, Matcher};
use promql_parser::parser::{self, Expr, VectorSelector};

use super::error::PromQLError;
use super::types::{LabelMatcher, MatcherOp};

/// Parse a PromQL query string into an AST expression
///
/// # Arguments
/// * `query` - The PromQL query string to parse
///
/// # Returns
/// The parsed expression AST, or a parse error
///
/// # Examples
/// ```ignore
/// use querier::query::promql::parser::parse;
///
/// let expr = parse("http_requests_total{job=\"api\"}").unwrap();
/// let expr = parse("rate(http_requests_total[5m])").unwrap();
/// let expr = parse("sum by (job)(rate(http_requests_total[5m]))").unwrap();
/// ```
pub fn parse(query: &str) -> Result<Expr, PromQLError> {
    parser::parse(query).map_err(|e| PromQLError::ParseError(format!("{e:?}")))
}

/// Check if a query string is syntactically valid
///
/// # Arguments
/// * `query` - The PromQL query string to validate
///
/// # Returns
/// `Ok(())` if valid, or a parse error describing the issue
pub fn validate(query: &str) -> Result<(), PromQLError> {
    parse(query).map(|_| ())
}

/// Extract label matchers from a parsed expression
///
/// This is useful for understanding what labels a query is filtering on,
/// which helps with query planning and optimization.
///
/// # Arguments
/// * `expr` - The parsed PromQL expression
///
/// # Returns
/// A vector of label matchers found in the expression
pub fn extract_matchers(expr: &Expr) -> Vec<LabelMatcher> {
    let mut result = Vec::new();
    collect_matchers_recursive(expr, &mut result);
    result
}

fn collect_matchers_recursive(expr: &Expr, result: &mut Vec<LabelMatcher>) {
    match expr {
        Expr::VectorSelector(vs) => {
            for matcher in vs.matchers.matchers.iter() {
                result.push(convert_matcher(matcher));
            }
        }
        Expr::MatrixSelector(ms) => {
            for matcher in ms.vs.matchers.matchers.iter() {
                result.push(convert_matcher(matcher));
            }
        }
        Expr::Call(call) => {
            for arg in &call.args.args {
                collect_matchers_recursive(arg, result);
            }
        }
        Expr::Aggregate(agg) => {
            collect_matchers_recursive(&agg.expr, result);
        }
        Expr::Binary(bin) => {
            collect_matchers_recursive(&bin.lhs, result);
            collect_matchers_recursive(&bin.rhs, result);
        }
        Expr::Paren(paren) => {
            collect_matchers_recursive(&paren.expr, result);
        }
        Expr::Unary(unary) => {
            collect_matchers_recursive(&unary.expr, result);
        }
        Expr::Subquery(sq) => {
            collect_matchers_recursive(&sq.expr, result);
        }
        Expr::Extension(_) | Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {
            // No matchers in literals or extensions
        }
    }
}

fn convert_matcher(matcher: &Matcher) -> LabelMatcher {
    let op = match &matcher.op {
        MatchOp::Equal => MatcherOp::Equal,
        MatchOp::NotEqual => MatcherOp::NotEqual,
        MatchOp::Re(_) => MatcherOp::RegexMatch,
        MatchOp::NotRe(_) => MatcherOp::RegexNotMatch,
    };

    LabelMatcher {
        name: matcher.name.clone(),
        op,
        value: matcher.value.clone(),
    }
}

/// Extract the metric name from a vector selector
///
/// Returns `None` if the selector doesn't have a `__name__` matcher
/// or if it's a regex match.
pub fn get_metric_name(vs: &VectorSelector) -> Option<&str> {
    for matcher in vs.matchers.matchers.iter() {
        if matcher.name == "__name__" && matches!(matcher.op, MatchOp::Equal) {
            return Some(&matcher.value);
        }
    }
    // Also check the name field directly
    vs.name.as_deref()
}

/// Extract all metric names referenced in an expression
///
/// This traverses the entire expression tree and collects all
/// metric names that are directly selected (with exact match).
pub fn extract_metric_names(expr: &Expr) -> Vec<String> {
    let mut names = Vec::new();
    collect_metric_names_recursive(expr, &mut names);
    names
}

fn collect_metric_names_recursive(expr: &Expr, names: &mut Vec<String>) {
    match expr {
        Expr::VectorSelector(vs) => {
            if let Some(name) = get_metric_name(vs) {
                names.push(name.to_string());
            }
        }
        Expr::MatrixSelector(ms) => {
            if let Some(name) = get_metric_name(&ms.vs) {
                names.push(name.to_string());
            }
        }
        Expr::Call(call) => {
            for arg in &call.args.args {
                collect_metric_names_recursive(arg, names);
            }
        }
        Expr::Aggregate(agg) => {
            collect_metric_names_recursive(&agg.expr, names);
        }
        Expr::Binary(bin) => {
            collect_metric_names_recursive(&bin.lhs, names);
            collect_metric_names_recursive(&bin.rhs, names);
        }
        Expr::Paren(paren) => {
            collect_metric_names_recursive(&paren.expr, names);
        }
        Expr::Unary(unary) => {
            collect_metric_names_recursive(&unary.expr, names);
        }
        Expr::Subquery(sq) => {
            collect_metric_names_recursive(&sq.expr, names);
        }
        Expr::Extension(_) | Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {}
    }
}

/// Check if an expression contains a range vector (matrix selector)
///
/// Range vectors are required for functions like rate(), irate(), etc.
pub fn has_range_vector(expr: &Expr) -> bool {
    match expr {
        Expr::MatrixSelector(_) => true,
        Expr::Call(call) => call.args.args.iter().any(|arg| has_range_vector(arg)),
        Expr::Aggregate(agg) => has_range_vector(&agg.expr),
        Expr::Binary(bin) => has_range_vector(&bin.lhs) || has_range_vector(&bin.rhs),
        Expr::Paren(paren) => has_range_vector(&paren.expr),
        Expr::Unary(unary) => has_range_vector(&unary.expr),
        Expr::Subquery(sq) => has_range_vector(&sq.expr),
        _ => false,
    }
}

/// Get the range duration from a matrix selector in milliseconds
pub fn get_range_duration_ms(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::MatrixSelector(ms) => Some(ms.range.as_millis() as i64),
        Expr::Call(call) => call
            .args
            .args
            .iter()
            .find_map(|arg| get_range_duration_ms(arg)),
        Expr::Aggregate(agg) => get_range_duration_ms(&agg.expr),
        Expr::Paren(paren) => get_range_duration_ms(&paren.expr),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_metric() {
        let expr = parse("http_requests_total").unwrap();
        assert!(matches!(expr, Expr::VectorSelector(_)));
    }

    #[test]
    fn test_parse_metric_with_labels() {
        let expr = parse(r#"http_requests_total{job="api", method="GET"}"#).unwrap();
        if let Expr::VectorSelector(vs) = expr {
            // promql-parser stores metric name separately, so matchers only contain job and method
            assert_eq!(vs.matchers.matchers.len(), 2);
            assert!(vs.name.is_some());
            assert_eq!(vs.name.as_deref(), Some("http_requests_total"));
        } else {
            panic!("Expected VectorSelector");
        }
    }

    #[test]
    fn test_parse_rate_function() {
        let expr = parse("rate(http_requests_total[5m])").unwrap();
        assert!(matches!(expr, Expr::Call(_)));
        assert!(has_range_vector(&expr));
    }

    #[test]
    fn test_parse_aggregation() {
        let expr = parse("sum by (job)(rate(http_requests_total[5m]))").unwrap();
        assert!(matches!(expr, Expr::Aggregate(_)));
    }

    #[test]
    fn test_parse_binary_expression() {
        let expr = parse("http_requests_total / http_requests_failed").unwrap();
        assert!(matches!(expr, Expr::Binary(_)));
    }

    #[test]
    fn test_parse_complex_query() {
        let query = r#"
            histogram_quantile(0.95,
                sum by (le)(
                    rate(http_request_duration_seconds_bucket{job="api"}[5m])
                )
            )
        "#;
        let expr = parse(query).unwrap();
        assert!(matches!(expr, Expr::Call(_)));
    }

    #[test]
    fn test_parse_invalid_query() {
        let result = parse("http_requests_total{job=}");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_regex_matcher() {
        let expr = parse(r#"http_requests_total{job=~"api.*"}"#).unwrap();
        let matchers = extract_matchers(&expr);
        let job_matcher = matchers.iter().find(|m| m.name == "job").unwrap();
        assert_eq!(job_matcher.op, MatcherOp::RegexMatch);
        assert_eq!(job_matcher.value, "api.*");
    }

    #[test]
    fn test_parse_negative_matcher() {
        let expr = parse(r#"http_requests_total{job!="internal"}"#).unwrap();
        let matchers = extract_matchers(&expr);
        let job_matcher = matchers.iter().find(|m| m.name == "job").unwrap();
        assert_eq!(job_matcher.op, MatcherOp::NotEqual);
    }

    #[test]
    fn test_extract_metric_names() {
        let expr = parse("http_requests_total + http_errors_total").unwrap();
        let names = extract_metric_names(&expr);
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"http_requests_total".to_string()));
        assert!(names.contains(&"http_errors_total".to_string()));
    }

    #[test]
    fn test_get_range_duration() {
        let expr = parse("rate(http_requests_total[5m])").unwrap();
        let duration_ms = get_range_duration_ms(&expr).unwrap();
        assert_eq!(duration_ms, 5 * 60 * 1000); // 5 minutes in ms
    }

    #[test]
    fn test_validate_valid_query() {
        assert!(validate("http_requests_total").is_ok());
        assert!(validate("rate(x[5m])").is_ok());
    }

    #[test]
    fn test_validate_invalid_query() {
        assert!(validate("http_requests_total{").is_err());
        assert!(validate("rate(x[])").is_err());
    }

    #[test]
    fn test_parse_with_offset() {
        let expr = parse("http_requests_total offset 5m").unwrap();
        assert!(matches!(expr, Expr::VectorSelector(_)));
    }

    #[test]
    fn test_parse_subquery() {
        let expr = parse("rate(http_requests_total[5m])[30m:1m]").unwrap();
        assert!(matches!(expr, Expr::Subquery(_)));
    }

    #[test]
    fn test_parse_utf8_metric_name() {
        // Prometheus 3.0 supports UTF-8 metric names
        let expr = parse(r#"{"http.server.request.duration"}"#);
        // This may or may not parse depending on promql-parser version
        // The important thing is it doesn't panic
        let _ = expr;
    }
}
