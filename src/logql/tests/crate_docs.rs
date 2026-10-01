//! Pins the examples in the crate-level documentation.
//!
//! The docs previously claimed parsing was a "later phase" and promised SQL
//! transpilation, long after the parser landed and while no transpiler was
//! ever planned. Nothing failed when that went stale, so it stayed wrong until
//! someone read it. These tests make the documented claims falsifiable.

use logql::{Expr, parse, parse_query, parse_selector};

/// The log-query example in the crate docs.
#[test]
fn documented_log_query_parses_as_a_log_query() {
    let q = r#"{service_name="api"} |= "error" | json | status >= 500"#;
    assert!(
        matches!(parse(q), Ok(Expr::Log(_))),
        "crate docs present this as a log query: {q}"
    );
    assert!(parse_query(q).is_ok(), "parse_query accepts it too");
}

/// The metric-query example in the crate docs.
#[test]
fn documented_metric_query_parses_as_a_metric_query() {
    let q = r#"sum by (service_name) (rate({service_name="api"} |= "error" [5m]))"#;
    assert!(
        matches!(parse(q), Ok(Expr::Metric(_))),
        "crate docs present this as a metric query: {q}"
    );
}

/// The docs say `parse_selector` is the narrower entry point for a bare
/// stream selector.
#[test]
fn parse_selector_takes_a_bare_stream_selector() {
    assert!(parse_selector(r#"{service_name="api"}"#).is_ok());
}

/// The docs promise 1-based line and column on both error types, so a caller
/// can underline the offending token.
#[test]
fn errors_report_one_based_positions() {
    let err = parse(r#"{service_name="api"} | | json"#)
        .expect_err("a doubled pipe is not a valid pipeline");
    let rendered = err.to_string();
    assert!(
        rendered.contains("line 1") && rendered.contains("column"),
        "error should name a 1-based position: {rendered}"
    );
}
