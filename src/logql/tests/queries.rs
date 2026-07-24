//! End-to-end LogQL parsing tests over realistic queries.
//!
//! These exercise the public [`logql::parse`] entry point the router and
//! querier use, asserting the top-level shape (log vs metric) and key
//! structural details rather than the full AST — the crate's unit tests
//! already pin the field-level behaviour.

use logql::{
    AggregationFunction, BinOp, Expr, LineFilterOp, MetricQuery, PipelineStage, RangeFunction,
    parse, parse_query, parse_selector,
};
use std::time::Duration;

/// Every query the Grafana Loki datasource commonly emits should parse.
#[test]
fn representative_queries_parse() {
    let queries = [
        r#"{job="api"}"#,
        r#"{service_name="frontend", level="error"}"#,
        r#"{namespace=~"prod-.*"}"#,
        r#"{job!="test", env!~"dev.*"}"#,
        r#"{job="api"} |= "error""#,
        r#"{job="api"} |= "error" != "timeout""#,
        r#"{job="api"} |~ "error|warning""#,
        r#"{job="api"} | json | level="error""#,
        r#"{job="api"} | logfmt | duration > 1s"#,
        r#"{job="api"} | pattern "<method> <path> <status>""#,
        r#"{job="api"} | json status="response.status" | status >= 500"#,
        r#"{app="x"} | line_format "{{.msg}}""#,
        r#"{app="x"} | label_format out="{{.a}}", renamed=orig"#,
        r#"{app="x"} | drop level, source"#,
        r#"rate({job="api"}[5m])"#,
        r#"count_over_time({job="api"} |= "error" [1h])"#,
        r#"sum by (level) (rate({job="api"}[5m]))"#,
        r#"topk(10, sum by (path) (rate({job="api"}[5m])))"#,
        r#"avg_over_time({job="api"} | json | unwrap duration [5m])"#,
        r#"quantile_over_time(0.99, {app="x"} | unwrap latency [5m])"#,
        r#"sum(rate({a="b"}[1m])) / sum(rate({a="c"}[1m]))"#,
        r#"rate({a="b"}[5m]) > bool 5"#,
    ];
    for q in queries {
        assert!(parse(q).is_ok(), "should parse: {q}");
    }
}

#[test]
fn dispatches_log_and_metric_queries() {
    assert!(matches!(parse(r#"{job="api"}"#).unwrap(), Expr::Log(_)));
    assert!(matches!(
        parse(r#"{job="api"} | json"#).unwrap(),
        Expr::Log(_)
    ));
    assert!(matches!(
        parse(r#"rate({job="api"}[5m])"#).unwrap(),
        Expr::Metric(_)
    ));
}

#[test]
fn full_log_pipeline_round_trips_structurally() {
    let query = parse_query(
        r#"{app="api", env="prod"} |= "error" != "healthcheck" | json | status >= 500 | line_format "{{.message}}""#,
    )
    .expect("parse");

    assert_eq!(query.selector.matchers.len(), 2);
    assert_eq!(query.pipeline.len(), 5);
    assert!(matches!(
        query.pipeline[0],
        PipelineStage::LineFilter(ref f) if f.op == LineFilterOp::Contains
    ));
    assert!(matches!(
        query.pipeline[1],
        PipelineStage::LineFilter(ref f) if f.op == LineFilterOp::NotContains
    ));
    assert!(matches!(query.pipeline[2], PipelineStage::Json(_)));
    assert!(matches!(query.pipeline[3], PipelineStage::LabelFilter(_)));
    assert!(matches!(query.pipeline[4], PipelineStage::LineFormat(_)));
}

#[test]
fn binary_metric_query_shape() {
    let Expr::Metric(MetricQuery::Binary(bin)) =
        parse(r#"sum(rate({a="b"}[1m])) / sum(rate({a="c"}[1m]))"#).unwrap()
    else {
        panic!("expected a binary metric query");
    };
    assert_eq!(bin.op, BinOp::Div);
    assert!(matches!(bin.left, MetricQuery::Vector(_)));
    assert!(matches!(bin.right, MetricQuery::Vector(_)));
}

#[test]
fn range_and_aggregation_functions_resolve() {
    let Expr::Metric(MetricQuery::Vector(v)) =
        parse(r#"sum by (level) (count_over_time({a="b"}[10m]))"#).unwrap()
    else {
        panic!("expected a vector aggregation");
    };
    assert_eq!(v.function, AggregationFunction::Sum);
    let MetricQuery::Range(r) = &v.inner else {
        panic!("expected range inner");
    };
    assert_eq!(r.function, RangeFunction::CountOverTime);
    assert_eq!(r.range, Duration::from_secs(600));
}

#[test]
fn selector_only_parse_matches_query_selector() {
    let selector = parse_selector(r#"{job="api", level="error"}"#).expect("parse selector");
    let query = parse_query(r#"{job="api", level="error"}"#).expect("parse query");
    assert_eq!(selector, query.selector);
}

#[test]
fn malformed_queries_are_rejected() {
    let bad = [
        r#"{job="api""#,            // unclosed selector
        r#"{job=}"#,                // missing value
        r#"rate({a="b"})"#,         // missing range window
        r#"sum(rate({a="b"}[5m])"#, // unclosed aggregation
        r#"{a="b"} | status =~ 5"#, // regex op against number
        r#"nonsense({a="b"}[5m])"#, // unknown function
    ];
    for q in bad {
        assert!(parse(q).is_err(), "should be rejected: {q}");
    }
}
