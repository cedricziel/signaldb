//! Expected-AST tests for representative LogQL queries.
//!
//! The corpus test (`corpus.rs`) proves queries parse without error; this
//! suite goes further and asserts the exact tree for one query per
//! construct, so structural regressions — mis-grouped precedence, dropped
//! stages, a matcher parsed as the wrong operator — are caught even when
//! the query still parses.

use logql::{
    AggregationFunction, BinOp, BinaryExpr, Expr, FilterOp, FilterValue, Grouping, LabelExtraction,
    LabelFilterExpr, LabelFilterPred, LabelFormat, LabelFormatValue, LabelMatcher, LabelPredicate,
    LineFilter, LineFilterOp, LogQuery, LogfmtStage, MatchOp, MetricQuery, PipelineStage,
    RangeAggregation, RangeFunction, StreamSelector, Unwrap, VectorAggregation, parse,
};
use std::time::Duration;

// --- small constructors to keep the expected trees readable ---

fn m(name: &str, op: MatchOp, value: &str) -> LabelMatcher {
    LabelMatcher {
        name: name.into(),
        op,
        value: value.into(),
    }
}

fn selector(matchers: Vec<LabelMatcher>) -> StreamSelector {
    StreamSelector { matchers }
}

fn log(selector: StreamSelector, pipeline: Vec<PipelineStage>) -> Expr {
    Expr::Log(LogQuery { selector, pipeline })
}

fn line(op: LineFilterOp, value: &str) -> PipelineStage {
    PipelineStage::LineFilter(LineFilter {
        op,
        value: value.into(),
        is_ip: false,
    })
}

fn pred(name: &str, op: FilterOp, value: FilterValue) -> LabelFilterExpr {
    LabelFilterExpr::Pred(LabelFilterPred {
        name: name.into(),
        op,
        value,
    })
}

fn range(
    function: RangeFunction,
    selector: StreamSelector,
    pipeline: Vec<PipelineStage>,
    range: Duration,
) -> MetricQuery {
    MetricQuery::Range(Box::new(RangeAggregation {
        function,
        log_query: LogQuery { selector, pipeline },
        range,
        offset: None,
        unwrap: None,
        param: None,
        grouping: None,
    }))
}

/// The heart of this suite: each case is `(query, expected AST)`.
fn cases() -> Vec<(&'static str, Expr)> {
    vec![
        // --- stream selectors ---
        (
            r#"{job="api"}"#,
            log(selector(vec![m("job", MatchOp::Eq, "api")]), vec![]),
        ),
        (
            r#"{job!="api", env=~"prod-.*", region!~"eu.*"}"#,
            log(
                selector(vec![
                    m("job", MatchOp::Neq, "api"),
                    m("env", MatchOp::Re, "prod-.*"),
                    m("region", MatchOp::Nre, "eu.*"),
                ]),
                vec![],
            ),
        ),
        // --- line filters (order + operators preserved) ---
        (
            r#"{a="b"} |= "keep" != "drop" |~ "re" !~ "nre""#,
            log(
                selector(vec![m("a", MatchOp::Eq, "b")]),
                vec![
                    line(LineFilterOp::Contains, "keep"),
                    line(LineFilterOp::NotContains, "drop"),
                    line(LineFilterOp::Regex, "re"),
                    line(LineFilterOp::NotRegex, "nre"),
                ],
            ),
        ),
        // --- ip line filter carries the is_ip flag ---
        (
            r#"{a="b"} |= ip("10.0.0.0/8")"#,
            log(
                selector(vec![m("a", MatchOp::Eq, "b")]),
                vec![PipelineStage::LineFilter(LineFilter {
                    op: LineFilterOp::Contains,
                    value: "10.0.0.0/8".into(),
                    is_ip: true,
                })],
            ),
        ),
        // --- json extraction expressions ---
        (
            r#"{a="b"} | json status="response.status", level"#,
            log(
                selector(vec![m("a", MatchOp::Eq, "b")]),
                vec![PipelineStage::Json(vec![
                    LabelExtraction {
                        name: "status".into(),
                        expr: Some("response.status".into()),
                    },
                    LabelExtraction {
                        name: "level".into(),
                        expr: None,
                    },
                ])],
            ),
        ),
        // --- logfmt flags ---
        (
            r#"{a="b"} | logfmt --strict --keep-empty host"#,
            log(
                selector(vec![m("a", MatchOp::Eq, "b")]),
                vec![PipelineStage::Logfmt(LogfmtStage {
                    flags: vec!["strict".into(), "keep-empty".into()],
                    extractions: vec![LabelExtraction {
                        name: "host".into(),
                        expr: None,
                    }],
                })],
            ),
        ),
        // --- label_format rename vs template ---
        (
            r#"{a="b"} | label_format out="{{.src}}", renamed=orig"#,
            log(
                selector(vec![m("a", MatchOp::Eq, "b")]),
                vec![PipelineStage::LabelFormat(vec![
                    LabelFormat {
                        dst: "out".into(),
                        value: LabelFormatValue::Template("{{.src}}".into()),
                    },
                    LabelFormat {
                        dst: "renamed".into(),
                        value: LabelFormatValue::Rename("orig".into()),
                    },
                ])],
            ),
        ),
        // --- drop with a bare label and a matcher ---
        (
            r#"{a="b"} | drop __error__, method="GET""#,
            log(
                selector(vec![m("a", MatchOp::Eq, "b")]),
                vec![PipelineStage::Drop(vec![
                    LabelPredicate {
                        name: "__error__".into(),
                        matcher: None,
                    },
                    LabelPredicate {
                        name: "method".into(),
                        matcher: Some((MatchOp::Eq, "GET".into())),
                    },
                ])],
            ),
        ),
        // --- label filter: comma is AND ---
        (
            r#"{a="b"} | logfmt | level="error", status>=500"#,
            log(
                selector(vec![m("a", MatchOp::Eq, "b")]),
                vec![
                    PipelineStage::Logfmt(LogfmtStage::default()),
                    PipelineStage::LabelFilter(LabelFilterExpr::And(
                        Box::new(pred(
                            "level",
                            FilterOp::Eq,
                            FilterValue::String("error".into()),
                        )),
                        Box::new(pred("status", FilterOp::Gte, FilterValue::Number(500.0))),
                    )),
                ],
            ),
        ),
        // --- label filter: bytes + duration values ---
        (
            r#"{a="b"} | logfmt | size > 20KB"#,
            log(
                selector(vec![m("a", MatchOp::Eq, "b")]),
                vec![
                    PipelineStage::Logfmt(LogfmtStage::default()),
                    PipelineStage::LabelFilter(pred(
                        "size",
                        FilterOp::Gt,
                        FilterValue::Bytes(20_000),
                    )),
                ],
            ),
        ),
        // --- label filter: and binds tighter than or ---
        (
            r#"{a="b"} | logfmt | x="1" and y="2" or z="3""#,
            log(
                selector(vec![m("a", MatchOp::Eq, "b")]),
                vec![
                    PipelineStage::Logfmt(LogfmtStage::default()),
                    PipelineStage::LabelFilter(LabelFilterExpr::Or(
                        Box::new(LabelFilterExpr::And(
                            Box::new(pred("x", FilterOp::Eq, FilterValue::String("1".into()))),
                            Box::new(pred("y", FilterOp::Eq, FilterValue::String("2".into()))),
                        )),
                        Box::new(pred("z", FilterOp::Eq, FilterValue::String("3".into()))),
                    )),
                ],
            ),
        ),
        // --- range aggregation over a line filter ---
        (
            r#"count_over_time({job="api"} |= "error" [5m])"#,
            Expr::Metric(range(
                RangeFunction::CountOverTime,
                selector(vec![m("job", MatchOp::Eq, "api")]),
                vec![line(LineFilterOp::Contains, "error")],
                Duration::from_secs(300),
            )),
        ),
        // --- unwrap is lifted out of the pipeline into the aggregation ---
        (
            r#"avg_over_time({a="b"} | json | unwrap duration [5m])"#,
            Expr::Metric(MetricQuery::Range(Box::new(RangeAggregation {
                function: RangeFunction::AvgOverTime,
                log_query: LogQuery {
                    selector: selector(vec![m("a", MatchOp::Eq, "b")]),
                    pipeline: vec![PipelineStage::Json(vec![])],
                },
                range: Duration::from_secs(300),
                offset: None,
                unwrap: Some(Unwrap {
                    label: "duration".into(),
                    conversion: None,
                }),
                param: None,
                grouping: None,
            }))),
        ),
        // --- vector aggregation with grouping wrapping a range agg ---
        (
            r#"sum by (level) (rate({job="api"}[1m]))"#,
            Expr::Metric(MetricQuery::Vector(Box::new(VectorAggregation {
                function: AggregationFunction::Sum,
                param: None,
                grouping: Some(Grouping {
                    without: false,
                    labels: vec!["level".into()],
                }),
                inner: range(
                    RangeFunction::Rate,
                    selector(vec![m("job", MatchOp::Eq, "api")]),
                    vec![],
                    Duration::from_secs(60),
                ),
            }))),
        ),
        // --- topk carries its scalar param ---
        (
            r#"topk(3, sum(rate({a="b"}[1m])))"#,
            Expr::Metric(MetricQuery::Vector(Box::new(VectorAggregation {
                function: AggregationFunction::Topk,
                param: Some(3.0),
                grouping: None,
                inner: MetricQuery::Vector(Box::new(VectorAggregation {
                    function: AggregationFunction::Sum,
                    param: None,
                    grouping: None,
                    inner: range(
                        RangeFunction::Rate,
                        selector(vec![m("a", MatchOp::Eq, "b")]),
                        vec![],
                        Duration::from_secs(60),
                    ),
                })),
            }))),
        ),
        // --- binary op precedence: a + b * c ---
        (
            r#"rate({a="b"}[1m]) + rate({a="c"}[1m]) * 2"#,
            Expr::Metric(MetricQuery::Binary(Box::new(BinaryExpr {
                op: BinOp::Add,
                left: range(
                    RangeFunction::Rate,
                    selector(vec![m("a", MatchOp::Eq, "b")]),
                    vec![],
                    Duration::from_secs(60),
                ),
                right: MetricQuery::Binary(Box::new(BinaryExpr {
                    op: BinOp::Mul,
                    left: range(
                        RangeFunction::Rate,
                        selector(vec![m("a", MatchOp::Eq, "c")]),
                        vec![],
                        Duration::from_secs(60),
                    ),
                    right: MetricQuery::Literal(2.0),
                    bool_modifier: false,
                })),
                bool_modifier: false,
            }))),
        ),
        // --- bool modifier on a comparison ---
        (
            r#"count_over_time({a="b"}[1m]) > bool 10"#,
            Expr::Metric(MetricQuery::Binary(Box::new(BinaryExpr {
                op: BinOp::Gt,
                left: range(
                    RangeFunction::CountOverTime,
                    selector(vec![m("a", MatchOp::Eq, "b")]),
                    vec![],
                    Duration::from_secs(60),
                ),
                right: MetricQuery::Literal(10.0),
                bool_modifier: true,
            }))),
        ),
        // --- vector(0) fallback ---
        (
            r#"vector(0)"#,
            Expr::Metric(MetricQuery::VectorLiteral(0.0)),
        ),
        // --- offset after the aggregation lands on the range agg ---
        (
            r#"count_over_time({a="b"}[5m]) offset 1h"#,
            Expr::Metric(MetricQuery::Range(Box::new(RangeAggregation {
                function: RangeFunction::CountOverTime,
                log_query: LogQuery {
                    selector: selector(vec![m("a", MatchOp::Eq, "b")]),
                    pipeline: vec![],
                },
                range: Duration::from_secs(300),
                offset: Some(Duration::from_secs(3600)),
                unwrap: None,
                param: None,
                grouping: None,
            }))),
        ),
    ]
}

#[test]
fn expected_asts_match() {
    let mut failures = Vec::new();
    for (query, expected) in cases() {
        match parse(query) {
            Ok(actual) if actual == expected => {}
            Ok(actual) => failures.push(format!(
                "  {query}\n    expected: {expected:?}\n    actual:   {actual:?}"
            )),
            Err(e) => failures.push(format!("  {query}\n    parse error: {e}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{} AST mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
