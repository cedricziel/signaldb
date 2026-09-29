//! `irVersion` 10: the `reduce`, `map` and `labels` stages.

use query_ir::{Document, IrError, RelationType, Series, SourceRegistry, ValueType, validate};
use serde_json::{Value, json};

fn resolver() -> query_ir::InMemoryResolver {
    query_ir::InMemoryResolver::new()
        .with_column("metrics", "metric.name", "metric_name", ValueType::String)
        .with_column("metrics", "service.name", "service_name", ValueType::String)
        .with_column("metrics", "value", "value", ValueType::Float64)
}

fn check_doc(v: Value) -> Result<RelationType, IrError> {
    let d: Document = serde_json::from_value(v).expect("document parses");
    validate(&d, &SourceRegistry::core(), &resolver()).map(|v| v.terminal)
}

/// `stages` after an open-labelled `sample` (step 1m).
fn open(stages: Value) -> Result<RelationType, IrError> {
    let mut pipeline = vec![json!({ "sample": { "fn": "latest" } })];
    pipeline.extend(stages.as_array().unwrap().iter().cloned());
    check_doc(json!({
        "irVersion": 10, "from": "metrics", "step": "1m",
        "range": { "from": "now-1h", "to": "now" }, "result": "series", "pipeline": pipeline,
    }))
}

/// `stages` after a series known to carry `metric.name` and `service.name`.
fn known(stages: Value) -> Result<RelationType, IrError> {
    open(
        json!([{ "reduce": { "fn": "sum", "by": ["metric.name", "service.name"] } }])
            .as_array()
            .unwrap()
            .iter()
            .chain(stages.as_array().unwrap())
            .cloned()
            .collect(),
    )
}

fn series(r: Result<RelationType, IrError>) -> Series {
    match r {
        Ok(RelationType::Series(s)) => s,
        other => panic!("expected a series, got {other:?}"),
    }
}

fn err(r: Result<RelationType, IrError>) -> String {
    r.expect_err("expected a validation error").to_string()
}

#[test]
fn reduce_label_sets() {
    let s = series(open(
        json!([{ "reduce": { "fn": "sum", "by": ["service.name"] } }]),
    ));
    assert_eq!(
        (s.labels, s.open_labels),
        (vec!["service.name".to_string()], false)
    );
    let s = series(open(json!([{ "reduce": { "fn": "avg" } }])));
    assert_eq!((s.labels.len(), s.open_labels), (0, false));
    let s = series(known(
        json!([{ "reduce": { "fn": "max", "without": ["service.name"] } }]),
    ));
    assert_eq!(
        (s.labels, s.open_labels),
        (vec!["metric.name".to_string()], true)
    );
    let s = series(known(json!([{ "reduce": { "fn": "topk", "arg": 3 } }])));
    assert_eq!(s.labels, vec!["metric.name", "service.name"]);
    let s = series(open(
        json!([{ "reduce": { "fn": "count_values", "label": "v", "by": ["service.name"] } }]),
    ));
    assert_eq!(s.labels, vec!["service.name", "v"]);
}

#[test]
fn reduce_operand_rules() {
    for (op, needle) in [
        (
            json!({ "fn": "sum", "by": ["a"], "without": ["b"] }),
            "without",
        ),
        (json!({ "fn": "topk" }), "arg"),
        (json!({ "fn": "topk", "arg": 1.5 }), "arg"),
        (json!({ "fn": "bottomk", "arg": 0 }), "arg"),
        (json!({ "fn": "quantile", "arg": 2 }), "arg"),
        (json!({ "fn": "sum", "arg": 1 }), "arg"),
        (json!({ "fn": "count_values" }), "label"),
        (json!({ "fn": "sum", "label": "x" }), "label"),
        (json!({ "fn": "sum", "by": [""] }), "label"),
    ] {
        let msg = err(open(json!([{ "reduce": op.clone() }])));
        assert!(msg.contains(needle), "{op}: {msg}");
    }
}

#[test]
fn series_stages_require_a_series() {
    let rows = json!({
        "irVersion": 10, "from": "metrics", "range": { "from": "now-1h", "to": "now" },
        "result": "series", "pipeline": [{ "reduce": { "fn": "sum" } }],
    });
    assert!(err(check_doc(rows)).contains("series"));
}

#[test]
fn map_arity_and_label_effects() {
    let s = series(known(json!([{ "map": { "fn": "abs" } }])));
    assert_eq!(s.labels, vec!["service.name"]);
    for op in [
        json!({ "fn": "round" }),
        json!({ "fn": "round", "args": [0.5] }),
        json!({ "fn": "clamp", "args": [0, 1] }),
        json!({ "fn": "clamp_min", "args": [0] }),
        json!({ "fn": "day_of_week" }),
    ] {
        assert!(open(json!([{ "map": op.clone() }])).is_ok(), "{op}");
    }
    for op in [
        json!({ "fn": "abs", "args": [1] }),
        json!({ "fn": "round", "args": [1, 2] }),
        json!({ "fn": "clamp", "args": [1] }),
        json!({ "fn": "clamp_max" }),
    ] {
        assert!(
            err(open(json!([{ "map": op.clone() }]))).contains("args"),
            "{op}"
        );
    }
}

fn time(stages: Value) -> Result<RelationType, IrError> {
    check_doc(json!({
        "irVersion": 10, "from": "time", "step": "1m",
        "range": { "from": "now-1h", "to": "now" }, "result": "scalar", "pipeline": stages,
    }))
}

#[test]
fn map_on_a_scalar_is_math_only() {
    assert!(matches!(
        time(json!([{ "map": { "fn": "sqrt" } }])),
        Ok(RelationType::Scalar(_))
    ));
    assert!(err(time(json!([{ "map": { "fn": "hour" } }]))).contains("hour"));
}

#[test]
fn labels_replace_and_join() {
    let s = series(known(json!([{ "labels": { "replace": {
        "dst": "svc", "replacement": "$1", "src": "service.name", "regex": "(.*)-prod" } } }])));
    assert_eq!(s.labels, vec!["metric.name", "service.name", "svc"]);
    let s = series(open(json!([{ "labels": { "join": {
        "dst": "id", "separator": "/", "src": ["a", "b"] } } }])));
    assert!(s.open_labels);
    let bad_regex = json!([{ "labels": { "replace": {
        "dst": "x", "replacement": "", "src": "a", "regex": "(" } } }]);
    assert!(err(open(bad_regex)).contains("regex"));
    let empty = json!([{ "labels": { "join": { "dst": "", "separator": "", "src": ["a"] } } }]);
    assert!(err(open(empty)).contains("label"));
}

#[test]
fn series_stages_require_irversion_10() {
    let d = json!({
        "irVersion": 9, "from": "metrics", "range": { "from": "now-1h", "to": "now" },
        "result": "series", "pipeline": [
            { "aggregate": { "aggs": [{ "fn": "count", "as": "n" }], "step": "1m" } },
            { "map": { "fn": "abs" } }
        ],
    });
    assert!(err(check_doc(d.clone())).contains("irVersion 10"));
    let parsed: Document = serde_json::from_value(d).unwrap();
    assert_eq!(parsed.minimum_ir_version(), 10);
}
