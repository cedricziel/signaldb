//! `irVersion` 10: the `binop` stage.

use query_ir::{Document, IrError, RelationType, Series, SourceRegistry, ValueType, validate};
use serde_json::{Value, json};

fn resolver() -> query_ir::InMemoryResolver {
    query_ir::InMemoryResolver::new()
        .with_column("metrics", "metric.name", "metric_name", ValueType::String)
        .with_column("metrics", "service.name", "service_name", ValueType::String)
        .with_column("logs", "service.name", "service_name", ValueType::String)
}

fn check(v: Value) -> Result<RelationType, IrError> {
    let d: Document = serde_json::from_value(v).expect("document parses");
    validate(&d, &SourceRegistry::core(), &resolver()).map(|v| v.terminal)
}

fn doc(from: &str, result: &str, pipeline: Value) -> Value {
    json!({
        "irVersion": 10, "from": from, "step": "1m",
        "range": { "from": "now-1h", "to": "now" },
        "result": result, "pipeline": pipeline,
    })
}

/// A sub-document: a metric series known to carry `by`.
fn known(by: Value) -> Value {
    json!({ "from": "metrics", "pipeline": [
        { "sample": { "fn": "latest" } },
        { "reduce": { "fn": "sum", "by": by } }
    ] })
}

/// `binop` with `known(left_by)` as the pipeline.
fn binop(left_by: Value, op: Value) -> Result<RelationType, IrError> {
    let left = known(left_by);
    let mut pipeline = left["pipeline"].as_array().unwrap().clone();
    pipeline.push(json!({ "binop": op }));
    check(doc("metrics", "series", Value::Array(pipeline)))
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

const MS: &str = r#"["metric.name", "service.name"]"#;

fn ms() -> Value {
    serde_json::from_str(MS).unwrap()
}

#[test]
fn arithmetic_with_a_number_drops_the_metric_name() {
    let s = series(binop(ms(), json!({ "op": "mul", "right": 100 })));
    assert_eq!(
        (s.labels, s.open_labels),
        (vec!["service.name".to_string()], false)
    );
    let s = series(binop(
        ms(),
        json!({ "op": "sub", "right": 2, "reverse": true }),
    ));
    assert_eq!(s.labels, vec!["service.name"]);
    let s = series(binop(ms(), json!({ "op": "gt", "right": 1 })));
    assert_eq!(s.labels, vec!["metric.name", "service.name"]);
    let s = series(binop(ms(), json!({ "op": "gt", "right": 1, "bool": true })));
    assert_eq!(s.labels, vec!["service.name"]);
}

#[test]
fn vector_matching_output_labels() {
    let s = series(binop(
        ms(),
        json!({ "op": "div", "right": known(ms()), "on": ["service.name"] }),
    ));
    assert_eq!(
        (s.labels, s.open_labels),
        (vec!["service.name".to_string()], false)
    );
    let s = series(binop(
        ms(),
        json!({ "op": "div", "right": known(json!(["service.name"])), "on": ["service.name"],
                "group": { "side": "left", "include": ["team"] } }),
    ));
    assert_eq!(s.labels, vec!["service.name", "team"]);
    let s = series(binop(
        ms(),
        json!({ "op": "and", "right": known(json!(["service.name"])) }),
    ));
    assert_eq!(s.labels, vec!["metric.name", "service.name"]);
    let open = json!({ "from": "metrics", "pipeline": [{ "sample": { "fn": "latest" } }] });
    let s = series(binop(
        ms(),
        json!({ "op": "add", "right": open, "ignoring": ["x"] }),
    ));
    assert!(s.open_labels);
}

#[test]
fn scalar_op_scalar_is_a_scalar() {
    let t = check(json!({
        "irVersion": 10, "from": "time", "step": "1m",
        "range": { "from": "now-1h", "to": "now" }, "result": "scalar",
        "pipeline": [{ "binop": { "op": "add", "right": { "from": "constant", "constant": 2 } } }],
    }));
    assert!(matches!(t, Ok(RelationType::Scalar(_))), "{t:?}");
}

#[test]
fn binop_rules() {
    let series_right = known(ms());
    for (op, needle) in [
        (
            json!({ "op": "add", "right": series_right, "on": ["a"], "ignoring": ["b"] }),
            "ignoring",
        ),
        (json!({ "op": "add", "right": 1, "on": ["a"] }), "on"),
        (
            json!({ "op": "add", "right": 1, "group": { "side": "left" } }),
            "group",
        ),
        (json!({ "op": "and", "right": 1 }), "series on both sides"),
        (
            json!({ "op": "or", "right": known(ms()), "group": { "side": "left" } }),
            "group",
        ),
        (
            json!({ "op": "unless", "right": known(ms()), "bool": true }),
            "bool",
        ),
        (json!({ "op": "add", "right": 1, "bool": true }), "bool"),
        (
            json!({ "op": "add", "right": known(ms()), "on": [""] }),
            "label",
        ),
    ] {
        let msg = err(binop(ms(), op.clone()));
        assert!(msg.contains(needle), "{op}: {msg}");
    }
}

#[test]
fn the_right_sub_document_is_validated_recursively() {
    let other_step =
        json!({ "from": "metrics", "pipeline": [{ "sample": { "fn": "latest", "step": "30s" } }] });
    assert!(err(binop(ms(), json!({ "op": "add", "right": other_step }))).contains("step"));
    let rows = json!({ "from": "metrics", "pipeline": [] });
    assert!(err(binop(ms(), json!({ "op": "add", "right": rows }))).contains("series or scalar"));
    let bad = json!({ "from": "metrics", "pipeline": [{ "reduce": { "fn": "topk" } }] });
    assert!(err(binop(ms(), json!({ "op": "add", "right": bad }))).contains("reduce"));
    let logs = json!({ "from": "logs", "pipeline": [] });
    assert!(err(binop(ms(), json!({ "op": "add", "right": logs }))).contains("right.from"));
    let no_constant = json!({ "from": "constant" });
    assert!(err(binop(ms(), json!({ "op": "add", "right": no_constant }))).contains("constant"));
}

#[test]
fn binop_requires_irversion_10() {
    let d = json!({
        "irVersion": 9, "from": "metrics", "range": { "from": "now-1h", "to": "now" },
        "result": "series", "pipeline": [
            { "aggregate": { "aggs": [{ "fn": "count", "as": "n" }], "step": "1m" } },
            { "binop": { "op": "mul", "right": 2 } }
        ],
    });
    assert!(err(check(d.clone())).contains("irVersion 10"));
    let parsed: Document = serde_json::from_value(d).unwrap();
    assert_eq!(parsed.minimum_ir_version(), 10);
}

#[test]
fn scalar_comparison_needs_bool() {
    let cmp = |b: bool| {
        check(doc(
            "time",
            "scalar",
            json!([{ "binop": { "op": "gt", "right": 1.0, "bool": b } }]),
        ))
    };
    assert!(err(cmp(false)).contains("needs `bool`"));
    assert!(matches!(cmp(true), Ok(RelationType::Scalar(_))));
}
