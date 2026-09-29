//! `irVersion` 10: the `filter`, `sort`, `absent` and `over_time` stages.

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
fn filter_sort_absent() {
    let s = series(known(json!([{ "filter": { "op": "gt", "value": 1 } }])));
    assert!(s.labels.contains(&"metric.name".to_string()));
    let s = series(known(
        json!([{ "filter": { "op": "gt", "value": 1, "bool": true } }]),
    ));
    assert!(!s.labels.contains(&"metric.name".to_string()));
    assert!(open(json!([{ "sort": "desc" }])).is_ok());
    let s = series(open(json!([{ "absent": { "labels": { "job": "api" } } }])));
    assert_eq!((s.labels, s.open_labels), (vec!["job".to_string()], false));
}

#[test]
fn over_time_rewindows_a_series() {
    let s = series(open(
        json!([{ "over_time": { "fn": "max", "window": "30m", "step": "5m" } }]),
    ));
    assert_eq!(s.step_ns, 300_000_000_000);
    for (op, needle) in [
        (
            json!({ "fn": "max", "window": "30m", "step": "30s" }),
            "step",
        ),
        (json!({ "fn": "quantile", "window": "30m" }), "arg"),
        (json!({ "fn": "avg", "window": "30m", "arg": 0.5 }), "arg"),
        (json!({ "fn": "avg", "window": "0s" }), "window"),
    ] {
        let msg = err(open(json!([{ "over_time": op.clone() }])));
        assert!(msg.contains(needle), "{op}: {msg}");
    }
}

#[test]
fn window_stages_require_a_series_and_irversion_10() {
    let time = json!({
        "irVersion": 10, "from": "time", "step": "1m",
        "range": { "from": "now-1h", "to": "now" }, "result": "series",
        "pipeline": [{ "sort": "asc" }],
    });
    assert!(err(check_doc(time)).contains("series"));
    let d = json!({
        "irVersion": 9, "from": "metrics", "range": { "from": "now-1h", "to": "now" },
        "result": "series", "pipeline": [
            { "aggregate": { "aggs": [{ "fn": "count", "as": "n" }], "step": "1m" } },
            { "sort": "asc" }
        ],
    });
    assert!(err(check_doc(d.clone())).contains("irVersion 10"));
    let parsed: Document = serde_json::from_value(d).unwrap();
    assert_eq!(parsed.minimum_ir_version(), 10);
}

#[test]
fn window_stages_reject_a_non_numeric_series() {
    let stepped_min = json!({ "aggregate": { "by": ["service.name"],
        "aggs": [{ "fn": "min", "of": "metric.name", "as": "m" }], "step": "1m" } });
    let doc = |pipeline: Value| {
        json!({ "irVersion": 10, "from": "metrics", "range": { "from": "now-1h", "to": "now" },
            "result": "series", "pipeline": pipeline })
    };
    assert_eq!(
        series(check_doc(doc(json!([stepped_min.clone()])))).value,
        ValueType::String
    );
    for stage in [
        json!({ "filter": { "op": "gt", "value": 1 } }),
        json!({ "sort": "desc" }),
        json!({ "over_time": { "fn": "sum", "window": "5m" } }),
    ] {
        let msg = err(check_doc(doc(json!([stepped_min.clone(), stage.clone()]))));
        assert!(msg.contains("numeric"), "{stage}: {msg}");
    }
}
