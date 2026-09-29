//! `irVersion` 10: the metric point stream, the `Scalar` relation and
//! envelope, the `scalar`/`vector` stages, the document `step`/`constant`
//! and the `time`/`constant` pseudo-sources.

use query_ir::{
    Document, IrError, RelationType, ScalarRelation, SourceRegistry, ValueType, validate,
};
use serde_json::{Value, json};

fn resolver() -> query_ir::InMemoryResolver {
    query_ir::InMemoryResolver::new()
        .with_column("metrics", "metric.name", "metric_name", ValueType::String)
        .with_column("metrics", "service.name", "service_name", ValueType::String)
        .with_column("metrics", "value", "value", ValueType::Float64)
}

fn check(v: Value) -> Result<RelationType, IrError> {
    let d: Document = serde_json::from_value(v).expect("document parses");
    validate(&d, &SourceRegistry::core(), &resolver()).map(|v| v.terminal)
}

fn metrics(result: &str, pipeline: Value) -> Value {
    json!({
        "irVersion": 10, "from": "metrics",
        "range": { "from": "now-1h", "to": "now" },
        "result": result, "pipeline": pipeline,
    })
}

fn err_text(r: Result<RelationType, IrError>) -> String {
    r.expect_err("expected a validation error").to_string()
}

fn count_series() -> Value {
    json!({ "aggregate": { "by": ["service.name"], "aggs": [{ "fn": "count", "as": "n" }], "step": "1m" } })
}

fn rate(by: Value) -> Value {
    json!({ "aggregate": { "by": by, "aggs": [{ "fn": "rate", "of": "value", "as": "r" }], "step": "1m" } })
}

#[test]
fn a_stage_other_than_where_drops_the_point_stream_identity() {
    for before in [
        json!({ "limit": 10 }),
        json!({ "order": [{ "of": "metric.name", "dir": "asc" }] }),
        json!({ "topk": { "n": 3, "of": "value" } }),
    ] {
        for op in [
            rate(json!([])),
            json!({ "histogram_quantile": { "q": 0.9, "step": "1m", "as": "p90" } }),
        ] {
            let msg = err_text(check(metrics("series", json!([before.clone(), op]))));
            assert!(msg.contains("metric point stream"), "{msg}");
        }
    }
}

#[test]
fn a_where_filtered_point_stream_keeps_its_identity() {
    let t = check(metrics(
        "series",
        json!([
            { "where": { "field": "metric.name", "op": "eq", "value": "up" } },
            rate(json!(["service.name"]))
        ]),
    ));
    assert!(
        matches!(t, Ok(RelationType::Series(s)) if !s.open_labels && s.labels == ["service.name"])
    );
}

#[test]
fn scalar_and_vector_convert_between_series_and_scalar() {
    let t = check(metrics("scalar", json!([count_series(), { "scalar": {} }]))).unwrap();
    assert_eq!(
        t,
        RelationType::Scalar(ScalarRelation {
            step_ns: 60_000_000_000
        })
    );
    let t = check(metrics(
        "series",
        json!([count_series(), { "scalar": {} }, { "vector": {} }]),
    ))
    .unwrap();
    assert!(
        matches!(t, RelationType::Series(s) if !s.open_labels && s.labels.is_empty() && s.value == ValueType::Float64)
    );
}

#[test]
fn scalar_and_vector_reject_the_wrong_input() {
    let msg = err_text(check(metrics(
        "series",
        json!([count_series(), { "scalar": {} }]),
    )));
    assert!(msg.contains("scalar"), "{msg}");
    assert!(err_text(check(metrics("scalar", json!([{ "scalar": {} }])))).contains("series"));
    assert!(
        err_text(check(metrics(
            "series",
            json!([count_series(), { "vector": {} }])
        )))
        .contains("scalar")
    );
}

#[test]
fn the_scalar_envelope_takes_no_fields() {
    let mut d = metrics("scalar", json!([count_series(), { "scalar": {} }]));
    d["fields"] = json!(["n"]);
    assert_eq!(check(d), Err(IrError::FieldsOnSeries));
}

#[test]
fn v10_shapes_are_rejected_below_irversion_10() {
    let mut d = metrics("scalar", json!([count_series(), { "scalar": {} }]));
    d["irVersion"] = json!(9);
    assert!(err_text(check(d)).contains("irVersion 10"));
    let mut d = metrics(
        "series",
        json!([count_series(), { "scalar": {} }, { "vector": {} }]),
    );
    d["irVersion"] = json!(9);
    assert!(err_text(check(d)).contains("irVersion 10"));
}

#[test]
fn minimum_ir_version_covers_the_v10_shapes() {
    for v in [
        metrics("scalar", json!([])),
        metrics(
            "series",
            json!([count_series(), { "scalar": {} }, { "vector": {} }]),
        ),
    ] {
        let d: Document = serde_json::from_value(v).unwrap();
        assert_eq!(d.minimum_ir_version(), 10, "{d:?}");
    }
}

fn pseudo(from: &str) -> Value {
    json!({
        "irVersion": 10, "from": from, "step": "15s",
        "range": { "from": "now-1h", "to": "now" }, "result": "scalar",
    })
}

#[test]
fn the_time_and_constant_pseudo_sources_seed_a_scalar() {
    let expected = RelationType::Scalar(ScalarRelation {
        step_ns: 15_000_000_000,
    });
    assert_eq!(check(pseudo("time")).unwrap(), expected);
    let mut c = pseudo("constant");
    c["constant"] = json!(2.5);
    assert_eq!(check(c).unwrap(), expected);
    let mut v = pseudo("time");
    v["result"] = json!("series");
    v["pipeline"] = json!([{ "vector": {} }]);
    assert!(matches!(check(v).unwrap(), RelationType::Series(_)));
}

#[test]
fn pseudo_source_rules() {
    assert!(err_text(check(pseudo("constant"))).contains("constant"));
    let mut t = pseudo("time");
    t["constant"] = json!(1.0);
    assert!(err_text(check(t)).contains("constant"));
    let mut m = metrics("series", json!([count_series()]));
    m["constant"] = json!(1.0);
    assert!(err_text(check(m)).contains("constant"));

    let mut no_step = pseudo("time");
    no_step.as_object_mut().unwrap().remove("step");
    assert!(err_text(check(no_step)).contains("step"));
    let mut bad_step = pseudo("time");
    bad_step["step"] = json!("0s");
    assert!(err_text(check(bad_step)).contains("step"));

    let mut where_on_time = pseudo("time");
    where_on_time["pipeline"] = json!([{ "where": { "field": "metric.name", "op": "exists" } }]);
    assert!(err_text(check(where_on_time)).contains("scalar"));

    let names = SourceRegistry::core().names();
    assert!(!names.iter().any(|n| n == "time" || n == "constant"));
}

#[test]
fn document_step_and_pseudo_sources_require_irversion_10() {
    let mut t = pseudo("time");
    t["irVersion"] = json!(9);
    t["result"] = json!("rows");
    assert!(err_text(check(t)).contains("irVersion 10"));
    let mut m = metrics("series", json!([count_series()]));
    m["irVersion"] = json!(9);
    m["step"] = json!("1m");
    assert!(err_text(check(m.clone())).contains("irVersion 10"));
    let d: Document = serde_json::from_value(m).unwrap();
    assert_eq!(d.minimum_ir_version(), 10);
    let d: Document = serde_json::from_value(pseudo("time")).unwrap();
    assert_eq!(d.minimum_ir_version(), 10);
}
