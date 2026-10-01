//! `irVersion` 10: the `sample` stage (metric point stream → open-labelled
//! Series).

use query_ir::{Document, IrError, RelationType, SourceRegistry, ValueType, validate};
use serde_json::{Value, json};

fn resolver() -> query_ir::InMemoryResolver {
    query_ir::InMemoryResolver::new()
        .with_column("metrics", "metric.name", "metric_name", ValueType::String)
        .with_column("metrics", "service.name", "service_name", ValueType::String)
}

fn check(v: Value) -> Result<RelationType, IrError> {
    let d: Document = serde_json::from_value(v).expect("document parses");
    validate(&d, &SourceRegistry::core(), &resolver()).map(|v| v.terminal)
}

/// A metrics document at v10 with a document `step` of 1m.
fn metrics(pipeline: Value) -> Value {
    json!({
        "irVersion": 10, "from": "metrics", "step": "1m",
        "range": { "from": "now-1h", "to": "now" },
        "result": "series", "pipeline": pipeline,
    })
}

fn sample(op: Value) -> Value {
    json!({ "sample": op })
}

fn err_text(r: Result<RelationType, IrError>) -> String {
    r.expect_err("expected a validation error").to_string()
}

#[test]
fn sample_over_a_filtered_point_stream_is_an_open_series_at_the_document_step() {
    let t = check(metrics(json!([
        { "where": { "field": "metric.name", "op": "eq", "value": "up" } },
        sample(json!({ "fn": "latest" }))
    ])))
    .unwrap();
    let RelationType::Series(s) = t else {
        panic!("expected a series, got {t:?}");
    };
    assert!(s.open_labels);
    assert_eq!(s.labels, vec!["metric.name"]);
    assert_eq!(s.value, ValueType::Float64);
    assert_eq!(s.step_ns, 60_000_000_000);
}

#[test]
fn sample_step_overrides_the_document_step_and_one_is_required() {
    let t = check(metrics(json!([sample(
        json!({ "fn": "rate", "window": "5m", "step": "30s" })
    )])))
    .unwrap();
    assert!(matches!(t, RelationType::Series(s) if s.step_ns == 30_000_000_000));

    let mut d = metrics(json!([sample(json!({ "fn": "rate", "window": "5m" }))]));
    d.as_object_mut().unwrap().remove("step");
    assert!(err_text(check(d)).contains("step"));
}

#[test]
fn sample_needs_an_unbroken_point_stream() {
    let mut logs = metrics(json!([sample(json!({ "fn": "latest" }))]));
    logs["from"] = json!("logs");
    for d in [
        logs,
        metrics(json!([{ "limit": 5 }, sample(json!({ "fn": "latest" }))])),
        metrics(json!([
            sample(json!({ "fn": "latest" })),
            sample(json!({ "fn": "latest" }))
        ])),
    ] {
        assert!(err_text(check(d)).contains("metric point stream"));
    }
}

#[test]
fn sample_operand_rules() {
    let bad = [
        (json!({ "fn": "latest", "window": "5m" }), "window"),
        (json!({ "fn": "rate" }), "window"),
        (
            json!({ "fn": "rate", "window": "5m", "lookback": "5m" }),
            "lookback",
        ),
        (json!({ "fn": "quantile_over_time", "window": "5m" }), "arg"),
        (
            json!({ "fn": "quantile_over_time", "window": "5m", "arg": 1.5 }),
            "arg",
        ),
        (json!({ "fn": "rate", "window": "5m", "arg": 0.5 }), "arg"),
        (json!({ "fn": "rate", "window": "0s" }), "window"),
        (json!({ "fn": "latest", "lookback": "-1m" }), "lookback"),
        (json!({ "fn": "latest", "offset": "-1m" }), "offset"),
        (
            json!({ "fn": "latest", "offset": "-0.0000000001s" }),
            "offset",
        ),
        (
            json!({ "fn": "latest", "at": "yesterday-ish" }),
            "sample.at",
        ),
    ];
    for (op, needle) in bad {
        let msg = err_text(check(metrics(json!([sample(op.clone())]))));
        assert!(msg.contains(needle), "{op}: {msg}");
    }
    let good = [
        json!({ "fn": "latest", "lookback": "2m", "offset": "1h", "at": "now-1h" }),
        json!({ "fn": "quantile_over_time", "window": "5m", "arg": 0.99, "of": "metric.sum" }),
        json!({ "fn": "increase", "window": "10m", "of": "metric.count", "offset": "0s" }),
    ];
    for op in good {
        assert!(check(metrics(json!([sample(op.clone())]))).is_ok(), "{op}");
    }
}

/// The Series frame's value column is always `value`: `sample` names none.
#[test]
fn sample_takes_no_output_name() {
    let doc = metrics(json!([sample(json!({ "fn": "latest", "as": "v" }))]));
    let err = serde_json::from_value::<Document>(doc).expect_err("`as` is unknown");
    assert!(err.to_string().contains("as"), "{err}");
}

#[test]
fn sample_requires_irversion_10() {
    let mut d = metrics(json!([sample(json!({ "fn": "latest", "step": "1m" }))]));
    d["irVersion"] = json!(9);
    d.as_object_mut().unwrap().remove("step");
    assert!(err_text(check(d.clone())).contains("irVersion 10"));
    let parsed: Document = serde_json::from_value(d).unwrap();
    assert_eq!(parsed.minimum_ir_version(), 10);
}
