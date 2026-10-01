//! `irVersion` 10: `histogram_fraction` and the histogram `window`.

use query_ir::{Document, IrError, RelationType, Series, SourceRegistry, ValueType, validate};
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

fn doc(pipeline: Value) -> Value {
    json!({
        "irVersion": 10, "from": "metrics",
        "range": { "from": "now-1h", "to": "now" },
        "result": "series", "pipeline": pipeline,
    })
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

fn metrics(stage: Value) -> Result<RelationType, IrError> {
    check(doc(json!([stage])))
}

#[test]
fn histogram_fraction_and_window() {
    let s = series(metrics(json!({ "histogram_fraction": {
        "lower": 0, "upper": 0.25, "by": ["service.name"], "step": "1m", "window": "5m", "as": "f" } })));
    assert_eq!(s.labels, vec!["service.name"]);
    assert!(
        metrics(
            json!({ "histogram_quantile": { "q": 0.9, "step": "1m", "window": "5m", "as": "p" } })
        )
        .is_ok()
    );
    for (stage, needle) in [
        (
            json!({ "histogram_fraction": { "lower": 1, "upper": 0, "step": "1m", "as": "f" } }),
            "lower",
        ),
        (
            json!({ "histogram_fraction": { "lower": 0, "upper": 1, "step": "1m", "window": "0s", "as": "f" } }),
            "window",
        ),
        (
            json!({ "histogram_quantile": { "q": 0.9, "step": "1m", "window": "x", "as": "p" } }),
            "window",
        ),
    ] {
        let msg = err(metrics(stage.clone()));
        assert!(msg.contains(needle), "{stage}: {msg}");
    }
    let limited = check(doc(json!([{ "limit": 1 },
        { "histogram_fraction": { "lower": 0, "upper": 1, "step": "1m", "as": "f" } }])));
    assert!(err(limited).contains("metric point stream"));
}

#[test]
fn histogram_fraction_and_window_require_irversion_10() {
    let v9 = |stage: Value| {
        json!({ "irVersion": 9, "from": "metrics", "range": { "from": "now-1h", "to": "now" },
                "result": "series", "pipeline": [stage] })
    };
    for stage in [
        json!({ "histogram_fraction": { "lower": 0, "upper": 1, "step": "1m", "as": "f" } }),
        json!({ "histogram_quantile": { "q": 0.5, "step": "1m", "window": "5m", "as": "p" } }),
    ] {
        let d = v9(stage);
        assert!(err(check(d.clone())).contains("irVersion 10"), "{d}");
        let parsed: Document = serde_json::from_value(d).unwrap();
        assert_eq!(parsed.minimum_ir_version(), 10);
    }
}

#[test]
fn window_is_rejected_in_instant_mode() {
    let e = err(metrics(json!({"histogram_quantile": {
        "q": 0.5, "step": "1m", "mode": "instant", "window": "5m", "as": "p50"
    }})));
    assert!(e.contains("mode: instant"), "{e}");
}

#[test]
fn per_series_keeps_each_series_and_excludes_by() {
    for stage in [
        json!({ "histogram_quantile": { "q": 0.9, "per_series": true, "step": "1m", "mode": "rate", "as": "p" } }),
        json!({ "histogram_fraction": { "lower": 0, "upper": 1, "per_series": true, "step": "1m", "as": "f" } }),
    ] {
        let s = series(metrics(stage));
        assert_eq!((s.labels, s.open_labels), (Vec::<String>::new(), true));
    }
    let both = metrics(json!({ "histogram_quantile": {
        "q": 0.9, "per_series": true, "by": ["service.name"], "step": "1m", "as": "p" } }));
    assert!(err(both).contains("per_series"));
    let v9 = json!({ "irVersion": 9, "from": "metrics", "range": { "from": "now-1h", "to": "now" },
        "result": "series", "pipeline": [{ "histogram_quantile": { "q": 0.9, "per_series": true, "step": "1m", "as": "p" } }] });
    assert!(check(v9).is_err());
}
