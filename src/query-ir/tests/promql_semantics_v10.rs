//! `irVersion` 10: the label semantics PromQL and the IR share ("same series,
//! labels and values"). Which stages keep `metric.name`, what the histogram
//! stages label their output with, the histogram instant `lookback`, and the
//! constant-label `labels` idiom.

use query_ir::{Document, IrError, RelationType, Series, SourceRegistry, ValueType, validate};
use serde_json::{Value, json};

fn resolver() -> query_ir::InMemoryResolver {
    query_ir::InMemoryResolver::new()
        .with_column("metrics", "metric.name", "metric_name", ValueType::String)
        .with_column("metrics", "service.name", "service_name", ValueType::String)
}

fn check(pipeline: Value) -> Result<RelationType, IrError> {
    let d: Document = serde_json::from_value(json!({
        "irVersion": 10, "from": "metrics", "step": "1m",
        "range": { "from": "now-1h", "to": "now" }, "result": "series", "pipeline": pipeline,
    }))
    .expect("document parses");
    validate(&d, &SourceRegistry::core(), &resolver()).map(|v| v.terminal)
}

fn series(r: Result<RelationType, IrError>) -> Series {
    match r {
        Ok(RelationType::Series(s)) => s,
        other => panic!("expected a series, got {other:?}"),
    }
}

fn labels(pipeline: Value) -> (Vec<String>, bool) {
    let s = series(check(pipeline));
    (s.labels, s.open_labels)
}

fn err(pipeline: Value) -> String {
    check(pipeline)
        .expect_err("expected a validation error")
        .to_string()
}

fn named(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

/// `latest` and `last_over_time` keep the metric name; every other sample
/// function computes a new value and drops it, as Prometheus does.
#[test]
fn sample_keeps_the_metric_name_only_for_latest_and_last_over_time() {
    for (sample, kept) in [
        (json!({ "fn": "latest" }), true),
        (json!({ "fn": "last_over_time", "window": "5m" }), true),
        (json!({ "fn": "rate", "window": "5m" }), false),
        (json!({ "fn": "increase", "window": "5m" }), false),
        (json!({ "fn": "irate", "window": "5m" }), false),
        (json!({ "fn": "delta", "window": "5m" }), false),
        (json!({ "fn": "idelta", "window": "5m" }), false),
        (json!({ "fn": "deriv", "window": "5m" }), false),
        (json!({ "fn": "resets", "window": "5m" }), false),
        (json!({ "fn": "changes", "window": "5m" }), false),
        (json!({ "fn": "avg_over_time", "window": "5m" }), false),
        (json!({ "fn": "max_over_time", "window": "5m" }), false),
        (json!({ "fn": "count_over_time", "window": "5m" }), false),
        (json!({ "fn": "present_over_time", "window": "5m" }), false),
        (
            json!({ "fn": "quantile_over_time", "window": "5m", "arg": 0.5 }),
            false,
        ),
    ] {
        let expected = if kept {
            named(&["metric.name"])
        } else {
            vec![]
        };
        assert_eq!(
            labels(json!([{ "sample": sample.clone() }])),
            (expected, true),
            "{sample}"
        );
    }
}

#[test]
fn over_time_keeps_the_metric_name_only_for_last() {
    let known = json!({ "reduce": { "fn": "sum", "by": ["metric.name", "service.name"] } });
    let over = |f: &str| {
        labels(json!([
            { "sample": { "fn": "latest" } },
            known.clone(),
            { "over_time": { "fn": f, "window": "30m" } }
        ]))
    };
    assert_eq!(
        over("last"),
        (named(&["metric.name", "service.name"]), false)
    );
    for f in ["max", "avg", "sum", "count", "delta", "deriv", "changes"] {
        assert_eq!(over(f), (named(&["service.name"]), false), "{f}");
    }
}

/// Histogram stages label their output as Prometheus does: the `by` labels
/// when merging, each series' own labels less the name when per series.
#[test]
fn histogram_stages_drop_the_metric_name() {
    for stage in ["histogram_quantile", "histogram_fraction"] {
        let op = |extra: Value| {
            let mut op = match stage {
                "histogram_quantile" => json!({ "q": 0.5, "step": "1m", "as": "h" }),
                _ => json!({ "lower": 0, "upper": 1, "step": "1m", "as": "h" }),
            };
            op.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            json!([{ stage: op }])
        };
        assert_eq!(
            labels(op(json!({ "by": ["service.name"] }))),
            (named(&["service.name"]), false),
            "{stage}"
        );
        assert_eq!(labels(op(json!({}))), (vec![], false), "{stage}");
        assert_eq!(
            labels(op(json!({ "per_series": true }))),
            (vec![], true),
            "{stage}"
        );
    }
}

/// Instant mode may read each series' latest point within a lookback, as a
/// PromQL instant vector does; rate mode has its `window` instead.
#[test]
fn histogram_lookback_is_instant_mode_only() {
    let hq = |extra: Value| {
        let mut op = json!({ "q": 0.9, "step": "1m", "as": "h" });
        op.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        json!([{ "histogram_quantile": op }])
    };
    let hf = json!([{ "histogram_fraction": {
        "lower": 0, "upper": 1, "step": "1m", "mode": "instant", "lookback": "5m", "as": "h" } }]);
    assert!(check(hf).is_ok());
    assert!(check(hq(json!({ "mode": "instant", "lookback": "5m" }))).is_ok());
    for (extra, needle) in [
        (json!({ "mode": "rate", "lookback": "5m" }), "lookback"),
        (json!({ "lookback": "5m" }), "lookback"),
        (json!({ "mode": "instant", "lookback": "0s" }), "lookback"),
        (json!({ "mode": "instant", "lookback": "soon" }), "lookback"),
    ] {
        let msg = err(hq(extra.clone()));
        assert!(msg.contains(needle), "{extra}: {msg}");
    }
}

/// `label_replace(v, "dst", "value", "", "")` sets a constant label: an empty
/// `src` reads as the empty string. The destination must still be named.
#[test]
fn labels_replace_accepts_an_empty_source() {
    let replace = |dst: &str, src: &str| {
        json!([
            { "sample": { "fn": "latest" } },
            { "reduce": { "fn": "sum", "by": ["service.name"] } },
            { "labels": { "replace": {
                "dst": dst, "replacement": "prod", "src": src, "regex": "^(?s:)$" } } }
        ])
    };
    assert_eq!(
        labels(replace("env", "")),
        (named(&["service.name", "env"]), false)
    );
    assert!(err(replace("", "")).contains("label"));
}
