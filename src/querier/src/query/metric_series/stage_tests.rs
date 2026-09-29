//! The Series algebra stages over sampled Series, end to end through
//! [`IrService`](crate::query::ir_planner::IrService).

use serde_json::{Value as JsonValue, json};

use super::tests::{S, gauge, run, series_rows};

/// A Series document over `[60s, 120s]` at a 60s step: `latest` of the
/// fixture's gauges, then `stages`.
fn doc(stages: JsonValue) -> JsonValue {
    let mut pipeline = vec![json!({ "sample": { "fn": "latest" } })];
    pipeline.extend(stages.as_array().cloned().unwrap_or_default());
    json!({
        "irVersion": 10, "from": "metrics", "step": "60s",
        "range": { "from": 60 * S, "to": 120 * S },
        "result": "series", "pipeline": pipeline
    })
}

/// `(bucket seconds, labels, value)` of running `stages` over two gauge
/// series `code=200` (1, then 2) and `code=500` (10, then 20).
async fn over_two_series(stages: JsonValue) -> Vec<(i64, String, f64)> {
    let points = [
        gauge(60 * S, "a", 1.0, json!({"code": 200})),
        gauge(120 * S, "a", 2.0, json!({"code": 200})),
        gauge(60 * S, "b", 10.0, json!({"code": 500})),
        gauge(120 * S, "b", 20.0, json!({"code": 500})),
    ];
    series_rows(&run(&points, doc(stages)).await.unwrap())
}

fn rows(want: &[(i64, &str, f64)]) -> Vec<(i64, String, f64)> {
    want.iter()
        .map(|(t, l, v)| (*t, l.to_string(), *v))
        .collect()
}

const A: &str = r#"{"code":"200","metric.name":"temperature","service.name":"svc"}"#;
const B: &str = r#"{"code":"500","metric.name":"temperature","service.name":"svc"}"#;

#[tokio::test]
async fn labels_replace_expands_a_fully_matching_regex_and_keeps_the_name() {
    let replace = json!([{ "labels": { "replace": {
        "dst": "class", "replacement": "${1}xx", "src": "code", "regex": "(\\d)0+"
    } } }]);
    let got = over_two_series(replace).await;
    let a = r#"{"class":"2xx","code":"200","metric.name":"temperature","service.name":"svc"}"#;
    let b = r#"{"class":"5xx","code":"500","metric.name":"temperature","service.name":"svc"}"#;
    assert_eq!(
        got,
        rows(&[(60, a, 1.0), (120, a, 2.0), (60, b, 10.0), (120, b, 20.0)])
    );
}

#[tokio::test]
async fn labels_replace_is_anchored_so_a_partial_match_changes_nothing() {
    let replace = json!([{ "labels": { "replace": {
        "dst": "code", "replacement": "x", "src": "code", "regex": "20"
    } } }]);
    let got = over_two_series(replace).await;
    assert_eq!(
        got,
        rows(&[(60, A, 1.0), (120, A, 2.0), (60, B, 10.0), (120, B, 20.0)])
    );
}

#[tokio::test]
async fn labels_join_reads_an_absent_source_as_empty_and_an_empty_result_removes() {
    let join = json!([
        { "labels": { "join": { "dst": "j", "separator": "-", "src": ["code", "nope", "service.name"] } } },
        { "labels": { "join": { "dst": "code", "separator": "-", "src": ["nope"] } } }
    ]);
    let got = over_two_series(join).await;
    let a = r#"{"j":"200--svc","metric.name":"temperature","service.name":"svc"}"#;
    let b = r#"{"j":"500--svc","metric.name":"temperature","service.name":"svc"}"#;
    assert_eq!(
        got,
        rows(&[(60, a, 1.0), (120, a, 2.0), (60, b, 10.0), (120, b, 20.0)])
    );
}

/// Prometheus' `label_replace(vector(1), "dst", "value", "", "")` idiom.
#[tokio::test]
async fn labels_replace_with_an_empty_source_sets_a_constant_label() {
    let doc = json!({
        "irVersion": 10, "from": "constant", "constant": 1.0, "step": "60s",
        "range": { "from": 60 * S, "to": 120 * S }, "result": "series",
        "pipeline": [
            { "vector": {} },
            { "labels": { "replace": { "dst": "dst", "replacement": "value", "src": "", "regex": "" } } }
        ]
    });
    let got = series_rows(&run(&[], doc).await.unwrap());
    let l = r#"{"dst":"value"}"#;
    assert_eq!(got, rows(&[(60, l, 1.0), (120, l, 1.0)]));
}
