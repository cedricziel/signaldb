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

const UNNAMED_A: &str = r#"{"code":"200","service.name":"svc"}"#;
const UNNAMED_B: &str = r#"{"code":"500","service.name":"svc"}"#;

#[tokio::test]
async fn map_applies_to_every_value_and_drops_the_name() {
    let got = over_two_series(json!([{ "map": { "fn": "clamp", "args": [1.5, 15.0] } }])).await;
    let want = [
        (60, UNNAMED_A, 1.5),
        (120, UNNAMED_A, 2.0),
        (60, UNNAMED_B, 10.0),
        (120, UNNAMED_B, 15.0),
    ];
    assert_eq!(got, rows(&want));
    // min > max: Prometheus returns nothing.
    let empty = over_two_series(json!([{ "map": { "fn": "clamp", "args": [2.0, 1.0] } }])).await;
    assert!(empty.is_empty(), "{empty:?}");
}

#[tokio::test]
async fn map_ln_yields_nan_and_infinities() {
    let points = [
        gauge(60 * S, "a", -1.0, json!({"k": "neg"})),
        gauge(60 * S, "b", 0.0, json!({"k": "zero"})),
        gauge(60 * S, "c", f64::NAN, json!({"k": "nan"})),
    ];
    let mut doc = doc(json!([{ "map": { "fn": "ln" } }]));
    doc["range"]["to"] = json!(60 * S);
    let got = series_rows(&run(&points, doc).await.unwrap());
    let got: Vec<_> = got.iter().map(|(_, l, v)| format!("{l}={v}")).collect();
    assert_eq!(
        got,
        [
            r#"{"k":"nan","service.name":"svc"}=NaN"#,
            r#"{"k":"neg","service.name":"svc"}=NaN"#,
            r#"{"k":"zero","service.name":"svc"}=-inf"#,
        ]
    );
}

#[tokio::test]
async fn map_reads_a_scalar_and_calendar_functions_read_vector_time() {
    let doc = |from: &str, pipeline: JsonValue| {
        let mut doc = json!({
            "irVersion": 10, "from": from, "step": "3600s",
            "range": { "from": 0, "to": 7200 * S }, "pipeline": pipeline, "result": "series"
        });
        if from == "constant" {
            doc["constant"] = json!(16.0);
            doc["result"] = json!("scalar");
        }
        doc
    };
    let sqrt = run(&[], doc("constant", json!([{ "map": { "fn": "sqrt" } }])));
    let got = super::tests::scalar_rows(&sqrt.await.unwrap());
    assert_eq!(got, [(0, 4.0), (3600, 4.0), (7200, 4.0)]);
    let hour = json!([{ "vector": {} }, { "map": { "fn": "hour" } }]);
    let got = series_rows(&run(&[], doc("time", hour)).await.unwrap());
    assert_eq!(
        got,
        rows(&[(0, "{}", 0.0), (3600, "{}", 1.0), (7200, "{}", 2.0)])
    );
}

#[tokio::test]
async fn filter_keeps_matching_values_with_the_name_and_bool_yields_0_or_1() {
    let got = over_two_series(json!([{ "filter": { "op": "ge", "value": 2.0 } }])).await;
    assert_eq!(got, rows(&[(120, A, 2.0), (60, B, 10.0), (120, B, 20.0)]));
    let got =
        over_two_series(json!([{ "filter": { "op": "gt", "value": 2.0, "bool": true } }])).await;
    let want = [
        (60, UNNAMED_A, 0.0),
        (120, UNNAMED_A, 0.0),
        (60, UNNAMED_B, 1.0),
        (120, UNNAMED_B, 1.0),
    ];
    assert_eq!(got, rows(&want));
}

#[tokio::test]
async fn filter_never_keeps_nan_but_ne() {
    let points = [gauge(60 * S, "a", f64::NAN, json!({}))];
    let mut doc = doc(json!([{ "filter": { "op": "gt", "value": 0.0 } }]));
    doc["range"]["to"] = json!(60 * S);
    assert_eq!(run(&points, doc.clone()).await.unwrap().num_rows(), 0);
    doc["pipeline"][1] = json!({ "filter": { "op": "ne", "value": 0.0 } });
    assert_eq!(run(&points, doc).await.unwrap().num_rows(), 1);
}

#[tokio::test]
async fn sort_orders_an_instant_by_value_with_nan_last() {
    let points = [
        gauge(60 * S, "a", 20.0, json!({"k": "a"})),
        gauge(60 * S, "b", f64::NAN, json!({"k": "b"})),
        gauge(60 * S, "c", 2.0, json!({"k": "c"})),
    ];
    let keys = |direction: &str| {
        let mut doc = doc(json!([{ "sort": direction }]));
        doc["range"]["to"] = json!(60 * S);
        let points = points.clone();
        async move {
            let batch = run(&points, doc).await.unwrap();
            series_rows(&batch)
                .into_iter()
                .map(|(_, l, _)| l[6..7].to_string())
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(keys("asc").await, ["c", "a", "b"]);
    assert_eq!(keys("desc").await, ["a", "c", "b"]);
    // Over a range the series stay in label-set order, as in Prometheus.
    let got = over_two_series(json!([{ "sort": "desc" }])).await;
    assert_eq!(
        got,
        rows(&[(60, A, 1.0), (120, A, 2.0), (60, B, 10.0), (120, B, 20.0)])
    );
}
