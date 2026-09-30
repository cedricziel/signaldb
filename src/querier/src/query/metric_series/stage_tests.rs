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

/// `(labels, value)` at 60s of `stages` over `job=api,code=200` (1),
/// `job=api,code=500` (10) and `job=web,code=200` (`web`).
async fn over_three(web: f64, stages: JsonValue) -> Vec<(String, f64)> {
    let points = [
        gauge(60 * S, "a", 1.0, json!({"job": "api", "code": 200})),
        gauge(60 * S, "b", 10.0, json!({"job": "api", "code": 500})),
        gauge(60 * S, "c", web, json!({"job": "web", "code": 200})),
    ];
    let mut doc = doc(stages);
    doc["range"]["to"] = json!(60 * S);
    let batch = run(&points, doc).await.unwrap();
    series_rows(&batch)
        .into_iter()
        .map(|(_, l, v)| (l, v))
        .collect()
}

fn pairs(want: &[(&str, f64)]) -> Vec<(String, f64)> {
    want.iter().map(|(l, v)| (l.to_string(), *v)).collect()
}

#[tokio::test]
async fn reduce_by_keeps_exactly_the_listed_labels() {
    let sum_by = json!([{ "reduce": { "fn": "sum", "by": ["job"] } }]);
    let got = over_three(100.0, sum_by).await;
    assert_eq!(
        got,
        pairs(&[(r#"{"job":"api"}"#, 11.0), (r#"{"job":"web"}"#, 100.0)])
    );
    let by_name = json!([{ "reduce": { "fn": "count", "by": ["metric.name"] } }]);
    let got = over_three(100.0, by_name).await;
    assert_eq!(got, pairs(&[(r#"{"metric.name":"temperature"}"#, 3.0)]));
    let all = json!([{ "reduce": { "fn": "avg" } }]);
    assert_eq!(over_three(100.0, all).await, pairs(&[("{}", 37.0)]));
}

#[tokio::test]
async fn reduce_without_drops_the_listed_labels_and_the_name() {
    let without = json!([{ "reduce": { "fn": "max", "without": ["code"] } }]);
    let got = over_three(100.0, without).await;
    let want = [
        (r#"{"job":"api","service.name":"svc"}"#, 10.0),
        (r#"{"job":"web","service.name":"svc"}"#, 100.0),
    ];
    assert_eq!(got, pairs(&want));
}

#[tokio::test]
async fn reduce_functions_follow_promql() {
    let by_job = |func: &str, arg: Option<f64>| {
        let mut reduce = json!({ "fn": func, "by": ["job"] });
        if let Some(arg) = arg {
            reduce["arg"] = json!(arg);
        }
        json!([{ "reduce": reduce }])
    };
    let cases = [
        ("min", None, [1.0, 7.0]),
        ("group", None, [1.0, 1.0]),
        ("stddev", None, [4.5, 0.0]),
        ("stdvar", None, [20.25, 0.0]),
        ("quantile", Some(0.25), [3.25, 7.0]),
    ];
    for (func, arg, want) in cases {
        let got: Vec<f64> = over_three(7.0, by_job(func, arg))
            .await
            .into_iter()
            .map(|(_, v)| v)
            .collect();
        assert_eq!(got, want, "{func}");
    }
    // A NaN is ignored by min/max unless every value is NaN.
    let got = over_three(f64::NAN, by_job("max", None)).await;
    assert_eq!(got[0], (r#"{"job":"api"}"#.to_string(), 10.0));
    assert!(got[1].1.is_nan(), "{got:?}");
}

#[tokio::test]
async fn topk_and_bottomk_keep_each_groups_extreme_series_with_all_labels() {
    let topk = json!([{ "reduce": { "fn": "topk", "arg": 1.0, "by": ["job"] } }]);
    let got = over_three(100.0, topk).await;
    let want = [
        (
            r#"{"code":"200","job":"web","metric.name":"temperature","service.name":"svc"}"#,
            100.0,
        ),
        (
            r#"{"code":"500","job":"api","metric.name":"temperature","service.name":"svc"}"#,
            10.0,
        ),
    ];
    assert_eq!(got, pairs(&want));
    // NaN ranks last for bottomk as for topk.
    let bottomk = json!([{ "reduce": { "fn": "bottomk", "arg": 2.0 } }]);
    let got = over_three(f64::NAN, bottomk).await;
    let values: Vec<f64> = got.into_iter().map(|(_, v)| v).collect();
    assert_eq!(values, [1.0, 10.0]);
}

#[tokio::test]
async fn count_values_counts_series_per_value_under_a_new_label() {
    let count_values =
        json!([{ "reduce": { "fn": "count_values", "label": "v", "by": ["code"] } }]);
    let got = over_three(1.0, count_values).await;
    let want = [
        (r#"{"code":"200","v":"1"}"#, 2.0),
        (r#"{"code":"500","v":"10"}"#, 1.0),
    ];
    assert_eq!(got, pairs(&want));
}

#[tokio::test]
async fn absent_is_one_labelled_series_where_the_input_has_none() {
    let points = [gauge(60 * S, "a", 1.0, json!({}))];
    let doc = json!({
        "irVersion": 10, "from": "metrics", "step": "60s",
        "range": { "from": 60 * S, "to": 180 * S }, "result": "series",
        "pipeline": [
            { "sample": { "fn": "latest", "lookback": "30s" } },
            { "absent": { "labels": { "job": "x" } } }
        ]
    });
    let got = series_rows(&run(&points, doc).await.unwrap());
    let l = r#"{"job":"x"}"#;
    assert_eq!(got, rows(&[(120, l, 1.0), (180, l, 1.0)]));
}

/// PromQL `f(temperature[3m:1m])` lowered by `ql_ir`, over a gauge valued
/// 1, 5, 2, 4, 3 at 0s, 60s, …, 240s, evaluated at 120s and 240s.
async fn subquery(func: &str) -> Vec<(i64, String, f64)> {
    let points: Vec<_> = [1.0, 5.0, 2.0, 4.0, 3.0]
        .into_iter()
        .enumerate()
        .map(|(i, v)| gauge(60 * i as i64 * S, "a", v, json!({})))
        .collect();
    let params = ql_ir::PromqlParams::range(120 * S, 240 * S, 120 * S);
    let doc = ql_ir::promql_to_ir(&format!("{func}(temperature[3m:1m])"), &params).unwrap();
    let doc = serde_json::to_value(doc).unwrap();
    series_rows(&run(&points, doc).await.unwrap())
}

#[tokio::test]
async fn over_time_re_windows_the_inner_series_at_the_outer_instants() {
    // At 120s the window (-60s, 120s] reads the inner instants 0s, 60s and
    // 120s, which lie before the range start.
    let unnamed = r#"{"service.name":"svc"}"#;
    let got = subquery("max_over_time").await;
    assert_eq!(got, rows(&[(120, unnamed, 5.0), (240, unnamed, 4.0)]));
    let got = subquery("count_over_time").await;
    assert_eq!(got, rows(&[(120, unnamed, 3.0), (240, unnamed, 3.0)]));
    let named = r#"{"metric.name":"temperature","service.name":"svc"}"#;
    let got = subquery("last_over_time").await;
    assert_eq!(got, rows(&[(120, named, 2.0), (240, named, 3.0)]));
}

#[tokio::test]
async fn binop_with_a_number_is_per_value_and_drops_the_name_unless_filtering() {
    let binop = |op: &str, reverse: bool, bool: bool| json!([{ "binop": { "op": op, "right": 10.0, "reverse": reverse, "bool": bool } }]);
    // `10 - v`
    let got = over_two_series(binop("sub", true, false)).await;
    let want = [
        (60, UNNAMED_A, 9.0),
        (120, UNNAMED_A, 8.0),
        (60, UNNAMED_B, 0.0),
        (120, UNNAMED_B, -10.0),
    ];
    assert_eq!(got, rows(&want));
    // `10 < v` keeps v's value and name.
    let got = over_two_series(binop("lt", true, false)).await;
    assert_eq!(got, rows(&[(120, B, 20.0)]));
    let got = over_two_series(binop("ge", false, true)).await;
    let want = [
        (60, UNNAMED_A, 0.0),
        (120, UNNAMED_A, 0.0),
        (60, UNNAMED_B, 1.0),
        (120, UNNAMED_B, 1.0),
    ];
    assert_eq!(got, rows(&want));
}

#[tokio::test]
async fn binop_between_a_scalar_and_a_number_is_a_scalar() {
    let doc = |pipeline: JsonValue| {
        json!({
            "irVersion": 10, "from": "time", "step": "60s",
            "range": { "from": 60 * S, "to": 120 * S }, "result": "scalar", "pipeline": pipeline
        })
    };
    let div = json!([{ "binop": { "op": "div", "right": 0.0 } }]);
    let got = super::tests::scalar_rows(&run(&[], doc(div)).await.unwrap());
    assert_eq!(got, [(60, f64::INFINITY), (120, f64::INFINITY)]);
    let pow = json!([{ "binop": { "op": "pow", "right": 2.0, "reverse": true } }, {
        "binop": { "op": "eq", "right": 1.152921504606847e18, "bool": true }
    }]);
    let got = super::tests::scalar_rows(&run(&[], doc(pow)).await.unwrap());
    assert_eq!(got, [(60, 1.0), (120, 0.0)]);
}
