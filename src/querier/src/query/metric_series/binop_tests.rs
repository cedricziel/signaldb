//! `binop` with a sub-document operand: vector matching through
//! [`IrService`](crate::query::ir_planner::IrService).

use serde_json::{Value as JsonValue, json};

use super::tests::{Pt, S, gauge, run, series_rows};
use crate::query::error::QuerierError;

/// Metric `metric`'s gauge at 60s with `attrs`.
fn point(metric: &'static str, series: &'static str, value: f64, attrs: JsonValue) -> Pt {
    Pt {
        metric,
        ..gauge(60 * S, series, value, attrs)
    }
}

/// `latest` of one metric.
fn select(metric: &str) -> JsonValue {
    json!([
        { "where": { "field": "metric.name", "op": "eq", "value": metric } },
        { "sample": { "fn": "latest" } }
    ])
}

/// `x <binop> y` at 60s, `binop` given all but its right operand.
async fn x_op_y(points: &[Pt], mut binop: JsonValue) -> Result<Vec<(String, f64)>, QuerierError> {
    binop["right"] = json!({ "from": "metrics", "pipeline": select("y") });
    let mut pipeline = select("x").as_array().cloned().unwrap_or_default();
    pipeline.push(json!({ "binop": binop }));
    let doc = json!({
        "irVersion": 10, "from": "metrics", "step": "60s",
        "range": { "from": 60 * S, "to": 60 * S }, "result": "series", "pipeline": pipeline
    });
    let batch = run(points, doc).await?;
    Ok(series_rows(&batch)
        .into_iter()
        .map(|(_, l, v)| (l, v))
        .collect())
}

fn pairs(want: &[(&str, f64)]) -> Vec<(String, f64)> {
    want.iter().map(|(l, v)| (l.to_string(), *v)).collect()
}

/// `x` per (job, inst): api/1 = 10, api/2 = 20, web/1 = 30; `y` per job:
/// api = 2, web = 3.
fn jobs() -> Vec<Pt> {
    vec![
        point("x", "x1", 10.0, json!({"job": "api", "inst": "1"})),
        point("x", "x2", 20.0, json!({"job": "api", "inst": "2"})),
        point("x", "x3", 30.0, json!({"job": "web", "inst": "1"})),
        point("y", "y1", 2.0, json!({"job": "api"})),
        point("y", "y2", 3.0, json!({"job": "web"})),
    ]
}

#[tokio::test]
async fn group_left_divides_each_series_by_its_jobs_one_series() {
    let binop = json!({ "op": "div", "on": ["job"], "group": { "side": "left" } });
    let got = x_op_y(&jobs(), binop).await.unwrap();
    let want = [
        (r#"{"inst":"1","job":"api","service.name":"svc"}"#, 5.0),
        (r#"{"inst":"1","job":"web","service.name":"svc"}"#, 10.0),
        (r#"{"inst":"2","job":"api","service.name":"svc"}"#, 10.0),
    ];
    assert_eq!(got, pairs(&want));
}

#[tokio::test]
async fn one_to_one_matches_every_label_but_the_name() {
    let mut points = jobs();
    points.retain(|p| p.series != "x2");
    let got = x_op_y(&points, json!({ "op": "sub", "ignoring": ["inst"] }))
        .await
        .unwrap();
    let want = [
        (r#"{"job":"api","service.name":"svc"}"#, 8.0),
        (r#"{"job":"web","service.name":"svc"}"#, 27.0),
    ];
    assert_eq!(got, pairs(&want));
    // Without `ignoring`, `inst` keeps every pair apart.
    let got = x_op_y(&points, json!({ "op": "sub" })).await.unwrap();
    assert!(got.is_empty(), "{got:?}");
}

#[tokio::test]
async fn many_to_many_is_invalid_input() {
    let mut points = jobs();
    points.push(point("y", "y3", 4.0, json!({"job": "api", "inst": "9"})));
    let err = x_op_y(&points, json!({ "op": "add", "on": ["job"] }))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QuerierError::InvalidInput(m) if m.contains("many-to-many")),
        "{err}"
    );
}

#[tokio::test]
async fn comparisons_filter_with_the_name_or_yield_bool_without_it() {
    let binop = |bool: bool| json!({ "op": "gt", "on": ["job"], "group": { "side": "left" }, "bool": bool });
    let got = x_op_y(&jobs(), binop(false)).await.unwrap();
    let named = |job: &str, inst: &str| {
        format!(r#"{{"inst":"{inst}","job":"{job}","metric.name":"x","service.name":"svc"}}"#)
    };
    let want = [
        (named("api", "1"), 10.0),
        (named("web", "1"), 30.0),
        (named("api", "2"), 20.0),
    ];
    let mut want: Vec<_> = want.into_iter().collect();
    want.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(got, want);
    let got = x_op_y(&jobs(), binop(true)).await.unwrap();
    assert!(
        got.iter()
            .all(|(l, v)| !l.contains("metric.name") && *v == 1.0),
        "{got:?}"
    );
}

#[tokio::test]
async fn set_operators_keep_or_drop_whole_series() {
    let on = |op: &str| json!({ "op": op, "on": ["job"] });
    let mut points = jobs();
    points.retain(|p| p.series != "y2");
    let values = |rows: Vec<(String, f64)>| rows.into_iter().map(|(_, v)| v).collect::<Vec<_>>();
    assert_eq!(
        values(x_op_y(&points, on("and")).await.unwrap()),
        [10.0, 20.0]
    );
    assert_eq!(values(x_op_y(&points, on("unless")).await.unwrap()), [30.0]);
    let got = x_op_y(&points, on("or")).await.unwrap();
    assert_eq!(values(got), [10.0, 30.0, 20.0]);
}

#[tokio::test]
async fn a_scalar_operand_broadcasts_on_either_side() {
    let y_minus = |right: JsonValue, reverse: bool| {
        let mut pipeline = select("y").as_array().cloned().unwrap_or_default();
        pipeline.push(json!({ "binop": { "op": "sub", "right": right, "reverse": reverse } }));
        json!({
            "irVersion": 10, "from": "metrics", "step": "60s",
            "range": { "from": 60 * S, "to": 60 * S }, "result": "series", "pipeline": pipeline
        })
    };
    let values = |batch| series_rows(&batch).iter().map(|r| r.2).collect::<Vec<_>>();
    let constant = json!({ "from": "constant", "constant": 1.0 });
    let got = run(&jobs(), y_minus(constant, false)).await.unwrap();
    assert_eq!(values(got), [1.0, 2.0]);
    // `time() - y`
    let got = run(&jobs(), y_minus(json!({ "from": "time" }), true))
        .await
        .unwrap();
    assert_eq!(values(got), [58.0, 57.0]);
}

#[tokio::test]
async fn two_scalars_combine_into_a_scalar() {
    let doc = json!({
        "irVersion": 10, "from": "constant", "constant": 1.0, "step": "60s",
        "range": { "from": 60 * S, "to": 120 * S }, "result": "scalar",
        "pipeline": [{ "binop": { "op": "add", "right": { "from": "time" } } }]
    });
    let got = super::tests::scalar_rows(&run(&[], doc).await.unwrap());
    assert_eq!(got, [(60, 61.0), (120, 121.0)]);
}

#[tokio::test]
async fn on_with_ignoring_is_invalid_input() {
    let err = x_op_y(
        &jobs(),
        json!({ "op": "add", "on": ["job"], "ignoring": ["inst"] }),
    )
    .await
    .unwrap_err();
    assert!(matches!(&err, QuerierError::InvalidInput(_)), "{err}");
}

#[tokio::test]
async fn an_operand_with_two_series_of_one_labelset_is_invalid_input() {
    // `avg_over_time({__name__=~"x|z"}[1m]) + y`: x and z lose their names
    // to one label set.
    let points = [
        point("x", "x1", 1.0, json!({"job": "api"})),
        point("z", "z1", 2.0, json!({"job": "api"})),
        point("y", "y1", 3.0, json!({"job": "api"})),
    ];
    let doc = json!({
        "irVersion": 10, "from": "metrics", "step": "60s",
        "range": { "from": 60 * S, "to": 60 * S }, "result": "series",
        "pipeline": [
            { "where": { "field": "metric.name", "op": "in", "value": ["x", "z"] } },
            { "sample": { "fn": "avg_over_time", "window": "1m" } },
            { "binop": { "op": "add", "right": { "from": "metrics", "pipeline": select("y") } } }
        ]
    });
    let err = run(&points, doc).await.unwrap_err();
    assert!(
        matches!(&err, QuerierError::InvalidInput(m) if m.contains("same labelset")),
        "{err}"
    );
}
