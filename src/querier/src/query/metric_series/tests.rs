//! End-to-end fixtures for metric Series planning: a wide `metrics` table on
//! the typed attribute layout, queried through [`IrService`].

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::compute::{cast, concat_batches};
use datafusion::arrow::datatypes::{DataType, Float64Type, TimestampNanosecondType};
use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use serde_json::{Value as JsonValue, json};

use crate::query::error::QuerierError;
use crate::query::ir_planner::IrService;

pub(super) const S: i64 = 1_000_000_000;

/// One data point of the fixture table.
#[derive(Clone)]
pub(super) struct Pt {
    pub ts: i64,
    pub series: &'static str,
    pub metric: &'static str,
    pub kind: &'static str,
    pub value: Option<f64>,
    pub count: Option<i64>,
    pub sum: Option<f64>,
    pub start: Option<i64>,
    pub temporality: Option<i32>,
    pub monotonic: Option<bool>,
    pub flags: i32,
    pub scope: Option<&'static str>,
    pub scope_version: Option<&'static str>,
    pub resource: JsonValue,
    pub attrs: JsonValue,
}

/// A cumulative monotonic sum point with no start time.
pub(super) fn counter(ts: i64, series: &'static str, value: f64, attrs: JsonValue) -> Pt {
    Pt {
        metric: "requests",
        kind: "sum",
        temporality: Some(2),
        monotonic: Some(true),
        ..gauge(ts, series, value, attrs)
    }
}

/// A gauge point of service `svc`.
pub(super) fn gauge(ts: i64, series: &'static str, value: f64, attrs: JsonValue) -> Pt {
    Pt {
        ts,
        series,
        metric: "temperature",
        kind: "gauge",
        value: Some(value),
        count: None,
        sum: None,
        start: None,
        temporality: None,
        monotonic: None,
        flags: 0,
        scope: None,
        scope_version: None,
        resource: json!({"service.name": "svc"}),
        attrs,
    }
}

/// The wide `metrics` table rows for `points`.
pub(super) fn batch(points: &[Pt]) -> RecordBatch {
    fn col<T, A: From<Vec<T>> + Array + 'static>(points: &[Pt], f: impl Fn(&Pt) -> T) -> ArrayRef {
        Arc::new(A::from(points.iter().map(f).collect::<Vec<_>>()))
    }
    type Ts = TimestampNanosecondArray;
    let mut columns: Vec<(String, ArrayRef)> = vec![
        ("timestamp".into(), col::<_, Ts>(points, |p| p.ts)),
        ("start_timestamp".into(), col::<_, Ts>(points, |p| p.start)),
        (
            "service_name".into(),
            col::<_, StringArray>(points, |_| "svc"),
        ),
        (
            "metric_name".into(),
            col::<_, StringArray>(points, |p| p.metric),
        ),
        (
            "metric_type".into(),
            col::<_, StringArray>(points, |p| p.kind),
        ),
        (
            "series_id".into(),
            col::<_, StringArray>(points, |p| p.series),
        ),
        (
            "scope_name".into(),
            col::<_, StringArray>(points, |p| p.scope),
        ),
        (
            "scope_version".into(),
            col::<_, StringArray>(points, |p| p.scope_version),
        ),
        ("value".into(), col::<_, Float64Array>(points, |p| p.value)),
        ("count".into(), col::<_, Int64Array>(points, |p| p.count)),
        ("sum".into(), col::<_, Float64Array>(points, |p| p.sum)),
        ("flags".into(), col::<_, Int32Array>(points, |p| p.flags)),
        (
            "aggregation_temporality".into(),
            col::<_, Int32Array>(points, |p| p.temporality),
        ),
        (
            "is_monotonic".into(),
            col::<_, BooleanArray>(points, |p| p.monotonic),
        ),
    ];
    for (container, get) in [
        (
            "resource_attributes",
            (|p: &Pt| &p.resource) as fn(&Pt) -> &JsonValue,
        ),
        ("attributes", |p: &Pt| &p.attrs),
    ] {
        let rows: Vec<_> = points.iter().map(|p| get(p).as_object().cloned()).collect();
        let (f, c) = common::testing::typed_attribute_columns_from(
            "metrics",
            "physical-v4",
            container,
            &rows,
        );
        columns.extend(f.iter().map(|f| f.name().clone()).zip(c));
    }
    RecordBatch::try_from_iter(columns).unwrap()
}

pub(super) fn ctx(batch: RecordBatch) -> SessionContext {
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap();
    let schema = Arc::new(MemorySchemaProvider::new());
    schema
        .register_table("metrics".to_string(), Arc::new(table))
        .unwrap();
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog.register_schema("d", schema).unwrap();
    let ctx = SessionContext::new();
    ctx.register_catalog("t", catalog);
    ctx
}

/// Plan and run `doc` over `points`.
pub(super) async fn run(points: &[Pt], doc: JsonValue) -> Result<RecordBatch, QuerierError> {
    run_batch(batch(points), doc).await
}

/// Plan and run `doc` over a metrics table holding `batch`.
pub(super) async fn run_batch(
    batch: RecordBatch,
    doc: JsonValue,
) -> Result<RecordBatch, QuerierError> {
    let doc = serde_json::from_value(doc).unwrap();
    let (df, _) = IrService::new(ctx(batch))
        .plan(&doc, "t", "d", 0)
        .await?
        .expect("the metrics table is registered");
    let schema = df.schema().inner().clone();
    let batches = df.collect().await.map_err(QuerierError::from)?;
    Ok(concat_batches(&schema, &batches).unwrap())
}

pub(super) fn strings(batch: &RecordBatch, name: &str) -> Vec<String> {
    let col = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("no {name} in {:?}", batch.schema()));
    let col = cast(col, &DataType::Utf8).unwrap();
    let col = col.as_string::<i32>();
    (0..col.len())
        .map(|i| {
            if col.is_valid(i) {
                col.value(i).to_string()
            } else {
                "null".to_string()
            }
        })
        .collect()
}

#[tokio::test]
async fn the_point_qualifier_reads_a_point_attribute_shadowed_by_a_metric_field() {
    let points = [
        gauge(S, "a", 1.0, json!({"metric.name": "p"})),
        gauge(S, "b", 2.0, json!({})),
    ];
    let batch = run(
        &points,
        json!({
            "irVersion": 1, "from": "metrics", "range": { "from": 0, "to": 10 * S },
            "result": "rows", "fields": ["metric.name", "metric.value"],
            "pipeline": [{ "where": { "field": "point.metric.name", "op": "eq", "value": "p" } }]
        }),
    )
    .await
    .unwrap();
    assert_eq!(strings(&batch, "metric_name"), ["temperature"]);
    assert_eq!(strings(&batch, "value"), ["1.0"]);
}

/// A `sample` document over `[from, to]` seconds with a 60s step.
fn sample_doc(from: i64, to: i64, sample: JsonValue) -> JsonValue {
    json!({
        "irVersion": 10, "from": "metrics", "step": "60s",
        "range": { "from": from * S, "to": to * S },
        "result": "series", "pipeline": [{ "sample": sample }]
    })
}

/// `(bucket seconds, labels, value)` rows of a Series frame.
fn series_rows(batch: &RecordBatch) -> Vec<(i64, String, f64)> {
    let names: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, ["bucket", "__labels", "value"]);
    let bucket = batch.column(0).as_primitive::<TimestampNanosecondType>();
    let labels = batch.column(1).as_string::<i32>();
    let value = batch.column(2).as_primitive::<Float64Type>();
    (0..batch.num_rows())
        .map(|i| {
            (
                bucket.value(i) / S,
                labels.value(i).to_string(),
                value.value(i),
            )
        })
        .collect()
}

#[tokio::test]
async fn rate_over_two_series_of_one_service_stays_per_series() {
    let mut points = Vec::new();
    for (i, ts) in [30, 60, 90, 120].into_iter().enumerate() {
        let v = 30.0 * (i + 1) as f64;
        points.push(counter(ts * S, "a", v, json!({"code": 200})));
        points.push(counter(ts * S, "b", 2.0 * v, json!({"code": 500})));
    }
    let doc = sample_doc(60, 120, json!({ "fn": "rate", "window": "60s" }));
    let rows = series_rows(&run(&points, doc).await.unwrap());
    // A rate is no longer the metric: `metric.name` is dropped.
    let a = r#"{"code":"200","service.name":"svc"}"#;
    let b = r#"{"code":"500","service.name":"svc"}"#;
    let want = [(60, a, 0.5), (120, a, 0.5), (60, b, 1.0), (120, b, 1.0)];
    let want: Vec<_> = want
        .iter()
        .map(|(t, l, v)| (*t, l.to_string(), *v))
        .collect();
    assert_eq!(rows, want);
}

#[tokio::test]
async fn a_limit_after_sample_keeps_the_first_rows_of_the_sorted_frame() {
    let points: Vec<_> = (0..40)
        .rev()
        .map(|i| gauge(60 * S, "a", i as f64, json!({ "k": format!("{i:02}") })))
        .collect();
    let mut doc = sample_doc(60, 60, json!({ "fn": "latest" }));
    doc["pipeline"]
        .as_array_mut()
        .unwrap()
        .push(json!({ "limit": 3 }));
    let values: Vec<_> = series_rows(&run(&points, doc).await.unwrap())
        .into_iter()
        .map(|(_, _, v)| v)
        .collect();
    assert_eq!(values, [0.0, 1.0, 2.0]);
}

#[tokio::test]
async fn latest_takes_the_last_point_within_the_lookback() {
    let points = [
        gauge(0, "a", 1.0, json!({})),
        gauge(150 * S, "a", 2.0, json!({})),
    ];
    let run_latest = |sample| run(&points, sample_doc(240, 240, sample));
    let rows = series_rows(&run_latest(json!({ "fn": "latest" })).await.unwrap());
    let labels = r#"{"metric.name":"temperature","service.name":"svc"}"#;
    assert_eq!(rows, [(240, labels.to_string(), 2.0)]);
    let short = run_latest(json!({ "fn": "latest", "lookback": "1m" }));
    assert_eq!(short.await.unwrap().num_rows(), 0);
}

#[tokio::test]
async fn offset_shifts_the_read_window_and_at_pins_it() {
    let points = [
        gauge(0, "a", 1.0, json!({})),
        gauge(150 * S, "a", 2.0, json!({})),
    ];
    let values = |sample| async {
        let batch = run(&points, sample_doc(180, 240, sample)).await.unwrap();
        series_rows(&batch)
            .into_iter()
            .map(|(t, _, v)| (t, v))
            .collect::<Vec<_>>()
    };
    let offset = values(json!({ "fn": "latest", "offset": "2m" })).await;
    assert_eq!(offset, [(180, 1.0), (240, 1.0)]);
    let at = values(json!({ "fn": "latest", "at": 30 * S })).await;
    assert_eq!(at, [(180, 1.0), (240, 1.0)]);
}

/// `(bucket seconds, value)` rows of a Scalar frame.
fn scalar_rows(batch: &RecordBatch) -> Vec<(i64, f64)> {
    let names: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, ["bucket", "value"]);
    let bucket = batch.column(0).as_primitive::<TimestampNanosecondType>();
    let value = batch.column(1).as_primitive::<Float64Type>();
    (0..batch.num_rows())
        .map(|i| (bucket.value(i) / S, value.value(i)))
        .collect()
}

fn scalar_doc(pipeline: JsonValue) -> JsonValue {
    json!({
        "irVersion": 10, "from": "metrics", "step": "60s",
        "range": { "from": 60 * S, "to": 180 * S },
        "result": "scalar", "pipeline": pipeline
    })
}

#[tokio::test]
async fn scalar_is_the_only_series_value_else_nan() {
    let points = [
        gauge(50 * S, "a", 1.0, json!({})),
        gauge(110 * S, "a", 2.0, json!({})),
        gauge(110 * S, "b", 3.0, json!({"k": "b"})),
    ];
    let latest = json!({ "sample": { "fn": "latest", "lookback": "30s" } });
    let batch = run(&points, scalar_doc(json!([latest, { "scalar": {} }])))
        .await
        .unwrap();
    let rows = scalar_rows(&batch);
    // One series at 60s, two at 120s, none at 180s.
    assert_eq!(rows[0], (60, 1.0));
    assert_eq!(rows.iter().map(|r| r.0).collect::<Vec<_>>(), [60, 120, 180]);
    assert!(rows[1].1.is_nan() && rows[2].1.is_nan());
}

#[tokio::test]
async fn vector_turns_a_scalar_into_one_unlabelled_series() {
    let points = [gauge(50 * S, "a", 1.0, json!({}))];
    let pipeline = json!([
        { "sample": { "fn": "latest", "lookback": "30s" } }, { "scalar": {} }, { "vector": {} }
    ]);
    let mut doc = scalar_doc(pipeline);
    doc["result"] = json!("series");
    let rows = series_rows(&run(&points, doc).await.unwrap());
    assert_eq!(rows[0], (60, "{}".to_string(), 1.0));
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn time_and_constant_are_scalars_over_the_document_instants() {
    let pseudo = |from: &str, extra: JsonValue| {
        let mut doc = scalar_doc(json!([]));
        doc["from"] = json!(from);
        if let JsonValue::Object(extra) = extra {
            doc.as_object_mut().unwrap().extend(extra);
        }
        doc
    };
    let time = run(&[], pseudo("time", json!({}))).await.unwrap();
    assert_eq!(scalar_rows(&time), [(60, 60.0), (120, 120.0), (180, 180.0)]);
    let constant = pseudo("constant", json!({ "constant": 2.5 }));
    let constant = run(&[], constant).await.unwrap();
    assert_eq!(scalar_rows(&constant), [(60, 2.5), (120, 2.5), (180, 2.5)]);
    let vector = pseudo(
        "time",
        json!({ "result": "series", "pipeline": [{ "vector": {} }] }),
    );
    let rows = series_rows(&run(&[], vector).await.unwrap());
    assert_eq!(rows[2], (180, "{}".to_string(), 180.0));
}

#[tokio::test]
async fn scalar_over_an_empty_series_is_nan_at_every_instant() {
    let points = [gauge(0, "a", 1.0, json!({}))];
    let latest = json!({ "sample": { "fn": "latest", "lookback": "1s" } });
    let batch = run(&points, scalar_doc(json!([latest, { "scalar": {} }])))
        .await
        .unwrap();
    let rows = scalar_rows(&batch);
    assert_eq!(rows.iter().map(|r| r.0).collect::<Vec<_>>(), [60, 120, 180]);
    assert!(rows.iter().all(|r| r.1.is_nan()), "{rows:?}");
}

#[tokio::test]
async fn scalar_over_a_step_aggregate_is_not_supported() {
    let points = [gauge(60 * S, "a", 1.0, json!({}))];
    let aggregate = json!({ "aggregate": {
        "by": [], "aggs": [{ "fn": "sum", "of": "metric.value", "as": "v" }], "step": "60s"
    } });
    let err = run(&points, scalar_doc(json!([aggregate, { "scalar": {} }])))
        .await
        .unwrap_err();
    assert!(matches!(err, QuerierError::Unsupported(_)), "{err}");
}

/// Every entry point that enumerates instants refuses more than 11000.
#[tokio::test]
async fn more_than_11000_instants_is_invalid_input() {
    let latest = json!({ "sample": { "fn": "latest", "lookback": "1s" } });
    let docs = [
        json!({ "from": "time" }),
        json!({ "from": "constant", "constant": 1.0 }),
        json!({ "pipeline": [latest, { "scalar": {} }] }),
        json!({ "result": "series", "pipeline": [{ "sample": { "fn": "latest", "at": 0 } }] }),
    ];
    for extra in docs {
        let mut doc = scalar_doc(json!([]));
        doc["step"] = json!("10ms");
        doc.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let err = run(&[], doc.clone()).await.unwrap_err();
        assert!(
            matches!(&err, QuerierError::InvalidInput(m) if m.contains("11000 instants")),
            "{doc}: {err}"
        );
    }
}

/// A point flagged NO_RECORDED_VALUE (a staleness marker).
fn stale(p: Pt) -> Pt {
    Pt { flags: 1, ..p }
}

#[tokio::test]
async fn range_functions_skip_stale_points_and_a_stale_newest_point_ends_latest() {
    let points = [
        gauge(30 * S, "a", 1.0, json!({})),
        stale(gauge(90 * S, "a", 100.0, json!({}))),
    ];
    let values = |sample| async {
        let batch = run(&points, sample_doc(60, 120, sample)).await.unwrap();
        series_rows(&batch)
            .into_iter()
            .map(|(t, _, v)| (t, v))
            .collect::<Vec<_>>()
    };
    let sum = values(json!({ "fn": "sum_over_time", "window": "2m" })).await;
    assert_eq!(sum, [(60, 1.0), (120, 1.0)]);
    let latest = values(json!({ "fn": "latest" })).await;
    assert_eq!(latest, [(60, 1.0)]);
}

/// `(bucket seconds, value)` of a `sample` Series over `[from, to]`.
async fn sample_values(points: &[Pt], from: i64, to: i64, sample: JsonValue) -> Vec<(i64, f64)> {
    let batch = run(points, sample_doc(from, to, sample)).await.unwrap();
    series_rows(&batch)
        .into_iter()
        .map(|(t, _, v)| (t, v))
        .collect()
}

#[tokio::test]
async fn two_series_differing_only_by_scope_keep_apart_by_their_scope_labels() {
    let points = [
        Pt {
            scope: Some("lib.a"),
            ..gauge(50 * S, "a", 1.0, json!({}))
        },
        Pt {
            scope: Some("lib.b"),
            ..gauge(50 * S, "b", 2.0, json!({}))
        },
    ];
    let batch = run(&points, sample_doc(60, 60, json!({ "fn": "latest" })))
        .await
        .unwrap();
    let labels = |scope| {
        format!(
            r#"{{"metric.name":"temperature","otel.scope.name":"{scope}","service.name":"svc"}}"#
        )
    };
    assert_eq!(
        series_rows(&batch),
        [(60, labels("lib.a"), 1.0), (60, labels("lib.b"), 2.0)]
    );
}

#[tokio::test]
async fn last_over_time_keeps_the_metric_name_and_other_functions_drop_it() {
    let points = [
        gauge(20 * S, "a", 1.0, json!({})),
        gauge(50 * S, "a", 2.0, json!({})),
    ];
    for (func, keeps) in [
        ("last_over_time", true),
        ("max_over_time", false),
        ("delta", false),
    ] {
        let doc = sample_doc(60, 60, json!({ "fn": func, "window": "60s" }));
        let rows = series_rows(&run(&points, doc).await.unwrap());
        let named = rows.iter().all(|(_, l, _)| l.contains("metric.name"));
        assert!(!rows.is_empty(), "{func}");
        assert_eq!(named, keeps, "{func}: {rows:?}");
    }
}

/// A cumulative histogram point: only `count` and `sum` are set.
fn histogram(ts: i64, count: i64, sum: f64) -> Pt {
    Pt {
        kind: "histogram",
        metric: "latency",
        value: None,
        count: Some(count),
        sum: Some(sum),
        temporality: Some(2),
        ..gauge(ts, "h", 0.0, json!({}))
    }
}

#[tokio::test]
async fn count_and_sum_of_a_cumulative_histogram_behave_as_counters() {
    let (counts, sums) = ([4, 9, 12, 20], [1.0, 3.5, 4.0, 9.0]);
    let at = |i: usize| (30 * (i as i64 + 1)) * S;
    let hist: Vec<_> = (0..4)
        .map(|i| histogram(at(i), counts[i], sums[i]))
        .collect();
    let as_counter = |v: &dyn Fn(usize) -> f64| -> Vec<Pt> {
        (0..4)
            .map(|i| counter(at(i), "c", v(i), json!({})))
            .collect()
    };
    for (of, func, expected) in [
        ("metric.count", "rate", as_counter(&|i| counts[i] as f64)),
        ("metric.sum", "increase", as_counter(&|i| sums[i])),
    ] {
        let sample = |of: Option<&str>| {
            let mut s = json!({ "fn": func, "window": "90s" });
            if let Some(of) = of {
                s["of"] = json!(of);
            }
            s
        };
        let got = sample_values(&hist, 60, 120, sample(Some(of))).await;
        let want = sample_values(&expected, 60, 120, sample(None)).await;
        assert_eq!(got.len(), 2, "{of}");
        assert_eq!(got, want, "{of}");
    }
}

#[tokio::test]
async fn instants_start_at_from_even_off_the_step_grid() {
    let points = [gauge(50 * S, "a", 1.0, json!({}))];
    let got = sample_values(&points, 70, 190, json!({ "fn": "latest" })).await;
    assert_eq!(got, [(70, 1.0), (130, 1.0), (190, 1.0)]);
}

#[tokio::test]
async fn at_reads_far_outside_the_range_and_combines_with_offset() {
    let points = [
        gauge(5_000 * S, "a", 1.0, json!({})),
        gauge(5_100 * S, "a", 2.0, json!({})),
    ];
    let at = |extra: JsonValue| {
        let mut s = json!({ "fn": "latest", "at": 5_100 * S });
        s.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        s
    };
    let pinned = sample_values(&points, 60, 120, at(json!({}))).await;
    assert_eq!(pinned, [(60, 2.0), (120, 2.0)]);
    let shifted = sample_values(&points, 60, 120, at(json!({ "offset": "1m" }))).await;
    assert_eq!(shifted, [(60, 1.0), (120, 1.0)]);
    let empty = at(json!({ "at": 1_000_000 * S }));
    assert!(sample_values(&points, 60, 120, empty).await.is_empty());
}

#[tokio::test]
async fn nan_and_infinities_pass_through_sample_functions() {
    let points = [
        gauge(50 * S, "nan", f64::NAN, json!({"k": "nan"})),
        gauge(50 * S, "inf", f64::INFINITY, json!({"k": "inf"})),
        gauge(50 * S, "-inf", f64::NEG_INFINITY, json!({"k": "-inf"})),
    ];
    let latest = json!({ "fn": "latest" });
    let sum = json!({ "fn": "sum_over_time", "window": "60s" });
    for (func, sample) in [("latest", latest), ("sum_over_time", sum)] {
        let got = sample_values(&points, 60, 60, sample).await;
        let got: Vec<_> = got.into_iter().map(|(_, v)| v).collect();
        // Sorted by label set: `-inf`, `inf`, `nan`.
        assert_eq!(got[..2], [f64::NEG_INFINITY, f64::INFINITY], "{func}");
        assert!(got[2].is_nan(), "{func}: {got:?}");
    }
}

#[tokio::test]
async fn a_metrics_table_without_the_sampled_column_is_a_clear_error() {
    let full = batch(&[gauge(50 * S, "a", 1.0, json!({}))]);
    let without = |name: &str| {
        let keep: Vec<_> = (0..full.num_columns())
            .filter(|i| full.schema().field(*i).name() != name)
            .collect();
        full.project(&keep).unwrap()
    };
    let doc = |of: &str| sample_doc(60, 60, json!({ "fn": "latest", "of": of }));
    let err = run_batch(without("count"), doc("metric.count"))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, QuerierError::InvalidInput(m) if m.contains("`count`")),
        "{err}"
    );
    // Series are keyed on their label sets, so `series_id` is not needed.
    let rows = run_batch(without("series_id"), doc("metric.value")).await;
    assert_eq!(series_rows(&rows.unwrap()).len(), 1);
}

#[tokio::test]
async fn rate_over_a_gauge_is_invalid_input() {
    let points = [
        gauge(30 * S, "a", 1.0, json!({})),
        gauge(60 * S, "a", 2.0, json!({})),
    ];
    let doc = sample_doc(60, 60, json!({ "fn": "rate", "window": "60s" }));
    let err = run(&points, doc).await.unwrap_err();
    assert!(
        matches!(&err, QuerierError::InvalidInput(m) if m.contains("delta")),
        "{err}"
    );
}

/// Series whose label sets render identically, as Prometheus sees one series.
#[tokio::test]
async fn series_indistinguishable_by_their_labels_are_one_series() {
    let as_requests = |p: Pt| Pt {
        metric: "requests",
        ..p
    };
    let cases: [(&str, Vec<Pt>); 3] = [
        (
            "int vs string attribute",
            vec![
                counter(30 * S, "int", 1.0, json!({"code": 200})),
                counter(60 * S, "str", 4.0, json!({"code": "200"})),
                counter(90 * S, "int", 7.0, json!({"code": 200})),
            ],
        ),
        (
            "gauge vs sum",
            vec![
                as_requests(gauge(30 * S, "g", 1.0, json!({"code": 200}))),
                counter(60 * S, "c", 4.0, json!({"code": 200})),
                counter(90 * S, "c", 7.0, json!({"code": 200})),
            ],
        ),
        (
            "empty vs absent scope version",
            vec![
                Pt {
                    scope_version: Some(""),
                    ..counter(30 * S, "empty", 1.0, json!({"code": 200}))
                },
                counter(60 * S, "none", 4.0, json!({"code": 200})),
                Pt {
                    scope_version: Some(""),
                    ..counter(90 * S, "empty", 7.0, json!({"code": 200}))
                },
            ],
        ),
    ];
    let labels = r#"{"code":"200","metric.name":"requests","service.name":"svc"}"#;
    for (case, points) in cases {
        let latest = run(&points, sample_doc(120, 120, json!({ "fn": "latest" })));
        let latest = series_rows(&latest.await.unwrap());
        assert_eq!(latest, [(120, labels.to_string(), 7.0)], "{case}");
        let rate = json!({ "fn": "rate", "window": "100s" });
        let rate = series_rows(&run(&points, sample_doc(120, 120, rate)).await.unwrap());
        let unnamed = r#"{"code":"200","service.name":"svc"}"#;
        // The merged, time-ordered points 1, 4, 7: an increase of 6.
        assert_eq!(rate, [(120, unnamed.to_string(), 0.06)], "{case}");
    }
}

/// Dropping `metric.name` can make two metrics' results collide; the Series
/// frame keeps both rows so the router rejects them, as Prometheus does.
#[tokio::test]
async fn a_collision_after_dropping_the_name_keeps_both_rows() {
    let other = |p: Pt| Pt {
        metric: "other",
        ..p
    };
    let points = [
        counter(30 * S, "a", 1.0, json!({})),
        counter(90 * S, "a", 7.0, json!({})),
        other(counter(30 * S, "b", 1.0, json!({}))),
        other(counter(90 * S, "b", 4.0, json!({}))),
    ];
    let doc = sample_doc(120, 120, json!({ "fn": "rate", "window": "100s" }));
    let rows = series_rows(&run(&points, doc).await.unwrap());
    let unnamed = r#"{"service.name":"svc"}"#;
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert!(
        rows.iter().all(|(t, l, _)| *t == 120 && l == unnamed),
        "{rows:?}"
    );
}

/// A window far longer than the range reads back over many steps, but the
/// range itself evaluates only a handful of instants.
#[tokio::test]
async fn a_long_window_over_a_short_range_is_fine() {
    let points = [
        counter(1_000 * S, "a", 1.0, json!({})),
        counter(100_000 * S, "a", 3.0, json!({})),
    ];
    let mut doc = sample_doc(
        100_000,
        100_300,
        json!({ "fn": "increase", "window": "3d" }),
    );
    doc["step"] = json!("15s");
    let rows = series_rows(&run(&points, doc).await.unwrap());
    assert_eq!(rows.len(), 21, "{rows:?}");
    assert!(rows.iter().all(|(_, _, v)| *v == 2.0), "{rows:?}");
}

#[tokio::test]
async fn a_staleness_marker_without_a_value_ends_latest() {
    let points = [
        gauge(30 * S, "a", 1.0, json!({})),
        stale(Pt {
            value: None,
            ..gauge(90 * S, "a", 0.0, json!({}))
        }),
    ];
    let latest = sample_values(&points, 60, 120, json!({ "fn": "latest" })).await;
    assert_eq!(latest, [(60, 1.0)]);
}

/// A recorded point and a staleness marker at the same timestamp: the
/// marker wins, whatever the input order, so the series has ended.
#[tokio::test]
async fn a_marker_tied_with_a_point_ends_latest() {
    let point = gauge(90 * S, "a", 2.0, json!({}));
    let marker = stale(gauge(90 * S, "a", 3.0, json!({})));
    let early = gauge(30 * S, "a", 1.0, json!({}));
    for points in [
        [early.clone(), point.clone(), marker.clone()],
        [marker.clone(), point.clone(), early.clone()],
    ] {
        let latest = sample_values(&points, 60, 120, json!({ "fn": "latest" })).await;
        assert_eq!(latest, [(60, 1.0)]);
    }
}

#[tokio::test]
async fn rate_and_increase_read_across_a_staleness_marker() {
    let points = [
        counter(30 * S, "a", 1.0, json!({})),
        stale(counter(60 * S, "a", f64::NAN, json!({}))),
        counter(90 * S, "a", 7.0, json!({})),
    ];
    let window = |func| json!({ "fn": func, "window": "100s" });
    let increase = sample_values(&points, 120, 120, window("increase")).await;
    assert_eq!(increase, [(120, 6.0)]);
    let rate = sample_values(&points, 120, 120, window("rate")).await;
    assert_eq!(rate, [(120, 0.06)]);
}

/// The scan reads exactly what `sample` reads, not the hull of the range and
/// a far-away `at`.
#[test]
fn the_scan_window_is_the_sample_read_window() {
    use crate::query::ir_planner::ResolvedWindow;
    let window = ResolvedWindow {
        start_ns: 10_000 * S,
        end_ns: 10_600 * S,
    };
    let scan = |sample: JsonValue| {
        let doc = serde_json::from_value(sample_doc(0, 0, sample)).unwrap();
        let w = super::sample::scan_window(&doc, window, 20_000 * S).unwrap();
        (w.start_ns / S, w.end_ns / S)
    };
    let at = json!({ "fn": "rate", "window": "1m", "at": 100 * S, "offset": "30s" });
    assert_eq!(scan(at), (10, 70));
    let offset = json!({ "fn": "latest", "lookback": "1m", "offset": "1h" });
    assert_eq!(scan(offset), (6_340, 7_000));
}
