//! End-to-end fixtures for metric Series planning: a wide `metrics` table on
//! the typed attribute layout, queried through [`IrService`].

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::compute::{cast, concat_batches};
use datafusion::arrow::datatypes::DataType;
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
    pub resource: JsonValue,
    pub attrs: JsonValue,
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

pub(super) fn ctx(points: &[Pt]) -> SessionContext {
    let batch = batch(points);
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
    let doc = serde_json::from_value(doc).unwrap();
    let (df, _) = IrService::new(ctx(points))
        .plan(&doc, "t", "d", 0)
        .await?
        .expect("the metrics table is registered");
    let batches = df.collect().await.map_err(QuerierError::QueryFailed)?;
    Ok(concat_batches(&batches[0].schema(), &batches).unwrap())
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
