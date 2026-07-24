//! # Metrics Query Service (PromQL)
//!
//! DataFusion-backed execution of PromQL queries against the tenant's
//! metrics Iceberg tables. Parses PromQL, lowers it to a
//! [`MetricPlan`](super::promql::MetricPlan), and runs a bucketed
//! aggregation over the union of the gauge and sum tables — the same
//! DataFrame-first approach as the trace/log/profile paths.
//!
//! The result is a matrix: one row per (time bucket, series) with a
//! `bucket` timestamp, `metric_name`, the grouping columns, and a
//! `value`. The router shapes that into Prometheus matrix JSON.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::{Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, IntervalMonthDayNano, TimeUnit};
use datafusion::functions::datetime::expr_fn::date_bin;
use datafusion::functions::regex::expr_fn::regexp_like;
use datafusion::functions::string::expr_fn::contains;
use datafusion::functions_aggregate::expr_fn::{
    avg, count, first_value, last_value, max, min, sum,
};
use datafusion::logical_expr::{Expr, SortExpr, col, lit, not};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::scalar::ScalarValue;

use super::promql::{Grouping, LabelMatch, MatchKind, MetricAgg, MetricPlan, plan_promql};
use super::{error::QuerierError, table_ref::build_table_reference};

/// The metrics tables a PromQL query scans (gauge + sum cover counters
/// and gauges; histograms are handled separately with histogram_quantile).
const METRIC_TABLES: &[&str] = &["metrics_gauge", "metrics_sum"];

/// Columns projected from each metrics table before the union.
const SCAN_COLUMNS: &[&str] = &[
    "timestamp",
    "service_name",
    "metric_name",
    "value",
    "attributes",
    "resource_attributes",
];

const LOG_ATTRIBUTES: &str = "attributes";
const RESOURCE_ATTRIBUTES: &str = "resource_attributes";

/// Upper bound on distinct attribute documents scanned for discovery.
const LABEL_SCAN_LIMIT: usize = 1000;

/// Executes PromQL queries against the metrics tables.
pub struct MetricsService {
    session_context: Arc<SessionContext>,
}

impl Debug for MetricsService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsService")
            .field("session_context", &"set")
            .finish()
    }
}

impl Clone for MetricsService {
    fn clone(&self) -> Self {
        Self {
            session_context: Arc::clone(&self.session_context),
        }
    }
}

impl MetricsService {
    pub fn new(session_context: SessionContext) -> Self {
        Self {
            session_context: Arc::new(session_context),
        }
    }

    /// Execute a PromQL range query, returning matrix RecordBatches.
    /// `start`/`end` are inclusive unix nanoseconds; `step` is the bucket
    /// width in nanoseconds.
    pub async fn query_range(
        &self,
        query: &str,
        start: i64,
        end: i64,
        step: i64,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<RecordBatch>, QuerierError> {
        if step <= 0 {
            return Err(QuerierError::InvalidInput(
                "step must be a positive nanosecond interval".to_string(),
            ));
        }
        let plan = plan_promql(query)?;
        let group_cols = self.group_columns(&plan)?;

        let df = self.scan_union(tenant_slug, dataset_slug).await?;
        let df = apply_filters(df, &plan, start, end)?;

        // Bucket timestamps into step-aligned windows (cast the
        // microsecond storage timestamp to nanoseconds first).
        let bucket = bucket_expr(step);

        let df = if let Some(range) = plan.range {
            self.range_query(df, bucket, range, plan.aggregate, &group_cols)?
        } else {
            self.simple_query(df, bucket, plan.aggregate, &group_cols)?
        };

        df.sort(vec![SortExpr::new(col("bucket"), true, true)])
            .map_err(QuerierError::QueryFailed)?
            .collect()
            .await
            .map_err(QuerierError::QueryFailed)
    }

    /// Aggregate `value` per (bucket, metric_name, group) directly.
    fn simple_query(
        &self,
        df: DataFrame,
        bucket: Expr,
        aggregate: MetricAgg,
        group_cols: &[&str],
    ) -> Result<DataFrame, QuerierError> {
        let mut group_exprs = vec![bucket, col("metric_name")];
        group_exprs.extend(group_cols.iter().map(|c| col(*c)));

        let value = aggregate_expr(aggregate).alias("value");
        let df = df
            .aggregate(group_exprs, vec![value])
            .map_err(QuerierError::QueryFailed)?;

        // Normalize `value` to Float64 (count aggregates to Int64).
        let mut proj = vec![col("bucket"), col("metric_name")];
        proj.extend(group_cols.iter().map(|c| col(*c)));
        proj.push(cast_value_f64(col("value")).alias("value"));
        df.select(proj).map_err(QuerierError::QueryFailed)
    }

    /// Two-stage `rate`/`increase`: per-series counter delta over the
    /// bucket, then an optional outer aggregation.
    fn range_query(
        &self,
        df: DataFrame,
        bucket: Expr,
        range: super::promql::RangeSpec,
        aggregate: MetricAgg,
        group_cols: &[&str],
    ) -> Result<DataFrame, QuerierError> {
        use super::promql::RangeFn;

        // Stage 1: (last - first)[/seconds] per (bucket, metric_name, service).
        let order = vec![SortExpr::new(col("timestamp"), true, true)];
        let df = df
            .aggregate(
                vec![bucket, col("metric_name"), col("service_name")],
                vec![
                    first_value(col("value"), order.clone()).alias("first"),
                    last_value(col("value"), order).alias("last"),
                ],
            )
            .map_err(QuerierError::QueryFailed)?;

        let delta = col("last") - col("first");
        let per_series = match range.function {
            RangeFn::Increase => delta,
            RangeFn::Rate => delta / lit(range.seconds),
        };
        let df = df
            .select(vec![
                col("bucket"),
                col("metric_name"),
                col("service_name"),
                cast_value_f64(per_series).alias("value"),
            ])
            .map_err(QuerierError::QueryFailed)?;

        // Stage 2: an outer aggregation folds the per-series rates. A bare
        // `rate(...)` (Natural grouping over `service_name`) is already the
        // result.
        if group_cols == ["service_name"] {
            return Ok(df);
        }
        let mut group_exprs = vec![col("bucket"), col("metric_name")];
        group_exprs.extend(group_cols.iter().map(|c| col(*c)));
        let df = df
            .aggregate(group_exprs, vec![aggregate_expr(aggregate).alias("value")])
            .map_err(QuerierError::QueryFailed)?;
        let mut proj = vec![col("bucket"), col("metric_name")];
        proj.extend(group_cols.iter().map(|c| col(*c)));
        proj.push(cast_value_f64(col("value")).alias("value"));
        df.select(proj).map_err(QuerierError::QueryFailed)
    }

    /// Resolve the grouping labels to physical columns. Only labels backed
    /// by a dedicated column can be grouped.
    fn group_columns(&self, plan: &MetricPlan) -> Result<Vec<&'static str>, QuerierError> {
        match &plan.grouping {
            // Bare selector: one series per service.
            Grouping::Natural => Ok(vec!["service_name"]),
            // sum(x): collapse everything.
            Grouping::Collapse => Ok(vec![]),
            Grouping::By(labels) => labels
                .iter()
                .map(|l| {
                    column_for_label(l).ok_or_else(|| {
                        QuerierError::Unsupported(format!("grouping by attribute label '{l}'"))
                    })
                })
                .collect(),
        }
    }

    /// The union of the metrics tables, each projected to [`SCAN_COLUMNS`].
    async fn scan_union(
        &self,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<DataFrame, QuerierError> {
        let mut union: Option<DataFrame> = None;
        for table in METRIC_TABLES {
            let table_ref = build_table_reference(tenant_slug, dataset_slug, table)
                .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
            // A missing table (e.g. no sum metrics ingested yet) is not an
            // error — skip it.
            let Ok(df) = self.session_context.table(table_ref).await else {
                continue;
            };
            let projected = df
                .select_columns(SCAN_COLUMNS)
                .map_err(QuerierError::QueryFailed)?;
            union = Some(match union {
                None => projected,
                Some(existing) => existing
                    .union(projected)
                    .map_err(QuerierError::QueryFailed)?,
            });
        }
        union.ok_or_else(|| {
            QuerierError::InvalidInput("no metrics tables available for this dataset".to_string())
        })
    }

    /// List the Prometheus label names present in the window: the
    /// well-known ones (`__name__`, `job`) plus attribute keys discovered
    /// in the `attributes`/`resource_attributes` documents.
    pub async fn get_labels(
        &self,
        start: i64,
        end: i64,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<String>, QuerierError> {
        let mut labels: BTreeSet<String> =
            ["__name__", "job"].iter().map(|s| s.to_string()).collect();
        let df = self.scan_union(tenant_slug, dataset_slug).await?;
        let df = window(df, start, end)?;
        let batches = df
            .select_columns(&[LOG_ATTRIBUTES, RESOURCE_ATTRIBUTES])
            .map_err(QuerierError::QueryFailed)?
            .distinct()
            .map_err(QuerierError::QueryFailed)?
            .limit(0, Some(LABEL_SCAN_LIMIT))
            .map_err(QuerierError::QueryFailed)?
            .collect()
            .await
            .map_err(QuerierError::QueryFailed)?;
        for batch in &batches {
            for column in [LOG_ATTRIBUTES, RESOURCE_ATTRIBUTES] {
                collect_attribute_keys(batch, column, &mut labels)?;
            }
        }
        Ok(labels.into_iter().collect())
    }

    /// List the distinct values of one Prometheus label in the window.
    pub async fn get_label_values(
        &self,
        label: &str,
        start: i64,
        end: i64,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<String>, QuerierError> {
        if label.is_empty() {
            return Err(QuerierError::InvalidInput(
                "label name must not be empty".to_string(),
            ));
        }
        let df = self.scan_union(tenant_slug, dataset_slug).await?;
        let df = window(df, start, end)?;

        // `__name__` → metric_name; other known labels → their column.
        let column = match label {
            "__name__" => Some("metric_name"),
            _ => column_for_label(label),
        };
        if let Some(column) = column {
            let batches = df
                .select_columns(&[column])
                .map_err(QuerierError::QueryFailed)?
                .distinct()
                .map_err(QuerierError::QueryFailed)?
                .collect()
                .await
                .map_err(QuerierError::QueryFailed)?;
            return distinct_non_empty(&batches, column);
        }

        // Otherwise pull the value out of the attribute documents.
        let batches = df
            .select_columns(&[LOG_ATTRIBUTES, RESOURCE_ATTRIBUTES])
            .map_err(QuerierError::QueryFailed)?
            .distinct()
            .map_err(QuerierError::QueryFailed)?
            .limit(0, Some(LABEL_SCAN_LIMIT))
            .map_err(QuerierError::QueryFailed)?
            .collect()
            .await
            .map_err(QuerierError::QueryFailed)?;
        let mut values = BTreeSet::new();
        for batch in &batches {
            for column in [LOG_ATTRIBUTES, RESOURCE_ATTRIBUTES] {
                collect_attribute_values(batch, column, label, &mut values)?;
            }
        }
        Ok(values.into_iter().collect())
    }

    /// List the distinct series (label sets) matching a PromQL selector.
    /// Series identity is `__name__` (metric_name) and `job` (service_name).
    pub async fn get_series(
        &self,
        selector: &str,
        start: i64,
        end: i64,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<BTreeMap<String, String>>, QuerierError> {
        let plan = plan_promql(selector.trim())?;
        let df = self.scan_union(tenant_slug, dataset_slug).await?;
        let df = apply_filters(df, &plan, start, end)?;

        let batches = df
            .select_columns(&["metric_name", "service_name"])
            .map_err(QuerierError::QueryFailed)?
            .distinct()
            .map_err(QuerierError::QueryFailed)?
            .limit(0, Some(LABEL_SCAN_LIMIT))
            .map_err(QuerierError::QueryFailed)?
            .collect()
            .await
            .map_err(QuerierError::QueryFailed)?;

        let mut series = BTreeSet::new();
        for batch in &batches {
            let name = string_column(batch, "metric_name")?;
            let service = string_column(batch, "service_name")?;
            for i in 0..batch.num_rows() {
                let mut labels = BTreeMap::new();
                if !name.is_null(i) && !name.value(i).is_empty() {
                    labels.insert("__name__".to_string(), name.value(i).to_string());
                }
                if !service.is_null(i) && !service.value(i).is_empty() {
                    labels.insert("job".to_string(), service.value(i).to_string());
                }
                if !labels.is_empty() {
                    series.insert(labels);
                }
            }
        }
        Ok(series.into_iter().collect())
    }
}

/// Inclusive nanosecond time-window filter on `timestamp`.
fn window(df: DataFrame, start: i64, end: i64) -> Result<DataFrame, QuerierError> {
    df.filter(
        col("timestamp")
            .gt_eq(lit(ScalarValue::TimestampNanosecond(Some(start), None)))
            .and(col("timestamp").lt_eq(lit(ScalarValue::TimestampNanosecond(Some(end), None)))),
    )
    .map_err(QuerierError::QueryFailed)
}

/// Collect the sorted, distinct, non-empty values of a string column.
fn distinct_non_empty(batches: &[RecordBatch], column: &str) -> Result<Vec<String>, QuerierError> {
    let mut values = BTreeSet::new();
    for batch in batches {
        let col = string_column(batch, column)?;
        for i in 0..batch.num_rows() {
            if !col.is_null(i) && !col.value(i).is_empty() {
                values.insert(col.value(i).to_string());
            }
        }
    }
    Ok(values.into_iter().collect())
}

/// Add every JSON object key from an attribute column to `keys`.
fn collect_attribute_keys(
    batch: &RecordBatch,
    column: &str,
    keys: &mut BTreeSet<String>,
) -> Result<(), QuerierError> {
    let attrs = string_column(batch, column)?;
    for i in 0..batch.num_rows() {
        if attrs.is_null(i) {
            continue;
        }
        if let Ok(serde_json::Value::Object(map)) =
            serde_json::from_str::<serde_json::Value>(attrs.value(i))
        {
            keys.extend(map.keys().cloned());
        }
    }
    Ok(())
}

/// Add the value of `label` from each attribute document to `values`.
fn collect_attribute_values(
    batch: &RecordBatch,
    column: &str,
    label: &str,
    values: &mut BTreeSet<String>,
) -> Result<(), QuerierError> {
    let attrs = string_column(batch, column)?;
    for i in 0..batch.num_rows() {
        if attrs.is_null(i) {
            continue;
        }
        if let Ok(serde_json::Value::Object(map)) =
            serde_json::from_str::<serde_json::Value>(attrs.value(i))
            && let Some(value) = map.get(label)
        {
            match value {
                serde_json::Value::String(s) => {
                    values.insert(s.clone());
                }
                other => {
                    values.insert(other.to_string());
                }
            }
        }
    }
    Ok(())
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray, QuerierError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| QuerierError::InvalidInput(format!("missing column '{name}'")))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            QuerierError::InvalidInput(format!("column '{name}' is not a string column"))
        })
}

/// Apply the metric-name filter, label matchers, and time window.
fn apply_filters(
    df: DataFrame,
    plan: &MetricPlan,
    start: i64,
    end: i64,
) -> Result<DataFrame, QuerierError> {
    let mut predicate = col("metric_name").eq(lit(plan.metric_name.clone()));
    for m in &plan.matchers {
        predicate = predicate.and(matcher_expr(m)?);
    }
    df.filter(
        col("timestamp")
            .gt_eq(lit(ScalarValue::TimestampNanosecond(Some(start), None)))
            .and(col("timestamp").lt_eq(lit(ScalarValue::TimestampNanosecond(Some(end), None))))
            .and(predicate),
    )
    .map_err(QuerierError::QueryFailed)
}

/// Lower one label matcher to a filter expression, mapping well-known
/// labels to columns and others to the attribute JSON.
fn matcher_expr(m: &LabelMatch) -> Result<Expr, QuerierError> {
    match column_for_label(&m.name) {
        Some(column) => Ok(match m.op {
            MatchKind::Eq => col(column).eq(lit(m.value.clone())),
            MatchKind::Neq => col(column).not_eq(lit(m.value.clone())),
            MatchKind::Re => regexp_like(col(column), lit(m.value.clone()), None),
            MatchKind::Nre => not(regexp_like(col(column), lit(m.value.clone()), None)),
        }),
        None => {
            let fragment = attribute_fragment(&m.name, &m.value);
            let present = contains(col(LOG_ATTRIBUTES), lit(fragment.clone()))
                .or(contains(col(RESOURCE_ATTRIBUTES), lit(fragment.clone())));
            match m.op {
                MatchKind::Eq => Ok(present),
                MatchKind::Neq => Ok(not(present)),
                _ => Err(QuerierError::Unsupported(format!(
                    "regex matcher on attribute label '{}'",
                    m.name
                ))),
            }
        }
    }
}

/// Well-known PromQL labels mapped to dedicated columns.
fn column_for_label(label: &str) -> Option<&'static str> {
    match label {
        "job" | "service" | "service_name" => Some("service_name"),
        _ => None,
    }
}

fn attribute_fragment(key: &str, value: &str) -> String {
    let json_key = serde_json::to_string(key).unwrap_or_else(|_| format!("\"{key}\""));
    let json_value = serde_json::to_string(value).unwrap_or_else(|_| format!("\"{value}\""));
    format!("{json_key}:{json_value}")
}

fn aggregate_expr(agg: MetricAgg) -> Expr {
    let value = col("value");
    match agg {
        MetricAgg::Sum => sum(value),
        MetricAgg::Avg => avg(value),
        MetricAgg::Min => min(value),
        MetricAgg::Max => max(value),
        MetricAgg::Count => count(value),
        // Last value in the bucket, ordered by timestamp.
        MetricAgg::Last => last_value(value, vec![SortExpr::new(col("timestamp"), true, true)]),
    }
}

fn cast_ns(expr: Expr) -> Expr {
    datafusion::logical_expr::cast(expr, DataType::Timestamp(TimeUnit::Nanosecond, None))
}

/// Cast a value to Float64 for a uniform matrix value column.
fn cast_value_f64(expr: Expr) -> Expr {
    datafusion::logical_expr::cast(expr, DataType::Float64)
}

/// The step-aligned `date_bin` bucket expression, aligned to the epoch.
/// The microsecond storage timestamp is cast to nanoseconds first.
fn bucket_expr(step: i64) -> Expr {
    let stride = lit(ScalarValue::IntervalMonthDayNano(Some(
        IntervalMonthDayNano::new(0, 0, step),
    )));
    let origin = lit(ScalarValue::TimestampNanosecond(Some(0), None));
    date_bin(stride, cast_ns(col("timestamp")), origin).alias("bucket")
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::catalog::memory::MemTable;
    use datafusion::catalog::{
        CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
    };

    fn metrics_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "start_timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            Field::new("attributes", DataType::Utf8, true),
            Field::new("resource_attributes", DataType::Utf8, true),
        ]))
    }

    fn service_with_data() -> MetricsService {
        let schema = metrics_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![100, 200, 300])),
                Arc::new(TimestampNanosecondArray::from(vec![None, None, None])),
                Arc::new(StringArray::from(vec!["api", "api", "web"])),
                Arc::new(StringArray::from(vec!["reqs", "reqs", "reqs"])),
                Arc::new(Float64Array::from(vec![1.0, 3.0, 5.0])),
                Arc::new(StringArray::from(vec![
                    r#"{"code":"200"}"#,
                    r#"{"code":"500"}"#,
                    r#"{"code":"200"}"#,
                ])),
                Arc::new(StringArray::from(vec!["{}", "{}", "{}"])),
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let schema_provider = Arc::new(MemorySchemaProvider::new());
        schema_provider
            .register_table("metrics_gauge".to_string(), Arc::new(table))
            .unwrap();
        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog.register_schema("d", schema_provider).unwrap();
        ctx.register_catalog("t", catalog);
        MetricsService::new(ctx)
    }

    /// Collect (metric_name, service?, value) tuples from a matrix.
    async fn matrix(
        service: &MetricsService,
        query: &str,
        step: i64,
    ) -> Vec<(String, Option<String>, f64)> {
        let batches = service
            .query_range(query, 0, 1000, step, "t", "d")
            .await
            .expect("query");
        let mut out = Vec::new();
        for batch in &batches {
            let name = batch
                .column_by_name("metric_name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let value = batch
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let service_col = batch
                .column_by_name("service_name")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            for i in 0..batch.num_rows() {
                let svc = service_col.map(|c| c.value(i).to_string());
                out.push((name.value(i).to_string(), svc, value.value(i)));
            }
        }
        out
    }

    #[tokio::test]
    async fn bare_selector_last_value_per_series() {
        let service = service_with_data();
        // Step 1000ns: one bucket. Bare selector -> last value per service.
        let out = matrix(&service, "reqs", 1000).await;
        let api = out
            .iter()
            .find(|(_, s, _)| s.as_deref() == Some("api"))
            .unwrap();
        let web = out
            .iter()
            .find(|(_, s, _)| s.as_deref() == Some("web"))
            .unwrap();
        assert_eq!(api.2, 3.0); // last of [1,3]
        assert_eq!(web.2, 5.0);
    }

    #[tokio::test]
    async fn sum_collapses_series() {
        let service = service_with_data();
        let out = matrix(&service, "sum(reqs)", 1000).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].2, 9.0); // 1+3+5
    }

    #[tokio::test]
    async fn sum_by_service() {
        let service = service_with_data();
        let out = matrix(&service, "sum by (job) (reqs)", 1000).await;
        let api = out
            .iter()
            .find(|(_, s, _)| s.as_deref() == Some("api"))
            .unwrap();
        assert_eq!(api.2, 4.0); // 1+3
    }

    #[tokio::test]
    async fn label_matcher_filters_attributes() {
        let service = service_with_data();
        // code="500" only matches the api/500 row (value 3).
        let out = matrix(&service, r#"sum(reqs{code="500"})"#, 1000).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].2, 3.0);
    }

    #[tokio::test]
    async fn count_and_max() {
        let service = service_with_data();
        assert_eq!(matrix(&service, "count(reqs)", 1000).await[0].2, 3.0);
        assert_eq!(matrix(&service, "max(reqs)", 1000).await[0].2, 5.0);
    }

    #[tokio::test]
    async fn step_splits_buckets() {
        let service = service_with_data();
        // Step 150ns → buckets [0,150),[150,300),[300,450): ts 100/200/300
        // land in three distinct buckets.
        let out = matrix(&service, "sum(reqs)", 150).await;
        assert_eq!(out.len(), 3);
        assert_eq!(out.iter().map(|(_, _, v)| *v).sum::<f64>(), 9.0);
    }

    #[tokio::test]
    async fn increase_is_last_minus_first_per_bucket() {
        let service = service_with_data();
        // api counter: 1 -> 3 in one bucket => increase 2; web has one
        // sample => increase 0.
        let out = matrix(&service, "increase(reqs[1m])", 1000).await;
        let api = out
            .iter()
            .find(|(_, s, _)| s.as_deref() == Some("api"))
            .unwrap();
        assert_eq!(api.2, 2.0);
    }

    #[tokio::test]
    async fn rate_divides_increase_by_range_seconds() {
        let service = service_with_data();
        // increase 2 over a 60s range = rate 2/60.
        let out = matrix(&service, "rate(reqs[1m])", 1000).await;
        let api = out
            .iter()
            .find(|(_, s, _)| s.as_deref() == Some("api"))
            .unwrap();
        assert!((api.2 - 2.0 / 60.0).abs() < 1e-9, "got {}", api.2);
    }

    #[tokio::test]
    async fn sum_over_increase_folds_series() {
        let service = service_with_data();
        // api increase 2, web increase 0 => sum 2.
        let out = matrix(&service, "sum(increase(reqs[1m]))", 1000).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].2, 2.0);
    }

    #[tokio::test]
    async fn label_names_include_known_and_attribute_keys() {
        let service = service_with_data();
        let labels = service.get_labels(0, 1000, "t", "d").await.unwrap();
        assert!(labels.contains(&"__name__".to_string()));
        assert!(labels.contains(&"job".to_string()));
        assert!(labels.contains(&"code".to_string()));
    }

    #[tokio::test]
    async fn label_values_for_name_job_and_attribute() {
        let service = service_with_data();
        assert_eq!(
            service
                .get_label_values("__name__", 0, 1000, "t", "d")
                .await
                .unwrap(),
            vec!["reqs".to_string()]
        );
        assert_eq!(
            service
                .get_label_values("job", 0, 1000, "t", "d")
                .await
                .unwrap(),
            vec!["api".to_string(), "web".to_string()]
        );
        assert_eq!(
            service
                .get_label_values("code", 0, 1000, "t", "d")
                .await
                .unwrap(),
            vec!["200".to_string(), "500".to_string()]
        );
    }

    #[tokio::test]
    async fn series_returns_name_and_job_sets() {
        let service = service_with_data();
        let series = service.get_series("reqs", 0, 1000, "t", "d").await.unwrap();
        assert_eq!(series.len(), 2);
        assert!(
            series
                .iter()
                .all(|s| s.get("__name__") == Some(&"reqs".to_string()))
        );
        let jobs: BTreeSet<_> = series
            .iter()
            .filter_map(|s| s.get("job").cloned())
            .collect();
        assert_eq!(jobs, BTreeSet::from(["api".to_string(), "web".to_string()]));
    }

    #[tokio::test]
    async fn non_positive_step_rejected() {
        let service = service_with_data();
        assert!(matches!(
            service.query_range("reqs", 0, 1000, 0, "t", "d").await,
            Err(QuerierError::InvalidInput(_))
        ));
    }
}
