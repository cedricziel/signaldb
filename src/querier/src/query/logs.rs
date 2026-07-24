//! # Log Query Service
//!
//! DataFusion-backed execution of LogQL log queries against the
//! tenant-scoped `logs` Iceberg table. Parses a LogQL query, lowers it to
//! a filter [`Expr`](datafusion::logical_expr::Expr) via
//! [`super::logql::log_query_filter`], and runs it through the DataFrame
//! API — the same shape as [`super::trace`] and [`super::profile`], so
//! user-controlled query values never enter a SQL string.
//!
//! Alongside line queries this service backs the Loki metadata endpoints:
//! label names, label values, and series discovery.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::{
    arrow::array::{Array, RecordBatch, StringArray},
    arrow::datatypes::{DataType, IntervalMonthDayNano},
    functions::datetime::expr_fn::date_bin,
    functions::unicode::expr_fn::character_length,
    functions_aggregate::expr_fn::{avg, count, max, min, sum},
    logical_expr::{Expr, cast, col, lit},
    prelude::{DataFrame, SessionContext},
    scalar::ScalarValue,
};
use logql::{Expr as LogqlExpr, LogQuery, parse, parse_query, parse_selector};

use super::logql::log_query_filter;
use super::logql_metric::{Aggregate, plan_metric_query};
use super::{
    LogQueryParams, MetricQueryParams, error::QuerierError, table_ref::build_table_reference,
};

/// Columns projected for a log-line query, in wire order. The Flight
/// layer streams these; the router shapes them into Loki streams.
pub const LOG_COLUMNS: &[&str] = &[
    "timestamp",
    "body",
    "service_name",
    "severity_text",
    "trace_id",
    "span_id",
    "log_attributes",
    "resource_attributes",
];

/// LogQL label names backed by dedicated columns, in Loki label form.
const KNOWN_LABELS: &[&str] = &[
    "detected_level",
    "level",
    "service_name",
    "span_id",
    "trace_id",
];

/// Columns whose distinct values define a series' identity.
const SERIES_COLUMNS: &[&str] = &["service_name", "severity_text"];

/// Upper bound on distinct attribute documents scanned for discovery.
const LABEL_SCAN_LIMIT: usize = 1000;

/// Scan direction for a log query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Forward,
    Backward,
}

impl Direction {
    /// Parse the Loki `direction` string, defaulting to `Backward`.
    pub fn parse(value: Option<&str>) -> Self {
        match value {
            Some("forward") => Direction::Forward,
            _ => Direction::Backward,
        }
    }

    fn ascending(self) -> bool {
        matches!(self, Direction::Forward)
    }
}

/// Executes LogQL log queries and Loki metadata lookups.
pub struct LogsService {
    session_context: Arc<SessionContext>,
}

impl Debug for LogsService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogsService")
            .field("session_context", &"set")
            .finish()
    }
}

impl Clone for LogsService {
    fn clone(&self) -> Self {
        Self {
            session_context: Arc::clone(&self.session_context),
        }
    }
}

impl LogsService {
    pub fn new(session_context: SessionContext) -> Self {
        Self {
            session_context: Arc::new(session_context),
        }
    }

    async fn logs_table(
        &self,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<DataFrame, QuerierError> {
        let table_ref = build_table_reference(tenant_slug, dataset_slug, "logs")
            .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
        self.session_context.table(table_ref).await.map_err(|e| {
            log::error!(
                "Failed to access logs table for tenant_slug={tenant_slug}, dataset_slug={dataset_slug}: {e}"
            );
            QuerierError::QueryFailed(e)
        })
    }

    /// Execute a LogQL log query, returning the projected log-line
    /// RecordBatches ordered by timestamp per the request's direction.
    /// The window (`start`/`end`) is inclusive unix-epoch nanoseconds.
    pub async fn query_logs(
        &self,
        params: &LogQueryParams,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<RecordBatch>, QuerierError> {
        let parsed =
            parse_query(&params.query).map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
        let filter = log_query_filter(&parsed)?;
        let direction = Direction::parse(params.direction.as_deref());

        let df = self.logs_table(tenant_slug, dataset_slug).await?;
        let df = shape_log_query(
            df,
            filter,
            params.start,
            params.end,
            params.limit,
            direction,
        )?;
        df.collect().await.map_err(QuerierError::QueryFailed)
    }

    /// Execute a LogQL metric query, returning a matrix: one row per
    /// (time bucket, series) with a `bucket` timestamp, the grouping
    /// columns, and a `value`. Rows are bucketed by `step` via
    /// `date_bin` (see [`super::logql_metric`] for the semantics).
    pub async fn query_metric(
        &self,
        params: &MetricQueryParams,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<RecordBatch>, QuerierError> {
        if params.step <= 0 {
            return Err(QuerierError::InvalidInput(
                "step must be a positive nanosecond interval".to_string(),
            ));
        }
        let LogqlExpr::Metric(metric) =
            parse(&params.query).map_err(|e| QuerierError::InvalidInput(e.to_string()))?
        else {
            return Err(QuerierError::InvalidInput(
                "not a metric query (use query_logs for log queries)".to_string(),
            ));
        };
        let plan = plan_metric_query(&metric)?;
        let filter = log_query_filter(&plan.log_query)?;

        // Resolve the grouping columns; unknown labels can't be grouped
        // with the substring attribute model.
        let group_cols: Vec<&'static str> = if plan.group_labels.is_empty() {
            SERIES_COLUMNS.to_vec()
        } else {
            plan.group_labels
                .iter()
                .map(|label| {
                    column_for_label(label).ok_or_else(|| {
                        QuerierError::Unsupported(format!("grouping by attribute label '{label}'"))
                    })
                })
                .collect::<Result<_, _>>()?
        };

        let mut df = self.logs_table(tenant_slug, dataset_slug).await?;
        df = apply_window(df, params.start, params.end)?;
        if let Some(filter) = filter {
            df = df.filter(filter).map_err(QuerierError::QueryFailed)?;
        }

        // Bucket timestamps into step-aligned windows.
        let stride = lit(ScalarValue::IntervalMonthDayNano(Some(
            IntervalMonthDayNano::new(0, 0, params.step),
        )));
        // Align buckets to the unix epoch.
        let origin = lit(ScalarValue::TimestampNanosecond(Some(0), None));
        let bucket = date_bin(stride, col("timestamp"), origin).alias("bucket");

        let mut group_exprs = vec![bucket];
        group_exprs.extend(group_cols.iter().map(|c| col(*c)));

        let value = aggregate_expr(&plan.aggregate).alias("value");
        df = df
            .aggregate(group_exprs, vec![value])
            .map_err(QuerierError::QueryFailed)?;

        // Normalize `value` to Float64 (count/bytes aggregate to Int64)
        // and apply the `rate` divisor in the same projection.
        let mut proj = vec![col("bucket")];
        proj.extend(group_cols.iter().map(|c| col(*c)));
        let value = cast(col("value"), DataType::Float64);
        let value = match plan.rate_divisor_seconds {
            Some(seconds) => value / lit(seconds),
            None => value,
        };
        proj.push(value.alias("value"));
        df = df.select(proj).map_err(QuerierError::QueryFailed)?;

        df.sort(vec![col("bucket").sort(true, true)])
            .map_err(QuerierError::QueryFailed)?
            .collect()
            .await
            .map_err(QuerierError::QueryFailed)
    }

    /// List the label names available for logs. Returns the labels backed
    /// by dedicated columns plus any attribute keys discovered in the
    /// window's `log_attributes` / `resource_attributes` documents.
    pub async fn get_labels(
        &self,
        start: i64,
        end: i64,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<String>, QuerierError> {
        let mut labels: BTreeSet<String> = KNOWN_LABELS.iter().map(|s| s.to_string()).collect();

        let df = self.logs_table(tenant_slug, dataset_slug).await?;
        let df = apply_window(df, start, end)?;
        let batches = df
            .select_columns(&["log_attributes", "resource_attributes"])
            .map_err(QuerierError::QueryFailed)?
            .distinct()
            .map_err(QuerierError::QueryFailed)?
            .limit(0, Some(LABEL_SCAN_LIMIT))
            .map_err(QuerierError::QueryFailed)?
            .collect()
            .await
            .map_err(QuerierError::QueryFailed)?;

        for batch in &batches {
            for column in ["log_attributes", "resource_attributes"] {
                let attrs = string_column(batch, column)?;
                for i in 0..batch.num_rows() {
                    if attrs.is_null(i) {
                        continue;
                    }
                    if let Ok(serde_json::Value::Object(map)) =
                        serde_json::from_str::<serde_json::Value>(attrs.value(i))
                    {
                        labels.extend(map.keys().cloned());
                    }
                }
            }
        }

        Ok(labels.into_iter().collect())
    }

    /// List the distinct values of one label in the window.
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

        let df = self.logs_table(tenant_slug, dataset_slug).await?;
        let df = apply_window(df, start, end)?;

        // Known labels are dedicated columns: distinct on the column.
        if let Some(column) = column_for_label(label) {
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
            .select_columns(&["log_attributes", "resource_attributes"])
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
            for column in ["log_attributes", "resource_attributes"] {
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
            }
        }
        Ok(values.into_iter().collect())
    }

    /// List the distinct series (label sets) matching a stream selector.
    /// Series identity is the dedicated columns in [`SERIES_COLUMNS`].
    pub async fn get_series(
        &self,
        selector: &str,
        start: i64,
        end: i64,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<BTreeMap<String, String>>, QuerierError> {
        // An empty matcher list is invalid in Loki's /series.
        let parsed = parse_selector(selector.trim())
            .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
        let filter = log_query_filter(&LogQuery {
            selector: parsed,
            pipeline: Vec::new(),
        })?;

        let mut df = self.logs_table(tenant_slug, dataset_slug).await?;
        df = apply_window(df, start, end)?;
        if let Some(filter) = filter {
            df = df.filter(filter).map_err(QuerierError::QueryFailed)?;
        }

        let batches = df
            .select_columns(SERIES_COLUMNS)
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
            let service = string_column(batch, "service_name")?;
            let severity = string_column(batch, "severity_text")?;
            for i in 0..batch.num_rows() {
                let mut labels = BTreeMap::new();
                if !service.is_null(i) && !service.value(i).is_empty() {
                    labels.insert("service_name".to_string(), service.value(i).to_string());
                }
                if !severity.is_null(i) && !severity.value(i).is_empty() {
                    labels.insert("level".to_string(), severity.value(i).to_string());
                }
                if !labels.is_empty() {
                    series.insert(labels);
                }
            }
        }
        Ok(series.into_iter().collect())
    }
}

/// Apply the projection, filter, ordering, and limit of a log-line query
/// to a base DataFrame. Split out so it can be tested against an
/// in-memory table.
pub fn shape_log_query(
    df: DataFrame,
    filter: Option<Expr>,
    start: i64,
    end: i64,
    limit: u32,
    direction: Direction,
) -> Result<DataFrame, QuerierError> {
    let mut df = apply_window(df, start, end)?;
    if let Some(filter) = filter {
        df = df.filter(filter).map_err(QuerierError::QueryFailed)?;
    }
    df.select_columns(LOG_COLUMNS)
        .map_err(QuerierError::QueryFailed)?
        .sort(vec![col("timestamp").sort(direction.ascending(), true)])
        .map_err(QuerierError::QueryFailed)?
        .limit(0, Some(limit as usize))
        .map_err(QuerierError::QueryFailed)
}

/// Inclusive nanosecond bounds on the `timestamp` column.
fn apply_window(df: DataFrame, start: i64, end: i64) -> Result<DataFrame, QuerierError> {
    df.filter(col("timestamp").gt_eq(lit(ScalarValue::TimestampNanosecond(Some(start), None))))
        .map_err(QuerierError::QueryFailed)?
        .filter(col("timestamp").lt_eq(lit(ScalarValue::TimestampNanosecond(Some(end), None))))
        .map_err(QuerierError::QueryFailed)
}

/// Build the DataFusion aggregate expression for a metric plan's value.
/// Unwrapped labels are cast from their stored string form to `Float64`.
fn aggregate_expr(aggregate: &Aggregate) -> Expr {
    match aggregate {
        Aggregate::Count => count(lit(1i64)),
        Aggregate::BytesSum => sum(character_length(col("body"))),
        Aggregate::UnwrapSum(label) => sum(unwrap_value(label)),
        Aggregate::UnwrapAvg(label) => avg(unwrap_value(label)),
        Aggregate::UnwrapMin(label) => min(unwrap_value(label)),
        Aggregate::UnwrapMax(label) => max(unwrap_value(label)),
    }
}

/// The unwrapped label's numeric value: the dedicated column when the
/// label maps to one, else the value pulled from the attribute JSON is
/// not addressable here, so unwrap is limited to numeric columns.
fn unwrap_value(label: &str) -> Expr {
    let column = column_for_label(label).unwrap_or(label);
    cast(col(column), DataType::Float64)
}

/// The dedicated column a known label maps to.
fn column_for_label(label: &str) -> Option<&'static str> {
    match label {
        "service_name" | "service" | "job" => Some("service_name"),
        "level" | "severity" | "detected_level" => Some("severity_text"),
        "trace_id" => Some("trace_id"),
        "span_id" => Some("span_id"),
        _ => None,
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::TimestampNanosecondArray;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::catalog::memory::MemTable;
    use datafusion::catalog::{
        CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
    };

    fn logs_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("body", DataType::Utf8, true),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("severity_text", DataType::Utf8, true),
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("span_id", DataType::Utf8, true),
            Field::new("log_attributes", DataType::Utf8, true),
            Field::new("resource_attributes", DataType::Utf8, true),
        ]))
    }

    fn str_col(values: &[&str]) -> Arc<StringArray> {
        Arc::new(StringArray::from(values.to_vec()))
    }

    /// A context with a `t.d.logs` table holding three sample rows.
    fn service_with_data() -> LogsService {
        let schema = logs_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![100, 200, 300])),
                str_col(&["boom happened", "all good", "boom again"]),
                str_col(&["api", "api", "web"]),
                str_col(&["error", "info", "error"]),
                str_col(&["t1", "t2", "t3"]),
                str_col(&["s1", "s2", "s3"]),
                str_col(&[
                    r#"{"namespace":"prod","pod":"api-1"}"#,
                    r#"{"namespace":"prod","pod":"api-2"}"#,
                    r#"{"namespace":"staging","pod":"web-1"}"#,
                ]),
                str_col(&["{}", "{}", "{}"]),
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let schema_provider = Arc::new(MemorySchemaProvider::new());
        schema_provider
            .register_table("logs".to_string(), Arc::new(table))
            .unwrap();
        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog.register_schema("d", schema_provider).unwrap();
        ctx.register_catalog("t", catalog);

        LogsService::new(ctx)
    }

    fn params(query: &str, start: i64, end: i64, direction: Direction) -> LogQueryParams {
        LogQueryParams {
            query: query.to_string(),
            start,
            end,
            limit: 100,
            direction: Some(
                if direction == Direction::Forward {
                    "forward"
                } else {
                    "backward"
                }
                .to_string(),
            ),
        }
    }

    async fn rows(service: &LogsService, query: &str, direction: Direction) -> Vec<(i64, String)> {
        let batches = service
            .query_logs(&params(query, 0, 1000, direction), "t", "d")
            .await
            .expect("query");
        let mut out = Vec::new();
        for batch in &batches {
            let ts = batch
                .column_by_name("timestamp")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let body = string_column(batch, "body").unwrap();
            for i in 0..batch.num_rows() {
                out.push((ts.value(i), body.value(i).to_string()));
            }
        }
        out
    }

    #[tokio::test]
    async fn selector_query_returns_matching_lines() {
        let service = service_with_data();
        let out = rows(&service, r#"{service_name="api"}"#, Direction::Forward).await;
        assert_eq!(
            out,
            vec![
                (100, "boom happened".to_string()),
                (200, "all good".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn line_filter_narrows_results() {
        let service = service_with_data();
        let out = rows(
            &service,
            r#"{service_name="api"} |= "boom""#,
            Direction::Forward,
        )
        .await;
        assert_eq!(out, vec![(100, "boom happened".to_string())]);
    }

    #[tokio::test]
    async fn attribute_label_matches_json() {
        let service = service_with_data();
        let out = rows(&service, r#"{namespace="staging"}"#, Direction::Forward).await;
        assert_eq!(out, vec![(300, "boom again".to_string())]);
    }

    #[tokio::test]
    async fn direction_backward_orders_newest_first() {
        let service = service_with_data();
        let out = rows(&service, r#"{service_name="api"}"#, Direction::Backward).await;
        assert_eq!(out.first().unwrap().0, 200);
        assert_eq!(out.last().unwrap().0, 100);
    }

    #[tokio::test]
    async fn time_window_bounds_are_applied() {
        let service = service_with_data();
        let batches = service
            .query_logs(
                &params(r#"{service_name="api"}"#, 150, 1000, Direction::Forward),
                "t",
                "d",
            )
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1); // only ts=200 falls in [150, 1000]
    }

    #[tokio::test]
    async fn get_label_values_for_known_columns() {
        let service = service_with_data();
        assert_eq!(
            service
                .get_label_values("service_name", 0, 1000, "t", "d")
                .await
                .unwrap(),
            vec!["api".to_string(), "web".to_string()]
        );
        assert_eq!(
            service
                .get_label_values("level", 0, 1000, "t", "d")
                .await
                .unwrap(),
            vec!["error".to_string(), "info".to_string()]
        );
    }

    #[tokio::test]
    async fn get_label_values_from_attributes() {
        let service = service_with_data();
        assert_eq!(
            service
                .get_label_values("namespace", 0, 1000, "t", "d")
                .await
                .unwrap(),
            vec!["prod".to_string(), "staging".to_string()]
        );
    }

    #[tokio::test]
    async fn get_labels_includes_known_and_attribute_keys() {
        let service = service_with_data();
        let labels = service.get_labels(0, 1000, "t", "d").await.unwrap();
        assert!(labels.contains(&"service_name".to_string()));
        assert!(labels.contains(&"level".to_string()));
        assert!(labels.contains(&"namespace".to_string()));
        assert!(labels.contains(&"pod".to_string()));
    }

    #[tokio::test]
    async fn get_series_returns_distinct_label_sets() {
        let service = service_with_data();
        let series = service
            .get_series(r#"{service_name="api"}"#, 0, 1000, "t", "d")
            .await
            .unwrap();
        // api has both error and info severities.
        assert_eq!(series.len(), 2);
        assert!(
            series
                .iter()
                .all(|s| s.get("service_name") == Some(&"api".to_string()))
        );
        let levels: BTreeSet<_> = series
            .iter()
            .filter_map(|s| s.get("level").cloned())
            .collect();
        assert_eq!(
            levels,
            BTreeSet::from(["error".to_string(), "info".to_string()])
        );
    }

    #[tokio::test]
    async fn empty_label_name_is_rejected() {
        let service = service_with_data();
        assert!(matches!(
            service.get_label_values("", 0, 1000, "t", "d").await,
            Err(QuerierError::InvalidInput(_))
        ));
    }

    // ---- metric queries (#374) ----

    fn metric_params(query: &str, step: i64) -> MetricQueryParams {
        MetricQueryParams {
            query: query.to_string(),
            start: 0,
            end: 1000,
            step,
        }
    }

    /// Collect a matrix result as (value, service_name?, level?) tuples.
    async fn matrix(service: &LogsService, query: &str, step: i64) -> Vec<(f64, Option<String>)> {
        use datafusion::arrow::array::Float64Array;
        let batches = service
            .query_metric(&metric_params(query, step), "t", "d")
            .await
            .expect("metric query");
        let mut out = Vec::new();
        for batch in &batches {
            let value = batch
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let service_col = string_column(batch, "service_name").ok();
            for i in 0..batch.num_rows() {
                let svc = service_col.and_then(|c| {
                    if c.is_null(i) {
                        None
                    } else {
                        Some(c.value(i).to_string())
                    }
                });
                out.push((value.value(i), svc));
            }
        }
        out
    }

    #[tokio::test]
    async fn count_over_time_counts_rows_per_series() {
        let service = service_with_data();
        // Step 1000ns puts all three rows in one bucket. api has 2 rows,
        // web has 1.
        let out = matrix(
            &service,
            r#"count_over_time({service_name=~".+"}[1000ns])"#,
            1000,
        )
        .await;
        // Natural series group by (service_name, severity_text): the two
        // api rows differ in severity, so there are three series of 1.
        assert_eq!(out.len(), 3);
        assert!(out.iter().all(|(v, _)| *v == 1.0));
    }

    #[tokio::test]
    async fn sum_by_groups_across_series() {
        let service = service_with_data();
        // sum by (service_name) over count — api=2, web=1.
        let out = matrix(
            &service,
            r#"sum by (service_name) (count_over_time({service_name=~".+"}[1000ns]))"#,
            1000,
        )
        .await;
        let api = out
            .iter()
            .find(|(_, s)| s.as_deref() == Some("api"))
            .unwrap();
        assert_eq!(api.0, 2.0);
    }

    #[tokio::test]
    async fn rate_divides_count_by_range_seconds() {
        let service = service_with_data();
        // Each api series has 1 row; rate = 1 / (1000ns as seconds) = 1e6.
        let out = matrix(&service, r#"rate({service_name="api"}[1000ns])"#, 1000).await;
        assert!(!out.is_empty());
        for (value, _) in &out {
            assert!((value - 1.0 / 0.000_001).abs() < 1.0, "got {value}");
        }
    }

    #[tokio::test]
    async fn step_buckets_split_rows_over_time() {
        let service = service_with_data();
        // Step 150ns: ts=100 in bucket [0,150), ts=200/300 in later
        // buckets. api rows at 100 and 200 land in different buckets.
        let out = matrix(
            &service,
            r#"count_over_time({service_name="api"}[150ns])"#,
            150,
        )
        .await;
        // Two buckets, each with count 1 for api.
        assert_eq!(out.len(), 2);
        assert!(
            out.iter()
                .all(|(v, s)| *v == 1.0 && s.as_deref() == Some("api"))
        );
    }

    #[tokio::test]
    async fn log_query_rejected_as_metric() {
        let service = service_with_data();
        assert!(matches!(
            service
                .query_metric(&metric_params(r#"{service_name="api"}"#, 1000), "t", "d")
                .await,
            Err(QuerierError::InvalidInput(_))
        ));
    }

    #[tokio::test]
    async fn non_positive_step_is_rejected() {
        let service = service_with_data();
        assert!(matches!(
            service
                .query_metric(
                    &metric_params(r#"count_over_time({a="b"}[5m])"#, 0),
                    "t",
                    "d"
                )
                .await,
            Err(QuerierError::InvalidInput(_))
        ));
    }
}
