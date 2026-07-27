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
    arrow::array::{
        Array, ArrayRef, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray,
    },
    arrow::compute::{concat_batches, take},
    arrow::datatypes::{DataType, Field, IntervalMonthDayNano, Schema, SchemaRef, TimeUnit},
    functions::datetime::expr_fn::date_bin,
    functions::unicode::expr_fn::character_length,
    functions_aggregate::expr_fn::{
        approx_percentile_cont, avg, count, first_value, last_value, max, min, stddev_pop, sum,
        var_pop,
    },
    logical_expr::{Expr, cast, col, lit},
    prelude::{DataFrame, SessionContext},
    scalar::ScalarValue,
};
use logql::{BinOp, Expr as LogqlExpr, LogQuery, MetricQuery, parse, parse_query, parse_selector};

use super::logql::log_query_filter;
use super::logql_metric::{
    Aggregate, ArithOp, LabelReplaceSpec, OuterAgg, ScalarOp, SortSpec, TopKSpec, plan_metric_query,
};
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
        // `vector(N)`: a scalar promoted to a constant, no-label series at
        // every step bucket — no table scan.
        if let MetricQuery::VectorLiteral(n) = metric {
            return synthesize_vector(n, params.start, params.end, params.step);
        }
        // Vector-to-vector operations (`a / b`, `a > b`): evaluate each
        // operand and join on (bucket, series labels). Scalar operations and
        // everything else lower to a single plan below.
        if let MetricQuery::Binary(bin) = &metric
            && let Some(kind) = classify_binary(bin)
        {
            let left = plan_metric_query(&bin.left)?;
            let right = plan_metric_query(&bin.right)?;
            let left_batches = self
                .execute_plan(&left, params, tenant_slug, dataset_slug)
                .await?;
            let right_batches = self
                .execute_plan(&right, params, tenant_slug, dataset_slug)
                .await?;
            return match kind {
                BinaryKind::Arith(op) => join_binary(left_batches, op, right_batches),
                BinaryKind::Compare(op) => {
                    join_compare(left_batches, op, bin.bool_modifier, right_batches)
                }
                BinaryKind::Logical(op) => join_logical(left_batches, op, right_batches),
            };
        }

        let plan = plan_metric_query(&metric)?;
        self.execute_plan(&plan, params, tenant_slug, dataset_slug)
            .await
    }

    /// Execute a single lowered [`MetricPlan`] into a matrix. Shared by
    /// top-level metric queries and each operand of a vector-to-vector
    /// binary operation.
    async fn execute_plan(
        &self,
        plan: &super::logql_metric::MetricPlan,
        params: &MetricQueryParams,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<RecordBatch>, QuerierError> {
        let filter = log_query_filter(&plan.log_query)?;

        // Resolve the output grouping columns (the `by` labels); unknown
        // labels can't be grouped with the substring attribute model.
        let out_group_cols: Vec<&'static str> = plan
            .group_labels
            .iter()
            .map(|label| {
                column_for_label(label).ok_or_else(|| {
                    QuerierError::Unsupported(format!("grouping by attribute label '{label}'"))
                })
            })
            .collect::<Result<_, _>>()?;

        // The range aggregation groups by the output columns directly for a
        // plain or `sum`-folded query (defaulting to the natural series
        // identity). A non-`sum` outer aggregation instead groups by the
        // full series identity so the second pass has per-series values to
        // reduce across.
        let range_group_cols: Vec<&'static str> = match plan.outer_agg {
            None if out_group_cols.is_empty() => SERIES_COLUMNS.to_vec(),
            None => out_group_cols.clone(),
            Some(_) => SERIES_COLUMNS.to_vec(),
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
        // Align buckets to the unix epoch. Cast the (microsecond) storage
        // timestamp to nanoseconds so the nanosecond stride/origin agree.
        let origin = lit(ScalarValue::TimestampNanosecond(Some(0), None));
        let timestamp_ns = cast(
            col("timestamp"),
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None),
        );
        let bucket = date_bin(stride, timestamp_ns, origin).alias("bucket");

        let mut group_exprs = vec![bucket];
        group_exprs.extend(range_group_cols.iter().map(|c| col(*c)));

        let value = aggregate_expr(&plan.aggregate).alias("value");
        df = df
            .aggregate(group_exprs, vec![value])
            .map_err(QuerierError::QueryFailed)?;

        // Normalize `value` to Float64 (count/bytes aggregate to Int64)
        // and apply the `rate` divisor in the same projection.
        let mut proj = vec![col("bucket")];
        proj.extend(range_group_cols.iter().map(|c| col(*c)));
        let value = cast(col("value"), DataType::Float64);
        let value = match plan.rate_divisor_seconds {
            Some(seconds) => value / lit(seconds),
            None => value,
        };
        proj.push(value.alias("value"));
        df = df.select(proj).map_err(QuerierError::QueryFailed)?;

        // Second pass: reduce the per-series range aggregation across series
        // within each output group (`avg`/`min`/`max`/`count`).
        if let Some(outer) = plan.outer_agg {
            let mut group_exprs = vec![col("bucket")];
            group_exprs.extend(out_group_cols.iter().map(|c| col(*c)));
            let reduced = outer_agg_expr(outer, col("value")).alias("value");
            df = df
                .aggregate(group_exprs, vec![reduced])
                .map_err(QuerierError::QueryFailed)?;

            // `count` reduces to Int64; keep the matrix value Float64.
            let mut proj = vec![col("bucket")];
            proj.extend(out_group_cols.iter().map(|c| col(*c)));
            proj.push(cast(col("value"), DataType::Float64).alias("value"));
            df = df.select(proj).map_err(QuerierError::QueryFailed)?;
        }

        // Vector-scalar arithmetic: scale/shift every value in place. The
        // surviving group columns are the outer-agg output when present,
        // else the range aggregation's own grouping.
        if let Some(scalar_op) = plan.scalar_op {
            let current_group_cols = if plan.outer_agg.is_some() {
                &out_group_cols
            } else {
                &range_group_cols
            };
            let mut proj = vec![col("bucket")];
            proj.extend(current_group_cols.iter().map(|c| col(*c)));
            proj.push(scalar_op_expr(col("value"), scalar_op).alias("value"));
            df = df.select(proj).map_err(QuerierError::QueryFailed)?;
        }

        let batches = df
            .sort(vec![col("bucket").sort(true, true)])
            .map_err(QuerierError::QueryFailed)?
            .collect()
            .await
            .map_err(QuerierError::QueryFailed)?;

        // `topk`/`bottomk`: keep the k highest/lowest series per bucket.
        let batches = match plan.topk {
            Some(spec) => apply_topk(batches, spec)?,
            None => batches,
        };

        // `label_replace`: rewrite a label from a regex capture of another.
        let batches = match &plan.label_replace {
            Some(spec) => apply_label_replace(batches, spec)?,
            None => batches,
        };

        // `sort`/`sort_desc`: order the output series by value.
        match plan.sort {
            Some(spec) => apply_sort(batches, spec),
            None => Ok(batches),
        }
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
        Aggregate::UnwrapQuantile { quantile, label } => {
            approx_percentile_cont(unwrap_value(label).sort(true, false), lit(*quantile), None)
        }
        Aggregate::UnwrapStddev(label) => stddev_pop(unwrap_value(label)),
        Aggregate::UnwrapStdvar(label) => var_pop(unwrap_value(label)),
        // Order by timestamp so first/last pick the earliest/latest sample.
        Aggregate::UnwrapFirst(label) => first_value(
            unwrap_value(label),
            vec![col("timestamp").sort(true, false)],
        ),
        Aggregate::UnwrapLast(label) => last_value(
            unwrap_value(label),
            vec![col("timestamp").sort(true, false)],
        ),
    }
}

/// Build the second-pass expression that reduces the per-series range
/// aggregation across series within a group (non-`sum` outer aggregation).
fn outer_agg_expr(outer: OuterAgg, value: Expr) -> Expr {
    match outer {
        OuterAgg::Avg => avg(value),
        OuterAgg::Min => min(value),
        OuterAgg::Max => max(value),
        OuterAgg::Count => count(value),
        // Loki follows Prometheus: `stddev`/`stdvar` are population
        // statistics across the series in the group.
        OuterAgg::Stddev => stddev_pop(value),
        OuterAgg::Stdvar => var_pop(value),
    }
}

/// Apply a scalar arithmetic op to a value expression, honoring the
/// operand order recorded in the [`ScalarOp`].
fn scalar_op_expr(value: Expr, scalar_op: ScalarOp) -> Expr {
    let scalar = lit(scalar_op.scalar);
    let (lhs, rhs) = if scalar_op.scalar_is_lhs {
        (scalar, value)
    } else {
        (value, scalar)
    };
    match scalar_op.op {
        ArithOp::Add => lhs + rhs,
        ArithOp::Sub => lhs - rhs,
        ArithOp::Mul => lhs * rhs,
        ArithOp::Div => lhs / rhs,
    }
}

/// A comparison operator for a vector-to-vector comparison.
#[derive(Debug, Clone, Copy)]
enum CmpOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
}

/// A logical/set operator for a vector-to-vector expression.
#[derive(Debug, Clone, Copy)]
enum LogicalOp {
    /// `and` — keep left series with a matching right series.
    And,
    /// `or` — left series, plus right series with no left match.
    Or,
    /// `unless` — left series with no matching right series.
    Unless,
}

/// The kind of vector-to-vector operation to evaluate via a join.
enum BinaryKind {
    Arith(ArithOp),
    Compare(CmpOp),
    Logical(LogicalOp),
}

/// Classify a **vector-to-vector** binary expression, or `None` when it
/// should instead lower to a single plan: a scalar operand (handled by
/// `plan_binary` as a `ScalarOp`), or an operator not supported between two
/// vectors (`mod`/`pow`, logical/set) which `plan_binary` then rejects.
fn classify_binary(bin: &logql::BinaryExpr) -> Option<BinaryKind> {
    let is_literal =
        |q: &MetricQuery| matches!(q, MetricQuery::Literal(_) | MetricQuery::VectorLiteral(_));
    if is_literal(&bin.left) || is_literal(&bin.right) {
        return None;
    }
    match bin.op {
        BinOp::Add => Some(BinaryKind::Arith(ArithOp::Add)),
        BinOp::Sub => Some(BinaryKind::Arith(ArithOp::Sub)),
        BinOp::Mul => Some(BinaryKind::Arith(ArithOp::Mul)),
        BinOp::Div => Some(BinaryKind::Arith(ArithOp::Div)),
        BinOp::Eq => Some(BinaryKind::Compare(CmpOp::Eq)),
        BinOp::Neq => Some(BinaryKind::Compare(CmpOp::Neq)),
        BinOp::Gt => Some(BinaryKind::Compare(CmpOp::Gt)),
        BinOp::Gte => Some(BinaryKind::Compare(CmpOp::Gte)),
        BinOp::Lt => Some(BinaryKind::Compare(CmpOp::Lt)),
        BinOp::Lte => Some(BinaryKind::Compare(CmpOp::Lte)),
        BinOp::And => Some(BinaryKind::Logical(LogicalOp::And)),
        BinOp::Or => Some(BinaryKind::Logical(LogicalOp::Or)),
        BinOp::Unless => Some(BinaryKind::Logical(LogicalOp::Unless)),
        _ => None,
    }
}

/// Whether a comparison holds for two values.
fn cmp_holds(op: CmpOp, l: f64, r: f64) -> bool {
    match op {
        CmpOp::Eq => l == r,
        CmpOp::Neq => l != r,
        CmpOp::Gt => l > r,
        CmpOp::Gte => l >= r,
        CmpOp::Lt => l < r,
        CmpOp::Lte => l <= r,
    }
}

/// Apply a scalar arithmetic operator to two `f64`s.
fn arith_apply(op: ArithOp, l: f64, r: f64) -> f64 {
    match op {
        ArithOp::Add => l + r,
        ArithOp::Sub => l - r,
        ArithOp::Mul => l * r,
        ArithOp::Div => l / r,
    }
}

/// One matrix row: its bucket, sorted `(label, value)` pairs, and value.
type MatrixRow = (i64, Vec<(String, String)>, f64);

/// A series key for joining: `(bucket, sorted label pairs)`.
type SeriesKey = (i64, Vec<(String, String)>);

/// Joined matrix rows indexed by [`SeriesKey`]; ordered by key.
type JoinMap = BTreeMap<SeriesKey, f64>;

/// A series' label set (sorted pairs), used to group its points for sort.
type SeriesLabels = Vec<(String, String)>;

/// A series' `(bucket, value)` points.
type SeriesPoints = Vec<(i64, f64)>;

/// Read a LogQL matrix batch into `(bucket, sorted label pairs, value)`
/// rows. Every string column other than `value` is treated as a series
/// label, so the join key is agnostic to which grouping produced it.
fn logql_matrix_rows(batch: &RecordBatch) -> Result<Vec<MatrixRow>, QuerierError> {
    let bucket = batch
        .column_by_name("bucket")
        .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
        .ok_or_else(|| QuerierError::InvalidInput("bucket column missing".to_string()))?;
    let value = batch
        .column_by_name("value")
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
        .ok_or_else(|| QuerierError::InvalidInput("value column missing".to_string()))?;

    let label_cols: Vec<(String, &StringArray)> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| f.name() != "bucket" && f.name() != "value")
        .filter_map(|(i, f)| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .map(|a| (f.name().to_string(), a))
        })
        .collect();

    let mut rows = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let mut labels: Vec<(String, String)> = label_cols
            .iter()
            .filter(|(_, a)| !a.is_null(i))
            .map(|(name, a)| (name.clone(), a.value(i).to_string()))
            .collect();
        labels.sort();
        let b = if bucket.is_null(i) {
            i64::MIN
        } else {
            bucket.value(i)
        };
        let v = if value.is_null(i) {
            f64::NAN
        } else {
            value.value(i)
        };
        rows.push((b, labels, v));
    }
    Ok(rows)
}

/// Index a matrix's rows by `(bucket, labels)` for one side of a join.
fn index_matrix(batches: &[RecordBatch]) -> Result<JoinMap, QuerierError> {
    let mut index = BTreeMap::new();
    for batch in batches {
        for (bucket, labels, v) in logql_matrix_rows(batch)? {
            index.insert((bucket, labels), v);
        }
    }
    Ok(index)
}

/// Rebuild a matrix batch from joined `(bucket, labels) -> value` rows,
/// reusing `schema`'s column layout (the left side's). A `BTreeMap` input
/// keeps the output ordered by `(bucket, labels)`.
fn rebuild_matrix(schema: SchemaRef, rows: JoinMap) -> Result<Vec<RecordBatch>, QuerierError> {
    let label_names: Vec<String> = schema
        .fields()
        .iter()
        .filter(|f| f.name() != "bucket" && f.name() != "value")
        .map(|f| f.name().to_string())
        .collect();

    let mut buckets = Vec::with_capacity(rows.len());
    let mut label_values: Vec<Vec<Option<String>>> =
        vec![Vec::with_capacity(rows.len()); label_names.len()];
    let mut values = Vec::with_capacity(rows.len());
    for ((bucket, labels), value) in rows {
        buckets.push(bucket);
        for (j, name) in label_names.iter().enumerate() {
            label_values[j].push(
                labels
                    .iter()
                    .find(|(k, _)| k == name)
                    .map(|(_, v)| v.clone()),
            );
        }
        values.push(value);
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(2 + label_names.len());
    columns.push(Arc::new(TimestampNanosecondArray::from(buckets)));
    for col_values in label_values {
        columns.push(Arc::new(StringArray::from(col_values)));
    }
    columns.push(Arc::new(Float64Array::from(values)));

    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
    Ok(vec![batch])
}

/// Join two matrices on `(bucket, labels)` and combine matched values with
/// `op` — the vector-to-vector arithmetic case (e.g. an error ratio). The
/// output reuses the left side's schema; unmatched series are dropped
/// (one-to-one matching), as in Loki/Prometheus without `on`/`ignoring`.
fn join_binary(
    left: Vec<RecordBatch>,
    op: ArithOp,
    right: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>, QuerierError> {
    if left.is_empty() {
        return Ok(left);
    }
    let schema = left[0].schema();
    let rhs = index_matrix(&right)?;

    let mut out: JoinMap = BTreeMap::new();
    for batch in &left {
        for (bucket, labels, lv) in logql_matrix_rows(batch)? {
            if let Some(&rv) = rhs.get(&(bucket, labels.clone())) {
                out.insert((bucket, labels), arith_apply(op, lv, rv));
            }
        }
    }
    rebuild_matrix(schema, out)
}

/// Join two matrices on `(bucket, labels)` and compare matched values. Only
/// series present on both sides participate. Without `bool` the left value
/// is kept where the comparison holds (a filter); with `bool` each matched
/// pair maps to `1`/`0`.
fn join_compare(
    left: Vec<RecordBatch>,
    op: CmpOp,
    bool_modifier: bool,
    right: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>, QuerierError> {
    if left.is_empty() {
        return Ok(left);
    }
    let schema = left[0].schema();
    let rhs = index_matrix(&right)?;

    let mut out: JoinMap = BTreeMap::new();
    for batch in &left {
        for (bucket, labels, lv) in logql_matrix_rows(batch)? {
            if let Some(&rv) = rhs.get(&(bucket, labels.clone())) {
                let holds = cmp_holds(op, lv, rv);
                if bool_modifier {
                    out.insert((bucket, labels), if holds { 1.0 } else { 0.0 });
                } else if holds {
                    out.insert((bucket, labels), lv);
                }
            }
        }
    }
    rebuild_matrix(schema, out)
}

/// Combine two matrices with a logical/set operator, matching series on
/// `(bucket, labels)`. `and` keeps left series present on the right,
/// `unless` keeps those absent from the right, and `or` is the union
/// (left series plus right-only series). Values are carried through, never
/// combined; the output reuses the left side's schema.
fn join_logical(
    left: Vec<RecordBatch>,
    op: LogicalOp,
    right: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>, QuerierError> {
    if left.is_empty() {
        // `or` still yields the right side when the left is empty.
        return match op {
            LogicalOp::Or => Ok(right),
            LogicalOp::And | LogicalOp::Unless => Ok(left),
        };
    }
    let schema = left[0].schema();
    let rhs = index_matrix(&right)?;

    let mut out: JoinMap = BTreeMap::new();
    for batch in &left {
        for (bucket, labels, lv) in logql_matrix_rows(batch)? {
            let matched = rhs.contains_key(&(bucket, labels.clone()));
            let keep = match op {
                LogicalOp::And | LogicalOp::Or => matched,
                LogicalOp::Unless => !matched,
            };
            // `or` also keeps left series regardless of a right match.
            if keep || matches!(op, LogicalOp::Or) {
                out.insert((bucket, labels), lv);
            }
        }
    }

    // `or` adds right series that have no left counterpart.
    if matches!(op, LogicalOp::Or) {
        let lhs = index_matrix(&left)?;
        for batch in &right {
            for (bucket, labels, rv) in logql_matrix_rows(batch)? {
                if !lhs.contains_key(&(bucket, labels.clone())) {
                    out.insert((bucket, labels), rv);
                }
            }
        }
    }

    rebuild_matrix(schema, out)
}

/// Apply `label_replace` to a finished matrix: for each series, match the
/// `src` label's value against `regex` (anchored to the whole value) and,
/// on a match, set the `dst` label to the expanded `replacement` (with
/// `$1` / `${name}` captures). An empty result deletes the label; a
/// non-match leaves the series unchanged. The output schema is the union
/// of all labels present, so a new `dst` label adds a column.
fn apply_label_replace(
    batches: Vec<RecordBatch>,
    spec: &LabelReplaceSpec,
) -> Result<Vec<RecordBatch>, QuerierError> {
    use std::collections::BTreeSet;

    if batches.is_empty() {
        return Ok(batches);
    }
    let re = regex::Regex::new(&format!("^(?:{})$", spec.regex))
        .map_err(|e| QuerierError::InvalidInput(format!("invalid label_replace regex: {e}")))?;
    // src/dst are logical label names; resolve to physical column names.
    let src_col = column_for_label(&spec.src)
        .map(|s| s.to_string())
        .unwrap_or_else(|| spec.src.clone());
    let dst_col = column_for_label(&spec.dst)
        .map(|s| s.to_string())
        .unwrap_or_else(|| spec.dst.clone());

    let mut rows: Vec<(i64, BTreeMap<String, String>, f64)> = Vec::new();
    for batch in &batches {
        for (bucket, labels, value) in logql_matrix_rows(batch)? {
            let mut map: BTreeMap<String, String> = labels.into_iter().collect();
            let src_val = map.get(&src_col).cloned().unwrap_or_default();
            if let Some(caps) = re.captures(&src_val) {
                let mut dst_val = String::new();
                caps.expand(&spec.replacement, &mut dst_val);
                if dst_val.is_empty() {
                    map.remove(&dst_col);
                } else {
                    map.insert(dst_col.clone(), dst_val);
                }
            }
            rows.push((bucket, map, value));
        }
    }

    // The output schema is the union of every label present after the
    // rewrite (so a freshly-introduced `dst` becomes a column).
    let names: Vec<String> = rows
        .iter()
        .flat_map(|(_, map, _)| map.keys().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    rows.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.iter().cmp(b.1.iter())));

    let mut buckets = Vec::with_capacity(rows.len());
    let mut label_values: Vec<Vec<Option<String>>> =
        vec![Vec::with_capacity(rows.len()); names.len()];
    let mut values = Vec::with_capacity(rows.len());
    for (bucket, map, value) in &rows {
        buckets.push(*bucket);
        for (j, name) in names.iter().enumerate() {
            label_values[j].push(map.get(name).cloned());
        }
        values.push(*value);
    }

    let mut fields = vec![Field::new(
        "bucket",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        false,
    )];
    for name in &names {
        fields.push(Field::new(name, DataType::Utf8, true));
    }
    fields.push(Field::new("value", DataType::Float64, false));
    let schema = Arc::new(Schema::new(fields));

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(2 + names.len());
    columns.push(Arc::new(TimestampNanosecondArray::from(buckets)));
    for col_values in label_values {
        columns.push(Arc::new(StringArray::from(col_values)));
    }
    columns.push(Arc::new(Float64Array::from(values)));

    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
    Ok(vec![batch])
}

/// Synthesize the matrix for `vector(N)`: a single no-label series holding
/// the constant `n` at every step-aligned bucket in `[start, end]`. Buckets
/// align to the unix epoch, matching `date_bin(step, timestamp)` used by
/// the scanning path.
fn synthesize_vector(
    n: f64,
    start: i64,
    end: i64,
    step: i64,
) -> Result<Vec<RecordBatch>, QuerierError> {
    let mut buckets = Vec::new();
    if step > 0 {
        let mut b = start.div_euclid(step) * step;
        while b <= end {
            buckets.push(b);
            b += step;
        }
    }
    let values = vec![n; buckets.len()];

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "bucket",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("value", DataType::Float64, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(buckets)),
        Arc::new(Float64Array::from(values)),
    ];
    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
    Ok(vec![batch])
}

/// Order the output series by value for `sort` / `sort_desc`. Series are
/// ranked by their value at their latest bucket (a range matrix has one
/// value per bucket, so `sort` is only strictly defined for instant
/// queries; the latest value is the natural stand-in). Rows are emitted
/// series-by-series in that order, which the router preserves as the series
/// order of the response.
fn apply_sort(batches: Vec<RecordBatch>, spec: SortSpec) -> Result<Vec<RecordBatch>, QuerierError> {
    use std::cmp::Ordering;

    if batches.is_empty() {
        return Ok(batches);
    }
    let schema = batches[0].schema();

    // Group each series' (bucket, value) points by its label set.
    let mut series: BTreeMap<SeriesLabels, SeriesPoints> = BTreeMap::new();
    for batch in &batches {
        for (bucket, labels, v) in logql_matrix_rows(batch)? {
            series.entry(labels).or_default().push((bucket, v));
        }
    }

    // Rank series by the value at the latest bucket.
    let latest = |points: &[(i64, f64)]| -> f64 {
        points
            .iter()
            .max_by_key(|(t, _)| *t)
            .map(|(_, v)| *v)
            .unwrap_or(f64::NAN)
    };
    let mut ordered: Vec<(SeriesLabels, SeriesPoints)> = series.into_iter().collect();
    ordered.sort_by(|a, b| {
        let ord = latest(&a.1)
            .partial_cmp(&latest(&b.1))
            .unwrap_or(Ordering::Equal);
        if spec.desc { ord.reverse() } else { ord }
    });

    // Emit rows series-by-series (each series' points in bucket order).
    let label_names: Vec<String> = schema
        .fields()
        .iter()
        .filter(|f| f.name() != "bucket" && f.name() != "value")
        .map(|f| f.name().to_string())
        .collect();

    let total: usize = ordered.iter().map(|(_, p)| p.len()).sum();
    let mut buckets = Vec::with_capacity(total);
    let mut label_values: Vec<Vec<Option<String>>> =
        vec![Vec::with_capacity(total); label_names.len()];
    let mut values = Vec::with_capacity(total);
    for (labels, mut points) in ordered {
        points.sort_by_key(|(t, _)| *t);
        for (bucket, v) in points {
            buckets.push(bucket);
            for (j, name) in label_names.iter().enumerate() {
                label_values[j].push(
                    labels
                        .iter()
                        .find(|(k, _)| k == name)
                        .map(|(_, v)| v.clone()),
                );
            }
            values.push(v);
        }
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(2 + label_names.len());
    columns.push(Arc::new(TimestampNanosecondArray::from(buckets)));
    for col_values in label_values {
        columns.push(Arc::new(StringArray::from(col_values)));
    }
    columns.push(Arc::new(Float64Array::from(values)));

    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
    Ok(vec![batch])
}

/// Keep the `k` highest (or lowest, for `bottomk`) series by value in each
/// time bucket. Operates on the finished matrix, so it is agnostic to which
/// label columns are present — it ranks rows within a `bucket` and selects
/// them with Arrow's `take`, preserving every column.
fn apply_topk(batches: Vec<RecordBatch>, spec: TopKSpec) -> Result<Vec<RecordBatch>, QuerierError> {
    use std::cmp::Ordering;

    if batches.is_empty() {
        return Ok(batches);
    }
    let schema = batches[0].schema();
    let batch =
        concat_batches(&schema, &batches).map_err(|e| QuerierError::InvalidInput(e.to_string()))?;

    let bucket = batch
        .column_by_name("bucket")
        .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
        .ok_or_else(|| QuerierError::InvalidInput("bucket column missing".to_string()))?;
    let value = batch
        .column_by_name("value")
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
        .ok_or_else(|| QuerierError::InvalidInput("value column missing".to_string()))?;

    // Group row indices by bucket (BTreeMap keeps buckets ascending), rank
    // within each, and keep the first `k`.
    let mut by_bucket: BTreeMap<i64, Vec<usize>> = BTreeMap::new();
    for i in 0..batch.num_rows() {
        let b = if bucket.is_null(i) {
            i64::MIN
        } else {
            bucket.value(i)
        };
        by_bucket.entry(b).or_default().push(i);
    }
    let val = |i: usize| {
        if value.is_null(i) {
            f64::NAN
        } else {
            value.value(i)
        }
    };
    let mut keep: Vec<u32> = Vec::new();
    for (_bucket, mut idxs) in by_bucket {
        idxs.sort_by(|&a, &b| {
            let ord = val(a).partial_cmp(&val(b)).unwrap_or(Ordering::Equal);
            if spec.bottom { ord } else { ord.reverse() }
        });
        idxs.truncate(spec.k);
        keep.extend(idxs.iter().map(|&i| i as u32));
    }

    let indices = datafusion::arrow::array::UInt32Array::from(keep);
    let columns = batch
        .columns()
        .iter()
        .map(|c| take(c, &indices, None).map_err(|e| QuerierError::InvalidInput(e.to_string())))
        .collect::<Result<Vec<_>, _>>()?;
    let out = RecordBatch::try_new(schema, columns)
        .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
    Ok(vec![out])
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
    async fn count_outer_aggregation_counts_series() {
        let service = service_with_data();
        // Three natural series (api/error, api/info, web/error) collapse to
        // a single value: the series count.
        let out = matrix(
            &service,
            r#"count(count_over_time({service_name=~".+"}[1000ns]))"#,
            1000,
        )
        .await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, 3.0);
    }

    #[tokio::test]
    async fn count_by_service_counts_series_per_group() {
        let service = service_with_data();
        // api has two series (error, info), web one.
        let out = matrix(
            &service,
            r#"count by (service_name) (count_over_time({service_name=~".+"}[1000ns]))"#,
            1000,
        )
        .await;
        let get = |name: &str| {
            out.iter()
                .find(|(_, s)| s.as_deref() == Some(name))
                .unwrap()
                .0
        };
        assert_eq!(out.len(), 2);
        assert_eq!(get("api"), 2.0);
        assert_eq!(get("web"), 1.0);
    }

    #[tokio::test]
    async fn avg_by_service_reduces_across_series() {
        let service = service_with_data();
        // Each series has count 1, so the per-service average is 1 — but
        // the three natural series collapse to two grouped rows.
        let out = matrix(
            &service,
            r#"avg by (service_name) (count_over_time({service_name=~".+"}[1000ns]))"#,
            1000,
        )
        .await;
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|(v, _)| *v == 1.0));
    }

    /// A context whose `api` service has two series with differing row
    /// counts in one bucket: `error`×3 and `info`×1 — so a cross-series
    /// `stddev`/`stdvar` over them is non-zero.
    fn service_with_varying_counts() -> LogsService {
        let schema = logs_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![100, 200, 300, 400])),
                str_col(&["a", "b", "c", "d"]),
                str_col(&["api", "api", "api", "api"]),
                str_col(&["error", "error", "error", "info"]),
                str_col(&["t1", "t2", "t3", "t4"]),
                str_col(&["s1", "s2", "s3", "s4"]),
                str_col(&["{}", "{}", "{}", "{}"]),
                str_col(&["{}", "{}", "{}", "{}"]),
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

    /// A context with one `api`/`info` series whose `trace_id` column
    /// carries the numeric strings 10, 20, 30, 40 — a stand-in for an
    /// unwrappable numeric column, so `unwrap trace_id` yields those
    /// samples in a single bucket.
    fn service_with_numeric_unwrap() -> LogsService {
        let schema = logs_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![100, 200, 300, 400])),
                str_col(&["a", "b", "c", "d"]),
                str_col(&["api", "api", "api", "api"]),
                str_col(&["info", "info", "info", "info"]),
                str_col(&["10", "20", "30", "40"]),
                str_col(&["s1", "s2", "s3", "s4"]),
                str_col(&["{}", "{}", "{}", "{}"]),
                str_col(&["{}", "{}", "{}", "{}"]),
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

    #[tokio::test]
    async fn quantile_over_time_ranks_unwrapped_samples() {
        let service = service_with_numeric_unwrap();
        let q = |phi: &str| {
            let query = format!(
                r#"quantile_over_time({phi}, {{service_name="api"}} | unwrap trace_id [1000ns])"#
            );
            let service = &service;
            async move { matrix(service, &query, 1000).await }
        };
        // Extremes map to min/max exactly; the median lands between them.
        let lo = q("0.0").await;
        assert_eq!(lo.len(), 1);
        assert!((lo[0].0 - 10.0).abs() < 1e-6, "q0 {}", lo[0].0);

        let hi = q("1.0").await;
        assert!((hi[0].0 - 40.0).abs() < 1e-6, "q1 {}", hi[0].0);

        let mid = q("0.5").await;
        assert!(mid[0].0 > 10.0 && mid[0].0 < 40.0, "q0.5 {}", mid[0].0);
    }

    #[tokio::test]
    async fn stddev_stdvar_over_time_reduce_unwrapped_samples() {
        let service = service_with_numeric_unwrap();
        // Samples [10, 20, 30, 40]: population variance 125, stddev ~11.18.
        let var = matrix(
            &service,
            r#"stdvar_over_time({service_name="api"} | unwrap trace_id [1000ns])"#,
            1000,
        )
        .await;
        assert_eq!(var.len(), 1);
        assert!((var[0].0 - 125.0).abs() < 1e-6, "stdvar {}", var[0].0);

        let dev = matrix(
            &service,
            r#"stddev_over_time({service_name="api"} | unwrap trace_id [1000ns])"#,
            1000,
        )
        .await;
        assert!(
            (dev[0].0 - 125.0_f64.sqrt()).abs() < 1e-6,
            "stddev {}",
            dev[0].0
        );
    }

    #[tokio::test]
    async fn scalar_arithmetic_scales_values() {
        let service = service_with_data();
        // Each of the three series has count 1; `* 10` scales them to 10.
        let scaled = matrix(
            &service,
            r#"count_over_time({service_name=~".+"}[1000ns]) * 10"#,
            1000,
        )
        .await;
        assert_eq!(scaled.len(), 3);
        assert!(scaled.iter().all(|(v, _)| *v == 10.0));

        // Operand order matters for subtraction: 100 - 1 = 99.
        let shifted = matrix(
            &service,
            r#"100 - count_over_time({service_name=~".+"}[1000ns])"#,
            1000,
        )
        .await;
        assert!(shifted.iter().all(|(v, _)| *v == 99.0));
    }

    #[tokio::test]
    async fn vector_to_vector_division_joins_on_labels() {
        let service = service_with_varying_counts();
        // Numerator: error/info logs containing "a" → only (api, error) = 1.
        // Denominator: all api logs → (api, error) = 3, (api, info) = 1.
        // Join on (api, error): 1/3. (api, info) has no numerator → dropped.
        let out = matrix(
            &service,
            r#"count_over_time({service_name="api"} |= "a" [1000ns]) / count_over_time({service_name="api"}[1000ns])"#,
            1000,
        )
        .await;
        assert_eq!(out.len(), 1);
        assert!((out[0].0 - 1.0 / 3.0).abs() < 1e-9, "ratio {}", out[0].0);
        assert_eq!(out[0].1.as_deref(), Some("api"));
    }

    #[tokio::test]
    async fn vector_to_vector_comparison_filters_and_bools() {
        let service = service_with_varying_counts();
        // Left (all api): (api,error)=3, (api,info)=1.
        // Right (api logs containing "a"): (api,error)=1.
        // Matched only on (api,error).
        let all = r#"count_over_time({service_name="api"}[1000ns])"#;
        let with_a = r#"count_over_time({service_name="api"} |= "a" [1000ns])"#;

        // `>` keeps the left value where 3 > 1 holds.
        let gt = matrix(&service, &format!("{all} > {with_a}"), 1000).await;
        assert_eq!(gt.len(), 1);
        assert_eq!(gt[0].0, 3.0);

        // `<` drops it (3 < 1 is false); no series remain.
        let lt = matrix(&service, &format!("{all} < {with_a}"), 1000).await;
        assert!(lt.is_empty());

        // `> bool` maps the matched pair to 1 instead of the left value.
        let gt_bool = matrix(&service, &format!("{all} > bool {with_a}"), 1000).await;
        assert_eq!(gt_bool.len(), 1);
        assert_eq!(gt_bool[0].0, 1.0);
    }

    #[tokio::test]
    async fn label_replace_adds_a_label_from_a_capture() {
        let service = service_with_data();
        // Capture the whole service_name into a new `tier` label.
        let batches = service
            .query_metric(
                &metric_params(
                    r#"label_replace(count_over_time({service_name="api"}[1000ns]), "tier", "be-$1", "service_name", "(.+)")"#,
                    1000,
                ),
                "t",
                "d",
            )
            .await
            .expect("query");
        let mut tiers = Vec::new();
        for batch in &batches {
            let tier = string_column(batch, "tier").expect("tier column present");
            for i in 0..batch.num_rows() {
                tiers.push(tier.value(i).to_string());
            }
        }
        assert!(!tiers.is_empty());
        assert!(tiers.iter().all(|t| t == "be-api"), "tiers {tiers:?}");
    }

    #[tokio::test]
    async fn label_replace_leaves_series_unchanged_when_regex_misses() {
        let service = service_with_data();
        // The regex matches no service_name, so no `tier` column is added.
        let batches = service
            .query_metric(
                &metric_params(
                    r#"label_replace(count_over_time({service_name="api"}[1000ns]), "tier", "x", "service_name", "nomatch")"#,
                    1000,
                ),
                "t",
                "d",
            )
            .await
            .expect("query");
        assert!(batches.iter().all(|b| b.column_by_name("tier").is_none()));
    }

    #[tokio::test]
    async fn vector_to_vector_logical_ops() {
        let service = service_with_data();
        // `all` = three series (api/error, api/info, web/error); `api` = the
        // two api series.
        let all = r#"count_over_time({service_name=~".+"}[1000ns])"#;
        let api = r#"count_over_time({service_name="api"}[1000ns])"#;

        // `and`: left series that also appear on the right → the two api.
        let and = matrix(&service, &format!("{all} and {api}"), 1000).await;
        assert_eq!(and.len(), 2);
        assert!(and.iter().all(|(_, s)| s.as_deref() == Some("api")));

        // `unless`: left series absent from the right → web only.
        let unless = matrix(&service, &format!("{all} unless {api}"), 1000).await;
        assert_eq!(unless.len(), 1);
        assert_eq!(unless[0].1.as_deref(), Some("web"));

        // `or`: union — the two api series plus the right-only web series.
        let or = matrix(&service, &format!("{api} or {all}"), 1000).await;
        assert_eq!(or.len(), 3);
    }

    #[tokio::test]
    async fn vector_literal_is_a_constant_no_label_series() {
        let service = service_with_data();
        // step 1000 over [0, 1000] → buckets 0 and 1000, both valued 5.
        let out = matrix(&service, r#"vector(5)"#, 1000).await;
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|(v, s)| *v == 5.0 && s.is_none()));
    }

    #[tokio::test]
    async fn sort_orders_series_by_value() {
        let service = service_with_varying_counts();
        // Two api series: (api,error)=3, (api,info)=1.
        let asc = matrix(
            &service,
            r#"sort(count_over_time({service_name="api"}[1000ns]))"#,
            1000,
        )
        .await;
        assert_eq!(asc.len(), 2);
        assert_eq!(asc[0].0, 1.0); // lowest first
        assert_eq!(asc[1].0, 3.0);

        let desc = matrix(
            &service,
            r#"sort_desc(count_over_time({service_name="api"}[1000ns]))"#,
            1000,
        )
        .await;
        assert_eq!(desc[0].0, 3.0); // highest first
        assert_eq!(desc[1].0, 1.0);
    }

    #[tokio::test]
    async fn topk_and_bottomk_keep_k_series_per_bucket() {
        let service = service_with_varying_counts();
        // Two `api` series with counts [3, 1] in one bucket.
        let top1 = matrix(
            &service,
            r#"topk(1, count_over_time({service_name="api"}[1000ns]))"#,
            1000,
        )
        .await;
        assert_eq!(top1.len(), 1);
        assert_eq!(top1[0].0, 3.0);

        let bottom1 = matrix(
            &service,
            r#"bottomk(1, count_over_time({service_name="api"}[1000ns]))"#,
            1000,
        )
        .await;
        assert_eq!(bottom1.len(), 1);
        assert_eq!(bottom1[0].0, 1.0);

        // k beyond the series count keeps all of them.
        let top5 = matrix(
            &service,
            r#"topk(5, count_over_time({service_name="api"}[1000ns]))"#,
            1000,
        )
        .await;
        assert_eq!(top5.len(), 2);
    }

    #[tokio::test]
    async fn first_and_last_over_time_pick_by_timestamp() {
        let service = service_with_numeric_unwrap();
        // Samples arrive as 10@100, 20@200, 30@300, 40@400 in one bucket.
        let first = matrix(
            &service,
            r#"first_over_time({service_name="api"} | unwrap trace_id [1000ns])"#,
            1000,
        )
        .await;
        assert_eq!(first.len(), 1);
        assert!((first[0].0 - 10.0).abs() < 1e-6, "first {}", first[0].0);

        let last = matrix(
            &service,
            r#"last_over_time({service_name="api"} | unwrap trace_id [1000ns])"#,
            1000,
        )
        .await;
        assert!((last[0].0 - 40.0).abs() < 1e-6, "last {}", last[0].0);
    }

    #[tokio::test]
    async fn stddev_and_stdvar_reduce_across_series() {
        let service = service_with_varying_counts();
        // Series counts within `api` are [3, 1]: population variance is 1,
        // population stddev is 1.
        let stdvar = matrix(
            &service,
            r#"stdvar by (service_name) (count_over_time({service_name="api"}[1000ns]))"#,
            1000,
        )
        .await;
        assert_eq!(stdvar.len(), 1);
        assert!((stdvar[0].0 - 1.0).abs() < 1e-9, "stdvar {}", stdvar[0].0);

        let stddev = matrix(
            &service,
            r#"stddev by (service_name) (count_over_time({service_name="api"}[1000ns]))"#,
            1000,
        )
        .await;
        assert_eq!(stddev.len(), 1);
        assert!((stddev[0].0 - 1.0).abs() < 1e-9, "stddev {}", stddev[0].0);
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
