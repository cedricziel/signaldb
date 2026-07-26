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

use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, IntervalMonthDayNano, Schema, TimeUnit};
use datafusion::functions::datetime::expr_fn::date_bin;
use datafusion::functions::regex::expr_fn::regexp_like;
use datafusion::functions::string::expr_fn::contains;
use datafusion::functions_aggregate::expr_fn::{
    avg, count, first_value, last_value, max, min, nth_value, regr_slope, stddev_pop, sum, var_pop,
};
use datafusion::logical_expr::{Expr, SortExpr, cast, col, lit, not};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::scalar::ScalarValue;

use super::promql::{
    ArithOp, CmpOp, Grouping, HistogramFn, LabelMatch, LogicalOp, MatchKind, MetricAgg, MetricPlan,
    QueryPlan, SequenceFn, TopKSpec, ValueOp, plan_promql, plan_query,
};
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
        match plan_query(query)? {
            QueryPlan::Single(plan) if plan.absent => {
                self.eval_absent(&plan, start, end, step, tenant_slug, dataset_slug)
                    .await
            }
            QueryPlan::Single(plan) => {
                self.eval_plan(&plan, start, end, step, tenant_slug, dataset_slug)
                    .await
            }
            QueryPlan::BinaryVector { left, op, right } => {
                self.eval_binary(
                    &left,
                    op,
                    &right,
                    start,
                    end,
                    step,
                    tenant_slug,
                    dataset_slug,
                )
                .await
            }
            QueryPlan::BinaryCompare {
                left,
                op,
                right,
                return_bool,
            } => {
                self.eval_binary_compare(
                    &left,
                    op,
                    &right,
                    return_bool,
                    start,
                    end,
                    step,
                    tenant_slug,
                    dataset_slug,
                )
                .await
            }
            QueryPlan::BinaryLogical { left, op, right } => {
                self.eval_binary_logical(
                    &left,
                    op,
                    &right,
                    start,
                    end,
                    step,
                    tenant_slug,
                    dataset_slug,
                )
                .await
            }
        }
    }

    /// Execute a single lowered [`MetricPlan`] to matrix RecordBatches.
    #[allow(clippy::too_many_arguments)]
    async fn eval_plan(
        &self,
        plan: &MetricPlan,
        start: i64,
        end: i64,
        step: i64,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<RecordBatch>, QuerierError> {
        // histogram_quantile / histogram_fraction target the histogram table
        // with a distinct (row-wise, interpolated) execution path.
        if plan.quantile.is_some() || plan.histogram_fraction.is_some() {
            let phi = plan.quantile.unwrap_or(0.0);
            return self
                .histogram_query(plan, phi, start, end, step, tenant_slug, dataset_slug)
                .await;
        }

        // histogram_count/histogram_sum aggregate the histogram table's
        // scalar count/sum column.
        if let Some(hf) = plan.histogram_fn {
            return self
                .histogram_scalar_query(plan, hf, start, end, step, tenant_slug, dataset_slug)
                .await;
        }

        // resets/changes count events over the ordered per-bucket samples.
        if let Some(sf) = plan.sequence_fn {
            return self
                .sequence_query(plan, sf, start, end, step, tenant_slug, dataset_slug)
                .await;
        }

        let group_cols = self.group_columns(plan)?;

        let df = self.scan_union(tenant_slug, dataset_slug).await?;
        // `offset` shifts the sampled window back in time.
        let df = apply_filters(df, plan, start - plan.offset_ns, end - plan.offset_ns)?;

        // Bucket timestamps into step-aligned windows (cast the
        // microsecond storage timestamp to nanoseconds first).
        let bucket = bucket_expr(step, plan.offset_ns);

        let df = if plan.timestamp {
            // `timestamp(v)`: the value is each series' latest sample time in
            // seconds, rather than the sample value.
            let mut group_exprs = vec![bucket, col("metric_name")];
            group_exprs.extend(group_cols.iter().map(|c| col(*c)));
            let secs = cast_value_f64(cast(cast_ns(col("timestamp")), DataType::Int64)) / lit(1e9);
            let df = df
                .aggregate(group_exprs, vec![max(secs).alias("value")])
                .map_err(QuerierError::QueryFailed)?;
            let mut proj = vec![col("bucket"), col("metric_name")];
            proj.extend(group_cols.iter().map(|c| col(*c)));
            proj.push(cast_value_f64(col("value")).alias("value"));
            df.select(proj).map_err(QuerierError::QueryFailed)?
        } else if let Some(range) = plan.range {
            self.range_query(
                df,
                bucket,
                range,
                plan.aggregate,
                plan.agg_param,
                &group_cols,
            )?
        } else {
            self.simple_query(df, bucket, plan.aggregate, plan.agg_param, &group_cols)?
        };

        // A second aggregation folds the per-series result, e.g. the outer
        // `sum` in `sum(avg_over_time(x[5m]))`.
        let (df, group_cols) = if let Some((outer_agg, outer_grouping)) = &plan.outer_agg {
            let outer_cols = Self::group_columns_for(outer_grouping)?;
            let mut group_exprs = vec![col("bucket"), col("metric_name")];
            group_exprs.extend(outer_cols.iter().map(|c| col(*c)));
            let df = df
                .aggregate(
                    group_exprs,
                    vec![aggregate_expr(*outer_agg, None).alias("value")],
                )
                .map_err(QuerierError::QueryFailed)?;
            let mut proj = vec![col("bucket"), col("metric_name")];
            proj.extend(outer_cols.iter().map(|c| col(*c)));
            proj.push(cast_value_f64(col("value")).alias("value"));
            let df = df.select(proj).map_err(QuerierError::QueryFailed)?;
            (df, outer_cols)
        } else {
            (df, group_cols)
        };

        // Apply math / scalar-arithmetic transforms to the result value.
        let df = apply_transforms_df(df, &plan.transforms, &group_cols)?;

        // `vector CMP scalar` without `bool`: keep only matching samples.
        let df = if let Some(cmp) = &plan.filter {
            df.filter(cmp_bool_expr(
                col("value"),
                cmp.op,
                cmp.scalar,
                cmp.scalar_left,
            ))
            .map_err(QuerierError::QueryFailed)?
        } else {
            df
        };

        // `sort`/`sort_desc` order the output within each bucket by value.
        let mut sort_keys = vec![SortExpr::new(col("bucket"), true, true)];
        if let Some(desc) = plan.sort {
            sort_keys.push(SortExpr::new(col("value"), !desc, false));
        }
        let batches = df
            .sort(sort_keys)
            .map_err(QuerierError::QueryFailed)?
            .collect()
            .await
            .map_err(QuerierError::QueryFailed)?;

        // `topk`/`bottomk`: keep the k highest/lowest series per bucket.
        if let Some(spec) = plan.topk {
            return apply_topk(batches, spec);
        }
        Ok(batches)
    }

    /// Execute a `left OP right` vector-to-vector arithmetic: evaluate both
    /// sides, then match one-to-one on (bucket, service_name) and combine the
    /// values. The output series drops the metric name (`__name__`), matching
    /// Prometheus. Only series present on both sides are emitted.
    #[allow(clippy::too_many_arguments)]
    async fn eval_binary(
        &self,
        left: &MetricPlan,
        op: ArithOp,
        right: &MetricPlan,
        start: i64,
        end: i64,
        step: i64,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<RecordBatch>, QuerierError> {
        let left_batches = self
            .eval_plan(left, start, end, step, tenant_slug, dataset_slug)
            .await?;
        let right_batches = self
            .eval_plan(right, start, end, step, tenant_slug, dataset_slug)
            .await?;

        // Index the right side by (bucket, service_name).
        let mut rhs: BTreeMap<(i64, String), f64> = BTreeMap::new();
        for batch in &right_batches {
            for (bucket, service, value) in matrix_rows(batch)? {
                rhs.insert((bucket, service), value);
            }
        }

        // BTreeMap keeps output sorted by (bucket, series).
        let mut out: BTreeMap<(i64, String), f64> = BTreeMap::new();
        for batch in &left_batches {
            for (bucket, service, lv) in matrix_rows(batch)? {
                if let Some(&rv) = rhs.get(&(bucket, service.clone())) {
                    out.insert((bucket, service), arith_f64(op, lv, rv, false));
                }
            }
        }

        let mut ts = Vec::with_capacity(out.len());
        let mut names = Vec::with_capacity(out.len());
        let mut services = Vec::with_capacity(out.len());
        let mut values = Vec::with_capacity(out.len());
        for ((bucket, service), value) in out {
            ts.push(bucket);
            names.push(String::new()); // __name__ is dropped for binary ops
            services.push(service);
            values.push(value);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "bucket",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampNanosecondArray::from(ts)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(services)),
            Arc::new(Float64Array::from(values)),
        ];
        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
        Ok(vec![batch])
    }

    /// Execute a `left CMP right` vector-to-vector comparison. Series are
    /// matched one-to-one on (bucket, service_name). Without `bool` the
    /// left series is kept (with its metric name) where the comparison holds;
    /// with `bool` each matched pair maps to 1/0 and the name is dropped.
    #[allow(clippy::too_many_arguments)]
    async fn eval_binary_compare(
        &self,
        left: &MetricPlan,
        op: CmpOp,
        right: &MetricPlan,
        return_bool: bool,
        start: i64,
        end: i64,
        step: i64,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<RecordBatch>, QuerierError> {
        let left_batches = self
            .eval_plan(left, start, end, step, tenant_slug, dataset_slug)
            .await?;
        let right_batches = self
            .eval_plan(right, start, end, step, tenant_slug, dataset_slug)
            .await?;

        let mut rhs: BTreeMap<(i64, String), f64> = BTreeMap::new();
        for batch in &right_batches {
            for (bucket, service, value) in matrix_rows(batch)? {
                rhs.insert((bucket, service), value);
            }
        }

        // (bucket, service) → (metric_name, value).
        let mut out: BTreeMap<(i64, String), (String, f64)> = BTreeMap::new();
        for batch in &left_batches {
            for (bucket, metric, service, lv) in matrix_rows_named(batch)? {
                let Some(&rv) = rhs.get(&(bucket, service.clone())) else {
                    continue;
                };
                let holds = cmp_f64(lv, op, rv, false);
                if return_bool {
                    let v = if holds { 1.0 } else { 0.0 };
                    out.insert((bucket, service), (String::new(), v));
                } else if holds {
                    out.insert((bucket, service), (metric, lv));
                }
            }
        }

        let mut ts = Vec::with_capacity(out.len());
        let mut names = Vec::with_capacity(out.len());
        let mut services = Vec::with_capacity(out.len());
        let mut values = Vec::with_capacity(out.len());
        for ((bucket, service), (metric, value)) in out {
            ts.push(bucket);
            names.push(metric);
            services.push(service);
            values.push(value);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "bucket",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampNanosecondArray::from(ts)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(services)),
            Arc::new(Float64Array::from(values)),
        ];
        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
        Ok(vec![batch])
    }

    /// Execute a `left and|or|unless right` set operation. Series identity is
    /// (bucket, service_name). `and` keeps left series with a match on the
    /// right; `unless` keeps those without; `or` is the union, preferring the
    /// left series where both sides have one. Labels/values are preserved.
    #[allow(clippy::too_many_arguments)]
    async fn eval_binary_logical(
        &self,
        left: &MetricPlan,
        op: LogicalOp,
        right: &MetricPlan,
        start: i64,
        end: i64,
        step: i64,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<RecordBatch>, QuerierError> {
        let left_batches = self
            .eval_plan(left, start, end, step, tenant_slug, dataset_slug)
            .await?;
        let right_batches = self
            .eval_plan(right, start, end, step, tenant_slug, dataset_slug)
            .await?;

        let mut right_ids: BTreeSet<(i64, String)> = BTreeSet::new();
        for batch in &right_batches {
            for (bucket, _m, service, _v) in matrix_rows_named(batch)? {
                right_ids.insert((bucket, service));
            }
        }

        // (bucket, service) → (metric_name, value).
        let mut out: BTreeMap<(i64, String), (String, f64)> = BTreeMap::new();
        for batch in &left_batches {
            for (bucket, metric, service, v) in matrix_rows_named(batch)? {
                let in_right = right_ids.contains(&(bucket, service.clone()));
                let keep = match op {
                    LogicalOp::And => in_right,
                    LogicalOp::Unless => !in_right,
                    LogicalOp::Or => true,
                };
                if keep {
                    out.insert((bucket, service), (metric, v));
                }
            }
        }
        // `or` adds the right series whose identity is absent from the left.
        if op == LogicalOp::Or {
            let left_ids: BTreeSet<(i64, String)> = out.keys().cloned().collect();
            for batch in &right_batches {
                for (bucket, metric, service, v) in matrix_rows_named(batch)? {
                    if !left_ids.contains(&(bucket, service.clone())) {
                        out.entry((bucket, service)).or_insert((metric, v));
                    }
                }
            }
        }

        let mut ts = Vec::with_capacity(out.len());
        let mut names = Vec::with_capacity(out.len());
        let mut services = Vec::with_capacity(out.len());
        let mut values = Vec::with_capacity(out.len());
        for ((bucket, service), (metric, value)) in out {
            ts.push(bucket);
            names.push(metric);
            services.push(service);
            values.push(value);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "bucket",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampNanosecondArray::from(ts)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(services)),
            Arc::new(Float64Array::from(values)),
        ];
        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
        Ok(vec![batch])
    }

    /// `absent(v)` / `absent_over_time(v[range])`: emit a `1`-valued series
    /// for every step bucket in `[start, end]` where the selector matched no
    /// data. The output carries the `job`/`service` equality matcher (if any)
    /// as `service_name`, mirroring Prometheus's use of the selector labels.
    async fn eval_absent(
        &self,
        plan: &MetricPlan,
        start: i64,
        end: i64,
        step: i64,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<RecordBatch>, QuerierError> {
        // Evaluate the inner selector (without the absent flag) and collect
        // the buckets that produced data.
        let mut inner = plan.clone();
        inner.absent = false;
        // A scan over a missing table errors inside eval_plan for some paths;
        // treat any evaluation as "no data" only via its rows.
        let present: BTreeSet<i64> = match self
            .eval_plan(&inner, start, end, step, tenant_slug, dataset_slug)
            .await
        {
            Ok(batches) => {
                let mut set = BTreeSet::new();
                for batch in &batches {
                    for (bucket, _s, _v) in matrix_rows(batch)? {
                        set.insert(bucket);
                    }
                }
                set
            }
            Err(_) => BTreeSet::new(),
        };

        // The output series label: the value of a `job`/`service` equality
        // matcher, if the selector has one.
        let service = plan
            .matchers
            .iter()
            .find(|m| {
                matches!(m.op, MatchKind::Eq) && column_for_label(&m.name) == Some("service_name")
            })
            .map(|m| m.value.clone())
            .unwrap_or_default();

        // Emit 1 for every step bucket (aligned to the epoch) with no data.
        let first = start.div_euclid(step) * step;
        let mut ts = Vec::new();
        let mut names = Vec::new();
        let mut services = Vec::new();
        let mut values = Vec::new();
        let mut bucket = first;
        while bucket <= end {
            if !present.contains(&bucket) {
                ts.push(bucket);
                names.push(String::new());
                services.push(service.clone());
                values.push(1.0);
            }
            bucket += step;
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "bucket",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampNanosecondArray::from(ts)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(services)),
            Arc::new(Float64Array::from(values)),
        ];
        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
        Ok(vec![batch])
    }

    /// Aggregate `value` per (bucket, metric_name, group) directly.
    fn simple_query(
        &self,
        df: DataFrame,
        bucket: Expr,
        aggregate: MetricAgg,
        param: Option<f64>,
        group_cols: &[&str],
    ) -> Result<DataFrame, QuerierError> {
        let mut group_exprs = vec![bucket, col("metric_name")];
        group_exprs.extend(group_cols.iter().map(|c| col(*c)));

        let value = aggregate_expr(aggregate, param).alias("value");
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
        param: Option<f64>,
        group_cols: &[&str],
    ) -> Result<DataFrame, QuerierError> {
        use super::promql::RangeFn;

        // Stage 1: reduce each (bucket, metric_name, service) to one `value`.
        let df = match range.function {
            // `deriv`: per-second slope from a linear regression of the
            // samples against time (nanoseconds → seconds).
            RangeFn::Deriv => {
                let secs = cast_value_f64(cast(cast_ns(col("timestamp")), DataType::Int64))
                    / lit(1_000_000_000.0);
                df.aggregate(
                    vec![bucket, col("metric_name"), col("service_name")],
                    vec![regr_slope(col("value"), secs).alias("value")],
                )
                .map_err(QuerierError::QueryFailed)?
                .select(vec![
                    col("bucket"),
                    col("metric_name"),
                    col("service_name"),
                    cast_value_f64(col("value")).alias("value"),
                ])
                .map_err(QuerierError::QueryFailed)?
            }
            // `irate`/`idelta`: the last two samples in the window.
            RangeFn::Irate | RangeFn::Idelta => {
                let order = vec![SortExpr::new(col("timestamp"), true, true)];
                let secs = cast_value_f64(cast(cast_ns(col("timestamp")), DataType::Int64))
                    / lit(1_000_000_000.0);
                let reduced = df
                    .aggregate(
                        vec![bucket, col("metric_name"), col("service_name")],
                        vec![
                            nth_value(col("value"), -1, order.clone()).alias("v_last"),
                            nth_value(col("value"), -2, order.clone()).alias("v_prev"),
                            nth_value(secs.clone(), -1, order.clone()).alias("t_last"),
                            nth_value(secs, -2, order).alias("t_prev"),
                        ],
                    )
                    .map_err(QuerierError::QueryFailed)?;
                let d = col("v_last") - col("v_prev");
                let per_series = match range.function {
                    RangeFn::Idelta => d,
                    RangeFn::Irate => d / (col("t_last") - col("t_prev")),
                    _ => unreachable!("only irate/idelta here"),
                };
                reduced
                    .select(vec![
                        col("bucket"),
                        col("metric_name"),
                        col("service_name"),
                        cast_value_f64(per_series).alias("value"),
                    ])
                    .map_err(QuerierError::QueryFailed)?
            }
            // rate/increase/delta: (last - first)[/seconds].
            _ => {
                let order = vec![SortExpr::new(col("timestamp"), true, true)];
                let reduced = df
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
                    RangeFn::Increase | RangeFn::Delta => delta,
                    RangeFn::Rate => delta / lit(range.seconds),
                    RangeFn::Deriv | RangeFn::Irate | RangeFn::Idelta => {
                        unreachable!("handled above")
                    }
                };
                reduced
                    .select(vec![
                        col("bucket"),
                        col("metric_name"),
                        col("service_name"),
                        cast_value_f64(per_series).alias("value"),
                    ])
                    .map_err(QuerierError::QueryFailed)?
            }
        };

        // Stage 2: an outer aggregation folds the per-series rates. A bare
        // `rate(...)` (Natural grouping over `service_name`) is already the
        // result.
        if group_cols == ["service_name"] {
            return Ok(df);
        }
        let mut group_exprs = vec![col("bucket"), col("metric_name")];
        group_exprs.extend(group_cols.iter().map(|c| col(*c)));
        let df = df
            .aggregate(
                group_exprs,
                vec![aggregate_expr(aggregate, param).alias("value")],
            )
            .map_err(QuerierError::QueryFailed)?;
        let mut proj = vec![col("bucket"), col("metric_name")];
        proj.extend(group_cols.iter().map(|c| col(*c)));
        proj.push(cast_value_f64(col("value")).alias("value"));
        df.select(proj).map_err(QuerierError::QueryFailed)
    }

    /// `histogram_quantile(phi, metric)`: interpolate the phi-quantile per
    /// (step bucket, series) from stored OTLP histogram buckets.
    ///
    /// SignalDB stores whole histograms per row — `bucket_counts` and
    /// `explicit_bounds` as JSON arrays — rather than Prometheus `_bucket`
    /// series keyed by `le`. Data points that fall in the same step bucket
    /// are merged by summing their bucket counts element-wise (exact for a
    /// single point per bucket and for delta temporality), then the quantile
    /// is interpolated. Output matches the matrix shape: `bucket`,
    /// `metric_name`, `service_name`, `value`.
    #[allow(clippy::too_many_arguments)]
    async fn histogram_query(
        &self,
        plan: &MetricPlan,
        phi: f64,
        start: i64,
        end: i64,
        step: i64,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<RecordBatch>, QuerierError> {
        let table_ref = build_table_reference(tenant_slug, dataset_slug, "metrics_histogram")
            .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
        // No histogram table yet → empty result, not an error.
        let Ok(df) = self.session_context.table(table_ref).await else {
            return Ok(vec![]);
        };
        let df = apply_filters(df, plan, start - plan.offset_ns, end - plan.offset_ns)?;
        // `histogram_quantile(phi, rate(metric[range]))` rates the buckets:
        // the quantile is scale-invariant, so the per-series delta of counts
        // (last − first, ordered by time) suffices — the ÷seconds cancels.
        let rate_mode = plan.range.is_some();
        let batches = df
            .select(vec![
                bucket_expr(step, plan.offset_ns),
                col("metric_name"),
                col("service_name"),
                col("bucket_counts"),
                col("explicit_bounds"),
                cast_ns(col("timestamp")).alias("ts"),
            ])
            .map_err(QuerierError::QueryFailed)?
            .collect()
            .await
            .map_err(QuerierError::QueryFailed)?;

        // Per (bucket, metric_name, service_name): merged counts (instant) or
        // first/last counts (rate). BTreeMap keeps output sorted.
        let mut merged: BTreeMap<(i64, String, String), HistogramAcc> = BTreeMap::new();
        let mut rated: BTreeMap<(i64, String, String), RateHistAcc> = BTreeMap::new();
        for batch in &batches {
            let bucket = batch
                .column_by_name("bucket")
                .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
                .ok_or_else(|| {
                    QuerierError::InvalidInput("bucket column is not a timestamp".to_string())
                })?;
            let name = string_column(batch, "metric_name")?;
            let service = string_column(batch, "service_name")?;
            let counts = string_column(batch, "bucket_counts")?;
            let bounds = string_column(batch, "explicit_bounds")?;
            let sample_ts = batch
                .column_by_name("ts")
                .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
                .ok_or_else(|| {
                    QuerierError::InvalidInput("ts column is not a timestamp".to_string())
                })?;
            for i in 0..batch.num_rows() {
                if bucket.is_null(i) || counts.is_null(i) || bounds.is_null(i) {
                    continue;
                }
                let (Some(row_counts), Some(row_bounds)) = (
                    parse_f64_array(counts.value(i)),
                    parse_f64_array(bounds.value(i)),
                ) else {
                    continue;
                };
                // OTLP invariant: one more bucket count than bound.
                if row_counts.len() != row_bounds.len() + 1 || row_bounds.is_empty() {
                    continue;
                }
                let key = (
                    bucket.value(i),
                    name.value(i).to_string(),
                    service.value(i).to_string(),
                );
                if rate_mode {
                    let t = if sample_ts.is_null(i) {
                        0
                    } else {
                        sample_ts.value(i)
                    };
                    rated
                        .entry(key)
                        .or_insert_with(|| RateHistAcc::new(row_bounds, t, row_counts.clone()))
                        .observe(t, &row_counts);
                } else {
                    merged
                        .entry(key)
                        .or_insert_with(|| HistogramAcc::new(row_bounds, row_counts.len()))
                        .merge(&row_counts);
                }
            }
        }

        // Normalize both modes to (key, bounds, counts) for interpolation.
        // (bucket, metric, service), bounds, counts-for-interpolation.
        type HistGroup = ((i64, String, String), Vec<f64>, Vec<f64>);
        let groups: Vec<HistGroup> = if rate_mode {
            rated
                .into_iter()
                .map(|(k, acc)| (k, acc.bounds.clone(), acc.delta()))
                .collect()
        } else {
            merged
                .into_iter()
                .map(|(k, acc)| (k, acc.bounds, acc.counts))
                .collect()
        };

        let mut ts = Vec::with_capacity(groups.len());
        let mut names = Vec::with_capacity(groups.len());
        let mut services = Vec::with_capacity(groups.len());
        let mut values = Vec::with_capacity(groups.len());
        for ((bucket_ns, metric, service), bounds, counts) in groups {
            let q = match plan.histogram_fraction {
                Some((lo, hi)) => histogram_fraction(lo, hi, &bounds, &counts),
                None => histogram_quantile(phi, &bounds, &counts),
            };
            let val = apply_transforms_f64(q, &plan.transforms);
            // `vector CMP scalar` without `bool`: drop non-matching series.
            if let Some(cmp) = &plan.filter
                && !cmp_f64(val, cmp.op, cmp.scalar, cmp.scalar_left)
            {
                continue;
            }
            ts.push(bucket_ns);
            names.push(metric);
            services.push(service);
            values.push(val);
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "bucket",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampNanosecondArray::from(ts)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(services)),
            Arc::new(Float64Array::from(values)),
        ];
        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
        Ok(vec![batch])
    }

    /// `histogram_count(v)` / `histogram_sum(v)`: sum the stored histogram's
    /// `count`/`sum` column per (step bucket, series).
    #[allow(clippy::too_many_arguments)]
    async fn histogram_scalar_query(
        &self,
        plan: &MetricPlan,
        hf: HistogramFn,
        start: i64,
        end: i64,
        step: i64,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<RecordBatch>, QuerierError> {
        let table_ref = build_table_reference(tenant_slug, dataset_slug, "metrics_histogram")
            .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
        // No histogram table yet → empty result, not an error.
        let Ok(df) = self.session_context.table(table_ref).await else {
            return Ok(vec![]);
        };
        let df = apply_filters(df, plan, start - plan.offset_ns, end - plan.offset_ns)?;
        let column = match hf {
            HistogramFn::Count => "count",
            HistogramFn::Sum => "sum",
        };
        let df = df
            .aggregate(
                vec![
                    bucket_expr(step, plan.offset_ns),
                    col("metric_name"),
                    col("service_name"),
                ],
                vec![sum(col(column)).alias("value")],
            )
            .map_err(QuerierError::QueryFailed)?
            .select(vec![
                col("bucket"),
                col("metric_name"),
                col("service_name"),
                cast_value_f64(col("value")).alias("value"),
            ])
            .map_err(QuerierError::QueryFailed)?;
        df.sort(vec![SortExpr::new(col("bucket"), true, true)])
            .map_err(QuerierError::QueryFailed)?
            .collect()
            .await
            .map_err(QuerierError::QueryFailed)
    }

    /// `resets(v[range])` / `changes(v[range])`: count sequence events over
    /// the ordered samples in each (step bucket, series). Computed row-wise
    /// in Rust since the aggregate path can't see consecutive samples.
    #[allow(clippy::too_many_arguments)]
    async fn sequence_query(
        &self,
        plan: &MetricPlan,
        sf: SequenceFn,
        start: i64,
        end: i64,
        step: i64,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<RecordBatch>, QuerierError> {
        let df = self.scan_union(tenant_slug, dataset_slug).await?;
        let df = apply_filters(df, plan, start - plan.offset_ns, end - plan.offset_ns)?;
        let batches = df
            .select(vec![
                bucket_expr(step, plan.offset_ns),
                col("metric_name"),
                col("service_name"),
                col("value"),
                cast_ns(col("timestamp")).alias("ts"),
            ])
            .map_err(QuerierError::QueryFailed)?
            .collect()
            .await
            .map_err(QuerierError::QueryFailed)?;

        // Group ordered (timestamp, value) samples per (bucket, series).
        let mut groups: BTreeMap<(i64, String, String), Vec<(i64, f64)>> = BTreeMap::new();
        for batch in &batches {
            let bucket = batch
                .column_by_name("bucket")
                .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
                .ok_or_else(|| {
                    QuerierError::InvalidInput("bucket column is not a timestamp".to_string())
                })?;
            let name = string_column(batch, "metric_name")?;
            let service = string_column(batch, "service_name")?;
            let value = batch
                .column_by_name("value")
                .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
                .ok_or_else(|| {
                    QuerierError::InvalidInput("value column is not Float64".to_string())
                })?;
            let ts = batch
                .column_by_name("ts")
                .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
                .ok_or_else(|| {
                    QuerierError::InvalidInput("ts column is not a timestamp".to_string())
                })?;
            for i in 0..batch.num_rows() {
                if bucket.is_null(i) || value.is_null(i) || ts.is_null(i) {
                    continue;
                }
                groups
                    .entry((
                        bucket.value(i),
                        name.value(i).to_string(),
                        service.value(i).to_string(),
                    ))
                    .or_default()
                    .push((ts.value(i), value.value(i)));
            }
        }

        let mut out_ts = Vec::with_capacity(groups.len());
        let mut names = Vec::with_capacity(groups.len());
        let mut services = Vec::with_capacity(groups.len());
        let mut values = Vec::with_capacity(groups.len());
        for ((bucket_ns, metric, service), mut samples) in groups {
            samples.sort_by_key(|(t, _)| *t);
            let mut count = 0.0;
            for w in samples.windows(2) {
                let prev = w[0].1;
                let cur = w[1].1;
                let event = match sf {
                    SequenceFn::Resets => cur < prev,
                    SequenceFn::Changes => cur != prev,
                };
                if event {
                    count += 1.0;
                }
            }
            out_ts.push(bucket_ns);
            names.push(metric);
            services.push(service);
            values.push(count);
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "bucket",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampNanosecondArray::from(out_ts)),
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(services)),
            Arc::new(Float64Array::from(values)),
        ];
        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
        Ok(vec![batch])
    }

    /// Resolve the grouping labels to physical columns. Only labels backed
    /// by a dedicated column can be grouped.
    fn group_columns(&self, plan: &MetricPlan) -> Result<Vec<&'static str>, QuerierError> {
        Self::group_columns_for(&plan.grouping)
    }

    /// Resolve a [`Grouping`] to physical group columns.
    fn group_columns_for(grouping: &Grouping) -> Result<Vec<&'static str>, QuerierError> {
        match grouping {
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
            // `without (labels)` keeps every materialized dimension except
            // the excluded ones. `service_name` (job/service) is the only
            // groupable column, so drop it when it is excluded, otherwise
            // keep it — matching `by` semantics over the supported labels.
            Grouping::Without(labels) => {
                let drops_service = labels
                    .iter()
                    .any(|l| column_for_label(l) == Some("service_name"));
                if drops_service {
                    Ok(vec![])
                } else {
                    Ok(vec!["service_name"])
                }
            }
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

/// Extract `(bucket_ns, service_name, value)` rows from a matrix batch.
/// Series with no `service_name` column (collapsed groupings) use "".
fn matrix_rows(batch: &RecordBatch) -> Result<Vec<(i64, String, f64)>, QuerierError> {
    let bucket = batch
        .column_by_name("bucket")
        .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
        .ok_or_else(|| {
            QuerierError::InvalidInput("bucket column is not a timestamp".to_string())
        })?;
    let service = batch
        .column_by_name("service_name")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let value = batch
        .column_by_name("value")
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
        .ok_or_else(|| QuerierError::InvalidInput("value column is not Float64".to_string()))?;
    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        if bucket.is_null(i) || value.is_null(i) {
            continue;
        }
        let svc = service.map(|c| c.value(i).to_string()).unwrap_or_default();
        out.push((bucket.value(i), svc, value.value(i)));
    }
    Ok(out)
}

/// Like [`matrix_rows`] but keeps the metric name: `(bucket, metric, service,
/// value)`. Used by comparison filtering, which preserves the left labels.
fn matrix_rows_named(batch: &RecordBatch) -> Result<Vec<(i64, String, String, f64)>, QuerierError> {
    let bucket = batch
        .column_by_name("bucket")
        .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
        .ok_or_else(|| {
            QuerierError::InvalidInput("bucket column is not a timestamp".to_string())
        })?;
    let metric = string_column(batch, "metric_name")?;
    let service = batch
        .column_by_name("service_name")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let value = batch
        .column_by_name("value")
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
        .ok_or_else(|| QuerierError::InvalidInput("value column is not Float64".to_string()))?;
    let mut out = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        if bucket.is_null(i) || value.is_null(i) {
            continue;
        }
        let svc = service.map(|c| c.value(i).to_string()).unwrap_or_default();
        out.push((
            bucket.value(i),
            metric.value(i).to_string(),
            svc,
            value.value(i),
        ));
    }
    Ok(out)
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

fn aggregate_expr(agg: MetricAgg, param: Option<f64>) -> Expr {
    use datafusion::functions_aggregate::expr_fn::percentile_cont;
    let value = col("value");
    match agg {
        MetricAgg::Sum => sum(value),
        MetricAgg::Avg => avg(value),
        MetricAgg::Min => min(value),
        MetricAgg::Max => max(value),
        MetricAgg::Count => count(value),
        MetricAgg::Stddev => stddev_pop(value),
        MetricAgg::StdVar => var_pop(value),
        // `group()` yields a constant 1 for every output series.
        MetricAgg::Group => max(lit(1.0_f64)),
        // `quantile(phi, …)` — the phi-quantile of the grouped values.
        MetricAgg::Quantile => {
            let phi = param.unwrap_or(0.5);
            percentile_cont(SortExpr::new(value, true, true), lit(phi))
        }
        // Last value in the bucket, ordered by timestamp.
        MetricAgg::Last => last_value(value, vec![SortExpr::new(col("timestamp"), true, true)]),
    }
}

/// Merges OTLP histogram data points that share a step bucket and series.
struct HistogramAcc {
    /// Upper bounds of the finite buckets (`explicit_bounds`).
    bounds: Vec<f64>,
    /// Running element-wise sum of `bucket_counts` (one more than `bounds`).
    counts: Vec<f64>,
}

impl HistogramAcc {
    fn new(bounds: Vec<f64>, count_len: usize) -> Self {
        Self {
            bounds,
            counts: vec![0.0; count_len],
        }
    }

    fn merge(&mut self, row_counts: &[f64]) {
        if row_counts.len() == self.counts.len() {
            for (acc, add) in self.counts.iter_mut().zip(row_counts) {
                *acc += add;
            }
        }
    }
}

/// Tracks the first and last histogram data points (by timestamp) for a
/// series in a bucket, so `histogram_quantile(phi, rate(metric[range]))` can
/// interpolate over the per-bucket count delta.
struct RateHistAcc {
    bounds: Vec<f64>,
    first_ts: i64,
    first: Vec<f64>,
    last_ts: i64,
    last: Vec<f64>,
}

impl RateHistAcc {
    fn new(bounds: Vec<f64>, ts: i64, counts: Vec<f64>) -> Self {
        Self {
            bounds,
            first_ts: ts,
            first: counts.clone(),
            last_ts: ts,
            last: counts,
        }
    }

    fn observe(&mut self, ts: i64, counts: &[f64]) {
        if counts.len() != self.first.len() {
            return;
        }
        if ts <= self.first_ts {
            self.first_ts = ts;
            self.first = counts.to_vec();
        }
        if ts >= self.last_ts {
            self.last_ts = ts;
            self.last = counts.to_vec();
        }
    }

    /// Per-bucket increase, clamped to ≥ 0 (a decrease means a counter reset).
    fn delta(&self) -> Vec<f64> {
        self.last
            .iter()
            .zip(&self.first)
            .map(|(l, f)| (l - f).max(0.0))
            .collect()
    }
}

/// Parse a JSON numeric array (`"[1,2,3]"`) into `f64`s.
fn parse_f64_array(raw: &str) -> Option<Vec<f64>> {
    let value: serde_json::Value = serde_json::from_str(raw).ok()?;
    let array = value.as_array()?;
    array.iter().map(|v| v.as_f64()).collect()
}

/// Interpolate the `phi`-quantile of a classic histogram, following
/// Prometheus's `bucketQuantile`: locate the bucket the rank falls in and
/// linearly interpolate within it, assuming a uniform spread.
///
/// `bounds` are the finite bucket upper bounds; `counts` are the
/// non-cumulative per-bucket counts with one extra `+Inf` bucket
/// (`counts.len() == bounds.len() + 1`).
fn histogram_quantile(phi: f64, bounds: &[f64], counts: &[f64]) -> f64 {
    if phi.is_nan() {
        return f64::NAN;
    }
    if phi < 0.0 {
        return f64::NEG_INFINITY;
    }
    if phi > 1.0 {
        return f64::INFINITY;
    }
    if bounds.is_empty() || counts.len() != bounds.len() + 1 {
        return f64::NAN;
    }
    let total: f64 = counts.iter().sum();
    if total <= 0.0 {
        return f64::NAN;
    }

    // Cumulative counts across buckets.
    let mut cumulative = Vec::with_capacity(counts.len());
    let mut running = 0.0;
    for &c in counts {
        running += c;
        cumulative.push(running);
    }

    let rank = phi * total;
    let last = counts.len() - 1;
    let b = cumulative.iter().position(|&c| c >= rank).unwrap_or(last);

    // Rank lands in the open-ended `+Inf` bucket: clamp to the top finite
    // bound (we can't extrapolate beyond it).
    if b == last {
        return bounds[bounds.len() - 1];
    }

    let bucket_end = bounds[b];
    let (bucket_start, rank_in_bucket, count_in_bucket) = if b == 0 {
        // Prometheus: a non-positive first bound is returned as-is.
        if bucket_end <= 0.0 {
            return bucket_end;
        }
        (0.0, rank, cumulative[0])
    } else {
        (bounds[b - 1], rank - cumulative[b - 1], counts[b])
    };
    if count_in_bucket <= 0.0 {
        return bucket_start;
    }
    bucket_start + (bucket_end - bucket_start) * (rank_in_bucket / count_in_bucket)
}

/// The fraction of a classic histogram's observations in `(lower, upper]`,
/// via the cumulative distribution (the inverse of [`histogram_quantile`]'s
/// interpolation): buckets span `(prev_bound, bound]` with the first starting
/// at 0, observations spread uniformly, and the open `+Inf` bucket sits above
/// the last finite bound.
fn histogram_fraction(lower: f64, upper: f64, bounds: &[f64], counts: &[f64]) -> f64 {
    if bounds.is_empty() || counts.len() != bounds.len() + 1 {
        return f64::NAN;
    }
    let total: f64 = counts.iter().sum();
    if total <= 0.0 {
        return f64::NAN;
    }
    (hist_cumulative(upper, bounds, counts) - hist_cumulative(lower, bounds, counts)) / total
}

/// Cumulative count of observations `<= x`, interpolated within the bucket.
fn hist_cumulative(x: f64, bounds: &[f64], counts: &[f64]) -> f64 {
    let n = bounds.len();
    // At or above the top finite bound: all finite-bucket observations count;
    // the `+Inf` bucket's observations are strictly greater.
    if x >= bounds[n - 1] {
        return counts[..n].iter().sum();
    }
    // First finite bucket whose upper bound is >= x contains x.
    let b = bounds.iter().position(|&bd| bd >= x).unwrap_or(0);
    let bucket_start = if b == 0 { 0.0 } else { bounds[b - 1] };
    let before: f64 = counts[..b].iter().sum();
    if x <= bucket_start {
        return before;
    }
    let width = bounds[b] - bucket_start;
    if width <= 0.0 {
        return before;
    }
    let frac = ((x - bucket_start) / width).clamp(0.0, 1.0);
    before + counts[b] * frac
}

/// Re-project a matrix DataFrame with the `value` column transformed by
/// `ops`, preserving `bucket`, `metric_name`, and the grouping columns.
fn apply_transforms_df(
    df: DataFrame,
    ops: &[ValueOp],
    group_cols: &[&str],
) -> Result<DataFrame, QuerierError> {
    if ops.is_empty() {
        return Ok(df);
    }
    let mut proj = vec![col("bucket"), col("metric_name")];
    proj.extend(group_cols.iter().map(|c| col(*c)));
    proj.push(apply_value_ops_expr(col("value"), ops).alias("value"));
    df.select(proj).map_err(QuerierError::QueryFailed)
}

/// Compose the value transforms into a DataFusion expression.
fn apply_value_ops_expr(mut value: Expr, ops: &[ValueOp]) -> Expr {
    use datafusion::functions::math::expr_fn as math;
    use datafusion::logical_expr::when;
    for op in ops {
        value = match op {
            ValueOp::Abs => math::abs(value),
            ValueOp::Ceil => math::ceil(value),
            ValueOp::Floor => math::floor(value),
            ValueOp::Round => math::round(vec![value]),
            ValueOp::Sqrt => math::sqrt(value),
            ValueOp::Exp => math::exp(value),
            ValueOp::Ln => math::ln(value),
            ValueOp::Log2 => math::log2(value),
            ValueOp::Log10 => math::log10(value),
            ValueOp::Sgn => math::signum(value),
            ValueOp::ClampMin(m) => when(value.clone().lt(lit(*m)), lit(*m))
                .otherwise(value)
                .unwrap(),
            ValueOp::ClampMax(m) => when(value.clone().gt(lit(*m)), lit(*m))
                .otherwise(value)
                .unwrap(),
            ValueOp::Clamp(lo, hi) => {
                let clamped_low = when(value.clone().lt(lit(*lo)), lit(*lo))
                    .otherwise(value)
                    .unwrap();
                when(clamped_low.clone().gt(lit(*hi)), lit(*hi))
                    .otherwise(clamped_low)
                    .unwrap()
            }
            ValueOp::Arith(arith, s, scalar_left) => arith_expr(*arith, value, *s, *scalar_left),
            ValueOp::CompareBool(op, s, scalar_left) => {
                when(cmp_bool_expr(value, *op, *s, *scalar_left), lit(1.0_f64))
                    .otherwise(lit(0.0_f64))
                    .unwrap()
            }
        };
    }
    value
}

/// Build a boolean `value CMP scalar` (or `scalar CMP value`) expression,
/// used both for the `bool` value map and for comparison filtering.
fn cmp_bool_expr(value: Expr, op: CmpOp, scalar: f64, scalar_left: bool) -> Expr {
    let s = lit(scalar);
    let (l, r) = if scalar_left { (s, value) } else { (value, s) };
    match op {
        CmpOp::Eq => l.eq(r),
        CmpOp::Ne => l.not_eq(r),
        CmpOp::Gt => l.gt(r),
        CmpOp::Lt => l.lt(r),
        CmpOp::Ge => l.gt_eq(r),
        CmpOp::Le => l.lt_eq(r),
    }
}

/// Build `value OP scalar` (or `scalar OP value`) as an expression.
fn arith_expr(op: ArithOp, value: Expr, scalar: f64, scalar_left: bool) -> Expr {
    use datafusion::functions::math::expr_fn::power;
    let s = lit(scalar);
    match (op, scalar_left) {
        (ArithOp::Add, _) => value + s,
        (ArithOp::Mul, _) => value * s,
        (ArithOp::Sub, false) => value - s,
        (ArithOp::Sub, true) => s - value,
        (ArithOp::Div, false) => value / s,
        (ArithOp::Div, true) => s / value,
        (ArithOp::Mod, false) => value % s,
        (ArithOp::Mod, true) => s % value,
        (ArithOp::Pow, false) => power(value, s),
        (ArithOp::Pow, true) => power(s, value),
    }
}

/// Apply the value transforms to a scalar (histogram path).
fn apply_transforms_f64(mut v: f64, ops: &[ValueOp]) -> f64 {
    for op in ops {
        v = match op {
            ValueOp::Abs => v.abs(),
            ValueOp::Ceil => v.ceil(),
            ValueOp::Floor => v.floor(),
            ValueOp::Round => v.round(),
            ValueOp::Sqrt => v.sqrt(),
            ValueOp::Exp => v.exp(),
            ValueOp::Ln => v.ln(),
            ValueOp::Log2 => v.log2(),
            ValueOp::Log10 => v.log10(),
            ValueOp::Sgn => v.signum(),
            ValueOp::ClampMin(m) => v.max(*m),
            ValueOp::ClampMax(m) => v.min(*m),
            ValueOp::Clamp(lo, hi) => v.clamp(*lo, *hi),
            ValueOp::Arith(arith, s, scalar_left) => arith_f64(*arith, v, *s, *scalar_left),
            ValueOp::CompareBool(op, s, scalar_left) => {
                if cmp_f64(v, *op, *s, *scalar_left) {
                    1.0
                } else {
                    0.0
                }
            }
        };
    }
    v
}

/// Evaluate a scalar comparison for the histogram (row-wise) path.
fn cmp_f64(v: f64, op: CmpOp, scalar: f64, scalar_left: bool) -> bool {
    let (l, r) = if scalar_left {
        (scalar, v)
    } else {
        (v, scalar)
    };
    match op {
        CmpOp::Eq => l == r,
        CmpOp::Ne => l != r,
        CmpOp::Gt => l > r,
        CmpOp::Lt => l < r,
        CmpOp::Ge => l >= r,
        CmpOp::Le => l <= r,
    }
}

fn arith_f64(op: ArithOp, v: f64, s: f64, scalar_left: bool) -> f64 {
    match (op, scalar_left) {
        (ArithOp::Add, _) => v + s,
        (ArithOp::Mul, _) => v * s,
        (ArithOp::Sub, false) => v - s,
        (ArithOp::Sub, true) => s - v,
        (ArithOp::Div, false) => v / s,
        (ArithOp::Div, true) => s / v,
        (ArithOp::Mod, false) => v % s,
        (ArithOp::Mod, true) => s % v,
        (ArithOp::Pow, false) => v.powf(s),
        (ArithOp::Pow, true) => s.powf(v),
    }
}

/// Keep the k highest (or lowest, for `bottomk`) series per bucket. The input
/// matrix has columns `bucket`, `metric_name`, `service_name`, `value`.
fn apply_topk(batches: Vec<RecordBatch>, spec: TopKSpec) -> Result<Vec<RecordBatch>, QuerierError> {
    use std::cmp::Ordering;

    if batches.is_empty() {
        return Ok(batches);
    }
    let schema = batches[0].schema();

    struct Row {
        bucket: i64,
        name: String,
        service: String,
        value: f64,
    }
    let mut rows: Vec<Row> = Vec::new();
    for batch in &batches {
        let bucket = batch
            .column_by_name("bucket")
            .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
            .ok_or_else(|| QuerierError::InvalidInput("bucket column missing".to_string()))?;
        let name = string_column(batch, "metric_name")?;
        let service = string_column(batch, "service_name")?;
        let value = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| QuerierError::InvalidInput("value column missing".to_string()))?;
        for i in 0..batch.num_rows() {
            rows.push(Row {
                bucket: bucket.value(i),
                name: name.value(i).to_string(),
                service: if service.is_null(i) {
                    String::new()
                } else {
                    service.value(i).to_string()
                },
                value: if value.is_null(i) {
                    f64::NAN
                } else {
                    value.value(i)
                },
            });
        }
    }

    // Rank within each bucket; BTreeMap keeps buckets in ascending order.
    let mut by_bucket: BTreeMap<i64, Vec<usize>> = BTreeMap::new();
    for (i, r) in rows.iter().enumerate() {
        by_bucket.entry(r.bucket).or_default().push(i);
    }
    let mut keep: Vec<usize> = Vec::new();
    for (_bucket, mut idxs) in by_bucket {
        idxs.sort_by(|&a, &b| {
            let (va, vb) = (rows[a].value, rows[b].value);
            let ord = va.partial_cmp(&vb).unwrap_or(Ordering::Equal);
            if spec.bottom { ord } else { ord.reverse() }
        });
        idxs.truncate(spec.k);
        keep.extend(idxs);
    }

    let mut ts = Vec::with_capacity(keep.len());
    let mut names = Vec::with_capacity(keep.len());
    let mut services = Vec::with_capacity(keep.len());
    let mut values = Vec::with_capacity(keep.len());
    for &i in &keep {
        ts.push(rows[i].bucket);
        names.push(rows[i].name.clone());
        services.push(rows[i].service.clone());
        values.push(rows[i].value);
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(ts)),
        Arc::new(StringArray::from(names)),
        Arc::new(StringArray::from(services)),
        Arc::new(Float64Array::from(values)),
    ];
    let out = RecordBatch::try_new(schema, columns)
        .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
    Ok(vec![out])
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
fn bucket_expr(step: i64, offset_ns: i64) -> Expr {
    let stride = lit(ScalarValue::IntervalMonthDayNano(Some(
        IntervalMonthDayNano::new(0, 0, step),
    )));
    let origin = lit(ScalarValue::TimestampNanosecond(Some(0), None));
    // `offset` shifts the sampled window back in time; add it back to the
    // timestamp before binning so results align to the query grid.
    let ts = if offset_ns != 0 {
        cast_ns(col("timestamp"))
            + lit(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano::new(0, 0, offset_ns),
            )))
    } else {
        cast_ns(col("timestamp"))
    };
    date_bin(stride, ts, origin).alias("bucket")
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
    async fn offset_shifts_the_query_window() {
        let service = service_with_data();
        // Without offset the samples (ts 100–300ns) fall in the [0,1000] window.
        assert!(!matrix(&service, "reqs", 1000).await.is_empty());
        // A 5m offset shifts the sampled window far into the past → no samples.
        assert!(matrix(&service, "reqs offset 5m", 1000).await.is_empty());
    }

    #[tokio::test]
    async fn sum_collapses_series() {
        let service = service_with_data();
        let out = matrix(&service, "sum(reqs)", 1000).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].2, 9.0); // 1+3+5
    }

    #[tokio::test]
    async fn quantile_over_time_is_percentile_per_series() {
        let service = service_with_data();
        // api samples [1, 3] → median 2; web [5] → 5.
        let out = matrix(&service, "quantile_over_time(0.5, reqs[1m])", 1000).await;
        let by = |svc: &str| {
            out.iter()
                .find(|(_, s, _)| s.as_deref() == Some(svc))
                .unwrap()
                .2
        };
        assert!((by("api") - 2.0).abs() < 1e-9, "got {}", by("api"));
        assert!((by("web") - 5.0).abs() < 1e-9, "got {}", by("web"));
    }

    #[tokio::test]
    async fn quantile_is_percentile_across_the_bucket() {
        let service = service_with_data();
        // Values in the bucket are [1, 3, 5]; the 0.5-quantile (median) is 3.
        let out = matrix(&service, "quantile(0.5, reqs)", 1000).await;
        assert_eq!(out.len(), 1);
        assert!((out[0].2 - 3.0).abs() < 1e-9, "got {}", out[0].2);
        // The 0-quantile is the minimum.
        let out = matrix(&service, "quantile(0, reqs)", 1000).await;
        assert!((out[0].2 - 1.0).abs() < 1e-9, "got {}", out[0].2);
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
    async fn deriv_is_regression_slope_per_second() {
        let service = service_with_data();
        // api samples: (t=100ns, 1), (t=200ns, 3). Slope over Δt=100ns is
        // 2 / 100e-9 = 2e7 values/second.
        let out = matrix(&service, "deriv(reqs[1m])", 1000).await;
        let api = out
            .iter()
            .find(|(_, s, _)| s.as_deref() == Some("api"))
            .unwrap();
        assert!((api.2 - 2.0e7).abs() < 1.0, "got {}", api.2);
    }

    #[tokio::test]
    async fn irate_and_idelta_use_last_two_samples() {
        let service = service_with_data();
        // api samples: (100ns, 1), (200ns, 3). idelta = 3 - 1 = 2.
        let out = matrix(&service, "idelta(reqs[1m])", 1000).await;
        let api = out
            .iter()
            .find(|(_, s, _)| s.as_deref() == Some("api"))
            .unwrap();
        assert_eq!(api.2, 2.0);

        // irate = 2 / (Δt = 100ns = 1e-7 s) = 2e7.
        let out = matrix(&service, "irate(reqs[1m])", 1000).await;
        let api = out
            .iter()
            .find(|(_, s, _)| s.as_deref() == Some("api"))
            .unwrap();
        assert!((api.2 - 2.0e7).abs() < 1.0, "got {}", api.2);
    }

    #[tokio::test]
    async fn changes_and_resets_count_sequence_events() {
        let service = service_with_data();
        // api samples [1, 3] → 1 change, 0 resets. web [5] → 0.
        let changes = matrix(&service, "changes(reqs[1m])", 1000).await;
        let api = changes
            .iter()
            .find(|(_, s, _)| s.as_deref() == Some("api"))
            .unwrap();
        assert_eq!(api.2, 1.0);
        let resets = matrix(&service, "resets(reqs[1m])", 1000).await;
        let api = resets
            .iter()
            .find(|(_, s, _)| s.as_deref() == Some("api"))
            .unwrap();
        assert_eq!(api.2, 0.0);
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
    async fn over_time_reduces_samples_per_bucket() {
        let service = service_with_data();
        // api has samples [1,3] in one bucket, web has [5].
        let avg = matrix(&service, "avg_over_time(reqs[1m])", 1000).await;
        let api = avg
            .iter()
            .find(|(_, s, _)| s.as_deref() == Some("api"))
            .unwrap();
        assert_eq!(api.2, 2.0); // (1+3)/2
        assert_eq!(
            matrix(&service, "sum_over_time(reqs[1m])", 1000)
                .await
                .iter()
                .find(|(_, s, _)| s.as_deref() == Some("api"))
                .unwrap()
                .2,
            4.0
        );
        assert_eq!(
            matrix(&service, "count_over_time(reqs[1m])", 1000)
                .await
                .iter()
                .find(|(_, s, _)| s.as_deref() == Some("api"))
                .unwrap()
                .2,
            2.0
        );
        // Population stddev of [1,3] is 1.0.
        let sd = matrix(&service, "stddev_over_time(reqs[1m])", 1000).await;
        let api_sd = sd
            .iter()
            .find(|(_, s, _)| s.as_deref() == Some("api"))
            .unwrap();
        assert!((api_sd.2 - 1.0).abs() < 1e-9, "got {}", api_sd.2);
    }

    #[tokio::test]
    async fn over_time_under_aggregation_folds_series() {
        let service = service_with_data();
        // avg_over_time per series: api=(1+3)/2=2, web=5. sum → 7.
        let out = matrix(&service, "sum(avg_over_time(reqs[1m]))", 1000).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].2, 7.0);
        // max per service: api=3, web=5 (max_over_time), then min across → 3.
        let out = matrix(&service, "min(max_over_time(reqs[1m]))", 1000).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].2, 3.0);
    }

    #[tokio::test]
    async fn scalar_arithmetic_and_math_transform_value() {
        let service = service_with_data();
        // Bare selector → last value per series: api=3, web=5.
        let by = |out: Vec<(String, Option<String>, f64)>, svc: &str| {
            out.into_iter()
                .find(|(_, s, _)| s.as_deref() == Some(svc))
                .unwrap()
                .2
        };
        assert_eq!(by(matrix(&service, "reqs * 2", 1000).await, "api"), 6.0);
        assert_eq!(by(matrix(&service, "reqs + 10", 1000).await, "web"), 15.0);
        // clamp_max caps web's 5 at 4; api's 3 stays.
        assert_eq!(
            by(matrix(&service, "clamp_max(reqs, 4)", 1000).await, "web"),
            4.0
        );
        assert_eq!(
            by(matrix(&service, "clamp_max(reqs, 4)", 1000).await, "api"),
            3.0
        );
        // 10 / reqs → api 10/3.
        assert!((by(matrix(&service, "10 / reqs", 1000).await, "api") - 10.0 / 3.0).abs() < 1e-9);
        // abs over a rate that is negative is not exercised here; abs of a
        // positive last value is a no-op.
        assert_eq!(by(matrix(&service, "abs(reqs)", 1000).await, "api"), 3.0);
    }

    #[tokio::test]
    async fn vector_arithmetic_matches_on_service() {
        let service = service_with_data();
        // reqs last per series: api=3, web=5. `reqs / reqs` matches each
        // series to itself → 1.0, and drops the metric name.
        let out = matrix(&service, "reqs / reqs", 1000).await;
        assert_eq!(out.len(), 2);
        for (name, _svc, v) in &out {
            assert_eq!(name, "");
            assert_eq!(*v, 1.0);
        }
        // `reqs * reqs`: api 3*3=9, web 5*5=25.
        let mul = matrix(&service, "reqs * reqs", 1000).await;
        let api = mul
            .iter()
            .find(|(_, s, _)| s.as_deref() == Some("api"))
            .unwrap();
        assert_eq!(api.2, 9.0);
        // `reqs - reqs` = 0 per series.
        let sub = matrix(&service, "reqs - reqs", 1000).await;
        assert!(sub.iter().all(|(_, _, v)| *v == 0.0));
    }

    #[tokio::test]
    async fn logical_set_ops_match_on_identity() {
        let service = service_with_data();
        // reqs: api=3, web=5. `reqs > 4` keeps only web(5).
        // `and` → series with a match on the right = web only.
        let and = matrix(&service, "reqs and (reqs > 4)", 1000).await;
        assert_eq!(and.len(), 1);
        assert_eq!(and[0].1.as_deref(), Some("web"));
        assert_eq!(and[0].2, 5.0);
        // `unless` → series without a match = api only.
        let unless = matrix(&service, "reqs unless (reqs > 4)", 1000).await;
        assert_eq!(unless.len(), 1);
        assert_eq!(unless[0].1.as_deref(), Some("api"));
        // `or` → union = both series.
        let or = matrix(&service, "reqs or (reqs > 4)", 1000).await;
        assert_eq!(or.len(), 2);
    }

    #[tokio::test]
    async fn vector_comparison_filters_and_bools() {
        let service = service_with_data();
        // api=3, web=5. `reqs >= reqs` holds for both → both kept, with the
        // left value and metric name preserved (filtering comparison).
        let out = matrix(&service, "reqs >= reqs", 1000).await;
        assert_eq!(out.len(), 2);
        let api = out
            .iter()
            .find(|(_, s, _)| s.as_deref() == Some("api"))
            .unwrap();
        assert_eq!(api.0, "reqs"); // metric name preserved for filtering
        assert_eq!(api.2, 3.0);
        // A comparison that never holds → empty.
        assert!(matrix(&service, "reqs > reqs", 1000).await.is_empty());
        // `bool` maps to 1/0 and drops the name: 3>=3 and 5>=5 → 1.0 each.
        let b = matrix(&service, "reqs >= bool reqs", 1000).await;
        assert!(!b.is_empty());
        assert!(b.iter().all(|(n, _, v)| n.is_empty() && *v == 1.0));
    }

    #[tokio::test]
    async fn scalar_comparison_filters_series() {
        let service = service_with_data();
        // Bare selector → api=3, web=5. `reqs > 4` keeps only web.
        let out = matrix(&service, "reqs > 4", 1000).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1.as_deref(), Some("web"));
        assert_eq!(out[0].2, 5.0);

        // `4 < reqs` is equivalent (scalar on the left).
        let out = matrix(&service, "4 < reqs", 1000).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1.as_deref(), Some("web"));

        // No series match → empty result.
        assert!(matrix(&service, "reqs > 100", 1000).await.is_empty());
    }

    #[tokio::test]
    async fn scalar_comparison_bool_maps_to_one_or_zero() {
        let service = service_with_data();
        // `reqs > bool 4`: api(3) → 0, web(5) → 1. Both series retained.
        let out = matrix(&service, "reqs > bool 4", 1000).await;
        let by = |svc: &str| {
            out.iter()
                .find(|(_, s, _)| s.as_deref() == Some(svc))
                .unwrap()
                .2
        };
        assert_eq!(out.len(), 2);
        assert_eq!(by("api"), 0.0);
        assert_eq!(by("web"), 1.0);
    }

    #[tokio::test]
    async fn topk_and_bottomk_select_series_per_bucket() {
        let service = service_with_data();
        // One bucket, series api=3 and web=5.
        let top = matrix(&service, "topk(1, reqs)", 1000).await;
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].1.as_deref(), Some("web"));
        assert_eq!(top[0].2, 5.0);

        let bottom = matrix(&service, "bottomk(1, reqs)", 1000).await;
        assert_eq!(bottom.len(), 1);
        assert_eq!(bottom[0].1.as_deref(), Some("api"));
        assert_eq!(bottom[0].2, 3.0);

        // k larger than the series count keeps all.
        assert_eq!(matrix(&service, "topk(5, reqs)", 1000).await.len(), 2);
    }

    #[tokio::test]
    async fn non_positive_step_rejected() {
        let service = service_with_data();
        assert!(matches!(
            service.query_range("reqs", 0, 1000, 0, "t", "d").await,
            Err(QuerierError::InvalidInput(_))
        ));
    }

    // ---- histogram_quantile ----

    #[test]
    fn histogram_quantile_interpolates_within_bucket() {
        // bounds [1,2,4], counts [1,2,3,4] (incl. +Inf), total 10.
        // rank 5 lands in the (2,4] bucket: 2 + 2*(5-3)/3 = 3.333…
        let bounds = [1.0, 2.0, 4.0];
        let counts = [1.0, 2.0, 3.0, 4.0];
        assert!((histogram_quantile(0.5, &bounds, &counts) - (2.0 + 2.0 * 2.0 / 3.0)).abs() < 1e-9);
        // rank 1 lands at the top of the first bucket: exactly its bound.
        assert!((histogram_quantile(0.1, &bounds, &counts) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn histogram_quantile_in_inf_bucket_clamps_to_top_bound() {
        let bounds = [1.0, 2.0, 4.0];
        let counts = [1.0, 2.0, 3.0, 4.0];
        // rank 9.5 falls in the +Inf bucket → clamp to the highest bound.
        assert_eq!(histogram_quantile(0.95, &bounds, &counts), 4.0);
    }

    #[test]
    fn histogram_quantile_edge_cases() {
        let bounds = [1.0, 2.0];
        let counts = [1.0, 1.0, 1.0];
        assert!(histogram_quantile(f64::NAN, &bounds, &counts).is_nan());
        assert_eq!(
            histogram_quantile(-0.1, &bounds, &counts),
            f64::NEG_INFINITY
        );
        assert_eq!(histogram_quantile(1.5, &bounds, &counts), f64::INFINITY);
        // No observations → NaN.
        assert!(histogram_quantile(0.5, &bounds, &[0.0, 0.0, 0.0]).is_nan());
        // Malformed (counts length != bounds+1) → NaN.
        assert!(histogram_quantile(0.5, &bounds, &[1.0, 1.0]).is_nan());
    }

    #[test]
    fn parse_f64_array_handles_json_and_junk() {
        assert_eq!(parse_f64_array("[1, 2.5, 3]"), Some(vec![1.0, 2.5, 3.0]));
        assert_eq!(parse_f64_array("[]"), Some(vec![]));
        assert_eq!(parse_f64_array("not json"), None);
        assert_eq!(parse_f64_array(r#"{"a":1}"#), None);
    }

    fn service_with_histogram() -> MetricsService {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
            Field::new("sum", DataType::Float64, true),
            Field::new("bucket_counts", DataType::Utf8, true),
            Field::new("explicit_bounds", DataType::Utf8, true),
            Field::new("attributes", DataType::Utf8, true),
            Field::new("resource_attributes", DataType::Utf8, true),
        ]));
        // Two data points for the same series in one step bucket. Merging
        // scales the counts (×2) without moving the quantile.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                // Two cumulative data points in one step bucket at t=100/200.
                Arc::new(TimestampNanosecondArray::from(vec![100, 200])),
                Arc::new(StringArray::from(vec!["api", "api"])),
                Arc::new(StringArray::from(vec!["latency", "latency"])),
                Arc::new(datafusion::arrow::array::Int64Array::from(vec![10, 10])),
                Arc::new(Float64Array::from(vec![55.0, 55.0])),
                Arc::new(StringArray::from(vec!["[1,2,3,4]", "[2,4,6,8]"])),
                Arc::new(StringArray::from(vec!["[1,2,4]", "[1,2,4]"])),
                Arc::new(StringArray::from(vec!["{}", "{}"])),
                Arc::new(StringArray::from(vec!["{}", "{}"])),
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let schema_provider = Arc::new(MemorySchemaProvider::new());
        schema_provider
            .register_table("metrics_histogram".to_string(), Arc::new(table))
            .unwrap();
        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog.register_schema("d", schema_provider).unwrap();
        ctx.register_catalog("t", catalog);
        MetricsService::new(ctx)
    }

    #[tokio::test]
    async fn histogram_quantile_query_interpolates_merged_series() {
        let service = service_with_histogram();
        let out = matrix(&service, "histogram_quantile(0.5, latency)", 1000).await;
        assert_eq!(out.len(), 1);
        let (name, svc, value) = &out[0];
        assert_eq!(name, "latency");
        assert_eq!(svc.as_deref(), Some("api"));
        assert!(
            (value - (2.0 + 2.0 * 2.0 / 3.0)).abs() < 1e-9,
            "got {value}"
        );
    }

    #[tokio::test]
    async fn histogram_quantile_over_rate_uses_bucket_delta() {
        let service = service_with_histogram();
        // counts go [1,2,3,4] → [2,4,6,8] over the bucket; the delta [1,2,3,4]
        // has the same shape, so the 0.5-quantile is the same 3.333….
        let out = matrix(&service, "histogram_quantile(0.5, rate(latency[5m]))", 1000).await;
        assert_eq!(out.len(), 1);
        assert!(
            (out[0].2 - (2.0 + 2.0 * 2.0 / 3.0)).abs() < 1e-9,
            "got {}",
            out[0].2
        );
    }

    #[tokio::test]
    async fn histogram_fraction_is_cdf_delta() {
        let service = service_with_histogram();
        // Merged counts [3,6,9,12] over bounds [1,2,4], total 30.
        // Observations in (0, 2] = 3+6 = 9 → fraction 0.3.
        let out = matrix(&service, "histogram_fraction(0, 2, latency)", 1000).await;
        assert_eq!(out.len(), 1);
        assert!((out[0].2 - 0.3).abs() < 1e-9, "got {}", out[0].2);
        // (0, 4] covers three finite buckets = 18/30 = 0.6.
        let out = matrix(&service, "histogram_fraction(0, 4, latency)", 1000).await;
        assert!((out[0].2 - 0.6).abs() < 1e-9, "got {}", out[0].2);
    }

    #[tokio::test]
    async fn histogram_quantile_without_table_is_empty() {
        // The gauge-only service has no metrics_histogram table.
        let service = service_with_data();
        let out = matrix(&service, "histogram_quantile(0.9, latency)", 1000).await;
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn sort_orders_output_by_value() {
        let service = service_with_data();
        // api=3, web=5. sort → ascending (api first); sort_desc → web first.
        let asc = matrix(&service, "sort(reqs)", 1000).await;
        assert_eq!(asc.first().unwrap().1.as_deref(), Some("api"));
        assert_eq!(asc.last().unwrap().1.as_deref(), Some("web"));
        let desc = matrix(&service, "sort_desc(reqs)", 1000).await;
        assert_eq!(desc.first().unwrap().1.as_deref(), Some("web"));
    }

    #[tokio::test]
    async fn timestamp_returns_latest_sample_time_in_seconds() {
        let service = service_with_data();
        // api's latest sample is at t=200ns → 200e-9 s; web's at 300ns.
        let out = matrix(&service, "timestamp(reqs)", 1000).await;
        let api = out
            .iter()
            .find(|(_, s, _)| s.as_deref() == Some("api"))
            .unwrap();
        assert!((api.2 - 200e-9).abs() < 1e-18, "got {}", api.2);
        let web = out
            .iter()
            .find(|(_, s, _)| s.as_deref() == Some("web"))
            .unwrap();
        assert!((web.2 - 300e-9).abs() < 1e-18, "got {}", web.2);
    }

    #[tokio::test]
    async fn absent_emits_one_for_empty_buckets() {
        let service = service_with_data();
        // reqs exists (bucket 0) → fewer absent buckets than a missing metric.
        let present = matrix(&service, "absent(reqs)", 1000).await;
        let missing = matrix(&service, "absent(nope)", 1000).await;
        assert!(missing.len() > present.len());
        assert!(missing.iter().all(|(n, _, v)| n.is_empty() && *v == 1.0));
        // A `job` equality matcher becomes the output service label.
        let ghost = matrix(&service, r#"absent(reqs{job="ghost"})"#, 1000).await;
        assert!(!ghost.is_empty());
        assert!(
            ghost
                .iter()
                .all(|(_, s, v)| s.as_deref() == Some("ghost") && *v == 1.0)
        );
    }

    #[tokio::test]
    async fn histogram_count_and_sum_aggregate_the_columns() {
        let service = service_with_histogram();
        // Two points: count 10+10 = 20, sum 55+55 = 110.
        let out = matrix(&service, "histogram_count(latency)", 1000).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].2, 20.0);

        let out = matrix(&service, "histogram_sum(latency)", 1000).await;
        assert_eq!(out.len(), 1);
        assert!((out[0].2 - 110.0).abs() < 1e-9, "got {}", out[0].2);
    }
}
