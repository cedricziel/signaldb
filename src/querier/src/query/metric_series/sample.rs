//! The `sample` stage (D11): evaluate a metric point stream into a Series
//! frame `(bucket, __labels, value)` at the instants `t = from + k·step`.
//!
//! Each point is assigned to every instant whose read window
//! `(t − offset − window, t − offset]` covers it, then one windowed
//! accumulator per `(series_id, instant)` evaluates the function. `at` pins
//! the read window to `at − offset` and repeats its value at every instant.

use common::query_ir::{
    Document, Literal, Sample, SampleFn, SampleOf, Stage, ValueType, coerce, parse_duration_ns,
};
use common::schema::typed_attributes::has_typed_container;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::functions_aggregate::expr_fn::first_value;
use datafusion::functions_nested::expr_fn::gen_series;
use datafusion::logical_expr::{Expr, cast, col, lit};
use datafusion::prelude::{DataFrame, ident};
use datafusion::scalar::ScalarValue;

use super::label_ops::labels_drop_name_udf;
use super::labels::{LABELS_COLUMN, bag_arg, series_labels_udf};
use crate::query::error::QuerierError;
use crate::query::ir_planner::ResolvedWindow;
use crate::query::metric_ops::instants::covering_instants_udf;
use crate::query::metric_ops::range::range_udaf;
use crate::query::metric_ops::range_math::RangeFn;

/// `latest`'s lookback when the stage names none.
const DEFAULT_LOOKBACK_NS: i64 = 5 * 60 * 1_000_000_000;
/// Most evaluation instants one query may evaluate (Prometheus' 11k-point limit).
pub(crate) const MAX_INSTANTS: i64 = 11_000;
const INSTANT: &str = "__instant";
/// The raw columns a series' labels derive from, carried through the
/// aggregate so `series_labels` runs once per output row, not per point.
const LABEL_INPUTS: [&str; 6] = [
    "metric_name",
    "service_name",
    "scope_name",
    "scope_version",
    "__resource",
    "__attrs",
];

/// What a `sample` stage needs from the document beyond the stage itself.
pub(crate) struct SampleEnv<'a> {
    pub window: ResolvedWindow,
    pub doc_step: Option<&'a str>,
    pub now_ns: i64,
    /// The scanned table's physical column names.
    pub schema_cols: &'a [String],
}

/// One stage's resolved read parameters, all in nanoseconds.
struct Read {
    f: RangeFn,
    window_ns: i64,
    step_ns: i64,
    /// The first and last read-window end (`t − offset`, or `at − offset`).
    first: i64,
    last: i64,
    offset_ns: i64,
    at: bool,
}

fn duration(field: &str, value: &str, min: i64) -> Result<i64, QuerierError> {
    parse_duration_ns(value)
        .filter(|ns| *ns >= min)
        .ok_or_else(|| QuerierError::InvalidInput(format!("invalid {field} duration '{value}'")))
}

/// Reject a range that evaluates more than [`MAX_INSTANTS`] instants.
pub(crate) fn check_instant_count(
    window: ResolvedWindow,
    step_ns: i64,
) -> Result<(), QuerierError> {
    let span = window.end_ns.saturating_sub(window.start_ns);
    if span >= 0 && span / step_ns >= MAX_INSTANTS {
        return Err(QuerierError::InvalidInput(format!(
            "the range evaluates more than {MAX_INSTANTS} instants at this step; \
             increase the step or narrow the range"
        )));
    }
    Ok(())
}

fn range_fn(sample: &Sample) -> RangeFn {
    match sample.func {
        SampleFn::Latest => RangeFn::Latest,
        SampleFn::Rate => RangeFn::Rate,
        SampleFn::Increase => RangeFn::Increase,
        SampleFn::Irate => RangeFn::Irate,
        SampleFn::Delta => RangeFn::Delta,
        SampleFn::Idelta => RangeFn::Idelta,
        SampleFn::Deriv => RangeFn::Deriv,
        SampleFn::Resets => RangeFn::Resets,
        SampleFn::Changes => RangeFn::Changes,
        SampleFn::AvgOverTime => RangeFn::AvgOverTime,
        SampleFn::MinOverTime => RangeFn::MinOverTime,
        SampleFn::MaxOverTime => RangeFn::MaxOverTime,
        SampleFn::SumOverTime => RangeFn::SumOverTime,
        SampleFn::CountOverTime => RangeFn::CountOverTime,
        SampleFn::LastOverTime => RangeFn::LastOverTime,
        SampleFn::StddevOverTime => RangeFn::StddevOverTime,
        SampleFn::StdvarOverTime => RangeFn::StdvarOverTime,
        SampleFn::PresentOverTime => RangeFn::PresentOverTime,
        SampleFn::QuantileOverTime => RangeFn::QuantileOverTime(sample.arg.unwrap_or(f64::NAN)),
    }
}

/// Whether a function's result is no longer the metric itself, so its
/// Series drops `metric.name` (as PromQL drops `__name__`).
fn drops_name(func: SampleFn) -> bool {
    !matches!(func, SampleFn::Latest | SampleFn::LastOverTime)
}

fn read(
    sample: &Sample,
    window: ResolvedWindow,
    doc_step: Option<&str>,
    now_ns: i64,
) -> Result<Read, QuerierError> {
    let (what, window_ns) = match (&sample.window, &sample.lookback) {
        (Some(w), _) => ("window", duration("sample.window", w, 1)?),
        (None, Some(l)) => ("lookback", duration("sample.lookback", l, 1)?),
        (None, None) => ("lookback", DEFAULT_LOOKBACK_NS),
    };
    let step = sample.step.as_deref().or(doc_step).ok_or_else(|| {
        QuerierError::InvalidInput("sample requires a `step`, on the stage or the document".into())
    })?;
    let step_ns = duration("sample.step", step, 1)?;
    check_instant_count(window, step_ns)?;
    let offset_ns = match &sample.offset {
        Some(o) => duration("sample.offset", o, 0)?,
        None => 0,
    };
    let at = match &sample.at {
        Some(at) => match coerce(at, &ValueType::TimestampNs) {
            Ok(Literal::Timestamp(ts)) => Some(ts.resolve(now_ns)),
            _ => {
                return Err(QuerierError::InvalidInput(format!(
                    "invalid sample.at: {at}"
                )));
            }
        },
        None => None,
    };
    if at.is_none() && (window_ns - 1) / step_ns + 1 > MAX_INSTANTS {
        return Err(QuerierError::InvalidInput(format!(
            "sample {what} spans more than {MAX_INSTANTS} steps; \
             increase the step or shrink the {what}"
        )));
    }
    let (first, last) = at.map_or((window.start_ns, window.end_ns), |at| (at, at));
    Ok(Read {
        f: range_fn(sample),
        window_ns,
        step_ns,
        first: first.saturating_sub(offset_ns),
        last: last.saturating_sub(offset_ns),
        offset_ns,
        at: at.is_some(),
    })
}

/// The scan window a document needs: its range, widened to the `sample`
/// stage's read windows (which reach back by `window + offset`, or sit at
/// `at`). Validation allows at most one `sample`, on the point stream.
pub(crate) fn scan_window(
    doc: &Document,
    window: ResolvedWindow,
    now_ns: i64,
) -> Result<ResolvedWindow, QuerierError> {
    let Some(sample) = doc.pipeline.iter().find_map(|stage| match stage {
        Stage::Sample(sample) => Some(sample),
        _ => None,
    }) else {
        return Ok(window);
    };
    let r = read(sample, window, doc.step.as_deref(), now_ns)?;
    Ok(ResolvedWindow {
        start_ns: window.start_ns.min(r.first.saturating_sub(r.window_ns)),
        end_ns: window.end_ns.max(r.last),
    })
}

fn ts_lit(ns: i64) -> Expr {
    lit(ScalarValue::TimestampNanosecond(Some(ns), None))
}

/// Lower `sample` over a metric point stream into the Series frame; also
/// returns the Series' step.
pub(crate) fn lower_sample(
    df: DataFrame,
    sample: &Sample,
    env: &SampleEnv<'_>,
) -> Result<(DataFrame, i64), QuerierError> {
    let r = read(sample, env.window, env.doc_step, env.now_ns)?;
    let has = |c: &str| env.schema_cols.iter().any(|s| s == c);
    let or_null = |c: &str| {
        if has(c) {
            ident(c)
        } else {
            lit(ScalarValue::Null)
        }
    };
    let bag = |c: &str| {
        bag_arg(
            c,
            has_typed_container(env.schema_cols.iter().map(String::as_str), c),
        )
    };
    if !has("series_id") {
        return Err(QuerierError::Unsupported(
            "sample needs the metrics table's `series_id` column".into(),
        ));
    }
    let (of, value) = match sample.of {
        SampleOf::Value => ("value", ident("value")),
        SampleOf::Count => ("count", cast(ident("count"), DataType::Float64)),
        SampleOf::Sum => ("sum", ident("sum")),
    };
    if !has(of) {
        return Err(QuerierError::InvalidInput(format!(
            "the metrics table has no `{of}` column to sample"
        )));
    }
    let ts = ident("timestamp");
    // `covering_instants` bounds each point's instants by the step; for `at`
    // there is one read instant, so any positive step will do.
    let step = if r.at { r.window_ns } else { r.step_ns };
    let instants = covering_instants_udf().call(vec![
        ts.clone(),
        lit(r.first),
        lit(r.last),
        lit(step),
        lit(r.window_ns),
    ]);
    let mut columns = vec![
        ident("series_id"),
        ts.clone(),
        value.alias("__value"),
        or_null("start_timestamp").alias("__start"),
        or_null("aggregation_temporality").alias("__temporality"),
        or_null("is_monotonic").alias("__monotonic"),
        or_null("metric_type").alias("__kind"),
        instants.alias(INSTANT),
    ];
    columns.extend(LABEL_INPUTS[..4].iter().map(|c| or_null(c).alias(*c)));
    columns.push(bag("resource_attributes").alias("__resource"));
    columns.push(bag("attributes").alias("__attrs"));
    let points = df
        .filter(
            ts.clone()
                .gt(ts_lit(r.first.saturating_sub(r.window_ns)))
                .and(ts.clone().lt_eq(ts_lit(r.last))),
        )?
        .select(columns)?
        .unnest_columns(&[INSTANT])?;
    let range = range_udaf(r.f, r.window_ns).call(vec![
        ts,
        col("__value"),
        col("__start"),
        col("__temporality"),
        col("__monotonic"),
        col("__kind"),
        col(INSTANT),
    ]);
    let mut aggs = vec![range.alias("value")];
    aggs.extend(
        LABEL_INPUTS
            .iter()
            .map(|c| first_value(ident(*c), vec![]).alias(*c)),
    );
    let evaluated = points
        .aggregate(vec![ident("series_id"), col(INSTANT)], aggs)?
        .filter(col("value").is_not_null())?;
    let mut labels = series_labels_udf().call(LABEL_INPUTS.iter().map(|c| ident(*c)).collect());
    if drops_name(sample.func) {
        labels = labels_drop_name_udf().call(vec![labels]);
    }
    // The evaluation instant `t`: the read-window end plus the offset, or,
    // under `at`, every instant of the range.
    let evaluated = evaluated.select(vec![
        col(INSTANT),
        labels.alias(LABELS_COLUMN),
        col("value"),
    ])?;
    let evaluated = if r.at {
        let all = gen_series(
            lit(env.window.start_ns),
            lit(env.window.end_ns),
            lit(r.step_ns),
        );
        evaluated
            .with_column(INSTANT, all)?
            .unnest_columns(&[INSTANT])?
    } else {
        evaluated.with_column(INSTANT, col(INSTANT) + lit(r.offset_ns))?
    };
    let bucket = cast(
        col(INSTANT),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
    );
    let frame = evaluated.select(vec![
        bucket.alias("bucket"),
        ident(LABELS_COLUMN),
        col("value"),
    ])?;
    Ok((frame, r.step_ns))
}
