//! The stages that read a Series across instants (D11): `absent`, and
//! `over_time`, a subquery re-windowing a Series evaluated at its own step.

use common::query_ir::{Absent, OverTime, OverTimeFn, Stage, parse_duration_ns};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::JoinType;
use datafusion::logical_expr::{cast, col, lit};
use datafusion::prelude::DataFrame;
use datafusion::scalar::ScalarValue;

use super::FrameEnv;
use super::labels::{LABELS_COLUMN, encode};
use super::scalar::instants;
use super::stages::drop_name;
use crate::query::error::QuerierError;
use crate::query::ir_planner::ResolvedWindow;
use crate::query::metric_ops::instants::covering_instants_udf;
use crate::query::metric_ops::range::range_udaf;
use crate::query::metric_ops::range_math::RangeFn;

const INSTANT: &str = "__instant";

/// The step of the frame `stage` outputs, given its input's: `sample` and
/// `over_time` evaluate at their own `step`, else the document's.
pub(crate) fn output_step(
    stage: &Stage,
    input: Option<i64>,
    doc_step: Option<&str>,
) -> Option<i64> {
    let own = |step: &Option<String>| {
        step.as_deref()
            .or(doc_step)
            .and_then(parse_duration_ns)
            .filter(|ns| *ns > 0)
    };
    match stage {
        Stage::Sample(sample) => own(&sample.step),
        Stage::OverTime(over) => own(&over.step),
        _ => input,
    }
}

/// The window each stage evaluates over. An `over_time` reads its input in
/// `(t − window, t]` at every instant `t` of its own window, so the stages
/// before it evaluate from the first multiple of their step after
/// `start − window` on: as in Prometheus, a subquery's instants are the
/// multiples of its resolution since the epoch, whatever the query start.
pub(crate) fn stage_windows(
    pipeline: &[Stage],
    window: ResolvedWindow,
    input_step: Option<i64>,
    doc_step: Option<&str>,
) -> Vec<ResolvedWindow> {
    let mut steps = Vec::with_capacity(pipeline.len());
    let mut step = input_step;
    for stage in pipeline {
        steps.push(step);
        step = output_step(stage, step, doc_step);
    }
    let mut windows = vec![window; pipeline.len()];
    let mut current = window;
    for (i, stage) in pipeline.iter().enumerate().rev() {
        windows[i] = current;
        if let (Stage::OverTime(over), Some(step)) = (stage, steps[i])
            && let Some(w) = parse_duration_ns(&over.window).filter(|w| *w > 0)
        {
            let from = current.start_ns.saturating_sub(w);
            current.start_ns = from.div_euclid(step).saturating_add(1).saturating_mul(step);
        }
    }
    windows
}

/// `absent`: at every instant where the input has no series, one series
/// labelled `labels` with value 1.
pub(super) fn lower_absent(
    df: DataFrame,
    absent: &Absent,
    env: &FrameEnv<'_>,
    step_ns: i64,
) -> Result<DataFrame, QuerierError> {
    let present = df
        .select(vec![col("bucket").alias("__present")])?
        .distinct()?;
    let labels = encode(&absent.labels)?;
    Ok(instants(env.ctx, env.window, step_ns)?
        .join(
            present,
            JoinType::LeftAnti,
            &["bucket"],
            &["__present"],
            None,
        )?
        .select(vec![
            col("bucket"),
            lit(labels).alias(LABELS_COLUMN),
            lit(1.0).alias("value"),
        ])?)
}

fn range_fn(over: &OverTime) -> RangeFn {
    match over.func {
        OverTimeFn::Avg => RangeFn::AvgOverTime,
        OverTimeFn::Min => RangeFn::MinOverTime,
        OverTimeFn::Max => RangeFn::MaxOverTime,
        OverTimeFn::Sum => RangeFn::SumOverTime,
        OverTimeFn::Count => RangeFn::CountOverTime,
        OverTimeFn::Last => RangeFn::LastOverTime,
        OverTimeFn::Stddev => RangeFn::StddevOverTime,
        OverTimeFn::Stdvar => RangeFn::StdvarOverTime,
        OverTimeFn::Present => RangeFn::PresentOverTime,
        OverTimeFn::Quantile => RangeFn::QuantileOverTime(over.arg.unwrap_or(f64::NAN)),
        OverTimeFn::Delta => RangeFn::Delta,
        OverTimeFn::Deriv => RangeFn::Deriv,
        OverTimeFn::Changes => RangeFn::Changes,
        OverTimeFn::Resets => RangeFn::Resets,
    }
}

/// `over_time`: at every instant `t` of the window (every `step_ns`), the
/// function of each series' values in `(t − window, t]`, read as a gauge;
/// `metric.name` survives `last` only.
pub(super) fn lower_over_time(
    df: DataFrame,
    over: &OverTime,
    window: ResolvedWindow,
    step_ns: i64,
) -> Result<DataFrame, QuerierError> {
    let window_ns = parse_duration_ns(&over.window)
        .filter(|ns| *ns > 0)
        .ok_or_else(|| {
            QuerierError::InvalidInput(format!("invalid over_time.window '{}'", over.window))
        })?;
    let covering = covering_instants_udf().call(vec![
        col("bucket"),
        lit(window.start_ns),
        lit(window.end_ns),
        lit(step_ns),
        lit(window_ns),
    ]);
    let points = df
        .select(vec![
            col(LABELS_COLUMN),
            col("bucket"),
            col("value"),
            covering.alias(INSTANT),
        ])?
        .unnest_columns(&[INSTANT])?;
    let range = range_udaf(range_fn(over), window_ns).call(vec![
        col("bucket"),
        col("value"),
        lit(ScalarValue::Int64(None)),
        lit(ScalarValue::Int32(None)),
        lit(ScalarValue::Boolean(None)),
        lit("gauge"),
        col(INSTANT),
    ]);
    let bucket = cast(
        col(INSTANT),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
    );
    let out = points
        .aggregate(
            vec![col(LABELS_COLUMN), col(INSTANT)],
            vec![range.alias("value")],
        )?
        .filter(col("value").is_not_null())?
        .select(vec![
            bucket.alias("bucket"),
            col(LABELS_COLUMN),
            col("value"),
        ])?;
    if over.func == OverTimeFn::Last {
        return Ok(out);
    }
    drop_name(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_subqueries_reach_back_on_their_input_grids() {
        let pipeline: Vec<Stage> = serde_json::from_value(serde_json::json!([
            { "sample": { "fn": "latest", "step": "1m" } },
            { "over_time": { "fn": "avg", "window": "5m", "step": "2m" } },
            { "over_time": { "fn": "max", "window": "1h" } },
            { "map": { "fn": "abs" } }
        ]))
        .unwrap();
        let m = 60_000_000_000;
        let range = ResolvedWindow {
            start_ns: 100 * m,
            end_ns: 200 * m,
        };
        let starts: Vec<i64> = stage_windows(&pipeline, range, None, Some("10m"))
            .iter()
            .map(|w| w.start_ns / m)
            .collect();
        // max reads 1h of its 2m input (58m back); avg 5m of its 1m input.
        assert_eq!(starts, [38, 42, 100, 100]);
    }

    #[test]
    fn subquery_instants_are_multiples_of_the_resolution_since_the_epoch() {
        let pipeline: Vec<Stage> = serde_json::from_value(serde_json::json!([
            { "sample": { "fn": "latest", "step": "1m" } },
            { "over_time": { "fn": "max", "window": "5m" } }
        ]))
        .unwrap();
        let s = 1_000_000_000;
        let range = ResolvedWindow {
            start_ns: 1000 * s,
            end_ns: 1300 * s,
        };
        let windows = stage_windows(&pipeline, range, None, Some("90s"));
        // (700s, 1000s] holds the 1m multiples 720s, 780s, …, 960s.
        assert_eq!(windows[0].start_ns, 720 * s);
        assert_eq!(windows[1], range);
    }
}
