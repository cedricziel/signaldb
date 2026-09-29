//! Metric Series planning (D11): the Series frame `(bucket, __labels, value)`
//! and the label-set UDFs its stages group, match and rewrite by.

use common::query_ir::{Direction, Document, Stage};
use datafusion::functions::math::expr_fn::isnan;
use datafusion::prelude::{DataFrame, SessionContext, ident};

use crate::query::error::QuerierError;
use crate::query::ir_planner::ResolvedWindow;

pub mod label_ops;
pub mod labels;
mod reduce;
pub mod sample;
pub mod scalar;
mod stages;
mod value_fn;
pub(crate) mod vector_match;
mod window;

pub(crate) use window::{output_step, stage_windows};

#[cfg(test)]
mod stage_tests;
#[cfg(test)]
mod tests;

/// What a Series or Scalar stage reads beyond its input frame.
pub(crate) struct FrameEnv<'a> {
    pub ctx: &'a SessionContext,
    pub window: ResolvedWindow,
    /// The input's evaluation step; `None` for a Series from anything but
    /// `sample` (a step aggregate, a histogram quantile), which sits on
    /// epoch-aligned buckets rather than on the evaluation instants.
    pub step_ns: Option<i64>,
    /// The document's `step`, which an `over_time` without its own
    /// evaluates at.
    pub doc_step: Option<&'a str>,
}

/// Lower one Series/Scalar stage that needs nothing but its input frame.
pub(crate) fn lower_stage(
    df: DataFrame,
    stage: &Stage,
    env: &FrameEnv<'_>,
) -> Result<DataFrame, QuerierError> {
    let Some(step_ns) = env.step_ns else {
        return Err(QuerierError::Unsupported(format!(
            "{} over a non-sampled Series",
            stage.name()
        )));
    };
    match stage {
        Stage::Scalar(_) => scalar::to_scalar(env.ctx, df, env.window, step_ns),
        Stage::Vector(_) => scalar::to_vector(df),
        Stage::Reduce(r) => reduce::lower_reduce(df, r),
        Stage::Labels(op) => stages::lower_labels(df, op),
        Stage::Map(map) => stages::lower_map(df, map),
        Stage::Filter(filter) => stages::lower_filter(df, filter),
        Stage::Absent(absent) => window::lower_absent(df, absent, env, step_ns),
        Stage::OverTime(over) => {
            let out_step = output_step(stage, None, env.doc_step).ok_or_else(|| {
                QuerierError::InvalidInput("over_time requires a `step`".to_string())
            })?;
            window::lower_over_time(df, over, env.window, out_step)
        }
        // Only the terminal order changes (see `terminal_order`).
        Stage::Sort(_) => Ok(df),
        other => Err(QuerierError::Unsupported(format!(
            "{} stage is not supported yet",
            other.name()
        ))),
    }
}

/// The value order a terminal `sort` asks for. As in Prometheus it orders
/// the series of an instant query (a range that is one instant); over a range
/// the series stay in label-set order.
pub(crate) fn terminal_order(doc: &Document, window: ResolvedWindow) -> Option<Direction> {
    match doc.pipeline.last() {
        Some(Stage::Sort(direction)) if window.start_ns == window.end_ns => Some(*direction),
        _ => None,
    }
}

/// Order a terminal metric frame — by value first when `by_value` (NaN
/// last either way, as in Prometheus), then by label set (when it has one),
/// then instant — once, after every Series stage has run.
pub(crate) fn sort_frame(
    df: DataFrame,
    by_value: Option<Direction>,
) -> Result<DataFrame, QuerierError> {
    let mut keys = Vec::new();
    if let Some(direction) = by_value {
        keys.push(isnan(ident("value")).sort(true, true));
        keys.push(ident("value").sort(direction == Direction::Asc, true));
    }
    if df
        .schema()
        .has_column_with_unqualified_name(labels::LABELS_COLUMN)
    {
        keys.push(ident(labels::LABELS_COLUMN).sort(true, true));
    }
    keys.push(ident("bucket").sort(true, true));
    Ok(df.sort(keys)?)
}
