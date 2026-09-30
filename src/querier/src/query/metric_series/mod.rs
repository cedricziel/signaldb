//! Metric Series planning (D11): the Series frame `(bucket, __labels, value)`
//! and the label-set UDFs its stages group, match and rewrite by.

use common::query_ir::Stage;
use datafusion::prelude::{DataFrame, SessionContext, ident};

use crate::query::error::QuerierError;
use crate::query::ir_planner::ResolvedWindow;

pub mod label_ops;
pub mod labels;
pub mod sample;
pub mod scalar;
mod stages;
mod value_fn;
pub(crate) mod vector_match;

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
        Stage::Labels(op) => stages::lower_labels(df, op),
        Stage::Map(map) => stages::lower_map(df, map),
        Stage::Filter(filter) => stages::lower_filter(df, filter),
        other => Err(QuerierError::Unsupported(format!(
            "{} stage is not supported yet",
            other.name()
        ))),
    }
}

/// Order a terminal metric frame by label set (when it has one), then
/// instant — once, after every Series stage has run.
pub(crate) fn sort_frame(df: DataFrame) -> Result<DataFrame, QuerierError> {
    let mut keys = Vec::new();
    if df
        .schema()
        .has_column_with_unqualified_name(labels::LABELS_COLUMN)
    {
        keys.push(ident(labels::LABELS_COLUMN).sort(true, true));
    }
    keys.push(ident("bucket").sort(true, true));
    Ok(df.sort(keys)?)
}
