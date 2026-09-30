//! Metric Series planning (D11): the Series frame `(bucket, __labels, value)`
//! and the label-set UDFs its stages group, match and rewrite by.

use std::sync::Arc;

use common::query_ir::{BinopOperand, Direction, Document, Stage, is_pseudo_source, safe_ident};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::functions::math::expr_fn::isnan;
use datafusion::logical_expr::{col, lit};
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
mod binop_tests;
#[cfg(test)]
mod stage_tests;
#[cfg(test)]
mod tests;

/// What a Series or Scalar stage reads beyond its input frame.
pub(crate) struct FrameEnv<'a> {
    pub ctx: &'a SessionContext,
    pub window: ResolvedWindow,
    /// The input's evaluation step; `None` for a frame not on the
    /// evaluation instants (a step aggregate's epoch-aligned buckets).
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
        Stage::Binop(binop) => match &binop.right {
            BinopOperand::Number(n) => stages::lower_number_binop(df, binop, *n),
            BinopOperand::Document(_) => Err(QuerierError::Unsupported(
                "binop with a sub-document operand is planned by the IR planner".to_string(),
            )),
        },
        // Only the terminal order changes (see `terminal_order`).
        Stage::Sort(_) => Ok(df),
        other => Err(QuerierError::Unsupported(format!(
            "{} stage is not supported yet",
            other.name()
        ))),
    }
}

/// The step a pipeline over `from` outputs at: a pseudo-source starts at
/// the document `step`, and each stage's [`output_step`] follows.
pub(crate) fn pipeline_step(from: &str, pipeline: &[Stage], doc_step: Option<&str>) -> Option<i64> {
    let start = is_pseudo_source(from)
        .then(|| doc_step.and_then(common::query_ir::parse_duration_ns))
        .flatten()
        .filter(|ns| *ns > 0);
    pipeline
        .iter()
        .fold(start, |step, stage| output_step(stage, step, doc_step))
}

/// Whether a `binop` sub-document yields a Scalar rather than a Series.
pub(crate) fn yields_scalar(from: &str, pipeline: &[Stage]) -> bool {
    pipeline
        .iter()
        .fold(is_pseudo_source(from), |scalar, stage| match stage {
            Stage::Scalar(_) => true,
            Stage::Binop(binop) => {
                scalar
                    && match &binop.right {
                        BinopOperand::Number(_) => true,
                        BinopOperand::Document(sub) => yields_scalar(&sub.from, &sub.pipeline),
                    }
            }
            Stage::Map(_) | Stage::Filter(_) => scalar,
            _ => false,
        })
}

/// A planned metric frame as a `binop` operand.
pub(crate) fn operand(df: DataFrame) -> vector_match::Operand {
    if df
        .schema()
        .has_column_with_unqualified_name(labels::LABELS_COLUMN)
    {
        vector_match::Operand::Series(df)
    } else {
        vector_match::Operand::Scalar(df)
    }
}

/// A `histogram_quantile` frame (`bucket`, a column per `by` label, and
/// `value`) as a Series labelled by the `by` labels. Several metrics
/// matching one `by` label set are two series with one label set: a 400,
/// as in Prometheus.
pub(crate) fn histogram_as_series(
    df: DataFrame,
    by: &[String],
    value: &str,
) -> Result<DataFrame, QuerierError> {
    let labels = if by.is_empty() {
        lit("{}")
    } else {
        let columns = by.iter().map(|b| ident(safe_ident(b))).collect();
        labels::labels_of_udf(by.to_vec()).call(columns)
    };
    let df = df
        .with_column("value", ident(value))?
        .filter(col("value").is_not_null())?;
    stages::rewrite_labels(df, labels)
}

/// Whether [`lower_stage`] (or the planner's `binop`) lowers `stage`.
pub(crate) fn is_frame_stage(stage: &Stage) -> bool {
    matches!(
        stage,
        Stage::Scalar(_)
            | Stage::Vector(_)
            | Stage::Reduce(_)
            | Stage::Labels(_)
            | Stage::Map(_)
            | Stage::Filter(_)
            | Stage::Sort(_)
            | Stage::Absent(_)
            | Stage::OverTime(_)
            | Stage::Binop(_)
    )
}

/// A Series with no series: the operand read from a dataset without the
/// source's table.
pub(crate) fn empty_series(ctx: &SessionContext) -> Result<vector_match::Operand, QuerierError> {
    let schema = Schema::new(vec![
        Field::new(
            "bucket",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(labels::LABELS_COLUMN, DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]);
    let batch = RecordBatch::new_empty(Arc::new(schema));
    Ok(vector_match::Operand::Series(ctx.read_batch(batch)?))
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
