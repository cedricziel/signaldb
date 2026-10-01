//! Scalars (D11): the frame `(bucket, value)` with one value per evaluation
//! instant — the `scalar`/`vector` stages and the `time`/`constant`
//! pseudo-sources.

use common::query_ir::{Document, InMemoryResolver, SourceRegistry, validate};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::JoinType;
use datafusion::functions_aggregate::expr_fn::{count, max};
use datafusion::functions_nested::expr_fn::gen_series;
use datafusion::logical_expr::{Expr, cast, col, lit, when};
use datafusion::prelude::{DataFrame, SessionContext};

use super::labels::LABELS_COLUMN;
use crate::query::error::QuerierError;
use crate::query::ir_planner::ResolvedWindow;
use crate::query::metric_ops::instants::check_grid;

fn bucket_type() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, None)
}

/// Every evaluation instant `t = start + k·step <= end`, as `bucket` and as
/// nanoseconds `__t`; see [`check_grid`].
pub(super) fn instants(
    ctx: &SessionContext,
    window: ResolvedWindow,
    step_ns: i64,
) -> Result<DataFrame, QuerierError> {
    check_grid(window.start_ns, window.end_ns, step_ns)?;
    let all = gen_series(lit(window.start_ns), lit(window.end_ns), lit(step_ns));
    Ok(ctx
        .read_empty()?
        .select(vec![all.alias("__t")])?
        .unnest_columns(&["__t"])?
        .select(vec![
            cast(col("__t"), bucket_type()).alias("bucket"),
            col("__t"),
        ])?)
}

/// `scalar`: at every instant, the value of the Series' only series, or NaN
/// when it has none or several there.
pub(crate) fn to_scalar(
    ctx: &SessionContext,
    series: DataFrame,
    window: ResolvedWindow,
    step_ns: i64,
) -> Result<DataFrame, QuerierError> {
    let per_instant = series.aggregate(
        vec![col("bucket").alias("__b")],
        vec![count(lit(1)).alias("__n"), max(col("value")).alias("__v")],
    )?;
    let only = when(col("__n").eq(lit(1_i64)), col("__v")).otherwise(lit(f64::NAN))?;
    Ok(instants(ctx, window, step_ns)?
        .join(per_instant, JoinType::Left, &["bucket"], &["__b"], None)?
        .select(vec![col("bucket"), only.alias("value")])?)
}

/// `vector`: a Scalar as a Series of one series with no labels.
pub(crate) fn to_vector(scalar: DataFrame) -> Result<DataFrame, QuerierError> {
    Ok(scalar.select(vec![
        col("bucket"),
        lit("{}").alias(LABELS_COLUMN),
        col("value"),
    ])?)
}

/// The frame a document reading the `time` or `constant` pseudo-source
/// starts from: a Scalar at the document step (also returned) over the
/// instants its first stage evaluates at.
pub(crate) fn pseudo_source_frame(
    ctx: &SessionContext,
    doc: &Document,
    window: ResolvedWindow,
) -> Result<(DataFrame, i64), QuerierError> {
    validate(doc, &SourceRegistry::core(), &InMemoryResolver::new())
        .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
    let step_ns = doc
        .step
        .as_deref()
        .and_then(common::query_ir::parse_duration_ns)
        .filter(|ns| *ns > 0)
        .ok_or_else(|| {
            QuerierError::InvalidInput(format!(
                "the {} source requires a document `step`",
                doc.from
            ))
        })?;
    let value: Expr = match doc.constant {
        Some(c) => lit(c),
        None => cast(col("__t"), DataType::Float64) / lit(1e9),
    };
    let windows = super::stage_windows(&doc.pipeline, window, Some(step_ns), doc.step.as_deref());
    let first = windows.first().copied().unwrap_or(window);
    let frame = instants(ctx, first, step_ns)?.select(vec![col("bucket"), value.alias("value")])?;
    Ok((frame, step_ns))
}
