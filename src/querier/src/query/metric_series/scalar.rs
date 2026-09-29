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
use super::sample::check_instant_count;
use crate::query::error::QuerierError;
use crate::query::ir_planner::ResolvedWindow;

fn bucket_type() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, None)
}

/// Every evaluation instant `t = start + k·step <= end`, as `bucket` and as
/// nanoseconds `__t`; at most [`MAX_INSTANTS`](super::sample::MAX_INSTANTS).
fn instants(
    ctx: &SessionContext,
    window: ResolvedWindow,
    step_ns: i64,
) -> Result<DataFrame, QuerierError> {
    check_instant_count(window, step_ns)?;
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

/// Plan a document reading the `time` or `constant` pseudo-source: a Scalar
/// over the document's instants, then its (Scalar/Series) stages.
pub(crate) fn plan_pseudo_source(
    ctx: &SessionContext,
    doc: &Document,
    window: ResolvedWindow,
) -> Result<DataFrame, QuerierError> {
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
    let mut df =
        instants(ctx, window, step_ns)?.select(vec![col("bucket"), value.alias("value")])?;
    let env = super::FrameEnv {
        ctx,
        window,
        step_ns: Some(step_ns),
    };
    for stage in &doc.pipeline {
        df = super::lower_stage(df, stage, &env)?;
    }
    super::sort_frame(df, super::terminal_order(doc, window))
}
