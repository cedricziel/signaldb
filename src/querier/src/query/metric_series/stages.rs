//! The Series stages that rewrite a frame row by row (D11).

use common::query_ir::{Binop, BinopOp, CompareOp, Filter, Labels, Map};
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::DataFrame;

use super::label_ops::{label_join_udf, label_replace_udf, labels_drop_name_udf};
use super::labels::LABELS_COLUMN;
use super::value_fn::{ValueOp, value_expr};
use crate::query::error::QuerierError;

fn is_series(df: &DataFrame) -> bool {
    df.schema().has_column_with_unqualified_name(LABELS_COLUMN)
}

/// Replace every value by `op` of it, dropping the rows it drops, and drop
/// the metric name of a Series when `drop_name`.
pub(super) fn with_values(
    df: DataFrame,
    op: ValueOp,
    drop_name: bool,
) -> Result<DataFrame, QuerierError> {
    let mut df = df
        .with_column("value", value_expr(op))?
        .filter(col("value").is_not_null())?;
    if drop_name && is_series(&df) {
        let labels = labels_drop_name_udf().call(vec![col(LABELS_COLUMN)]);
        df = df.with_column(LABELS_COLUMN, labels)?;
    }
    Ok(df)
}

/// `map`: a function of every value; a Series loses its metric name.
pub(super) fn lower_map(df: DataFrame, map: &Map) -> Result<DataFrame, QuerierError> {
    with_values(df, ValueOp::map(map.func, &map.args), true)
}

/// `filter`: keep the values that compare true, or with `bool` yield 0/1
/// (and drop the metric name).
pub(super) fn lower_filter(df: DataFrame, filter: &Filter) -> Result<DataFrame, QuerierError> {
    let op = match filter.op {
        CompareOp::Eq => BinopOp::Eq,
        CompareOp::Ne => BinopOp::Ne,
        CompareOp::Gt => BinopOp::Gt,
        CompareOp::Ge => BinopOp::Ge,
        CompareOp::Lt => BinopOp::Lt,
        CompareOp::Le => BinopOp::Le,
    };
    let op = ValueOp::number(op, filter.value, false, filter.bool);
    with_values(df, op, filter.bool)
}

/// `labels`: PromQL's `label_replace` / `label_join` on every series.
pub(super) fn lower_labels(df: DataFrame, op: &Labels) -> Result<DataFrame, QuerierError> {
    let labels = col(LABELS_COLUMN);
    let rewritten = match op {
        Labels::Replace(r) => label_replace_udf().call(vec![
            labels,
            lit(r.dst.as_str()),
            lit(r.replacement.as_str()),
            lit(r.src.as_str()),
            lit(r.regex.as_str()),
        ]),
        Labels::Join(j) => {
            let mut args = vec![labels, lit(j.dst.as_str()), lit(j.separator.as_str())];
            args.extend(j.src.iter().map(|s| lit(s.as_str())));
            label_join_udf().call(args)
        }
    };
    Ok(df.with_column(LABELS_COLUMN, rewritten)?)
}

/// `binop` with a number: arithmetic drops a Series' metric name, and so
/// does a `bool` comparison; a filtering comparison keeps the frame's value.
pub(super) fn lower_number_binop(
    df: DataFrame,
    binop: &Binop,
    number: f64,
) -> Result<DataFrame, QuerierError> {
    if binop.op.is_set() {
        return Err(QuerierError::InvalidInput(
            "binop `and`/`or`/`unless` need a series on both sides".to_string(),
        ));
    }
    let drop_name = !binop.op.is_comparison() || binop.bool;
    let op = ValueOp::number(binop.op, number, binop.reverse, binop.bool);
    with_values(df, op, drop_name)
}
