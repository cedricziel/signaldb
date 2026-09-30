//! The Series stages that rewrite a frame row by row (D11).

use common::query_ir::{Binop, BinopOp, CompareOp, Filter, Labels, Map};
use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::functions_aggregate::expr_fn::{count, first_value};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility, col,
    lit,
};
use datafusion::prelude::DataFrame;

use super::label_ops::{label_join_udf, label_replace_udf, labels_drop_name_udf};
use super::labels::{LABELS_COLUMN, utf8_array};
use super::value_fn::{ValueOp, value_expr};
use crate::query::error::QuerierError;
use crate::query::metric_ops::instants::invalid;

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
    let df = df
        .with_column("value", value_expr(op))?
        .filter(col("value").is_not_null())?;
    if drop_name && is_series(&df) {
        return self::drop_name(df);
    }
    Ok(df)
}

/// A Series without `metric.name`, checked as [`rewrite_labels`] checks.
pub(super) fn drop_name(df: DataFrame) -> Result<DataFrame, QuerierError> {
    rewrite_labels(df, labels_drop_name_udf().call(vec![col(LABELS_COLUMN)]))
}

/// A Series (`bucket`, `__labels`, `value`) relabelled by `labels`. Two
/// series left with one label set at one instant are an invalid-input
/// error, as in Prometheus ("vector cannot contain metrics with the same
/// labelset").
pub(super) fn rewrite_labels(df: DataFrame, labels: Expr) -> Result<DataFrame, QuerierError> {
    // A filter, not a projection: the optimizer prunes a projected column
    // no later stage reads, and the check with it.
    let check = ScalarUDF::new_from_impl(UniqueLabelset {
        signature: Signature::any(2, Volatility::Immutable),
    });
    Ok(df
        .select(vec![
            col("bucket"),
            labels.alias(LABELS_COLUMN),
            col("value"),
        ])?
        .aggregate(
            vec![col("bucket"), col(LABELS_COLUMN)],
            vec![
                count(lit(1)).alias("__n"),
                first_value(col("value"), vec![]).alias("value"),
            ],
        )?
        .filter(check.call(vec![col("__n"), col(LABELS_COLUMN)]))?
        .select_columns(&["bucket", LABELS_COLUMN, "value"])?)
}

/// `unique_labelset(n, labels)`: true, or an invalid-input error where `n`
/// rows share `labels` at one instant.
#[derive(Debug, PartialEq, Eq, Hash)]
struct UniqueLabelset {
    signature: Signature,
}

impl ScalarUDFImpl for UniqueLabelset {
    fn name(&self) -> &str {
        "unique_labelset"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = cast(&args.args[0].to_array(args.number_rows)?, &DataType::Int64)?;
        let n = n.as_primitive::<Int64Type>();
        if let Some(row) = (0..n.len()).find(|&i| n.is_valid(i) && n.value(i) > 1) {
            let labels = utf8_array(&args.args[1], args.number_rows)?;
            return Err(invalid(format!(
                "vector cannot contain metrics with the same labelset {}",
                labels.as_string::<i32>().value(row)
            )));
        }
        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
    }
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
    rewrite_labels(df, rewritten)
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
