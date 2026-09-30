//! The `reduce` stage (D11): PromQL's aggregation operators over a Series,
//! folding its series into groups at every instant.

use std::sync::Arc;

use common::query_ir::{Reduce, ReduceFn};
use datafusion::arrow::array::{Array, AsArray, Float64Builder, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Float64Type};
use datafusion::error::{DataFusionError, Result};
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::functions::math::expr_fn::isnan;
use datafusion::functions_aggregate::expr_fn::{
    array_agg, avg, count, max, min, stddev_pop, sum, var_pop,
};
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprFunctionExt, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility, col, lit, when,
};
use datafusion::prelude::DataFrame;
use datafusion::scalar::ScalarValue;

use super::label_ops::{labels_drop_udf, labels_keep_udf};
use super::labels::{LABELS_COLUMN, METRIC_NAME, decode, encode, utf8_array};
use crate::query::error::QuerierError;
use crate::query::metric_ops::range_math::quantile;

const GROUP: &str = "__group";

/// The label set of each series' group: exactly the `by` labels, or every
/// label but the `without` ones and `metric.name`, or none.
fn group_labels(reduce: &Reduce) -> Expr {
    let names = |names: &[String]| names.iter().map(|n| lit(n.as_str())).collect::<Vec<_>>();
    match (&reduce.by, &reduce.without) {
        (Some(by), _) => {
            let mut args = vec![col(LABELS_COLUMN)];
            args.extend(names(by));
            labels_keep_udf().call(args)
        }
        (None, Some(without)) => {
            let mut args = vec![col(LABELS_COLUMN), lit(METRIC_NAME)];
            args.extend(names(without));
            labels_drop_udf().call(args)
        }
        (None, None) => lit("{}"),
    }
}

pub(super) fn lower_reduce(df: DataFrame, reduce: &Reduce) -> Result<DataFrame, QuerierError> {
    let value = || col("value");
    let group = group_labels(reduce);
    let aggregate = match reduce.func {
        ReduceFn::Topk | ReduceFn::Bottomk => return top(df, reduce, group),
        ReduceFn::CountValues => {
            let label = reduce.label.clone().unwrap_or_default();
            let labels = ScalarUDF::new_from_impl(CountValuesLabels {
                label,
                signature: Signature::any(2, Volatility::Immutable),
            })
            .call(vec![group, value()]);
            let counted = df
                .select(vec![col("bucket"), labels.alias(LABELS_COLUMN)])?
                .aggregate(
                    vec![col("bucket"), col(LABELS_COLUMN)],
                    vec![count(lit(1)).alias("value")],
                )?;
            return Ok(counted.select(vec![
                col("bucket"),
                col(LABELS_COLUMN),
                cast_f64(col("value")).alias("value"),
            ])?);
        }
        ReduceFn::Sum => sum(value()),
        ReduceFn::Avg => avg(value()),
        // NaN only when every value is NaN, as in Prometheus.
        ReduceFn::Min => min(unless_nan(value())?),
        ReduceFn::Max => max(unless_nan(value())?),
        ReduceFn::Count => cast_f64(count(value())),
        ReduceFn::Group => max(lit(1.0)),
        ReduceFn::Stddev => stddev_pop(value()),
        ReduceFn::Stdvar => var_pop(value()),
        ReduceFn::Quantile => ScalarUDF::new_from_impl(Quantile {
            q: reduce.arg.unwrap_or(f64::NAN).to_bits(),
            signature: Signature::any(1, Volatility::Immutable),
        })
        .call(vec![array_agg(value())]),
    };
    let grouped = df.aggregate(
        vec![col("bucket"), group.alias(GROUP)],
        vec![aggregate.alias("value")],
    )?;
    Ok(grouped.select(vec![
        col("bucket"),
        col(GROUP).alias(LABELS_COLUMN),
        coalesce(vec![col("value"), lit(f64::NAN)]).alias("value"),
    ])?)
}

fn cast_f64(e: Expr) -> Expr {
    datafusion::logical_expr::cast(e, DataType::Float64)
}

fn unless_nan(e: Expr) -> Result<Expr> {
    when(isnan(e.clone()), lit(ScalarValue::Float64(None))).otherwise(e)
}

/// `topk`/`bottomk`: the k largest (smallest) series of each group at every
/// instant, with their own labels; NaN ranks last, ties by label set.
fn top(df: DataFrame, reduce: &Reduce, group: Expr) -> Result<DataFrame, QuerierError> {
    let k = reduce.arg.unwrap_or(1.0) as u64;
    let descending = reduce.func == ReduceFn::Topk;
    let rank = row_number()
        .partition_by(vec![col("bucket"), group])
        .order_by(vec![
            isnan(col("value")).sort(true, true),
            col("value").sort(!descending, true),
            col(LABELS_COLUMN).sort(true, true),
        ])
        .build()?;
    Ok(df
        .with_column("__rank", rank)?
        .filter(col("__rank").lt_eq(lit(k)))?
        .select(vec![col("bucket"), col(LABELS_COLUMN), col("value")])?)
}

/// A float as Prometheus prints it in a label (`strconv.FormatFloat(v, 'f',
/// -1, 64)`).
fn format_value(v: f64) -> String {
    match v {
        f64::INFINITY => "+Inf".to_string(),
        f64::NEG_INFINITY => "-Inf".to_string(),
        v => format!("{v}"),
    }
}

/// `count_values_labels(group, value)`: the group label set with `label`
/// set to the value.
#[derive(Debug, PartialEq, Eq, Hash)]
struct CountValuesLabels {
    label: String,
    signature: Signature,
}

impl ScalarUDFImpl for CountValuesLabels {
    fn name(&self) -> &str {
        "count_values_labels"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let groups = utf8_array(&args.args[0], rows)?;
        let groups = groups.as_string::<i32>();
        let values = cast(&args.args[1].to_array(rows)?, &DataType::Float64)?;
        let values = values.as_primitive::<Float64Type>();
        let mut out = StringBuilder::new();
        for row in 0..rows {
            if groups.is_null(row) || values.is_null(row) {
                out.append_null();
                continue;
            }
            let mut set = decode(groups.value(row))?;
            set.insert(self.label.clone(), format_value(values.value(row)));
            out.append_value(encode(&set)?);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

/// `quantile(values)`: Prometheus' φ-quantile of each list, interpolated
/// linearly between the closest ranks.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Quantile {
    q: u64,
    signature: Signature,
}

impl ScalarUDFImpl for Quantile {
    fn name(&self) -> &str {
        "series_quantile"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let lists = args.args[0].to_array(args.number_rows)?;
        let lists = lists
            .as_list_opt::<i32>()
            .ok_or_else(|| DataFusionError::Internal("series_quantile expects a list".into()))?;
        let mut out = Float64Builder::with_capacity(lists.len());
        for row in 0..lists.len() {
            let values = lists.value(row);
            let values = values.as_primitive::<Float64Type>();
            let values: Vec<f64> = values.iter().flatten().collect();
            out.append_option(
                (!values.is_empty()).then(|| quantile(f64::from_bits(self.q), values)),
            );
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::format_value;

    #[test]
    fn values_print_as_prometheus_labels() {
        let got: Vec<_> = [
            1.0,
            0.5,
            -0.0,
            1e21,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ]
        .into_iter()
        .map(format_value)
        .collect();
        assert_eq!(
            got,
            [
                "1",
                "0.5",
                "-0",
                "1000000000000000000000",
                "NaN",
                "+Inf",
                "-Inf"
            ]
        );
    }
}
