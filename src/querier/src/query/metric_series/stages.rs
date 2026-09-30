//! The Series stages that rewrite a frame row by row (D11).

use common::query_ir::Labels;
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::DataFrame;

use super::label_ops::{label_join_udf, label_replace_udf};
use super::labels::LABELS_COLUMN;
use crate::query::error::QuerierError;

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
