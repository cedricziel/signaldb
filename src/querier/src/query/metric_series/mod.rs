//! Metric Series planning (D11): the Series frame `(bucket, __labels, value)`
//! and the label-set UDFs its stages group, match and rewrite by.

use datafusion::prelude::{DataFrame, ident};

use crate::query::error::QuerierError;

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "label rewrites are not wired into a stage yet")
)]
pub mod label_ops;
pub mod labels;
pub mod sample;
pub(crate) mod vector_match;

#[cfg(test)]
mod tests;

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
