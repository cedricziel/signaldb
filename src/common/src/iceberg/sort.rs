//! Sorting rows by a table's declared sort order.
//!
//! A table declares one sort order (see [`super::schemas::TableSchema::sort_order_for`]),
//! and every producer that wants its files attributed that order has to
//! actually put the rows in it first. This module is the shared bridge from
//! the declaration to the sort:
//!
//! - [`declared_sort_columns`] resolves a table's declared order into column
//!   names and directions — what a query engine needs to plan the sort;
//! - [`write_sort_key`] turns that into a producer's decision: which key to
//!   sort by, and whether the files may attest it;
//! - [`sort_batch_by`] performs the sort columnar-style for producers holding
//!   Arrow batches in memory;
//! - [`is_sorted_by`] checks the result, so a producer can verify its own
//!   honesty rather than assert it.
//!
//! The invariant these serve: a file may only be attributed the declared
//! order if its rows really are in that order. A false attribution does not
//! make a query slower, it makes it wrong — the engine drops a sort it
//! believed was redundant.

use anyhow::{Context, Result};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::{SortColumn, SortOptions, lexsort_to_indices, take_record_batch};
use iceberg_rust::spec::partition::Transform;
use iceberg_rust::spec::sort::{NullOrder, SortDirection};
use iceberg_rust::spec::table_metadata::TableMetadata;

/// One column of a table's declared sort order, resolved to a name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredSortColumn {
    /// Column name in the table's current schema.
    pub name: String,
    /// Whether rows are ordered from high to low on this column.
    pub descending: bool,
    /// Whether nulls sort before non-null values.
    pub nulls_first: bool,
}

impl DeclaredSortColumn {
    /// The Arrow sort options this column's direction implies.
    pub fn sort_options(&self) -> SortOptions {
        SortOptions {
            descending: self.descending,
            nulls_first: self.nulls_first,
        }
    }
}

/// The table's declared sort order as named columns, or an empty vector when
/// the table declares no order.
///
/// Only the leading run of identity-transformed fields is returned. A sort on
/// a transformed value (`bucket(id)`, say) orders the transform's output, not
/// the column's, so naming the column would describe a different order than
/// the one the rows are in — and everything downstream of this function
/// treats the returned columns as literal column orderings.
///
/// # Errors
///
/// Returns an error if the metadata has no readable current schema or default
/// sort order, since then nothing about the table's ordering can be stated.
pub fn declared_sort_columns(metadata: &TableMetadata) -> Result<Vec<DeclaredSortColumn>> {
    let schema = metadata
        .current_schema()
        .map_err(|e| anyhow::anyhow!("Failed to resolve the current schema: {e}"))?;
    let order = metadata
        .default_sort_order()
        .map_err(|e| anyhow::anyhow!("Failed to resolve the default sort order: {e}"))?;

    Ok(order
        .fields
        .iter()
        .take_while(|field| field.transform == Transform::Identity)
        .map_while(|field| {
            schema
                .fields()
                .iter()
                .find(|schema_field| schema_field.id == field.source_id)
                .map(|schema_field| DeclaredSortColumn {
                    name: schema_field.name.clone(),
                    descending: field.direction == SortDirection::Descending,
                    nulls_first: field.null_order == NullOrder::First,
                })
        })
        .collect())
}

/// The canonical sort key for a signal table by name, for a producer that
/// wants to sort rows a table has not (yet) declared an order for.
///
/// Same key [`super::schemas::TableSchema::sort_order_for`] declares, resolved
/// by name instead of by field id, so it needs no schema. Returns an empty
/// vector for a name that is not a signal table.
///
/// A producer using this key is producing sorted files, but the *table* has
/// not declared the order, so those files must still be written unattested:
/// attestation is a claim about the table's declared order, and there isn't
/// one to claim.
pub fn canonical_sort_columns(table_name: &str) -> Vec<DeclaredSortColumn> {
    super::schemas::TableSchema::from_table_name(table_name)
        .map(|table| {
            table
                .sort_key_columns()
                .iter()
                .map(|name| DeclaredSortColumn {
                    name: (*name).to_string(),
                    descending: false,
                    nulls_first: true,
                })
                .collect()
        })
        .unwrap_or_default()
}

/// What a producer should do with a table that declares no sort order.
///
/// Attestation is a claim about the table's *declared* order, so neither
/// choice ever attests — the difference is only whether the rows are laid out
/// in key order anyway.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UndeclaredFallback {
    /// Write rows in whatever order they arrived. Ingest's choice: sorting
    /// rows nothing may attest costs the write path and buys the reader
    /// nothing.
    LeaveUnsorted,
    /// Sort by the canonical key regardless. Compaction's choice: it has
    /// always laid partitions out sorted, and abandoning that because a
    /// table's declaration has not been backfilled yet would regress the
    /// physical layout.
    CanonicalKey,
}

/// The key a producer sorts its rows by, and whether the files it writes may
/// attest that key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteSortKey {
    /// Columns to sort by, in key order; empty means "do not sort".
    pub columns: Vec<DeclaredSortColumn>,
    /// Whether the written files may claim the table's declared sort order.
    pub attest: bool,
}

/// Resolve the sort key a producer writing into `metadata`'s table should use.
///
/// One decision function for every producer, so ingest and compaction cannot
/// drift on what "the declared order" means or on when a file may claim it.
/// `attest` is true only when the table actually declares an order — a
/// producer sorting by the canonical fallback is producing sorted files that
/// say nothing about themselves, which readers treat as unsorted.
pub fn write_sort_key(
    metadata: &TableMetadata,
    table_name: &str,
    fallback: UndeclaredFallback,
) -> WriteSortKey {
    match declared_sort_columns(metadata) {
        Ok(columns) if !columns.is_empty() => {
            return WriteSortKey {
                columns,
                attest: true,
            };
        }
        Ok(_) => {}
        Err(error) => tracing::warn!(
            %error,
            table = %table_name,
            "Cannot read the declared sort order; writing files unattested"
        ),
    }

    let columns = match fallback {
        UndeclaredFallback::LeaveUnsorted => Vec::new(),
        UndeclaredFallback::CanonicalKey => canonical_sort_columns(table_name),
    };
    WriteSortKey {
        columns,
        attest: false,
    }
}

/// Bind `columns` to `batch`'s arrays, ready for a sort kernel.
///
/// # Errors
///
/// Returns an error if a key column is absent from the batch — the caller
/// asked about an order the data cannot be in, and silently working with a
/// prefix would answer about a different key than the one requested.
fn resolve_sort_columns(
    batch: &RecordBatch,
    columns: &[DeclaredSortColumn],
) -> Result<Vec<SortColumn>> {
    columns
        .iter()
        .map(|column| {
            let values = batch
                .column_by_name(&column.name)
                .with_context(|| {
                    format!("Sort key column '{}' is absent from the batch", column.name)
                })?
                .clone();
            Ok(SortColumn {
                values,
                options: Some(column.sort_options()),
            })
        })
        .collect()
}

/// Sort `batch` by `columns`, in that key order.
///
/// A columnar sort: one `lexsort_to_indices` pass over the key columns
/// followed by a `take` over every column, rather than any row-wise
/// comparison.
///
/// # Errors
///
/// Returns an error if a key column is absent from the batch — the caller
/// asked for an order the data cannot be put in, and silently sorting by a
/// prefix would produce a batch that does not honor the declared key.
pub fn sort_batch_by(batch: &RecordBatch, columns: &[DeclaredSortColumn]) -> Result<RecordBatch> {
    if columns.is_empty() || batch.num_rows() < 2 {
        return Ok(batch.clone());
    }

    let sort_columns = resolve_sort_columns(batch, columns)?;
    let indices = lexsort_to_indices(&sort_columns, None)
        .context("Failed to sort rows by the declared key")?;
    take_record_batch(batch, &indices).context("Failed to apply the sort permutation")
}

/// Whether `batch`'s rows are in `columns`' order.
///
/// Implemented by re-deriving the sort permutation and checking it is the
/// identity, so it uses exactly the comparison rules the sort itself does
/// rather than a second hand-rolled ordering that could disagree with them.
/// Intended for tests and debug assertions: it costs a sort.
pub fn is_sorted_by(batch: &RecordBatch, columns: &[DeclaredSortColumn]) -> Result<bool> {
    if columns.is_empty() || batch.num_rows() < 2 {
        return Ok(true);
    }

    let sort_columns = resolve_sort_columns(batch, columns)?;
    let indices =
        lexsort_to_indices(&sort_columns, None).context("Failed to derive the sort permutation")?;
    Ok(indices
        .values()
        .iter()
        .enumerate()
        .all(|(position, index)| *index as usize == position))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use std::sync::Arc;

    fn batch(timestamps: &[i64], names: &[&str]) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("service_name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(timestamps.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    fn key() -> Vec<DeclaredSortColumn> {
        vec![
            DeclaredSortColumn {
                name: "timestamp".to_string(),
                descending: false,
                nulls_first: true,
            },
            DeclaredSortColumn {
                name: "service_name".to_string(),
                descending: false,
                nulls_first: true,
            },
        ]
    }

    fn timestamps_of(batch: &RecordBatch) -> Vec<i64> {
        batch
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn services_of(batch: &RecordBatch) -> Vec<String> {
        let column = batch.column_by_name("service_name").unwrap();
        let strings = column.as_any().downcast_ref::<StringArray>().unwrap();
        (0..strings.len())
            .map(|i| strings.value(i).to_string())
            .collect()
    }

    #[test]
    fn out_of_order_rows_come_back_in_key_order() {
        let unsorted = batch(&[30, 10, 20, 10], &["c", "b", "a", "a"]);
        let sorted = sort_batch_by(&unsorted, &key()).unwrap();

        assert_eq!(timestamps_of(&sorted), vec![10, 10, 20, 30]);
        // Ties on the leading column break on the second key column.
        assert_eq!(services_of(&sorted), vec!["a", "b", "a", "c"]);
        assert_eq!(sorted.num_rows(), unsorted.num_rows());
    }

    #[test]
    fn sorting_is_verifiable_and_the_check_catches_unsorted_input() {
        let unsorted = batch(&[30, 10, 20], &["c", "b", "a"]);
        assert!(
            !is_sorted_by(&unsorted, &key()).unwrap(),
            "the check must not pass rows that are out of order"
        );
        let sorted = sort_batch_by(&unsorted, &key()).unwrap();
        assert!(is_sorted_by(&sorted, &key()).unwrap());
    }

    #[test]
    fn an_absent_key_column_is_an_error_rather_than_a_partial_sort() {
        let batch = batch(&[3, 1, 2], &["c", "b", "a"]);
        let mut key = key();
        key.push(DeclaredSortColumn {
            name: "trace_id".to_string(),
            descending: false,
            nulls_first: true,
        });

        let error = sort_batch_by(&batch, &key).expect_err("a missing key column must not sort");
        assert!(error.to_string().contains("trace_id"), "{error}");
        assert!(is_sorted_by(&batch, &key).is_err());
    }

    #[test]
    fn an_empty_key_leaves_rows_untouched() {
        let original = batch(&[3, 1, 2], &["c", "b", "a"]);
        let result = sort_batch_by(&original, &[]).unwrap();
        assert_eq!(timestamps_of(&result), vec![3, 1, 2]);
        assert!(is_sorted_by(&original, &[]).unwrap());
    }

    #[test]
    fn the_canonical_key_by_name_matches_the_declared_one() {
        // Same key, resolved two ways: by name for a table with no
        // declaration, by field id for one that has declared it.
        use crate::iceberg::schemas::TableSchema;

        for table in TableSchema::all() {
            let by_name: Vec<String> = canonical_sort_columns(table.table_name())
                .into_iter()
                .map(|column| column.name)
                .collect();
            let expected: Vec<String> = table
                .sort_key_columns()
                .iter()
                .map(|name| (*name).to_string())
                .collect();
            assert_eq!(by_name, expected, "canonical key of {}", table.table_name());
            assert!(!by_name.is_empty());
        }

        assert!(canonical_sort_columns("not_a_signal_table").is_empty());
    }

    #[test]
    fn a_descending_key_sorts_the_other_way() {
        let key = vec![DeclaredSortColumn {
            name: "timestamp".to_string(),
            descending: true,
            nulls_first: false,
        }];
        let sorted = sort_batch_by(&batch(&[1, 3, 2], &["a", "b", "c"]), &key).unwrap();
        assert_eq!(timestamps_of(&sorted), vec![3, 2, 1]);
        assert!(is_sorted_by(&sorted, &key).unwrap());
    }
}
