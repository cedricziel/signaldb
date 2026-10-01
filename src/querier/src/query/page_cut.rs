//! The page cut of a paged Query IR result
//! (`openspec/changes/query-result-pagination-and-tail`, design D2/D8).
//!
//! The planner sorts the result by the page's total order, with each sort key
//! carried as an extra column ([`key_column`]), and bounds the sort with a
//! fetch. [`cut_page`] then takes `size` units off the front without ever
//! splitting a tie group (rows with equal full keys) or, for the `trace`
//! envelope, a trace. It reports the last emitted key and whether more
//! follows, and drops the key columns.
//!
//! The cut runs over the collected, already-bounded sort output rather than
//! as a streaming operator: a sort with a fetch consumes its whole input
//! before emitting, so stopping early would save nothing.

use common::query_cursor::{KeyPart, KeyValue, PageReport};
use common::query_ir::{PageUnit, SortKey};
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::row::{RowConverter, Rows, SortField};
use datafusion::scalar::ScalarValue;

use super::error::QuerierError;

const KEY_COLUMN_PREFIX: &str = "__sdb_page_key_";

/// The name of the column carrying sort key `index`.
pub(crate) fn key_column(index: usize) -> String {
    format!("{KEY_COLUMN_PREFIX}{index}")
}

/// The bounds one page is cut to.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CutLimits {
    pub size: usize,
    pub unit: PageUnit,
    /// Cut at exactly `size` rows, even inside a tie group.
    pub exact: bool,
    /// Rows left under a trailing `limit`: the cut never passes them, and
    /// reaching them ends the walk.
    pub ceiling: Option<usize>,
    /// Rows one group may hold: a tie group, or one trace's spans.
    pub max_tie_rows: usize,
    pub max_bytes: usize,
}

/// Cut one page off `batches`, which hold the sorted result plus one
/// [`key_column`] per entry of `order`.
pub(crate) fn cut_page(
    batches: &[RecordBatch],
    order: &[SortKey],
    limits: CutLimits,
) -> Result<(Vec<RecordBatch>, PageReport), QuerierError> {
    let Some(first) = batches.first() else {
        return Ok((Vec::new(), PageReport::default()));
    };
    let all = concat_batches(&first.schema(), batches).map_err(arrow_error)?;
    let n = all.num_rows();
    let key_indices = (0..order.len())
        .map(|i| all.schema().index_of(&key_column(i)).map_err(arrow_error))
        .collect::<Result<Vec<_>, _>>()?;
    let keys: Vec<_> = key_indices.iter().map(|&i| all.column(i).clone()).collect();
    // Group boundaries: where the full key changes (a tie group), or for
    // whole traces where the leading `trace_id` key does.
    let group_width = match limits.unit {
        PageUnit::Rows => keys.len(),
        PageUnit::Traces => 1,
    };
    let rows = row_keys(&keys[..group_width])?;
    let row_bytes = all.get_array_memory_size() / n.max(1);

    let mut cut = 0;
    let mut units = 0;
    if limits.exact {
        cut = n.min(limits.size);
        units = cut;
    } else {
        while cut < n && units < limits.size {
            let mut end = cut + 1;
            while end < n && rows.row(end) == rows.row(cut) {
                end += 1;
            }
            let group = end - cut;
            if group > limits.max_tie_rows {
                return Err(QuerierError::ResourceExhausted(match limits.unit {
                    PageUnit::Rows => format!(
                        "more than {} rows share one sort key \
                         ([querier].page_max_tie_rows); add an `order` key to break the tie",
                        limits.max_tie_rows
                    ),
                    PageUnit::Traces => format!(
                        "one trace has more than {} spans \
                         ([querier].match_max_trace_spans); narrow the query",
                        limits.max_tie_rows
                    ),
                }));
            }
            if (cut + group) * row_bytes > limits.max_bytes {
                // The page ends before a group that does not fit, unless it
                // is the first: a page never splits one.
                if cut > 0 {
                    break;
                }
                return Err(QuerierError::ResourceExhausted(format!(
                    "one sort key group (or trace) exceeds the page byte bound of {} \
                     ([querier].page_max_bytes)",
                    limits.max_bytes
                )));
            }
            cut = end;
            units += match limits.unit {
                PageUnit::Rows => group,
                PageUnit::Traces => 1,
            };
        }
    }
    let capped = limits.ceiling.is_some_and(|ceiling| cut >= ceiling);
    if let Some(ceiling) = limits.ceiling.filter(|&ceiling| cut > ceiling) {
        (cut, units) = (ceiling, ceiling);
    }

    let last_key = cut
        .checked_sub(1)
        .map(|last| {
            order
                .iter()
                .zip(&keys)
                .map(|(key, column)| {
                    Ok(KeyPart {
                        field: key.field.clone(),
                        value: key_value(
                            ScalarValue::try_from_array(column, last)
                                .map_err(QuerierError::QueryFailed)?,
                        )?,
                    })
                })
                .collect::<Result<Vec<_>, QuerierError>>()
        })
        .transpose()?;
    let mut page = all.slice(0, cut);
    for &i in key_indices.iter().rev() {
        page.remove_column(i);
    }
    Ok((
        vec![page],
        PageReport {
            last_key,
            has_more: cut < n && !capped,
            emitted: units as u64,
        },
    ))
}

/// A sort key value as the cursor carries it.
pub(crate) fn key_value(value: ScalarValue) -> Result<KeyValue, QuerierError> {
    use ScalarValue::*;
    if value.is_null() {
        return Ok(KeyValue::Null);
    }
    Ok(match value {
        Int8(Some(v)) => KeyValue::I64(v.into()),
        Int16(Some(v)) => KeyValue::I64(v.into()),
        Int32(Some(v)) => KeyValue::I64(v.into()),
        Int64(Some(v))
        | TimestampNanosecond(Some(v), _)
        | TimestampMicrosecond(Some(v), _)
        | TimestampMillisecond(Some(v), _)
        | TimestampSecond(Some(v), _) => KeyValue::I64(v),
        UInt8(Some(v)) => KeyValue::I64(v.into()),
        UInt16(Some(v)) => KeyValue::I64(v.into()),
        UInt32(Some(v)) => KeyValue::I64(v.into()),
        UInt64(Some(v)) => KeyValue::I64(
            i64::try_from(v)
                .map_err(|_| QuerierError::Unsupported("a u64 sort key above i64::MAX".into()))?,
        ),
        Float32(Some(v)) => KeyValue::F64(v.into()),
        Float64(Some(v)) => KeyValue::F64(v),
        Boolean(Some(v)) => KeyValue::Bool(v),
        Utf8(Some(v)) | LargeUtf8(Some(v)) | Utf8View(Some(v)) => KeyValue::Str(v),
        Binary(Some(v)) | LargeBinary(Some(v)) | BinaryView(Some(v)) => KeyValue::Bytes(v),
        FixedSizeBinary(_, Some(v)) => KeyValue::Bytes(v),
        Dictionary(_, inner) => return key_value(*inner),
        other => {
            return Err(QuerierError::Unsupported(format!(
                "a {} sort key cannot be paginated",
                other.data_type()
            )));
        }
    })
}

/// `keys` row-encoded, so rows compare by their full key.
fn row_keys(keys: &[ArrayRef]) -> Result<Rows, QuerierError> {
    let fields = keys.iter().map(|k| SortField::new(k.data_type().clone()));
    let converter = RowConverter::new(fields.collect()).map_err(arrow_error)?;
    converter.convert_columns(keys).map_err(arrow_error)
}

fn arrow_error(e: datafusion::arrow::error::ArrowError) -> QuerierError {
    QuerierError::QueryFailed(e.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::query_ir::Direction;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    const BIG: usize = usize::MAX / 2;

    fn order() -> Vec<SortKey> {
        ["timestamp", "trace_id"]
            .iter()
            .map(|f| SortKey {
                field: f.to_string(),
                dir: Direction::Asc,
            })
            .collect()
    }

    /// Rows `(value, k0 = ts, k1 = trace)`, already sorted.
    fn batch(rows: &[(i64, &str)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new(key_column(0), DataType::Int64, false),
            Field::new(key_column(1), DataType::Utf8, false),
        ]));
        let ts: Vec<i64> = rows.iter().map(|r| r.0).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values(0..rows.len() as i64)),
                Arc::new(Int64Array::from(ts)),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
            ],
        )
        .expect("batch")
    }

    fn limits(size: usize, unit: PageUnit) -> CutLimits {
        CutLimits {
            size,
            unit,
            exact: false,
            ceiling: None,
            max_tie_rows: BIG,
            max_bytes: BIG,
        }
    }

    fn cut(rows: &[(i64, &str)], limits: CutLimits) -> Result<(usize, PageReport), QuerierError> {
        let (page, report) = cut_page(&[batch(rows)], &order(), limits)?;
        let schema = page[0].schema();
        assert_eq!(schema.fields().len(), 1, "key columns are dropped");
        Ok((page[0].num_rows(), report))
    }

    #[test]
    fn passes_size_rows_then_completes_the_tie_group() {
        let rows = [(1, "a"), (2, "a"), (2, "a"), (2, "a"), (3, "a")];
        let (n, report) = cut(&rows, limits(2, PageUnit::Rows)).expect("cut");
        assert_eq!(n, 4);
        assert!(report.has_more);
        assert_eq!(report.emitted, 4);
        assert_eq!(
            report.last_key,
            Some(vec![
                KeyPart {
                    field: "timestamp".into(),
                    value: KeyValue::I64(2)
                },
                KeyPart {
                    field: "trace_id".into(),
                    value: KeyValue::Str("a".into())
                },
            ])
        );
    }

    #[test]
    fn has_more_is_exact_at_the_boundary() {
        let three = [(1, "a"), (2, "a"), (3, "a")];
        let (n, report) = cut(&three, limits(3, PageUnit::Rows)).expect("cut");
        assert_eq!((n, report.has_more), (3, false));
        let (n, report) = cut(&three, limits(2, PageUnit::Rows)).expect("cut");
        assert_eq!((n, report.has_more), (2, true));
    }

    #[test]
    fn a_tie_group_past_the_bound_is_a_resource_error() {
        let rows = [(1, "a"), (2, "a"), (2, "a"), (2, "a")];
        let mut l = limits(2, PageUnit::Rows);
        l.max_tie_rows = 2;
        assert!(matches!(
            cut(&rows, l),
            Err(QuerierError::ResourceExhausted(m)) if m.contains("page_max_tie_rows")
        ));
    }

    #[test]
    fn the_byte_bound_ends_a_page_early_on_a_key_boundary() {
        let rows = [(1, "a"), (2, "a"), (2, "a"), (3, "a"), (4, "a")];
        let row_bytes = batch(&rows).get_array_memory_size() / rows.len();
        let mut l = limits(5, PageUnit::Rows);
        l.max_bytes = row_bytes * 2;
        let (n, report) = cut(&rows, l).expect("cut");
        assert_eq!(
            n, 1,
            "the tie group (2, a) does not fit beside the first row"
        );
        assert!(report.has_more);
    }

    #[test]
    fn a_first_tie_group_past_the_byte_bound_is_a_resource_error() {
        let rows = [(1, "a"), (1, "a"), (2, "a")];
        let row_bytes = batch(&rows).get_array_memory_size() / rows.len();
        let mut l = limits(1, PageUnit::Rows);
        l.max_bytes = row_bytes;
        assert!(matches!(
            cut(&rows, l),
            Err(QuerierError::ResourceExhausted(m)) if m.contains("page_max_bytes")
        ));
    }

    #[test]
    fn a_ceiling_caps_a_tie_group_and_ends_the_walk() {
        let tied = [(1, "a"); 12];
        let mut l = limits(2, PageUnit::Rows);
        l.ceiling = Some(10);
        let (n, report) = cut(&tied, l).expect("cut");
        assert_eq!((n, report.emitted, report.has_more), (10, 10, false));
    }

    #[test]
    fn exact_cuts_inside_a_tie_group() {
        let rows = [(1, "a"), (2, "a"), (2, "a")];
        let mut l = limits(2, PageUnit::Rows);
        l.exact = true;
        let (n, report) = cut(&rows, l).expect("cut");
        assert_eq!((n, report.emitted, report.has_more), (2, 2, true));
    }

    fn trace_order() -> Vec<SortKey> {
        let mut keys = order();
        keys.swap(0, 1);
        keys
    }

    #[test]
    fn the_traces_unit_counts_and_never_splits_traces() {
        // Leading key trace id, as the trace envelope requires.
        let schema = Arc::new(Schema::new(vec![
            Field::new(key_column(0), DataType::Utf8, false),
            Field::new(key_column(1), DataType::Int64, false),
        ]));
        let b = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "a", "b", "b", "b", "c"])),
                Arc::new(Int64Array::from(vec![1, 2, 1, 2, 3, 1])),
            ],
        )
        .expect("batch");
        let mut l = limits(2, PageUnit::Traces);
        l.max_tie_rows = 3;
        let (page, report) = cut_page(std::slice::from_ref(&b), &trace_order(), l).expect("cut");
        assert_eq!(page[0].num_rows(), 5);
        assert_eq!(report.emitted, 2);
        assert!(report.has_more);
        assert_eq!(
            report.last_key.expect("key")[0].value,
            KeyValue::Str("b".into())
        );

        l.max_tie_rows = 2;
        assert!(matches!(
            cut_page(std::slice::from_ref(&b), &trace_order(), l),
            Err(QuerierError::ResourceExhausted(m)) if m.contains("match_max_trace_spans")
        ));

        l.max_tie_rows = 3;
        let row_bytes = b.get_array_memory_size() / b.num_rows();
        l.max_bytes = row_bytes;
        assert!(matches!(
            cut_page(&[b], &trace_order(), l),
            Err(QuerierError::ResourceExhausted(m)) if m.contains("page_max_bytes")
        ));
    }

    #[test]
    fn an_empty_result_is_an_empty_final_page() {
        let (page, report) = cut_page(&[], &order(), limits(2, PageUnit::Rows)).expect("cut");
        assert!(page.is_empty());
        assert_eq!(report, PageReport::default());
        let (n, report) = cut(&[], limits(2, PageUnit::Rows)).expect("cut");
        assert_eq!((n, report.has_more, report.last_key), (0, false, None));
    }

    #[test]
    fn key_values_keep_their_type() {
        assert_eq!(
            key_value(ScalarValue::TimestampNanosecond(Some(5), None)).expect("ts"),
            KeyValue::I64(5)
        );
        assert_eq!(
            key_value(ScalarValue::Utf8(None)).expect("null"),
            KeyValue::Null
        );
    }
}
