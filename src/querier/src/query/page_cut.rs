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
//! [`bound_to_page`] builds the plan side: the lexicographic keyset predicate
//! that resumes after a cursor, and the bounded sort.
//!
//! The cut runs over the collected, already-bounded sort output rather than
//! as a streaming operator: a sort with a fetch consumes its whole input
//! before emitting, so stopping early would save nothing.

use common::query_cursor::{KeyPart, KeyValue, PageReport, PageRequest};
use common::query_ir::{Direction, PageUnit, SortKey};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::array::UInt32Array;
use datafusion::arrow::compute::{concat_batches, take_record_batch};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::functions_window::expr_fn::dense_rank;
use datafusion::logical_expr::{Expr, ExprFunctionExt, SortExpr, lit};
use datafusion::prelude::{DataFrame, ident};
use datafusion::scalar::ScalarValue;

use super::error::QuerierError;

const KEY_COLUMN_PREFIX: &str = "__sdb_page_key_";

/// The name of the column carrying sort key `index`.
pub(crate) fn key_column(index: usize) -> String {
    format!("{KEY_COLUMN_PREFIX}{index}")
}

const RANK_COLUMN: &str = "__sdb_page_rank";

/// Resume `df` after `page.after` and sort it by `page.order`, bounded to
/// what [`cut_page`] needs: `size` rows plus a tie group's worth past them,
/// or the first `size + 1` whole traces. `df` carries one [`key_column`]
/// per order key.
pub(crate) fn bound_to_page(
    df: DataFrame,
    page: &PageRequest,
    max_tie_rows: usize,
) -> Result<DataFrame, QuerierError> {
    let mut df = df;
    if let Some(after) = &page.after {
        let predicate = keyset_predicate(&df, &page.order, after)?;
        df = df.filter(predicate).map_err(QuerierError::QueryFailed)?;
    }
    let newest = page.tail.is_some_and(|t| t.newest);
    if let Some(tail) = page.tail {
        let field = page.order.first().map_or("", |k| k.field.as_str());
        let through = key_literal(&df, &key_column(0), field, &KeyValue::I64(tail.through_ns))?
            .ok_or_else(|| QuerierError::InvalidInput("a tail needs a tail-time bound".into()))?;
        df = df
            .filter(ident(key_column(0)).lt_eq(lit(through)))
            .map_err(QuerierError::QueryFailed)?;
    }
    // The first tail call reads the order backwards for the newest rows;
    // the cut reverses them back.
    let sort: Vec<SortExpr> = page
        .order
        .iter()
        .enumerate()
        .map(|(i, k)| ident(key_column(i)).sort((k.dir == Direction::Asc) != newest, false))
        .collect();
    let size = page.size as usize;
    let df = match page.unit {
        PageUnit::Rows => {
            let past = if page.exact || newest {
                0
            } else {
                max_tie_rows
            };
            let fetch = size.saturating_add(past).saturating_add(1);
            df.sort(sort)?.limit(0, Some(fetch))?
        }
        PageUnit::Traces => {
            let rank = dense_rank()
                .order_by(sort[..1].to_vec())
                .build()?
                .alias(RANK_COLUMN);
            df.window(vec![rank])?
                .filter(ident(RANK_COLUMN).lt_eq(lit(page.size as u64 + 1)))?
                .sort(sort)?
        }
    };
    Ok(df)
}

/// Rows strictly after `after` in `order`, nulls last:
/// `(k1 ≷ v1) ∨ (k1 = v1 ∧ k2 ≷ v2) ∨ …`, plus the leading key's bound on its
/// own so a time-leading order prunes partitions.
fn keyset_predicate(
    df: &DataFrame,
    order: &[SortKey],
    after: &[KeyPart],
) -> Result<Expr, QuerierError> {
    if after.len() != order.len() || after.iter().zip(order).any(|(a, k)| a.field != k.field) {
        return Err(QuerierError::InvalidInput(
            "the page cursor does not match the document's order".into(),
        ));
    }
    let mut disjuncts = Vec::new();
    let mut equal_prefix: Option<Expr> = None;
    let mut leading_bound = None;
    for (i, (key, part)) in order.iter().zip(after).enumerate() {
        let column = ident(key_column(i));
        let equal = match key_literal(df, &key_column(i), &key.field, &part.value)? {
            // Nothing sorts after a null: nulls are last.
            None => column.is_null(),
            Some(value) => {
                let inclusive = match key.dir {
                    Direction::Asc => column.clone().gt_eq(lit(value.clone())),
                    Direction::Desc => column.clone().lt_eq(lit(value.clone())),
                };
                if i == 0 {
                    leading_bound = Some(inclusive.or(column.clone().is_null()));
                }
                let beyond = strictly_beyond(column.clone(), &value, key.dir);
                let beyond = beyond.or(column.clone().is_null());
                disjuncts.push(match &equal_prefix {
                    Some(prefix) => prefix.clone().and(beyond),
                    None => beyond,
                });
                column.eq(lit(value))
            }
        };
        equal_prefix = Some(match equal_prefix {
            Some(prefix) => prefix.and(equal),
            None => equal,
        });
    }
    let predicate = disjuncts.into_iter().reduce(Expr::or).unwrap_or(lit(false));
    Ok(match leading_bound {
        Some(bound) => bound.and(predicate),
        None => predicate,
    })
}

/// `value` as a literal of `column`'s type, or `None` for a null.
fn key_literal(
    df: &DataFrame,
    column: &str,
    field_name: &str,
    value: &KeyValue,
) -> Result<Option<ScalarValue>, QuerierError> {
    let field = df
        .schema()
        .field_with_unqualified_name(column)
        .map_err(QuerierError::QueryFailed)?;
    let scalar = match value {
        KeyValue::Null => return Ok(None),
        // Timestamps travel in nanoseconds (see `key_value`); the cast
        // converts to the column's unit.
        KeyValue::I64(v) => match field.data_type() {
            DataType::Timestamp(_, tz) => ScalarValue::TimestampNanosecond(Some(*v), tz.clone()),
            _ => ScalarValue::Int64(Some(*v)),
        },
        KeyValue::F64(v) => ScalarValue::Float64(Some(*v)),
        KeyValue::Str(v) => ScalarValue::Utf8(Some(v.clone())),
        KeyValue::Bytes(v) => ScalarValue::Binary(Some(v.clone())),
        KeyValue::Bool(v) => ScalarValue::Boolean(Some(*v)),
    };
    let typed = scalar.cast_to(field.data_type()).map_err(|_| {
        QuerierError::InvalidInput(format!(
            "the page cursor's '{field_name}' value does not fit its {} type",
            field.data_type()
        ))
    })?;
    Ok(Some(typed))
}

/// `column` strictly after `value` in `dir`. On an integer or timestamp key
/// it is the inclusive comparison against the next value: the Iceberg scan
/// maps a comparison on the partition source column onto the partition
/// (hour) unchanged, so a strict `>`/`<` would prune the bound's own hour.
fn strictly_beyond(column: Expr, value: &ScalarValue, dir: Direction) -> Expr {
    use ScalarValue::*;
    let step: i64 = if dir == Direction::Asc { 1 } else { -1 };
    let next = match value {
        Int64(Some(v)) => v.checked_add(step).map(|n| Int64(Some(n))),
        UInt64(Some(v)) => v.checked_add_signed(step).map(|n| UInt64(Some(n))),
        TimestampNanosecond(Some(v), tz) => v
            .checked_add(step)
            .map(|n| TimestampNanosecond(Some(n), tz.clone())),
        TimestampMicrosecond(Some(v), tz) => v
            .checked_add(step)
            .map(|n| TimestampMicrosecond(Some(n), tz.clone())),
        TimestampMillisecond(Some(v), tz) => v
            .checked_add(step)
            .map(|n| TimestampMillisecond(Some(n), tz.clone())),
        TimestampSecond(Some(v), tz) => v
            .checked_add(step)
            .map(|n| TimestampSecond(Some(n), tz.clone())),
        _ => None,
    };
    match (next, dir) {
        (Some(next), Direction::Asc) => column.gt_eq(lit(next)),
        (Some(next), Direction::Desc) => column.lt_eq(lit(next)),
        (None, Direction::Asc) => column.gt(lit(value.clone())),
        (None, Direction::Desc) => column.lt(lit(value.clone())),
    }
}

/// The bounds one page is cut to.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CutLimits {
    pub size: usize,
    pub unit: PageUnit,
    /// Cut at exactly `size` rows, even inside a tie group.
    pub exact: bool,
    /// Emit the page in reverse (a first tail call's newest rows, oldest
    /// first).
    pub reverse: bool,
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
    let converter = RowConverter::new(
        keys[..group_width]
            .iter()
            .map(|k| SortField::new(k.data_type().clone()))
            .collect(),
    )
    .map_err(arrow_error)?;
    let rows = converter
        .convert_columns(&keys[..group_width])
        .map_err(arrow_error)?;
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
            if limits.unit == PageUnit::Rows && group > limits.max_tie_rows {
                return Err(QuerierError::ResourceExhausted(format!(
                    "more than {} rows share one sort key \
                     ([querier].page_max_tie_rows); add an `order` key to break the tie",
                    limits.max_tie_rows
                )));
            }
            if (cut + group) * row_bytes > limits.max_bytes {
                if cut > 0 {
                    break;
                }
                if limits.unit == PageUnit::Traces {
                    return Err(QuerierError::ResourceExhausted(format!(
                        "one trace exceeds the page byte bound of {} \
                         ([querier].page_max_bytes)",
                        limits.max_bytes
                    )));
                }
            }
            cut = end;
            units += match limits.unit {
                PageUnit::Rows => group,
                PageUnit::Traces => 1,
            };
        }
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
    if limits.reverse {
        let indices = UInt32Array::from_iter_values((0..cut as u32).rev());
        page = take_record_batch(&page, &indices).map_err(arrow_error)?;
    }
    Ok((
        vec![page],
        PageReport {
            last_key,
            has_more: cut < n,
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
        // Timestamps in nanoseconds, whatever the column's unit, so a tail's
        // nanosecond bound and a row's key compare alike.
        Int64(Some(v)) | TimestampNanosecond(Some(v), _) => KeyValue::I64(v),
        TimestampMicrosecond(Some(v), _) => KeyValue::I64(v.saturating_mul(1_000)),
        TimestampMillisecond(Some(v), _) => KeyValue::I64(v.saturating_mul(1_000_000)),
        TimestampSecond(Some(v), _) => KeyValue::I64(v.saturating_mul(1_000_000_000)),
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
            reverse: false,
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
        l.max_tie_rows = 1;
        let (page, report) = cut_page(std::slice::from_ref(&b), &trace_order(), l).expect("cut");
        assert_eq!(page[0].num_rows(), 5);
        assert_eq!(report.emitted, 2);
        assert!(report.has_more);
        assert_eq!(
            report.last_key.expect("key")[0].value,
            KeyValue::Str("b".into())
        );

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
    fn a_nanosecond_key_compares_against_a_microsecond_column() {
        let ctx = datafusion::prelude::SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            key_column(0),
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(
                datafusion::arrow::array::TimestampMicrosecondArray::from(vec![1, 2, 3]),
            )],
        )
        .expect("batch");
        let df = ctx.read_batch(batch).expect("df");
        let literal = key_literal(&df, &key_column(0), "timestamp", &KeyValue::I64(2_000))
            .expect("literal")
            .expect("not null");
        assert_eq!(
            literal,
            ScalarValue::TimestampMicrosecond(Some(2), None),
            "2,000 ns is 2 µs"
        );
    }

    #[test]
    fn the_keyset_predicate_compares_against_the_next_value() {
        let ctx = datafusion::prelude::SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            key_column(0),
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).expect("batch");
        let df = ctx.read_batch(batch).expect("df");
        let order = [SortKey {
            field: "timestamp".into(),
            dir: Direction::Asc,
        }];
        let after = [KeyPart {
            field: "timestamp".into(),
            value: KeyValue::I64(1),
        }];
        let predicate = keyset_predicate(&df, &order, &after)
            .expect("predicate")
            .to_string();
        assert!(!predicate.contains(" > "), "{predicate}");
        assert!(predicate.contains(">= Int64(2)"), "{predicate}");
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
