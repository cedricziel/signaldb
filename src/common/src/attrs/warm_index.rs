//! Warm-tier containment tokens for typed attribute homes (see
//! `openspec/changes/otel-native-schema`, spec `typed-attribute-storage`,
//! "Warm tier"): one `List<Binary>` column per table, `attr_index`, holding a
//! deterministic token per `(key, value)` pair actually written to a typed
//! home (never residue), so a Parquet bloom filter on the list leaf can
//! prune files for an unpromoted `key = value` predicate without
//! stringifying typed literals. The token is derived from exactly the value
//! written to its home ([`crate::attrs::typed::HomeValue`]), so write and
//! query encode identical bytes — no false negatives.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BinaryBuilder, ListBuilder};
use datafusion::arrow::datatypes::{DataType, Field};

use super::typed::HomeValue;

/// Table-level column name for the warm containment index.
pub const WARM_INDEX_COLUMN: &str = "attr_index";

/// Unit separator between the key and the canonical value bytes; a key
/// containing it only widens the tolerated false-positive surface.
const KEY_VALUE_SEP: u8 = 0x1F;

const TAG_STRING: u8 = b's';
const TAG_INT: u8 = b'i';
const TAG_DOUBLE: u8 = b'd';
const TAG_BOOL: u8 = b'b';

/// Errors from [`WarmIndexBuilder::finish`].
#[derive(Debug, thiserror::Error)]
pub enum WarmIndexError {
    #[error("field '{0}' is not a List field for the warm-index column")]
    NotAListField(String),
}

/// Encodes one containment token: `type_tag ‖ key ‖ 0x1F ‖
/// canonical_value_bytes`. Value bytes are UTF-8 for a string, big-endian
/// `i64`/`f64::to_bits` for a number (`-0.0` folded to `+0.0`), and a single
/// `0x00`/`0x01` byte for a bool. Returns `None` for `NaN`, which has no
/// canonical byte encoding and so contributes no token.
pub fn encode_token(key: &str, value: HomeValue<'_>) -> Option<Vec<u8>> {
    if matches!(value, HomeValue::Double(d) if d.is_nan()) {
        return None;
    }
    let mut token = Vec::with_capacity(1 + key.len() + 1 + 8);
    token.push(match value {
        HomeValue::Str(_) => TAG_STRING,
        HomeValue::Int(_) => TAG_INT,
        HomeValue::Double(_) => TAG_DOUBLE,
        HomeValue::Bool(_) => TAG_BOOL,
    });
    token.extend_from_slice(key.as_bytes());
    token.push(KEY_VALUE_SEP);
    match value {
        HomeValue::Str(s) => token.extend_from_slice(s.as_bytes()),
        HomeValue::Int(i) => token.extend_from_slice(&i.to_be_bytes()),
        HomeValue::Double(d) => {
            let canonical = if d == 0.0 { 0.0_f64 } else { d };
            token.extend_from_slice(&canonical.to_bits().to_be_bytes());
        }
        HomeValue::Bool(b) => token.push(u8::from(b)),
    }
    Some(token)
}

/// Accumulates warm-index tokens row by row, deduplicating per row, and
/// finishes to the `List<Binary>` array for [`WARM_INDEX_COLUMN`].
pub struct WarmIndexBuilder {
    rows: Vec<Vec<Vec<u8>>>,
}

impl WarmIndexBuilder {
    /// `rows` is the number of rows the finished array must have.
    pub fn new(rows: usize) -> Self {
        Self {
            rows: vec![Vec::new(); rows],
        }
    }

    /// Adds `token` to `row`. Out-of-range rows are silently ignored;
    /// duplicates within a row are dropped at [`Self::finish`].
    pub fn add(&mut self, row: usize, token: Vec<u8>) {
        if let Some(row) = self.rows.get_mut(row) {
            row.push(token);
        }
    }

    /// Builds the `List<Binary>` array, with `field`'s list-item field used
    /// verbatim so the array matches the target table schema exactly. A row
    /// with no tokens gets an empty list, not a null — matching
    /// [`ATTR_TOKENS_COLUMN`](crate::schema::ATTR_TOKENS_COLUMN)'s
    /// convention so an ANDed containment predicate never nulls out rows the
    /// typed-home columns matched. Token order within a row is not
    /// preserved — sorted for deduplication, which is all a bloom filter
    /// leaf needs.
    pub fn finish(self, field: &Field) -> Result<ArrayRef, WarmIndexError> {
        let DataType::List(item_field) = field.data_type() else {
            return Err(WarmIndexError::NotAListField(field.name().clone()));
        };

        let mut builder = ListBuilder::new(BinaryBuilder::new()).with_field(item_field.clone());
        for mut tokens in self.rows {
            tokens.sort_unstable();
            tokens.dedup();
            builder.append_value(tokens.iter().map(|token| Some(token.as_slice())));
        }
        Ok(Arc::new(builder.finish()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, BinaryArray, ListArray};

    #[test]
    fn encoding_is_deterministic_and_type_tagged() {
        assert_eq!(
            encode_token("k", HomeValue::Str("v")),
            encode_token("k", HomeValue::Str("v"))
        );
        // A type tag keeps an int and a look-alike string from colliding.
        assert_ne!(
            encode_token("status_code", HomeValue::Int(200)),
            encode_token("status_code", HomeValue::Str("200"))
        );
    }

    #[test]
    fn double_folds_signed_zero_and_drops_nan() {
        assert_eq!(
            encode_token("k", HomeValue::Double(0.0)),
            encode_token("k", HomeValue::Double(-0.0))
        );
        assert_eq!(encode_token("k", HomeValue::Double(f64::NAN)), None);
    }

    #[test]
    fn bool_and_separator_containing_key_both_encode() {
        let false_token = encode_token("k", HomeValue::Bool(false)).unwrap();
        let true_token = encode_token("k", HomeValue::Bool(true)).unwrap();
        assert_eq!(false_token.last(), Some(&0u8));
        assert_eq!(true_token.last(), Some(&1u8));
        // Collisions from a key containing the separator are false
        // positives only; no test needed beyond "it still encodes".
        assert!(encode_token("weird\u{1f}key", HomeValue::Bool(true)).is_some());
    }

    fn binary_list_field() -> Field {
        Field::new(
            WARM_INDEX_COLUMN,
            DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
            true,
        )
    }

    fn list_values(array: &ArrayRef, row: usize) -> Vec<Vec<u8>> {
        let list = array
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("List<Binary>");
        let entries = list.value(row);
        let entries = entries
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Binary items");
        (0..entries.len())
            .map(|i| entries.value(i).to_vec())
            .collect()
    }

    #[test]
    fn builder_dedups_tokens_per_row() {
        let mut builder = WarmIndexBuilder::new(1);
        let token = encode_token("k", HomeValue::Str("v")).unwrap();
        builder.add(0, token.clone());
        builder.add(0, token.clone());
        let array = builder.finish(&binary_list_field()).unwrap();
        assert_eq!(list_values(&array, 0), vec![token]);
    }

    #[test]
    fn empty_row_is_an_empty_list_matching_the_target_field_exactly() {
        let target = binary_list_field();
        let builder = WarmIndexBuilder::new(1);
        let array = builder.finish(&target).unwrap();
        assert!(!array.is_null(0), "empty list, never null");
        assert!(list_values(&array, 0).is_empty());
        assert_eq!(array.data_type(), target.data_type());
    }
}
