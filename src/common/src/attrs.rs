//! # Attribute-column decoding
//!
//! Signal tables carry their unpromoted attributes in container columns
//! (`log_attributes`, `resource_attributes`, `span_attributes`). Persisted
//! rows use the typed layout ([`typed`]): four typed maps plus a CBOR residue
//! column, detected by the presence of the container's `_residue` column.
//! Unflushed rows the querier merges in from the writer still arrive in the
//! Flight wire format, where a container is one JSON-in-`Utf8` column.
//!
//! [`json_documents`] is the one place that detects the form and decodes to
//! native-typed per-row JSON objects; every reader (label discovery, the
//! Loki-compatible router API, trace/profile row decoding) builds on it
//! rather than re-detecting the layout. [`attr_documents`] is the
//! string-valued convenience built on top of it. [`string_map_documents`]
//! decodes a single `Map<Utf8, Utf8>` column directly — for a caller that
//! already knows which one typed home it wants (e.g. the compactor's label
//! backfill reading a key's string home) rather than a whole container.

use std::collections::BTreeMap;

use datafusion::arrow::array::{Array, MapArray, RecordBatch, StringArray};
use serde_json::{Map as JsonMap, Value as JsonValue};
use tracing::warn;

use crate::schema::typed_attributes::residue_column;

pub mod expr;
pub mod typed;
pub mod warm_index;

/// One row's attributes, decoded but not yet rendered to strings.
pub type JsonDocument = JsonMap<String, JsonValue>;

/// One row's attributes as string key/value pairs.
pub type AttrDocument = BTreeMap<String, String>;

/// Why an attribute column could not be decoded. A row that is merely null,
/// unparseable, or not a JSON object is not an error — it decodes to `None`;
/// see [`json_documents`] for how a malformed column itself degrades.
#[derive(Debug, thiserror::Error)]
pub enum AttrDecodeError {
    #[error("missing column '{0}'")]
    MissingColumn(String),
}

/// Renders a decoded JSON attribute value the way the legacy writer stored
/// it: a JSON string as-is, everything else as its JSON text, so a value is
/// never silently dropped.
fn render_value(value: JsonValue) -> String {
    match value {
        JsonValue::String(s) => s,
        other => other.to_string(),
    }
}

/// Read an attribute container's per-row documents, native-typed, from
/// either the typed layout or a wire-format JSON-in-`Utf8` column. An absent
/// container yields `None` for every row; a malformed one (wrong Arrow
/// shape, a typed-layout decode error) logs a warning and also degrades to
/// `None` for every row rather than failing the batch — the policy
/// [`attr_documents`] inherits.
pub fn json_documents(batch: &RecordBatch, name: &str) -> Vec<Option<JsonDocument>> {
    let Some(column) = batch.column_by_name(name) else {
        if batch.column_by_name(&residue_column(name)).is_none() {
            return vec![None; batch.num_rows()];
        }
        return match typed::decode_container(batch, name) {
            Ok(rows) => rows,
            Err(error) => {
                warn!(container = name, %error, "failed to decode typed attribute container");
                vec![None; batch.num_rows()]
            }
        };
    };

    // Wire-format (unflushed) batches carry the container as JSON text.
    if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
        return json_string_documents(arr);
    }

    warn!(
        container = name,
        arrow_type = %column.data_type(),
        "attribute column has an unsupported Arrow type"
    );
    vec![None; batch.num_rows()]
}

/// Decode a flat-JSON-object `Utf8` attribute column into per-row
/// documents. A null, unparseable, or non-object row decodes to `None` —
/// this is the normal degrade path, not a malformed-column condition, so it
/// is not logged.
fn json_string_documents(arr: &StringArray) -> Vec<Option<JsonDocument>> {
    (0..arr.len())
        .map(|i| {
            if arr.is_null(i) {
                return None;
            }
            match serde_json::from_str::<JsonValue>(arr.value(i)) {
                Ok(JsonValue::Object(map)) => Some(map),
                _ => None,
            }
        })
        .collect()
}

/// Read an attribute container's per-row documents as string key/value maps;
/// see [`json_documents`] for the layout detection and malformed-column
/// policy this builds on.
pub fn attr_documents(
    batch: &RecordBatch,
    name: &str,
) -> Result<Vec<Option<AttrDocument>>, AttrDecodeError> {
    if batch.column_by_name(name).is_none() && batch.column_by_name(&residue_column(name)).is_none()
    {
        return Err(AttrDecodeError::MissingColumn(name.to_string()));
    }
    Ok(json_documents(batch, name)
        .into_iter()
        .map(|row| row.map(|obj| obj.into_iter().map(|(k, v)| (k, render_value(v))).collect()))
        .collect())
}

/// Decode a single `Map<Utf8, Utf8>` typed-layout home column — not a whole
/// container — into per-row string documents. For a caller that already
/// knows which one typed home it wants by column name (e.g.
/// `log_attributes_str`), rather than a whole container's layout.
pub fn string_map_documents(
    batch: &RecordBatch,
    name: &str,
) -> Result<Vec<Option<AttrDocument>>, AttrDecodeError> {
    let Some(column) = batch.column_by_name(name) else {
        return Err(AttrDecodeError::MissingColumn(name.to_string()));
    };
    let Some(map) = column.as_any().downcast_ref::<MapArray>() else {
        warn!(
            column = name,
            arrow_type = %column.data_type(),
            "attribute column is not a Map<Utf8, Utf8>"
        );
        return Ok(vec![None; batch.num_rows()]);
    };

    let mut out = Vec::with_capacity(map.len());
    for i in 0..map.len() {
        if map.is_null(i) {
            out.push(None);
            continue;
        }
        let entries = map.value(i);
        let (Some(keys), Some(values)) = (
            entries.column(0).as_any().downcast_ref::<StringArray>(),
            entries.column(1).as_any().downcast_ref::<StringArray>(),
        ) else {
            warn!(
                column = name,
                "map attribute column has non-string keys or values"
            );
            out.push(None);
            continue;
        };
        let mut doc = AttrDocument::new();
        for j in 0..entries.len() {
            if !keys.is_null(j) && !values.is_null(j) {
                doc.insert(keys.value(j).to_string(), values.value(j).to_string());
            }
        }
        out.push(Some(doc));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, MapBuilder, StringBuilder};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn json_batch(rows: Vec<Option<&str>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "log_attributes",
            DataType::Utf8,
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(rows))]).unwrap()
    }

    /// Build a `Map<Utf8, Utf8>` column; `None` is a null row.
    fn map_batch(name: &str, rows: Vec<Option<Vec<(&str, &str)>>>) -> RecordBatch {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for row in rows {
            match row {
                Some(pairs) => {
                    for (k, v) in pairs {
                        builder.keys().append_value(k);
                        builder.values().append_value(v);
                    }
                    builder.append(true).unwrap();
                }
                None => builder.append(false).unwrap(),
            }
        }
        let array = builder.finish();
        let schema = Arc::new(Schema::new(vec![Field::new(
            name,
            array.data_type().clone(),
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(array) as ArrayRef]).unwrap()
    }

    #[test]
    fn missing_column_is_an_error() {
        let batch = json_batch(vec![Some("{}")]);
        assert!(matches!(
            attr_documents(&batch, "resource_attributes"),
            Err(AttrDecodeError::MissingColumn(_))
        ));
    }

    #[test]
    fn decodes_wire_format_json_string_columns() {
        let batch = json_batch(vec![Some(r#"{"user.id":"u-1","retries":3}"#), None]);
        let docs = attr_documents(&batch, "log_attributes").unwrap();
        let doc = docs[0].as_ref().unwrap();
        assert_eq!(doc["user.id"], "u-1");
        assert_eq!(doc["retries"], "3", "non-string values render as JSON text");
        assert!(docs[1].is_none());
    }

    /// A row whose JSON is malformed or is not an object must not fail the
    /// batch — the other rows in the same response still carry their
    /// attributes.
    #[test]
    fn unparseable_rows_decode_to_none_without_failing_the_batch() {
        let batch = json_batch(vec![
            Some("{not json"),
            Some(r#"["an","array"]"#),
            Some(r#"{"ok":"yes"}"#),
        ]);
        let docs = attr_documents(&batch, "log_attributes").unwrap();
        assert!(docs[0].is_none());
        assert!(docs[1].is_none());
        assert_eq!(docs[2].as_ref().unwrap()["ok"], "yes");
    }

    /// A whole-container `Map<Utf8, Utf8>` column is the retired physical
    /// layout: it no longer decodes.
    #[test]
    fn whole_container_map_column_is_not_decoded() {
        let batch = map_batch("log_attributes", vec![Some(vec![("k", "v")])]);
        assert_eq!(json_documents(&batch, "log_attributes"), vec![None]);
    }

    /// A typed-layout batch (four typed maps + CBOR residue) decodes each
    /// value to its rendered string form, across an int, a double, a bool,
    /// a string, and an array (residue) value.
    #[test]
    fn decodes_typed_layout_columns() {
        use serde_json::json;

        let row = serde_json::Map::from_iter([
            ("count".to_string(), json!(3)),
            ("ratio".to_string(), json!(1.5)),
            ("ok".to_string(), json!(true)),
            ("name".to_string(), json!("hello")),
            ("tags".to_string(), json!(["a", "b"])),
        ]);
        let (fields, arrays) =
            crate::testing::typed_attribute_columns("span_attributes", &[Some(row), None]);
        let schema = Arc::new(Schema::new(fields.to_vec()));
        let batch = RecordBatch::try_new(schema, arrays.to_vec()).unwrap();

        let docs = attr_documents(&batch, "span_attributes").unwrap();
        let doc = docs[0].as_ref().unwrap();
        assert_eq!(doc["count"], "3");
        assert_eq!(doc["ratio"], "1.5");
        assert_eq!(doc["ok"], "true");
        assert_eq!(doc["name"], "hello");
        assert_eq!(doc["tags"], "[\"a\",\"b\"]");
        assert!(docs[1].is_none(), "a null row has no attributes");
    }

    /// A container with no `_residue` column at all (never written, or a
    /// batch that doesn't carry it) decodes to `None` for every row, not an
    /// error — [`attr_documents`] is the one that reports it missing.
    #[test]
    fn absent_container_decodes_to_none() {
        let batch = json_batch(vec![Some("{}")]);
        let docs = json_documents(&batch, "resource_attributes");
        assert_eq!(docs, vec![None]);
    }

    #[test]
    fn string_map_documents_decodes_one_map_column() {
        let batch = map_batch(
            "log_attributes_str",
            vec![Some(vec![("http.method", "GET")]), None],
        );
        let docs = string_map_documents(&batch, "log_attributes_str").unwrap();
        assert_eq!(docs[0].as_ref().unwrap()["http.method"], "GET");
        assert!(docs[1].is_none(), "a null map row has no attributes");
    }

    #[test]
    fn string_map_documents_missing_column_is_an_error() {
        let batch = json_batch(vec![Some("{}")]);
        assert!(matches!(
            string_map_documents(&batch, "log_attributes_str"),
            Err(AttrDecodeError::MissingColumn(_))
        ));
    }
}
