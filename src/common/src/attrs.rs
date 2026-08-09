//! # Attribute-column decoding
//!
//! Signal tables carry their unpromoted attributes in container columns
//! (`log_attributes`, `resource_attributes`, `span_attributes`) that exist in
//! two storage forms: legacy `Utf8` columns holding a flat JSON object, and
//! typed `Map<Utf8, Utf8>` columns on tables written by current versions.
//!
//! Every reader has to handle both, so the decode lives here rather than in
//! each service — the querier reads these columns for label discovery, and the
//! router reads them when shaping rows for the Loki-compatible API.

use std::collections::BTreeMap;

use datafusion::arrow::array::{Array, MapArray, RecordBatch, StringArray};

/// One row's attributes as string key/value pairs.
pub type AttrDocument = BTreeMap<String, String>;

/// Why an attribute column could not be decoded. A row that is merely null,
/// unparseable, or not a JSON object is not an error — it decodes to `None`.
#[derive(Debug, thiserror::Error)]
pub enum AttrDecodeError {
    #[error("missing column '{0}'")]
    MissingColumn(String),
    #[error("'{column}' is not an attribute column ({detail})")]
    UnsupportedType { column: String, detail: String },
}

/// Read an attribute column's per-row documents as string key/value maps,
/// handling both storage forms. Null, unparseable, or non-object rows yield
/// `None`, so a malformed row degrades to "no attributes" rather than failing
/// the whole batch.
pub fn attr_documents(
    batch: &RecordBatch,
    name: &str,
) -> Result<Vec<Option<AttrDocument>>, AttrDecodeError> {
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| AttrDecodeError::MissingColumn(name.to_string()))?;

    if let Some(map) = column.as_any().downcast_ref::<MapArray>() {
        let mut out = Vec::with_capacity(map.len());
        for i in 0..map.len() {
            if map.is_null(i) {
                out.push(None);
                continue;
            }
            let entries = map.value(i);
            let keys = entries
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| AttrDecodeError::UnsupportedType {
                    column: name.to_string(),
                    detail: "map keys are not strings".to_string(),
                })?;
            let values = entries
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| AttrDecodeError::UnsupportedType {
                    column: name.to_string(),
                    detail: "map values are not strings".to_string(),
                })?;
            let mut doc = AttrDocument::new();
            for j in 0..entries.len() {
                if !keys.is_null(j) && !values.is_null(j) {
                    doc.insert(keys.value(j).to_string(), values.value(j).to_string());
                }
            }
            out.push(Some(doc));
        }
        return Ok(out);
    }

    let attrs = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| AttrDecodeError::UnsupportedType {
            column: name.to_string(),
            detail: format!("unexpected Arrow type {}", column.data_type()),
        })?;
    let mut out = Vec::with_capacity(attrs.len());
    for i in 0..attrs.len() {
        if attrs.is_null(i) {
            out.push(None);
            continue;
        }
        match serde_json::from_str::<serde_json::Value>(attrs.value(i)) {
            Ok(serde_json::Value::Object(map)) => {
                let doc = map
                    .into_iter()
                    .map(|(k, v)| {
                        // A JSON string renders as its contents; anything else
                        // (number, bool, nested value) as its JSON text, so a
                        // value is never silently dropped.
                        let rendered = match v {
                            serde_json::Value::String(s) => s,
                            other => other.to_string(),
                        };
                        (k, rendered)
                    })
                    .collect();
                out.push(Some(doc));
            }
            _ => out.push(None),
        }
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
    fn map_batch(rows: Vec<Option<Vec<(&str, &str)>>>) -> RecordBatch {
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
            "log_attributes",
            array.data_type().clone(),
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(array) as ArrayRef]).unwrap()
    }

    #[test]
    fn decodes_typed_map_columns() {
        let batch = map_batch(vec![
            Some(vec![("http.method", "GET"), ("http.status", "200")]),
            None,
        ]);
        let docs = attr_documents(&batch, "log_attributes").unwrap();
        assert_eq!(docs[0].as_ref().unwrap()["http.method"], "GET");
        assert_eq!(docs[0].as_ref().unwrap()["http.status"], "200");
        assert!(docs[1].is_none(), "a null map row has no attributes");
    }

    #[test]
    fn decodes_legacy_json_string_columns() {
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

    #[test]
    fn missing_column_is_an_error() {
        let batch = json_batch(vec![Some("{}")]);
        assert!(matches!(
            attr_documents(&batch, "resource_attributes"),
            Err(AttrDecodeError::MissingColumn(_))
        ));
    }
}
