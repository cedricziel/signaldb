//! Typed-attribute-layout fixture shared by `common`, `querier`, and
//! `router` tests: builds one container's typed columns from JSON rows,
//! using a real schema's Arrow fields.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{Field, Fields, Schema};
use serde_json::{Map, Value as JsonValue};

use crate::attrs::json_documents;
use crate::attrs::typed::TypedAttrBuilder;
use crate::schema::SCHEMA_DEFINITIONS;
use crate::schema::type_authority::{CanonicalType, ObservedKind, Placement};
use crate::schema::typed_attributes;

/// `container`'s typed fields from `table`'s `version` schema, e.g.
/// `("traces", "physical-v5")` or `("logs", "physical-v4")`.
fn typed_attribute_fields_from(table: &str, version: &str, container: &str) -> [Field; 5] {
    let resolved = match table {
        "traces" => SCHEMA_DEFINITIONS.resolve_trace_schema(version),
        "logs" => SCHEMA_DEFINITIONS.resolve_log_schema(version),
        "metrics_gauge" => {
            SCHEMA_DEFINITIONS.resolve_table_schema(&SCHEMA_DEFINITIONS.metrics_gauge, version)
        }
        "metrics_sum" => {
            SCHEMA_DEFINITIONS.resolve_table_schema(&SCHEMA_DEFINITIONS.metrics_sum, version)
        }
        other => panic!("unsupported table '{other}' in the typed-attribute test fixture"),
    }
    .unwrap_or_else(|_| panic!("{table} {version} schema resolves"));
    let arrow_fields: Fields = resolved
        .to_iceberg_schema()
        .expect("schema converts to Iceberg")
        .fields()
        .try_into()
        .expect("Iceberg fields convert to Arrow");
    typed_attributes::typed_columns(container).map(|name| {
        arrow_fields
            .iter()
            .find(|f| f.name() == &name)
            .unwrap_or_else(|| panic!("missing field {name} in {table} {version} schema"))
            .as_ref()
            .clone()
    })
}

/// A scalar goes to its canonical-type home; arrays, objects, and nulls fall
/// to residue.
fn default_placement(_key: &str, observed: ObservedKind) -> Placement {
    match observed {
        ObservedKind::String => Placement::Home(CanonicalType::String),
        ObservedKind::Int64 => Placement::Home(CanonicalType::Int64),
        ObservedKind::Float64 => Placement::Home(CanonicalType::Float64),
        ObservedKind::Bool => Placement::Home(CanonicalType::Bool),
        _ => Placement::Residue { off_type: false },
    }
}

/// Builds `container`'s five typed-attribute fields and arrays from `rows`
/// (`None` is a null row), placed with [`default_placement`]. A caller
/// appends the result onto its own batch schema alongside the columns under
/// test. Uses the real `traces` `physical-v5` schema; see
/// [`typed_attribute_columns_from`] for another table/version.
pub fn typed_attribute_columns(
    container: &str,
    rows: &[Option<Map<String, JsonValue>>],
) -> ([Field; 5], [ArrayRef; 5]) {
    typed_attribute_columns_from("traces", "physical-v5", container, rows)
}

/// [`typed_attribute_columns`], but against `table`'s `version` schema
/// (e.g. `("logs", "physical-v4")`) instead of the default `traces`
/// `physical-v5`.
pub fn typed_attribute_columns_from(
    table: &str,
    version: &str,
    container: &str,
    rows: &[Option<Map<String, JsonValue>>],
) -> ([Field; 5], [ArrayRef; 5]) {
    let fields = typed_attribute_fields_from(table, version, container);
    let mut builder = TypedAttrBuilder::new(&fields).expect("typed attribute builder");
    for row in rows {
        builder
            .append_row(row.as_ref(), default_placement)
            .expect("append typed attribute row");
    }
    let arrays = builder.finish().expect("finish typed attribute builder");
    (fields, arrays)
}

/// Rewrites `batch`'s `containers` (each a legacy `Map<Utf8,Utf8>` or
/// JSON-string column) onto the typed layout, using `table`'s `version`
/// schema for the typed field shapes — column order is otherwise preserved.
/// Lets a fixture written once against the legacy layout also exercise the
/// typed layout, for a test that asserts the two layouts behave
/// identically, without duplicating the row data.
pub fn to_typed_layout(
    table: &str,
    version: &str,
    batch: &RecordBatch,
    containers: &[&str],
) -> RecordBatch {
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for field in batch.schema().fields() {
        let name = field.name().as_str();
        if containers.contains(&name) {
            let rows = json_documents(batch, name);
            let (typed_fields, typed_arrays) =
                typed_attribute_columns_from(table, version, name, &rows);
            fields.extend(typed_fields);
            columns.extend(typed_arrays);
        } else {
            fields.push(field.as_ref().clone());
            columns.push(
                batch
                    .column_by_name(name)
                    .expect("field is in schema")
                    .clone(),
            );
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .expect("rebuild the batch onto the typed layout")
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{MapBuilder, MapFieldNames, StringBuilder};
    use datafusion::arrow::datatypes::DataType;

    fn map_field_named(name: &str) -> Field {
        let entries = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("keys", DataType::Utf8, false),
                    Field::new("values", DataType::Utf8, true),
                ]
                .into(),
            ),
            false,
        );
        Field::new(name, DataType::Map(Arc::new(entries), false), true)
    }

    fn build_map(pairs: &[&[(&str, &str)]]) -> ArrayRef {
        let names = MapFieldNames {
            entry: "entries".to_string(),
            key: "keys".to_string(),
            value: "values".to_string(),
        };
        let mut b = MapBuilder::new(Some(names), StringBuilder::new(), StringBuilder::new());
        for row in pairs {
            for (k, v) in *row {
                b.keys().append_value(k);
                b.values().append_value(v);
            }
            b.append(true).unwrap();
        }
        Arc::new(b.finish())
    }

    /// Rewriting a legacy `Map<Utf8,Utf8>` container onto the typed layout
    /// keeps every other column untouched and preserves the same key/value
    /// pairs, now spread across the typed homes.
    #[test]
    fn to_typed_layout_preserves_columns_and_attribute_values() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            map_field_named("span_attributes"),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::StringArray::from(vec![
                    "t0", "t1",
                ])),
                build_map(&[&[("http.route", "/a")], &[]]),
            ],
        )
        .unwrap();

        let typed = to_typed_layout("traces", "physical-v5", &batch, &["span_attributes"]);
        assert_eq!(
            typed.schema().field(0).name(),
            "trace_id",
            "the untouched column keeps its place"
        );
        assert!(
            typed
                .schema()
                .field_with_name("span_attributes_str")
                .is_ok(),
            "the container is now the typed layout"
        );

        let rows = json_documents(&typed, "span_attributes");
        assert_eq!(
            rows[0].as_ref().and_then(|r| r.get("http.route")),
            Some(&JsonValue::String("/a".to_string()))
        );
        assert_eq!(rows[1], Some(Map::new()));
    }
}
