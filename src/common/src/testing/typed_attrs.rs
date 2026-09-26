//! Typed-attribute-layout fixture shared by `common`, `querier`, and
//! `router` tests: builds one container's typed columns from JSON rows,
//! using a real schema's Arrow fields.

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{Field, Fields};
use serde_json::{Map, Value as JsonValue};

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
