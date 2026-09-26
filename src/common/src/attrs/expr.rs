//! Expression- and projection-level compat helpers for LogQL/PromQL/
//! TraceQL/Tempo (not the Query IR planner, which carries its own compat
//! path in `ir_planner`/`differential`). `schema: None` means "assume the
//! legacy layout" — a caller with no schema in hand (e.g. a differential
//! test) keeps today's `get_field` behavior unchanged.

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::functions::core::expr_fn::{coalesce, get_field};
use datafusion::logical_expr::{Expr, cast, col};
use datafusion::prelude::ident;

use crate::schema::type_authority::CanonicalType;
use crate::schema::typed_attributes::{has_typed_container, home_column, typed_columns};

/// Whether `container` is stored in the typed layout in `schema` (its
/// residue column is present) rather than the legacy single-map layout.
pub fn is_typed_layout(schema: &Schema, container: &str) -> bool {
    has_typed_container(schema.fields().iter().map(|f| f.name().as_str()), container)
}

/// The compatibility string expression for `key` in `container`: the
/// legacy `get_field` extraction, or a coalesce over the four typed homes
/// (int/double/bool cast to `Utf8`) when `schema` shows `container` has
/// been rewritten onto the typed layout. Matches what the legacy writer
/// stored on the wire (`"200"`, `"true"`, `"1.5"`).
pub fn compat_attr_expr(schema: Option<&Schema>, container: &str, key: &str) -> Expr {
    if !schema.is_some_and(|schema| is_typed_layout(schema, container)) {
        return get_field(col(container), key);
    }
    typed_compat_attr_expr(container, key)
}

/// [`compat_attr_expr`]'s typed-layout branch, callable directly by a caller
/// that already knows `container_col` is on the typed layout — e.g. the IR
/// planner, which builds `container_col` from a column *identifier* (via
/// [`ident`], not [`col`]) so a `parent.`-qualified container name (which
/// contains a `.` that must not be parsed as a table qualifier) still
/// addresses the right physical column.
pub fn typed_compat_attr_expr(container_col: &str, key: &str) -> Expr {
    let home =
        |canonical: CanonicalType| get_field(ident(home_column(container_col, canonical)), key);
    coalesce(vec![
        home(CanonicalType::String),
        cast(home(CanonicalType::Int64), DataType::Utf8),
        cast(home(CanonicalType::Float64), DataType::Utf8),
        cast(home(CanonicalType::Bool), DataType::Utf8),
    ])
}

/// The column names to project for `containers`: each container itself on
/// the legacy layout, or its five typed columns on the typed layout. For a
/// caller that hands the projected batch to a decoder (e.g.
/// `attrs::attr_documents`) that already reads whichever layout is present.
pub fn select_columns_for_containers(schema: Option<&Schema>, containers: &[&str]) -> Vec<String> {
    containers
        .iter()
        .flat_map(|&container| match schema {
            Some(schema) if is_typed_layout(schema, container) => typed_columns(container).to_vec(),
            _ => vec![container.to_string()],
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{typed_attribute_columns, typed_attribute_columns_from};
    use datafusion::arrow::array::{Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::Fields;
    use datafusion::prelude::SessionContext;
    use serde_json::json;
    use std::sync::Arc;

    /// A legacy `logs` `physical-v4`-shaped schema: `log_attributes` is a
    /// plain `Map<Utf8,Utf8>` column, no typed homes or residue.
    fn legacy_logs_schema() -> Schema {
        let entries = datafusion::arrow::datatypes::Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                datafusion::arrow::datatypes::Field::new("keys", DataType::Utf8, false),
                datafusion::arrow::datatypes::Field::new("values", DataType::Utf8, true),
            ])),
            false,
        );
        Schema::new(vec![datafusion::arrow::datatypes::Field::new(
            "log_attributes",
            DataType::Map(Arc::new(entries), false),
            true,
        )])
    }

    #[test]
    fn legacy_layout_extracts_get_field_on_the_container_column() {
        let schema = legacy_logs_schema();
        assert!(!is_typed_layout(&schema, "log_attributes"));
        let expr = compat_attr_expr(Some(&schema), "log_attributes", "http.route");
        assert_eq!(
            expr.to_string(),
            r#"get_field(log_attributes, Utf8("http.route"))"#
        );
        // No schema at all is the same as the legacy layout.
        assert_eq!(expr, compat_attr_expr(None, "log_attributes", "http.route"));
    }

    #[test]
    fn typed_layout_coalesces_the_four_typed_homes() {
        let (fields, _) = typed_attribute_columns("span_attributes", &[]);
        let schema = Schema::new(fields.to_vec());
        assert!(is_typed_layout(&schema, "span_attributes"));
        let expr = compat_attr_expr(Some(&schema), "span_attributes", "http.status_code");
        let text = expr.to_string();
        assert!(text.starts_with("coalesce("), "{text}");
        assert!(text.contains("get_field(span_attributes_str"), "{text}");
        assert!(
            text.contains("CAST(get_field(span_attributes_int") && text.contains("AS Utf8"),
            "{text}"
        );
        assert!(text.contains("get_field(span_attributes_double"), "{text}");
        assert!(text.contains("get_field(span_attributes_bool"), "{text}");
    }

    /// [`typed_compat_attr_expr`] is built from a column *identifier*, not
    /// `col()` — a `parent.`-qualified container name (containing a `.` that
    /// must not be parsed as a table qualifier) still addresses its own
    /// typed home columns, exactly like the unprefixed case.
    #[test]
    fn typed_compat_attr_expr_keeps_a_dotted_prefix_as_one_identifier() {
        let expr = typed_compat_attr_expr("parent.span_attributes", "http.status_code");
        let text = expr.to_string();
        assert!(
            text.contains("get_field(parent.span_attributes_str"),
            "{text}"
        );
        assert!(
            text.contains("CAST(get_field(parent.span_attributes_int") && text.contains("AS Utf8"),
            "{text}"
        );
    }

    #[test]
    fn select_columns_for_containers_picks_the_layout_columns() {
        let legacy = legacy_logs_schema();
        assert_eq!(
            select_columns_for_containers(Some(&legacy), &["log_attributes"]),
            vec!["log_attributes".to_string()]
        );
        assert_eq!(
            select_columns_for_containers(None, &["log_attributes"]),
            vec!["log_attributes".to_string()]
        );

        let (fields, _) = typed_attribute_columns("span_attributes", &[]);
        let typed = Schema::new(fields.to_vec());
        assert_eq!(
            select_columns_for_containers(Some(&typed), &["span_attributes"]),
            typed_columns("span_attributes").to_vec()
        );
    }

    /// Evaluates `compat_attr_expr` against a one-row, in-memory typed
    /// `span_attributes` batch, proving the string rendering end to end:
    /// an int key filters as its decimal string, a string key as-is, a
    /// bool key as `"true"`, and a missing key as null.
    #[tokio::test]
    async fn typed_layout_renders_each_home_as_the_legacy_string_form() {
        let row = Some(serde_json::Map::from_iter([
            ("s".to_string(), json!("hello")),
            ("i".to_string(), json!(200)),
            ("d".to_string(), json!(1.5)),
            ("b".to_string(), json!(true)),
        ]));
        let (fields, arrays) = typed_attribute_columns("span_attributes", &[row]);

        let schema = Arc::new(Schema::new(fields.to_vec()));
        let batch = RecordBatch::try_new(schema.clone(), arrays.to_vec())
            .expect("build one-row typed span_attributes batch");

        let ctx = SessionContext::new();
        ctx.register_batch("span_attributes", batch)
            .expect("register batch as a table");
        let df = ctx
            .table("span_attributes")
            .await
            .expect("scan the registered table");

        for (key, expected) in [
            ("s", Some("hello")),
            ("i", Some("200")),
            ("d", Some("1.5")),
            ("b", Some("true")),
            ("missing", None),
        ] {
            let expr = compat_attr_expr(Some(schema.as_ref()), "span_attributes", key).alias("v");
            let result = df
                .clone()
                .select(vec![expr])
                .expect("project compat expr")
                .collect()
                .await
                .expect("collect");
            let column = result[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8 result column");
            let actual = (!column.is_null(0)).then(|| column.value(0));
            assert_eq!(actual, expected, "key {key}");
        }
    }

    /// `typed_attribute_columns_from` extends the fixture to a different
    /// table/version (`logs` `physical-v4` here) without duplicating the
    /// fixture itself.
    #[test]
    fn typed_attribute_columns_from_resolves_a_non_default_table_and_version() {
        let (fields, _) =
            typed_attribute_columns_from("logs", "physical-v4", "log_attributes", &[]);
        assert_eq!(fields[0].name(), "log_attributes_str");
    }
}
