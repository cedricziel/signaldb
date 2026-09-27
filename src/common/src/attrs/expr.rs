//! Expression- and projection-level compat helpers for LogQL/PromQL/
//! TraceQL/Tempo (not the Query IR planner, which carries its own compat
//! path in `ir_planner`/`differential`). The typed layout is the only
//! attribute layout these build expressions for; `schema` is accepted only
//! to assert that precondition in debug builds (`None` skips the check, for
//! a caller with no schema in hand, e.g. a differential test).

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::functions::core::expr_fn::{coalesce, get_field};
use datafusion::logical_expr::{Expr, cast, lit};
use datafusion::prelude::ident;
use datafusion::scalar::ScalarValue;

use crate::schema::type_authority::CanonicalType;
use crate::schema::typed_attributes::{has_typed_container, home_column, typed_columns};

/// Whether `container` is stored in the typed layout in `schema` (its
/// residue column is present) rather than the legacy single-map layout.
pub fn is_typed_layout(schema: &Schema, container: &str) -> bool {
    has_typed_container(schema.fields().iter().map(|f| f.name().as_str()), container)
}

/// The compatibility string expression for `key` in `container`: a coalesce
/// over the four typed homes (int/double/bool cast to `Utf8`). Matches what
/// the legacy writer stored on the wire (`"200"`, `"true"`, `"1.5"`). When
/// `schema` is given, asserts (debug builds only) that `container` is
/// actually on the typed layout — a caller with no schema in hand skips the
/// check.
pub fn compat_attr_expr(schema: Option<&Schema>, container: &str, key: &str) -> Expr {
    debug_assert!(
        schema.is_none_or(|schema| is_typed_layout(schema, container)),
        "container '{container}' must be on the typed attribute layout"
    );
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

/// The IR's typed-attribute read for a resolved typed-home reference:
/// `get_field(key)` on each home column, coalesced same-typed (no cast —
/// every home in `homes` is expected to share one canonical type). `promoted`
/// runs parallel to `homes`: `promoted[i]`, when `Some`, is a redundant typed
/// copy of `homes[i]`'s value (a per-level `attr_<level>_<key>` column, or a
/// single-level legacy `label_<key>` column) checked before that home, so the
/// flat read is `coalesce(p1, get_field(home1, key), p2, get_field(home2,
/// key), ...)` with any absent `p_i` omitted. `homes` empty reads as a typed
/// NULL rather than an error. `prefix` addresses `<prefix><home>` — built
/// with [`ident`], never [`col`], so a dotted prefix (e.g. a `parent.`-scoped
/// reference) stays one identifier rather than a table qualifier.
pub fn typed_home_expr(
    homes: &[String],
    promoted: &[Option<String>],
    key: &str,
    prefix: &str,
) -> Expr {
    if homes.is_empty() {
        return lit(ScalarValue::Utf8(None));
    }
    let mut parts: Vec<Expr> = Vec::with_capacity(homes.len() * 2);
    for (i, home) in homes.iter().enumerate() {
        if let Some(label) = promoted.get(i).and_then(Option::as_deref) {
            parts.push(ident(format!("{prefix}{label}")));
        }
        parts.push(get_field(ident(format!("{prefix}{home}")), key));
    }
    if parts.len() == 1 {
        parts.remove(0)
    } else {
        coalesce(parts)
    }
}

/// The column names to project for `columns`: each entry expands to its
/// five typed columns when `schema` shows it is an attribute container (or
/// unconditionally, when no `schema` is given); every other entry passes
/// through unchanged.
pub fn select_columns_for_containers(schema: Option<&Schema>, columns: &[&str]) -> Vec<String> {
    columns
        .iter()
        .flat_map(|&c| match schema {
            Some(schema) if !is_typed_layout(schema, c) => vec![c.to_string()],
            _ => typed_columns(c).to_vec(),
        })
        .collect()
}

/// Projects `df` onto `columns`, expanding any attribute container name in
/// it to its typed columns via [`select_columns_for_containers`] first.
pub fn select_attr_columns(
    df: datafusion::dataframe::DataFrame,
    columns: &[&str],
) -> datafusion::error::Result<datafusion::dataframe::DataFrame> {
    let projected = select_columns_for_containers(Some(df.schema().as_arrow()), columns);
    df.select_columns(&projected.iter().map(String::as_str).collect::<Vec<_>>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{typed_attribute_columns, typed_attribute_columns_from};
    use datafusion::arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
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
    fn is_typed_layout_detects_the_residue_column() {
        let legacy = legacy_logs_schema();
        assert!(!is_typed_layout(&legacy, "log_attributes"));
        let (fields, _) = typed_attribute_columns("span_attributes", &[]);
        let typed = Schema::new(fields.to_vec());
        assert!(is_typed_layout(&typed, "span_attributes"));
    }

    #[test]
    fn compat_attr_expr_reads_the_typed_layout_with_or_without_a_schema() {
        let expr = compat_attr_expr(None, "span_attributes", "http.status_code");
        assert_eq!(
            expr,
            typed_compat_attr_expr("span_attributes", "http.status_code")
        );

        let (fields, _) = typed_attribute_columns("span_attributes", &[]);
        let typed = Schema::new(fields.to_vec());
        assert_eq!(
            compat_attr_expr(Some(&typed), "span_attributes", "http.status_code"),
            expr
        );
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
    fn typed_home_expr_reads_directly_when_there_is_exactly_one_home() {
        let homes = vec!["span_attributes_int".to_string()];
        let expr = typed_home_expr(&homes, &[None], "status", "");
        assert_eq!(
            expr.to_string(),
            r#"get_field(span_attributes_int, Utf8("status"))"#
        );
    }

    #[test]
    fn typed_home_expr_coalesces_multiple_homes_with_each_home_s_promoted_column_first() {
        let homes = vec![
            "span_attributes_str".to_string(),
            "resource_attributes_str".to_string(),
        ];
        let promoted = vec![Some("attr_record_host".to_string()), None];
        let text = typed_home_expr(&homes, &promoted, "host", "").to_string();
        assert!(text.starts_with("coalesce("), "{text}");
        assert!(text.contains("attr_record_host"));
        assert!(
            text.find("attr_record_host") < text.find("span_attributes_str"),
            "{text}"
        );
        assert!(text.contains("resource_attributes_str"));
        assert!(!text.contains("attr_resource_host"), "{text}");
    }

    #[test]
    fn typed_home_expr_reads_typed_null_when_no_home_is_committed() {
        let expr = typed_home_expr(&[], &[], "status", "");
        assert_eq!(expr, lit(ScalarValue::Utf8(None)));
    }

    #[test]
    fn typed_home_expr_keeps_a_dotted_prefix_as_one_identifier() {
        let homes = vec!["span_attributes_str".to_string()];
        let text = typed_home_expr(&homes, &[None], "host", "parent.").to_string();
        assert!(
            text.contains("get_field(parent.span_attributes_str"),
            "{text}"
        );
    }

    #[test]
    fn select_columns_for_containers_returns_the_typed_columns() {
        assert_eq!(
            select_columns_for_containers(None, &["log_attributes"]),
            typed_columns("log_attributes").to_vec()
        );

        let (fields, _) = typed_attribute_columns("span_attributes", &[]);
        let typed = Schema::new(fields.to_vec());
        assert_eq!(
            select_columns_for_containers(Some(&typed), &["span_attributes"]),
            typed_columns("span_attributes").to_vec()
        );
    }

    #[test]
    fn select_columns_for_containers_passes_non_container_names_through() {
        let (fields, _) = typed_attribute_columns("span_attributes", &[]);
        let mut all_fields = fields.to_vec();
        all_fields.push(datafusion::arrow::datatypes::Field::new(
            "trace_id",
            DataType::Utf8,
            true,
        ));
        let schema = Schema::new(all_fields);
        assert_eq!(
            select_columns_for_containers(Some(&schema), &["trace_id", "span_attributes"]),
            [
                vec!["trace_id".to_string()],
                typed_columns("span_attributes").to_vec()
            ]
            .concat()
        );
    }

    #[tokio::test]
    async fn select_attr_columns_projects_containers_and_leaves_the_rest() {
        let (fields, arrays) = typed_attribute_columns("span_attributes", &[None]);
        let mut all_fields = fields.to_vec();
        all_fields.push(datafusion::arrow::datatypes::Field::new(
            "trace_id",
            DataType::Utf8,
            true,
        ));
        let mut all_arrays: Vec<ArrayRef> = arrays.to_vec();
        all_arrays.push(Arc::new(StringArray::from(vec!["t1"])) as ArrayRef);
        let schema = Arc::new(Schema::new(all_fields));
        let batch = RecordBatch::try_new(schema.clone(), all_arrays).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("spans", batch).unwrap();
        let df = ctx.table("spans").await.unwrap();

        let projected = select_attr_columns(df, &["trace_id", "span_attributes"]).unwrap();
        let names: Vec<String> = projected
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(
            names,
            [
                vec!["trace_id".to_string()],
                typed_columns("span_attributes").to_vec()
            ]
            .concat()
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
