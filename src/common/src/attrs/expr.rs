//! Expression- and projection-level compat helpers for LogQL/PromQL/
//! TraceQL/Tempo (not the Query IR planner, which carries its own compat
//! path in `ir_planner`/`differential`). The typed layout is the only
//! attribute layout these build expressions for; `schema` is accepted only
//! to assert that precondition in debug builds (`None` skips the check, for
//! a caller with no schema in hand, e.g. a differential test).

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::functions::core::expr_fn::{coalesce, get_field};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, cast, lit};
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
    let mut parts = typed_home_parts(homes, promoted, key, prefix);
    if parts.len() == 1 {
        parts.remove(0)
    } else {
        coalesce(parts)
    }
}

/// The flat list [`typed_home_expr`] coalesces: `promoted[i]`'s ident (when
/// present) before `homes[i]`'s `get_field`, in home order.
fn typed_home_parts(
    homes: &[String],
    promoted: &[Option<String>],
    key: &str,
    prefix: &str,
) -> Vec<Expr> {
    let mut parts: Vec<Expr> = Vec::with_capacity(homes.len() * 2);
    for (i, home) in homes.iter().enumerate() {
        if let Some(label) = promoted.get(i).and_then(Option::as_deref) {
            parts.push(ident(format!("{prefix}{label}")));
        }
        parts.push(get_field(ident(format!("{prefix}{home}")), key));
    }
    parts
}

/// A typed attribute filter comparison (`= != < <= > >=`), lowered to give
/// DataFusion a prunable disjunct on a promoted column instead of hiding it
/// behind a coalesce. When at least one `promoted[i]` is `Some`, rewrites
/// `coalesce(parts...) op literal` as `(x1 IS NOT NULL AND x1 op literal) OR
/// (x1 IS NULL AND R(rest))`, recursively — exactly [`typed_home_expr`]'s
/// coalesce under SQL three-valued logic, so `NOT`/`!=` stay correct, but
/// each part's own nullness is now a directly prunable predicate. With no
/// promoted column, returns the plain `typed_home_expr(...) op literal` form
/// unchanged (the shape the warm-index probe recognizes).
pub fn typed_home_filter_expr(
    homes: &[String],
    promoted: &[Option<String>],
    key: &str,
    prefix: &str,
    op: Operator,
    literal: Expr,
) -> Expr {
    if !promoted.iter().any(Option::is_some) {
        return binary(typed_home_expr(homes, promoted, key, prefix), op, literal);
    }
    coalesce_op_expr(&typed_home_parts(homes, promoted, key, prefix), op, literal)
}

fn coalesce_op_expr(parts: &[Expr], op: Operator, literal: Expr) -> Expr {
    match parts.split_first() {
        None => binary(lit(ScalarValue::Utf8(None)), op, literal),
        Some((head, [])) => binary(head.clone(), op, literal),
        Some((head, rest)) => head
            .clone()
            .is_not_null()
            .and(binary(head.clone(), op, literal.clone()))
            .or(head
                .clone()
                .is_null()
                .and(coalesce_op_expr(rest, op, literal))),
    }
}

fn binary(lhs: Expr, op: Operator, rhs: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(Box::new(lhs), op, Box::new(rhs)))
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
    use datafusion::logical_expr::not;
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
    fn typed_home_filter_expr_keeps_the_plain_coalesce_form_without_a_promoted_column() {
        let one_home = vec!["span_attributes_int".to_string()];
        let expr =
            typed_home_filter_expr(&one_home, &[None], "status", "", Operator::Eq, lit(200i64));
        assert_eq!(
            expr.to_string(),
            typed_home_expr(&one_home, &[None], "status", "")
                .eq(lit(200i64))
                .to_string()
        );

        let two_homes = vec![
            "span_attributes_int".to_string(),
            "resource_attributes_int".to_string(),
        ];
        let expr = typed_home_filter_expr(
            &two_homes,
            &[None, None],
            "status",
            "",
            Operator::Eq,
            lit(200i64),
        );
        assert_eq!(
            expr.to_string(),
            typed_home_expr(&two_homes, &[None, None], "status", "")
                .eq(lit(200i64))
                .to_string()
        );
    }

    #[test]
    fn typed_home_filter_expr_rewrites_to_a_disjunction_when_promoted() {
        let one_home = vec!["span_attributes_int".to_string()];
        let promoted_one = vec![Some("attr_record_status".to_string())];
        let text = typed_home_filter_expr(
            &one_home,
            &promoted_one,
            "status",
            "",
            Operator::Eq,
            lit(200i64),
        )
        .to_string();
        assert!(text.contains("attr_record_status IS NOT NULL"), "{text}");
        assert!(text.contains("attr_record_status = Int64(200)"), "{text}");
        assert!(text.contains("attr_record_status IS NULL"), "{text}");
        assert!(
            text.contains("get_field(span_attributes_int") && text.contains("= Int64(200)"),
            "{text}"
        );

        let two_homes = vec![
            "span_attributes_str".to_string(),
            "resource_attributes_str".to_string(),
        ];
        let promoted_two = vec![Some("attr_record_host".to_string()), None];
        let text = typed_home_filter_expr(
            &two_homes,
            &promoted_two,
            "host",
            "",
            Operator::Eq,
            lit("a"),
        )
        .to_string();
        // Three parts total (the promoted ident plus both homes) fold into
        // two nested disjunctions, one per fallback step (the last part is
        // the recursion base case: no null check, just the comparison).
        assert_eq!(text.matches(" OR ").count(), 2, "{text}");
        assert_eq!(text.matches("IS NOT NULL").count(), 2, "{text}");
    }

    /// The OR-rewrite matches the coalesce form's SQL three-valued-logic
    /// truth table row for row, including where both sides are NULL — across
    /// (promoted non-null), (promoted null, home non-null), and (both null).
    #[tokio::test]
    async fn typed_home_filter_expr_matches_the_coalesce_form_row_for_row() {
        use datafusion::arrow::array::{BooleanArray, Int64Array};

        let promoted_field =
            datafusion::arrow::datatypes::Field::new("attr_record_status", DataType::Int64, true);
        let promoted_array: ArrayRef = Arc::new(Int64Array::from(vec![Some(200i64), None, None]));

        let rows = [
            Some(serde_json::Map::from_iter([(
                "status".to_string(),
                json!(999),
            )])),
            Some(serde_json::Map::from_iter([(
                "status".to_string(),
                json!(200),
            )])),
            None,
        ];
        let (home_fields, home_arrays) = typed_attribute_columns("span_attributes", &rows);

        let mut fields = vec![promoted_field];
        fields.extend(home_fields);
        let mut arrays = vec![promoted_array];
        arrays.extend(home_arrays);
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays).expect("build the fixture batch");

        let ctx = SessionContext::new();
        ctx.register_batch("t", batch).unwrap();
        let df = ctx.table("t").await.unwrap();

        let homes = vec!["span_attributes_int".to_string()];
        let promoted = vec![Some("attr_record_status".to_string())];
        for op in [Operator::Eq, Operator::NotEq] {
            let coalesce_form = super::binary(
                typed_home_expr(&homes, &promoted, "status", ""),
                op,
                lit(200i64),
            );
            let rewritten =
                typed_home_filter_expr(&homes, &promoted, "status", "", op, lit(200i64));
            for (name, expr) in [
                ("plain", coalesce_form.clone()),
                ("negated", not(coalesce_form.clone())),
            ] {
                let rewritten_expr = if name == "negated" {
                    not(rewritten.clone())
                } else {
                    rewritten.clone()
                };
                let result = df
                    .clone()
                    .select(vec![
                        expr.clone().alias("coalesce_form"),
                        rewritten_expr.alias("rewritten"),
                    ])
                    .expect("project both forms")
                    .collect()
                    .await
                    .expect("collect");
                let batch = &result[0];
                let coalesce_col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("Boolean coalesce column");
                let rewritten_col = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("Boolean rewritten column");
                for i in 0..coalesce_col.len() {
                    assert_eq!(
                        coalesce_col.is_null(i),
                        rewritten_col.is_null(i),
                        "{op:?} {name} row {i} nullness"
                    );
                    if !coalesce_col.is_null(i) {
                        assert_eq!(
                            coalesce_col.value(i),
                            rewritten_col.value(i),
                            "{op:?} {name} row {i} value"
                        );
                    }
                }
            }
        }
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
