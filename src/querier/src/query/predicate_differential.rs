//! # `Predicate::evaluate` vs. the lowered plan
//!
//! [`query_ir::predicate::Predicate::evaluate`] is documented as "the
//! reference semantics ... the lowered plan must satisfy". This module is
//! that check: for a table-driven list of predicates, it lowers each one
//! through the real path (`ir_planner::plan_document`, same as production)
//! over a small in-memory `logs` table, and separately evaluates the same
//! predicate row-by-row with `Predicate::evaluate`. The two kept-row sets
//! must agree.
//!
//! Modeled on [`super::differential`], reusing its fixture-building helpers
//! and `plan_document` call shape.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray, TimestampNanosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::catalog::{CatalogProvider, MemTable, SchemaProvider};
use datafusion::prelude::SessionContext;

use common::query_ir::{
    ComparisonOp, Document, Leaf, Predicate, Range, Record, ResultEnvelope, Stage,
};

use super::ir_planner::plan_document;

const TENANT: &str = "t";
const DATASET: &str = "d";
const FROM_NS: &str = "0";
const TO_NS: &str = "1000";

// ---------------------------------------------------------------------------
// Fixture: a `logs` table shaped like `differential.rs`'s `logs_fixture`,
// with `log_attributes` entries chosen to cover the tricky cases: an
// explicitly-null attribute value (key present, value NULL — distinct from a
// missing key), a missing key, an empty string, and a numeric-looking string
// value compared against a JSON-number literal.
//
// Row identity for the row-set comparison is the (unique) `timestamp`.
// ---------------------------------------------------------------------------

/// One `log_attributes` entry: `(key, Some(value))` for a present value,
/// `(key, None)` for a key present with a NULL value.
type Attr = (&'static str, Option<&'static str>);

/// (timestamp, body, attributes) — a key omitted from the attribute list is
/// simply not in the map (absent), distinct from `(key, None)` (present, NULL).
const ROWS: &[(i64, &str, &[Attr])] = &[
    (
        10,
        "row0",
        &[
            ("name", Some("apple")),
            ("num", Some("9")),
            ("empty", Some("")),
        ],
    ),
    (20, "row1", &[("name", Some("banana")), ("num", Some("10"))]),
    (30, "row2", &[]),
    (
        40,
        "row3",
        &[("name", Some("apple")), ("num", Some("apple"))],
    ),
    (
        50,
        "row4",
        &[("name", Some("")), ("num", Some("5")), ("empty", Some("x"))],
    ),
    (60, "row5", &[("name", None), ("num", Some("9"))]),
];

fn map_field_named(name: &str) -> Field {
    let entries = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Utf8, true),
        ])),
        false,
    );
    Field::new(name, DataType::Map(Arc::new(entries), false), true)
}

/// Like `differential.rs`'s `build_map`, but a value of `None` appends a
/// present key with a NULL value (rather than omitting the key entirely),
/// so the fixture can carry the "key present, value null" case.
fn build_map(rows: &[&[Attr]]) -> Arc<dyn datafusion::arrow::array::Array> {
    use datafusion::arrow::array::{MapBuilder, MapFieldNames, StringBuilder};
    let names = MapFieldNames {
        entry: "entries".to_string(),
        key: "keys".to_string(),
        value: "values".to_string(),
    };
    let mut b = MapBuilder::new(Some(names), StringBuilder::new(), StringBuilder::new());
    for row in rows {
        for (k, v) in *row {
            b.keys().append_value(k);
            match v {
                Some(s) => b.values().append_value(s),
                None => b.values().append_null(),
            }
        }
        b.append(true).unwrap();
    }
    Arc::new(b.finish())
}

fn register(ctx: &SessionContext, table: &str, schema: Arc<Schema>, batch: RecordBatch) {
    let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    let sp = Arc::new(MemorySchemaProvider::new());
    sp.register_table(table.to_string(), Arc::new(mem)).unwrap();
    let cat = Arc::new(MemoryCatalogProvider::new());
    cat.register_schema(DATASET, sp).unwrap();
    ctx.register_catalog(TENANT, cat);
}

fn logs_fixture() -> SessionContext {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("body", DataType::Utf8, true),
        Field::new("service_name", DataType::Utf8, true),
        Field::new("severity_text", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        map_field_named("log_attributes"),
        map_field_named("resource_attributes"),
    ]));
    let ts: Vec<i64> = ROWS.iter().map(|(t, _, _)| *t).collect();
    let bodies: Vec<&str> = ROWS.iter().map(|(_, b, _)| *b).collect();
    let n = ROWS.len();
    let attrs: Vec<&[Attr]> = ROWS.iter().map(|(_, _, a)| *a).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampNanosecondArray::from(ts)),
            Arc::new(StringArray::from(bodies)),
            Arc::new(StringArray::from(vec![Some("api"); n])),
            Arc::new(StringArray::from(vec![Some("INFO"); n])),
            Arc::new(StringArray::from(vec![Some("t1"); n])),
            Arc::new(StringArray::from(vec![Some("s1"); n])),
            build_map(&attrs),
            build_map(&vec![&[][..]; n]),
        ],
    )
    .unwrap();
    let ctx = SessionContext::new();
    register(&ctx, "logs", schema, batch);
    ctx
}

/// The reference `Record` for each fixture row: a JSON `null` marks the
/// "key present, value null" row (`row5`'s `name`); an omitted key marks a
/// genuinely missing one — both denote *absent* per `Predicate::evaluate`'s
/// doc comment.
fn records() -> Vec<(i64, Record)> {
    ROWS.iter()
        .map(|(ts, _, attrs)| {
            let record: Record = attrs
                .iter()
                .map(|(k, v)| {
                    let value = match v {
                        Some(s) => serde_json::Value::String(s.to_string()),
                        None => serde_json::Value::Null,
                    };
                    (k.to_string(), value)
                })
                .collect();
            (*ts, record)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The two sides
// ---------------------------------------------------------------------------

/// Row timestamps the real planner keeps for `predicate`, via the same
/// `plan_document` call production code (and `differential.rs`) uses.
async fn planned_kept(ctx: &SessionContext, predicate: &Predicate) -> HashSet<i64> {
    let doc = Document {
        ir_version: 1,
        from: "logs".to_string(),
        range: Range {
            from: serde_json::Value::String(FROM_NS.to_string()),
            to: serde_json::Value::String(TO_NS.to_string()),
        },
        result: ResultEnvelope::Rows,
        fields: Some(vec!["timestamp".to_string()]),
        pipeline: vec![Stage::Where(predicate.clone())],
    };
    let (df, _) = plan_document(ctx, &doc, TENANT, DATASET, 0)
        .await
        .expect("document plans")
        .expect("logs table is registered");
    let batches = df.collect().await.expect("plan executes");
    let mut kept = HashSet::new();
    for batch in &batches {
        let ts = batch
            .column_by_name("timestamp")
            .expect("timestamp is projected")
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("timestamp is a nanosecond timestamp column");
        for i in 0..batch.num_rows() {
            kept.insert(ts.value(i));
        }
    }
    kept
}

/// Row timestamps [`Predicate::evaluate`] keeps.
fn evaluated_kept(predicate: &Predicate) -> HashSet<i64> {
    records()
        .into_iter()
        .filter(|(_, record)| predicate.evaluate(record).matches())
        .map(|(ts, _)| ts)
        .collect()
}

fn leaf(field: &str, op: ComparisonOp, value: Option<serde_json::Value>) -> Predicate {
    Predicate::Leaf(Leaf {
        field: field.to_string(),
        op,
        value,
    })
}

// ---------------------------------------------------------------------------
// Table-driven cases
// ---------------------------------------------------------------------------

async fn assert_agrees(name: &str, predicate: &Predicate) {
    let ctx = logs_fixture();
    let planned = planned_kept(&ctx, predicate).await;
    let evaluated = evaluated_kept(predicate);
    assert_eq!(
        planned, evaluated,
        "{name}: planned vs. evaluated kept-row sets diverge\npredicate: {predicate:?}"
    );
}

#[tokio::test]
async fn agrees_on_eq_missing_key_and_null_value_both_absent() {
    // `name = "apple"` — row2 (no key) and row5 (key present, value NULL)
    // must both stay excluded, same as row4's empty string (present, not
    // absent, and not equal).
    let p = leaf("name", ComparisonOp::Eq, Some(serde_json::json!("apple")));
    assert_agrees("eq_missing_and_null_absent", &p).await;
}

#[tokio::test]
async fn agrees_on_ne_also_excludes_absent_rows() {
    // Kleene `not(eq)`: absent (row2, row5) matches neither `eq` nor `ne`.
    let p = leaf("name", ComparisonOp::Ne, Some(serde_json::json!("apple")));
    assert_agrees("ne_excludes_absent", &p).await;
}

#[tokio::test]
async fn agrees_on_eq_against_empty_string() {
    // row4's `name` is present and `""`, distinct from absent.
    let p = leaf("name", ComparisonOp::Eq, Some(serde_json::json!("")));
    assert_agrees("eq_empty_string", &p).await;
}

#[tokio::test]
async fn agrees_on_exists() {
    let p = leaf("name", ComparisonOp::Exists, None);
    assert_agrees("exists", &p).await;
}

#[tokio::test]
async fn agrees_on_not_exists() {
    let p = Predicate::Not(Box::new(leaf("name", ComparisonOp::Exists, None)));
    assert_agrees("not_exists", &p).await;
}

#[tokio::test]
async fn agrees_on_in() {
    let p = leaf(
        "name",
        ComparisonOp::In,
        Some(serde_json::json!(["apple", "banana"])),
    );
    assert_agrees("in", &p).await;
}

#[tokio::test]
async fn agrees_on_and_over_absent() {
    // `name exists AND num = "9"` — row5 fails on `name exists` (absent
    // value), row2 fails on both; only row0 and row3's `num` disagree.
    let p = Predicate::And(vec![
        leaf("name", ComparisonOp::Exists, None),
        leaf("num", ComparisonOp::Eq, Some(serde_json::json!("9"))),
    ]);
    assert_agrees("and_over_absent", &p).await;
}

#[tokio::test]
async fn agrees_on_or_over_absent() {
    let p = Predicate::Or(vec![
        leaf("name", ComparisonOp::Eq, Some(serde_json::json!("banana"))),
        leaf("num", ComparisonOp::Eq, Some(serde_json::json!("9"))),
    ]);
    assert_agrees("or_over_absent", &p).await;
}

#[tokio::test]
async fn agrees_on_not_over_and_with_absent() {
    // `not(name exists AND num = "9")` — De Morgan under Kleene logic still
    // needs the lowered plan's NULL handling to coincide with `Truth::and`
    // then `!`, not classical two-valued negation.
    let p = Predicate::Not(Box::new(Predicate::And(vec![
        leaf("name", ComparisonOp::Exists, None),
        leaf("num", ComparisonOp::Eq, Some(serde_json::json!("9"))),
    ])));
    assert_agrees("not_over_and_with_absent", &p).await;
}

// ---------------------------------------------------------------------------
// Known divergences — every other case above must pass; these are reported,
// not silently routed around (see this module's doc comment and the task
// that added it).
// ---------------------------------------------------------------------------

/// **Known divergence — regex.** `Predicate::evaluate`'s reference evaluator
/// deliberately never runs the pattern (see `eval_leaf`'s `Regex` arm: "The
/// reference evaluator does not run regexes ... real matching happens in the
/// lowered plan", returning `Truth::False` unconditionally for a
/// string/string regex leaf). The lowered plan runs the real
/// `regexp_like` UDF and matches `row0`'s body. This is a documented,
/// deliberate approximation in the reference evaluator, not a planner bug —
/// left `#[ignore]`d rather than papered over.
#[tokio::test]
#[ignore = "known divergence: Predicate::evaluate never runs regex (see eval_leaf's Regex arm doc); the lowered plan does"]
async fn regex_diverges_from_the_reference_evaluators_documented_approximation() {
    let p = leaf(
        "body",
        ComparisonOp::Regex,
        Some(serde_json::json!("^row0$")),
    );
    assert_agrees("regex_documented_divergence", &p).await;
}

/// **Known divergence — numeric literal against an untyped string
/// attribute.** An attribute field with no declared logical type resolves to
/// `ValueType::String` (`SchemaResolver::resolve`'s fallback), so a JSON
/// *number* literal is coerced to its decimal string form
/// (`query_ir::value::coerce`'s `ValueType::String` arm) and the plan does a
/// **lexicographic** string comparison against the (also string) attribute
/// value: `"9" > "10"` lexicographically, so row0/row5 (`num = "9"`) pass
/// `num > 10`. `Predicate::evaluate`'s `cmp_json`, given a JSON `String`
/// actual and a JSON `Number` expected, returns `None` (no defined ordering
/// between mismatched JSON types) — `Truth::False` for every row, never
/// matching. Both sides are internally consistent; they disagree because an
/// untyped attribute's comparison type depends on how the *literal* was
/// spelled in the query (a JSON number vs. a JSON string), which the
/// reference evaluator has no equivalent concept of. Left `#[ignore]`d and
/// reported rather than changing planner behaviour.
#[tokio::test]
#[ignore = "known divergence: an untyped attribute's numeric-literal comparison is string-lexicographic in the plan but always False in Predicate::evaluate (mismatched JSON types) — see this test's doc comment"]
async fn numeric_literal_against_string_attribute_diverges() {
    let p = leaf("num", ComparisonOp::Gt, Some(serde_json::json!(10)));
    assert_agrees("numeric_vs_string_attribute_divergence", &p).await;
}
