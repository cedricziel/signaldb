//! `irVersion` 9: the `count_distinct` aggregate.
//!
//! One addition, only to `aggregate`: `fn: "count_distinct"` — an approximate
//! (DataFusion `approx_distinct`, HyperLogLog) count of distinct non-null
//! values of an `of` field. It accepts `string`/`int64`/`bool`/`timestamp`
//! operands (`approx_distinct` rejects floating point, and every other type
//! along with it) and always outputs `int64`.

use query_ir::{Document, IrError, SourceRegistry, ValueType, validate};
use serde_json::json;

/// A document with one aggregate, at the given version.
fn doc(ir_version: i64, agg: serde_json::Value) -> Document {
    serde_json::from_value(json!({
        "irVersion": ir_version,
        "from": "logs",
        "range": { "from": "now-1h", "to": "now" },
        "result": "table",
        "pipeline": [{ "aggregate": { "by": [], "aggs": [agg] } }],
    }))
    .expect("document parses")
}

fn check(ir_version: i64, agg: serde_json::Value) -> Result<(), IrError> {
    let d = doc(ir_version, agg);
    validate(&d, &SourceRegistry::core(), &resolver()).map(|_| ())
}

/// A resolver with one column of each candidate operand type.
fn resolver() -> query_ir::InMemoryResolver {
    query_ir::InMemoryResolver::new()
        .with_column("logs", "session.id", "session_id", ValueType::String)
        .with_column("logs", "retry.count", "retry_count", ValueType::Int64)
        .with_column("logs", "is.sampled", "is_sampled", ValueType::Bool)
        .with_column("logs", "seen.at", "seen_at", ValueType::TimestampNs)
        .with_column("logs", "duration", "duration", ValueType::Float64)
}

/// `string`/`int64`/`bool`/`timestamp` operands all validate at v9.
#[test]
fn count_distinct_accepts_string_int64_bool_timestamp() {
    for field in ["session.id", "retry.count", "is.sampled", "seen.at"] {
        let r = check(9, json!({ "fn": "count_distinct", "of": field, "as": "n" }));
        assert!(r.is_ok(), "{field} should validate at v9: {r:?}");
    }
}

/// A `float64` operand is rejected, naming the field and its type —
/// `approx_distinct` rejects floating point, and equality on floats is a
/// poor fit for a distinct count anyway.
#[test]
fn count_distinct_rejects_float64_naming_field_and_type() {
    let r = check(
        9,
        json!({ "fn": "count_distinct", "of": "duration", "as": "n" }),
    );
    assert!(r.is_err(), "float64 must be rejected");
    let msg = format!("{:?}", r.unwrap_err());
    assert!(msg.contains("duration"), "should name the field: {msg}");
    assert!(msg.contains("float64"), "should name the type: {msg}");
}

/// `count_distinct` always outputs `int64`, whatever the operand's type.
#[test]
fn count_distinct_output_is_int64() {
    let d = doc(
        9,
        json!({ "fn": "count_distinct", "of": "session.id", "as": "n" }),
    );
    let rel = validate(&d, &SourceRegistry::core(), &resolver()).expect("validates");
    let col = format!("{rel:?}");
    assert!(col.contains("Int64"), "should be Int64, got {col}");
}

/// `count_distinct` requires an `of` field, like every other aggregate that
/// isn't `count`.
#[test]
fn count_distinct_requires_an_of_field() {
    let r = check(9, json!({ "fn": "count_distinct", "as": "n" }));
    assert!(r.is_err(), "count_distinct without `of` must be rejected");
}

/// `count_distinct` is rejected at `irVersion` 8, naming the version (9) it
/// needs — mirrors the v5 gating for `stddev`/`stdvar`/`first`/`last`.
#[test]
fn count_distinct_is_gated_below_v9() {
    let r = check(
        8,
        json!({ "fn": "count_distinct", "of": "session.id", "as": "n" }),
    );
    assert!(r.is_err(), "count_distinct must not validate at v8");
    let msg = format!("{:?}", r.unwrap_err());
    assert!(msg.contains('9'), "error should name v9, got {msg}");
}

/// `count_distinct` accepts a scope predicate, like every other aggregate.
#[test]
fn count_distinct_accepts_a_scope_predicate() {
    let r = check(
        9,
        json!({
            "fn": "count_distinct",
            "of": "session.id",
            "as": "n",
            "where": { "field": "session.id", "op": "exists" }
        }),
    );
    assert!(r.is_ok(), "a scoped count_distinct should validate: {r:?}");
}

/// The wire name round-trips through serde.
#[test]
fn count_distinct_wire_name_round_trips() {
    let agg: query_ir::Agg =
        serde_json::from_value(json!({ "fn": "count_distinct", "of": "x", "as": "n" }))
            .expect("parses");
    assert_eq!(agg.func, query_ir::AggFn::CountDistinct);
    let back = serde_json::to_value(&agg).expect("serializes");
    assert_eq!(back["fn"], "count_distinct");
}
