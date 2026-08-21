//! `irVersion` 5: the aggregate vocabulary LogQL needs.
//!
//! Two additions, both to `aggregate`. Neither changes the shape of a
//! document — a v4 document means exactly what it meant before.
//!
//! - Four functions the IR lacked: `stddev`, `stdvar`, `first`, `last`.
//! - An optional `divisor` on an aggregate, so a count can be reported per
//!   unit rather than absolute. That is all `rate` is: the querier already
//!   plans it as `Count` plus `rate_divisor_seconds`, and without it the IR
//!   cannot express the single most-used LogQL metric function.
//!
//! `divisor` rather than `per_seconds` because dividing an aggregate by a
//! scalar is not inherently about time, and the IR is signal-agnostic.

use query_ir::{Document, IrError, SourceRegistry, validate};
use serde_json::json;

/// A document with one aggregate, at the given version.
fn doc(ir_version: i64, agg: serde_json::Value) -> Document {
    serde_json::from_value(json!({
        "irVersion": ir_version,
        "from": "logs",
        "range": { "from": "now-1h", "to": "now" },
        "result": "series",
        "pipeline": [{ "aggregate": { "by": ["service.name"], "step": "1m", "aggs": [agg] } }],
    }))
    .expect("document parses")
}

fn check(ir_version: i64, agg: serde_json::Value) -> Result<(), IrError> {
    let d = doc(ir_version, agg);
    validate(&d, &SourceRegistry::core(), &permissive()).map(|_| ())
}

/// A resolver that admits any field, so these tests exercise the aggregate
/// rules rather than field resolution.
fn permissive() -> query_ir::InMemoryResolver {
    query_ir::InMemoryResolver::new()
        .with_column("logs", "duration", "duration", query_ir::ValueType::Float64)
        .with_column(
            "logs",
            "service.name",
            "service_name",
            query_ir::ValueType::String,
        )
}

/// The four new functions are accepted at v5 and produce a numeric output.
#[test]
fn new_aggregate_functions_validate_at_v5() {
    for func in ["stddev", "stdvar", "first", "last"] {
        let r = check(5, json!({ "fn": func, "of": "duration", "as": "x" }));
        assert!(r.is_ok(), "{func} should validate at v5: {r:?}");
    }
}

/// …and are rejected at v4, naming the version they need. Silently accepting
/// them would let a v4 client believe a query ran that its server could not
/// have planned.
#[test]
fn new_aggregate_functions_are_gated_below_v5() {
    for func in ["stddev", "stdvar", "first", "last"] {
        let r = check(4, json!({ "fn": func, "of": "duration", "as": "x" }));
        assert!(r.is_err(), "{func} must not validate at v4");
        let msg = format!("{:?}", r.unwrap_err());
        assert!(msg.contains('5'), "{func}: error should name v5, got {msg}");
    }
}

/// `divisor` turns an absolute aggregate into a per-unit one.
#[test]
fn divisor_validates_at_v5() {
    let r = check(5, json!({ "fn": "count", "as": "rate", "divisor": 300.0 }));
    assert!(r.is_ok(), "count with a divisor should validate: {r:?}");
}

#[test]
fn divisor_is_gated_below_v5() {
    let r = check(4, json!({ "fn": "count", "as": "rate", "divisor": 300.0 }));
    assert!(r.is_err(), "divisor must not validate at v4");
}

/// Dividing by zero has no answer, and neither does dividing by a negative
/// window. Rejecting at validation beats emitting a plan that yields infinity.
#[test]
fn divisor_must_be_positive() {
    for bad in [0.0, -1.0, -300.0] {
        let r = check(5, json!({ "fn": "count", "as": "rate", "divisor": bad }));
        assert!(r.is_err(), "divisor {bad} must be rejected");
    }
}

/// NaN and infinity cannot arrive over the wire — JSON has no encoding for
/// them, and `serde_json` turns both into `null`, which reads as "no divisor".
/// They are reachable only from a Rust caller building a document directly,
/// which is exactly what `ql-ir` does, so the guard is not dead code.
#[test]
fn non_finite_divisors_are_rejected_when_built_in_rust() {
    // Over JSON: silently becomes absent rather than invalid.
    let d = doc(
        5,
        json!({ "fn": "count", "as": "rate", "divisor": f64::INFINITY }),
    );
    let query_ir::Stage::Aggregate(agg) = &d.pipeline[0] else {
        panic!("expected an aggregate stage");
    };
    assert_eq!(
        agg.aggs[0].divisor, None,
        "JSON cannot carry infinity, so it arrives as absent"
    );

    // Constructed directly: rejected.
    for bad in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
        let mut d = doc(5, json!({ "fn": "count", "as": "rate" }));
        let query_ir::Stage::Aggregate(agg) = &mut d.pipeline[0] else {
            panic!("expected an aggregate stage");
        };
        agg.aggs[0].divisor = Some(bad);
        assert!(
            validate(&d, &SourceRegistry::core(), &permissive()).is_err(),
            "divisor {bad} must be rejected"
        );
    }
}

/// A divided aggregate is always fractional, even when the underlying
/// function returns an integer — `count` divided by 300 is not an `Int64`.
#[test]
fn dividing_a_count_yields_a_float() {
    let d = doc(5, json!({ "fn": "count", "as": "rate", "divisor": 300.0 }));
    let rel = validate(&d, &SourceRegistry::core(), &permissive()).expect("validates");
    let col = format!("{rel:?}");
    assert!(
        col.contains("Float64"),
        "a divided count should be Float64, got {col}"
    );
}

/// Existing documents keep their meaning: no divisor, unchanged output type.
#[test]
fn an_undivided_count_is_still_an_integer() {
    let d = doc(4, json!({ "fn": "count", "as": "n" }));
    let rel = validate(&d, &SourceRegistry::core(), &permissive()).expect("validates");
    assert!(format!("{rel:?}").contains("Int64"), "{rel:?}");
}
