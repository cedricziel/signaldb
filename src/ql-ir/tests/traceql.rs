//! TraceQL lowered onto the query IR.
//!
//! Written before the lowering exists. What these assert is the *contract*:
//! a TraceQL spanset becomes a `where` stage of conjoined equality leaves over
//! logical field names, with TraceQL's scoping carried by the IR's container
//! qualifiers rather than flattened away.

use query_ir::{ComparisonOp, Predicate, Stage};
use serde_json::json;

/// Lower a spanset and return its single `where` predicate, failing loudly if
/// the document is not shaped the way every test here assumes.
fn where_of(traceql_query: &str) -> Predicate {
    let doc = ql_ir::traceql_to_ir(traceql_query, "now-1h", "now")
        .unwrap_or_else(|e| panic!("{traceql_query} should lower: {e}"));
    assert_eq!(doc.from, "traces");
    match doc.pipeline.as_slice() {
        [Stage::Where(p)] => p.clone(),
        other => panic!("expected exactly one where stage, got {other:?}"),
    }
}

/// The four intrinsics map onto the logical names the IR declares for the
/// `traces` source — not onto TraceQL's spelling, and not onto physical
/// columns.
#[test]
fn intrinsics_map_to_logical_field_names() {
    let cases: &[(&str, &str, serde_json::Value)] = &[
        (r#"{ name = "GET /api" }"#, "span.name", json!("GET /api")),
        // `status`/`kind` are closed enums stored Title-cased
        // (`status_code_to_str`/`span_kind_to_str` in
        // conversion_traces.rs: "Ok"/"Error"/"Unspecified",
        // "Internal"/"Server"/...); TraceQL spells them lowercase, so the
        // leaf's value must be normalized to the stored form or it can
        // never match a real row (found by the ir-single-lowering
        // differential harness comparing this lowering against
        // search_filter::to_expr, which already normalizes the same way).
        (r#"{ status = error }"#, "status.code", json!("Error")),
        (r#"{ kind = server }"#, "span_kind", json!("Server")),
        (
            r#"{ resource.service.name = "api" }"#,
            "service.name",
            json!("api"),
        ),
        (r#"{ .service.name = "api" }"#, "service.name", json!("api")),
    ];
    for (query, field, value) in cases {
        match where_of(query) {
            Predicate::Leaf(leaf) => {
                assert_eq!(&leaf.field, field, "{query}");
                assert_eq!(leaf.op, ComparisonOp::Eq, "{query}");
                assert_eq!(leaf.value.as_ref(), Some(value), "{query}");
            }
            other => panic!("{query} should lower to one leaf, got {other:?}"),
        }
    }
}

/// TraceQL's scopes survive as the IR's container qualifiers. This is the
/// property that makes the mapping worth having: a scoped matcher stays
/// scoped, and an unscoped one stays coalescing.
#[test]
fn attribute_scope_becomes_a_container_qualifier() {
    let cases: &[(&str, &str)] = &[
        (r#"{ span.http.method = "GET" }"#, "span.http.method"),
        (
            r#"{ resource.k8s.pod.name = "p" }"#,
            "resource.k8s.pod.name",
        ),
        // Unscoped: a bare logical name, which the resolver coalesces across
        // containers exactly as TraceQL's `.key` does.
        (r#"{ .http.method = "GET" }"#, "http.method"),
    ];
    for (query, field) in cases {
        match where_of(query) {
            Predicate::Leaf(leaf) => assert_eq!(&leaf.field, field, "{query}"),
            other => panic!("{query} should lower to one leaf, got {other:?}"),
        }
    }
}

/// `&&` becomes an `and` predicate, preserving order.
#[test]
fn conjunction_becomes_an_and_predicate() {
    let p = where_of(r#"{ resource.service.name = "api" && span.http.method = "GET" }"#);
    match p {
        Predicate::And(parts) => {
            assert_eq!(parts.len(), 2);
            match (&parts[0], &parts[1]) {
                (Predicate::Leaf(a), Predicate::Leaf(b)) => {
                    assert_eq!(a.field, "service.name");
                    assert_eq!(b.field, "span.http.method");
                }
                other => panic!("expected two leaves, got {other:?}"),
            }
        }
        other => panic!("expected an and, got {other:?}"),
    }
}

/// Non-string literals keep their JSON type rather than being stringified —
/// the IR has a type system, and flattening everything to text would lose it.
#[test]
fn literal_types_are_preserved() {
    match where_of(r#"{ span.http.status_code = 500 }"#) {
        Predicate::Leaf(leaf) => assert_eq!(leaf.value, Some(json!(500))),
        other => panic!("got {other:?}"),
    }
    match where_of(r#"{ span.ok = true }"#) {
        Predicate::Leaf(leaf) => assert_eq!(leaf.value, Some(json!(true))),
        other => panic!("got {other:?}"),
    }
}

/// An empty spanset selects everything, so it lowers to a document with no
/// `where` stage at all — not to a vacuously-true predicate.
#[test]
fn empty_spanset_produces_no_where_stage() {
    let doc = ql_ir::traceql_to_ir("{}", "now-1h", "now").expect("an empty spanset lowers");
    assert!(doc.pipeline.is_empty(), "{:?}", doc.pipeline);
}

/// The lowering does not re-validate the language: a parse failure is the
/// parser's error, surfaced unchanged so a caller can still tell a malformed
/// query from an unimplemented one.
#[test]
fn parse_errors_propagate_with_their_class() {
    let err = ql_ir::traceql_to_ir("notbraces", "now-1h", "now").expect_err("not TraceQL");
    assert!(
        matches!(
            err,
            ql_ir::LowerError::Parse(traceql::ParseError::Syntax(_))
        ),
        "got {err:?}"
    );

    let err = ql_ir::traceql_to_ir(r#"{ span.x != "y" }"#, "now-1h", "now")
        .expect_err("unimplemented operator");
    assert!(
        matches!(
            err,
            ql_ir::LowerError::Parse(traceql::ParseError::Unsupported(_))
        ),
        "got {err:?}"
    );
}

/// The document is a real one: it validates against the IR's own rules rather
/// than merely deserialising.
#[test]
fn lowered_documents_are_valid_ir() {
    let doc =
        ql_ir::traceql_to_ir(r#"{ .service.name = "api" }"#, "now-1h", "now").expect("lowers");
    let json = serde_json::to_value(&doc).expect("serialises");
    assert_eq!(json["irVersion"], 1);
    assert_eq!(json["from"], "traces");
    assert_eq!(json["result"], "rows");
}
