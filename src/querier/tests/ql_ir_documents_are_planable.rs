//! Every document `ql-ir` emits must be one the querier can actually plan.
//!
//! `ql-ir` cannot check this itself. It is a leaf crate by design — it has no
//! access to `LogicalSchema`, so its field names (`span.name`, `status.code`,
//! `span_kind`, `service.name`, `severity_text`) are only as correct as
//! whoever wrote them. A typo there produces a document that parses, validates
//! against a permissive test resolver, and then fails at plan time with
//! "unknown field" — or worse, silently resolves as an attribute that is
//! always absent, quietly returning no rows.
//!
//! This test is the join: it lowers a corpus through `ql-ir` and resolves
//! every referenced field against the real `LogicalSchema`. It is the
//! prerequisite for routing the compat endpoints through the IR planner —
//! whatever that swap ends up looking like, it cannot be safe until the names
//! line up.

use common::query_ir::{Predicate, Stage};
use common::schema::logical::LogicalSchema;

/// Every field a document's predicates and grouping reference.
fn referenced_fields(doc: &common::query_ir::Document) -> Vec<String> {
    fn from_predicate(p: &Predicate, out: &mut Vec<String>) {
        match p {
            Predicate::Leaf(l) => out.push(l.field.clone()),
            Predicate::And(parts) | Predicate::Or(parts) => {
                parts.iter().for_each(|p| from_predicate(p, out))
            }
            Predicate::Not(inner) => from_predicate(inner, out),
        }
    }

    let mut out = Vec::new();
    for stage in &doc.pipeline {
        match stage {
            Stage::Where(p) => from_predicate(p, &mut out),
            Stage::Aggregate(a) => {
                out.extend(a.by.iter().cloned());
                for agg in &a.aggs {
                    out.extend(agg.of.clone());
                    // A scoping predicate addresses fields too, and they need
                    // resolving just as much as a `where` stage's do.
                    if let Some(scope) = &agg.scope {
                        from_predicate(scope, &mut out);
                    }
                }
            }
            _ => {}
        }
    }
    out
}

/// Assert a query lowers to exactly the expected logical field, and that the
/// real schema declares it.
///
/// Both halves are needed, and an earlier version of this test had only the
/// second. Checking "does the emitted name resolve?" cannot catch a typo: an
/// unrecognised name is indistinguishable from an ordinary attribute key, so
/// `status_code` (the physical column) sails through as if it were an
/// attribute and the query silently matches nothing. Naming the expectation is
/// what makes the failure visible.
#[track_caller]
fn assert_lowers_to(
    doc: &common::query_ir::Document,
    schema: &LogicalSchema,
    source: &str,
    expected: &str,
    query: &str,
) {
    let fields = referenced_fields(doc);
    assert!(
        fields.iter().any(|f| f == expected),
        "{query}: expected a predicate on `{expected}`, got {fields:?}"
    );
    assert!(
        schema.resolve(source, expected).is_some(),
        "{query}: `{expected}` is not declared by LogicalSchema for {source} —          a query using it would resolve as an always-absent attribute and          silently return nothing"
    );
}

#[test]
fn traceql_intrinsics_lower_to_declared_fields() {
    let schema = LogicalSchema::core();
    // Every TraceQL intrinsic, with the logical field it must become. These
    // are the names `ql-ir` rewrites; getting one wrong is invisible at
    // runtime, which is why they are pinned here against the real schema.
    let cases: &[(&str, &str)] = &[
        (r#"{ name = "GET /api" }"#, "span.name"),
        (r#"{ status = error }"#, "status.code"),
        (r#"{ kind = server }"#, "span_kind"),
        (r#"{ resource.service.name = "api" }"#, "service.name"),
        (r#"{ .service.name = "api" }"#, "service.name"),
    ];
    for (query, expected) in cases {
        let doc = ql_ir::traceql_to_ir(query, "now-1h", "now")
            .unwrap_or_else(|e| panic!("{query} should lower: {e}"));
        assert_eq!(doc.from, "traces");
        assert_lowers_to(&doc, &schema, "traces", expected, query);
    }
}

/// Attribute keys are deliberately *not* declared — they resolve through the
/// container fallback — so they must pass through unrewritten.
#[test]
fn traceql_attributes_pass_through_unrewritten() {
    let cases: &[(&str, &str)] = &[
        (r#"{ span.http.method = "GET" }"#, "span.http.method"),
        (
            r#"{ resource.k8s.pod.name = "p" }"#,
            "resource.k8s.pod.name",
        ),
        (r#"{ .http.method = "GET" }"#, "http.method"),
    ];
    for (query, expected) in cases {
        let doc = ql_ir::traceql_to_ir(query, "now-1h", "now").expect("lowers");
        let fields = referenced_fields(&doc);
        assert!(
            fields.iter().any(|f| f == expected),
            "{query}: expected `{expected}`, got {fields:?}"
        );
    }
}

#[test]
fn logql_labels_lower_to_declared_fields() {
    let schema = LogicalSchema::core();
    let cases: &[(&str, &str)] = &[
        (r#"{service_name="api"}"#, "service.name"),
        (r#"{service="api"}"#, "service.name"),
        (r#"{job="api"}"#, "service.name"),
        (r#"{level="error"}"#, "severity_text"),
        (r#"{severity="error"}"#, "severity_text"),
        (r#"{detected_level="error"}"#, "severity_text"),
        (r#"{service_name="api"} |= "boom""#, "body"),
        (
            r#"sum by (service_name) (rate({service_name="api"}[1m]))"#,
            "service.name",
        ),
    ];
    for (query, expected) in cases {
        let doc = ql_ir::logql_to_ir(query, "now-1h", "now")
            .unwrap_or_else(|e| panic!("{query} should lower: {e}"));
        assert_eq!(doc.from, "logs");
        assert_lowers_to(&doc, &schema, "logs", expected, query);
    }
}

/// The version a lowering claims must be one this build supports — otherwise
/// the querier rejects its own compat surface.
#[test]
fn lowered_versions_are_within_the_supported_range() {
    for query in [
        r#"count_over_time({service_name="api"}[5m])"#,
        r#"rate({service_name="api"}[5m])"#,
        r#"stddev_over_time({service_name="api"} | unwrap duration [5m])"#,
    ] {
        // Skipping on failure (`let Ok(..) else { continue }`) meant a query
        // that stopped lowering silently stopped being tested — which is how
        // `stddev_over_time` emitting an invalid document went unnoticed.
        let doc = ql_ir::logql_to_ir(query, "now-1h", "now")
            .unwrap_or_else(|e| panic!("{query} should lower: {e}"));
        assert!(
            common::query_ir::is_supported(doc.ir_version),
            "{query} claims irVersion {}, outside the supported range",
            doc.ir_version
        );
    }
}
