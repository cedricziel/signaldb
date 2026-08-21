//! LogQL lowered onto the query IR.
//!
//! Log queries first, then the metric shapes `irVersion` 5 made expressible.
//! What is still out of reach gets an explicit `Inexpressible` error naming
//! the construct, never a silently narrower query.

use query_ir::{ComparisonOp, Predicate, Stage};
use serde_json::json;

fn doc(q: &str) -> query_ir::Document {
    ql_ir::logql_to_ir(q, "now-1h", "now").unwrap_or_else(|e| panic!("{q} should lower: {e}"))
}

fn where_of(q: &str) -> Predicate {
    match doc(q).pipeline.first() {
        Some(Stage::Where(p)) => p.clone(),
        other => panic!("{q}: expected a leading where stage, got {other:?}"),
    }
}

fn leaves(p: &Predicate) -> Vec<(String, ComparisonOp, Option<serde_json::Value>)> {
    match p {
        Predicate::Leaf(l) => vec![(l.field.clone(), l.op, l.value.clone())],
        Predicate::And(parts) | Predicate::Or(parts) => parts.iter().flat_map(leaves).collect(),
        Predicate::Not(inner) => leaves(inner),
    }
}

/// A stream selector becomes a `where` over logical field names. Loki's
/// `service_name` is SignalDB's `service.name`; a label with no logical
/// equivalent passes through bare, so the resolver coalesces it across
/// attribute containers.
#[test]
fn stream_selector_labels_map_to_logical_fields() {
    let cases: &[(&str, &str)] = &[
        (r#"{service_name="api"}"#, "service.name"),
        (r#"{service="api"}"#, "service.name"),
        (r#"{job="api"}"#, "service.name"),
        (r#"{level="error"}"#, "severity_text"),
        (r#"{severity="error"}"#, "severity_text"),
        (r#"{trace_id="abc"}"#, "trace_id"),
        // No logical equivalent: stays bare and resolves as an attribute.
        (r#"{k8s_namespace="prod"}"#, "k8s_namespace"),
    ];
    for (query, field) in cases {
        let ls = leaves(&where_of(query));
        assert_eq!(ls.len(), 1, "{query}");
        assert_eq!(&ls[0].0, field, "{query}");
    }
}

/// The four matcher operators keep their meaning.
#[test]
fn matcher_operators_map_to_ir_comparisons() {
    let eq = leaves(&where_of(r#"{service_name="api"}"#));
    assert_eq!(eq[0].1, ComparisonOp::Eq);

    let re = leaves(&where_of(r#"{service_name=~"api-.*"}"#));
    assert_eq!(re[0].1, ComparisonOp::Regex);

    // Negations wrap rather than inventing a `NotRegex` operator.
    assert!(matches!(
        where_of(r#"{service_name!="api"}"#),
        Predicate::Not(_)
    ));
    assert!(matches!(
        where_of(r#"{service_name!~"api-.*"}"#),
        Predicate::Not(_)
    ));
}

/// Line filters match the log body. `|=` is a substring test, which the IR
/// spells `contains`.
#[test]
fn line_filters_become_body_predicates() {
    let p = where_of(r#"{service_name="api"} |= "error""#);
    let ls = leaves(&p);
    assert!(
        ls.iter().any(|(f, op, v)| f == "body"
            && *op == ComparisonOp::Contains
            && v.as_ref() == Some(&json!("error"))),
        "{ls:?}"
    );

    let p = where_of(r#"{service_name="api"} |~ "err.*""#);
    assert!(
        leaves(&p)
            .iter()
            .any(|(f, op, _)| f == "body" && *op == ComparisonOp::Regex),
        "regex line filter"
    );
}

/// A pipeline of filters conjoins with the selector rather than replacing it.
#[test]
fn selector_and_pipeline_conjoin() {
    let ls = leaves(&where_of(
        r#"{service_name="api", level="error"} |= "timeout""#,
    ));
    let fields: Vec<_> = ls.iter().map(|(f, _, _)| f.as_str()).collect();
    assert!(fields.contains(&"service.name"), "{fields:?}");
    assert!(fields.contains(&"severity_text"), "{fields:?}");
    assert!(fields.contains(&"body"), "{fields:?}");
}

/// `count_over_time` is a bucketed count — an `aggregate` with a `step`.
#[test]
fn count_over_time_becomes_a_stepped_aggregate() {
    let d = doc(r#"count_over_time({service_name="api"}[5m])"#);
    assert_eq!(d.result, query_ir::ResultEnvelope::Series);
    let agg = d
        .pipeline
        .iter()
        .find_map(|s| match s {
            Stage::Aggregate(a) => Some(a.clone()),
            _ => None,
        })
        .expect("an aggregate stage");
    assert_eq!(agg.step.as_deref(), Some("5m"));
    assert_eq!(agg.aggs.len(), 1);
    assert_eq!(agg.aggs[0].func, query_ir::AggFn::Count);
    assert_eq!(agg.aggs[0].divisor, None, "a count is not a rate");
}

/// `rate` is the same count, divided by the window — the reason `divisor`
/// exists. 5m is 300 seconds.
#[test]
fn rate_becomes_a_count_with_a_divisor() {
    let d = doc(r#"rate({service_name="api"}[5m])"#);
    let agg = d
        .pipeline
        .iter()
        .find_map(|s| match s {
            Stage::Aggregate(a) => Some(a.clone()),
            _ => None,
        })
        .expect("an aggregate stage");
    assert_eq!(agg.aggs[0].func, query_ir::AggFn::Count);
    assert_eq!(agg.aggs[0].divisor, Some(300.0));
    assert_eq!(d.ir_version, 5, "a divisor requires v5");
}

/// A vector aggregation supplies the grouping.
#[test]
fn sum_by_supplies_the_grouping() {
    let d = doc(r#"sum by (service_name) (count_over_time({service_name="api"}[1m]))"#);
    let agg = d
        .pipeline
        .iter()
        .find_map(|s| match s {
            Stage::Aggregate(a) => Some(a.clone()),
            _ => None,
        })
        .expect("an aggregate stage");
    assert_eq!(agg.by, vec!["service.name".to_string()]);
}

/// Documents that need no v5 feature declare v1, so they remain executable by
/// an older server. Version is claimed from what the query uses, not stamped
/// at the maximum.
#[test]
fn version_reflects_the_features_used() {
    assert_eq!(doc(r#"{service_name="api"}"#).ir_version, 1);
    assert_eq!(
        doc(r#"count_over_time({service_name="api"}[5m])"#).ir_version,
        1
    );
    assert_eq!(doc(r#"rate({service_name="api"}[5m])"#).ir_version, 5);
}

/// What the IR still cannot say is refused by name. A partially-lowered query
/// would return more rows than asked for while looking successful.
#[test]
fn inexpressible_constructs_are_named_not_dropped() {
    for q in [
        r#"sum(rate({a="b"}[5m])) / sum(rate({c="d"}[5m]))"#,
        r#"label_replace(rate({a="b"}[5m]), "x", "$1", "y", "(.*)")"#,
    ] {
        let err = ql_ir::logql_to_ir(q, "now-1h", "now")
            .expect_err(&format!("{q} has no IR equivalent yet"));
        assert!(
            matches!(err, ql_ir::LowerError::Inexpressible(_)),
            "{q}: got {err:?}"
        );
    }
}

/// A parse failure stays a parse failure, carrying the position the parser
/// reported.
#[test]
fn parse_errors_propagate() {
    let err = ql_ir::logql_to_ir("{service_name=}", "now-1h", "now").expect_err("malformed");
    assert!(matches!(err, ql_ir::LowerError::ParseLogql(_)), "{err:?}");
}
