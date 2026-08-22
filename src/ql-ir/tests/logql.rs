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

    // D9: a negative matcher ORs in "the field is absent" rather than
    // relying on a plain `not`, so Loki's absent-matches-negation semantics
    // survive the IR's Kleene `not`. See
    // `negative_matchers_also_match_an_absent_field` for the exact shape.
    assert!(matches!(
        where_of(r#"{service_name!="api"}"#),
        Predicate::Or(_)
    ));
    assert!(matches!(
        where_of(r#"{service_name!~"api-.*"}"#),
        Predicate::Or(_)
    ));
}

/// D9 (`ir-single-lowering`): Loki treats a missing label as the empty
/// string, so `!=`, `!~`, and `=""` must all match a stream that lacks the
/// label entirely — not just one holding a different value. `=~` is the one
/// documented exception (see the module doc).
#[test]
fn negative_matchers_also_match_an_absent_field() {
    let neq = where_of(r#"{region!="eu"}"#);
    match &neq {
        Predicate::Or(parts) if parts.len() == 2 => {
            assert!(matches!(
                &parts[0],
                Predicate::Leaf(l) if l.field == "region" && l.op == ComparisonOp::Ne
            ));
            assert!(matches!(
                &parts[1],
                Predicate::Not(inner) if matches!(
                    inner.as_ref(),
                    Predicate::Leaf(l) if l.field == "region" && l.op == ComparisonOp::Exists
                )
            ));
        }
        other => panic!("expected or[ne, not(exists)], got {other:?}"),
    }

    let nre = where_of(r#"{region!~"e.*"}"#);
    match &nre {
        Predicate::Or(parts) if parts.len() == 2 => {
            assert!(matches!(
                &parts[0],
                Predicate::Not(inner) if matches!(
                    inner.as_ref(),
                    Predicate::Leaf(l) if l.field == "region" && l.op == ComparisonOp::Regex
                )
            ));
            assert!(matches!(
                &parts[1],
                Predicate::Not(inner) if matches!(
                    inner.as_ref(),
                    Predicate::Leaf(l) if l.field == "region" && l.op == ComparisonOp::Exists
                )
            ));
        }
        other => panic!("expected or[not(regex), not(exists)], got {other:?}"),
    }

    let empty = where_of(r#"{region=""}"#);
    match &empty {
        Predicate::Or(parts) if parts.len() == 2 => {
            assert!(matches!(
                &parts[0],
                Predicate::Leaf(l) if l.field == "region"
                    && l.op == ComparisonOp::Eq
                    && l.value.as_ref() == Some(&json!(""))
            ));
            assert!(matches!(
                &parts[1],
                Predicate::Not(inner) if matches!(
                    inner.as_ref(),
                    Predicate::Leaf(l) if l.field == "region" && l.op == ComparisonOp::Exists
                )
            ));
        }
        other => panic!("expected or[eq \"\", not(exists)], got {other:?}"),
    }

    // `=~` is unaffected — a plain regex, the documented corner case.
    assert!(matches!(
        where_of(r#"{region=~"e.*"}"#),
        Predicate::Leaf(l) if l.op == ComparisonOp::Regex
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

/// D7 (`ir-single-lowering`): a bare range aggregation with no outer `by` at
/// all is, in real Loki, one series per matching *stream* — not one series
/// total. The default grouping is the stream identity (`service.name`,
/// `severity_text`), the same two columns `logs.rs::SERIES_COLUMNS` names on
/// the querier side (pinned against each other there, since this crate has
/// no access to the real `LogicalSchema`).
#[test]
fn ungrouped_range_aggregation_defaults_to_the_stream_identity() {
    for query in [
        r#"count_over_time({service_name="api"}[5m])"#,
        r#"rate({service_name="api"}[5m])"#,
    ] {
        let agg = doc(query)
            .pipeline
            .iter()
            .find_map(|s| match s {
                Stage::Aggregate(a) => Some(a.clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{query}: expected an aggregate stage"));
        assert_eq!(
            agg.by,
            vec!["service.name".to_string(), "severity_text".to_string()],
            "{query}: an ungrouped range aggregation should default to the stream identity"
        );
    }
}

/// An explicit `by (...)` still wins — the stream-identity default only
/// applies when the query supplies no grouping at all.
#[test]
fn explicit_grouping_overrides_the_stream_identity_default() {
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

// ---------------------------------------------------------------------------
// Review findings on #1379. Each of these lowered to something the IR rejects,
// or to something that means a different query — and every one slipped past
// the original suite, which only ever exercised `count_over_time` and `rate`
// (both `count`, the one aggregate that needs no field).
// ---------------------------------------------------------------------------

/// `AggFn::needs_field()` is true for everything but `count`, so emitting
/// `of: None` for `sum_over_time` produces a document the validator refuses.
/// The field comes from the `unwrap` stage LogQL already requires for these.
#[test]
fn field_taking_range_functions_carry_their_unwrapped_field() {
    let cases: &[(&str, query_ir::AggFn)] = &[
        (
            r#"sum_over_time({a="b"} | unwrap duration [5m])"#,
            query_ir::AggFn::Sum,
        ),
        (
            r#"avg_over_time({a="b"} | unwrap duration [5m])"#,
            query_ir::AggFn::Avg,
        ),
        (
            r#"min_over_time({a="b"} | unwrap duration [5m])"#,
            query_ir::AggFn::Min,
        ),
        (
            r#"max_over_time({a="b"} | unwrap duration [5m])"#,
            query_ir::AggFn::Max,
        ),
        (
            r#"stddev_over_time({a="b"} | unwrap duration [5m])"#,
            query_ir::AggFn::Stddev,
        ),
        (
            r#"stdvar_over_time({a="b"} | unwrap duration [5m])"#,
            query_ir::AggFn::Stdvar,
        ),
        (
            r#"first_over_time({a="b"} | unwrap duration [5m])"#,
            query_ir::AggFn::First,
        ),
        (
            r#"last_over_time({a="b"} | unwrap duration [5m])"#,
            query_ir::AggFn::Last,
        ),
    ];
    for (query, expected) in cases {
        let d = doc(query);
        let agg = d
            .pipeline
            .iter()
            .find_map(|s| match s {
                Stage::Aggregate(a) => Some(a.clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{query}: expected an aggregate"));
        assert_eq!(agg.aggs[0].func, *expected, "{query}");
        assert_eq!(
            agg.aggs[0].of.as_deref(),
            Some("duration"),
            "{query}: a field-taking aggregate needs `of`"
        );
    }
}

/// Whatever is emitted must satisfy the IR's own validator — the check that
/// would have caught every finding here at once.
#[test]
fn every_lowered_document_validates() {
    let corpus = [
        r#"{service_name="api"}"#,
        r#"{service_name="api"} |= "boom""#,
        r#"count_over_time({service_name="api"}[5m])"#,
        r#"rate({service_name="api"}[5m])"#,
        r#"sum_over_time({service_name="api"} | unwrap duration [5m])"#,
        r#"sum by (service_name) (rate({service_name="api"}[1m]))"#,
    ];
    let resolver = query_ir::InMemoryResolver::new()
        .with_column(
            "logs",
            "service.name",
            "service_name",
            query_ir::ValueType::String,
        )
        .with_column(
            "logs",
            "severity_text",
            "severity_text",
            query_ir::ValueType::String,
        )
        .with_column("logs", "body", "body", query_ir::ValueType::String)
        .with_column("logs", "duration", "duration", query_ir::ValueType::Float64);
    for query in corpus {
        let d = doc(query);
        query_ir::validate(&d, &query_ir::SourceRegistry::core(), &resolver)
            .unwrap_or_else(|e| panic!("{query} lowered to a document the IR rejects: {e:?}"));
    }
}

/// The IR aggregates once; LogQL's vector-over-range is two levels. Collapsing
/// them is only sound when combining partial results with the outer function
/// reproduces the inner one over the union — `sum` of counts is a count,
/// `min` of mins is a min. `avg` of counts is not a count, so the pairing must
/// be refused rather than quietly answering a different question.
#[test]
fn only_collapsible_vector_aggregations_are_accepted() {
    for query in [
        r#"sum by (service_name) (count_over_time({a="b"}[1m]))"#,
        r#"sum by (service_name) (rate({a="b"}[1m]))"#,
        r#"min by (service_name) (min_over_time({a="b"} | unwrap duration [1m]))"#,
        r#"max by (service_name) (max_over_time({a="b"} | unwrap duration [1m]))"#,
    ] {
        assert!(
            ql_ir::logql_to_ir(query, "now-1h", "now").is_ok(),
            "{query} collapses soundly and should lower"
        );
    }

    for query in [
        r#"avg by (service_name) (count_over_time({a="b"}[1m]))"#,
        r#"max by (service_name) (rate({a="b"}[1m]))"#,
        r#"min by (service_name) (count_over_time({a="b"}[1m]))"#,
    ] {
        let err = ql_ir::logql_to_ir(query, "now-1h", "now")
            .expect_err(&format!("{query} does not collapse and must be refused"));
        assert!(
            matches!(err, ql_ir::LowerError::Inexpressible(_)),
            "{query}: got {err:?}"
        );
    }
}

/// A sub-second window must not round to zero. `as_secs()` turned `[500ms]`
/// into a `0s` step, which is not a window at all.
#[test]
fn subsecond_ranges_keep_their_precision() {
    let cases: &[(&str, &str)] = &[
        (r#"count_over_time({a="b"}[500ms])"#, "500ms"),
        (r#"count_over_time({a="b"}[1500ms])"#, "1500ms"),
        (r#"count_over_time({a="b"}[30s])"#, "30s"),
        (r#"count_over_time({a="b"}[5m])"#, "5m"),
        (r#"count_over_time({a="b"}[2h])"#, "2h"),
    ];
    for (query, expected) in cases {
        let d = doc(query);
        let agg = d
            .pipeline
            .iter()
            .find_map(|s| match s {
                Stage::Aggregate(a) => Some(a.clone()),
                _ => None,
            })
            .expect("an aggregate");
        assert_eq!(agg.step.as_deref(), Some(*expected), "{query}");
    }
}

/// `unwrap` means two different things depending on where it appears, and the
/// docs now claim both. Pinned, because that claim was wrong once already.
#[test]
fn unwrap_is_supported_only_where_it_feeds_an_aggregate() {
    // Supplying a range aggregate's field: supported, and the field lands.
    let d = doc(r#"sum_over_time({a="b"} | unwrap duration [5m])"#);
    let agg = d
        .pipeline
        .iter()
        .find_map(|s| match s {
            Stage::Aggregate(a) => Some(a.clone()),
            _ => None,
        })
        .expect("an aggregate");
    assert_eq!(agg.aggs[0].of.as_deref(), Some("duration"));

    // In a bare log query there is no aggregate for it to feed, so it is
    // refused rather than silently ignored.
    let err = ql_ir::logql_to_ir(r#"{a="b"} | unwrap duration"#, "now-1h", "now")
        .expect_err("a bare unwrap has nothing to unwrap into");
    assert!(
        matches!(err, ql_ir::LowerError::Inexpressible(_)),
        "got {err:?}"
    );
}
