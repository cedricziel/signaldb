//! # Tempo `tags` → IR predicate (design D4, `ir-single-lowering`)
//!
//! Tempo's `tags` request parameter is a space-separated logfmt encoding
//! (`key=value key2=value2`, [`super::search_filter::parse_tags`]) that
//! produces the same [`traceql::Condition`] values TraceQL parsing does — but
//! it has no TraceQL text of its own to lower. `ql_ir` lowers *languages*
//! (TraceQL, LogQL); `tags` is an HTTP parameter encoding, not a language, so
//! this shim stays on the querier side of that boundary rather than growing
//! `ql_ir`'s surface for one compat parameter (D4).
//!
//! Each condition is lowered by
//! [`ql_ir::traceql_condition_to_predicate`] — the exact per-condition step
//! `ql_ir::traceql_to_ir` uses internally for TraceQL's own `q` parameter,
//! exposed for exactly this reuse — so field-naming and status/kind
//! normalization cannot drift from `ql-ir`'s: there is only one
//! implementation, not two that happen to agree.
//!
//! An earlier version of this shim rendered each condition back into
//! one-condition TraceQL spanset *text* and re-parsed it through
//! `ql_ir::traceql_to_ir`, reasoning that reusing the same function this way
//! also couldn't drift. That was true for field-naming, but wrong in a
//! different way: TraceQL's string-literal grammar has no escape syntax
//! (`traceql::parser::parse_value` rejects any embedded `"` outright, and
//! never un-escapes a backslash), so round-tripping an arbitrary tags value
//! through it was unsound — a value containing a backslash silently changed
//! (Rust's `Debug` escaping doubled it, and the doubled form parsed back as
//! itself), a value with an embedded `"` was rejected even though the old
//! path accepted it, and a *key* containing `&&` split into a bogus
//! multi-clause parse instead of one leaf on that literal key. Lowering the
//! already-parsed [`Condition`] directly has no text to corrupt.

use common::query_ir::Predicate;
use traceql::Condition;

use super::error::QuerierError;

/// Lower Tempo `tags` conditions (already parsed by
/// [`super::search_filter::parse_tags`]) to one IR predicate — a conjunction
/// when there is more than one condition, matching the order the old
/// (`search_filter::to_expr`-per-condition) path applied them.
pub(crate) fn conditions_to_predicate(conditions: &[Condition]) -> Result<Predicate, QuerierError> {
    let mut predicates = Vec::with_capacity(conditions.len());
    for condition in conditions {
        predicates
            .push(ql_ir::traceql_condition_to_predicate(condition).map_err(QuerierError::from)?);
    }
    Ok(match predicates.len() {
        1 => predicates.remove(0),
        _ => Predicate::And(predicates),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::query_ir::{ComparisonOp, Leaf};
    use traceql::{FilterValue, Selector};

    /// Extract the single `Predicate` a one-condition TraceQL query lowers
    /// to, for comparison against the shim's output.
    fn ir_predicate_for(query: &str) -> Predicate {
        let doc = ql_ir::traceql_to_ir(query, "0", "0").unwrap();
        match doc.pipeline.into_iter().next() {
            Some(common::query_ir::Stage::Where(predicate)) => predicate,
            other => panic!("expected exactly one Where stage, got {other:?}"),
        }
    }

    /// Pin: for every selector kind `parse_tags` can produce, the shim must
    /// equal the predicate `ql_ir::traceql_to_ir` produces for the
    /// equivalent TraceQL text (task 3.2) — by construction, since both call
    /// `ql_ir::traceql_condition_to_predicate`, but pinned explicitly so a
    /// refactor that breaks this is caught here rather than downstream.
    #[test]
    fn matches_traceql_to_ir_for_every_tags_selector_kind() {
        let cases: &[(Condition, &str)] = &[
            (
                Condition {
                    selector: Selector::ServiceName,
                    value: FilterValue::String("api".to_string()),
                },
                r#"{ resource.service.name = "api" }"#,
            ),
            (
                Condition {
                    selector: Selector::SpanName,
                    value: FilterValue::String("GET /x".to_string()),
                },
                r#"{ name = "GET /x" }"#,
            ),
            (
                Condition {
                    selector: Selector::Status,
                    value: FilterValue::String("error".to_string()),
                },
                r#"{ status = "error" }"#,
            ),
            (
                Condition {
                    selector: Selector::Kind,
                    value: FilterValue::String("server".to_string()),
                },
                r#"{ kind = "server" }"#,
            ),
            (
                Condition {
                    selector: Selector::AnyAttribute("http.method".to_string()),
                    value: FilterValue::String("GET".to_string()),
                },
                r#"{ .http.method = "GET" }"#,
            ),
        ];
        for (condition, equivalent_traceql) in cases {
            let shimmed = conditions_to_predicate(std::slice::from_ref(condition)).unwrap();
            let direct = ir_predicate_for(equivalent_traceql);
            assert_eq!(
                shimmed, direct,
                "condition {condition:?} should equal traceql_to_ir({equivalent_traceql:?})"
            );
        }
    }

    #[test]
    fn conjoins_multiple_conditions_in_order() {
        let conditions = vec![
            Condition {
                selector: Selector::ServiceName,
                value: FilterValue::String("api".to_string()),
            },
            Condition {
                selector: Selector::AnyAttribute("http.method".to_string()),
                value: FilterValue::String("GET".to_string()),
            },
        ];
        let predicate = conditions_to_predicate(&conditions).unwrap();
        let expected = Predicate::And(vec![
            ir_predicate_for(r#"{ resource.service.name = "api" }"#),
            ir_predicate_for(r#"{ .http.method = "GET" }"#),
        ]);
        assert_eq!(predicate, expected);
    }

    #[test]
    fn a_single_condition_is_not_wrapped_in_and() {
        let conditions = vec![Condition {
            selector: Selector::ServiceName,
            value: FilterValue::String("api".to_string()),
        }];
        let predicate = conditions_to_predicate(&conditions).unwrap();
        assert!(
            !matches!(predicate, Predicate::And(_)),
            "a single condition must not be wrapped in a redundant And: {predicate:?}"
        );
    }

    /// `search_filter::parse_tags`'s own real output, routed through the
    /// shim end to end — not just the synthetic `Condition`s built by hand
    /// above.
    #[test]
    fn lowers_real_parse_tags_output() {
        let conditions = super::super::search_filter::parse_tags(
            "service.name=filter-test-service http.method=GET",
        )
        .unwrap();
        let predicate = conditions_to_predicate(&conditions).unwrap();
        let expected = Predicate::And(vec![
            ir_predicate_for(r#"{ resource.service.name = "filter-test-service" }"#),
            ir_predicate_for(r#"{ .http.method = "GET" }"#),
        ]);
        assert_eq!(predicate, expected);
    }

    /// A backslash in a tags value must survive unchanged. A text
    /// round-trip through TraceQL doubled it (Rust's `Debug` escaping, then
    /// parsed back literally by a grammar with no un-escaping) — silently
    /// filtering on the wrong string, never observed as an error.
    #[test]
    fn a_backslash_in_the_value_is_preserved_exactly() {
        let condition = Condition {
            selector: Selector::AnyAttribute("file.path".to_string()),
            value: FilterValue::String(r"C:\Users\foo".to_string()),
        };
        let predicate = conditions_to_predicate(std::slice::from_ref(&condition)).unwrap();
        assert_eq!(
            predicate,
            Predicate::Leaf(Leaf {
                field: "file.path".to_string(),
                op: ComparisonOp::Eq,
                value: Some(serde_json::Value::String(r"C:\Users\foo".to_string())),
            }),
            "the leaf's value must be the original single-backslash string, not a doubled one"
        );
    }

    /// A tags value containing a literal `"` must lower successfully — the
    /// old (`search_filter::to_expr`) path never rejects it, since it
    /// compares the raw string directly with no grammar of its own to
    /// offend. A text round-trip rejected this outright (TraceQL's string
    /// literal has no escape for an embedded quote).
    #[test]
    fn a_value_with_an_embedded_quote_is_accepted() {
        let condition = Condition {
            selector: Selector::AnyAttribute("weird.key".to_string()),
            value: FilterValue::String(r#"va"lue"#.to_string()),
        };
        let predicate = conditions_to_predicate(std::slice::from_ref(&condition))
            .unwrap_or_else(|e| panic!("embedded-quote value should lower, got {e}"));
        assert_eq!(
            predicate,
            Predicate::Leaf(Leaf {
                field: "weird.key".to_string(),
                op: ComparisonOp::Eq,
                value: Some(serde_json::Value::String(r#"va"lue"#.to_string())),
            })
        );
    }

    /// A tags *key* containing `&&` must stay one leaf on that literal key.
    /// Splicing an unescaped key into TraceQL text handed `&&` to the
    /// spanset grammar's own AND operator, corrupting one condition into a
    /// bogus multi-clause parse.
    #[test]
    fn a_key_containing_ampersand_ampersand_stays_one_leaf() {
        let condition = Condition {
            selector: Selector::AnyAttribute("weird&&key".to_string()),
            value: FilterValue::String("value".to_string()),
        };
        let predicate = conditions_to_predicate(std::slice::from_ref(&condition))
            .unwrap_or_else(|e| panic!("a literal `&&` in a key should lower, got {e}"));
        assert_eq!(
            predicate,
            Predicate::Leaf(Leaf {
                field: "weird&&key".to_string(),
                op: ComparisonOp::Eq,
                value: Some(serde_json::Value::String("value".to_string())),
            })
        );
    }
}
