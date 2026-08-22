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
//! Rather than re-implementing `ql_ir::traceql_lower`'s field-naming and
//! status/kind normalization rules for a second time, each condition is
//! rendered back into the one-condition TraceQL spanset text that would have
//! produced it, then handed to [`ql_ir::traceql_to_ir`] — the *same* code
//! path `ql-ir` uses for TraceQL's own `q` parameter. This is not a
//! duplicate lowering that could drift from ql-ir's: it calls ql-ir's public
//! function directly, so the two cannot disagree by construction. The
//! module's unit tests still pin this against `traceql_to_ir` explicitly, to
//! document the guarantee and catch a signature change that would silently
//! break it.

use common::query_ir::{Predicate, Stage};
use traceql::{Condition, FilterValue, Selector};

use super::error::QuerierError;

/// Lower Tempo `tags` conditions (already parsed by
/// [`super::search_filter::parse_tags`]) to one IR predicate — a conjunction
/// when there is more than one condition, matching the order the old
/// (`search_filter::to_expr`-per-condition) path applied them.
pub(crate) fn conditions_to_predicate(conditions: &[Condition]) -> Result<Predicate, QuerierError> {
    let mut predicates = Vec::with_capacity(conditions.len());
    for condition in conditions {
        predicates.push(condition_to_predicate(condition)?);
    }
    Ok(match predicates.len() {
        1 => predicates.remove(0),
        _ => Predicate::And(predicates),
    })
}

/// Lower one condition via `ql_ir::traceql_to_ir`, extracting its single
/// `where` stage.
fn condition_to_predicate(condition: &Condition) -> Result<Predicate, QuerierError> {
    let rendered = render(condition)?;
    // The range is discarded — only the predicate the single condition
    // lowers to is used — so any well-formed range literal will do.
    let doc = ql_ir::traceql_to_ir(&rendered, "0", "0").map_err(QuerierError::from)?;
    match doc.pipeline.into_iter().next() {
        Some(Stage::Where(predicate)) => Ok(predicate),
        other => unreachable!(
            "a single non-empty TraceQL spanset always lowers to exactly one Where stage, got {other:?}"
        ),
    }
}

/// Render one condition as the one-condition TraceQL spanset text that
/// would parse back to the same [`Condition`] — see the module doc for why
/// this beats duplicating `ql_ir::traceql_lower`'s field-naming rules.
fn render(condition: &Condition) -> Result<String, QuerierError> {
    let value = render_value(&condition.value)?;
    Ok(match &condition.selector {
        Selector::ServiceName => format!("{{ resource.service.name = {value} }}"),
        Selector::SpanName => format!("{{ name = {value} }}"),
        Selector::Status => format!("{{ status = {value} }}"),
        Selector::Kind => format!("{{ kind = {value} }}"),
        Selector::SpanAttribute(key) => format!("{{ span.{key} = {value} }}"),
        Selector::ResourceAttribute(key) => format!("{{ resource.{key} = {value} }}"),
        Selector::AnyAttribute(key) => format!("{{ .{key} = {value} }}"),
        // `Selector` is `#[non_exhaustive]`, and `parse_tags` only ever
        // produces the variants above (`search_filter::tags_selector`) — but
        // saying so beats silently mis-rendering a selector this build
        // cannot express as tags.
        other => {
            return Err(QuerierError::Unsupported(format!(
                "tags selector {other:?} is recognised but not lowered by this build"
            )));
        }
    })
}

/// A filter value as a TraceQL literal. `Debug` on a `String` produces a
/// double-quoted, escaped Rust string literal, which is also a valid TraceQL
/// string literal for every value `parse_tags` can produce (its own value
/// parsing is unquoted-or-double-quoted logfmt, with no escape sequences to
/// preserve).
fn render_value(value: &FilterValue) -> Result<String, QuerierError> {
    Ok(match value {
        FilterValue::String(s) => format!("{s:?}"),
        FilterValue::Number(n) => n.clone(),
        FilterValue::Bool(b) => b.to_string(),
        other => {
            return Err(QuerierError::Unsupported(format!(
                "tags value {other:?} is recognised but not lowered by this build"
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Extract the single `Predicate` a one-condition TraceQL query lowers
    /// to, for comparison against the shim's output.
    fn ir_predicate_for(query: &str) -> Predicate {
        let doc = ql_ir::traceql_to_ir(query, "0", "0").unwrap();
        match doc.pipeline.into_iter().next() {
            Some(Stage::Where(predicate)) => predicate,
            other => panic!("expected exactly one Where stage, got {other:?}"),
        }
    }

    /// Pin: for every selector kind `parse_tags` can produce, the shim must
    /// equal the predicate `ql_ir::traceql_to_ir` produces for the
    /// equivalent TraceQL text (task 3.2) — by construction, since both call
    /// the same function, but pinned explicitly so a refactor that breaks
    /// this is caught here rather than downstream.
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
            let shimmed = condition_to_predicate(condition).unwrap();
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
}
