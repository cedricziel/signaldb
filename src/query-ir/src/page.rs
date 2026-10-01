//! # Pagination and live tail
//!
//! A document may carry a document-level `page` (`irVersion` 14) or `tail`
//! (`irVersion` 15). Both walk a result in a total order: a page continues a
//! `rows`/`trace` result after a cursor's sort key over a frozen window, a
//! tail follows a `rows` result forward in time. This module owns the parts that
//! need no schema: which documents can be paged or tailed, and the order
//! they are walked in. Cursors themselves are opaque here; the router
//! encodes them.

use serde::{Deserialize, Serialize};

use super::document::{Document, ResultEnvelope};
use super::stage::Stage;
use super::validate::{IrError, require_feature};
use super::version::{Feature, OperatorRegistry};

/// A document-level `page`: walk the result `size` rows (or traces) at a time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrPage))]
#[serde(deny_unknown_fields)]
pub struct Page {
    /// Rows (or, for the `trace` envelope, traces) per page; the server
    /// default when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u32>,
    /// The previous response's `page.next_cursor`; absent on the first page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// A document-level `tail`: follow the window forward in time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema), schema(as = IrTail))]
#[serde(deny_unknown_fields)]
pub struct Tail {
    /// The previous response's `tail.cursor`; absent on the first call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// How far behind the server clock the tail reads (a duration such as
    /// `10s`), clamped to the server's bounds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settle: Option<String>,
}

/// The schema-free rules for `page`/`tail`: the version that carries each,
/// and that the document can be walked at all (design D5). A violation names
/// the stage, envelope or range bound at fault.
pub fn check(doc: &Document) -> Result<(), IrError> {
    if doc.page.is_none() && doc.tail.is_none() {
        return Ok(());
    }
    let registry = OperatorRegistry {
        version: doc.ir_version,
    };
    if doc.page.is_some() {
        require_feature(&registry, Feature::Page, "page")?;
    }
    let tail = doc.tail.is_some();
    if tail {
        require_feature(&registry, Feature::Tail, "tail")?;
    }
    let reject = |at: String, reason: String| {
        Err(if tail {
            IrError::NotTailable { at, reason }
        } else {
            IrError::NotPaginatable { at, reason }
        })
    };
    let verb = if tail { "tailed" } else { "paginated" };
    if !matches!(doc.result, ResultEnvelope::Rows | ResultEnvelope::Trace) {
        return reject(
            "result".to_string(),
            format!("a {} result cannot be {verb}", doc.result.as_str()),
        );
    }
    if tail && doc.result == ResultEnvelope::Trace {
        return reject(
            "result".to_string(),
            "a tail delivers spans as they end, so it cannot group whole traces; tail the rows envelope instead"
                .to_string(),
        );
    }
    let last = doc.pipeline.len().saturating_sub(1);
    for (i, stage) in doc.pipeline.iter().enumerate() {
        let at = format!("pipeline[{i}].{}", stage.name());
        match stage {
            Stage::Where(_) | Stage::Extract(_) | Stage::Correlate(_) => {}
            Stage::Order(_) | Stage::Match(_) | Stage::Limit(_) if tail => {
                return reject(
                    at,
                    format!(
                        "a tail has a fixed order and per-call size, so a {} stage cannot be tailed",
                        stage.name()
                    ),
                );
            }
            Stage::Order(_) | Stage::Match(_) => {}
            Stage::Limit(_) if i == last && doc.result == ResultEnvelope::Trace => {
                return reject(
                    at,
                    "a trace page counts traces, so a span limit cannot cap the walk".to_string(),
                );
            }
            Stage::Limit(_) if i == last => {}
            Stage::Limit(_) => {
                return reject(at, "only a trailing limit can be paginated".to_string());
            }
            other => {
                return reject(at, format!("a {} stage cannot be {verb}", other.name()));
            }
        }
    }
    let last_order = doc
        .pipeline
        .iter()
        .enumerate()
        .rev()
        .find_map(|(i, s)| match s {
            Stage::Order(keys) => Some((i, keys)),
            _ => None,
        });
    if doc.result == ResultEnvelope::Trace
        && let Some((i, keys)) = last_order
        && keys.first().is_some_and(|k| k.of != "trace_id")
    {
        return reject(
            format!("pipeline[{i}].order"),
            "the trace envelope pages whole traces, so its leading order key must be trace_id"
                .to_string(),
        );
    }
    if tail {
        if doc.range.to.as_str().map(str::trim) != Some("now") {
            return reject(
                "range.to".to_string(),
                "a tail follows the clock, so range.to must be `now`".to_string(),
            );
        }
        if doc.page.as_ref().is_some_and(|p| p.cursor.is_some()) {
            return reject(
                "page.cursor".to_string(),
                "a tail carries its position in tail.cursor, not page.cursor".to_string(),
            );
        }
    }
    Ok(())
}

/// Reject a `page.size` of zero or above the server's `max`.
pub fn check_size(doc: &Document, max: u32) -> Result<(), IrError> {
    match doc.page.as_ref().and_then(|p| p.size) {
        Some(0) => Err(IrError::Invalid("page.size must be greater than 0".into())),
        Some(size) if size > max => Err(IrError::Invalid(format!(
            "page.size {size} exceeds the server maximum of {max}"
        ))),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn doc(v: serde_json::Value) -> Document {
        serde_json::from_value(v).expect("document parses")
    }

    fn paged(result: &str, pipeline: serde_json::Value) -> Document {
        doc(json!({
            "irVersion": 14, "from": "traces",
            "range": { "from": "now-1h", "to": "now" },
            "result": result, "pipeline": pipeline, "page": { "size": 10 }
        }))
    }

    fn tailed(range_to: &str, pipeline: serde_json::Value) -> Document {
        doc(json!({
            "irVersion": 15, "from": "logs",
            "range": { "from": "now-1h", "to": range_to },
            "result": "rows", "pipeline": pipeline, "tail": {}
        }))
    }

    fn not_paginatable_at(doc: &Document) -> String {
        match check(doc) {
            Err(IrError::NotPaginatable { at, .. }) => at,
            other => panic!("expected not_paginatable, got {other:?}"),
        }
    }

    fn not_tailable_at(doc: &Document) -> String {
        match check(doc) {
            Err(IrError::NotTailable { at, .. }) => at,
            other => panic!("expected not_tailable, got {other:?}"),
        }
    }

    #[test]
    fn page_and_tail_are_optional_and_round_trip() {
        let plain = json!({
            "irVersion": 12, "from": "logs",
            "range": { "from": "now-1h", "to": "now" }, "result": "rows", "pipeline": []
        });
        let parsed = doc(plain.clone());
        assert_eq!(parsed.page, None);
        assert_eq!(parsed.tail, None);
        assert_eq!(serde_json::to_value(&parsed).expect("serializes"), plain);
    }

    #[test]
    fn page_is_gated_at_v14_and_tail_at_v15() {
        let mut d = paged("rows", json!([]));
        d.ir_version = 13;
        assert!(matches!(check(&d), Err(IrError::Invalid(m)) if m.contains("irVersion 14")));
        d.ir_version = 14;
        assert_eq!(check(&d), Ok(()));

        let mut t = tailed("now", json!([]));
        t.ir_version = 14;
        assert!(matches!(check(&t), Err(IrError::Invalid(m)) if m.contains("irVersion 15")));
        t.ir_version = 15;
        assert_eq!(check(&t), Ok(()));
    }

    #[test]
    fn every_non_paginatable_stage_is_named() {
        let cases = [
            (
                json!([{ "aggregate": { "by": [], "aggs": [{ "fn": "count", "as": "n" }] } }]),
                "pipeline[0].aggregate",
            ),
            (
                json!([{ "topk": { "n": 5, "of": "duration" } }]),
                "pipeline[0].topk",
            ),
            (
                json!([{ "where": { "field": "service.name", "op": "eq", "value": "a" } },
                       { "bottomk": { "n": 5, "of": "duration" } }]),
                "pipeline[1].bottomk",
            ),
            (
                json!([{ "describe": { "target": "fields" } }]),
                "pipeline[0].describe",
            ),
            (
                json!([{ "limit": 10 }, { "where": { "field": "service.name", "op": "eq", "value": "a" } }]),
                "pipeline[0].limit",
            ),
        ];
        for (pipeline, at) in cases {
            assert_eq!(not_paginatable_at(&paged("rows", pipeline)), at);
        }
    }

    #[test]
    fn every_non_row_envelope_is_named() {
        for result in [
            "series",
            "table",
            "heatmap",
            "flamegraph",
            "graph",
            "metadata",
            "scalar",
        ] {
            assert_eq!(not_paginatable_at(&paged(result, json!([]))), "result");
        }
    }

    #[test]
    fn a_trailing_limit_and_row_preserving_stages_paginate() {
        let d = paged(
            "rows",
            json!([
                { "where": { "field": "service.name", "op": "eq", "value": "a" } },
                { "order": [{ "of": "duration", "dir": "desc" }] },
                { "limit": 2500 }
            ]),
        );
        assert_eq!(check(&d), Ok(()));
    }

    #[test]
    fn a_trace_page_needs_trace_id_leading() {
        let d = paged(
            "trace",
            json!([{ "order": [{ "of": "duration", "dir": "desc" }] }]),
        );
        assert_eq!(not_paginatable_at(&d), "pipeline[0].order");
        let d = paged(
            "trace",
            json!([{ "order": [{ "of": "trace_id", "dir": "desc" }] }]),
        );
        assert_eq!(check(&d), Ok(()));
        assert_eq!(check(&paged("trace", json!([]))), Ok(()));
        // A trailing limit counts spans, a trace page counts whole traces.
        let d = paged("trace", json!([{ "limit": 10 }]));
        assert_eq!(not_paginatable_at(&d), "pipeline[0].limit");
    }

    #[test]
    fn every_non_tailable_case_is_named() {
        assert_eq!(not_tailable_at(&tailed("now-5m", json!([]))), "range.to");
        assert_eq!(
            not_tailable_at(&tailed("2026-01-01T00:00:00Z", json!([]))),
            "range.to"
        );
        assert_eq!(
            not_tailable_at(&tailed(
                "now",
                json!([{ "order": [{ "of": "timestamp", "dir": "asc" }] }])
            )),
            "pipeline[0].order"
        );
        assert_eq!(
            not_tailable_at(&tailed("now", json!([{ "limit": 5 }]))),
            "pipeline[0].limit"
        );
        assert_eq!(
            not_tailable_at(&tailed(
                "now",
                json!([{ "aggregate": { "by": [], "aggs": [{ "fn": "count", "as": "n" }] } }])
            )),
            "pipeline[0].aggregate"
        );
        let mut t = tailed("now", json!([]));
        t.page = Some(Page {
            size: Some(10),
            cursor: Some("sdbc1.x.y".into()),
        });
        assert_eq!(not_tailable_at(&t), "page.cursor");

        let mut m = paged(
            "rows",
            json!([{ "match": { "spansets": { "a": { "field": "service.name", "op": "eq", "value": "x" } } } }]),
        );
        m.ir_version = 15;
        m.tail = Some(Tail {
            cursor: None,
            settle: None,
        });
        m.page = None;
        assert_eq!(not_tailable_at(&m), "pipeline[0].match");
    }

    #[test]
    fn a_trace_envelope_cannot_be_tailed() {
        let mut t = paged("trace", json!([]));
        t.ir_version = 15;
        t.page = None;
        t.tail = Some(Tail {
            cursor: None,
            settle: None,
        });
        assert_eq!(not_tailable_at(&t), "result");
    }

    #[test]
    fn page_size_is_bounded() {
        let mut d = paged("rows", json!([]));
        assert_eq!(check_size(&d, 10), Ok(()));
        assert!(
            matches!(check_size(&d, 9), Err(IrError::Invalid(m)) if m.contains("maximum of 9"))
        );
        d.page = Some(Page {
            size: Some(0),
            cursor: None,
        });
        assert!(check_size(&d, 9).is_err());
    }

    #[test]
    fn minimum_ir_version_counts_page_and_tail() {
        assert_eq!(paged("rows", json!([])).minimum_ir_version(), 14);
        assert_eq!(tailed("now", json!([])).minimum_ir_version(), 15);
    }
}
