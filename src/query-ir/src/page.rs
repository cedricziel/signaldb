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
use super::stage::{Direction, Stage};
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

/// The prefix of the columns the planner adds to a paged result; a paged
/// document cannot name a field with it.
pub const RESERVED_PREFIX: &str = "__sdb_";
/// One key of the total order a page or tail walks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortKey {
    /// A logical field name of the terminal relation.
    pub field: String,
    pub dir: Direction,
}

impl SortKey {
    fn new(field: &str, dir: Direction) -> Self {
        Self {
            field: field.to_string(),
            dir,
        }
    }
}

/// What `page.size` counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PageUnit {
    Rows,
    /// Whole traces: the `trace` envelope never splits one across pages.
    Traces,
}

impl PageUnit {
    pub fn of(doc: &Document) -> Self {
        if doc.result == ResultEnvelope::Trace {
            PageUnit::Traces
        } else {
            PageUnit::Rows
        }
    }
}

/// A sort key the planner computes as a 64-bit hash of a log's `body`, so
/// lines without trace context that share a timestamp still order totally.
pub const BODY_HASH: &str = "__sdb_body_hash";

/// A source's time column and the tie-breakers appended to every order.
struct SourceOrder {
    time: &'static str,
    /// The column a tail advances on; the span end for traces, since a span
    /// is exported after it ends.
    tail_time: &'static str,
    tie_breakers: &'static [&'static str],
}

fn source_order(source: &str) -> Option<SourceOrder> {
    let (time, tail_time, tie_breakers): (_, _, &'static [&'static str]) = match source {
        "traces" => (
            "start_time_unix_nano",
            "end_time_unix_nano",
            &["trace_id", "span_id"],
        ),
        "logs" => (
            "timestamp",
            "timestamp",
            &[
                "trace_id",
                "span_id",
                "service.name",
                "observed_timestamp",
                BODY_HASH,
            ],
        ),
        "metrics" => ("timestamp", "timestamp", &["series.id"]),
        "exemplars" => (
            "timestamp",
            "timestamp",
            &["series.id", "trace.id", "span.id"],
        ),
        "profiles" => ("timestamp", "timestamp", &["profile.id"]),
        _ => return None,
    };
    Some(SourceOrder {
        time,
        tail_time,
        tie_breakers,
    })
}

/// The total order a `page` walks `doc` in: the last `order` stage's keys,
/// else a `match` pipeline's (and the `trace` envelope's) native
/// `(trace_id, start, span_id)`, else the source time column newest first;
/// then the source's tie-breakers, ascending, unless already present.
pub fn pagination_order(doc: &Document) -> Vec<SortKey> {
    let source = source_order(&doc.from);
    let explicit = doc.pipeline.iter().rev().find_map(|stage| match stage {
        Stage::Order(keys) => Some(keys),
        _ => None,
    });
    let has_match = doc.pipeline.iter().any(|s| matches!(s, Stage::Match(_)));
    let mut keys: Vec<SortKey> = match (explicit, &source) {
        (Some(keys), _) => keys.iter().map(|k| SortKey::new(&k.of, k.dir)).collect(),
        (None, Some(source)) if has_match || doc.result == ResultEnvelope::Trace => vec![
            SortKey::new("trace_id", Direction::Asc),
            SortKey::new(source.time, Direction::Asc),
            SortKey::new("span_id", Direction::Asc),
        ],
        (None, Some(source)) => vec![SortKey::new(source.time, Direction::Desc)],
        (None, None) => Vec::new(),
    };
    append_tie_breakers(&mut keys, source.as_ref());
    keys
}

/// The order a `tail` delivers in: the source's tail-time ascending, then
/// its tie-breakers.
pub fn tail_order(doc: &Document) -> Vec<SortKey> {
    let source = source_order(&doc.from);
    let mut keys: Vec<SortKey> = source
        .iter()
        .map(|s| SortKey::new(s.tail_time, Direction::Asc))
        .collect();
    append_tie_breakers(&mut keys, source.as_ref());
    keys
}

fn append_tie_breakers(keys: &mut Vec<SortKey>, source: Option<&SourceOrder>) {
    for field in source.map_or(&[][..], |s| s.tie_breakers) {
        if !keys.iter().any(|k| k.field == *field) {
            keys.push(SortKey::new(field, Direction::Asc));
        }
    }
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
    let stage_names = doc.pipeline.iter().flat_map(|stage| match stage {
        Stage::Extract(e) => e.as_fields.iter().map(|f| f.name.as_str()).collect(),
        Stage::Order(keys) => keys.iter().map(|k| k.of.as_str()).collect(),
        _ => Vec::new(),
    });
    let mut names = doc
        .fields
        .iter()
        .flatten()
        .map(String::as_str)
        .chain(stage_names);
    if let Some(name) = names.find(|n| n.starts_with(RESERVED_PREFIX)) {
        return Err(IrError::Invalid(format!(
            "'{name}': names starting with {RESERVED_PREFIX} are reserved for paging"
        )));
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
                return reject(at, format!("the {} stage cannot be {verb}", other.name()));
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

    fn keys(keys: &[SortKey]) -> Vec<(&str, Direction)> {
        keys.iter().map(|k| (k.field.as_str(), k.dir)).collect()
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
    fn a_paged_document_cannot_name_the_reserved_columns() {
        let reserved =
            |d: &Document| matches!(check(d), Err(IrError::Invalid(m)) if m.contains("'__sdb_x'"));
        let mut d = paged("rows", json!([]));
        d.fields = Some(vec!["__sdb_x".into()]);
        assert!(reserved(&d));
        let extract = json!([{ "extract": { "parser": "json", "as": [{ "name": "__sdb_x", "type": "string" }] } }]);
        assert!(reserved(&paged("rows", extract)));
        let order = json!([{ "order": [{ "of": "__sdb_x", "dir": "asc" }] }]);
        assert!(reserved(&paged("rows", order)));
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
    fn pagination_order_defaults_newest_first_with_tie_breakers() {
        let logs = doc(json!({
            "irVersion": 14, "from": "logs", "range": { "from": "now-1h", "to": "now" },
            "result": "rows", "pipeline": []
        }));
        assert_eq!(
            keys(&pagination_order(&logs)),
            [
                ("timestamp", Direction::Desc),
                ("trace_id", Direction::Asc),
                ("span_id", Direction::Asc),
                ("service.name", Direction::Asc),
                ("observed_timestamp", Direction::Asc),
                (BODY_HASH, Direction::Asc),
            ]
        );
        let mut d = logs.clone();
        for (source, expected) in [
            (
                "traces",
                vec![
                    ("start_time_unix_nano", Direction::Desc),
                    ("trace_id", Direction::Asc),
                    ("span_id", Direction::Asc),
                ],
            ),
            (
                "metrics",
                vec![
                    ("timestamp", Direction::Desc),
                    ("series.id", Direction::Asc),
                ],
            ),
            (
                "exemplars",
                vec![
                    ("timestamp", Direction::Desc),
                    ("series.id", Direction::Asc),
                    ("trace.id", Direction::Asc),
                    ("span.id", Direction::Asc),
                ],
            ),
            (
                "profiles",
                vec![
                    ("timestamp", Direction::Desc),
                    ("profile.id", Direction::Asc),
                ],
            ),
        ] {
            d.from = source.to_string();
            assert_eq!(keys(&pagination_order(&d)), expected, "{source}");
        }
    }

    #[test]
    fn pagination_order_leads_with_explicit_order_keys() {
        let d = paged(
            "rows",
            json!([{ "order": [{ "of": "duration", "dir": "desc" }, { "of": "span_id", "dir": "desc" }] }]),
        );
        assert_eq!(
            keys(&pagination_order(&d)),
            [
                ("duration", Direction::Desc),
                ("span_id", Direction::Desc),
                ("trace_id", Direction::Asc),
            ]
        );
    }

    #[test]
    fn match_and_trace_envelope_keep_native_trace_order() {
        let native = [
            ("trace_id", Direction::Asc),
            ("start_time_unix_nano", Direction::Asc),
            ("span_id", Direction::Asc),
        ];
        let m = paged(
            "rows",
            json!([{ "match": { "spansets": { "a": { "field": "service.name", "op": "eq", "value": "x" } } } }]),
        );
        assert_eq!(keys(&pagination_order(&m)), native);
        assert_eq!(keys(&pagination_order(&paged("trace", json!([])))), native);
    }

    #[test]
    fn tail_order_is_tail_time_ascending() {
        let t = tailed("now", json!([]));
        assert_eq!(
            keys(&tail_order(&t)),
            [
                ("timestamp", Direction::Asc),
                ("trace_id", Direction::Asc),
                ("span_id", Direction::Asc),
                ("service.name", Direction::Asc),
                ("observed_timestamp", Direction::Asc),
                (BODY_HASH, Direction::Asc),
            ]
        );
        let traces = paged("rows", json!([]));
        assert_eq!(
            keys(&tail_order(&traces)),
            [
                ("end_time_unix_nano", Direction::Asc),
                ("trace_id", Direction::Asc),
                ("span_id", Direction::Asc),
            ]
        );
    }

    #[test]
    fn minimum_ir_version_counts_page_and_tail() {
        assert_eq!(paged("rows", json!([])).minimum_ir_version(), 14);
        assert_eq!(tailed("now", json!([])).minimum_ir_version(), 15);
    }
}
