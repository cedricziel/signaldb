//! Live tail over `POST /api/v1/query`
//! (`openspec/changes/query-result-pagination-and-tail`, design D6/D7).
//!
//! A tail is a sequence of ordinary calls, each carrying the previous call's
//! `tail.cursor`. A call at server time `T` reads rows whose tail-time (the
//! span end for traces, else the source time column) lies after the cursor
//! and at or before `T - settle`, oldest first. The first call returns the
//! newest `page.size` rows instead. The router keeps no state between calls.

use common::auth::TenantContext;
use common::config::QuerierConfig;
use common::query_cursor::{
    Cursor, CursorKind, KeyPart, KeyValue, PageReport, PageRequest, TailBound, fingerprint,
};
use common::query_ir::{Direction, Document, PageUnit, SortKey, tail_order};

use super::api_error::ApiError;
use super::query::{QueryRange, QueryTail, QueryWarning, ResolvedWindow, resolve_window};
use super::query_paging::{cursor_error, ir_error};

/// One call of a tail, planned from the request and its cursor.
pub(super) struct TailPlan {
    pub request: PageRequest,
    /// The scan window the querier runs the document over.
    pub window: ResolvedWindow,
    fingerprint: String,
    dir: String,
    settled_ns: i64,
    settle_ns: i64,
    first: bool,
    /// `[from, to]` a lagging cursor skipped.
    skipped: Option<(i64, i64)>,
}

fn nanos(d: std::time::Duration) -> i64 {
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
}

/// The position strictly after every row whose tail-time is `t`: the
/// tie-breakers are null, and nulls sort last.
fn after_time(order: &[SortKey], t: i64) -> Vec<KeyPart> {
    order
        .iter()
        .enumerate()
        .map(|(i, k)| KeyPart {
            field: k.field.clone(),
            value: if i == 0 {
                KeyValue::I64(t)
            } else {
                KeyValue::Null
            },
        })
        .collect()
}

/// Validate a tailed document and plan this call: settle, position and
/// scan window. The caller has checked the document's version and structure.
pub(super) fn plan(
    limits: &QuerierConfig,
    ctx: &TenantContext,
    document: &serde_json::Value,
    doc: &Document,
    range: &QueryRange,
    now_ns: i64,
) -> Result<TailPlan, ApiError> {
    common::query_ir::page::check(doc).map_err(ir_error)?;
    common::query_ir::page::check_size(doc, limits.page_max_size).map_err(ir_error)?;
    let tail = doc.tail.as_ref();
    let requested = match tail.and_then(|t| t.settle.as_deref()) {
        Some(settle) => common::query_ir::parse_duration_ns(settle).ok_or_else(|| {
            ApiError::bad_request(format!("tail.settle: '{settle}' is not a duration"))
        })?,
        None => 0,
    };
    let settle_ns = requested.clamp(nanos(limits.tail_min_settle), nanos(limits.tail_max_settle));
    let settled_ns = now_ns.saturating_sub(settle_ns);
    let order = tail_order(doc);
    let dir: String = order
        .iter()
        .map(|k| if k.dir == Direction::Asc { 'a' } else { 'd' })
        .collect();
    let fingerprint = fingerprint(&ctx.tenant_id, &ctx.dataset_id, document);
    // A traces tail keys on span end; the scan prunes on span start, which
    // lies at most the longest tailable span before it.
    let span_slack = if doc.from == "traces" {
        nanos(limits.tail_max_span_duration)
    } else {
        0
    };
    let (after, start_ns, skipped) = match tail.and_then(|t| t.cursor.as_deref()) {
        Some(token) => {
            let cursor = Cursor::decode(token, CursorKind::Tail, &fingerprint, now_ns, i64::MAX)
                .map_err(cursor_error)?;
            let position = match cursor.key.first() {
                Some(KeyPart {
                    value: KeyValue::I64(t),
                    ..
                }) if cursor.dir == dir
                    && cursor
                        .key
                        .iter()
                        .map(|k| &k.field)
                        .eq(order.iter().map(|k| &k.field)) =>
                {
                    *t
                }
                _ => {
                    return Err(ApiError::bad_request(
                        "tail.cursor does not match the document's order",
                    ));
                }
            };
            let floor = now_ns.saturating_sub(nanos(limits.tail_max_lag));
            if position < floor {
                (
                    Some(after_time(&order, floor)),
                    floor,
                    Some((position, floor)),
                )
            } else {
                (Some(cursor.key), position, None)
            }
        }
        None => (None, resolve_window(range, now_ns)?.start_ns, None),
    };
    let first = after.is_none();
    let size = doc
        .page
        .as_ref()
        .and_then(|p| p.size)
        .unwrap_or(limits.page_default_size);
    Ok(TailPlan {
        request: PageRequest {
            size,
            // Spans arrive as they end, not as whole traces.
            unit: PageUnit::Rows,
            order,
            after,
            exact: false,
            tail: Some(TailBound {
                through_ns: settled_ns,
                newest: first,
            }),
        },
        window: ResolvedWindow {
            start_ns: if first {
                start_ns
            } else {
                start_ns.saturating_sub(span_slack)
            },
            end_ns: settled_ns,
        },
        fingerprint,
        dir,
        settled_ns,
        settle_ns,
        first,
        skipped,
    })
}

impl TailPlan {
    /// The document the querier runs: this call's scan window as absolute
    /// nanoseconds, and no `tail` (the ticket's page carries the call's
    /// bound; an absolute range would no longer validate as a tail).
    pub(super) fn ticket_document(&self, document: &serde_json::Value) -> serde_json::Value {
        let mut document = document.clone();
        document["range"] = serde_json::json!({
            "from": self.window.start_ns.to_string(),
            "to": self.window.end_ns.to_string(),
        });
        if let Some(object) = document.as_object_mut() {
            object.remove("tail");
        }
        document
    }

    /// The response's `tail` member. A call that read everything up to the
    /// settle line moves the cursor there, so an idle tail does not rescan.
    pub(super) fn response(&self, report: Option<&PageReport>, now_ns: i64) -> QueryTail {
        let backlog = report.filter(|r| !self.first && r.has_more);
        let key = match backlog.and_then(|r| r.last_key.clone()) {
            Some(key) => key,
            None => after_time(&self.request.order, self.settled_ns),
        };
        let cursor = Cursor {
            kind: CursorKind::Tail,
            fingerprint: self.fingerprint.clone(),
            window: [self.window.start_ns, self.settled_ns],
            key,
            dir: self.dir.clone(),
            emitted: 0,
            issued_at_ns: now_ns,
            settled_through_ns: Some(self.settled_ns),
        };
        QueryTail {
            cursor: cursor.encode(),
            settled_through_ns: self.settled_ns,
            settle_ns: self.settle_ns,
            caught_up: backlog.is_none(),
        }
    }

    /// `tail_lagged`, when this call skipped forward past a lagging cursor.
    pub(super) fn warning(&self) -> Option<QueryWarning> {
        let (from, to) = self.skipped?;
        Some(QueryWarning {
            code: "tail_lagged".to_string(),
            message: format!(
                "the tail fell more than [querier].tail_max_lag behind and skipped \
                 rows with a tail-time in ({from}, {to}] (unix ns); page that interval \
                 with an absolute range to read it"
            ),
            field: None,
            suggestions: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use serde_json::json;

    const NOW: i64 = 10_000_000_000_000;
    const S: i64 = 1_000_000_000;

    fn ctx(tenant: &str) -> TenantContext {
        TenantContext::new(
            tenant.into(),
            "default".into(),
            tenant.into(),
            "default".into(),
            None,
            common::auth::TenantSource::Config,
        )
    }

    fn document(source: &str, tail: serde_json::Value) -> serde_json::Value {
        json!({
            "irVersion": 15, "from": source, "range": { "from": "now-1h", "to": "now" },
            "result": "rows", "pipeline": [], "page": { "size": 2 }, "tail": tail
        })
    }

    fn plan_for(
        tenant: &str,
        document: &serde_json::Value,
        now_ns: i64,
    ) -> Result<TailPlan, ApiError> {
        let doc: Document = serde_json::from_value(document.clone()).expect("document");
        let range = QueryRange {
            from: "now-1h".into(),
            to: "now".into(),
        };
        plan(
            &QuerierConfig::default(),
            &ctx(tenant),
            document,
            &doc,
            &range,
            now_ns,
        )
    }

    fn report(has_more: bool) -> PageReport {
        PageReport {
            last_key: Some(after_time(&tail_order_of("logs"), NOW - 20 * S)),
            has_more,
            emitted: 2,
        }
    }

    fn tail_order_of(source: &str) -> Vec<SortKey> {
        let doc: Document = serde_json::from_value(document(source, json!({}))).expect("doc");
        tail_order(&doc)
    }

    #[test]
    fn the_first_call_reads_the_newest_rows_up_to_the_settle_line() {
        let first = plan_for("acme", &document("logs", json!({})), NOW).expect("first");
        let bound = first.request.tail.expect("tail bound");
        assert!(bound.newest);
        assert_eq!(
            bound.through_ns,
            NOW - 10 * S,
            "the default settle is the floor"
        );
        assert_eq!(first.window.start_ns, NOW - 3_600 * S);
        let tail = first.response(Some(&report(true)), NOW);
        assert!(tail.caught_up, "the first call leaves no backlog behind it");
        assert_eq!(tail.settled_through_ns, NOW - 10 * S);
    }

    #[test]
    fn settle_is_clamped_and_echoed() {
        let low = plan_for("acme", &document("logs", json!({ "settle": "0s" })), NOW).expect("low");
        assert_eq!(low.settle_ns, 10 * S);
        let high =
            plan_for("acme", &document("logs", json!({ "settle": "1h" })), NOW).expect("high");
        assert_eq!(high.settle_ns, 300 * S);
    }

    #[test]
    fn a_follow_up_continues_after_the_cursor_and_drains_a_backlog() {
        let first = plan_for("acme", &document("logs", json!({})), NOW).expect("first");
        let cursor = first.response(None, NOW).cursor;
        let later = NOW + 5 * S;
        let next = plan_for(
            "acme",
            &document("logs", json!({ "cursor": cursor })),
            later,
        )
        .expect("next");
        assert!(!next.request.tail.expect("bound").newest);
        assert_eq!(
            next.request.after.as_ref().expect("after")[0].value,
            KeyValue::I64(NOW - 10 * S)
        );
        let busy = next.response(Some(&report(true)), later);
        assert!(!busy.caught_up, "a size-bounded call leaves a backlog");
        assert!(next.warning().is_none());
    }

    #[test]
    fn a_traces_tail_scans_back_by_the_longest_span() {
        let first = plan_for("acme", &document("traces", json!({})), NOW).expect("first");
        let cursor = first.response(None, NOW).cursor;
        let tailed = document("traces", json!({ "cursor": cursor }));
        let next = plan_for("acme", &tailed, NOW).expect("next");
        assert_eq!(next.request.order[0].field, "end_time_unix_nano");
        let ticket = next.ticket_document(&tailed);
        assert!(
            ticket.get("tail").is_none(),
            "the querier sees a plain page"
        );
        let doc: Document = serde_json::from_value(ticket).expect("ticket document");
        assert_eq!(common::query_ir::page::check(&doc), Ok(()));
        assert_eq!(next.window.start_ns, NOW - 10 * S - 3_600 * S);
    }

    #[test]
    fn a_lagging_cursor_skips_forward_with_a_warning() {
        let first = plan_for("acme", &document("logs", json!({})), NOW).expect("first");
        let cursor = first.response(None, NOW).cursor;
        let much_later = NOW + 3_600 * S;
        let next = plan_for(
            "acme",
            &document("logs", json!({ "cursor": cursor })),
            much_later,
        )
        .expect("next");
        let floor = much_later - 300 * S;
        assert_eq!(
            next.request.after.as_ref().expect("after")[0].value,
            KeyValue::I64(floor)
        );
        let warning = next.warning().expect("tail_lagged");
        assert_eq!(warning.code, "tail_lagged");
        assert!(warning.message.contains(&floor.to_string()));
    }

    #[test]
    fn a_tail_cursor_is_tenant_bound() {
        let first = plan_for("acme", &document("logs", json!({})), NOW).expect("first");
        let cursor = first.response(None, NOW).cursor;
        let err = plan_for("other", &document("logs", json!({ "cursor": cursor })), NOW)
            .err()
            .expect("rejected");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }
}
