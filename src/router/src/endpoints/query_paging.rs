//! Pagination over `POST /api/v1/query`
//! (`openspec/changes/query-result-pagination-and-tail`, design D3/D4/D8).
//!
//! A paged document's first page resolves `range` to an absolute window; the
//! cursor carries that window, the last row's sort key and the rows walked so
//! far, bound to the tenant, dataset and document by a fingerprint. Each
//! page re-runs the document over the frozen window, strictly after the key.
//! The router keeps no state between pages.

use axum::http::StatusCode;
use common::auth::TenantContext;
use common::config::QuerierConfig;
use common::query_cursor::{Cursor, CursorError, CursorKind, PageReport, PageRequest, fingerprint};
use common::query_ir::{Direction, Document, IrError, PageUnit, Stage, pagination_order};

use super::api_error::{ApiError, ApiErrorDetail};
use super::query::{QueryPage, QueryRange, ResolvedWindow, resolve_window};

/// One page of a walk, planned from the request and its cursor.
pub(super) struct PagePlan {
    pub request: PageRequest,
    /// The walk's frozen window.
    pub window: ResolvedWindow,
    fingerprint: String,
    /// Rows (or traces) walked before this page.
    emitted: u64,
    dir: String,
}

/// Validate a paged document and resolve its page: size, order, window and
/// the position to resume after. `document` is the request as received; the
/// caller has checked its version and structure.
pub(super) fn plan(
    limits: &QuerierConfig,
    ctx: &TenantContext,
    document: &serde_json::Value,
    doc: &Document,
    range: &QueryRange,
    now_ns: i64,
) -> Result<PagePlan, ApiError> {
    common::query_ir::page::check(doc).map_err(ir_error)?;
    common::query_ir::page::check_size(doc, limits.page_max_size).map_err(ir_error)?;
    let page = doc.page.clone().unwrap_or(common::query_ir::Page {
        size: None,
        cursor: None,
    });
    let order = pagination_order(doc);
    let dir: String = order
        .iter()
        .map(|k| if k.dir == Direction::Asc { 'a' } else { 'd' })
        .collect();
    let fingerprint = fingerprint(&ctx.tenant_id, &ctx.dataset_id, document);
    let (window, after, emitted) = match &page.cursor {
        Some(token) => {
            let ttl = i64::try_from(limits.page_cursor_ttl.as_nanos()).unwrap_or(i64::MAX);
            let cursor = Cursor::decode(token, CursorKind::Page, &fingerprint, now_ns, ttl)
                .map_err(cursor_error)?;
            if cursor.dir != dir
                || !cursor
                    .key
                    .iter()
                    .map(|k| &k.field)
                    .eq(order.iter().map(|k| &k.field))
            {
                return Err(ApiError::bad_request(
                    "page.cursor does not match the document's order",
                ));
            }
            if cursor.emitted >= limits.page_max_walk_rows {
                return Err(ApiError::resource_limit(format!(
                    "the walk reached [querier].page_max_walk_rows ({}); \
                     narrow the range or the filter",
                    limits.page_max_walk_rows
                )));
            }
            let window = ResolvedWindow {
                start_ns: cursor.window[0],
                end_ns: cursor.window[1],
            };
            (window, Some(cursor.key), cursor.emitted)
        }
        None => (resolve_window(range, now_ns)?, None, 0),
    };
    let mut size = page.size.unwrap_or(limits.page_default_size);
    // A trailing `limit` caps the whole walk: the page that reaches it is
    // cut there exactly and ends the walk.
    let mut exact = false;
    if let Some(Stage::Limit(limit)) = doc.pipeline.last() {
        let remaining = limit.saturating_sub(emitted);
        if remaining <= u64::from(size) {
            size = u32::try_from(remaining).unwrap_or(size);
            exact = true;
        }
    }
    Ok(PagePlan {
        request: PageRequest {
            size,
            unit: PageUnit::of(doc),
            order,
            after,
            exact,
        },
        window,
        fingerprint,
        emitted,
        dir,
    })
}

impl PagePlan {
    /// The document the querier runs: the frozen window as absolute
    /// nanoseconds, and no cursor.
    pub(super) fn ticket_document(&self, document: &serde_json::Value) -> serde_json::Value {
        let mut document = document.clone();
        document["range"] = serde_json::json!({
            "from": self.window.start_ns.to_string(),
            "to": self.window.end_ns.to_string(),
        });
        if let Some(page) = document.get_mut("page").and_then(|p| p.as_object_mut()) {
            page.remove("cursor");
        }
        document
    }

    /// The response's `page` member: a cursor after the last emitted row
    /// while more of the result exists.
    pub(super) fn response(&self, report: Option<&PageReport>, now_ns: i64) -> QueryPage {
        let next_cursor = report
            .filter(|r| r.has_more && !self.request.exact)
            .and_then(|r| {
                Some(
                    Cursor {
                        kind: CursorKind::Page,
                        fingerprint: self.fingerprint.clone(),
                        window: [self.window.start_ns, self.window.end_ns],
                        key: r.last_key.clone()?,
                        dir: self.dir.clone(),
                        emitted: self.emitted.saturating_add(r.emitted),
                        issued_at_ns: now_ns,
                        settled_through_ns: None,
                    }
                    .encode(),
                )
            });
        QueryPage { next_cursor }
    }
}

/// A validation error, with `not_paginatable`/`not_tailable` naming the
/// stage, envelope or range bound in `details`.
pub(super) fn ir_error(err: IrError) -> ApiError {
    let reason = match &err {
        IrError::NotPaginatable { at, .. } => Some(("not_paginatable", at.clone())),
        IrError::NotTailable { at, .. } => Some(("not_tailable", at.clone())),
        _ => None,
    };
    let api = ApiError::bad_request(err.to_string());
    match reason {
        Some((reason, at)) => api.with_details(vec![ApiErrorDetail {
            row: None,
            column: Some(at),
            reason: reason.to_string(),
        }]),
        None => api,
    }
}

fn cursor_error(err: CursorError) -> ApiError {
    let status = match err {
        CursorError::Expired(_) => StatusCode::GONE,
        CursorError::Corrupt | CursorError::Mismatch => StatusCode::BAD_REQUEST,
    };
    ApiError::new(status, format!("page.cursor: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::query_cursor::{KeyPart, KeyValue};
    use serde_json::json;

    const NOW: i64 = 1_000_000_000_000;

    fn ctx(tenant: &str, dataset: &str) -> TenantContext {
        TenantContext::new(
            tenant.into(),
            dataset.into(),
            tenant.into(),
            dataset.into(),
            None,
            common::auth::TenantSource::Config,
        )
    }

    fn document(pipeline: serde_json::Value, cursor: Option<&str>) -> serde_json::Value {
        let mut page = json!({ "size": 2 });
        if let Some(cursor) = cursor {
            page["cursor"] = json!(cursor);
        }
        json!({
            "irVersion": 14, "from": "logs", "range": { "from": "now-1h", "to": "now" },
            "result": "rows", "pipeline": pipeline, "page": page
        })
    }

    fn plan_for(
        ctx: &TenantContext,
        document: &serde_json::Value,
        now_ns: i64,
    ) -> Result<PagePlan, ApiError> {
        let doc: Document = serde_json::from_value(document.clone()).expect("document");
        let range = QueryRange {
            from: "now-1h".into(),
            to: "now".into(),
        };
        plan(
            &QuerierConfig::default(),
            ctx,
            document,
            &doc,
            &range,
            now_ns,
        )
    }

    fn report(emitted: u64) -> PageReport {
        PageReport {
            last_key: Some(
                ["timestamp", "trace_id", "span_id", "service.name"]
                    .iter()
                    .map(|f| KeyPart {
                        field: f.to_string(),
                        value: KeyValue::I64(1),
                    })
                    .collect(),
            ),
            has_more: true,
            emitted,
        }
    }

    /// The cursor the first page of `pipeline` hands out.
    fn first_cursor(ctx: &TenantContext, pipeline: serde_json::Value) -> String {
        let first = plan_for(ctx, &document(pipeline, None), NOW).expect("first page");
        first
            .response(Some(&report(2)), NOW)
            .next_cursor
            .expect("more follows")
    }

    #[test]
    fn a_cursor_continues_over_the_frozen_window() {
        let acme = ctx("acme", "default");
        let first = plan_for(&acme, &document(json!([]), None), NOW).expect("first");
        assert_eq!(first.window.end_ns, NOW);
        let cursor = first_cursor(&acme, json!([]));
        let later = NOW + 60_000_000_000;
        let next = plan_for(&acme, &document(json!([]), Some(&cursor)), later).expect("next");
        assert_eq!(next.window.end_ns, NOW, "a relative range does not drift");
        assert_eq!(next.emitted, 2);
        assert!(next.request.after.is_some());
        let ticket = next.ticket_document(&document(json!([]), Some(&cursor)));
        assert_eq!(ticket["range"]["to"], json!(NOW.to_string()));
        assert!(ticket["page"].get("cursor").is_none());
    }

    #[test]
    fn a_cursor_is_bound_to_tenant_dataset_and_document() {
        let cursor = first_cursor(&ctx("acme", "default"), json!([]));
        let status = |ctx: &TenantContext, pipeline| {
            plan_for(ctx, &document(pipeline, Some(&cursor)), NOW)
                .err()
                .map(|e| e.status)
        };
        assert_eq!(
            status(&ctx("other", "default"), json!([])),
            Some(StatusCode::BAD_REQUEST)
        );
        assert_eq!(
            status(&ctx("acme", "alternate"), json!([])),
            Some(StatusCode::BAD_REQUEST)
        );
        let edited = json!([{ "where": { "field": "body", "op": "contains", "value": "x" } }]);
        assert_eq!(
            status(&ctx("acme", "default"), edited),
            Some(StatusCode::BAD_REQUEST)
        );
    }

    #[test]
    fn a_corrupt_cursor_is_400_and_an_expired_one_410() {
        let acme = ctx("acme", "default");
        let err = plan_for(&acme, &document(json!([]), Some("sdbc1.x.y")), NOW)
            .err()
            .expect("corrupt");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        let cursor = first_cursor(&acme, json!([]));
        let ttl = i64::try_from(QuerierConfig::default().page_cursor_ttl.as_nanos()).expect("ttl");
        let err = plan_for(&acme, &document(json!([]), Some(&cursor)), NOW + ttl + 1)
            .err()
            .expect("expired");
        assert_eq!(err.status, StatusCode::GONE);
    }

    #[test]
    fn a_trailing_limit_caps_the_walk() {
        let acme = ctx("acme", "default");
        let pipeline = json!([{ "limit": 3 }]);
        let first = plan_for(&acme, &document(pipeline.clone(), None), NOW).expect("first");
        assert_eq!((first.request.size, first.request.exact), (2, false));
        let cursor = first_cursor(&acme, pipeline.clone());
        let last = plan_for(&acme, &document(pipeline, Some(&cursor)), NOW).expect("last");
        assert_eq!((last.request.size, last.request.exact), (1, true));
        assert_eq!(last.response(Some(&report(1)), NOW).next_cursor, None);
    }

    #[test]
    fn the_final_page_carries_no_cursor() {
        let acme = ctx("acme", "default");
        let first = plan_for(&acme, &document(json!([]), None), NOW).expect("first");
        let done = PageReport {
            has_more: false,
            ..report(1)
        };
        assert_eq!(first.response(Some(&done), NOW).next_cursor, None);
    }

    #[test]
    fn not_paginatable_names_the_stage_in_details() {
        let acme = ctx("acme", "default");
        let pipeline =
            json!([{ "aggregate": { "by": [], "aggs": [{ "fn": "count", "as": "n" }] } }]);
        let err = plan_for(&acme, &document(pipeline, None), NOW)
            .err()
            .expect("rejected");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        let body = format!("{err:?}");
        assert!(
            body.contains("not_paginatable") && body.contains("pipeline[0].aggregate"),
            "{body}"
        );
    }
}
