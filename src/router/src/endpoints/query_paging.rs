//! Pagination over `POST /api/v1/query`
//! (`openspec/changes/archive/2026-10-02-query-result-pagination-and-tail`, design D3/D4/D8).
//!
//! A paged document's first page resolves `range` to an absolute window; the
//! cursor carries that window, the last row's sort key and the rows walked so
//! far, bound to the tenant, dataset and document by a fingerprint. Each
//! page re-runs the document over the frozen window, strictly after the key.
//! The router keeps no state between pages.

use axum::http::StatusCode;
use common::auth::TenantContext;
use common::config::QuerierConfig;
use common::query_cursor::{
    Cursor, CursorError, CursorKind, Expected, PageReport, PageRequest, SigningKey, fingerprint,
};
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
    key: Option<SigningKey>,
}

/// Validate a paged document and resolve its page: size, order, window and
/// the position to resume after. `document` is the request as received; the
/// caller has checked its version and structure. `secret`, when the
/// deployment has one, signs and verifies the cursors.
pub(super) fn plan(
    limits: &QuerierConfig,
    secret: Option<&str>,
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
    let key = secret.map(SigningKey::derive);
    let (window, after, emitted) = match &page.cursor {
        Some(token) => {
            let expected = Expected {
                kind: CursorKind::Page,
                fingerprint: &fingerprint,
                now_ns,
                ttl_ns: i64::try_from(limits.page_cursor_ttl.as_nanos()).unwrap_or(i64::MAX),
                key: key.as_ref(),
            };
            let cursor = Cursor::decode(token, expected).map_err(cursor_error)?;
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
    // The walk budget bounds this page too, not only the next request.
    let budget =
        u32::try_from(limits.page_max_walk_rows.saturating_sub(emitted)).unwrap_or(u32::MAX);
    let mut size = page.size.unwrap_or(limits.page_default_size).min(budget);
    // A trailing `limit` caps the whole walk: the querier never cuts past the
    // rows left under it, even inside a tie group, and reaching them ends it.
    let ceiling = match doc.pipeline.last() {
        Some(Stage::Limit(limit)) => {
            let remaining = u32::try_from(limit.saturating_sub(emitted)).unwrap_or(u32::MAX);
            size = size.min(remaining);
            Some(remaining)
        }
        _ => None,
    };
    Ok(PagePlan {
        request: PageRequest {
            size,
            unit: PageUnit::of(doc),
            order,
            after,
            ceiling,
            tail: None,
        },
        window,
        fingerprint,
        emitted,
        dir,
        key,
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
    pub(super) fn response(&self, report: &PageReport, now_ns: i64) -> Result<QueryPage, ApiError> {
        let more = report.has_more
            && self
                .request
                .ceiling
                .is_none_or(|c| report.emitted < c.into());
        let next_cursor = report
            .last_key
            .clone()
            .filter(|_| more)
            .map(|key| {
                Cursor {
                    kind: CursorKind::Page,
                    fingerprint: self.fingerprint.clone(),
                    window: [self.window.start_ns, self.window.end_ns],
                    key,
                    dir: self.dir.clone(),
                    emitted: self.emitted.saturating_add(report.emitted),
                    issued_at_ns: now_ns,
                    settled_through_ns: None,
                }
                .encode(self.key.as_ref())
            })
            .transpose()
            .map_err(|e| {
                ApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("could not encode the page cursor: {e}"),
                )
            })?;
        Ok(QueryPage { next_cursor })
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

pub(super) fn cursor_error(err: CursorError) -> ApiError {
    let status = match err {
        CursorError::Expired(_) => StatusCode::GONE,
        CursorError::Corrupt | CursorError::Mismatch => StatusCode::BAD_REQUEST,
    };
    ApiError::new(status, format!("cursor: {err}"))
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
            None,
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
                pagination_order(
                    &serde_json::from_value(document(json!([]), None)).expect("document"),
                )
                .into_iter()
                .map(|k| KeyPart {
                    field: k.field,
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
            .response(&report(2), NOW)
            .expect("response")
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
        assert_eq!((first.request.size, first.request.ceiling), (2, Some(3)));
        // A tie group past the limit is cut at it and ends the walk, even
        // when the querier reports more.
        assert_eq!(
            first
                .response(&report(3), NOW)
                .expect("response")
                .next_cursor,
            None
        );
        let cursor = first_cursor(&acme, pipeline.clone());
        let last = plan_for(&acme, &document(pipeline, Some(&cursor)), NOW).expect("last");
        assert_eq!((last.request.size, last.request.ceiling), (1, Some(1)));
    }

    #[test]
    fn a_signing_secret_expires_an_unsigned_cursor() {
        let acme = ctx("acme", "default");
        let unsigned = first_cursor(&acme, json!([]));
        let doc_json = document(json!([]), Some(&unsigned));
        let range = QueryRange {
            from: "now-1h".into(),
            to: "now".into(),
        };
        let signed = |doc_json: &serde_json::Value| {
            let doc: Document = serde_json::from_value(doc_json.clone()).expect("document");
            plan(
                &QuerierConfig::default(),
                Some("secret"),
                &acme,
                doc_json,
                &doc,
                &range,
                NOW,
            )
        };
        // Gone rather than bad, so a client restarts after a key rotation.
        let err = signed(&doc_json).err().expect("rejected");
        assert_eq!(err.status, StatusCode::GONE);
        // A cursor the signed router issued continues.
        let first = signed(&document(json!([]), None)).expect("first");
        let cursor = first
            .response(&report(2), NOW)
            .expect("response")
            .next_cursor
            .expect("cursor");
        let next = document(json!([]), Some(&cursor));
        assert!(signed(&next).is_ok());
    }

    #[test]
    fn the_walk_budget_bounds_the_page_size() {
        let acme = ctx("acme", "default");
        let limits = QuerierConfig {
            page_max_walk_rows: 3,
            ..QuerierConfig::default()
        };
        let cursor = first_cursor(&acme, json!([]));
        let doc_json = document(json!([]), Some(&cursor));
        let doc: Document = serde_json::from_value(doc_json.clone()).expect("document");
        let range = QueryRange {
            from: "now-1h".into(),
            to: "now".into(),
        };
        let next = plan(&limits, None, &acme, &doc_json, &doc, &range, NOW).expect("next");
        assert_eq!(next.request.size, 1, "two walked, one left");
    }

    #[test]
    fn the_final_page_carries_no_cursor() {
        let acme = ctx("acme", "default");
        let first = plan_for(&acme, &document(json!([]), None), NOW).expect("first");
        let done = PageReport {
            has_more: false,
            ..report(1)
        };
        assert_eq!(
            first.response(&done, NOW).expect("response").next_cursor,
            None
        );
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
