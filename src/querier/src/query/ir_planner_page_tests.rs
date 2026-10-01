//! `IrService::query` with a ticket `page` (`query-result-pagination`).

use std::sync::Arc;

use common::query_cursor::{KeyPart, KeyValue, PageReport, PageRequest, TailBound};
use common::query_ir::{Document, PageUnit, pagination_order, tail_order};
use datafusion::arrow::array::{Array, RecordBatch, StringArray, TimestampNanosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::prelude::SessionContext;
use serde_json::json;

use super::IrService;
use crate::query::IrQueryParams;

/// `(timestamp, trace_id, body)`: two timestamp ties, one full-key tie.
const ROWS: &[(i64, &str, &str)] = &[
    (10, "a", "r1"),
    (20, "b", "r2"),
    (20, "a", "r3"),
    (30, "c", "r4"),
    (40, "d", "r5"),
    // A duplicate of r5: the one full-key tie.
    (40, "d", "r5"),
    (50, "e", "r7"),
];

fn ctx() -> SessionContext {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "observed_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("body", DataType::Utf8, true),
        Field::new("service_name", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampNanosecondArray::from_iter_values(
                ROWS.iter().map(|r| r.0),
            )),
            Arc::new(TimestampNanosecondArray::from(vec![None; ROWS.len()])),
            Arc::new(StringArray::from_iter_values(ROWS.iter().map(|r| r.2))),
            Arc::new(StringArray::from_iter_values(ROWS.iter().map(|_| "api"))),
            Arc::new(StringArray::from_iter_values(ROWS.iter().map(|r| r.1))),
            Arc::new(StringArray::from_iter_values(ROWS.iter().map(|_| "s"))),
        ],
    )
    .expect("batch");
    super::tests::single_table_ctx("logs", schema, batch)
}

fn document(pipeline: serde_json::Value) -> serde_json::Value {
    json!({
        "irVersion": 12, "from": "logs", "range": { "from": 0, "to": 100 },
        "result": "rows", "fields": ["body"], "pipeline": pipeline
    })
}

fn request(doc: &serde_json::Value, size: u32, after: Option<Vec<KeyPart>>) -> PageRequest {
    let parsed: Document = serde_json::from_value(doc.clone()).expect("document");
    PageRequest {
        size,
        unit: PageUnit::of(&parsed),
        order: pagination_order(&parsed),
        after,
        ceiling: None,
        tail: None,
    }
}

async fn page(
    svc: &IrService,
    doc: &serde_json::Value,
    page: PageRequest,
) -> (Vec<String>, PageReport, usize) {
    let params = IrQueryParams {
        document: doc.clone(),
        now_ns: 0,
        page: Some(page),
    };
    let (batches, _, report) = svc.query(&params, "t", "d").await.expect("query");
    let mut bodies = Vec::new();
    let mut columns = 0;
    for b in &batches {
        columns = b.num_columns();
        let body = b
            .column_by_name("body")
            .expect("body")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        bodies.extend((0..body.len()).map(|i| body.value(i).to_string()));
    }
    (bodies, report.page.expect("page report"), columns)
}

async fn walk(svc: &IrService, doc: &serde_json::Value, size: u32) -> Vec<Vec<String>> {
    let mut pages = Vec::new();
    let mut after = None;
    loop {
        let (bodies, report, columns) = page(svc, doc, request(doc, size, after)).await;
        assert_eq!(columns, 1, "sort key columns are not projected");
        assert_eq!(report.emitted as usize, bodies.len());
        // Rows inside a tie group come back in no defined order.
        let mut bodies = bodies;
        bodies.sort();
        pages.push(bodies);
        if !report.has_more {
            return pages;
        }
        after = report.last_key;
    }
}

#[tokio::test]
async fn a_walk_reproduces_the_unpaged_order_newest_first() {
    let svc = IrService::new(ctx());
    let pages = walk(&svc, &document(json!([])), 2).await;
    // Newest first, then trace_id ascending; the two r5 lines tie on the
    // full key and stay together, so the first page carries three rows.
    assert_eq!(
        pages,
        [vec!["r5", "r5", "r7"], vec!["r3", "r4"], vec!["r1", "r2"],]
    );
}

#[tokio::test]
async fn an_order_stage_leads_and_a_trailing_limit_is_left_to_the_router() {
    let svc = IrService::new(ctx());
    let doc = document(json!([
        { "where": { "field": "timestamp", "op": "lte", "value": 40 } },
        { "order": [{ "of": "trace_id", "dir": "desc" }] },
        { "limit": 1 }
    ]));
    let pages = walk(&svc, &doc, 4).await;
    assert_eq!(pages, [vec!["r2", "r4", "r5", "r5"], vec!["r1", "r3"]]);
}

#[tokio::test]
async fn a_ceiling_cuts_inside_a_tie_group_and_ends_the_walk() {
    let svc = IrService::new(ctx());
    let doc = document(json!([]));
    let mut req = request(&doc, 2, None);
    req.ceiling = Some(2);
    let (bodies, report, _) = page(&svc, &doc, req).await;
    assert_eq!((bodies.len(), report.has_more), (2, false));
}

#[tokio::test]
async fn the_leading_time_key_bounds_the_scan() {
    let svc = IrService::new(ctx());
    let doc = document(json!([]));
    let (_, first, _) = page(&svc, &doc, request(&doc, 2, None)).await;
    let parsed: Document = serde_json::from_value(doc.clone()).expect("document");
    let plan = super::plan_document(
        svc.session_context.as_ref(),
        &parsed,
        super::PlanRequest::new("t", "d", 0)
            .with_page(Some((&request(&doc, 2, first.last_key), 10_000))),
    )
    .await
    .expect("plan")
    .expect("table")
    .0
    .into_optimized_plan()
    .expect("optimized");
    // The redundant leading bound sits beside the lexicographic predicate,
    // where the scan's pruning can use it.
    assert!(
        plan.display_indent().to_string().contains("timestamp <= "),
        "{}",
        plan.display_indent()
    );
}

/// 15,000 lines with one timestamp and no trace context: the body hash
/// orders them totally, so no tie group hits `page_max_tie_rows`.
#[tokio::test]
async fn identical_timestamps_without_trace_context_walk_without_a_tie_error() {
    const N: usize = 15_000;
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("body", DataType::Utf8, true),
        Field::new("service_name", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![7; N])),
            Arc::new(StringArray::from_iter_values(
                (0..N).map(|i| format!("line-{i}")),
            )),
            Arc::new(StringArray::from(vec!["api"; N])),
            Arc::new(StringArray::from(vec![None::<&str>; N])),
            Arc::new(StringArray::from(vec![None::<&str>; N])),
        ],
    )
    .expect("batch");
    let svc = IrService::new(super::tests::single_table_ctx("logs", schema, batch));
    let mut seen = std::collections::HashSet::new();
    for page in walk(&svc, &document(json!([])), 1_000).await {
        assert!(page.len() <= 1_000);
        for body in page {
            assert!(seen.insert(body), "a line came back twice");
        }
    }
    assert_eq!(seen.len(), N);
}

/// The `trace` envelope pages whole traces, and none of the planner's
/// helper columns reach the result.
#[tokio::test]
async fn a_trace_page_holds_whole_traces_and_only_the_requested_columns() {
    use datafusion::arrow::array::UInt64Array;
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("start_time_unix_nano", DataType::UInt64, false),
    ]));
    let spans = [
        ("t1", "a"),
        ("t2", "b"),
        ("t1", "c"),
        ("t3", "d"),
        ("t2", "e"),
    ];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from_iter_values(spans.iter().map(|s| s.0))),
            Arc::new(StringArray::from_iter_values(spans.iter().map(|s| s.1))),
            Arc::new(UInt64Array::from_iter_values(1..=spans.len() as u64)),
        ],
    )
    .expect("batch");
    let svc = IrService::new(super::tests::single_table_ctx("traces", schema, batch));
    let doc = json!({
        "irVersion": 12, "from": "traces", "range": { "from": 0, "to": 100 },
        "result": "trace", "fields": ["trace_id", "span_id"], "pipeline": []
    });
    let traces_of = |batches: &[RecordBatch]| {
        let mut ids = Vec::new();
        for b in batches {
            assert_eq!(b.num_columns(), 2, "{:?}", b.schema());
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8");
            ids.extend((0..col.len()).map(|i| col.value(i).to_string()));
        }
        ids
    };
    let params = |after| IrQueryParams {
        document: doc.clone(),
        now_ns: 0,
        page: Some(request(&doc, 2, after)),
    };
    let (first, _, report) = svc.query(&params(None), "t", "d").await.expect("page");
    let report = report.page.expect("page report");
    assert_eq!(traces_of(&first), ["t1", "t1", "t2", "t2"]);
    assert_eq!(report.emitted, 2);
    assert!(report.has_more);
    let (second, _, report) = svc
        .query(&params(report.last_key), "t", "d")
        .await
        .expect("page");
    assert_eq!(traces_of(&second), ["t3"]);
    assert!(!report.page.expect("page report").has_more);
}

fn tail_request(after: Option<i64>, through_ns: i64, size: u32) -> PageRequest {
    let doc: Document = serde_json::from_value(document(json!([]))).expect("document");
    let order = tail_order(&doc);
    PageRequest {
        size,
        unit: PageUnit::Rows,
        after: after.map(|t| {
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
        }),
        order,
        ceiling: None,
        tail: Some(TailBound {
            through_ns,
            newest: after.is_none(),
        }),
    }
}

#[tokio::test]
async fn a_first_tail_call_returns_the_newest_rows_oldest_first() {
    let svc = IrService::new(ctx());
    let doc = document(json!([]));
    // Settled through 40: r7 (50) is not yet visible to the tail.
    let (bodies, report, _) = page(&svc, &doc, tail_request(None, 40, 3)).await;
    assert_eq!(bodies.len(), 3);
    assert_eq!(
        bodies[0], "r4",
        "oldest of the newest three first: {bodies:?}"
    );
    assert!(bodies[1..].iter().all(|b| b == "r5" || b == "r6"));
    assert_eq!(report.emitted, 3);
}

#[tokio::test]
async fn a_follow_up_tail_call_returns_only_rows_after_the_cursor() {
    let svc = IrService::new(ctx());
    let doc = document(json!([]));
    let (bodies, report, _) = page(&svc, &doc, tail_request(Some(20), 50, 10)).await;
    assert_eq!(bodies.len(), 4);
    assert_eq!(bodies[0], "r4");
    assert_eq!(bodies[3], "r7");
    assert!(!report.has_more);

    let (bodies, report, _) = page(&svc, &doc, tail_request(Some(20), 50, 2)).await;
    assert_eq!(bodies[0], "r4", "{bodies:?}");
    assert!(report.has_more, "the backlog is drained in bounded calls");

    let (bodies, report, _) = page(&svc, &doc, tail_request(Some(50), 60, 10)).await;
    assert!(bodies.is_empty());
    assert!(!report.has_more, "an idle tail is caught up");
}

/// Iceberg stores `timestamp` in microseconds; a tail's bounds are
/// nanoseconds.
#[tokio::test]
async fn a_tail_over_a_microsecond_timestamp_column_compares_in_nanoseconds() {
    use datafusion::arrow::array::TimestampMicrosecondArray;
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("body", DataType::Utf8, true),
        Field::new("service_name", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![10, 20, 30])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(StringArray::from(vec!["api"; 3])),
            Arc::new(StringArray::from(vec![None::<&str>; 3])),
            Arc::new(StringArray::from(vec![None::<&str>; 3])),
        ],
    )
    .expect("batch");
    let svc = IrService::new(super::tests::single_table_ctx("logs", schema, batch));
    let doc = json!({
        "irVersion": 12, "from": "logs", "range": { "from": 0, "to": 100_000 },
        "result": "rows", "fields": ["body"], "pipeline": []
    });
    // After 10 µs, through 25 µs, in nanoseconds.
    let (bodies, _, _) = page(&svc, &doc, tail_request(Some(10_000), 25_000, 10)).await;
    assert_eq!(bodies, ["b"]);
}

/// A null tie-breaker sorts last, so it is the newest of its timestamp:
/// the backwards read puts it first, and the page ends on it.
#[tokio::test]
async fn a_first_tail_call_keeps_null_tie_breakers_last() {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("body", DataType::Utf8, true),
        Field::new("service_name", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![30, 40, 40])),
            Arc::new(StringArray::from(vec!["old", "traced", "untraced"])),
            Arc::new(StringArray::from(vec!["api"; 3])),
            Arc::new(StringArray::from(vec![Some("a"), Some("a"), None])),
            Arc::new(StringArray::from(vec![Some("s"), Some("s"), None])),
        ],
    )
    .expect("batch");
    let svc = IrService::new(super::tests::single_table_ctx("logs", schema, batch));
    let doc = document(json!([]));

    let (bodies, _, _) = page(&svc, &doc, tail_request(None, 40, 1)).await;
    assert_eq!(bodies, ["untraced"]);
    let (bodies, _, _) = page(&svc, &doc, tail_request(None, 40, 3)).await;
    assert_eq!(bodies, ["old", "traced", "untraced"]);
}

/// The report's key is the last row emitted, the newest, so resuming after
/// it repeats nothing.
#[tokio::test]
async fn a_first_tail_call_reports_the_key_of_its_newest_row() {
    let svc = IrService::new(ctx());
    let doc = document(json!([]));
    let (_, report, _) = page(&svc, &doc, tail_request(None, 40, 3)).await;
    let last_key = report.last_key.expect("last key");
    assert_eq!(last_key[0].value, KeyValue::I64(40));

    let mut next = tail_request(Some(0), 50, 10);
    next.after = Some(last_key);
    let (bodies, _, _) = page(&svc, &doc, next).await;
    assert_eq!(bodies, ["r7"]);
}
