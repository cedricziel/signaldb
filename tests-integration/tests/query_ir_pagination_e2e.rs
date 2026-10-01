//! End-to-end pagination over `POST /api/v1/query`
//! (`query-result-pagination`): a walk returns every row exactly once even
//! when the files under it are compacted between pages, and its cursor is
//! bound to the tenant that started it. Also: an ascending walk across an
//! hour partition boundary, an expired cursor, and a trace-envelope walk.

use crate::query_ir_e2e::{
    BASE_NS, build_router, log_record, logs_request, post_ir, post_ir_as, setup, setup_with, span,
    test_tenant_context, traces_request,
};
use axum::http::StatusCode;
use compactor::executor::{CompactionExecutor, CompactionStatus, ExecutorConfig};
use compactor::metrics::CompactionMetrics;
use compactor::planner::{CompactionCandidate, PartitionStats};
use std::time::Duration;
use tests_integration::compaction_helpers::busiest_partition;
use tokio::time::sleep;

const ROWS: usize = 12;

fn paged_document(cursor: Option<&str>) -> serde_json::Value {
    let mut page = serde_json::json!({ "size": 5 });
    if let Some(cursor) = cursor {
        page["cursor"] = serde_json::json!(cursor);
    }
    serde_json::json!({
        "irVersion": 14,
        "from": "logs",
        "range": {
            "from": (BASE_NS - 1_000_000_000).to_string(),
            "to": (BASE_NS + 10_000_000_000).to_string(),
        },
        "result": "rows",
        "fields": ["body"],
        "pipeline": [],
        "page": page,
    })
}

fn bodies(body: &serde_json::Value) -> Vec<String> {
    body["rows"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .map(|row| row[0].as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default()
}

async fn compact_logs(services: &crate::query_ir_e2e::TestServices) {
    let manager = services.catalog_manager.clone();
    let partition = busiest_partition(&manager, "test-tenant", "test-dataset", "logs")
        .await
        .expect("a logs partition");
    let executor =
        CompactionExecutor::new(manager, ExecutorConfig::default(), CompactionMetrics::new());
    let result = executor
        .execute_candidate(CompactionCandidate {
            tenant_id: "test-tenant".into(),
            dataset_id: "test-dataset".into(),
            table_name: "logs".into(),
            partition_id: partition.to_string(),
            stats: PartitionStats {
                file_count: 3,
                total_size_bytes: 3 * 1024,
                avg_file_size_bytes: 1024,
            },
        })
        .await
        .expect("compaction runs");
    assert_eq!(
        result.status,
        CompactionStatus::Success,
        "{:?}",
        result.error
    );
    assert!(
        result.input_files_count > result.output_files_count,
        "compaction rewrote the files: {result:?}"
    );
}

#[tokio::test]
async fn a_walk_returns_every_row_once_across_a_compaction() {
    let services = setup().await;
    let ctx = test_tenant_context();
    let app = build_router(&services).await;
    // Three ingests, each waited for until queryable: three commits, three
    // files for the compaction to merge.
    for batch in 0..3 {
        let records = (0..ROWS / 3)
            .map(|i| {
                let n = batch * (ROWS / 3) + i;
                log_record(n as i64 * 1_000_000, "INFO", &format!("line-{n:02}"))
            })
            .collect();
        services
            .log_handler
            .handle_grpc_otlp_logs(&ctx, logs_request("api", records))
            .await
            .expect("ingest");
        let expected = (batch + 1) * (ROWS / 3);
        let mut visible = 0;
        for _ in 0..40 {
            let mut all = paged_document(None);
            all["page"]["size"] = serde_json::json!(100);
            visible = bodies(&post_ir(&app, all).await.1).len();
            if visible == expected {
                break;
            }
            sleep(Duration::from_millis(500)).await;
        }
        assert_eq!(visible, expected, "batch {batch} became queryable");
    }

    let (status, first) = post_ir(&app, paged_document(None)).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let mut seen = bodies(&first);
    let mut cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("more pages")
        .to_string();

    // The cursor is bound to the tenant that started the walk.
    let (status, _) = post_ir_as(
        &app,
        paged_document(Some(&cursor)),
        "other-key-123",
        "other-tenant",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    compact_logs(&services).await;

    loop {
        let (status, page) = post_ir(&app, paged_document(Some(&cursor))).await;
        assert_eq!(status, StatusCode::OK, "{page}");
        assert!(
            page.get("page").is_some(),
            "a paged response has a page member"
        );
        seen.extend(bodies(&page));
        match page["page"]["next_cursor"].as_str() {
            Some(next) => cursor = next.to_string(),
            None => break,
        }
    }

    let expected: Vec<String> = (0..ROWS).rev().map(|n| format!("line-{n:02}")).collect();
    assert_eq!(seen, expected, "newest first, each row exactly once");
}

const HOUR_NS: i64 = 3_600_000_000_000;

/// Walk `doc` (which carries `page`) to its end, returning each page's body.
async fn walk(app: &axum::Router, doc: serde_json::Value) -> Vec<serde_json::Value> {
    let mut pages = Vec::new();
    let mut doc = doc;
    loop {
        let (status, page) = post_ir(app, doc.clone()).await;
        assert_eq!(status, StatusCode::OK, "{page}");
        let next = page["page"]["next_cursor"].as_str().map(str::to_string);
        pages.push(page);
        match next {
            Some(cursor) => doc["page"]["cursor"] = serde_json::json!(cursor),
            None => return pages,
        }
    }
}

#[tokio::test]
async fn an_ascending_walk_crosses_an_hour_partition_and_an_old_cursor_expires() {
    let services = setup_with(|config| {
        config.querier.page_cursor_ttl = Duration::from_secs(2);
    })
    .await;
    let app = build_router(&services).await;
    let hour = (BASE_NS / HOUR_NS + 1) * HOUR_NS;
    let offsets = [-2_000_000_000_i64, -1_000_000_000, 0, 1_000_000_000];
    let records = offsets
        .iter()
        .enumerate()
        .map(|(i, at)| log_record(hour + at - BASE_NS, "INFO", &format!("edge-{i}")))
        .collect();
    services
        .log_handler
        .handle_grpc_otlp_logs(&test_tenant_context(), logs_request("api", records))
        .await
        .expect("ingest");
    let doc = serde_json::json!({
        "irVersion": 14, "from": "logs",
        "range": { "from": (hour - 10_000_000_000).to_string(), "to": (hour + 10_000_000_000).to_string() },
        "result": "rows", "fields": ["body"],
        "pipeline": [{ "order": [{ "of": "timestamp", "dir": "asc" }] }],
        "page": { "size": 1 },
    });
    let mut walked = Vec::new();
    for _ in 0..40 {
        walked = walk(&app, doc.clone())
            .await
            .iter()
            .flat_map(bodies)
            .collect();
        if walked.len() == offsets.len() {
            break;
        }
        sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(walked, ["edge-0", "edge-1", "edge-2", "edge-3"]);

    let (_, first) = post_ir(&app, doc.clone()).await;
    let mut expired = doc;
    expired["page"]["cursor"] = first["page"]["next_cursor"].clone();
    sleep(Duration::from_secs(3)).await;
    let (status, body) = post_ir(&app, expired).await;
    assert_eq!(status, StatusCode::GONE, "{body}");
    assert_eq!(body["errorType"], "gone");
}

#[tokio::test]
async fn a_trace_envelope_walk_returns_each_trace_once_and_whole() {
    let services = setup().await;
    let app = build_router(&services).await;
    let spans = (0..3_u8)
        .flat_map(|t| (0..2_u8).map(move |s| (t, s)))
        .map(|(t, s)| {
            let mut span = span(&format!("op-{t}-{s}"), t + 1, 1_000);
            span.span_id = vec![t * 2 + s + 1; 8];
            span
        })
        .collect();
    services
        .trace_handler
        .handle_grpc_otlp_traces(&test_tenant_context(), traces_request("api", spans))
        .await
        .expect("ingest spans");
    let doc = serde_json::json!({
        "irVersion": 14, "from": "traces",
        "range": {
            "from": (BASE_NS - 1_000_000_000).to_string(),
            "to": (BASE_NS + 10_000_000_000).to_string(),
        },
        "result": "trace", "fields": ["trace_id", "span.name"], "pipeline": [],
        "page": { "size": 2 },
    });
    let mut traces: Vec<(String, usize)> = Vec::new();
    for _ in 0..40 {
        traces = walk(&app, doc.clone())
            .await
            .iter()
            .flat_map(|page| page["traces"].as_array().cloned().unwrap_or_default())
            .map(|t| {
                let id = t["trace_id"].as_str().unwrap_or_default().to_string();
                (id, t["spans"].as_array().map_or(0, Vec::len))
            })
            .collect();
        if traces.len() == 3 {
            break;
        }
        sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(traces.len(), 3, "{traces:?}");
    assert!(traces.iter().all(|(_, spans)| *spans == 2), "{traces:?}");
    let mut ids: Vec<_> = traces.iter().map(|(id, _)| id.clone()).collect();
    ids.dedup();
    assert_eq!(ids.len(), 3, "no trace on two pages");
}
