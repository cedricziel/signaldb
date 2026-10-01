//! End-to-end pagination over `POST /api/v1/query`
//! (`query-result-pagination`): a walk returns every row exactly once even
//! when the files under it are compacted between pages, and its cursor is
//! bound to the tenant that started it.

use crate::query_ir_e2e::{
    BASE_NS, build_router, log_record, logs_request, post_ir, post_ir_as, setup,
    test_tenant_context,
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
