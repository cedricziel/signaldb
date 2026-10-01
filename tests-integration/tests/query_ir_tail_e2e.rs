//! End-to-end live tail over `POST /api/v1/query` (`query-live-tail`):
//! successive calls deliver each new row once and in order, a long span is
//! delivered by its end time, and a row that becomes queryable behind the
//! cursor is not delivered (the documented at-most-once case).

use crate::query_ir_e2e::{
    BASE_NS, TestServices, build_router, log_record, logs_request, post_ir, setup_with, span,
    test_tenant_context, traces_request,
};
use axum::Router;
use axum::http::StatusCode;
use std::time::Duration;
use tokio::time::sleep;

const S: i64 = 1_000_000_000;

fn now_ns() -> i64 {
    chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
}

fn tail_document(source: &str, field: &str, cursor: Option<&str>) -> serde_json::Value {
    let mut tail = serde_json::json!({ "settle": "1s" });
    if let Some(cursor) = cursor {
        tail["cursor"] = serde_json::json!(cursor);
    }
    serde_json::json!({
        "irVersion": 15, "from": source,
        "range": { "from": "now-5m", "to": "now" },
        "result": "rows", "fields": [field], "pipeline": [],
        "page": { "size": 100 }, "tail": tail,
    })
}

fn cells(body: &serde_json::Value) -> Vec<String> {
    body["rows"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .map(|row| row[0].as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default()
}

async fn ingest_log(services: &TestServices, at_ns: i64, body: &str) {
    services
        .log_handler
        .handle_grpc_otlp_logs(
            &test_tenant_context(),
            logs_request("api", vec![log_record(at_ns - BASE_NS, "INFO", body)]),
        )
        .await
        .expect("ingest log");
}

/// Wait until `body` is queryable through an absolute-range query.
async fn until_visible(app: &Router, body: &str) {
    for _ in 0..40 {
        let (_, response) = post_ir(
            app,
            serde_json::json!({
                "irVersion": 1, "from": "logs",
                "range": { "from": (now_ns() - 600 * S).to_string(), "to": now_ns().to_string() },
                "result": "rows", "fields": ["body"],
                "pipeline": [{ "where": { "field": "body", "op": "eq", "value": body } }],
            }),
        )
        .await;
        if !cells(&response).is_empty() {
            return;
        }
        sleep(Duration::from_millis(500)).await;
    }
    panic!("{body} never became queryable");
}

/// Poll the tail until `wanted` shows up, returning every row delivered
/// meanwhile and the last cursor.
async fn tail_until(
    app: &Router,
    source: &str,
    field: &str,
    mut cursor: String,
    wanted: &str,
) -> (Vec<String>, String) {
    let mut delivered = Vec::new();
    for _ in 0..40 {
        let (status, body) = post_ir(app, tail_document(source, field, Some(&cursor))).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        delivered.extend(cells(&body));
        cursor = body["tail"]["cursor"].as_str().expect("cursor").to_string();
        if delivered.iter().any(|d| d == wanted) {
            return (delivered, cursor);
        }
        sleep(Duration::from_millis(500)).await;
    }
    panic!("the tail never delivered {wanted}; got {delivered:?}");
}

#[tokio::test]
async fn a_log_tail_delivers_each_new_row_once_and_skips_late_ones() {
    let services = setup_with(|config| {
        config.querier.tail_min_settle = Duration::from_secs(1);
    })
    .await;
    let app = build_router(&services).await;

    let start = now_ns();
    ingest_log(&services, start - 20 * S, "first").await;
    until_visible(&app, "first").await;

    let (status, first) = post_ir(&app, tail_document("logs", "body", None)).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(cells(&first), ["first"]);
    assert_eq!(first["tail"]["settle_ns"], 1_000_000_000_i64);
    let cursor = first["tail"]["cursor"]
        .as_str()
        .expect("cursor")
        .to_string();
    let settled = first["tail"]["settled_through_ns"]
        .as_i64()
        .expect("settled");

    // A row stamped behind the cursor, queryable only now: never delivered.
    ingest_log(&services, settled - 60 * S, "late").await;
    until_visible(&app, "late").await;
    sleep(Duration::from_secs(2)).await;
    ingest_log(&services, now_ns(), "second").await;
    ingest_log(&services, now_ns() + 1_000, "third").await;
    until_visible(&app, "third").await;

    let (delivered, _) = tail_until(&app, "logs", "body", cursor, "third").await;
    assert_eq!(delivered, ["second", "third"], "once each, oldest first");
}

#[tokio::test]
async fn a_traces_tail_delivers_a_long_span_by_its_end_time() {
    let services = setup_with(|config| {
        config.querier.tail_min_settle = Duration::from_secs(1);
    })
    .await;
    let app = build_router(&services).await;

    let (status, first) = post_ir(&app, tail_document("traces", "span.name", None)).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let cursor = first["tail"]["cursor"]
        .as_str()
        .expect("cursor")
        .to_string();

    sleep(Duration::from_secs(2)).await;
    // Started 90s ago, before the tail began; ended just now.
    let end = now_ns();
    let mut long = span("long-span", 7, 0);
    long.start_time_unix_nano = (end - 90 * S) as u64;
    long.end_time_unix_nano = end as u64;
    services
        .trace_handler
        .handle_grpc_otlp_traces(&test_tenant_context(), traces_request("api", vec![long]))
        .await
        .expect("ingest span");

    // Visible before the tail moves on: the test's 1s settle is below the
    // writer's commit lag, which would otherwise make the span late.
    for _ in 0..40 {
        let (_, response) = post_ir(
            &app,
            serde_json::json!({
                "irVersion": 1, "from": "traces",
                "range": { "from": (end - 600 * S).to_string(), "to": now_ns().to_string() },
                "result": "rows", "fields": ["span.name"], "pipeline": [],
            }),
        )
        .await;
        if !cells(&response).is_empty() {
            break;
        }
        sleep(Duration::from_millis(500)).await;
    }
    let (delivered, _) = tail_until(&app, "traces", "span.name", cursor, "long-span").await;
    assert_eq!(delivered, ["long-span"]);
}
