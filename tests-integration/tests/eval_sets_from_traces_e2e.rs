//! Building an eval set from traces end to end (change: agent-offline-evals,
//! task 5.6): agent traces, tool spans and evaluator results are ingested
//! through the OTLP handlers, and `POST /api/v1/eval-sets/{name}/cases/from-traces`
//! reads them back through the Query IR (querier over Iceberg) into cases.

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use opentelemetry_proto::tonic::{
    common::v1::KeyValue,
    logs::v1::LogRecord,
    trace::v1::{Span, Status},
};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::query_ir_e2e::{
    BASE_NS, build_router, logs_request, post_ir_until_rows, range, setup, string_value,
    test_tenant_context, traces_request,
};

const AGENT: &str = "support-triage";

fn attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(string_value(value)),
        ..Default::default()
    }
}

fn trace_id(seq: u8) -> String {
    format!("{seq:02x}").repeat(16)
}

/// A span of trace `seq` starting `offset_ms` after `BASE_NS`.
fn genai_span(seq: u8, span: u8, offset_ms: i64, name: &str, attributes: Vec<KeyValue>) -> Span {
    let start = BASE_NS + offset_ms * 1_000_000;
    Span {
        trace_id: vec![seq; 16],
        span_id: vec![seq, span, 0, 0, 0, 0, 0, 1],
        parent_span_id: vec![],
        name: name.to_string(),
        kind: 1,
        start_time_unix_nano: start as u64,
        end_time_unix_nano: (start + 10_000_000) as u64,
        attributes,
        dropped_attributes_count: 0,
        events: vec![],
        dropped_events_count: 0,
        links: vec![],
        dropped_links_count: 0,
        status: Some(Status {
            code: 1,
            message: String::new(),
        }),
        trace_state: String::new(),
        flags: 0,
    }
}

fn messages(role: &str, text: &str) -> String {
    json!([{"role": role, "parts": [{"type": "text", "content": text}]}]).to_string()
}

/// An `invoke_agent` span for `agent` plus one `execute_tool` span per tool.
fn agent_trace(seq: u8, offset_ms: i64, agent: &str, question: &str, tools: &[&str]) -> Vec<Span> {
    let mut spans = vec![genai_span(
        seq,
        0,
        offset_ms,
        &format!("invoke_agent {agent}"),
        vec![
            attr("gen_ai.operation.name", "invoke_agent"),
            attr("gen_ai.agent.name", agent),
            attr("gen_ai.input.messages", &messages("user", question)),
            attr(
                "gen_ai.output.messages",
                &messages("assistant", &format!("answered: {question}")),
            ),
        ],
    )];
    for (i, tool) in tools.iter().enumerate() {
        let i = i as u8 + 1;
        spans.push(genai_span(
            seq,
            i,
            offset_ms + i64::from(i),
            &format!("execute_tool {tool}"),
            vec![
                attr("gen_ai.operation.name", "execute_tool"),
                attr("gen_ai.tool.name", tool),
            ],
        ));
    }
    spans
}

/// A `gen_ai.evaluation.result` log record linked to trace `seq`.
fn eval_result(seq: u8, offset_ms: i64, label: &str, error: Option<&str>) -> LogRecord {
    let mut attributes = vec![
        attr("gen_ai.evaluation.name", "Correctness"),
        attr("gen_ai.evaluation.score.label", label),
    ];
    if let Some(error) = error {
        attributes.push(attr("error.type", error));
    }
    let time = (BASE_NS + offset_ms * 1_000_000) as u64;
    LogRecord {
        time_unix_nano: time,
        observed_time_unix_nano: time,
        severity_number: 9,
        severity_text: "INFO".to_string(),
        body: None,
        attributes,
        dropped_attributes_count: 0,
        flags: 0,
        trace_id: vec![seq; 16],
        span_id: vec![seq, 0, 0, 0, 0, 0, 0, 1],
        event_name: "gen_ai.evaluation.result".to_string(),
    }
}

async fn call(app: &Router, method: &str, uri: &str, body: Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header("Authorization", "Bearer test-key-123")
        .header("X-Tenant-ID", "test-tenant")
        .header("Content-Type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request");
    let response = app.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

const FROM_TRACES: &str = "/api/v1/eval-sets/triage/cases/from-traces";

#[tokio::test]
async fn cases_are_built_from_agent_traces_and_failing_results() {
    let services = setup().await;
    let ctx = test_tenant_context();

    // Traces 1-3 are the agent's, oldest to newest; trace 4 is another agent.
    let mut spans = agent_trace(1, 100, AGENT, "where is order 1182?", &["lookup_order"]);
    spans.extend(agent_trace(
        2,
        200,
        AGENT,
        "refund order 1182",
        &["lookup_order", "issue_refund"],
    ));
    spans.extend(agent_trace(
        3,
        300,
        AGENT,
        "cancel my plan",
        &["cancel_plan"],
    ));
    spans.extend(agent_trace(
        4,
        400,
        "billing-bot",
        "invoice?",
        &["get_invoice"],
    ));
    services
        .trace_handler
        .handle_grpc_otlp_traces(&ctx, traces_request("agents", spans))
        .await
        .expect("ingest agent traces");
    // Trace 2 failed Correctness; trace 1's judge errored (never a failure);
    // trace 3 passed.
    services
        .log_handler
        .handle_grpc_otlp_logs(
            &ctx,
            logs_request(
                "evals",
                vec![
                    eval_result(1, 500, "fail", Some("timeout")),
                    eval_result(2, 500, "fail", None),
                    eval_result(3, 500, "pass", None),
                ],
            ),
        )
        .await
        .expect("ingest eval results");

    let app = build_router(&services).await;
    let (status, body) = post_ir_until_rows(
        &app,
        json!({"irVersion": 1, "from": "traces", "range": range(), "result": "rows",
               "fields": ["trace_id"], "pipeline": [
                   {"where": {"field": "gen_ai.operation.name", "op": "eq", "value": "execute_tool"}}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "spans persisted: {body}");
    let (status, body) = post_ir_until_rows(
        &app,
        json!({"irVersion": 1, "from": "logs", "range": range(), "result": "rows",
               "fields": ["trace_id"], "pipeline": [
                   {"where": {"field": "event_name", "op": "eq", "value": "gen_ai.evaluation.result"}}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "results persisted: {body}");

    let (status, body) = call(
        &app,
        "POST",
        "/api/v1/eval-sets",
        json!({"name": "triage", "agent": AGENT}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    // Only trace 2 failed Correctness.
    let (status, body) = call(
        &app,
        "POST",
        FROM_TRACES,
        json!({"range": range(), "failing_evaluator": "Correctness",
               "expected_tools": true, "reference_from_answer": true, "tags": ["regression"]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let case_2 = format!("trace-{}", &trace_id(2)[..16]);
    assert_eq!(
        body,
        json!({"matches": 1, "already_present": 0, "added": 1, "added_ids": [case_2]})
    );

    // Without the evaluator filter: the agent's three traces match, trace 2
    // is already present, and a sample of one takes the newest (trace 3).
    let (status, body) = call(
        &app,
        "POST",
        FROM_TRACES,
        json!({"range": range(), "sample": 1}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let case_3 = format!("trace-{}", &trace_id(3)[..16]);
    assert_eq!(
        body,
        json!({"matches": 3, "already_present": 1, "added": 1, "added_ids": [case_3]})
    );

    let (status, body) = call(&app, "GET", "/api/v1/eval-sets/triage", Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let cases = body["cases"].as_array().expect("cases");
    assert_eq!(cases.len(), 2, "{body}");
    assert_eq!(
        cases[0],
        json!({
            "id": case_2,
            "input": "refund order 1182",
            "expected_tools": ["lookup_order", "issue_refund"],
            "reference": "answered: refund order 1182",
            "tags": ["regression"],
            "source": {"kind": "trace", "trace_id": trace_id(2)},
        })
    );
    assert_eq!(cases[1]["input"], "cancel my plan");
    assert_eq!(cases[1]["expected_tools"], json!([]));
}
