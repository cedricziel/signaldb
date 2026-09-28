//! Uploading an eval results file end to end (change: agent-offline-evals,
//! task 6.4): `POST /api/v1/evals/results` writes the rows through the log
//! ingest path (writer WAL → Iceberg), and the run reads back over the Query
//! IR like one sent over OTLP, with the pass rate the shared pass rule gives.
//! A result sent as a span event (task 7.x) reads back from `logs` the same way.

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use common::evals::{EvalResult, Verdict, verdict_of_result};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::query_ir_e2e::{
    BASE_NS, build_router, post_ir_until_rows, range, setup, span, string_value, traces_request,
};

const RUN_ID: &str = "run-upload-e2e";
const TRACE_1: &str = "0af7651916cd43dd8448eb211c80319c";
const TRACE_2: &str = "4bf92f3577b34da6a3ce929d0e0e4736";

/// Three Correctness results (a pass and a fail by score, and a judge
/// timeout that is never a failure whatever its score) plus one run-level
/// Tone result with no trace context.
fn results_csv() -> String {
    format!(
        "case_id,name,score,label,trace_id,span_id,error\n\
         case-1,Correctness,0.9,,{TRACE_1},00f067aa0ba902b7,\n\
         case-2,Correctness,0.2,,{TRACE_2},,\n\
         case-3,Correctness,0,,{TRACE_2},,timeout\n\
         case-1,Tone,,pass,,,\n"
    )
}

async fn upload(app: &Router, body: String) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri(format!(
            "/api/v1/evals/results?agent=support-triage&version=v1.9.0&set=triage-golden&run_id={RUN_ID}"
        ))
        .header("Authorization", "Bearer test-key-123")
        .header("X-Tenant-ID", "test-tenant")
        .header("Content-Type", "text/csv")
        .body(Body::from(body))
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

fn run_results_document(result: &str, extra: Value) -> Value {
    let mut doc = json!({
        "irVersion": 4,
        "from": "logs",
        "range": {"from": "now-1h", "to": "now"},
        "result": result,
        "pipeline": [
            {"where": {"field": "event_name", "op": "eq", "value": "gen_ai.evaluation.result"}},
            {"where": {"field": "signaldb.eval.run_id", "op": "eq", "value": RUN_ID}}
        ]
    });
    if let (Some(doc), Some(extra)) = (doc.as_object_mut(), extra.as_object()) {
        for (key, value) in extra {
            match (key.as_str(), value) {
                ("pipeline", Value::Array(stages)) => {
                    if let Some(Value::Array(pipeline)) = doc.get_mut("pipeline") {
                        pipeline.extend(stages.iter().cloned());
                    }
                }
                _ => {
                    doc.insert(key.clone(), value.clone());
                }
            }
        }
    }
    doc
}

fn column(body: &Value, name: &str) -> usize {
    body["columns"]
        .as_array()
        .and_then(|cols| cols.iter().position(|c| c["name"] == name))
        .unwrap_or_else(|| panic!("column `{name}` in {body}"))
}

#[tokio::test]
async fn an_uploaded_run_is_queryable_over_the_ir_with_its_pass_rate() {
    let services = setup().await;
    let app = build_router(&services).await;

    let (status, summary) = upload(&app, results_csv()).await;
    assert_eq!(status, StatusCode::CREATED, "{summary}");
    assert_eq!(summary["run_id"], RUN_ID);
    assert_eq!(summary["rows"], 4);
    assert_eq!(summary["run_level"], 1);

    // Every row is stored, each with its run attributes and trace context.
    let fields = [
        "gen_ai.evaluation.name",
        "gen_ai.evaluation.score.value",
        "gen_ai.evaluation.score.label",
        "error.type",
        "trace_id",
        "signaldb.eval.case_id",
        "signaldb.eval.set",
        "gen_ai.agent.version",
    ];
    let (status, body) = post_ir_until_rows(
        &app,
        run_results_document("rows", json!({"irVersion": 1, "fields": fields})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 4, "{body}");
    for row in rows {
        assert_eq!(row[6], "triage-golden", "{row}");
        assert_eq!(row[7], "v1.9.0", "{row}");
    }
    let run_level = rows
        .iter()
        .filter(|r| r[4].as_str().is_none_or(str::is_empty))
        .count();
    assert_eq!(run_level, 1, "one result has no trace context: {body}");

    // The pass rule over the stored Correctness results: the timeout is an
    // evaluator error, so the pass rate is 1 pass of 2 verdicts.
    let (mut passes, mut verdicts, mut errors) = (0, 0, 0);
    for row in rows.iter().filter(|r| r[0] == "Correctness") {
        let error = row[3].as_str().filter(|e| !e.is_empty());
        if error.is_some() {
            errors += 1;
        }
        match verdict_of_result(EvalResult {
            error,
            label: row[2].as_str().filter(|l| !l.is_empty()),
            score: row[1].as_f64(),
        }) {
            Some(Verdict::Pass) => {
                passes += 1;
                verdicts += 1;
            }
            Some(Verdict::Fail) => verdicts += 1,
            None => {}
        }
    }
    assert_eq!((passes, verdicts, errors), (1, 2, 1), "{body}");
    assert_eq!(summary["evaluators"][0]["pass_rate"], 0.5, "{summary}");

    // The run as `signaldb-cli evals upload --compare-to latest:<version>`
    // finds it: grouped by run id, with its last result time.
    let (status, body) = post_ir_until_rows(
        &app,
        run_results_document(
            "table",
            json!({"pipeline": [{"aggregate": {
                "by": ["signaldb.eval.run_id"],
                "aggs": [{"fn": "max", "of": "timestamp", "as": "last"}]
            }}]}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 1, "{body}");
    assert_eq!(rows[0][column(&body, "signaldb_eval_run_id")], RUN_ID);
    assert!(rows[0][column(&body, "last")].as_i64().is_some(), "{body}");
}

#[tokio::test]
async fn an_invalid_file_writes_nothing() {
    let services = setup().await;
    let app = build_router(&services).await;

    let (status, body) = upload(&app, "name,score\nCorrectness,0.5\n".to_string()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["details"][0]["column"], "case_id", "{body}");
}

/// Task 7.1/7.2: a result sent as a `gen_ai.evaluation.result` span event
/// over `/v1/traces` is stored as a log record, so it reads back over the IR
/// from `logs` with the span's trace id and the agent identity copied from
/// the span.
#[tokio::test]
async fn a_span_event_result_is_queryable_from_logs_with_the_span_trace_id() {
    use opentelemetry_proto::tonic::common::v1::KeyValue;
    use opentelemetry_proto::tonic::trace::v1::span::Event;

    let services = setup().await;
    let app = build_router(&services).await;

    let kv = |key: &str, value: &str| KeyValue {
        key: key.to_string(),
        value: Some(string_value(value)),
        ..Default::default()
    };
    let mut agent_span = span("invoke_agent triage", 7, 2_000_000_000);
    agent_span.attributes = vec![
        kv(common::evals::AGENT_NAME, "support-triage"),
        kv(common::evals::AGENT_VERSION, "v2.0.0"),
    ];
    agent_span.events = vec![Event {
        time_unix_nano: (BASE_NS + 1_000_000_000) as u64,
        name: common::evals::EVALUATION_RESULT_EVENT.to_string(),
        attributes: vec![
            kv(common::evals::EVALUATION_NAME, "Correctness"),
            kv(common::evals::EVALUATION_SCORE_LABEL, "pass"),
        ],
        dropped_attributes_count: 0,
    }];
    let trace_id_hex = hex_id(&agent_span.trace_id);

    let export = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("Authorization", "Bearer test-key-123")
        .header("X-Tenant-ID", "test-tenant")
        .header("Content-Type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&traces_request("support-triage", vec![agent_span])).expect("json"),
        ))
        .expect("request");
    let response = app.clone().oneshot(export).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);

    let (status, body) = post_ir_until_rows(
        &app,
        json!({
            "irVersion": 1,
            "from": "logs",
            "range": range(),
            "result": "rows",
            "fields": ["trace_id", "gen_ai.evaluation.name", "gen_ai.agent.version"],
            "pipeline": [
                {"where": {"field": "event_name", "op": "eq", "value": "gen_ai.evaluation.result"}}
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 1, "{body}");
    assert_eq!(rows[0][0], trace_id_hex, "{body}");
    assert_eq!(rows[0][1], "Correctness", "{body}");
    assert_eq!(rows[0][2], "v2.0.0", "{body}");
}

fn hex_id(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
