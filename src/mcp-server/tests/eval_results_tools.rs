//! The `upload_eval_results` tool as an MCP client sees it (openspec change
//! `agent-offline-evals`, task 6.3): listed with its required parameters,
//! the file and run metadata reach `POST /api/v1/evals/results` with the
//! dataset as `X-Dataset-ID`, and the router's row errors come back in the
//! tool error.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};

mod common;

use common::{McpSession, connect, result_json, spawn_router, tool_error_message, tool_is_error};
use mcp_server::{McpAppState, mcp_http_router};

/// `(query string, X-Dataset-ID, body)` of every upload the mock saw.
type Seen = Arc<Mutex<Vec<(String, Option<String>, String)>>>;

async fn whoami() -> Response {
    axum::Json(serde_json::json!({
        "user_id": "user-a",
        "tenant": {"id": "acme", "slug": "acme", "name": "Acme"},
        "dataset": "production",
        "granted_tenants": [{"tenant_id": "acme"}],
    }))
    .into_response()
}

/// The upload endpoint stand-in: records each request; a body with a
/// `bad` row is rejected with a row error, anything else summarized.
async fn upload(State(seen): State<Seen>, headers: HeaderMap, uri: Uri, body: String) -> Response {
    let dataset = headers
        .get("x-dataset-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    seen.lock().expect("seen lock").push((
        uri.query().unwrap_or_default().to_string(),
        dataset,
        body.clone(),
    ));
    if body.contains("bad") {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({
                "status": "error", "errorType": "bad_data",
                "error": "invalid results file: row 2, `score`: must be a finite number",
                "details": [{"row": 2, "column": "score", "reason": "must be a finite number"}]
            })),
        )
            .into_response();
    }
    (
        StatusCode::CREATED,
        axum::Json(serde_json::json!({
            "run_id": "run-42", "agent": "support-triage", "version": "v1.9.0",
            "set": "triage-golden", "rows": 1, "cases": 1, "span_linked": 0, "run_level": 1,
            "evaluators": [{"name": "Correctness", "results": 1, "errors": 0,
                            "mean": 0.9, "pass_rate": 1.0}],
            "_links": {"query": {"href": "/api/v1/query", "method": "POST"},
                       "runs": {"href": "/evals/runs"}}
        })),
    )
        .into_response()
}

async fn app() -> (axum::Router, Seen) {
    let seen = Seen::default();
    let mock = axum::Router::new()
        .route("/api/v1/whoami", axum::routing::get(whoami))
        .route("/api/v1/evals/results", axum::routing::post(upload))
        .with_state(seen.clone());
    let router_url = spawn_router(mock).await;
    let state = McpAppState::new(router_url).with_router_timeout(Duration::from_secs(10));
    (mcp_http_router(state, &[]), seen)
}

fn args(content: &str) -> serde_json::Value {
    serde_json::json!({
        "tenant": "acme", "dataset": "staging", "content": content, "format": "csv",
        "agent": "support-triage", "version": "v1.9.0", "set": "triage-golden",
        "run_id": "run-42",
    })
}

#[tokio::test]
async fn upload_eval_results_is_listed_with_its_parameters() {
    let client = connect().await;
    let tools = client
        .list_tools(None)
        .await
        .expect("tools/list succeeds")
        .tools;
    let tool = tools
        .iter()
        .find(|t| t.name == "upload_eval_results")
        .expect("tool is listed");
    let schema = serde_json::Value::Object((*tool.input_schema).clone());
    let required: Vec<&str> = schema["required"]
        .as_array()
        .expect("required")
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    for param in [
        "tenant", "dataset", "content", "format", "agent", "version", "set",
    ] {
        assert!(required.contains(&param), "`{param}` required: {schema}");
    }
    assert!(!required.contains(&"run_id"), "{schema}");
    assert!(
        tool.description
            .as_deref()
            .unwrap_or_default()
            .contains("evals:write")
    );
    client.cancel().await.ok();
}

#[tokio::test]
async fn the_file_and_run_reach_the_router() {
    let (app, seen) = app().await;
    let mut session = McpSession::open(app, "sk-acme-write").await;

    let content = "case_id,name,score\ncase-1,Correctness,0.9\n";
    let reply = session
        .call_tool("upload_eval_results", args(content))
        .await;
    assert!(!tool_is_error(&reply), "{reply}");
    let run = result_json(&reply);
    assert_eq!(run["run_id"], "run-42");
    assert_eq!(run["evaluators"][0]["pass_rate"], 1.0);

    let seen = seen.lock().expect("seen lock");
    let (query, dataset, body) = seen.last().expect("one upload");
    assert_eq!(dataset.as_deref(), Some("staging"));
    assert_eq!(body, content);
    for pair in [
        "agent=support-triage",
        "version=v1.9.0",
        "set=triage-golden",
        "run_id=run-42",
        "format=csv",
    ] {
        assert!(query.contains(pair), "`{pair}` in `{query}`");
    }
}

#[tokio::test]
async fn without_a_run_id_the_tool_generates_and_sends_one() {
    let (app, seen) = app().await;
    let mut session = McpSession::open(app, "sk-acme-write").await;

    let mut arguments = args("case_id,name,score\ncase-1,Correctness,0.9\n");
    arguments.as_object_mut().expect("object").remove("run_id");
    let reply = session.call_tool("upload_eval_results", arguments).await;
    assert!(!tool_is_error(&reply), "{reply}");

    let seen = seen.lock().expect("seen lock");
    let (query, _, _) = seen.last().expect("one upload");
    let run_id = query
        .split('&')
        .find_map(|pair| pair.strip_prefix("run_id="))
        .expect("run_id sent");
    assert_eq!(run_id.len(), 36, "a UUID: {query}");
}

#[tokio::test]
async fn row_errors_come_back_in_the_tool_error() {
    let (app, _) = app().await;
    let mut session = McpSession::open(app, "sk-acme-write").await;

    let reply = session
        .call_tool(
            "upload_eval_results",
            args("case_id,name,score\ncase-1,Correctness,bad\n"),
        )
        .await;
    assert!(tool_is_error(&reply), "{reply}");
    let message = tool_error_message(&reply);
    assert!(
        message.contains("row 2, `score`: must be a finite number"),
        "{message}"
    );
}
