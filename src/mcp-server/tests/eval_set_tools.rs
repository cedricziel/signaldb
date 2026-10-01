//! The agent eval set tools as an MCP client sees them (openspec change
//! `agent-offline-evals`, tasks 8.3 and 5.6): the seven tools are listed with their
//! required parameters, read and write credentials reach the router through
//! them, the `dataset` argument reaches the router as `X-Dataset-ID`,
//! `get_eval_set` pages a set's cases, and a malformed case is rejected
//! before any router request is made.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};

mod common;

use common::{McpSession, connect, result_json, spawn_router, tool_error_message, tool_is_error};
use mcp_server::{McpAppState, mcp_http_router};

/// (tool, required parameters).
const EVAL_SET_TOOLS: &[(&str, &[&str])] = &[
    ("list_eval_sets", &["tenant", "dataset"]),
    ("get_eval_set", &["tenant", "dataset", "name"]),
    ("create_eval_set", &["tenant", "dataset", "name", "agent"]),
    ("replace_eval_set", &["tenant", "dataset", "name", "agent"]),
    ("delete_eval_set", &["tenant", "dataset", "name"]),
    ("append_eval_cases", &["tenant", "dataset", "name", "cases"]),
    (
        "append_eval_cases_from_traces",
        &["tenant", "dataset", "name"],
    ),
];

#[tokio::test]
async fn eval_set_tools_are_listed_with_their_parameters() {
    let client = connect().await;
    let tools = client
        .list_tools(None)
        .await
        .expect("tools/list succeeds")
        .tools;

    for (name, required) in EVAL_SET_TOOLS {
        let tool = tools
            .iter()
            .find(|t| t.name == *name)
            .unwrap_or_else(|| panic!("tool `{name}` is listed"));
        let schema = serde_json::Value::Object((*tool.input_schema).clone());
        let listed_required: Vec<&str> = schema
            .get("required")
            .and_then(|r| r.as_array())
            .map(|r| r.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();
        for param in *required {
            assert!(
                listed_required.contains(param),
                "`{name}` must require `{param}`; schema: {schema}"
            );
        }
        let description = tool.description.as_deref().unwrap_or_default();
        assert!(
            description.contains("evals:read") || description.contains("evals:write"),
            "`{name}` must name its scope: {description}"
        );
        if schema["properties"].get("cases").is_some() {
            let text = schema.to_string();
            assert!(
                text.contains("hand_written") && text.contains("trace_id"),
                "`{name}` must advertise the case source kinds: {schema}"
            );
        }
    }

    client.cancel().await.ok();
}

// ---------------------------------------------------------------------------
// Full HTTP harness — the tenant-scope check and the forwarded credential
// need the `Extension<Parts>` only the Streamable HTTP transport populates.
// ---------------------------------------------------------------------------

/// `(method, path, X-Dataset-ID, body)` of every eval-sets request the mock
/// saw.
type Seen = Arc<Mutex<Vec<(Method, String, Option<String>, serde_json::Value)>>>;

async fn whoami() -> Response {
    axum::Json(serde_json::json!({
        "user_id": "user-a",
        "tenant": {"id": "acme", "slug": "acme", "name": "Acme"},
        "dataset": "production",
        "memberships": [],
        "datasets": [],
        "default_dataset": null,
        "granted_tenants": [{"tenant_id": "acme"}],
    }))
    .into_response()
}

const SET_CASES: usize = 5;

fn set_body(dataset: &str) -> serde_json::Value {
    let cases: Vec<_> = (1..=SET_CASES)
        .map(|i| serde_json::json!({"id": format!("edge-{i:02}"), "input": "refund"}))
        .collect();
    serde_json::json!({
        "name": "refunds", "agent": "support-triage", "case_count": SET_CASES,
        "dataset": dataset, "tenant_id": "acme",
        "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z",
        "cases": cases,
        "_links": {"self": {"href": "/api/v1/eval-sets/refunds"}}
    })
}

/// The eval sets API stand-in: records each request; a mutation needs the
/// `sk-acme-write` bearer (mirroring the `evals:write` extractor), and every
/// set read is `refunds` with five cases, echoing the request's dataset.
async fn behaviour(
    State(seen): State<Seen>,
    headers: HeaderMap,
    uri: Uri,
    method: Method,
    body: axum::body::Bytes,
) -> Response {
    let Some(rest) = uri.path().strip_prefix("/api/v1/eval-sets") else {
        return axum::Json(serde_json::json!({})).into_response();
    };
    let dataset = headers
        .get("x-dataset-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let body = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    seen.lock()
        .expect("seen lock")
        .push((method.clone(), rest.to_string(), dataset.clone(), body));
    let dataset = dataset.unwrap_or_else(|| "production".to_string());
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if method != Method::GET && bearer != "Bearer sk-acme-write" {
        return (
            StatusCode::FORBIDDEN,
            axum::Json(serde_json::json!({
                "status": "error", "errorType": "forbidden",
                "error": "evals:write scope required"
            })),
        )
            .into_response();
    }
    match (method, rest) {
        (Method::GET, "") => axum::Json(serde_json::json!({
            "items": [],
            "_links": {"self": {"href": "/api/v1/eval-sets"}}
        }))
        .into_response(),
        (Method::POST, "") => (StatusCode::CREATED, axum::Json(set_body(&dataset))).into_response(),
        (Method::POST, "/refunds/cases/from-traces") => axum::Json(serde_json::json!({
            "matches": 214, "already_present": 12, "added": 50,
            "added_ids": ["trace-4bf92f3577b34da6"]
        }))
        .into_response(),
        (Method::POST, _) => axum::Json(serde_json::json!({
            "added": 1, "already_present": 1,
            "added_ids": ["edge-41"], "already_present_ids": ["edge-01"]
        }))
        .into_response(),
        (Method::DELETE, _) => StatusCode::NO_CONTENT.into_response(),
        _ => axum::Json(set_body(&dataset)).into_response(),
    }
}

async fn app() -> (axum::Router, Seen) {
    let seen = Seen::default();
    let mock = axum::Router::new()
        .route("/api/v1/whoami", axum::routing::get(whoami))
        .fallback(behaviour)
        .with_state(seen.clone());
    let router_url = spawn_router(mock).await;
    let state = McpAppState::new(router_url).with_router_timeout(Duration::from_secs(10));
    (mcp_http_router(state, &[]), seen)
}

#[tokio::test]
async fn read_scope_credential_can_call_read_tools() {
    let (app, _) = app().await;
    let mut session = McpSession::open(app, "sk-acme-read").await;

    let reply = session
        .call_tool(
            "list_eval_sets",
            serde_json::json!({"tenant": "acme", "dataset": "production"}),
        )
        .await;
    assert!(!tool_is_error(&reply), "list_eval_sets: {reply}");

    let reply = session
        .call_tool(
            "get_eval_set",
            serde_json::json!({"tenant": "acme", "dataset": "production", "name": "refunds"}),
        )
        .await;
    assert!(!tool_is_error(&reply), "get_eval_set: {reply}");
    assert_eq!(result_json(&reply)["cases"][0]["id"], "edge-01");
}

#[tokio::test]
async fn write_scope_credential_can_call_write_tools() {
    let (app, _) = app().await;
    let mut session = McpSession::open(app, "sk-acme-write").await;

    let reply = session
        .call_tool(
            "create_eval_set",
            serde_json::json!({
                "tenant": "acme", "dataset": "production", "name": "refunds",
                "agent": "support-triage",
                "cases": [{"id": "edge-01", "input": "refund",
                           "source": {"kind": "trace", "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736"}}]
            }),
        )
        .await;
    assert!(!tool_is_error(&reply), "create_eval_set: {reply}");

    let reply = session
        .call_tool(
            "replace_eval_set",
            serde_json::json!({"tenant": "acme", "dataset": "production", "name": "refunds",
                               "agent": "support-triage"}),
        )
        .await;
    assert!(!tool_is_error(&reply), "replace_eval_set: {reply}");

    let reply = session
        .call_tool(
            "append_eval_cases",
            serde_json::json!({"tenant": "acme", "dataset": "production", "name": "refunds",
                               "cases": [{"id": "edge-01", "input": "a"}, {"id": "edge-41", "input": "b"}]}),
        )
        .await;
    assert!(!tool_is_error(&reply), "append_eval_cases: {reply}");
    let outcome = result_json(&reply);
    assert_eq!(outcome["added"], 1);
    assert_eq!(outcome["already_present"], 1);

    let reply = session
        .call_tool(
            "delete_eval_set",
            serde_json::json!({"tenant": "acme", "dataset": "production", "name": "refunds"}),
        )
        .await;
    assert!(!tool_is_error(&reply), "delete_eval_set: {reply}");
    assert_eq!(result_json(&reply)["deleted"], true);
}

#[tokio::test]
async fn dataset_argument_reaches_the_router() {
    let (app, seen) = app().await;
    let mut session = McpSession::open(app, "sk-acme-write").await;

    let base = serde_json::json!({"tenant": "acme", "dataset": "staging", "name": "refunds",
                                  "agent": "support-triage", "cases": [{"id": "a", "input": "x"}]});
    for (tool, _) in EVAL_SET_TOOLS {
        let reply = session.call_tool(tool, base.clone()).await;
        assert!(!tool_is_error(&reply), "{tool}: {reply}");
    }

    let seen = seen.lock().expect("seen lock");
    assert_eq!(seen.len(), EVAL_SET_TOOLS.len(), "{seen:?}");
    for (method, path, dataset, _) in seen.iter() {
        assert_eq!(
            dataset.as_deref(),
            Some("staging"),
            "{method} {path} must carry X-Dataset-ID"
        );
    }
}

#[tokio::test]
async fn get_eval_set_pages_the_cases() {
    let (app, _) = app().await;
    let mut session = McpSession::open(app, "sk-acme-read").await;
    let args = |extra: serde_json::Value| {
        let mut args =
            serde_json::json!({"tenant": "acme", "dataset": "production", "name": "refunds"});
        args.as_object_mut()
            .expect("object")
            .extend(extra.as_object().expect("object").clone());
        args
    };

    let page = result_json(
        &session
            .call_tool(
                "get_eval_set",
                args(serde_json::json!({"offset": 1, "limit": 2})),
            )
            .await,
    );
    assert_eq!(page["total_cases"], SET_CASES);
    assert_eq!(page["offset"], 1);
    assert_eq!(page["returned"], 2);
    assert_eq!(page["has_more"], true);
    let ids: Vec<&str> = page["cases"]
        .as_array()
        .expect("cases")
        .iter()
        .filter_map(|c| c["id"].as_str())
        .collect();
    assert_eq!(ids, ["edge-02", "edge-03"]);
    assert_eq!(page["agent"], "support-triage");

    let last = result_json(
        &session
            .call_tool("get_eval_set", args(serde_json::json!({"offset": 4})))
            .await,
    );
    assert_eq!(last["returned"], 1);
    assert_eq!(last["has_more"], false);
}

#[tokio::test]
async fn trace_source_without_a_trace_id_is_rejected_before_any_router_request() {
    let (app, seen) = app().await;
    let mut session = McpSession::open(app, "sk-acme-write").await;

    let reply = session
        .call_tool(
            "append_eval_cases",
            serde_json::json!({"tenant": "acme", "dataset": "production", "name": "refunds",
                               "cases": [{"id": "edge-41", "input": "x", "source": {"kind": "trace"}}]}),
        )
        .await;
    assert!(tool_is_error(&reply), "{reply}");
    assert!(tool_error_message(&reply).contains("trace_id"), "{reply}");
    assert!(seen.lock().expect("seen lock").is_empty());
}

#[tokio::test]
async fn append_eval_cases_from_traces_sends_the_trace_query() {
    let (app, seen) = app().await;
    let mut session = McpSession::open(app, "sk-acme-write").await;

    let reply = session
        .call_tool(
            "append_eval_cases_from_traces",
            serde_json::json!({
                "tenant": "acme", "dataset": "production", "name": "refunds",
                "failing_evaluator": "Correctness", "sample": 50, "expected_tools": true,
                "filters": [{"field": "deployment.environment", "op": "eq", "value": "prod"}],
            }),
        )
        .await;
    assert!(!tool_is_error(&reply), "{reply}");
    let outcome = result_json(&reply);
    assert_eq!(outcome["matches"], 214);
    assert_eq!(outcome["already_present"], 12);
    assert_eq!(outcome["added"], 50);

    let seen = seen.lock().expect("seen lock");
    let (method, path, _, body) = seen.last().expect("one request");
    assert_eq!(
        (method, path.as_str()),
        (&Method::POST, "/refunds/cases/from-traces")
    );
    assert_eq!(
        body["range"],
        serde_json::json!({"from": "now-7d", "to": "now"})
    );
    assert_eq!(body["failing_evaluator"], "Correctness");
    assert_eq!(body["sample"], 50);
    assert_eq!(body["expected_tools"], true);
    assert_eq!(body["filters"][0]["field"], "deployment.environment");
}

#[tokio::test]
async fn append_eval_cases_from_traces_needs_the_write_credential() {
    let (app, _) = app().await;
    let mut session = McpSession::open(app, "sk-acme-read").await;
    let reply = session
        .call_tool(
            "append_eval_cases_from_traces",
            serde_json::json!({"tenant": "acme", "dataset": "production", "name": "refunds"}),
        )
        .await;
    assert!(tool_is_error(&reply), "{reply}");
}
