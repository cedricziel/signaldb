//! The eval run read tools as an MCP client sees them (openspec change
//! `agent-offline-evals`, task 8.3b): `list_eval_runs` and
//! `compare_eval_runs` are listed with their required parameters, read
//! through the Query IR (`POST /api/v1/query`) with the dataset as
//! `X-Dataset-ID`, resolve `latest:<version>`, and report regressions with
//! their tool-call diffs.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use eval_model::test_util::{
    FakeIr, IrRows, RunRow, case_trace, latest, run_case, stats_row, tool_span,
};
use serde_json::{Value, json};

mod common;

use common::{McpSession, connect, result_json, spawn_router, tool_error_message, tool_is_error};
use mcp_server::{McpAppState, mcp_http_router};

/// `(X-Dataset-ID, IR document)` of every query the mock saw.
type Seen = Arc<Mutex<Vec<(Option<String>, Value)>>>;

const MIN_NS: i64 = 60_000_000_000;
/// 2026-01-01T00:00:00Z, long enough ago that every run is complete.
const T0: i64 = 1_767_225_600_000_000_000;

async fn whoami() -> Response {
    axum::Json(json!({
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

fn run_row(run: &str, version: &str, high: u64, score_sum: f64, start: i64) -> Vec<Value> {
    RunRow {
        run,
        set: "triage-golden",
        agent: Some("support-triage"),
        version,
        evaluator: "Correctness",
        n: 2,
        high,
        score_sum,
        first: start,
        last: start + MIN_NS,
        ..RunRow::default()
    }
    .row()
}

/// Two runs of `support-triage` on `triage-golden`: `refund-1` regressed.
fn ir_fake() -> FakeIr {
    FakeIr::new(IrRows {
        runs: vec![
            run_row("run-v2", "v2", 1, 1.1, T0 + 10 * MIN_NS),
            run_row("run-v1", "v1", 2, 1.7, T0),
        ],
        run_cases: vec![
            run_case("run-v1", "refund-1"),
            run_case("run-v1", "refund-2"),
            run_case("run-v2", "refund-1"),
            run_case("run-v2", "refund-2"),
        ],
        case_stats: vec![
            stats_row("run-v1", "refund-1", "Correctness", "pass", 0.9),
            stats_row("run-v1", "refund-2", "Correctness", "pass", 0.8),
            stats_row("run-v2", "refund-1", "Correctness", "fail", 0.2),
            stats_row("run-v2", "refund-2", "Correctness", "pass", 0.9),
        ],
        case_traces: vec![
            case_trace("run-v1", "refund-1", "trace-base"),
            case_trace("run-v2", "refund-1", "trace-cand"),
        ],
        latest: vec![latest("run-v1", T0 + MIN_NS)],
        tool_spans: vec![
            tool_span("trace-base", 1, "lookup_order"),
            tool_span("trace-base", 2, "check_policy"),
            tool_span("trace-base", 3, "issue_refund"),
            tool_span("trace-cand", 1, "lookup_order"),
            tool_span("trace-cand", 2, "issue_refund"),
        ],
    })
}

/// The Query IR stand-in: records the dataset and document, and answers
/// through [`FakeIr`].
async fn query(
    State((seen, fake)): State<(Seen, Arc<FakeIr>)>,
    headers: HeaderMap,
    body: String,
) -> Response {
    let doc: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    let dataset = headers
        .get("x-dataset-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    seen.lock().expect("seen lock").push((dataset, doc));
    axum::Json(fake.response_body(body.as_bytes())).into_response()
}

async fn app() -> (axum::Router, Seen) {
    let seen = Seen::default();
    let mock = axum::Router::new()
        .route("/api/v1/whoami", axum::routing::get(whoami))
        .route("/api/v1/query", axum::routing::post(query))
        .with_state((seen.clone(), Arc::new(ir_fake())));
    let router_url = spawn_router(mock).await;
    let state = McpAppState::new(router_url).with_router_timeout(Duration::from_secs(10));
    (mcp_http_router(state, &[]), seen)
}

#[tokio::test]
async fn eval_run_tools_are_listed_with_their_parameters() {
    let client = connect().await;
    let tools = client
        .list_tools(None)
        .await
        .expect("tools/list succeeds")
        .tools;
    for (name, required) in [
        ("list_eval_runs", &["tenant", "dataset"][..]),
        (
            "compare_eval_runs",
            &["tenant", "dataset", "baseline", "candidate"][..],
        ),
    ] {
        let tool = tools
            .iter()
            .find(|t| t.name == name)
            .unwrap_or_else(|| panic!("`{name}` is listed"));
        let schema = Value::Object((*tool.input_schema).clone());
        let listed: Vec<&str> = schema["required"]
            .as_array()
            .expect("required")
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        assert_eq!(listed.len(), required.len(), "`{name}`: {schema}");
        for param in required {
            assert!(listed.contains(param), "`{name}` requires `{param}`");
        }
        let description = tool.description.as_deref().unwrap_or_default();
        assert!(description.contains("logs:read"), "{description}");
        assert_eq!(
            tool.annotations.as_ref().and_then(|a| a.read_only_hint),
            Some(true)
        );
    }
    client.cancel().await.ok();
}

#[tokio::test]
async fn list_eval_runs_reads_runs_through_the_query_ir() {
    let (app, seen) = app().await;
    let mut session = McpSession::open(app, "sk-acme-read").await;

    let reply = session
        .call_tool(
            "list_eval_runs",
            json!({"tenant": "acme", "dataset": "staging", "agent": "support-triage", "limit": 1}),
        )
        .await;
    assert!(!tool_is_error(&reply), "{reply}");
    let list = result_json(&reply);
    assert_eq!(list["total_runs"], 2);
    assert_eq!(list["returned"], 1);
    assert_eq!(list["truncated"], true);
    assert!(
        list["note"]
            .as_str()
            .unwrap_or_default()
            .contains("newest 1 of 2")
    );
    let run = &list["runs"][0];
    assert_eq!(run["run_id"], "run-v2");
    assert_eq!(run["version"], "v2");
    assert_eq!(run["status"], "complete");
    assert_eq!(run["cases"], 2);
    assert_eq!(run["previous_run_id"], "run-v1");
    assert_eq!(run["evaluators"][0]["name"], "Correctness");
    assert_eq!(run["evaluators"][0]["pass_rate"], 0.5);

    let seen = seen.lock().expect("seen lock");
    assert!(!seen.is_empty());
    for (dataset, doc) in seen.iter() {
        assert_eq!(dataset.as_deref(), Some("staging"));
        assert_eq!(doc["from"], "logs");
        assert_eq!(doc["range"]["from"], "now-7d");
        assert!(
            doc["pipeline"].to_string().contains("support-triage"),
            "agent filter: {doc}"
        );
    }
}

#[tokio::test]
async fn compare_eval_runs_resolves_latest_and_lists_regressions() {
    let (app, seen) = app().await;
    let mut session = McpSession::open(app, "sk-acme-read").await;

    let reply = session
        .call_tool(
            "compare_eval_runs",
            json!({"tenant": "acme", "dataset": "production",
                   "baseline": "latest:v1", "candidate": "run-v2", "include_tools": true}),
        )
        .await;
    assert!(!tool_is_error(&reply), "{reply}");
    let cmp = result_json(&reply);
    assert_eq!(cmp["baseline"]["run_id"], "run-v1");
    assert_eq!(cmp["candidate"]["run_id"], "run-v2");
    assert_eq!(cmp["counts"]["regressions"], 1);
    assert_eq!(cmp["counts"]["improvements"], 1);
    assert_eq!(cmp["evaluators"][0]["worse"], 1);
    assert_eq!(cmp["evaluators"][0]["delta"]["unit"], "score");
    let case = &cmp["regressions"][0];
    assert_eq!(case["case_id"], "refund-1");
    assert_eq!(case["evaluators"][0]["name"], "Correctness");
    assert_eq!(case["evaluators"][0]["change"], "worse");
    assert_eq!(case["evaluators"][0]["baseline"]["verdict"], "pass");
    assert_eq!(case["evaluators"][0]["candidate"]["verdict"], "fail");
    assert_eq!(case["baseline_trace_id"], "trace-base");
    assert_eq!(case["candidate_trace_id"], "trace-cand");
    assert_eq!(
        case["tools"],
        json!([
            {"name": "lookup_order", "kind": "same"},
            {"name": "check_policy", "kind": "skipped"},
            {"name": "issue_refund", "kind": "same"},
        ])
    );

    let seen = seen.lock().expect("seen lock");
    let latest = seen
        .iter()
        .map(|(_, doc)| doc)
        .find(|doc| {
            doc["pipeline"].as_array().is_some_and(|stages| {
                stages.iter().any(|s| {
                    s["where"]["field"] == "signaldb.eval.run_id" && s["where"]["op"] == "ne"
                })
            })
        })
        .expect("latest:v1 resolved through the IR");
    let pipeline = latest["pipeline"].to_string();
    for part in ["support-triage", "\"v1\"", "triage-golden", "run-v2"] {
        assert!(pipeline.contains(part), "`{part}` in {pipeline}");
    }
    assert!(
        seen.iter().any(|(_, doc)| doc["from"] == "traces"),
        "tool spans read"
    );
}

#[tokio::test]
async fn an_unknown_run_or_reference_is_a_tool_error() {
    let (app, seen) = app().await;
    let mut session = McpSession::open(app, "sk-acme-read").await;

    let reply = session
        .call_tool(
            "compare_eval_runs",
            json!({"tenant": "acme", "dataset": "production",
                   "baseline": "run-gone", "candidate": "run-v2"}),
        )
        .await;
    assert!(tool_is_error(&reply), "{reply}");
    assert!(
        tool_error_message(&reply).contains("run `run-gone` has no results"),
        "{reply}"
    );

    let before = seen.lock().expect("seen lock").len();
    let reply = session
        .call_tool(
            "compare_eval_runs",
            json!({"tenant": "acme", "dataset": "production",
                   "baseline": "latest:", "candidate": "run-v2"}),
        )
        .await;
    assert!(tool_is_error(&reply), "{reply}");
    assert!(tool_error_message(&reply).contains("`baseline`"), "{reply}");
    assert_eq!(
        seen.lock().expect("seen lock").len(),
        before,
        "no query sent"
    );
}
