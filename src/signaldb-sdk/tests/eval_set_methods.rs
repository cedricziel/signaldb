//! Verifies the generated client exposes the eval-set operations
//! (openspec change `agent-offline-evals`, tasks 5.5 and 5.6).
//!
//! Compile-and-construct assertions: the surface is generated from
//! `api/signaldb-api.json`, so if an eval-set endpoint drops out of the
//! OpenAPI document this test stops compiling.

use signaldb_sdk::Client;
use signaldb_sdk::types::{
    AppendCasesFromTracesRequest, AppendCasesOutcome, AppendEvalCasesRequest, EvalCase,
    EvalCaseSource, EvalSetResponse, EvalSetSpec, QueryRange,
};

fn client() -> Client {
    Client::new(
        "http://localhost:8080",
        signaldb_sdk::RetryPolicy::default(),
    )
}

fn case(id: &str, source: Option<EvalCaseSource>) -> EvalCase {
    EvalCase {
        id: id.to_string(),
        input: "Where is my refund?".to_string(),
        expected_tools: vec!["lookup_order".to_string()],
        reference: None,
        tags: Vec::new(),
        source,
    }
}

#[test]
fn client_exposes_crud_builders() {
    let client = client();

    let _list = client.list_eval_sets();
    let _get = client.get_eval_set().name("triage-golden");
    let _delete = client.delete_eval_set().name("triage-golden");

    let spec = EvalSetSpec {
        name: "triage-golden".to_string(),
        agent: "support-triage".to_string(),
        description: None,
        cases: vec![case("edge-1", None)],
    };
    let _create = client.create_eval_set().body(spec.clone());
    let _replace = client.replace_eval_set().name("triage-golden").body(spec);
}

#[test]
fn client_exposes_append_builder() {
    let _append = client()
        .append_eval_cases()
        .name("triage-golden")
        .body(AppendEvalCasesRequest {
            cases: vec![case("edge-2", Some(EvalCaseSource::Upload))],
        });
}

#[test]
fn client_exposes_append_from_traces_builder() {
    let _append = client()
        .append_eval_cases_from_traces()
        .name("triage-golden")
        .body(AppendCasesFromTracesRequest {
            range: QueryRange {
                from: "now-7d".to_string(),
                to: "now".to_string(),
            },
            agent: None,
            operation: None,
            filters: Vec::new(),
            failing_evaluator: Some("Correctness".to_string()),
            sample: std::num::NonZeroU32::new(50),
            expected_tools: Some(true),
            reference_from_answer: None,
            tags: Vec::new(),
        });
}

/// The generated source enum speaks the server's `kind`-tagged wire format.
#[test]
fn case_source_round_trips_the_kind_tagged_shape() {
    let trace = EvalCaseSource::Trace("4bf92f3577b34da6a3ce929d0e0e4736".to_string());
    let json = serde_json::to_value(&trace).expect("serialize");
    assert_eq!(
        json,
        serde_json::json!({"kind": "trace", "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736"})
    );
    let upload: EvalCaseSource =
        serde_json::from_value(serde_json::json!({"kind": "upload"})).expect("upload");
    assert!(matches!(upload, EvalCaseSource::Upload));
    let hand: EvalCaseSource =
        serde_json::from_value(serde_json::json!({"kind": "hand_written"})).expect("hand");
    assert!(matches!(hand, EvalCaseSource::HandWritten));
}

#[test]
fn responses_deserialize() {
    let set: EvalSetResponse = serde_json::from_value(serde_json::json!({
        "tenant_id": "acme",
        "dataset": "production",
        "name": "triage-golden",
        "agent": "support-triage",
        "case_count": 1,
        "cases": [{"id": "edge-1", "input": "hi", "source": {"kind": "hand_written"}}],
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "_links": {"self": {"href": "/api/v1/eval-sets/triage-golden"}},
    }))
    .expect("eval set response deserializes");
    assert_eq!(set.case_count, 1);
    assert_eq!(set.cases[0].id, "edge-1");

    let outcome: AppendCasesOutcome = serde_json::from_value(serde_json::json!({
        "added": 1,
        "already_present": 1,
        "added_ids": ["edge-41"],
        "already_present_ids": ["edge-40"],
    }))
    .expect("append outcome deserializes");
    assert_eq!(outcome.added_ids, vec!["edge-41".to_string()]);
}
