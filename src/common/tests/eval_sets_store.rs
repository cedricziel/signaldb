//! Eval-set catalog storage (`common::eval_sets::store`, change:
//! agent-offline-evals, tasks 5.1/5.2). SQLite in-memory here; the Postgres
//! twin lives in `tests/eval_sets_store_postgres.rs`.

use common::catalog::Catalog;
use common::eval_sets::{EvalCase, EvalCaseSource, EvalCaseSourceCounts, EvalSetSpec, StoreError};

const TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";

async fn catalog_with(tenant: &str, datasets: &[&str]) -> Catalog {
    let catalog = Catalog::new_in_memory().await.expect("catalog");
    catalog
        .upsert_tenant(tenant, tenant, None, "config")
        .await
        .expect("upsert tenant");
    for dataset in datasets {
        catalog
            .ensure_dataset(tenant, dataset)
            .await
            .expect("ensure dataset");
    }
    catalog
}

fn case(id: &str) -> EvalCase {
    EvalCase {
        id: id.to_string(),
        input: format!("input for {id}"),
        expected_tools: vec!["lookup_order".to_string(), "issue_refund".to_string()],
        reference: Some(format!("reference for {id}")),
        tags: vec!["refunds".to_string()],
        source: EvalCaseSource::HandWritten,
    }
}

fn spec(name: &str, case_ids: &[&str]) -> EvalSetSpec {
    EvalSetSpec {
        name: name.to_string(),
        agent: "support-triage".to_string(),
        description: Some("edge cases".to_string()),
        cases: case_ids.iter().map(|id| case(id)).collect(),
    }
}

fn ids(cases: &[EvalCase]) -> Vec<&str> {
    cases.iter().map(|c| c.id.as_str()).collect()
}

#[tokio::test]
async fn insert_get_replace_delete_round_trip_keeps_case_order() {
    let catalog = catalog_with("acme", &["prod"]).await;

    let mut body = spec("refund-edge-cases", &["edge-3", "edge-1", "edge-2"]);
    body.cases[0].source = EvalCaseSource::Trace {
        trace_id: TRACE_ID.to_string(),
    };
    body.cases[1].source = EvalCaseSource::Upload;
    body.cases[2].expected_tools = Vec::new();
    body.cases[2].reference = None;

    let created = catalog
        .insert_eval_set("acme", "prod", body.clone())
        .await
        .expect("insert");
    assert_eq!(created.tenant_id, "acme");
    assert_eq!(created.dataset, "prod");
    assert_eq!(created.summary.name, "refund-edge-cases");
    assert_eq!(created.summary.agent, "support-triage");
    assert_eq!(created.summary.case_count, 3);
    assert_eq!(created.cases, body.cases, "cases round-trip in order");
    assert_eq!(created.summary.created_at, created.summary.updated_at);

    let fetched = catalog
        .get_eval_set("acme", "prod", "refund-edge-cases")
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(fetched, created);

    let mut replacement = spec("refund-edge-cases", &["new-2", "new-1"]);
    replacement.agent = "support-triage-v2".to_string();
    replacement.description = None;
    let replaced = catalog
        .replace_eval_set("acme", "prod", "refund-edge-cases", replacement)
        .await
        .expect("replace");
    assert_eq!(replaced.summary.agent, "support-triage-v2");
    assert!(replaced.summary.description.is_none());
    assert_eq!(ids(&replaced.cases), vec!["new-2", "new-1"]);
    assert_eq!(replaced.summary.case_count, 2);
    assert_eq!(replaced.summary.created_at, created.summary.created_at);
    assert!(replaced.summary.updated_at >= created.summary.updated_at);

    assert!(
        catalog
            .delete_eval_set("acme", "prod", "refund-edge-cases")
            .await
            .expect("delete")
    );
    assert!(
        catalog
            .get_eval_set("acme", "prod", "refund-edge-cases")
            .await
            .expect("get")
            .is_none()
    );
    assert!(
        !catalog
            .delete_eval_set("acme", "prod", "refund-edge-cases")
            .await
            .expect("delete again")
    );
}

#[tokio::test]
async fn list_returns_summaries_with_case_and_source_counts_sorted_by_name() {
    let catalog = catalog_with("acme", &["prod", "staging"]).await;
    let mut triage = spec("triage-golden", &["a", "b", "c", "d"]);
    triage.cases[0].source = EvalCaseSource::Trace {
        trace_id: TRACE_ID.to_string(),
    };
    triage.cases[1].source = EvalCaseSource::Trace {
        trace_id: TRACE_ID.to_string(),
    };
    triage.cases[2].source = EvalCaseSource::Upload;
    catalog
        .insert_eval_set("acme", "prod", triage)
        .await
        .expect("insert triage");
    catalog
        .insert_eval_set("acme", "prod", spec("empty-set", &[]))
        .await
        .expect("insert empty");
    // Same set name in another dataset: its cases must not be counted.
    catalog
        .insert_eval_set("acme", "staging", spec("triage-golden", &["x", "y"]))
        .await
        .expect("insert staging");

    let listed = catalog.list_eval_sets("acme", "prod").await.expect("list");
    let rows: Vec<(&str, u64, EvalCaseSourceCounts)> = listed
        .iter()
        .map(|l| (l.summary.name.as_str(), l.summary.case_count, l.sources))
        .collect();
    assert_eq!(
        rows,
        vec![
            ("empty-set", 0, EvalCaseSourceCounts::default()),
            (
                "triage-golden",
                4,
                EvalCaseSourceCounts {
                    trace: 2,
                    upload: 1,
                    hand_written: 1
                }
            ),
        ]
    );
    assert_eq!(listed[1].summary.agent, "support-triage");
    assert_eq!(listed[1].summary.description.as_deref(), Some("edge cases"));
}

#[tokio::test]
async fn duplicate_name_is_a_conflict() {
    let catalog = catalog_with("acme", &["prod"]).await;
    catalog
        .insert_eval_set("acme", "prod", spec("dupe", &["a"]))
        .await
        .expect("first insert");
    let err = catalog
        .insert_eval_set("acme", "prod", spec("dupe", &["b"]))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Conflict(name) if name == "dupe"));

    let kept = catalog
        .get_eval_set("acme", "prod", "dupe")
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(
        ids(&kept.cases),
        vec!["a"],
        "a conflict leaves the set as it was"
    );
}

#[tokio::test]
async fn replace_and_append_on_a_missing_set_are_not_found() {
    let catalog = catalog_with("acme", &["prod"]).await;
    let err = catalog
        .replace_eval_set("acme", "prod", "missing", spec("missing", &[]))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::NotFound(name) if name == "missing"));

    let err = catalog
        .append_eval_cases("acme", "prod", "missing", vec![case("a")])
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::NotFound(name) if name == "missing"));
}

#[tokio::test]
async fn replace_rejects_a_body_name_that_differs_from_the_path() {
    let catalog = catalog_with("acme", &["prod"]).await;
    catalog
        .insert_eval_set("acme", "prod", spec("one", &[]))
        .await
        .expect("insert");
    let err = catalog
        .replace_eval_set("acme", "prod", "one", spec("two", &[]))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Invalid(_)));
}

#[tokio::test]
async fn append_skips_existing_ids_and_appends_the_rest_in_order() {
    let catalog = catalog_with("acme", &["prod"]).await;
    let created = catalog
        .insert_eval_set("acme", "prod", spec("golden", &["edge-39", "edge-40"]))
        .await
        .expect("insert");

    let mut replacement_40 = case("edge-40");
    replacement_40.input = "must not overwrite".to_string();
    let outcome = catalog
        .append_eval_cases(
            "acme",
            "prod",
            "golden",
            vec![case("edge-42"), replacement_40, case("edge-41")],
        )
        .await
        .expect("append");
    assert_eq!(outcome.added, 2);
    assert_eq!(outcome.already_present, 1);
    assert_eq!(outcome.added_ids, vec!["edge-42", "edge-41"]);
    assert_eq!(outcome.already_present_ids, vec!["edge-40"]);

    let set = catalog
        .get_eval_set("acme", "prod", "golden")
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(
        ids(&set.cases),
        vec!["edge-39", "edge-40", "edge-42", "edge-41"]
    );
    assert_eq!(set.cases[1].input, "input for edge-40");
    assert_eq!(set.summary.case_count, 4);
    assert!(set.summary.updated_at >= created.summary.updated_at);
    assert_eq!(set.summary.created_at, created.summary.created_at);

    let outcome = catalog
        .append_eval_cases("acme", "prod", "golden", vec![case("edge-39")])
        .await
        .expect("append nothing new");
    assert_eq!(outcome.added, 0);
    assert_eq!(outcome.already_present, 1);
}

#[tokio::test]
async fn deleting_a_dataset_cascades_its_sets_and_cases() {
    let catalog = catalog_with("acme", &["prod", "staging"]).await;
    catalog
        .insert_eval_set("acme", "prod", spec("golden", &["a", "b"]))
        .await
        .expect("insert prod");
    catalog
        .insert_eval_set("acme", "staging", spec("golden", &["a"]))
        .await
        .expect("insert staging");

    let prod_id = catalog
        .get_datasets("acme")
        .await
        .expect("datasets")
        .into_iter()
        .find(|d| d.name == "prod")
        .expect("prod dataset")
        .id;
    assert!(
        catalog
            .delete_dataset_for_tenant("acme", &prod_id)
            .await
            .expect("delete dataset")
    );

    assert!(
        catalog
            .list_eval_sets("acme", "prod")
            .await
            .expect("list prod")
            .is_empty()
    );
    // Re-creating the dataset must not resurrect orphaned cases.
    catalog
        .ensure_dataset("acme", "prod")
        .await
        .expect("recreate prod");
    let recreated = catalog
        .insert_eval_set("acme", "prod", spec("golden", &[]))
        .await
        .expect("insert after cascade");
    assert_eq!(recreated.summary.case_count, 0);
    assert!(recreated.cases.is_empty());

    let staging = catalog
        .get_eval_set("acme", "staging", "golden")
        .await
        .expect("get staging")
        .expect("staging untouched");
    assert_eq!(staging.summary.case_count, 1);
}

#[tokio::test]
async fn tenants_and_datasets_are_isolated() {
    let catalog = catalog_with("acme", &["prod", "staging"]).await;
    catalog
        .upsert_tenant("globex", "globex", None, "config")
        .await
        .expect("upsert globex");
    catalog
        .ensure_dataset("globex", "prod")
        .await
        .expect("globex prod");

    catalog
        .insert_eval_set("acme", "prod", spec("shared", &["a", "b"]))
        .await
        .expect("acme prod");
    catalog
        .insert_eval_set("globex", "prod", spec("shared", &["z"]))
        .await
        .expect("same name in another tenant is fine");
    catalog
        .insert_eval_set("acme", "staging", spec("shared", &["s"]))
        .await
        .expect("same name in another dataset is fine");

    assert!(
        catalog
            .get_eval_set("globex", "staging", "shared")
            .await
            .expect("get")
            .is_none()
    );
    let globex = catalog
        .get_eval_set("globex", "prod", "shared")
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(ids(&globex.cases), vec!["z"]);

    assert!(
        catalog
            .delete_eval_set("globex", "prod", "shared")
            .await
            .expect("delete globex")
    );
    let acme = catalog
        .get_eval_set("acme", "prod", "shared")
        .await
        .expect("get acme")
        .expect("acme untouched");
    assert_eq!(ids(&acme.cases), vec!["a", "b"]);
    assert_eq!(
        catalog
            .list_eval_sets("acme", "staging")
            .await
            .expect("list staging")
            .len(),
        1
    );
}

#[tokio::test]
async fn unknown_dataset_is_rejected() {
    let catalog = catalog_with("acme", &["prod"]).await;
    let err = catalog
        .insert_eval_set("acme", "nope", spec("golden", &[]))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::UnknownDataset(d) if d == "nope"));
}

/// The validation rules are unit-tested in `store.rs`; this checks that a
/// rejected spec surfaces as `Invalid` through the catalog and stores
/// nothing.
#[tokio::test]
async fn invalid_specs_are_rejected_and_store_nothing() {
    let catalog = catalog_with("acme", &["prod"]).await;

    let mut empty_agent = spec("empty-agent", &["a"]);
    empty_agent.agent = "  ".to_string();
    assert!(matches!(
        catalog.insert_eval_set("acme", "prod", empty_agent).await,
        Err(StoreError::Invalid(msg)) if msg.contains("agent")
    ));
    assert!(
        catalog
            .list_eval_sets("acme", "prod")
            .await
            .expect("list")
            .is_empty(),
        "a rejected spec stores nothing"
    );
}

#[test]
fn case_source_serialises_as_a_kind_tagged_object() {
    use serde_json::json;

    assert_eq!(
        serde_json::to_value(EvalCaseSource::Trace {
            trace_id: TRACE_ID.to_string()
        })
        .unwrap(),
        json!({"kind": "trace", "trace_id": TRACE_ID})
    );
    assert_eq!(
        serde_json::to_value(EvalCaseSource::Upload).unwrap(),
        json!({"kind": "upload"})
    );
    assert_eq!(
        serde_json::to_value(EvalCaseSource::HandWritten).unwrap(),
        json!({"kind": "hand_written"})
    );

    let minimal: EvalCase = serde_json::from_value(json!({"id": "a", "input": "hi"})).unwrap();
    assert_eq!(minimal.source, EvalCaseSource::HandWritten);
    assert!(minimal.expected_tools.is_empty());
    assert!(minimal.tags.is_empty());
    assert!(minimal.reference.is_none());
}
