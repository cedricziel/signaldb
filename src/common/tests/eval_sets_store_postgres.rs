//! Eval-set catalog storage on Postgres — the SQLite suite lives in
//! `tests/eval_sets_store.rs` (change: agent-offline-evals, task 5.1).

use common::eval_sets::{EvalCase, EvalCaseSource, EvalCaseSourceCounts, EvalSetSpec, StoreError};
use common::testing::{connect_catalog_with_retry, start_container_with_retry};
use testcontainers_modules::postgres::Postgres;

fn case(id: &str) -> EvalCase {
    EvalCase {
        id: id.to_string(),
        input: format!("input for {id}"),
        expected_tools: vec!["lookup_order".to_string()],
        reference: None,
        tags: vec!["refunds".to_string()],
        source: EvalCaseSource::HandWritten,
    }
}

fn spec(name: &str, case_ids: &[&str]) -> EvalSetSpec {
    EvalSetSpec {
        name: name.to_string(),
        agent: "support-triage".to_string(),
        description: None,
        cases: case_ids.iter().map(|id| case(id)).collect(),
    }
}

fn ids(cases: &[EvalCase]) -> Vec<&str> {
    cases.iter().map(|c| c.id.as_str()).collect()
}

#[tokio::test]
async fn eval_sets_round_trip_append_and_cascade_on_postgres() {
    let container = start_container_with_retry(Postgres::default).await;
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let dsn = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let catalog = connect_catalog_with_retry(&dsn).await;

    catalog
        .upsert_tenant("acme", "acme", None, "config")
        .await
        .expect("upsert tenant");
    catalog
        .ensure_dataset("acme", "prod")
        .await
        .expect("ensure prod");
    catalog
        .ensure_dataset("acme", "staging")
        .await
        .expect("ensure staging");

    let mut body = spec("golden", &["c", "a", "b"]);
    body.cases[0].source = EvalCaseSource::Trace {
        trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
    };
    body.cases[1].source = EvalCaseSource::Upload;
    let created = catalog
        .insert_eval_set("acme", "prod", body.clone())
        .await
        .expect("insert");
    assert_eq!(created.cases, body.cases);
    assert_eq!(created.summary.case_count, 3);
    let fetched = catalog
        .get_eval_set("acme", "prod", "golden")
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(fetched, created, "create returns what a later get reads");

    let err = catalog
        .insert_eval_set("acme", "prod", spec("golden", &[]))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Conflict(_)));

    let err = catalog
        .insert_eval_set("acme", "missing", spec("golden", &[]))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::UnknownDataset(_)));

    let outcome = catalog
        .append_eval_cases("acme", "prod", "golden", vec![case("a"), case("d")])
        .await
        .expect("append");
    assert_eq!((outcome.added, outcome.already_present), (1, 1));
    let fetched = catalog
        .get_eval_set("acme", "prod", "golden")
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(ids(&fetched.cases), vec!["c", "a", "b", "d"]);
    assert_eq!(fetched.summary.created_at, created.summary.created_at);
    let listed = catalog.list_eval_sets("acme", "prod").await.expect("list");
    assert_eq!(
        listed[0].sources,
        EvalCaseSourceCounts {
            trace: 1,
            upload: 1,
            hand_written: 2
        }
    );

    let replaced = catalog
        .replace_eval_set("acme", "prod", "golden", spec("golden", &["z"]))
        .await
        .expect("replace");
    assert_eq!(ids(&replaced.cases), vec!["z"]);

    let err = catalog
        .replace_eval_set("acme", "prod", "absent", spec("absent", &[]))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::NotFound(_)));

    catalog
        .insert_eval_set("acme", "staging", spec("golden", &["s"]))
        .await
        .expect("insert staging");
    let summaries = catalog.list_eval_sets("acme", "prod").await.expect("list");
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].summary.case_count, 1);

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
            .expect("list")
            .is_empty()
    );
    assert!(
        catalog
            .delete_eval_set("acme", "staging", "golden")
            .await
            .expect("delete staging")
    );
}
