//! Tenant OTTL processors catalog storage on Postgres — the SQLite suite
//! lives in `tests/processors_store.rs` (change: tenant-ottl-processors,
//! task 2.1).

use common::processors::{ProcessorSpec, StoreError};
use common::testing::{connect_catalog_with_retry, start_container_with_retry};
use testcontainers_modules::postgres::Postgres;

fn spec(name: &str) -> ProcessorSpec {
    ProcessorSpec {
        name: name.to_string(),
        dataset: None,
        signal: "traces".to_string(),
        enabled: true,
        priority: 100,
        error_mode: "ignore".to_string(),
        description: None,
        statements: vec!["set(span.name, \"redacted\")".to_string()],
    }
}

#[tokio::test]
async fn processors_round_trip_and_cascade_on_postgres() {
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
        .expect("ensure dataset");

    let created = catalog
        .insert_processor("acme", &spec("redact-pii"))
        .await
        .expect("insert");
    assert_eq!(created.name, "redact-pii");

    let err = catalog
        .insert_processor("acme", &spec("redact-pii"))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Conflict(_)));

    let mut with_unknown_dataset = spec("scoped");
    with_unknown_dataset.dataset = Some("missing".into());
    let err = catalog
        .insert_processor("acme", &with_unknown_dataset)
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::UnknownDataset(_)));

    let mut scoped = spec("scoped");
    scoped.dataset = Some("prod".into());
    catalog
        .insert_processor("acme", &scoped)
        .await
        .expect("insert scoped");

    let datasets = catalog.get_datasets("acme").await.expect("datasets");
    let dataset_id = datasets
        .iter()
        .find(|d| d.name == "prod")
        .expect("prod dataset")
        .id
        .clone();
    assert!(
        catalog
            .delete_dataset_for_tenant("acme", &dataset_id)
            .await
            .expect("delete dataset")
    );

    let remaining = catalog.list_processors("acme").await.expect("list");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].name, "redact-pii");

    assert!(
        catalog
            .delete_processor("acme", "redact-pii")
            .await
            .expect("delete")
    );
}
