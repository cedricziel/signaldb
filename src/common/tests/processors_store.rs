//! Tenant OTTL processors catalog storage (`common::processors::store`,
//! change: tenant-ottl-processors, task 2.1). SQLite in-memory here; the
//! Postgres twin lives in `tests/processors_store_postgres.rs`.

use common::catalog::Catalog;
use common::processors::{ProcessorSpec, StoreError};

async fn catalog_with_tenant_and_dataset(tenant: &str, dataset: &str) -> Catalog {
    let catalog = Catalog::new_in_memory().await.expect("catalog");
    catalog
        .upsert_tenant(tenant, tenant, None, "config")
        .await
        .expect("upsert tenant");
    catalog
        .ensure_dataset(tenant, dataset)
        .await
        .expect("ensure dataset");
    catalog
}

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
async fn insert_get_replace_delete_round_trip() {
    let catalog = catalog_with_tenant_and_dataset("acme", "prod").await;

    let created = catalog
        .insert_processor("acme", &spec("redact-pii"))
        .await
        .expect("insert");
    assert_eq!(created.name, "redact-pii");
    assert_eq!(created.tenant_id, "acme");
    assert!(created.dataset.is_none());
    assert_eq!(created.statements, spec("redact-pii").statements);

    let fetched = catalog
        .get_processor("acme", "redact-pii")
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(fetched, created);

    let mut replacement = spec("redact-pii");
    replacement.description = Some("updated".into());
    replacement.priority = 5;
    let replaced = catalog
        .replace_processor("acme", "redact-pii", &replacement)
        .await
        .expect("replace");
    assert_eq!(replaced.description.as_deref(), Some("updated"));
    assert_eq!(replaced.priority, 5);

    assert!(
        catalog
            .delete_processor("acme", "redact-pii")
            .await
            .expect("delete")
    );
    assert!(
        catalog
            .get_processor("acme", "redact-pii")
            .await
            .expect("get")
            .is_none()
    );
    assert!(
        !catalog
            .delete_processor("acme", "redact-pii")
            .await
            .expect("delete again")
    );
}

#[tokio::test]
async fn replace_of_unknown_name_is_not_found() {
    let catalog = catalog_with_tenant_and_dataset("acme", "prod").await;
    let err = catalog
        .replace_processor("acme", "missing", &spec("missing"))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::NotFound(name) if name == "missing"));
}

#[tokio::test]
async fn duplicate_name_is_a_conflict() {
    let catalog = catalog_with_tenant_and_dataset("acme", "prod").await;
    catalog
        .insert_processor("acme", &spec("dupe"))
        .await
        .expect("first insert");
    let err = catalog
        .insert_processor("acme", &spec("dupe"))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Conflict(name) if name == "dupe"));
}

#[tokio::test]
async fn unknown_dataset_is_rejected() {
    let catalog = catalog_with_tenant_and_dataset("acme", "prod").await;
    let mut with_dataset = spec("scoped");
    with_dataset.dataset = Some("does-not-exist".into());
    let err = catalog
        .insert_processor("acme", &with_dataset)
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::UnknownDataset(d) if d == "does-not-exist"));
}

#[tokio::test]
async fn invalid_name_signal_and_error_mode_are_rejected() {
    let catalog = catalog_with_tenant_and_dataset("acme", "prod").await;

    let mut bad_name = spec("Bad_Name");
    bad_name.name = "Bad_Name".into();
    assert!(matches!(
        catalog.insert_processor("acme", &bad_name).await,
        Err(StoreError::Invalid(_))
    ));

    let mut bad_signal = spec("ok-name");
    bad_signal.signal = "spans".into();
    assert!(matches!(
        catalog.insert_processor("acme", &bad_signal).await,
        Err(StoreError::Invalid(_))
    ));

    let mut bad_mode = spec("ok-name-2");
    bad_mode.error_mode = "abort".into();
    assert!(matches!(
        catalog.insert_processor("acme", &bad_mode).await,
        Err(StoreError::Invalid(_))
    ));
}

#[tokio::test]
async fn tenants_are_isolated() {
    let catalog = catalog_with_tenant_and_dataset("acme", "prod").await;
    catalog
        .upsert_tenant("globex", "globex", None, "config")
        .await
        .expect("upsert tenant");
    catalog
        .insert_processor("acme", &spec("shared-name"))
        .await
        .expect("acme insert");
    catalog
        .insert_processor("globex", &spec("shared-name"))
        .await
        .expect("globex insert with same name is fine, different tenant");

    let acme_list = catalog.list_processors("acme").await.expect("list acme");
    assert_eq!(acme_list.len(), 1);
    let globex_list = catalog
        .list_processors("globex")
        .await
        .expect("list globex");
    assert_eq!(globex_list.len(), 1);

    assert!(
        !catalog
            .delete_processor("globex", "shared-name")
            .await
            .expect("cross-tenant delete is a no-op")
            || catalog
                .get_processor("acme", "shared-name")
                .await
                .expect("acme row untouched")
                .is_some()
    );
}

#[tokio::test]
async fn delete_dataset_for_tenant_cascades_processor_rows() {
    let catalog = catalog_with_tenant_and_dataset("acme", "prod").await;
    let datasets = catalog.get_datasets("acme").await.expect("datasets");
    let dataset_id = datasets
        .iter()
        .find(|d| d.name == "prod")
        .expect("prod dataset")
        .id
        .clone();

    let mut scoped = spec("scoped");
    scoped.dataset = Some("prod".into());
    catalog
        .insert_processor("acme", &scoped)
        .await
        .expect("insert scoped processor");
    catalog
        .insert_processor("acme", &spec("tenant-wide"))
        .await
        .expect("insert tenant-wide processor");

    assert!(
        catalog
            .delete_dataset_for_tenant("acme", &dataset_id)
            .await
            .expect("delete dataset")
    );

    let remaining = catalog.list_processors("acme").await.expect("list");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].name, "tenant-wide");
}

#[tokio::test]
async fn select_for_request_orders_tenant_wide_first_then_priority_then_name() {
    let catalog = catalog_with_tenant_and_dataset("acme", "prod").await;

    let mut dataset_high_priority = spec("dataset-rule");
    dataset_high_priority.dataset = Some("prod".into());
    dataset_high_priority.priority = 1;
    catalog
        .insert_processor("acme", &dataset_high_priority)
        .await
        .expect("insert dataset rule");

    let mut tenant_b = spec("tenant-rule-b");
    tenant_b.priority = 50;
    catalog
        .insert_processor("acme", &tenant_b)
        .await
        .expect("insert tenant rule b");

    let mut tenant_a = spec("tenant-rule-a");
    tenant_a.priority = 50;
    catalog
        .insert_processor("acme", &tenant_a)
        .await
        .expect("insert tenant rule a");

    let mut other_signal = spec("logs-rule");
    other_signal.signal = "logs".to_string();
    catalog
        .insert_processor("acme", &other_signal)
        .await
        .expect("insert logs rule");

    let mut disabled = spec("disabled-rule");
    disabled.enabled = false;
    catalog
        .insert_processor("acme", &disabled)
        .await
        .expect("insert disabled rule");

    let selected = catalog
        .select_processors_for_request("acme", "prod", "traces")
        .await
        .expect("select");
    let names: Vec<&str> = selected.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["tenant-rule-a", "tenant-rule-b", "dataset-rule"]
    );
}
