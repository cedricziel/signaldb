use common::catalog::Catalog;
use common::flight::transport::ServiceCapability;
use common::service_bootstrap::ServiceType;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

/// Testcontainers' Postgres wait strategy already blocks `start()` until the
/// container reports itself ready, but the mapped port can still refuse the
/// very first connection for a few milliseconds afterward. Retry the actual
/// connection attempt with a short, bounded backoff instead of guessing at a
/// fixed startup delay.
async fn connect_catalog_with_retry(dsn: &str) -> Catalog {
    const MAX_ATTEMPTS: u32 = 20;
    const RETRY_DELAY: Duration = Duration::from_millis(100);

    let mut last_err = None;
    for _ in 0..MAX_ATTEMPTS {
        match Catalog::new(dsn).await {
            Ok(catalog) => return catalog,
            Err(err) => {
                last_err = Some(err);
                sleep(RETRY_DELAY).await;
            }
        }
    }
    panic!(
        "Failed to create Catalog after {MAX_ATTEMPTS} attempts: {}",
        last_err.expect("at least one connection attempt was made")
    );
}

#[tokio::test]
async fn test_ingester_operations() {
    let container = Postgres::default()
        .start()
        .await
        .expect("Failed to start database");

    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let dsn = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let catalog = connect_catalog_with_retry(&dsn).await;

    let id = Uuid::new_v4();
    catalog
        .register_ingester(
            id,
            "127.0.0.1:8080",
            ServiceType::Writer,
            &[
                ServiceCapability::TraceIngestion,
                ServiceCapability::Storage,
            ],
        )
        .await
        .expect("Failed to register ingester");

    let ingesters = catalog
        .list_ingesters()
        .await
        .expect("Failed to list ingesters");
    assert_eq!(ingesters.len(), 1);
    assert_eq!(ingesters[0].id, id);
    assert_eq!(ingesters[0].address, "127.0.0.1:8080");
    assert_eq!(ingesters[0].service_type, ServiceType::Writer);
    assert_eq!(ingesters[0].capabilities.len(), 2);
    assert!(
        ingesters[0]
            .capabilities
            .contains(&ServiceCapability::TraceIngestion)
    );
    assert!(
        ingesters[0]
            .capabilities
            .contains(&ServiceCapability::Storage)
    );

    // Test heartbeat does not error
    catalog.heartbeat(id).await.expect("Failed to heartbeat");
}

#[tokio::test]
async fn test_shard_operations() {
    let container = Postgres::default()
        .start()
        .await
        .expect("Failed to start database");

    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let dsn = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let catalog = connect_catalog_with_retry(&dsn).await;

    // Initially no shards
    let shards = catalog.list_shards().await.expect("Failed to list shards");
    assert!(shards.is_empty());

    // Add a shard
    catalog
        .add_shard(1, 0, 100)
        .await
        .expect("Failed to add shard");
    let shards = catalog.list_shards().await.expect("Failed to list shards");
    assert_eq!(shards.len(), 1);
    let shard = &shards[0];
    assert_eq!(shard.id, 1);
    assert_eq!(shard.start_range, 0);
    assert_eq!(shard.end_range, 100);

    // Duplicate insertion is a no-op
    catalog
        .add_shard(1, 0, 100)
        .await
        .expect("Failed to add shard duplicate");
    let shards = catalog.list_shards().await.expect("Failed to list shards");
    assert_eq!(shards.len(), 1);

    // Test shard owners mapping
    let id = Uuid::new_v4();
    catalog
        .register_ingester(
            id,
            "127.0.0.1:8081",
            ServiceType::Writer,
            &[
                ServiceCapability::TraceIngestion,
                ServiceCapability::Storage,
            ],
        )
        .await
        .expect("Failed to register ingester");
    catalog
        .assign_shard(1, id)
        .await
        .expect("Failed to assign shard");
    let owners = catalog
        .list_shard_owners()
        .await
        .expect("Failed to list shard owners");
    assert_eq!(owners.len(), 1);
    let owner = &owners[0];
    assert_eq!(owner.shard_id, 1);
    assert_eq!(owner.ingester_id, id);

    // Duplicate assignment is a no-op
    catalog
        .assign_shard(1, id)
        .await
        .expect("Failed to assign shard duplicate");
    let owners = catalog
        .list_shard_owners()
        .await
        .expect("Failed to list shard owners");
    assert_eq!(owners.len(), 1);
}

/// Custom schema registries persist and round-trip on Postgres exactly as on
/// SQLite (the in-memory suite lives in tests/schema_registry.rs).
#[tokio::test]
async fn schema_registry_round_trips_on_postgres() {
    use common::schema_registry::{RegistrySource, SchemaResolver};
    use schema_model::RegistryDocument;

    let container = Postgres::default()
        .start()
        .await
        .expect("Failed to start database");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let dsn = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let catalog = connect_catalog_with_retry(&dsn).await;

    let doc =
        RegistryDocument::from_yaml(include_str!("../../schema-model/tests/fixtures/acme.yaml"))
            .expect("acme parses");
    let resolver = SchemaResolver::new(catalog.clone());
    let created = resolver.create("t1", &doc).await.expect("create");
    assert_eq!(created.source, RegistrySource::Custom);
    assert_eq!(created.entity_count, 2);

    let (summary, stored) = resolver
        .get("t1", "acme", "1.0.0")
        .await
        .expect("get")
        .expect("exists");
    assert_eq!(stored, doc);
    assert!(summary.updated_at.is_some());

    let mut edited = doc.clone();
    edited.description = Some("edited".into());
    resolver
        .replace("t1", "acme", "1.0.0", &edited)
        .await
        .expect("replace");
    let fresh = SchemaResolver::new(catalog);
    let res = fresh
        .resolve_attribute("t1", "acme.order.id")
        .await
        .expect("resolve");
    assert_eq!(res.hits.len(), 1);
    assert!(fresh.delete("t1", "acme", "1.0.0").await.expect("delete"));
    assert!(
        fresh
            .get("t2", "acme", "1.0.0")
            .await
            .expect("get")
            .is_none()
    );
}
