//! Fast SQLite-based catalog tests.
//!
//! These tests mirror the PostgreSQL integration tests in catalog_integration.rs
//! but use in-memory SQLite for fast execution without Docker dependencies.

use common::catalog::Catalog;
use common::flight::transport::ServiceCapability;
use common::service_bootstrap::ServiceType;
use uuid::Uuid;

#[tokio::test]
async fn test_ingester_operations_sqlite() {
    let catalog = Catalog::new_in_memory()
        .await
        .expect("Failed to create in-memory catalog");

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
async fn test_shard_operations_sqlite() {
    let catalog = Catalog::new_in_memory()
        .await
        .expect("Failed to create in-memory catalog");

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

#[tokio::test]
async fn test_discover_services_by_capability_sqlite() {
    let catalog = Catalog::new_in_memory()
        .await
        .expect("Failed to create in-memory catalog");

    // Register multiple ingesters with different capabilities
    let writer_id = Uuid::new_v4();
    catalog
        .register_ingester(
            writer_id,
            "127.0.0.1:8080",
            ServiceType::Writer,
            &[
                ServiceCapability::TraceIngestion,
                ServiceCapability::Storage,
            ],
        )
        .await
        .expect("Failed to register writer");

    let querier_id = Uuid::new_v4();
    catalog
        .register_ingester(
            querier_id,
            "127.0.0.1:9000",
            ServiceType::Querier,
            &[ServiceCapability::QueryExecution],
        )
        .await
        .expect("Failed to register querier");

    let router_id = Uuid::new_v4();
    catalog
        .register_ingester(
            router_id,
            "127.0.0.1:3000",
            ServiceType::Router,
            &[
                ServiceCapability::Routing,
                ServiceCapability::TraceIngestion,
            ],
        )
        .await
        .expect("Failed to register router");

    // Discover services with TraceIngestion capability
    let trace_services = catalog
        .discover_services_by_capability(ServiceCapability::TraceIngestion)
        .await
        .expect("Failed to discover services");
    assert_eq!(trace_services.len(), 2);
    let trace_ids: Vec<Uuid> = trace_services.iter().map(|s| s.id).collect();
    assert!(trace_ids.contains(&writer_id));
    assert!(trace_ids.contains(&router_id));

    // Discover services with QueryExecution capability
    let query_services = catalog
        .discover_services_by_capability(ServiceCapability::QueryExecution)
        .await
        .expect("Failed to discover services");
    assert_eq!(query_services.len(), 1);
    assert_eq!(query_services[0].id, querier_id);

    // Discover services with Storage capability
    let storage_services = catalog
        .discover_services_by_capability(ServiceCapability::Storage)
        .await
        .expect("Failed to discover services");
    assert_eq!(storage_services.len(), 1);
    assert_eq!(storage_services[0].id, writer_id);
}

#[tokio::test]
async fn test_deregister_ingester_sqlite() {
    let catalog = Catalog::new_in_memory()
        .await
        .expect("Failed to create in-memory catalog");

    let id = Uuid::new_v4();
    catalog
        .register_ingester(
            id,
            "127.0.0.1:8080",
            ServiceType::Writer,
            &[ServiceCapability::Storage],
        )
        .await
        .expect("Failed to register ingester");

    let ingesters = catalog
        .list_ingesters()
        .await
        .expect("Failed to list ingesters");
    assert_eq!(ingesters.len(), 1);

    // Deregister the ingester
    catalog
        .deregister_ingester(id)
        .await
        .expect("Failed to deregister ingester");

    let ingesters = catalog
        .list_ingesters()
        .await
        .expect("Failed to list ingesters");
    assert!(ingesters.is_empty());
}

#[tokio::test]
async fn test_ingester_update_sqlite() {
    let catalog = Catalog::new_in_memory()
        .await
        .expect("Failed to create in-memory catalog");

    let id = Uuid::new_v4();

    // Register initial ingester
    catalog
        .register_ingester(
            id,
            "127.0.0.1:8080",
            ServiceType::Writer,
            &[ServiceCapability::Storage],
        )
        .await
        .expect("Failed to register ingester");

    let ingesters = catalog
        .list_ingesters()
        .await
        .expect("Failed to list ingesters");
    assert_eq!(ingesters[0].address, "127.0.0.1:8080");
    assert_eq!(ingesters[0].capabilities.len(), 1);

    // Update ingester with new address and capabilities
    catalog
        .register_ingester(
            id,
            "127.0.0.1:8081",
            ServiceType::Writer,
            &[
                ServiceCapability::Storage,
                ServiceCapability::TraceIngestion,
            ],
        )
        .await
        .expect("Failed to update ingester");

    let ingesters = catalog
        .list_ingesters()
        .await
        .expect("Failed to list ingesters");
    assert_eq!(ingesters.len(), 1);
    assert_eq!(ingesters[0].address, "127.0.0.1:8081");
    assert_eq!(ingesters[0].capabilities.len(), 2);
}
