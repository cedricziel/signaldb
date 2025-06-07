use common::catalog::{Catalog, Ingester, Shard, ShardOwner};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

#[tokio::test]
async fn test_ingester_operations() {
    let container = Postgres::default()
        .start()
        .await
        .expect("Failed to start database");

    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let dsn = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", port);

    // Give the database some time to initialize
    sleep(Duration::from_secs(1)).await;
    let catalog = Catalog::new(&dsn).await.expect("Failed to create Catalog");

    let id = Uuid::new_v4();
    catalog
        .register_ingester(id, "127.0.0.1:8080")
        .await
        .expect("Failed to register ingester");

    let ingesters = catalog
        .list_ingesters()
        .await
        .expect("Failed to list ingesters");
    assert_eq!(ingesters.len(), 1);
    assert_eq!(ingesters[0].id, id);
    assert_eq!(ingesters[0].address, "127.0.0.1:8080");

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
    let dsn = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", port);

    sleep(Duration::from_secs(1)).await;
    let catalog = Catalog::new(&dsn).await.expect("Failed to create Catalog");

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
        .register_ingester(id, "127.0.0.1:8081")
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
