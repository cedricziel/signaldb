use common::config::IcebergConfig;
use common::iceberg::IcebergCatalog;
use tempfile::TempDir;

#[tokio::test]
async fn test_iceberg_catalog_basic_operations() {
    let temp_dir = TempDir::new().unwrap();
    let warehouse_path = temp_dir.path().join("warehouse");

    // Create warehouse directory
    std::fs::create_dir_all(&warehouse_path).unwrap();

    let config = IcebergConfig {
        catalog_type: "memory".to_string(),
        catalog_uri: "memory://".to_string(),
        warehouse_path: warehouse_path.to_string_lossy().to_string(),
    };

    // Create catalog
    let catalog = IcebergCatalog::new(config).await.unwrap();

    // List namespaces (should be empty)
    let namespaces = catalog.list_namespaces().await.unwrap();
    assert_eq!(namespaces.len(), 0);

    // Create a namespace
    catalog
        .create_namespace_if_not_exists("signaldb")
        .await
        .unwrap();

    // List namespaces again
    let namespaces = catalog.list_namespaces().await.unwrap();
    assert_eq!(namespaces.len(), 1);
    assert_eq!(namespaces[0].to_string(), "signaldb");

    // Creating the same namespace should be idempotent
    catalog
        .create_namespace_if_not_exists("signaldb")
        .await
        .unwrap();
    let namespaces = catalog.list_namespaces().await.unwrap();
    assert_eq!(namespaces.len(), 1);
}
