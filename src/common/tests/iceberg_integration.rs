use common::config::SchemaConfig;
use common::schema::create_catalog;
use iceberg::NamespaceIdent;
use std::collections::HashMap;

#[tokio::test]
async fn test_iceberg_sql_catalog_basic_operations() {
    let config = SchemaConfig {
        catalog_type: "sql".to_string(),
        catalog_uri: "sqlite::memory:".to_string(),
        storage_adapter: None,
    };

    // Create catalog
    let catalog = create_catalog(config).await.unwrap();

    // List namespaces (should be empty)
    let namespaces = catalog.list_namespaces(None).await.unwrap();
    assert_eq!(namespaces.len(), 0);

    // Create a namespace
    let namespace_ident = NamespaceIdent::from_strs(vec!["signaldb"]).unwrap();
    catalog
        .create_namespace(&namespace_ident, HashMap::new())
        .await
        .unwrap();

    // List namespaces again
    let namespaces = catalog.list_namespaces(None).await.unwrap();
    assert_eq!(namespaces.len(), 1);
    assert_eq!(namespaces[0].to_string(), "signaldb");

    // Creating the same namespace should be idempotent (this should not error)
    // Note: Iceberg catalogs typically handle this gracefully or return an error
    // that we can ignore for idempotency
    let result = catalog
        .create_namespace(&namespace_ident, HashMap::new())
        .await;

    // Either it succeeds (idempotent) or fails with "already exists"
    match result {
        Ok(_) => {
            // Idempotent creation succeeded
            let namespaces = catalog.list_namespaces(None).await.unwrap();
            assert_eq!(namespaces.len(), 1);
        }
        Err(e) => {
            // Should fail with "already exists" or similar
            assert!(e.to_string().contains("exist") || e.to_string().contains("Exist"));
            let namespaces = catalog.list_namespaces(None).await.unwrap();
            assert_eq!(namespaces.len(), 1);
        }
    }
}
