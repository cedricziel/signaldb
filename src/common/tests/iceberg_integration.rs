use common::config::SchemaConfig;
use common::schema::create_catalog;
use iceberg_rust::catalog::namespace::Namespace;
use std::collections::HashMap;

#[tokio::test]
async fn test_iceberg_sql_catalog_basic_operations() {
    let config = SchemaConfig {
        catalog_type: "sql".to_string(),
        catalog_uri: "sqlite::memory:".to_string(),
        default_schemas: common::config::DefaultSchemas::default(),
    };

    // Create catalog
    let catalog = create_catalog(config).await.unwrap();

    // List namespaces (should be empty)
    let namespaces = catalog.list_namespaces(None).await.unwrap();
    assert_eq!(namespaces.len(), 0);

    // Create a namespace
    let namespace = Namespace::try_new(&["signaldb".to_string()]).unwrap();
    catalog
        .create_namespace(&namespace, Some(HashMap::new()))
        .await
        .unwrap();

    // List namespaces again
    let namespaces = catalog.list_namespaces(None).await.unwrap();
    assert_eq!(namespaces.len(), 1);
    assert_eq!(
        format!("{:?}", namespaces[0]),
        r#"Namespace { name: ["signaldb"] }"#
    );

    // Creating the same namespace should be idempotent (this should not error)
    // Note: Iceberg catalogs typically handle this gracefully or return an error
    // that we can ignore for idempotency
    let result = catalog
        .create_namespace(&namespace, Some(HashMap::new()))
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
