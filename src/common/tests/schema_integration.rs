use common::config::SchemaConfig;
use common::schema::{create_catalog, create_default_catalog};
use iceberg_rust::catalog::namespace::Namespace;
use std::collections::HashMap;

#[tokio::test]
async fn test_memory_catalog() {
    let config = SchemaConfig {
        catalog_type: "memory".to_string(),
        catalog_uri: "memory://".to_string(),
        default_schemas: common::config::DefaultSchemas::default(),
    };

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
}

#[tokio::test]
async fn test_sql_catalog() {
    let config = SchemaConfig {
        catalog_type: "sql".to_string(),
        catalog_uri: "sqlite::memory:".to_string(),
        default_schemas: common::config::DefaultSchemas::default(),
    };

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
}

#[tokio::test]
async fn test_default_catalog() {
    let catalog = create_default_catalog().await.unwrap();

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
}

#[tokio::test]
async fn test_unsupported_catalog_type() {
    let config = SchemaConfig {
        catalog_type: "unsupported".to_string(),
        catalog_uri: "unsupported://".to_string(),
        default_schemas: common::config::DefaultSchemas::default(),
    };

    let result = create_catalog(config).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Unsupported catalog type")
    );
}
