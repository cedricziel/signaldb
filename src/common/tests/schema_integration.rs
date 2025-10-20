use common::config::SchemaConfig;
use common::schema::{create_catalog, create_default_catalog};

// Note: The published version of iceberg-sql-catalog (0.8.0) does not implement
// create_namespace, drop_namespace, or other namespace manipulation methods.
// These methods return todo!() and will panic if called.
//
// Namespaces are implicitly created when tables are created, and list_namespaces
// works by querying tables. These tests validate catalog creation and basic
// operations that ARE implemented.
//
// For full namespace operations testing, we would need to use the git version
// of iceberg-rust which requires upgrading DataFusion from v47 to v50+.

#[tokio::test]
async fn test_memory_catalog() {
    let config = SchemaConfig {
        catalog_type: "memory".to_string(),
        catalog_uri: "memory://".to_string(),
        default_schemas: common::config::DefaultSchemas::default(),
    };

    let catalog = create_catalog(config).await.unwrap();

    // List namespaces (should be empty initially when no tables exist)
    let namespaces = catalog.list_namespaces(None).await.unwrap();
    assert_eq!(namespaces.len(), 0);
}

#[tokio::test]
async fn test_sql_catalog() {
    let config = SchemaConfig {
        catalog_type: "sql".to_string(),
        catalog_uri: "sqlite::memory:".to_string(),
        default_schemas: common::config::DefaultSchemas::default(),
    };

    let catalog = create_catalog(config).await.unwrap();

    // List namespaces (should be empty initially when no tables exist)
    let namespaces = catalog.list_namespaces(None).await.unwrap();
    assert_eq!(namespaces.len(), 0);
}

#[tokio::test]
async fn test_default_catalog() {
    let catalog = create_default_catalog().await.unwrap();

    // List namespaces (should be empty initially when no tables exist)
    let namespaces = catalog.list_namespaces(None).await.unwrap();
    assert_eq!(namespaces.len(), 0);
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
            .contains("Unsupported catalog URI")
    );
}
