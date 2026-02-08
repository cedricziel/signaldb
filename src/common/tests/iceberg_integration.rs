use common::config::SchemaConfig;
use common::iceberg::create_catalog;

#[tokio::test]
async fn test_iceberg_sql_catalog_basic_operations() {
    let config = SchemaConfig {
        catalog_type: "sql".to_string(),
        catalog_uri: "sqlite::memory:".to_string(),
        default_schemas: common::config::DefaultSchemas::default(),
    };

    // Create catalog
    let catalog = create_catalog(config).await.unwrap();

    // List namespaces (should be empty initially)
    let namespaces = catalog.list_namespaces(None).await.unwrap();
    assert_eq!(namespaces.len(), 0);

    // Note: The published version of iceberg-sql-catalog (0.8.0) does not implement
    // create_namespace, drop_namespace, or other namespace manipulation methods.
    // These methods return todo!() and will panic if called.
    //
    // Namespaces are implicitly created when tables are created, and list_namespaces
    // works by querying tables. This test validates that the catalog can be created
    // and that list_namespaces returns an empty result when no tables exist.
    //
    // For full namespace operations testing, we would need to use the git version
    // of iceberg-rust which requires upgrading DataFusion from v47 to v50+.
}
