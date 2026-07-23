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

/// Regression test for #537: `ensure_table` must reflect commits made through
/// other handles (writer appends, compactor replaces) instead of returning a
/// point-in-time cached `Table` whose metadata predates them.
#[tokio::test]
async fn ensure_table_reflects_commits_made_through_other_handles() {
    use common::iceberg::table_manager::IcebergTableManager;
    use iceberg_rust::catalog::tabular::Tabular;

    let config = SchemaConfig {
        catalog_type: "sql".to_string(),
        catalog_uri: "sqlite::memory:".to_string(),
        default_schemas: common::config::DefaultSchemas::default(),
    };
    let catalog = create_catalog(config).await.unwrap();
    let manager = IcebergTableManager::new(catalog.clone());

    // First call creates the table.
    manager
        .ensure_table("tenant", "dataset", "logs")
        .await
        .unwrap();

    // Commit a metadata change through an independent fresh handle, the way
    // the writer or compactor would from another task or process.
    let ident = common::iceberg::names::build_table_identifier("tenant", "dataset", "logs");
    let mut fresh = match catalog.clone().load_tabular(&ident).await.unwrap() {
        Tabular::Table(table) => table,
        _ => panic!("expected a table for {ident}"),
    };
    fresh
        .new_transaction(None)
        .update_properties(vec![(
            "signaldb.test-marker".to_string(),
            "committed".to_string(),
        )])
        .commit()
        .await
        .unwrap();

    // A second ensure_table call must observe that commit.
    let observed = manager
        .ensure_table("tenant", "dataset", "logs")
        .await
        .unwrap();
    assert_eq!(
        observed
            .metadata()
            .properties
            .get("signaldb.test-marker")
            .map(String::as_str),
        Some("committed"),
        "ensure_table returned a stale handle that misses a committed update"
    );
}
