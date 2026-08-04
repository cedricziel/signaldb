use common::config::SchemaConfig;
use common::iceberg::create_catalog;

/// Regression test for #959: tables created before #895 lack
/// `write.metadata.delete-after-commit.enabled` / `previous-versions-max`,
/// so metadata files accumulate forever on them. `ensure_table` must
/// backfill the pruning properties when it loads such a table.
#[tokio::test]
async fn ensure_table_backfills_metadata_pruning_properties_on_existing_tables() {
    use common::iceberg::table_manager::IcebergTableManager;
    use common::iceberg::{names, schemas};
    use iceberg_rust::catalog::create::CreateTableBuilder;

    let config = SchemaConfig {
        catalog_type: "sql".to_string(),
        catalog_uri: "sqlite::memory:".to_string(),
        default_schemas: common::config::DefaultSchemas::default(),
        materialized_labels: Default::default(),
    };
    let catalog = create_catalog(config).await.unwrap();
    let labels = common::config::MaterializedLabels::default();

    // Create the table the way pre-#895 builds did: no pruning properties.
    let namespace = common::iceberg::names::build_namespace("tenant", "dataset").unwrap();
    catalog
        .clone()
        .create_namespace(&namespace, None)
        .await
        .unwrap();
    let table_schema = schemas::TableSchema::Logs;
    let mut builder = CreateTableBuilder::default();
    builder
        .with_name("logs".to_string())
        .with_schema(table_schema.schema_with_labels(&labels).unwrap())
        .with_partition_spec(table_schema.partition_spec().unwrap())
        .with_location(names::build_table_location("tenant", "dataset", "logs"));
    let ident = names::build_table_identifier("tenant", "dataset", "logs");
    catalog
        .clone()
        .create_table(ident.clone(), builder.create().unwrap())
        .await
        .unwrap();

    // Loading through the manager must backfill the pruning properties.
    let manager = IcebergTableManager::new(catalog.clone(), 100);
    let table = manager
        .ensure_table("tenant", "dataset", "logs", &labels)
        .await
        .unwrap();
    let properties = &table.metadata().properties;
    assert_eq!(
        properties
            .get("write.metadata.delete-after-commit.enabled")
            .map(String::as_str),
        Some("true"),
        "ensure_table must backfill delete-after-commit on pre-existing tables"
    );
    assert_eq!(
        properties
            .get("write.metadata.previous-versions-max")
            .map(String::as_str),
        Some("100"),
        "ensure_table must backfill previous-versions-max on pre-existing tables"
    );

    // The backfill must be durable: a fresh load (not just the returned
    // handle) sees the committed properties, and a second ensure_table
    // call must not re-commit (same metadata version).
    let log_len_after_backfill = {
        use iceberg_rust::catalog::tabular::Tabular;
        match catalog.clone().load_tabular(&ident).await.unwrap() {
            Tabular::Table(t) => {
                assert_eq!(
                    t.metadata()
                        .properties
                        .get("write.metadata.delete-after-commit.enabled")
                        .map(String::as_str),
                    Some("true"),
                    "backfilled properties must be committed, not handle-local"
                );
                t.metadata().metadata_log.len()
            }
            _ => panic!("expected a table for {ident}"),
        }
    };
    let again = manager
        .ensure_table("tenant", "dataset", "logs", &labels)
        .await
        .unwrap();
    assert_eq!(
        again.metadata().metadata_log.len(),
        log_len_after_backfill,
        "ensure_table must not commit again once the properties are present"
    );
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
        materialized_labels: Default::default(),
    };
    let catalog = create_catalog(config).await.unwrap();
    let manager = IcebergTableManager::new(catalog.clone(), 100);

    // First call creates the table.
    manager
        .ensure_table(
            "tenant",
            "dataset",
            "logs",
            &common::config::MaterializedLabels::default(),
        )
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
        .ensure_table(
            "tenant",
            "dataset",
            "logs",
            &common::config::MaterializedLabels::default(),
        )
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
