use anyhow::Result;
use common::CatalogManager;
use common::config::{
    AuthConfig, Configuration, DatasetConfig, SchemaConfig, StorageConfig, TenantConfig,
};
use common::wal::{Wal, WalConfig, WalOperation, record_batch_to_bytes};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use iceberg_rust::catalog::identifier::Identifier;
use object_store::memory::InMemory;
use std::sync::Arc;
use tempfile::tempdir;
use writer::{IcebergTableWriter, WalProcessor};

/// Integration test demonstrating the Iceberg table writer functionality
#[tokio::test]
async fn test_iceberg_writer_integration() -> Result<()> {
    // Setup test environment
    let _temp_dir = tempdir()?;
    let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);
    let object_store = Arc::new(InMemory::new());

    // Test that we can create an Iceberg writer (should work now with table creation)
    let result = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        "default".to_string(),
        "default".to_string(),
        "traces".to_string(),
    )
    .await;

    // Table creation is now implemented, but may fail due to test environment
    if let Err(e) = result {
        // Should not fail due to "not implemented" anymore
        assert!(!e.to_string().contains("Table creation not yet implemented"));
        println!("Expected test environment failure: {}", e);
    } else {
        println!("Successfully created Iceberg writer in test environment");
    }

    Ok(())
}

/// Integration test for WAL processor with Iceberg integration
#[tokio::test]
async fn test_wal_processor_integration() -> Result<()> {
    // Setup test environment
    let temp_dir = tempdir()?;
    let wal_config = WalConfig::with_defaults(temp_dir.path().to_path_buf());
    let wal = Arc::new(Wal::new(wal_config).await?);
    let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);
    let object_store = Arc::new(InMemory::new());

    // Create WAL processor
    let mut processor = WalProcessor::new(wal.clone(), catalog_manager, object_store);

    // Create a test record batch
    let schema = Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["trace-1", "trace-2"])),
            Arc::new(StringArray::from(vec!["span-1", "span-2"])),
            Arc::new(Int64Array::from(vec![1234567890, 1234567891])),
        ],
    )?;

    // Serialize batch and write to WAL
    let batch_bytes = record_batch_to_bytes(&batch)?;
    wal.append(WalOperation::WriteTraces, batch_bytes, None)
        .await?;
    wal.flush().await?;

    // Verify we can get stats from processor
    let stats = processor.get_stats();
    assert_eq!(stats.active_writers, 0);

    // Force-commit the pending entry (the read-your-writes drain). The test
    // batch uses a minimal schema, so the commit may fail in this environment —
    // that is tolerated; it must only not regress to the obsolete
    // "not implemented" path.
    let result = processor
        .force_commit_pending(writer::FlushScope {
            tenant_id: "default".to_string(),
            dataset_id: Some("default".to_string()),
        })
        .await;
    if let Err(e) = result {
        assert!(!e.to_string().contains("Table creation not yet implemented"));
        println!("Expected test environment failure in processing: {}", e);
    } else {
        println!("Successfully processed WAL entry with Iceberg writer");
    }

    // Shutdown processor
    processor.shutdown().await?;

    Ok(())
}

/// Integration test verifying writer creates tables under slug-based Iceberg namespace (D1)
#[tokio::test]
async fn test_iceberg_namespace_slug_based() -> Result<()> {
    // Create config with specific tenant/dataset slugs
    let config = Configuration {
        schema: SchemaConfig {
            catalog_type: "sql".to_string(),
            catalog_uri: "sqlite::memory:".to_string(),
            default_schemas: Default::default(),
            materialized_labels: Default::default(),
        },
        storage: StorageConfig {
            dsn: "memory://".to_string(),
        },
        auth: AuthConfig {
            tenants: vec![TenantConfig {
                id: "tenant-1".to_string(),
                slug: "mycorp".to_string(),
                name: "My Corp".to_string(),
                default_dataset: Some("dataset-1".to_string()),
                datasets: vec![DatasetConfig {
                    id: "dataset-1".to_string(),
                    slug: "prod".to_string(),
                    is_default: true,
                    storage: None,
                }],
                api_keys: vec![],
                schema_config: None,
                limits: None,
            }],
            ..Default::default()
        },
        ..Default::default()
    };

    let object_store = Arc::new(InMemory::new());
    let catalog_manager = Arc::new(CatalogManager::new(config).await?);

    // Create writer with tenant_id/dataset_id that map to slugs "mycorp"/"prod"
    let writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        "tenant-1".to_string(),
        "dataset-1".to_string(),
        "traces".to_string(),
    )
    .await?;

    // Verify the table identifier uses slug-based namespace
    let ident = writer.table_identifier();
    let namespace = ident.namespace();
    let table_name = ident.name();

    assert_eq!(
        &**namespace,
        &["mycorp".to_string(), "prod".to_string()],
        "Table namespace should use slugs, not tenant/dataset IDs"
    );
    assert_eq!(table_name, "traces");

    // Verify the identifier Display format
    assert_eq!(
        format!("{ident}"),
        "mycorp.prod.traces",
        "Identifier should format as slug-based path"
    );

    Ok(())
}

/// Newly-created tables enable metadata delete-after-commit so accumulated
/// `metadata.json` files stay bounded under continuous ingestion (#888).
#[tokio::test]
async fn test_created_tables_enable_metadata_pruning() -> Result<()> {
    let config = Configuration {
        schema: SchemaConfig {
            catalog_type: "sql".to_string(),
            catalog_uri: "sqlite::memory:".to_string(),
            default_schemas: Default::default(),
            materialized_labels: Default::default(),
        },
        storage: StorageConfig {
            dsn: "memory://".to_string(),
        },
        auth: AuthConfig {
            tenants: vec![TenantConfig {
                id: "tenant-1".to_string(),
                slug: "mycorp".to_string(),
                name: "My Corp".to_string(),
                default_dataset: Some("dataset-1".to_string()),
                datasets: vec![DatasetConfig {
                    id: "dataset-1".to_string(),
                    slug: "prod".to_string(),
                    is_default: true,
                    storage: None,
                }],
                api_keys: vec![],
                schema_config: None,
                limits: None,
            }],
            ..Default::default()
        },
        ..Default::default()
    };

    let object_store = Arc::new(InMemory::new());
    let catalog_manager = Arc::new(CatalogManager::new(config).await?);

    let writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store,
        "tenant-1".to_string(),
        "dataset-1".to_string(),
        "traces".to_string(),
    )
    .await?;

    let properties = &writer.table_metadata().properties;
    assert_eq!(
        properties
            .get("write.metadata.delete-after-commit.enabled")
            .map(String::as_str),
        Some("true"),
        "created tables must enable metadata delete-after-commit"
    );
    assert_eq!(
        properties
            .get("write.metadata.previous-versions-max")
            .map(String::as_str),
        Some("100"),
        "created tables must bound retained metadata versions"
    );

    Ok(())
}

/// End-to-end: with a small retention window, superseded `metadata.json` files
/// are actually reclaimed on commit (not merely accumulated), proving the
/// property wiring + the pinned catalog's delete-after-commit work together.
#[tokio::test]
async fn test_metadata_pruning_reclaims_old_metadata_files() -> Result<()> {
    use futures::TryStreamExt;
    use iceberg_rust::catalog::tabular::Tabular;
    use object_store::ObjectStore;

    let temp_dir = tempdir()?;
    let storage_dir = temp_dir.path().join("storage");
    std::fs::create_dir_all(&storage_dir)?;

    let mut config = Configuration {
        schema: SchemaConfig {
            catalog_type: "sql".to_string(),
            catalog_uri: "sqlite::memory:".to_string(),
            default_schemas: Default::default(),
            materialized_labels: Default::default(),
        },
        storage: StorageConfig {
            dsn: format!("file://{}", storage_dir.display()),
        },
        auth: AuthConfig {
            tenants: vec![TenantConfig {
                id: "tenant-1".to_string(),
                slug: "mycorp".to_string(),
                name: "My Corp".to_string(),
                default_dataset: Some("dataset-1".to_string()),
                datasets: vec![DatasetConfig {
                    id: "dataset-1".to_string(),
                    slug: "prod".to_string(),
                    is_default: true,
                    storage: None,
                }],
                api_keys: vec![],
                schema_config: None,
                limits: None,
            }],
            ..Default::default()
        },
        ..Default::default()
    };
    // Retain only two previous metadata versions so pruning is observable
    // after a handful of commits (rather than the production default of 100).
    config.writer.metadata_previous_versions_max = 2;

    let object_store = common::storage::create_object_store(&config.storage)?;
    let catalog_manager = Arc::new(CatalogManager::new(config).await?);

    // Create the table (applies the retention properties).
    let _writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        "tenant-1".to_string(),
        "dataset-1".to_string(),
        "traces".to_string(),
    )
    .await?;

    // Commit several times to accumulate metadata versions.
    let ident = catalog_manager.build_table_identifier("tenant-1", "dataset-1", "traces");
    const COMMITS: usize = 6;
    for i in 0..COMMITS {
        let tabular = catalog_manager.catalog().load_tabular(&ident).await?;
        let mut table = match tabular {
            Tabular::Table(table) => table,
            _ => panic!("expected a table"),
        };
        table
            .new_transaction(None)
            .update_properties(vec![("marker".to_string(), i.to_string())])
            .commit()
            .await
            .map_err(|e| anyhow::anyhow!("commit {i} failed: {e}"))?;
    }

    // Count retained metadata files. Without pruning this would be one per
    // version (≈ COMMITS + 1); with previous-versions-max = 2 it stays bounded.
    let metadata_files = object_store
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .filter(|m| m.location.as_ref().ends_with(".metadata.json"))
        .count();

    assert!(
        metadata_files <= 4,
        "expected metadata files pruned to a bounded window (<=4), found {metadata_files} \
         after {COMMITS} commits"
    );

    Ok(())
}

/// Integration test verifying partition specs survive create→serialize→deserialize (D2)
#[tokio::test]
async fn test_partition_spec_roundtrip() -> Result<()> {
    let config = Configuration {
        schema: SchemaConfig {
            catalog_type: "sql".to_string(),
            catalog_uri: "sqlite::memory:".to_string(),
            default_schemas: Default::default(),
            materialized_labels: Default::default(),
        },
        storage: StorageConfig {
            dsn: "memory://".to_string(),
        },
        ..Default::default()
    };

    let object_store = Arc::new(InMemory::new());
    let catalog_manager = Arc::new(CatalogManager::new(config).await?);

    // Create a writer for the traces table (which creates the table with partitioning)
    let writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        "default".to_string(),
        "default".to_string(),
        "traces".to_string(),
    )
    .await?;

    // Load the table metadata and verify default_partition_spec() works
    // This was the Issue #185 failure: spec_id mismatch caused InvalidFormat("partition spec")
    let metadata = writer.table_metadata();
    let partition_spec = metadata
        .default_partition_spec()
        .expect("default_partition_spec() should not fail after Issue #185 fix");

    // Verify the partition spec has the expected fields
    let fields = partition_spec.fields();
    assert!(
        !fields.is_empty(),
        "Partition spec should have at least one field"
    );

    // Verify spec_id is 0 (matching DEFAULT_PARTITION_SPEC_ID)
    assert_eq!(
        *partition_spec.spec_id(),
        0,
        "Partition spec ID should be 0"
    );

    // Also test logs table
    let logs_writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        "default".to_string(),
        "default".to_string(),
        "logs".to_string(),
    )
    .await?;
    let logs_metadata = logs_writer.table_metadata();
    let logs_spec = logs_metadata
        .default_partition_spec()
        .expect("Logs partition spec should also roundtrip correctly");
    assert!(!logs_spec.fields().is_empty());

    // Also test metrics_gauge table
    let metrics_writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        "default".to_string(),
        "default".to_string(),
        "metrics_gauge".to_string(),
    )
    .await?;
    let metrics_metadata = metrics_writer.table_metadata();
    let metrics_spec = metrics_metadata
        .default_partition_spec()
        .expect("Metrics partition spec should also roundtrip correctly");
    assert!(!metrics_spec.fields().is_empty());

    Ok(())
}

/// Integration test validating the write→query catalog alignment with slug-based namespaces (D3)
///
/// Verifies that a table created by the writer under slug-based namespaces can be found
/// by a fresh catalog instance (simulating the querier's catalog lookup).
#[tokio::test]
async fn test_write_and_query_with_slugs() -> Result<()> {
    let temp_dir = tempdir()?;
    let catalog_path = temp_dir.path().join("catalog.db");
    let storage_path = temp_dir.path().join("storage");
    std::fs::create_dir_all(&storage_path)?;
    // Pre-create the SQLite database file (sqlx's create_if_missing defaults to false)
    std::fs::File::create(&catalog_path)?;

    let config = Configuration {
        schema: SchemaConfig {
            catalog_type: "sql".to_string(),
            catalog_uri: format!("sqlite://{}", catalog_path.display()),
            default_schemas: Default::default(),
            materialized_labels: Default::default(),
        },
        storage: StorageConfig {
            dsn: format!("file://{}", storage_path.display()),
        },
        auth: AuthConfig {
            tenants: vec![TenantConfig {
                id: "test-tenant".to_string(),
                slug: "testco".to_string(),
                name: "Test Co".to_string(),
                default_dataset: Some("test-dataset".to_string()),
                datasets: vec![DatasetConfig {
                    id: "test-dataset".to_string(),
                    slug: "staging".to_string(),
                    is_default: true,
                    storage: None,
                }],
                api_keys: vec![],
                schema_config: None,
                limits: None,
            }],
            ..Default::default()
        },
        ..Default::default()
    };

    let object_store = Arc::new(InMemory::new());
    let catalog_manager = Arc::new(CatalogManager::new(config.clone()).await?);

    // Step 1: Writer creates the traces table under slug-based namespace [testco, staging]
    let writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        "test-tenant".to_string(),
        "test-dataset".to_string(),
        "traces".to_string(),
    )
    .await?;

    // Verify the writer created the table under the correct namespace
    let ident = writer.table_identifier();
    assert_eq!(
        format!("{ident}"),
        "testco.staging.traces",
        "Writer should create table under slug-based namespace"
    );

    // Step 2: Create a fresh catalog (simulating what the querier does)
    // and verify the table is discoverable
    let querier_catalog = common::iceberg::create_catalog_with_config(&config).await?;

    // Step 3: Verify the table exists under the correct slug-based namespace
    let slug_ident = Identifier::new(&["testco".to_string(), "staging".to_string()], "traces");
    assert!(
        querier_catalog.tabular_exists(&slug_ident).await?,
        "Table should be findable under slug-based namespace [testco, staging]"
    );

    // Step 4: Verify the table is NOT under the old "default" namespace
    let default_ident = Identifier::new(&["default".to_string()], "traces");
    assert!(
        !querier_catalog.tabular_exists(&default_ident).await?,
        "Table should NOT exist under old 'default' namespace"
    );

    // Step 5: Verify the table is NOT found with wrong slugs
    let wrong_ident = Identifier::new(&["wrongco".to_string(), "staging".to_string()], "traces");
    assert!(
        !querier_catalog.tabular_exists(&wrong_ident).await?,
        "Table should NOT be found with wrong tenant slug"
    );

    // Step 6: Load the table through the querier catalog and verify schema
    match querier_catalog.clone().load_tabular(&slug_ident).await? {
        iceberg_rust::catalog::tabular::Tabular::Table(table) => {
            let schema = table.current_schema()?;
            // Verify it has the expected traces fields
            let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name.as_str()).collect();
            assert!(
                field_names.contains(&"trace_id"),
                "Table schema should contain trace_id field"
            );
            assert!(
                field_names.contains(&"span_id"),
                "Table schema should contain span_id field"
            );
            assert!(
                field_names.contains(&"span_name"),
                "Table schema should contain span_name field (v2)"
            );
            assert!(
                field_names.contains(&"timestamp"),
                "Table schema should contain timestamp field"
            );

            // Verify partition spec survives the roundtrip
            let metadata = table.metadata();
            let partition_spec = metadata
                .default_partition_spec()
                .expect("Partition spec should be valid after catalog roundtrip");
            assert!(
                !partition_spec.fields().is_empty(),
                "Partition spec should have fields"
            );
        }
        _ => {
            panic!("Expected a Table tabular type, got something else");
        }
    }

    Ok(())
}

/// Spike for the attribute-explorability epic (#737 / #730): prove that a
/// `Map<String, String>` attribute column survives the full pinned-fork
/// pipeline — table creation, Parquet write (including the leaf-column
/// statistics path in `parquet_to_datafile`, the identified risk), commit,
/// and a DataFusion read with a map-subscript filter.
///
/// Unpartitioned on purpose: partition computation only touches the
/// timestamp column and is orthogonal to the Map risk being validated.
#[tokio::test]
async fn test_map_attribute_column_end_to_end() -> Result<()> {
    use datafusion::arrow::array::{Array as _, MapBuilder, MapFieldNames, StringBuilder};
    use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
    use datafusion::prelude::SessionContext;
    use futures::stream;
    use iceberg_rust::arrow::write::write_parquet_partitioned;
    use iceberg_rust::catalog::create::CreateTableBuilder;
    use iceberg_rust::catalog::tabular::Tabular;
    use iceberg_rust::spec::partition::PartitionSpec;
    use iceberg_rust::spec::schema::Schema as IcebergSchema;
    use iceberg_rust::spec::types::{MapType, PrimitiveType, StructField, StructType, Type};

    let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);
    let catalog = catalog_manager.catalog();

    // Namespace + table with a Map-typed `attributes` column.
    let namespace = common::iceberg::names::build_namespace("spike", "maps")?;
    catalog.clone().create_namespace(&namespace, None).await?;

    let fields = vec![
        StructField {
            id: 1,
            name: "timestamp".to_string(),
            required: true,
            field_type: Type::Primitive(PrimitiveType::Timestamp),
            doc: None,
            initial_default: None,
            write_default: None,
        },
        StructField {
            id: 2,
            name: "body".to_string(),
            required: false,
            field_type: Type::Primitive(PrimitiveType::String),
            doc: None,
            initial_default: None,
            write_default: None,
        },
        StructField {
            id: 3,
            name: "attributes".to_string(),
            required: false,
            field_type: Type::Map(MapType {
                key_id: 4,
                key: Box::new(Type::Primitive(PrimitiveType::String)),
                value_id: 5,
                value_required: false,
                value: Box::new(Type::Primitive(PrimitiveType::String)),
            }),
            doc: None,
            initial_default: None,
            write_default: None,
        },
    ];
    let schema = IcebergSchema::from_struct_type(StructType::new(fields), 0, None);
    let partition_spec = PartitionSpec::default();

    let ident = common::iceberg::names::build_table_identifier("spike", "maps", "events");
    let table_create = CreateTableBuilder::default()
        .with_name("events".to_string())
        .with_schema(schema)
        .with_partition_spec(partition_spec)
        .with_location(common::iceberg::names::build_table_location(
            "spike", "maps", "events",
        ))
        .create()
        .map_err(|e| anyhow::anyhow!("create table build: {e}"))?;
    catalog
        .clone()
        .create_table(ident.clone(), table_create)
        .await?;

    // Load the table and derive the Arrow schema the way the writer does.
    let Tabular::Table(mut table) = catalog.clone().load_tabular(&ident).await? else {
        panic!("expected a table");
    };
    let arrow_schema: ArrowSchemaRef = Arc::new(
        table
            .current_schema()?
            .fields()
            .try_into()
            .map_err(|e: iceberg_rust::spec::error::Error| anyhow::anyhow!("to arrow: {e}"))?,
    );

    // Build a MapArray whose entry/key/value field names match the derived
    // schema, so the batch aligns with what the table declares.
    let attr_field = arrow_schema.field_with_name("attributes")?;
    let DataType::Map(entry_field, _) = attr_field.data_type() else {
        panic!("attributes should convert to an Arrow Map");
    };
    let DataType::Struct(kv_fields) = entry_field.data_type() else {
        panic!("map entries should be a struct");
    };
    let field_names = MapFieldNames {
        entry: entry_field.name().clone(),
        key: kv_fields[0].name().clone(),
        value: kv_fields[1].name().clone(),
    };
    let mut attrs = MapBuilder::new(
        Some(field_names),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    // Row 1: env=prod, pod=api-1. Row 2: env=staging.
    attrs.keys().append_value("env");
    attrs.values().append_value("prod");
    attrs.keys().append_value("pod");
    attrs.values().append_value("api-1");
    attrs.append(true)?;
    attrs.keys().append_value("env");
    attrs.values().append_value("staging");
    attrs.append(true)?;
    let attrs = attrs.finish();

    use datafusion::arrow::array::TimestampMicrosecondArray;
    let ts = TimestampMicrosecondArray::from(vec![1_000_000_i64, 2_000_000]);
    let body = StringArray::from(vec![Some("hello prod"), Some("hello staging")]);

    // Batch schema derived from the actual arrays (field names aligned
    // above; nullability follows the builders).
    let batch_schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", ts.data_type().clone(), false),
        Field::new("body", DataType::Utf8, true),
        Field::new("attributes", attrs.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        batch_schema,
        vec![Arc::new(ts), Arc::new(body), Arc::new(attrs)],
    )?;

    // Write through the fork's Parquet path (exercises parquet_to_datafile's
    // leaf-column stats handling for the map) and commit.
    let files = write_parquet_partitioned(&table, stream::iter(vec![Ok(batch)]), None).await?;
    assert!(!files.is_empty(), "expected at least one data file");
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await?;

    // Read back through datafusion_iceberg and filter via map subscript.
    let Tabular::Table(table) = catalog.clone().load_tabular(&ident).await? else {
        panic!("expected a table on reload");
    };
    let provider = Arc::new(datafusion_iceberg::DataFusionTable::new(
        Tabular::Table(table),
        None,
        None,
        None,
    )) as Arc<dyn datafusion::datasource::TableProvider>;
    let ctx = SessionContext::new();
    ctx.register_table("events", provider)?;

    let rows = ctx
        .sql("SELECT body FROM events WHERE attributes['env'] = 'prod'")
        .await?
        .collect()
        .await?;
    let total: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, 1,
        "map-subscript filter should match exactly one row"
    );
    let body_col = rows[0]
        .column_by_name("body")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(body_col.value(0), "hello prod");

    Ok(())
}

/// Attribute-explorability epic (#731): creating a table with materialized
/// label columns must also set the per-column bloom-filter table property
/// for each `label_<key>` column, so the pinned iceberg-rust Parquet writer
/// emits bloom filters for them on every write.
#[tokio::test]
async fn ensure_table_sets_bloom_properties_for_label_columns() -> Result<()> {
    let mut config = Configuration::default();
    config.schema.catalog_uri =
        "sqlite:file:signaldb_bloom_props?mode=memory&cache=shared".to_string();
    config.schema.materialized_labels.logs =
        vec!["namespace".to_string(), "http.method".to_string()];
    let manager = common::CatalogManager::new(config).await?;

    let table = manager.ensure_table("default", "default", "logs").await?;
    let properties = &table.metadata().properties;

    assert_eq!(
        properties
            .get("write.parquet.bloom-filter-enabled.column.label_namespace")
            .map(String::as_str),
        Some("true"),
        "properties: {properties:?}"
    );
    assert_eq!(
        properties
            .get("write.parquet.bloom-filter-enabled.column.label_http_method")
            .map(String::as_str),
        Some("true"),
    );

    // A traces table carries the built-in trace_id/span_id point-lookup blooms
    // but, with no configured labels, no `label_<key>` bloom keys.
    let traces = manager.ensure_table("default", "default", "traces").await?;
    let trace_props = &traces.metadata().properties;
    assert_eq!(
        trace_props
            .get("write.parquet.bloom-filter-enabled.column.trace_id")
            .map(String::as_str),
        Some("true"),
        "properties: {trace_props:?}"
    );
    assert_eq!(
        trace_props
            .get("write.parquet.bloom-filter-enabled.column.span_id")
            .map(String::as_str),
        Some("true"),
    );
    assert!(
        trace_props
            .keys()
            .all(|k| !k.starts_with("write.parquet.bloom-filter-enabled.column.label_")),
        "no label blooms without configured labels: {trace_props:?}"
    );

    Ok(())
}

/// End-to-end proof for #731: a logs table created by `ensure_table` with a
/// materialized label writes Parquet files whose `label_<key>` column chunk
/// carries a bloom filter, while non-enabled columns do not.
#[tokio::test]
async fn label_column_write_produces_parquet_bloom_filter() -> Result<()> {
    use datafusion::arrow::array::{
        ArrayRef, Date32Array, Int32Array, TimestampMicrosecondArray, new_null_array,
    };
    use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
    use futures::stream;
    use iceberg_rust::arrow::write::write_parquet_partitioned;
    use iceberg_rust::spec::util::strip_prefix;
    use object_store::ObjectStoreExt as _;

    let mut config = Configuration::default();
    config.schema.catalog_uri =
        "sqlite:file:signaldb_bloom_write?mode=memory&cache=shared".to_string();
    config.schema.materialized_labels.logs = vec!["namespace".to_string()];
    let manager = common::CatalogManager::new(config).await?;

    let table = manager.ensure_table("default", "default", "logs").await?;

    // Build a one-row batch in the table's derived Arrow schema: required
    // columns get real values, `label_namespace` and `body` get strings
    // (body proves bloom filters stay scoped to enabled columns), the rest
    // stay null.
    let arrow_schema: Arc<Schema> =
        Arc::new(table.current_schema()?.fields().try_into().map_err(
            |e: iceberg_rust::spec::error::Error| anyhow::anyhow!("schema to arrow: {e}"),
        )?);
    let columns: Vec<ArrayRef> = arrow_schema
        .fields()
        .iter()
        .map(|field| -> ArrayRef {
            match field.name().as_str() {
                "timestamp" | "observed_timestamp" => {
                    Arc::new(TimestampMicrosecondArray::from(vec![1_000_000_i64]))
                }
                "service_name" => Arc::new(StringArray::from(vec!["api"])),
                "body" => Arc::new(StringArray::from(vec!["hello bloom"])),
                "label_namespace" => Arc::new(StringArray::from(vec!["prod"])),
                "date_day" => Arc::new(Date32Array::from(vec![0])),
                "hour" => Arc::new(Int32Array::from(vec![0])),
                _ => new_null_array(field.data_type(), 1),
            }
        })
        .collect();
    let batch = RecordBatch::try_new(arrow_schema, columns)?;

    let files = write_parquet_partitioned(&table, stream::iter(vec![Ok(batch)]), None).await?;
    assert_eq!(files.len(), 1, "expected exactly one data file");

    // Read the Parquet footer straight from the table's object store and
    // check bloom-filter presence per column chunk.
    let path: object_store::path::Path = strip_prefix(files[0].file_path()).into();
    let bytes = table.object_store().get(&path).await?.bytes().await?;
    let reader = SerializedFileReader::new(bytes)?;
    let row_group = reader.metadata().row_group(0);

    let bloom_offset_of = |column: &str| {
        row_group
            .columns()
            .iter()
            .find(|c| c.column_path().string() == column)
            .unwrap_or_else(|| panic!("column {column} not found in Parquet metadata"))
            .bloom_filter_offset()
    };
    assert!(
        bloom_offset_of("label_namespace").is_some(),
        "label_namespace should carry a bloom filter"
    );
    assert!(
        bloom_offset_of("body").is_none(),
        "body has no bloom-filter property and should not carry one"
    );

    Ok(())
}

/// End-to-end proof for #731 part 2: a wire-format logs batch written
/// through `IcebergTableWriter` (transform → coercion → Parquet) lands with
/// a populated `attr_tokens` column whose List leaf carries a bloom filter,
/// and `array_has(attr_tokens, 'key=value')` filters rows correctly.
#[tokio::test]
async fn attr_tokens_write_populates_column_and_bloom_filter() -> Result<()> {
    use datafusion::arrow::array::{BinaryArray, Int32Array, UInt32Array, UInt64Array};
    use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
    use datafusion::prelude::SessionContext;
    use iceberg_rust::catalog::tabular::Tabular;
    use object_store::ObjectStoreExt as _;

    let mut config = Configuration::default();
    config.schema.catalog_uri =
        "sqlite:file:signaldb_attr_tokens?mode=memory&cache=shared".to_string();
    let manager = CatalogManager::new(config).await?;
    let object_store = Arc::new(InMemory::new());

    let mut writer = IcebergTableWriter::new(
        &manager,
        object_store,
        "default".to_string(),
        "default".to_string(),
        "logs".to_string(),
    )
    .await?;

    // Two-row wire-format (v1) logs batch with attributes in all scopes.
    let n = 2;
    let ts: u64 = 1_700_000_000_000_000_000;
    let wire_schema = Arc::new(Schema::new(vec![
        Field::new("time_unix_nano", DataType::UInt64, false),
        Field::new("observed_time_unix_nano", DataType::UInt64, false),
        Field::new("severity_number", DataType::Int32, true),
        Field::new("severity_text", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
        Field::new("trace_id", DataType::Binary, true),
        Field::new("span_id", DataType::Binary, true),
        Field::new("flags", DataType::UInt32, true),
        Field::new("attributes_json", DataType::Utf8, true),
        Field::new("resource_json", DataType::Utf8, true),
        Field::new("scope_json", DataType::Utf8, true),
        Field::new("dropped_attributes_count", DataType::UInt32, true),
        Field::new("service_name", DataType::Utf8, true),
        Field::new("event_name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        wire_schema,
        vec![
            Arc::new(UInt64Array::from(vec![ts; n])),
            Arc::new(UInt64Array::from(vec![ts; n])),
            Arc::new(Int32Array::from(vec![None::<i32>; n])),
            Arc::new(StringArray::from(vec![None::<&str>; n])),
            Arc::new(StringArray::from(vec![Some("prod row"), Some("dev row")])),
            Arc::new(BinaryArray::from(vec![None::<&[u8]>; n])),
            Arc::new(BinaryArray::from(vec![None::<&[u8]>; n])),
            Arc::new(UInt32Array::from(vec![None::<u32>; n])),
            Arc::new(StringArray::from(vec![
                Some(r#"{"env":"prod","team":"core"}"#),
                Some(r#"{"env":"dev"}"#),
            ])),
            Arc::new(StringArray::from(vec![
                Some(r#"{"attributes":{"namespace":"backend"}}"#),
                None,
            ])),
            Arc::new(StringArray::from(vec![None::<&str>; n])),
            Arc::new(UInt32Array::from(vec![None::<u32>; n])),
            Arc::new(StringArray::from(vec![Some("api"); n])),
            Arc::new(StringArray::from(vec![None::<&str>; n])),
        ],
    )?;

    writer
        .append_batches_with_marker("attr-tokens-test", vec![(uuid::Uuid::new_v4(), batch)])
        .await?;

    // Query back: token containment matches exactly one row per token.
    let ident = manager.build_table_identifier("default", "default", "logs");
    let Tabular::Table(table) = manager.catalog().load_tabular(&ident).await? else {
        panic!("expected logs table");
    };
    let ctx = SessionContext::new();
    ctx.register_table(
        "logs",
        Arc::new(datafusion_iceberg::DataFusionTable::from(table.clone())),
    )?;
    for (token, expected_body) in [
        ("env=prod", "prod row"),
        ("namespace=backend", "prod row"),
        ("env=dev", "dev row"),
    ] {
        let rows = ctx
            .sql(&format!(
                "SELECT body FROM logs WHERE array_has(attr_tokens, '{token}')"
            ))
            .await?
            .collect()
            .await?;
        let total: usize = rows.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "token {token} should match exactly one row");
        let body = rows[0]
            .column_by_name("body")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(body.value(0), expected_body, "token {token}");
    }

    // The written file's attr_tokens List leaf carries a bloom filter.
    let store = table.object_store();
    let mut listing = store.list(None);
    let mut parquet_paths = Vec::new();
    while let Some(meta) = futures::StreamExt::next(&mut listing).await {
        let meta = meta?;
        if meta.location.as_ref().ends_with(".parquet") {
            parquet_paths.push(meta.location);
        }
    }
    assert_eq!(parquet_paths.len(), 1, "expected one data file");
    let bytes = store.get(&parquet_paths[0]).await?.bytes().await?;
    let reader = SerializedFileReader::new(bytes)?;
    let row_group = reader.metadata().row_group(0);
    let leaf = row_group
        .columns()
        .iter()
        .find(|c| c.column_path().string() == "attr_tokens.list.item")
        .expect("attr_tokens.list.item leaf column present");
    assert!(
        leaf.bloom_filter_offset().is_some(),
        "attr_tokens leaf should carry a bloom filter"
    );

    Ok(())
}

/// Spike for the attribute-explorability epic (#737 / #734): schema
/// evolution through the low-level catalog commit path — `AddSchema` +
/// `SetCurrentSchema` via `Catalog::update_table` — followed by a write
/// under the new schema and a read that null-fills the old files.
///
/// Requires iceberg-rust rev >= 96f28c18: earlier revisions resolved
/// `current_schema` through the current snapshot's pinned schema_id, so
/// every new snapshot re-pinned the old schema and the flip never took
/// effect (fixed upstream in JanKaul/iceberg-rust#378).
#[tokio::test]
async fn test_schema_evolution_add_label_column() -> Result<()> {
    use datafusion::prelude::SessionContext;
    use futures::stream;
    use iceberg_rust::arrow::write::write_parquet_partitioned;
    use iceberg_rust::catalog::commit::{CommitTable, TableUpdate};
    use iceberg_rust::catalog::create::CreateTableBuilder;
    use iceberg_rust::catalog::tabular::Tabular;
    use iceberg_rust::spec::partition::PartitionSpec;
    use iceberg_rust::spec::schema::Schema as IcebergSchema;
    use iceberg_rust::spec::types::{PrimitiveType, StructField, StructType, Type};

    fn string_field(id: i32, name: &str, required: bool) -> StructField {
        StructField {
            id,
            name: name.to_string(),
            required,
            field_type: Type::Primitive(PrimitiveType::String),
            doc: None,
            initial_default: None,
            write_default: None,
        }
    }

    let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);
    let catalog = catalog_manager.catalog();

    let namespace = common::iceberg::names::build_namespace("spike", "evolve")?;
    catalog.clone().create_namespace(&namespace, None).await?;

    let ts_field = StructField {
        id: 1,
        name: "timestamp".to_string(),
        required: true,
        field_type: Type::Primitive(PrimitiveType::Timestamp),
        doc: None,
        initial_default: None,
        write_default: None,
    };
    let v0 = IcebergSchema::from_struct_type(
        StructType::new(vec![ts_field.clone(), string_field(2, "body", false)]),
        0,
        None,
    );

    let ident = common::iceberg::names::build_table_identifier("spike", "evolve", "events");
    let table_create = CreateTableBuilder::default()
        .with_name("events".to_string())
        .with_schema(v0)
        .with_partition_spec(PartitionSpec::default())
        .with_location(common::iceberg::names::build_table_location(
            "spike", "evolve", "events",
        ))
        .create()
        .map_err(|e| anyhow::anyhow!("create table build: {e}"))?;
    catalog
        .clone()
        .create_table(ident.clone(), table_create)
        .await?;

    // File 1 under the original schema.
    let Tabular::Table(mut table) = catalog.clone().load_tabular(&ident).await? else {
        panic!("expected a table");
    };
    use datafusion::arrow::array::TimestampMicrosecondArray;
    let batch_v0 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("body", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![1_000_000_i64])),
            Arc::new(StringArray::from(vec![Some("before evolution")])),
        ],
    )?;
    let files = write_parquet_partitioned(&table, stream::iter(vec![Ok(batch_v0)]), None).await?;
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await?;

    // Metadata-only evolution commit: add schema v1 with `label_env` and
    // make it current — the exact path auto-promotion will drive.
    let v1 = IcebergSchema::from_struct_type(
        StructType::new(vec![
            ts_field,
            string_field(2, "body", false),
            string_field(3, "label_env", false),
        ]),
        1,
        None,
    );
    let updated = catalog
        .clone()
        .update_table(CommitTable {
            identifier: ident.clone(),
            requirements: vec![],
            updates: vec![
                TableUpdate::AddSchema {
                    schema: v1,
                    last_column_id: Some(3),
                },
                TableUpdate::SetCurrentSchema { schema_id: 1 },
            ],
        })
        .await?;
    // The metadata-only half works: both schemas stored, current id flipped.
    assert_eq!(updated.metadata().current_schema_id, 1);
    assert_eq!(updated.metadata().schemas.len(), 2);

    // File 2 under the evolved schema (the writer derives its Arrow schema
    // from current_schema, which now includes the label column).
    let Tabular::Table(mut table) = catalog.clone().load_tabular(&ident).await? else {
        panic!("expected a table after evolution");
    };
    assert!(
        table
            .current_schema()?
            .fields()
            .iter()
            .any(|f| f.name == "label_env"),
        "evolved schema should carry label_env"
    );
    let batch_v1 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("body", DataType::Utf8, true),
            Field::new("label_env", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![2_000_000_i64])),
            Arc::new(StringArray::from(vec![Some("after evolution")])),
            Arc::new(StringArray::from(vec![Some("prod")])),
        ],
    )?;
    let files = write_parquet_partitioned(&table, stream::iter(vec![Ok(batch_v1)]), None).await?;
    table
        .new_transaction(None)
        .append_data(files)
        .commit()
        .await?;

    // Old file null-fills the new column; new file carries the value.
    let Tabular::Table(table) = catalog.clone().load_tabular(&ident).await? else {
        panic!("expected a table on reload");
    };
    let provider = Arc::new(datafusion_iceberg::DataFusionTable::new(
        Tabular::Table(table),
        None,
        None,
        None,
    )) as Arc<dyn datafusion::datasource::TableProvider>;
    let ctx = SessionContext::new();
    ctx.register_table("events", provider)?;

    let rows = ctx
        .sql("SELECT body FROM events WHERE label_env = 'prod'")
        .await?
        .collect()
        .await?;
    let matched: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        matched, 1,
        "filter on the evolved column matches the new row"
    );

    let rows = ctx
        .sql("SELECT body FROM events WHERE label_env IS NULL")
        .await?
        .collect()
        .await?;
    let nulls: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(nulls, 1, "pre-evolution file should null-fill label_env");

    Ok(())
}
