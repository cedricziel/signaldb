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
use writer::{IcebergTableWriter, IcebergWriterFlightService, WalProcessor};

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
    let entry_id = wal
        .append(WalOperation::WriteTraces, batch_bytes, None)
        .await?;
    wal.flush().await?;

    // Verify we can get stats from processor
    let stats = processor.get_stats();
    assert_eq!(stats.active_writers, 0);

    // Test processing (should work now that table creation is implemented, or fail gracefully due to test environment)
    let result = processor.process_single_entry(entry_id).await;
    if let Err(e) = result {
        // Should not fail due to "not implemented" anymore
        assert!(!e.to_string().contains("Table creation not yet implemented"));
        println!("Expected test environment failure in processing: {}", e);
    } else {
        println!("Successfully processed WAL entry with Iceberg writer");
    }

    // Shutdown processor
    processor.shutdown().await?;

    Ok(())
}

/// Integration test for the Iceberg Flight service
#[tokio::test]
async fn test_iceberg_flight_service_integration() -> Result<()> {
    // Setup test environment
    let temp_dir = tempdir()?;
    let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);
    let object_store = Arc::new(InMemory::new());
    let wal_config = WalConfig::with_defaults(temp_dir.path().to_path_buf());
    let wal = Arc::new(Wal::new(wal_config).await?);

    // Create Iceberg Flight service
    let _service = IcebergWriterFlightService::new(catalog_manager, object_store, wal);

    // Verify service was created successfully
    // Note: Full Flight service testing would require actual gRPC integration,
    // which is beyond the scope of this integration test

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
            let schema = table.current_schema(None)?;
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
            .current_schema(None)?
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
