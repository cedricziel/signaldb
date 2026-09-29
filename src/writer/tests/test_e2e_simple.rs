use anyhow::Result;
use common::CatalogManager;
use common::config::{Configuration, SchemaConfig, StorageConfig};
use common::schema::type_authority::TypeAuthority;
use datafusion::arrow::array::{BooleanArray, Int32Array, RecordBatch, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;
use writer::IcebergTableWriter;

/// `metrics`' current `schemas.toml` version is the typed attribute
/// layout, so a writer that actually commits a batch needs a `TypeAuthority`
/// attached (see `IcebergTableWriter::with_type_authority`).
async fn create_writer(config: Configuration, tenant_id: &str) -> Result<IcebergTableWriter> {
    let catalog_manager = CatalogManager::new(config).await?;
    let sql_catalog = common::catalog::Catalog::new_in_memory().await?;
    let resolver = common::schema_registry::SchemaResolver::new(sql_catalog.clone());
    let type_authority = Arc::new(TypeAuthority::new(
        sql_catalog,
        resolver,
        Arc::new(Configuration::default()),
    ));
    let writer = IcebergTableWriter::new(
        &catalog_manager,
        tenant_id.to_string(),
        "test_dataset".to_string(),
        "metrics".to_string(),
    )
    .await?;
    Ok(writer.with_type_authority(type_authority))
}

/// Simple E2E test configuration
fn create_simple_test_config() -> Configuration {
    Configuration {
        schema: SchemaConfig {
            catalog_type: "sql".to_string(),
            catalog_uri: "sqlite::memory:".to_string(),
            ..Default::default()
        },
        storage: StorageConfig {
            dsn: "memory://".to_string(),
        },
        ..Default::default()
    }
}

/// Create simple wire-format (`data_json`) gauge data, the shape the acceptor
/// hands the writer before the wide-table transform runs.
fn create_simple_test_data(num_rows: usize) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("unit", DataType::Utf8, true),
        Field::new("start_time_unix_nano", DataType::UInt64, true),
        Field::new("time_unix_nano", DataType::UInt64, false),
        Field::new("attributes_json", DataType::Utf8, true),
        Field::new("resource_json", DataType::Utf8, true),
        Field::new("scope_json", DataType::Utf8, true),
        Field::new("metric_type", DataType::Utf8, false),
        Field::new("data_json", DataType::Utf8, false),
        Field::new("aggregation_temporality", DataType::Int32, true),
        Field::new("is_monotonic", DataType::Boolean, true),
    ]));

    let times: Vec<u64> = (0..num_rows)
        .map(|i| 1_700_000_001_000_000_000 + (i as u64 * 1_000_000))
        .collect();
    let data_json: Vec<String> = times
        .iter()
        .enumerate()
        .map(|(i, t)| {
            format!(
                r#"[{{"time_unix_nano":{t},"start_time_unix_nano":1700000000000000000,"value":{i}.0,"attributes":{{"host":"simple-test"}}}}]"#
            )
        })
        .collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["simple.metric"; num_rows])),
            Arc::new(StringArray::from(vec![
                Some("Simple test metric");
                num_rows
            ])),
            Arc::new(StringArray::from(vec![Some("count"); num_rows])),
            Arc::new(UInt64Array::from(vec![
                Some(1_700_000_000_000_000_000u64);
                num_rows
            ])),
            Arc::new(UInt64Array::from(times)),
            Arc::new(StringArray::from(vec![Some("{}"); num_rows])),
            Arc::new(StringArray::from(vec![
                Some(
                    r#"{"service.name":"e2e-simple-service"}"#
                );
                num_rows
            ])),
            Arc::new(StringArray::from(vec![Some(r#"{"name":"e2e"}"#); num_rows])),
            Arc::new(StringArray::from(vec!["gauge"; num_rows])),
            Arc::new(StringArray::from(data_json)),
            Arc::new(Int32Array::from(vec![None::<i32>; num_rows])),
            Arc::new(BooleanArray::from(vec![None::<bool>; num_rows])),
        ],
    )?;

    Ok(batch)
}

#[tokio::test]
async fn test_simple_e2e_append_with_marker() -> Result<()> {
    let config = create_simple_test_config();
    let mut writer = create_writer(config, "simple_tenant").await?;

    let entry_id = uuid::Uuid::new_v4();
    let test_data = create_simple_test_data(10)?;

    writer
        .append_batches_with_marker("wal-e2e", vec![(entry_id, test_data)])
        .await?;

    // The marker is the commit's proof of durability: it must contain
    // exactly the entry id we appended.
    let committed = writer.load_committed_marker("wal-e2e").await?;
    assert_eq!(committed, std::iter::once(entry_id).collect());

    Ok(())
}

#[tokio::test]
async fn test_simple_e2e_append_multiple_entries() -> Result<()> {
    let config = create_simple_test_config();
    let mut writer = create_writer(config, "simple_tenant").await?;

    let entries: Vec<_> = [5usize, 7, 3]
        .into_iter()
        .map(|rows| Ok((uuid::Uuid::new_v4(), create_simple_test_data(rows)?)))
        .collect::<Result<_>>()?;
    let ids: std::collections::HashSet<uuid::Uuid> = entries.iter().map(|(id, _)| *id).collect();

    writer
        .append_batches_with_marker("wal-e2e", entries)
        .await?;

    let committed = writer.load_committed_marker("wal-e2e").await?;
    assert_eq!(committed, ids);

    Ok(())
}
