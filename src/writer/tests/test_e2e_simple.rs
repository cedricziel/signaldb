use anyhow::Result;
use common::CatalogManager;
use common::config::{Configuration, DefaultSchemas, SchemaConfig, StorageConfig};
use datafusion::arrow::array::{
    Date32Array, Float64Array, Int32Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use object_store::memory::InMemory;
use std::sync::Arc;
use writer::IcebergTableWriter;

async fn create_writer(config: Configuration, tenant_id: &str) -> Result<IcebergTableWriter> {
    let catalog_manager = CatalogManager::new(config).await?;
    let object_store = Arc::new(InMemory::new());
    IcebergTableWriter::new(
        &catalog_manager,
        object_store,
        tenant_id.to_string(),
        "test_dataset".to_string(),
        "metrics_gauge".to_string(),
    )
    .await
}

/// Simple E2E test configuration
fn create_simple_test_config() -> Configuration {
    Configuration {
        schema: SchemaConfig {
            catalog_type: "sql".to_string(),
            catalog_uri: "sqlite::memory:".to_string(),
            default_schemas: DefaultSchemas {
                traces_enabled: true,
                logs_enabled: true,
                metrics_enabled: true,
                custom_schemas: Default::default(),
            },
        },
        storage: StorageConfig {
            dsn: "memory://".to_string(),
        },
        ..Default::default()
    }
}

/// Create simple test data for metrics_gauge table
fn create_simple_test_data(num_rows: usize) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "start_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_description", DataType::Utf8, true),
        Field::new("metric_unit", DataType::Utf8, true),
        Field::new("value", DataType::Float64, false),
        Field::new("flags", DataType::Int32, true),
        Field::new("resource_schema_url", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_schema_url", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Utf8, true),
        Field::new("scope_dropped_attr_count", DataType::Int32, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("exemplars", DataType::Utf8, true),
        Field::new("date_day", DataType::Date32, false),
        Field::new("hour", DataType::Int32, false),
    ]));

    let timestamps: Vec<i64> = (0..num_rows)
        .map(|i| 1_000_000_000 + (i as i64 * 1_000_000))
        .collect();
    let values: Vec<f64> = (0..num_rows).map(|i| i as f64).collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps)),
            Arc::new(TimestampNanosecondArray::from(vec![None; num_rows])),
            Arc::new(StringArray::from(vec!["e2e-simple-service"; num_rows])),
            Arc::new(StringArray::from(vec!["simple.metric"; num_rows])),
            Arc::new(StringArray::from(vec![
                Some("Simple test metric");
                num_rows
            ])),
            Arc::new(StringArray::from(vec![Some("count"); num_rows])),
            Arc::new(Float64Array::from(values)),
            Arc::new(Int32Array::from(vec![None; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![
                Some("{\"host\":\"simple-test\"}");
                num_rows
            ])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(Int32Array::from(vec![None; num_rows])),
            Arc::new(StringArray::from(vec![
                Some("{\"type\":\"simple\"}");
                num_rows
            ])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(Date32Array::from(vec![19000; num_rows])),
            Arc::new(Int32Array::from(vec![10; num_rows])),
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
