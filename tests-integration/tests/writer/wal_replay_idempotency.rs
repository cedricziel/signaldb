//! Regression tests for issue #546: at-least-once WAL processing must not
//! duplicate rows in Iceberg when entries are replayed after a crash
//! between the Iceberg commit and the WAL index write.
//!
//! The crash is simulated by deleting the WAL `.index` files after a
//! successful processing pass — exactly the state a process death leaves
//! behind when the commit landed but `mark_processed` never persisted —
//! then reprocessing with a fresh WAL and processor.

use anyhow::Result;
use common::CatalogManager;
use common::iceberg::names::build_table_identifier;
use common::wal::{Wal, WalConfig, WalOperation, record_batch_to_bytes};
use datafusion::arrow::array::{
    Array, Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::prelude::SessionContext;
use datafusion_iceberg::DataFusionTable;
use iceberg_rust::catalog::tabular::Tabular;
use object_store::memory::InMemory;
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;
use writer::WalProcessor;

/// Build a batch in the metrics_gauge storage schema.
fn metrics_gauge_batch(values: &[f64]) -> Result<RecordBatch> {
    let n = values.len();
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

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(
                (0..n).map(|i| 1_000_000_000 + i as i64).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>; n])),
            Arc::new(StringArray::from(vec!["test-service"; n])),
            Arc::new(StringArray::from(vec!["cpu.usage"; n])),
            Arc::new(StringArray::from(vec![None::<&str>; n])),
            Arc::new(StringArray::from(vec![Some("%"); n])),
            Arc::new(Float64Array::from(values.to_vec())),
            Arc::new(Int32Array::from(vec![None::<i32>; n])),
            Arc::new(StringArray::from(vec![None::<&str>; n])),
            Arc::new(StringArray::from(vec![None::<&str>; n])),
            Arc::new(StringArray::from(vec![None::<&str>; n])),
            Arc::new(StringArray::from(vec![None::<&str>; n])),
            Arc::new(StringArray::from(vec![None::<&str>; n])),
            Arc::new(StringArray::from(vec![None::<&str>; n])),
            Arc::new(Int32Array::from(vec![None::<i32>; n])),
            Arc::new(StringArray::from(vec![None::<&str>; n])),
            Arc::new(StringArray::from(vec![None::<&str>; n])),
            Arc::new(Date32Array::from(vec![19000; n])),
            Arc::new(Int32Array::from(vec![10; n])),
        ],
    )?;
    Ok(batch)
}

/// Count rows in the table by loading it fresh from the catalog (bypassing
/// any cached handle) and running SELECT COUNT(*).
async fn count_rows(catalog_manager: &CatalogManager, table_name: &str) -> Result<i64> {
    let ident = build_table_identifier("default", "default", table_name);
    let tabular = catalog_manager
        .catalog()
        .load_tabular(&ident)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to load table {ident}: {e}"))?;
    let table = match tabular {
        Tabular::Table(table) => table,
        _ => anyhow::bail!("Expected a table for {ident}"),
    };

    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(DataFusionTable::from(table)))?;
    let batches = ctx.sql("SELECT COUNT(*) FROM t").await?.collect().await?;
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("COUNT(*) should be Int64")
        .value(0);
    Ok(count)
}

/// Delete the WAL index files, simulating a crash where the Iceberg commit
/// landed but the processed-index write did not.
async fn drop_wal_indexes(wal_dir: &Path) -> Result<()> {
    let mut deleted = 0;
    let mut dir = tokio::fs::read_dir(wal_dir).await?;
    while let Some(entry) = dir.next_entry().await? {
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.ends_with(".index"))
        {
            tokio::fs::remove_file(entry.path()).await?;
            deleted += 1;
        }
    }
    anyhow::ensure!(deleted > 0, "expected at least one WAL index file");
    Ok(())
}

#[tokio::test]
async fn replay_after_crash_does_not_duplicate_rows() -> Result<()> {
    let wal_dir = tempdir()?;
    let wal_config = WalConfig::with_defaults(wal_dir.path().to_path_buf());
    let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);
    let object_store = Arc::new(InMemory::new());

    // Ingest two entries and process them normally.
    let wal = Arc::new(Wal::new(wal_config.clone()).await?);
    let batch1 = metrics_gauge_batch(&[1.0, 2.0, 3.0])?;
    let batch2 = metrics_gauge_batch(&[4.0, 5.0])?;
    wal.append(
        WalOperation::WriteMetrics,
        record_batch_to_bytes(&batch1)?,
        None,
    )
    .await?;
    wal.append(
        WalOperation::WriteMetrics,
        record_batch_to_bytes(&batch2)?,
        None,
    )
    .await?;
    wal.flush().await?;

    let mut processor =
        WalProcessor::new(wal.clone(), catalog_manager.clone(), object_store.clone());
    processor.process_pending_entries().await?;
    assert!(
        wal.get_unprocessed_entries().await?.is_empty(),
        "all entries should be marked processed after the first pass"
    );
    assert_eq!(count_rows(&catalog_manager, "metrics_gauge").await?, 5);

    // Simulate the crash: the commit landed, the index write did not.
    processor.shutdown().await?;
    drop(processor);
    drop(wal);
    drop_wal_indexes(wal_dir.path()).await?;

    // Restart: entries load as unprocessed and are replayed.
    let wal = Arc::new(Wal::new(wal_config.clone()).await?);
    let replayed = wal.get_unprocessed_entries().await?;
    assert_eq!(replayed.len(), 2, "index loss must resurface the entries");

    let mut processor =
        WalProcessor::new(wal.clone(), catalog_manager.clone(), object_store.clone());
    processor.process_pending_entries().await?;

    // The idempotency marker must prevent re-inserting the committed rows.
    assert_eq!(
        count_rows(&catalog_manager, "metrics_gauge").await?,
        5,
        "replay after crash must not duplicate rows"
    );
    assert!(
        wal.get_unprocessed_entries().await?.is_empty(),
        "replayed entries should be re-marked processed"
    );

    Ok(())
}

#[tokio::test]
async fn mixed_replay_commits_only_new_entries() -> Result<()> {
    let wal_dir = tempdir()?;
    let wal_config = WalConfig::with_defaults(wal_dir.path().to_path_buf());
    let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);
    let object_store = Arc::new(InMemory::new());

    let wal = Arc::new(Wal::new(wal_config.clone()).await?);
    let batch1 = metrics_gauge_batch(&[1.0, 2.0])?;
    wal.append(
        WalOperation::WriteMetrics,
        record_batch_to_bytes(&batch1)?,
        None,
    )
    .await?;
    wal.flush().await?;

    let mut processor =
        WalProcessor::new(wal.clone(), catalog_manager.clone(), object_store.clone());
    processor.process_pending_entries().await?;
    assert_eq!(count_rows(&catalog_manager, "metrics_gauge").await?, 2);
    processor.shutdown().await?;
    drop(processor);
    drop(wal);
    drop_wal_indexes(wal_dir.path()).await?;

    // Restart with the old entry resurfaced AND a new entry appended: the
    // old one must be skipped, the new one committed.
    let wal = Arc::new(Wal::new(wal_config.clone()).await?);
    let batch2 = metrics_gauge_batch(&[3.0, 4.0, 5.0])?;
    wal.append(
        WalOperation::WriteMetrics,
        record_batch_to_bytes(&batch2)?,
        None,
    )
    .await?;
    wal.flush().await?;
    assert_eq!(wal.get_unprocessed_entries().await?.len(), 2);

    let mut processor =
        WalProcessor::new(wal.clone(), catalog_manager.clone(), object_store.clone());
    processor.process_pending_entries().await?;

    assert_eq!(
        count_rows(&catalog_manager, "metrics_gauge").await?,
        5,
        "old entry must be deduplicated, new entry committed"
    );
    assert!(wal.get_unprocessed_entries().await?.is_empty());

    Ok(())
}

#[tokio::test]
async fn processing_is_idempotent_across_repeated_replays() -> Result<()> {
    let wal_dir = tempdir()?;
    let wal_config = WalConfig::with_defaults(wal_dir.path().to_path_buf());
    let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);
    let object_store = Arc::new(InMemory::new());

    let wal = Arc::new(Wal::new(wal_config.clone()).await?);
    let batch = metrics_gauge_batch(&[1.0])?;
    wal.append(
        WalOperation::WriteMetrics,
        record_batch_to_bytes(&batch)?,
        None,
    )
    .await?;
    wal.flush().await?;

    let mut processor =
        WalProcessor::new(wal.clone(), catalog_manager.clone(), object_store.clone());
    processor.process_pending_entries().await?;
    processor.shutdown().await?;
    drop(processor);
    drop(wal);

    // Crash-replay twice in a row: still exactly one row.
    for _ in 0..2 {
        drop_wal_indexes(wal_dir.path()).await?;
        let wal = Arc::new(Wal::new(wal_config.clone()).await?);
        let mut processor =
            WalProcessor::new(wal.clone(), catalog_manager.clone(), object_store.clone());
        processor.process_pending_entries().await?;
        assert_eq!(count_rows(&catalog_manager, "metrics_gauge").await?, 1);
        processor.shutdown().await?;
    }

    Ok(())
}
