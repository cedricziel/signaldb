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
use common::wal::manager::WalManager;
use common::wal::{Wal, WalConfig, WalOperation, record_batch_to_bytes};
use datafusion::arrow::array::Int64Array;
use datafusion::prelude::SessionContext;
use datafusion_iceberg::DataFusionTable;
use iceberg_rust::catalog::tabular::Tabular;
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;
use tests_integration::test_support::metrics_gauge_wire_batch;

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

/// The writer holds one WAL per tenant/dataset/signal, so open the fixed test
/// tenant's WAL through a manager and drive the processor with that manager.
async fn open_writer_wal(config: &WalConfig) -> Result<(Arc<WalManager>, Arc<Wal>)> {
    let manager = Arc::new(WalManager::uniform(config.clone()));
    let wal = manager.get_wal("default", "default", "metrics").await?;
    Ok((manager, wal))
}

/// Delete the WAL index files, simulating a crash where the Iceberg commit
/// landed but the processed-index write did not. Walks the tenant tree, since
/// the segments live under `{tenant}/{dataset}/{signal}/`.
async fn drop_wal_indexes(wal_dir: &Path) -> Result<()> {
    let deleted = drop_wal_indexes_in(wal_dir).await?;
    anyhow::ensure!(deleted > 0, "expected at least one WAL index file");
    Ok(())
}

async fn drop_wal_indexes_in(dir_path: &Path) -> Result<usize> {
    let mut deleted = 0;
    let mut dir = tokio::fs::read_dir(dir_path).await?;
    while let Some(entry) = dir.next_entry().await? {
        let path = entry.path();
        if path.is_dir() {
            deleted += Box::pin(drop_wal_indexes_in(&path)).await?;
        } else if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.ends_with(".index"))
        {
            tokio::fs::remove_file(path).await?;
            deleted += 1;
        }
    }
    Ok(deleted)
}

#[tokio::test]
async fn replay_after_crash_does_not_duplicate_rows() -> Result<()> {
    let wal_dir = tempdir()?;
    let wal_config = WalConfig::with_defaults(wal_dir.path().to_path_buf());
    let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);

    // Ingest two entries and process them normally.
    let (wal_manager, wal) = open_writer_wal(&wal_config).await?;
    let batch1 = metrics_gauge_wire_batch(&[1.0, 2.0, 3.0])?;
    let batch2 = metrics_gauge_wire_batch(&[4.0, 5.0])?;
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

    let mut processor = tests_integration::test_support::processor_with_type_authority(
        wal_manager.clone(),
        catalog_manager.clone(),
    )
    .await?;
    processor.process_pending_entries().await?;
    assert!(
        wal.get_unprocessed_entries().await?.is_empty(),
        "all entries should be marked processed after the first pass"
    );
    assert_eq!(count_rows(&catalog_manager, "metrics").await?, 5);

    // Simulate the crash: the commit landed, the index write did not.
    processor.shutdown().await?;
    drop(processor);
    drop(wal);
    drop(wal_manager);
    drop_wal_indexes(wal_dir.path()).await?;

    // Restart: entries load as unprocessed and are replayed.
    let (wal_manager, wal) = open_writer_wal(&wal_config).await?;
    let replayed = wal.get_unprocessed_entries().await?;
    assert_eq!(replayed.len(), 2, "index loss must resurface the entries");

    let mut processor = tests_integration::test_support::processor_with_type_authority(
        wal_manager.clone(),
        catalog_manager.clone(),
    )
    .await?;
    processor.process_pending_entries().await?;

    // The idempotency marker must prevent re-inserting the committed rows.
    assert_eq!(
        count_rows(&catalog_manager, "metrics").await?,
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

    let (wal_manager, wal) = open_writer_wal(&wal_config).await?;
    let batch1 = metrics_gauge_wire_batch(&[1.0, 2.0])?;
    wal.append(
        WalOperation::WriteMetrics,
        record_batch_to_bytes(&batch1)?,
        None,
    )
    .await?;
    wal.flush().await?;

    let mut processor = tests_integration::test_support::processor_with_type_authority(
        wal_manager.clone(),
        catalog_manager.clone(),
    )
    .await?;
    processor.process_pending_entries().await?;
    assert_eq!(count_rows(&catalog_manager, "metrics").await?, 2);
    processor.shutdown().await?;
    drop(processor);
    drop(wal);
    drop(wal_manager);
    drop_wal_indexes(wal_dir.path()).await?;

    // Restart with the old entry resurfaced AND a new entry appended: the
    // old one must be skipped, the new one committed.
    let (wal_manager, wal) = open_writer_wal(&wal_config).await?;
    let batch2 = metrics_gauge_wire_batch(&[3.0, 4.0, 5.0])?;
    wal.append(
        WalOperation::WriteMetrics,
        record_batch_to_bytes(&batch2)?,
        None,
    )
    .await?;
    wal.flush().await?;
    assert_eq!(wal.get_unprocessed_entries().await?.len(), 2);

    let mut processor = tests_integration::test_support::processor_with_type_authority(
        wal_manager.clone(),
        catalog_manager.clone(),
    )
    .await?;
    processor.process_pending_entries().await?;

    assert_eq!(
        count_rows(&catalog_manager, "metrics").await?,
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

    let (wal_manager, wal) = open_writer_wal(&wal_config).await?;
    let batch = metrics_gauge_wire_batch(&[1.0])?;
    wal.append(
        WalOperation::WriteMetrics,
        record_batch_to_bytes(&batch)?,
        None,
    )
    .await?;
    wal.flush().await?;

    let mut processor = tests_integration::test_support::processor_with_type_authority(
        wal_manager.clone(),
        catalog_manager.clone(),
    )
    .await?;
    processor.process_pending_entries().await?;
    processor.shutdown().await?;
    drop(processor);
    drop(wal);
    drop(wal_manager);

    // Crash-replay twice in a row: still exactly one row.
    for _ in 0..2 {
        drop_wal_indexes(wal_dir.path()).await?;
        let (wal_manager, _wal) = open_writer_wal(&wal_config).await?;
        let mut processor = tests_integration::test_support::processor_with_type_authority(
            wal_manager.clone(),
            catalog_manager.clone(),
        )
        .await?;
        processor.process_pending_entries().await?;
        assert_eq!(count_rows(&catalog_manager, "metrics").await?, 1);
        processor.shutdown().await?;
    }

    Ok(())
}
