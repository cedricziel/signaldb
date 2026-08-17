//! Ingest writes files that are sorted by the table's declared key and say so.
//!
//! The honesty invariant of `declared-data-ordering`: a file may only be
//! attributed the declared sort order if its rows really are in that order.
//! These tests drive the real writer persist path and check both halves —
//! the rows on disk, and the claim the manifest entry makes about them.

use anyhow::Result;
use common::catalog_manager::CatalogManager;
use object_store::memory::InMemory;
use std::sync::Arc;
use tests_integration::compaction_helpers::{
    aligned_hour_start, attested_sort_order_ids, context_for, load_table, trace_sort_keys,
};
use tests_integration::generators;
use writer::IcebergTableWriter;

const TENANT: &str = "sort-tenant";
const DATASET: &str = "sort-dataset";
const TABLE: &str = "traces";

async fn writer_for(table: &str) -> Result<(Arc<CatalogManager>, IcebergTableWriter)> {
    let config = common::testing::TestConfigBuilder::new()
        .in_memory()
        .with_tenant(TENANT, DATASET)
        .build();
    let catalog_manager = Arc::new(CatalogManager::new(config).await?);
    let writer = IcebergTableWriter::new(
        &catalog_manager,
        Arc::new(InMemory::new()),
        TENANT.to_string(),
        DATASET.to_string(),
        table.to_string(),
    )
    .await?;
    Ok((catalog_manager, writer))
}

/// The `trace_id` values of the table, in the order the scan yields them —
/// which for a single data file is the order they are stored in.
async fn stored_trace_ids(catalog_manager: &Arc<CatalogManager>) -> Result<Vec<String>> {
    let table = load_table(catalog_manager, TENANT, DATASET, TABLE).await?;
    let ctx = context_for(TABLE, table, false)?;
    let batches = ctx
        .sql(&format!("SELECT timestamp, trace_id FROM {TABLE}"))
        .await?
        .collect()
        .await?;
    Ok(trace_sort_keys(&batches)?
        .into_iter()
        .map(|(_, trace_id)| trace_id)
        .collect())
}

#[tokio::test]
async fn an_out_of_order_batch_group_is_persisted_sorted_and_attested() -> Result<()> {
    tests_integration::init_test_logging();
    let (catalog_manager, mut writer) = writer_for(TABLE).await?;

    // Every span shares one timestamp, so the declared traces key
    // (timestamp, trace_id) reduces to trace_id — and these ids arrive in
    // exactly the wrong order.
    let ids: Vec<String> = ["trace-e", "trace-b", "trace-d", "trace-a", "trace-c"]
        .iter()
        .map(|id| id.to_string())
        .collect();
    generators::generate_trace_files_with_ids(
        &mut writer,
        std::slice::from_ref(&ids),
        aligned_hour_start(2),
    )
    .await?;

    let stored = stored_trace_ids(&catalog_manager).await?;
    let mut expected = ids;
    expected.sort();
    assert_eq!(
        stored, expected,
        "the persisted file must hold its rows in the declared key's order"
    );

    let table = load_table(&catalog_manager, TENANT, DATASET, TABLE).await?;
    let attestations = attested_sort_order_ids(&table).await?;
    assert_eq!(attestations.len(), 1, "one write, one data file");
    assert!(
        attestations.values().all(|id| *id == Some(1)),
        "a sorted ingest file must attest the declared order: {attestations:?}"
    );

    Ok(())
}

#[tokio::test]
async fn every_ingest_file_of_a_multi_write_table_is_attested() -> Result<()> {
    // Attribution is per file, so it has to hold for every write, not just
    // the first: one unattested file in a range is enough to make the whole
    // scan keep its explicit sort.
    tests_integration::init_test_logging();
    let (catalog_manager, mut writer) = writer_for(TABLE).await?;

    let files: Vec<Vec<String>> = (0..4)
        .map(|file| {
            (0..5)
                .rev()
                .map(|row| format!("trace-{file}-{row}"))
                .collect()
        })
        .collect();
    generators::generate_trace_files_with_ids(&mut writer, &files, aligned_hour_start(3)).await?;

    let table = load_table(&catalog_manager, TENANT, DATASET, TABLE).await?;
    let attestations = attested_sort_order_ids(&table).await?;
    assert_eq!(attestations.len(), 4, "four writes, four data files");
    assert!(
        attestations.values().all(|id| *id == Some(1)),
        "every ingest file must be attested: {attestations:?}"
    );

    Ok(())
}
