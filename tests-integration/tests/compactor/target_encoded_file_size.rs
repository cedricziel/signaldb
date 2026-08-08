//! Compaction output must roll files on real *encoded* (Parquet, on-disk)
//! bytes, not the chunker's in-memory batch-size estimate (openspec change
//! `compactor-partition-scoped-lifecycle`, task 5.4; design decision D4).
//!
//! Before this fix, `ensure_table` never set the standard Iceberg property
//! `write.target-file-size-bytes`, so the pinned iceberg-rust writer (which
//! rolls output files on its own real bytes-written feedback,
//! JanKaul/iceberg-rust#388) always rolled at its unset-property fallback of
//! 512 MiB. `[compactor].target_file_size_mb` (default 128 MiB) therefore had
//! no effect on the writer at all: however many small files a partition held,
//! compaction merged them into exactly one output file, since no partition in
//! a homelab-scale deployment gets anywhere near 512 MiB. This test seeds a
//! partition whose real encoded size is a small multiple of a deliberately
//! tiny compaction target and asserts the rewrite actually rolls into several
//! files approximating that target, rather than collapsing to one file far
//! below it.

use anyhow::{Context, Result};
use common::catalog_manager::CatalogManager;
use compactor::executor::{CompactionExecutor, CompactionStatus, ExecutorConfig};
use compactor::iceberg::ManifestReader;
use compactor::metrics::CompactionMetrics;
use compactor::planner::{CompactionCandidate, PartitionStats};
use object_store::memory::InMemory;
use std::sync::Arc;
use tests_integration::generators::generate_trace_files_with_ids;
use writer::IcebergTableWriter;

const MILLIS_PER_HOUR: i64 = 3_600 * 1_000;

/// Deterministic xorshift64 stream rendered as hex, standing in for `rand`
/// (not a dependency of this crate). Real telemetry payloads are not
/// perfectly compressible; a short, per-row-constant `trace_id` (as the
/// shared fixture generator produces) would let zstd collapse the whole
/// partition to a handful of KB regardless of row count, which would make
/// the target-size assertion below meaningless. A high-entropy trace_id
/// keeps the compressed size roughly proportional to the row count, so the
/// test's roll-boundary math is predictable.
fn pseudo_random_hex(seed: u64, len: usize) -> String {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
    let mut out = String::with_capacity(len);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push_str(&format!("{state:016x}"));
    }
    out.truncate(len);
    out
}

/// Start of the hour, `hours_ago` hours before now, in epoch millis.
fn aligned_hour_start(hours_ago: i64) -> i64 {
    let now_ms = chrono::Utc::now().timestamp_millis();
    (now_ms / MILLIS_PER_HOUR - hours_ago) * MILLIS_PER_HOUR
}

/// Merging small files with a small target file size must produce several
/// files on the order of `total_encoded / target`, not one file far under
/// (or, as here, wildly over) target because the roll decision was made on
/// decoded bytes or an unset property instead of real encoded bytes.
#[tokio::test]
async fn compaction_rolls_output_files_at_the_real_encoded_target_size() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);
    let object_store = Arc::new(InMemory::new());

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "traces";

    let mut writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        tenant_id.to_string(),
        dataset_id.to_string(),
        table_name.to_string(),
    )
    .await
    .context("Failed to create writer")?;

    // A tiny target so the test runs fast: 200 KiB.
    let target_file_size_bytes: u64 = 200 * 1024;

    // Seed several input files' worth of high-entropy rows into one closed
    // hour partition. 4 files x 400 rows x an 800-byte trace_id is ~1.28 MB
    // of raw string data alone -- comfortably above the 512 MiB fork default
    // (proving the old behavior would have collapsed this to one file) and a
    // multiple of the 200 KiB target (proving the fix rolls more than once).
    let base_timestamp = aligned_hour_start(5);
    let rows_per_file = 400;
    let file_count = 4;
    let id_len = 800;
    let mut next_id = 0u64;
    let files: Vec<Vec<String>> = (0..file_count)
        .map(|_| {
            (0..rows_per_file)
                .map(|_| {
                    next_id += 1;
                    pseudo_random_hex(next_id, id_len)
                })
                .collect()
        })
        .collect();
    let total_rows = file_count * rows_per_file;

    generate_trace_files_with_ids(&mut writer, &files, base_timestamp)
        .await
        .context("Failed to seed high-entropy trace data")?;

    let manifest_reader = ManifestReader::new();
    let table_identifier =
        catalog_manager.build_table_identifier(tenant_id, dataset_id, table_name);
    let load_table = || async {
        match catalog_manager
            .catalog()
            .load_tabular(&table_identifier)
            .await
            .expect("Failed to load table")
        {
            iceberg_rust::catalog::tabular::Tabular::Table(t) => t,
            _ => panic!("Expected table"),
        }
    };

    let files_before = manifest_reader
        .get_snapshot_files(&load_table().await)
        .await?;
    assert_eq!(
        files_before.len(),
        file_count,
        "test setup must produce exactly the seeded input files"
    );
    let rows_before: u64 = files_before.iter().map(|f| f.record_count).sum();
    assert_eq!(rows_before, total_rows as u64);
    let target_hour = files_before[0]
        .partition_hours
        .expect("generated data file must carry a timestamp_hour partition value");

    let executor_config = ExecutorConfig {
        target_file_size_bytes,
        ..ExecutorConfig::default()
    };
    let metrics = CompactionMetrics::new();
    let executor = CompactionExecutor::new(catalog_manager.clone(), executor_config, metrics);

    let result = executor
        .execute_candidate(CompactionCandidate {
            tenant_id: tenant_id.to_string(),
            dataset_id: dataset_id.to_string(),
            table_name: table_name.to_string(),
            partition_id: target_hour.to_string(),
            stats: PartitionStats {
                file_count: files_before.len(),
                total_size_bytes: files_before.iter().map(|f| f.file_size_bytes).sum(),
                avg_file_size_bytes: 0,
            },
        })
        .await
        .context("Compaction execution returned a hard error")?;

    assert_eq!(
        result.status,
        CompactionStatus::Success,
        "compaction must succeed: {:?}",
        result.error
    );

    let files_after = manifest_reader
        .get_snapshot_files(&load_table().await)
        .await?;
    let rows_after: u64 = files_after.iter().map(|f| f.record_count).sum();
    assert_eq!(rows_after, rows_before, "compaction must not lose rows");

    let total_output_bytes: u64 = files_after.iter().map(|f| f.file_size_bytes).sum();
    tracing::info!(
        output_files = files_after.len(),
        total_output_bytes,
        target_file_size_bytes,
        "compacted output"
    );

    // Diagnostic: confirm the table property the writer actually reads was
    // reconciled to the configured target.
    let property_value = load_table()
        .await
        .metadata()
        .properties
        .get("write.target-file-size-bytes")
        .cloned();
    tracing::info!(
        ?property_value,
        "write.target-file-size-bytes after compaction"
    );
    assert_eq!(
        property_value,
        Some(target_file_size_bytes.to_string()),
        "table property must be reconciled to the configured target before the write"
    );

    // The core assertion this test exists for: the rewrite must have rolled
    // more than once. Before the fix, the writer always rolled at its
    // unset-property 512 MiB fallback, so a ~1 MB partition always collapsed
    // to exactly one output file regardless of `target_file_size_bytes`.
    assert!(
        files_after.len() > 1,
        "expected the ~{} bytes of encoded output to roll into several files at a {} byte \
         target, got a single {} byte file -- the writer is not honoring the real target",
        total_output_bytes,
        target_file_size_bytes,
        files_after.first().map(|f| f.file_size_bytes).unwrap_or(0)
    );

    // And it must be *approximating* the target, not merely "more than one":
    // every non-final file should be in the same order of magnitude as the
    // target rather than a small fraction of it (the failure mode a
    // decoded-bytes estimate produces, per the spec scenario "Output files
    // approximate the target size"). The writer only rolls between whole
    // incoming batches, so a couple of them landing just past the boundary is
    // expected slack -- allow up to 2x the target rather than requiring an
    // exact match.
    let mut sizes: Vec<u64> = files_after.iter().map(|f| f.file_size_bytes).collect();
    sizes.sort_unstable();
    // The largest file is the most reliable one to check: every file but
    // possibly the trailing partial one should have rolled at close to the
    // target, so the largest observed size is bounded above and must not be
    // a tiny sliver of the target either.
    let largest = *sizes.last().expect("at least one output file");
    assert!(
        largest <= target_file_size_bytes * 2,
        "largest output file ({largest} bytes) overshoots the {target_file_size_bytes} byte \
         target by more than 2x -- the writer is not rolling on real bytes"
    );
    assert!(
        largest >= target_file_size_bytes / 4,
        "largest output file ({largest} bytes) is far below the {target_file_size_bytes} byte \
         target -- output files are not approximating the target size"
    );

    Ok(())
}
