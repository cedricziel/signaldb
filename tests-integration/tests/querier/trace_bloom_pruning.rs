//! Verifies that a single-trace point lookup consults the Parquet bloom
//! filter on `trace_id` and skips row groups (here: whole data files, one row
//! group each) that cannot contain the target id, while still returning the
//! correct row.
//!
//! This is the read-side half of #826. The write side emits
//! `write.parquet.bloom-filter-enabled.column.trace_id = "true"` for the
//! traces table (see `common::schema::bloom_filter_properties_for_trace_columns`);
//! this test proves the filter is both written and consulted through the
//! pinned `datafusion_iceberg` provider — DataFusion's physical filter
//! pushdown injects the `trace_id` predicate into the scan even though the
//! provider reports `Inexact` and builds `ParquetSource` without a predicate.
//!
//! Crucially the trace ids are constructed so min/max statistics CANNOT prune:
//! every file shares the same low/high sentinel ids, so each file's
//! `[trace_id_min, trace_id_max]` brackets the target. The only structure that
//! can then skip a file's row group is its bloom filter — exactly the point of
//! #826 (real trace ids are random, so column statistics never prune a point
//! lookup).

use std::sync::Arc;

use anyhow::Result;
use common::catalog_manager::CatalogManager;
use datafusion::prelude::SessionContext;
use object_store::memory::InMemory;
use tests_integration::generators;
use writer::IcebergTableWriter;

/// Low/high sentinel ids present in EVERY file so that per-file `trace_id`
/// min/max always brackets the target — defeating statistics pruning.
const LOW_SENTINEL: &str = "00000000000000000000000000000000";
const HIGH_SENTINEL: &str = "ffffffffffffffffffffffffffffffff";

const NUM_FILES: usize = 5;
const TARGET_FILE: usize = 2;

/// A mid-range id (starts with `8`) that sorts between the sentinels and is
/// written into exactly one file.
const TARGET: &str = "88888888888888888888888888888888";

#[tokio::test]
async fn single_trace_lookup_prunes_row_groups_via_bloom_filter() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "traces";

    let config = common::testing::TestConfigBuilder::new()
        .in_memory()
        .with_tenant(tenant_id, dataset_id)
        .build();
    let catalog_manager = Arc::new(CatalogManager::new(config).await?);
    let object_store = Arc::new(InMemory::new());

    let mut writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        tenant_id.to_string(),
        dataset_id.to_string(),
        table_name.to_string(),
    )
    .await
    .expect("Failed to create Iceberg writer");

    // Build NUM_FILES files. Each holds the two sentinels plus filler ids in a
    // disjoint mid-range band (`{file}xxxx…`), and only TARGET_FILE also holds
    // TARGET. Every file's min/max = [LOW_SENTINEL, HIGH_SENTINEL] ⊇ TARGET, so
    // statistics can't prune; only the bloom filter can.
    let files: Vec<Vec<String>> = (0..NUM_FILES)
        .map(|file| {
            let mut ids = vec![LOW_SENTINEL.to_string(), HIGH_SENTINEL.to_string()];
            // Filler in a band that never equals TARGET: prefix with the file
            // index digit (1..=NUM_FILES) so bands are disjoint and none is `8`.
            let band = (file + 1) % 10;
            for i in 0..40 {
                ids.push(format!("{band}{i:031}"));
            }
            if file == TARGET_FILE {
                ids.push(TARGET.to_string());
            }
            ids
        })
        .collect();

    let base_ts = chrono::Utc::now().timestamp_millis() - (60 * 60 * 1000);
    generators::generate_trace_files_with_ids(&mut writer, &files, base_ts).await?;

    // Load the freshly-written table and expose it to DataFusion exactly as
    // the querier does (bare `DataFusionTable` over the catalog tabular).
    let identifier = catalog_manager.build_table_identifier(tenant_id, dataset_id, table_name);
    let tabular = catalog_manager
        .catalog()
        .load_tabular(&identifier)
        .await
        .expect("load table");
    let table = Arc::new(datafusion_iceberg::DataFusionTable::new(
        tabular, None, None, None,
    ));

    // Default session config — the real querier uses `SessionContext::new()`,
    // and `datafusion.execution.parquet.bloom_filter_on_read` defaults true.
    let ctx = SessionContext::new();
    ctx.register_table("traces", table)?;

    // Correctness: the lookup still returns exactly the target row.
    let batches = ctx
        .sql(&format!(
            "SELECT trace_id FROM traces WHERE trace_id = '{TARGET}'"
        ))
        .await?
        .collect()
        .await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1, "point lookup must return exactly the target span");

    // Pruning: read the analyzed plan and confirm the bloom filter — not
    // statistics — pruned the other files' row groups.
    let explain = ctx
        .sql(&format!(
            "EXPLAIN ANALYZE SELECT trace_id FROM traces WHERE trace_id = '{TARGET}'"
        ))
        .await?
        .collect()
        .await?;
    let rendered = datafusion::arrow::util::pretty::pretty_format_batches(&explain)?.to_string();
    println!("=== EXPLAIN ANALYZE ===\n{rendered}");

    let bloom = parse_pruning_metric(&rendered, "row_groups_pruned_bloom_filter")
        .expect("scan must expose a row_groups_pruned_bloom_filter metric");
    let stats =
        parse_pruning_metric(&rendered, "row_groups_pruned_statistics").unwrap_or(Pruning {
            total: 0,
            matched: 0,
        });

    // Statistics must be powerless here (sentinels defeat min/max): every row
    // group it evaluated it had to keep. This is the #826 premise — real trace
    // ids are random, so column min/max never prunes a point lookup.
    assert_eq!(
        stats.pruned(),
        0,
        "sentinel ids should make statistics pruning impossible, but it pruned {}",
        stats.pruned()
    );
    // The bloom filter must skip files that lack TARGET. We can't assert the
    // exact count (NUM_FILES - 1): bloom filters have false positives, so a
    // non-target file may occasionally still match. The load-bearing claim is
    // that the bloom filter prunes row groups statistics could not — i.e. at
    // least one, and strictly more than statistics managed (which was zero).
    assert!(
        bloom.pruned() >= 1,
        "bloom filter should prune ≥1 row group that statistics could not \
         (metric: {} total → {} matched, so {} pruned)",
        bloom.total,
        bloom.matched,
        bloom.pruned(),
    );
    assert!(
        bloom.pruned() > stats.pruned(),
        "bloom filter ({} pruned) should out-prune statistics ({} pruned) for a random-id point lookup",
        bloom.pruned(),
        stats.pruned(),
    );

    Ok(())
}

/// A DataFusion `PruningMetrics` value, rendered in `EXPLAIN ANALYZE` as
/// `"<name>=<total> total → <matched> matched"`. `pruned = total - matched`.
#[derive(Clone, Copy)]
struct Pruning {
    total: usize,
    matched: usize,
}

impl Pruning {
    fn pruned(&self) -> usize {
        self.total.saturating_sub(self.matched)
    }
}

/// Extracts a named `PruningMetrics` from the rendered analyzed plan.
fn parse_pruning_metric(plan: &str, name: &str) -> Option<Pruning> {
    let start = plan.find(&format!("{name}="))? + name.len() + 1;
    let rest = &plan[start..];
    // Format: "10 total → 1 matched" (arrow may be a unicode →).
    let total: usize = rest
        .split_whitespace()
        .next()?
        .trim_end_matches(|c: char| !c.is_ascii_digit())
        .parse()
        .ok()?;
    let matched_idx = rest.find("matched")?;
    let matched: usize = rest[..matched_idx]
        .split_whitespace()
        .rev()
        .find_map(|tok| tok.parse::<usize>().ok())?;
    Some(Pruning { total, matched })
}
