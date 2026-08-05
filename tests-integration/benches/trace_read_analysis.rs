//! Diagnostic for the trace point-lookup read path (not a Criterion bench).
//!
//! Baseline ANALYZE showed the lookup is dominated by `time_elapsed_opening`
//! (opening ~73 file footers across shattered hour-partitions) — pruning
//! already discards ~99% of row groups and `bytes_scanned` is a handful of
//! bytes. This dumps the stored schema and plans, then quantifies the levers
//! that reduce files opened: narrow projection (expected: no help),
//! time-window partition pruning, and compaction (fewer, larger files).

use std::sync::Arc;

use common::catalog_manager::CatalogManager;
use compactor::executor::{CompactionExecutor, CompactionStatus, ExecutorConfig};
use compactor::metrics::CompactionMetrics;
use compactor::planner::{CompactionPlanner, PlannerConfig};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use object_store::memory::InMemory;
use tests_integration::fixtures::{DataGeneratorConfig, PartitionGranularity};
use tests_integration::generators;
use writer::IcebergTableWriter;

const TENANT: &str = "bench-tenant";
const DATASET: &str = "bench-dataset";
const TABLE: &str = "traces";
const LOW_SENTINEL: &str = "00000000000000000000000000000000";
const HIGH_SENTINEL: &str = "ffffffffffffffffffffffffffffffff";
const TARGET: &str = "88888888888888888888888888888888";

/// Seed the dataset and return the catalog manager (so we can compact it) plus
/// the base timestamp in millis.
async fn seed() -> (Arc<CatalogManager>, i64) {
    let config = common::testing::TestConfigBuilder::new()
        .in_memory()
        .with_tenant(TENANT, DATASET)
        .build();
    let catalog_manager = Arc::new(CatalogManager::new(config).await.expect("catalog manager"));
    let object_store = Arc::new(InMemory::new());

    let mut writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        TENANT.to_string(),
        DATASET.to_string(),
        TABLE.to_string(),
    )
    .await
    .expect("create writer");

    let base_ts = chrono::Utc::now().timestamp_millis() - (3 * 24 * 60 * 60 * 1000);
    let gen_config = DataGeneratorConfig {
        partition_count: 3,
        files_per_partition: 4,
        rows_per_file: 1_000,
        base_timestamp: base_ts,
        partition_granularity: PartitionGranularity::Day,
    };
    generators::generate_traces(&mut writer, &gen_config)
        .await
        .expect("seed bulk traces");

    let files: Vec<Vec<String>> = (0..3)
        .map(|file| {
            let mut ids = vec![LOW_SENTINEL.to_string(), HIGH_SENTINEL.to_string()];
            let band = (file + 1) % 10;
            for i in 0..40 {
                ids.push(format!("{band}{i:031}"));
            }
            if file == 1 {
                ids.push(TARGET.to_string());
            }
            ids
        })
        .collect();
    generators::generate_trace_files_with_ids(&mut writer, &files, base_ts)
        .await
        .expect("seed known-id traces");

    (catalog_manager, base_ts)
}

/// Register the table's CURRENT state (call again after compaction).
async fn register(catalog_manager: &Arc<CatalogManager>) -> SessionContext {
    let identifier = catalog_manager.build_table_identifier(TENANT, DATASET, TABLE);
    let tabular = catalog_manager
        .catalog()
        .load_tabular(&identifier)
        .await
        .expect("load table");
    let table = Arc::new(datafusion_iceberg::DataFusionTable::new(
        tabular, None, None, None,
    ));
    let ctx = SessionContext::new();
    ctx.register_table(TABLE, table).expect("register table");
    ctx
}

/// Compact every partition of the seeded table.
async fn compact(catalog_manager: &Arc<CatalogManager>) {
    let planner_config = PlannerConfig {
        file_count_threshold: 2,
        max_input_file_size_bytes: u64::MAX, // include every file
        target_file_size_bytes: 128 * 1024 * 1024,
        // Tests seed data into recent hours and compact it immediately;
        // a production lateness allowance would defer every such partition.
        partition_lateness: std::time::Duration::ZERO,
    };
    let planner = CompactionPlanner::new(catalog_manager.clone(), planner_config.clone());
    let candidates = planner.plan().await.expect("plan compaction");
    println!("compaction: {} candidate partition(s)", candidates.len());
    let executor = CompactionExecutor::new(
        catalog_manager.clone(),
        ExecutorConfig::from(&planner_config),
        CompactionMetrics::new(),
    );
    for candidate in candidates {
        let result = executor
            .execute_candidate(candidate)
            .await
            .expect("run compaction");
        assert!(matches!(result.status, CompactionStatus::Success));
    }
}

async fn dump(ctx: &SessionContext, title: &str, sql: &str) {
    println!("\n================ {title} ================");
    println!("SQL: {sql}");
    match ctx.sql(sql).await {
        Ok(df) => match df.collect().await {
            Ok(batches) => match pretty_format_batches(&batches) {
                Ok(t) => println!("{t}"),
                Err(e) => println!("(format error: {e})"),
            },
            Err(e) => println!("(execution error: {e})"),
        },
        Err(e) => println!("(planning error: {e})"),
    }
}

fn main() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (catalog_manager, base_ts) = seed().await;
        let ctx = register(&catalog_manager).await;

        if let Ok(df) = ctx.sql(&format!("SELECT * FROM {TABLE} LIMIT 0")).await {
            println!("================ STORED SCHEMA ================");
            for f in df.schema().fields() {
                println!("  {:30} {:?}", f.name(), f.data_type());
            }
        }

        let lookup = format!("SELECT * FROM {TABLE} WHERE trace_id = '{TARGET}'");
        dump(&ctx, "EXPLAIN (logical + physical)", &format!("EXPLAIN {lookup}")).await;
        dump(&ctx, "ANALYZE: baseline SELECT *", &format!("EXPLAIN ANALYZE {lookup}")).await;

        // Lever A: narrow projection — expected NOT to help (cost is opening).
        let projected = format!(
            "SELECT trace_id, span_id, start_time_unix_nano, span_name FROM {TABLE} WHERE trace_id = '{TARGET}'"
        );
        dump(&ctx, "ANALYZE: projected columns", &format!("EXPLAIN ANALYZE {projected}")).await;

        // Lever B: time-window predicate → hour-partition pruning. `timestamp`
        // is Timestamp(Microsecond); base_ts is millis → *1000 to micros.
        let base_us = base_ts * 1_000;
        let (lo, hi) = (base_us - 3_600_000_000, base_us + 3_600_000_000);
        let windowed = format!(
            "SELECT * FROM {TABLE} WHERE trace_id = '{TARGET}' \
             AND timestamp BETWEEN to_timestamp_micros({lo}) AND to_timestamp_micros({hi})"
        );
        dump(&ctx, "ANALYZE: + time window (partition pruning)", &format!("EXPLAIN ANALYZE {windowed}")).await;

        // Lever C: compact, then re-run the baseline lookup on fewer files.
        compact(&catalog_manager).await;
        let ctx2 = register(&catalog_manager).await;
        dump(&ctx2, "ANALYZE: baseline SELECT * AFTER compaction", &format!("EXPLAIN ANALYZE {lookup}")).await;
    });
}
