//! Compaction throughput benchmark.
//!
//! Seeds a table with several small Parquet files in one partition (via the
//! writer, in-memory), plans a compaction candidate, and times the executor's
//! rewrite of those files into a compacted set.
//!
//! Compaction MUTATES the table (it commits a new snapshot), so the measured
//! closure cannot simply loop — each run needs a freshly-seeded table. We use
//! `iter_custom` to re-seed per iteration OUTSIDE the timed region and measure
//! only `execute_candidate`. Sample size is lowered because each sample pays a
//! full seed.
//!
//! In-memory numbers: relative regression signal, not production throughput.

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::catalog_manager::CatalogManager;
use compactor::executor::{CompactionExecutor, CompactionStatus, ExecutorConfig};
use compactor::metrics::CompactionMetrics;
use compactor::planner::{CompactionCandidate, CompactionPlanner, PlannerConfig};
use criterion::{Criterion, criterion_group, criterion_main};
use object_store::memory::InMemory;
use tests_integration::fixtures::{DataGeneratorConfig, PartitionGranularity};
use tests_integration::generators;
use tokio::runtime::Runtime;
use writer::IcebergTableWriter;

const TENANT: &str = "compact-tenant";
const DATASET: &str = "compact-dataset";
const TABLE: &str = "traces";
const NUM_FILES: usize = 6;
const ROWS_PER_FILE: usize = 100;

/// Seed a table with `NUM_FILES` small files and return an executor plus the
/// planned candidate that targets them.
async fn seed_and_plan() -> (CompactionExecutor, CompactionCandidate) {
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

    // Each generate call with these dims is one small live file.
    let gen_config = DataGeneratorConfig {
        partition_count: 1,
        files_per_partition: 1,
        rows_per_file: ROWS_PER_FILE,
        base_timestamp: chrono::Utc::now().timestamp_millis() - (60 * 60 * 1000),
        partition_granularity: PartitionGranularity::Hour,
    };
    for _ in 0..NUM_FILES {
        generators::generate_traces(&mut writer, &gen_config)
            .await
            .expect("seed file");
    }

    let planner_config = PlannerConfig {
        file_count_threshold: 3,
        min_input_file_size_bytes: 100,
        max_files_per_job: 50,
        target_file_size_bytes: 128 * 1024 * 1024,
    };
    let planner = CompactionPlanner::new(catalog_manager.clone(), planner_config.clone());
    let candidate = planner
        .plan()
        .await
        .expect("plan compaction")
        .into_iter()
        .next()
        .expect("a compaction candidate");

    let executor = CompactionExecutor::new(
        catalog_manager.clone(),
        ExecutorConfig::from(&planner_config),
        CompactionMetrics::new(),
    );
    (executor, candidate)
}

fn bench_compaction(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("compactor");
    group.sample_size(10); // each sample re-seeds a table — keep it cheap

    group.bench_function(format!("rewrite_{NUM_FILES}_files"), |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let (executor, candidate) = rt.block_on(seed_and_plan());
                let start = Instant::now();
                let result = rt
                    .block_on(executor.execute_candidate(candidate))
                    .expect("run compaction");
                total += start.elapsed();
                assert!(
                    matches!(result.status, CompactionStatus::Success),
                    "compaction must succeed: {:?}",
                    result.error
                );
                black_box(&result);
            }
            total
        });
    });

    group.finish();
}

criterion_group!(benches, bench_compaction);
criterion_main!(benches);
