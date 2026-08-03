//! Read-path benchmarks for the logs and metrics surfaces the UI exercises.
//!
//! Seeds a realistic in-memory dataset via the writer (memory catalog +
//! `InMemory` object store, exactly as `querier_read_paths.rs` does), registers
//! the resulting Iceberg tables with DataFusion the same way the querier does,
//! then times two representative read paths:
//!
//! - **logs filter** — a `service_name` equality plus a `body LIKE` substring
//!   over a time window (`logs` table)
//! - **metrics aggregation** — a grouped aggregate over a timestamp window
//!   (`metrics_gauge` table)
//!
//! IMPORTANT: these are **scan/aggregation proxies**, NOT the real LogQL/PromQL
//! engine. The querier's `query` module (LogQL line-filter pipelines, PromQL
//! range evaluation) is private and not exercised here. Instead we run raw
//! DataFusion SQL that mirrors the *shape* of the work those engines do — a
//! substring line filter for LogQL, a windowed group-by aggregate for a PromQL
//! range query — so the numbers track scan + filter + aggregation cost, not the
//! engine's lowering/planning cost.
//!
//! Numbers are in-memory, so they measure query CPU + scan cost, not S3/disk
//! latency. They are for relative regression tracking ("are we making it
//! worse"), not absolute production latency.

use std::hint::black_box;
use std::sync::Arc;

use common::catalog_manager::CatalogManager;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::prelude::SessionContext;
use object_store::memory::InMemory;
use tests_integration::fixtures::{DataGeneratorConfig, PartitionGranularity};
use tests_integration::generators;
use tokio::runtime::Runtime;
use writer::IcebergTableWriter;

const TENANT: &str = "bench-tenant";
const DATASET: &str = "bench-dataset";

/// Stored table names (verified against `apply_schema_transformation_if_needed`
/// in `src/writer/src/storage/iceberg.rs` and the compactor/writer integration
/// tests): logs land in `logs`, gauge metrics in `metrics_gauge`.
const LOGS_TABLE: &str = "logs";
const METRICS_TABLE: &str = "metrics_gauge";

/// One day in microseconds. The stored `timestamp` column is coerced to the
/// Iceberg microsecond precision on write, so time predicates use
/// `to_timestamp_micros`.
const DAY_US: i64 = 24 * 3_600_000_000;

/// Seeding shape shared by both signals: 3 day-partitions, 4 files each,
/// 1_000 rows per file → 12k rows per table across 3 days of realistic scan
/// volume.
const PARTITION_COUNT: usize = 3;
const FILES_PER_PARTITION: usize = 4;
const ROWS_PER_FILE: usize = 1_000;

/// Seed `logs` + `metrics_gauge` into one in-memory catalog/object store and
/// hand back a DataFusion context with both tables registered, plus the base
/// timestamp (epoch millis) used for seeding so callers can build bounded time
/// predicates.
async fn seed_signals_context() -> (SessionContext, i64) {
    let config = common::testing::TestConfigBuilder::new()
        .in_memory()
        .with_tenant(TENANT, DATASET)
        .build();
    let catalog_manager = Arc::new(CatalogManager::new(config).await.expect("catalog manager"));
    let object_store = Arc::new(InMemory::new());

    // Bulk data across a few day partitions, starting 3 days ago.
    let base_ts = chrono::Utc::now().timestamp_millis() - (PARTITION_COUNT as i64 * DAY_US / 1_000);
    let gen_config = DataGeneratorConfig {
        partition_count: PARTITION_COUNT,
        files_per_partition: FILES_PER_PARTITION,
        rows_per_file: ROWS_PER_FILE,
        base_timestamp: base_ts,
        partition_granularity: PartitionGranularity::Day,
    };

    let mut logs_writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        TENANT.to_string(),
        DATASET.to_string(),
        LOGS_TABLE.to_string(),
    )
    .await
    .expect("create logs writer");
    generators::generate_logs(&mut logs_writer, &gen_config)
        .await
        .expect("seed logs");

    let mut metrics_writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        TENANT.to_string(),
        DATASET.to_string(),
        METRICS_TABLE.to_string(),
    )
    .await
    .expect("create metrics writer");
    generators::generate_metrics(&mut metrics_writer, &gen_config)
        .await
        .expect("seed metrics");

    // Register both freshly-written tables exactly as the querier does.
    let ctx = SessionContext::new();
    for table in [LOGS_TABLE, METRICS_TABLE] {
        let identifier = catalog_manager.build_table_identifier(TENANT, DATASET, table);
        let tabular = catalog_manager
            .catalog()
            .load_tabular(&identifier)
            .await
            .expect("load table");
        let provider = Arc::new(datafusion_iceberg::DataFusionTable::new(
            tabular, None, None, None,
        ));
        ctx.register_table(table, provider).expect("register table");
    }

    (ctx, base_ts)
}

/// Count rows returned by a query (drains the batches).
async fn count_rows(ctx: &SessionContext, sql: &str) -> usize {
    let batches = ctx
        .sql(sql)
        .await
        .expect("plan query")
        .collect()
        .await
        .expect("run query");
    batches.iter().map(|b| b.num_rows()).sum()
}

fn bench_signal_read(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (ctx, base_ts) = rt.block_on(seed_signals_context());

    // Window covering the full 3-day seeded span. `timestamp` is stored at
    // microsecond precision; `base_ts` is millis, so *1000 to micros.
    let base_us = base_ts * 1_000;
    let (win_lo, win_hi) = (base_us, base_us + PARTITION_COUNT as i64 * DAY_US);

    // LogQL line-filter PROXY: service label equality + a `body` substring
    // (`|= "..."`) over the time window. `service_name` cycles service-0..4;
    // `body` is "Log message {i} from partition {p} file {f}", so the substring
    // "partition 1" selects the middle day-partition's files.
    let logs_sql = format!(
        "SELECT * FROM {LOGS_TABLE} \
         WHERE service_name = 'service-0' \
         AND body LIKE '%partition 1%' \
         AND timestamp BETWEEN to_timestamp_micros({win_lo}) AND to_timestamp_micros({win_hi})"
    );

    // PromQL range-query PROXY: an aggregate over the value column grouped by a
    // label (`service_name`), scoped to one metric over the time window — the
    // shape of `avg by (service_name) (cpu_usage[3d])`.
    let metrics_sql = format!(
        "SELECT service_name, avg(value) AS avg_value, count(*) AS samples \
         FROM {METRICS_TABLE} \
         WHERE metric_name = 'cpu_usage' \
         AND timestamp BETWEEN to_timestamp_micros({win_lo}) AND to_timestamp_micros({win_hi}) \
         GROUP BY service_name"
    );

    // Sanity: both proxy queries must actually hit rows, else the bench would
    // silently time an empty scan.
    let (log_hits, metric_groups) = rt.block_on(async {
        let logs = count_rows(&ctx, &logs_sql).await;
        let metrics = count_rows(&ctx, &metrics_sql).await;
        (logs, metrics)
    });
    assert!(log_hits > 0, "logs filter proxy matched no rows");
    assert!(
        metric_groups > 0,
        "metrics aggregation proxy produced no groups"
    );
    eprintln!("seeded: logs_filter_hits={log_hits}  metric_groups={metric_groups}");

    let mut group = c.benchmark_group("signal_read");

    // Logs: filter by service_name + body substring over a time window.
    group.bench_function("logs_filter_line_proxy", |b| {
        b.to_async(&rt).iter(|| async {
            let rows = count_rows(&ctx, &logs_sql).await;
            black_box(rows);
        });
    });

    // Metrics: windowed group-by aggregate over one metric.
    group.bench_function("metrics_range_aggregation_proxy", |b| {
        b.to_async(&rt).iter(|| async {
            let rows = count_rows(&ctx, &metrics_sql).await;
            black_box(rows);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_signal_read);
criterion_main!(benches);
