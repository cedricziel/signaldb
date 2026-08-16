//! Read-path benchmarks for the querier surface the UI exercises.
//!
//! Seeds a realistic in-memory dataset via the writer (memory catalog +
//! `InMemory` object store, exactly as `tests/querier/trace_bloom_pruning.rs`
//! does), registers the resulting Iceberg table with DataFusion the same way
//! the querier does, then times the two trace read paths the Explore UI hits:
//!
//! - **trace lookup by id** — opening a single trace (`WHERE trace_id = ?`)
//! - **trace search / groups** — listing recent trace groups (`DISTINCT trace_id`)
//!
//! Numbers are in-memory, so they measure query CPU + scan + pruning cost, not
//! S3/disk latency. They are for relative regression tracking ("are we making
//! it worse"), not absolute production latency.

use std::hint::black_box;
use std::sync::Arc;

use common::catalog_manager::CatalogManager;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::{Array, TimestampMicrosecondArray};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use object_store::memory::InMemory;
use tests_integration::fixtures::{DataGeneratorConfig, PartitionGranularity};
use tests_integration::generators::{
    self, BLOOM_HIGH_SENTINEL, BLOOM_LOW_SENTINEL, BLOOM_TARGET_TRACE_ID,
};
use tokio::runtime::Runtime;
use writer::IcebergTableWriter;

/// One hour in microseconds (the `timestamp` column is Timestamp(Microsecond)).
const HOUR_US: i64 = 3_600_000_000;

const TENANT: &str = "bench-tenant";
const DATASET: &str = "bench-dataset";
const TABLE: &str = "traces";

/// The bloom-pruning point-lookup target, so the lookup exercises a real
/// random-id point lookup (see `generators::generate_bloom_prunable_trace_files`).
const TARGET: &str = BLOOM_TARGET_TRACE_ID;
/// Spans of TARGET, one per spread instant — the completeness ground truth.
const TARGET_SPAN_COUNT: usize = 3;

/// Seed a traces table and hand back a DataFusion context with it registered,
/// plus the base timestamp (millis) used for seeding so the windowed lookup can
/// build a bounded time predicate.
async fn seed_traces_context() -> (SessionContext, i64) {
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

    // Bulk data across a few day partitions for realistic scan volume.
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

    // Filler files with sentinel + banded ids (no TARGET) at base_ts, for
    // realistic bloom/statistics conditions around the target.
    generators::generate_bloom_prunable_trace_files(&mut writer, 3, None, base_ts)
        .await
        .expect("seed filler traces");

    // SPREAD TARGET: one span in each of 3 distinct hour-partitions across ~2
    // days. No single narrow time window can capture the whole trace — this is
    // the case that defeats windowing and motivates the point index.
    for offset_ms in [0_i64, 25 * 3_600_000, 50 * 3_600_000] {
        generators::generate_trace_files_with_ids(
            &mut writer,
            &[vec![
                BLOOM_LOW_SENTINEL.to_string(),
                BLOOM_HIGH_SENTINEL.to_string(),
                TARGET.to_string(),
            ]],
            base_ts + offset_ms,
        )
        .await
        .expect("seed spread target span");
    }

    // Register the freshly-written table exactly as the querier does.
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
    (ctx, base_ts)
}

/// Build the point index `trace_id -> {distinct hour buckets}` as a small
/// in-memory table. In production this is a narrow Iceberg table sorted/bloomed
/// on `trace_id` and maintained by the writer/compactor; a `MemTable` stands in
/// for "the index is tiny and resident" so the bench isolates the step-2 win.
async fn build_trace_index(ctx: &SessionContext) {
    let batches = ctx
        .sql("SELECT DISTINCT trace_id, date_trunc('hour', timestamp) AS ts_hour FROM traces")
        .await
        .expect("plan index build")
        .collect()
        .await
        .expect("run index build");
    let schema = batches.first().map(|b| b.schema()).expect("index has rows");
    let mem = MemTable::try_new(schema, vec![batches]).expect("build index memtable");
    ctx.register_table("trace_index", Arc::new(mem))
        .expect("register index");
}

/// Id-only lookup via the point index, with NO time input from the caller:
/// (1) read the trace's discrete hour buckets from the index, (2) scan only
/// those partitions (OR of narrow `timestamp` ranges → Iceberg partition
/// pruning). Returns the span count so callers can assert completeness.
async fn lookup_via_index(ctx: &SessionContext) -> usize {
    // Step 1: the tiny point index → the exact hours this trace touches.
    let idx = ctx
        .sql(&format!(
            "SELECT ts_hour FROM trace_index WHERE trace_id = '{TARGET}'"
        ))
        .await
        .expect("plan index lookup")
        .collect()
        .await
        .expect("run index lookup");
    let mut buckets: Vec<i64> = Vec::new();
    for b in &idx {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("ts_hour is Timestamp(us)");
        for i in 0..col.len() {
            if col.is_valid(i) {
                buckets.push(col.value(i));
            }
        }
    }

    // Step 2: scan ONLY those hour partitions — one narrow range per bucket.
    let ranges: Vec<String> = buckets
        .iter()
        .map(|h| {
            format!(
                "(timestamp >= to_timestamp_micros({h}) AND timestamp < to_timestamp_micros({}))",
                h + HOUR_US
            )
        })
        .collect();
    let pred = if ranges.is_empty() {
        "false".to_string()
    } else {
        ranges.join(" OR ")
    };
    let batches = ctx
        .sql(&format!(
            "SELECT * FROM {TABLE} WHERE trace_id = '{TARGET}' AND ({pred})"
        ))
        .await
        .expect("plan index-guided scan")
        .collect()
        .await
        .expect("run index-guided scan");
    batches.iter().map(|b| b.num_rows()).sum()
}

fn bench_read_paths(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (ctx, base_ts) = rt.block_on(seed_traces_context());
    rt.block_on(build_trace_index(&ctx));

    // A ±1h window (in microseconds) around the FIRST seeded instant — the
    // bounded range a naive time-scoped lookup would use. `timestamp` is
    // Timestamp(Microsecond); base_ts is millis, so *1000 to micros.
    let base_us = base_ts * 1_000;
    let (win_lo, win_hi) = (base_us - HOUR_US, base_us + HOUR_US);

    // Completeness contrast, computed once: the spread trace has spans in 3
    // hours across ~2 days. Full scan = ground truth; a ±1h window MISSES the
    // far spans; the index path returns them ALL.
    let (full_rows, windowed_rows, index_rows) = rt.block_on(async {
        let count = |sql: String| {
            let ctx = &ctx;
            async move {
                let b = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
                b.iter().map(|x| x.num_rows()).sum::<usize>()
            }
        };
        let full = count(format!("SELECT * FROM {TABLE} WHERE trace_id = '{TARGET}'")).await;
        let windowed = count(format!(
            "SELECT * FROM {TABLE} WHERE trace_id = '{TARGET}' \
             AND timestamp BETWEEN to_timestamp_micros({win_lo}) AND to_timestamp_micros({win_hi})"
        ))
        .await;
        (full, windowed, lookup_via_index(&ctx).await)
    });
    assert_eq!(
        full_rows, TARGET_SPAN_COUNT,
        "spread target should have {TARGET_SPAN_COUNT} spans"
    );
    assert_eq!(
        index_rows, full_rows,
        "index path must return the COMPLETE spread trace"
    );
    eprintln!(
        "completeness: full-scan={full_rows}  ±1h-window={windowed_rows} (INCOMPLETE)  via-index={index_rows} (COMPLETE)"
    );

    let mut group = c.benchmark_group("querier_read");

    // Open a single trace WITHOUT a time filter. This scans every hour
    // partition (opens every file footer) — the cost of an unbounded lookup,
    // kept as the upper bound / regression guard against losing time pruning.
    group.bench_function("trace_lookup_by_id_unbounded", |b| {
        b.to_async(&rt).iter(|| async {
            let batches = ctx
                .sql(&format!(
                    "SELECT * FROM {TABLE} WHERE trace_id = '{TARGET}'"
                ))
                .await
                .expect("plan lookup")
                .collect()
                .await
                .expect("run lookup");
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            black_box(rows);
        });
    });

    // Open a single trace WITH a fixed ±1h window. Fast (prunes partitions) but
    // INCOMPLETE for a spread trace — it only finds spans near `base_ts`. Kept
    // to show that speed-via-windowing sacrifices correctness.
    group.bench_function("trace_lookup_by_id_windowed", |b| {
        b.to_async(&rt).iter(|| async {
            let batches = ctx
                .sql(&format!(
                    "SELECT * FROM {TABLE} WHERE trace_id = '{TARGET}' \
                     AND timestamp BETWEEN to_timestamp_micros({win_lo}) \
                     AND to_timestamp_micros({win_hi})"
                ))
                .await
                .expect("plan windowed lookup")
                .collect()
                .await
                .expect("run windowed lookup");
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            black_box(rows);
        });
    });

    // Id-only lookup via the point index: no time input, and returns the
    // COMPLETE spread trace. Fast because step 1 is a tiny index and step 2
    // scans only the discrete hour partitions the trace actually touches.
    group.bench_function("trace_lookup_by_id_via_index", |b| {
        b.to_async(&rt).iter(|| async {
            let rows = lookup_via_index(&ctx).await;
            black_box(rows);
        });
    });

    // List recent trace groups: full scan + distinct, as the trace list does.
    group.bench_function("trace_search_groups", |b| {
        b.to_async(&rt).iter(|| async {
            let batches = ctx
                .sql(&format!("SELECT DISTINCT trace_id FROM {TABLE} LIMIT 20"))
                .await
                .expect("plan search")
                .collect()
                .await
                .expect("run search");
            black_box(batches);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_read_paths);
criterion_main!(benches);
