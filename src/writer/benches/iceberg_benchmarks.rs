//! Iceberg writer append benchmarks.
//!
//! Times `IcebergTableWriter::append_batches_with_marker` — Parquet encode +
//! object-store put + Iceberg snapshot commit — against an in-memory catalog
//! and object store, so the numbers track CPU + commit-protocol cost, not
//! S3/disk latency.
//!
//! An append mutates the table (it commits a snapshot), so a single writer
//! cannot simply be re-used across iterations: the table would grow and
//! later iterations would pay for a bigger metadata tree. Each iteration
//! therefore gets a FRESH catalog + writer, created OUTSIDE the timed region
//! via `iter_custom`; only the append itself is measured. `writer/creation`
//! benches that setup cost on its own.
//!
//! `ingest_sort` isolates the one step the declared-ordering contract added
//! to every append: the columnar sort of a commit group by the table's sort
//! key, so the files written from it can attest that key. It is timed on its
//! own, on the same batches `single_batch_writes` appends, so the two can be
//! read against each other as "sort cost as a share of the append".
//!
//! Numbers are for relative regression tracking, not absolute production
//! throughput.

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::CatalogManager;
use common::catalog::Catalog;
use common::config::{Configuration, SchemaConfig, StorageConfig};
use common::flight::conversion::otlp_metrics_to_arrow;
use common::iceberg::sort::{canonical_sort_columns, sort_batch_by};
use common::schema::type_authority::TypeAuthority;
use common::schema_registry::SchemaResolver;
use common::testing::sample_metrics_request;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::arrow::array::{RecordBatch, UInt32Array};
use datafusion::arrow::compute::take_record_batch;
use tokio::runtime::Runtime;
use writer::IcebergTableWriter;
use writer::schema_transform::transform_metrics_to_wide;

/// Fresh-per-iteration setup is expensive (catalog + table create), so keep
/// samples low and warm-up short; Criterion still runs enough iterations per
/// sample to settle, and the per-iteration cost is known to be large enough
/// that a 3s warm-up would only repeat the expensive setup.
const SAMPLE_SIZE: usize = 10;
const WARM_UP: Duration = Duration::from_millis(500);

fn create_benchmark_config() -> Configuration {
    Configuration {
        schema: SchemaConfig {
            catalog_type: "memory".to_string(),
            catalog_uri: "memory://".to_string(),
            ..Default::default()
        },
        storage: StorageConfig {
            dsn: "memory://".to_string(),
        },
        ..Default::default()
    }
}

/// A fresh catalog + `metrics` (wide layout) writer under a unique tenant,
/// so no two iterations share table state.
async fn create_writer(config: &Configuration) -> IcebergTableWriter {
    let catalog_manager = CatalogManager::new(config.clone())
        .await
        .expect("Failed to create catalog manager");
    IcebergTableWriter::new(
        &catalog_manager,
        format!("bench_tenant_{}", uuid::Uuid::new_v4().simple()),
        "bench_dataset".to_string(),
        "metrics".to_string(),
    )
    .await
    .expect("Failed to create writer")
}

/// [`create_writer`] plus its own in-memory `TypeAuthority`, which an append
/// to the typed attribute layout requires.
async fn create_appending_writer(config: &Configuration) -> IcebergTableWriter {
    let sql_catalog = Catalog::new_in_memory()
        .await
        .expect("Failed to create type catalog");
    let type_authority = TypeAuthority::new(
        sql_catalog.clone(),
        SchemaResolver::new(sql_catalog),
        Arc::new(config.clone()),
    );
    create_writer(config)
        .await
        .with_type_authority(Arc::new(type_authority))
}

/// A `metrics` batch already in the wide STORED schema (so no wire->wide
/// transform runs inside the timed append) with `num_rows` rows and ~100
/// distinct metric names — realistic cardinality for a metrics table. Built
/// via the real wire conversion + wide transform, both run once here rather
/// than per benchmark iteration.
fn create_benchmark_data(num_rows: usize) -> RecordBatch {
    let request = sample_metrics_request(num_rows);
    let wire_batch = otlp_metrics_to_arrow(&request).expect("otlp -> wire batch");
    transform_metrics_to_wide(wire_batch, &[]).expect("wire -> wide batch")
}

/// Time only `append_batches_with_marker` of `batches`, giving each of the
/// `iters` runs a fresh writer created outside the measured window.
fn time_appends(
    rt: &Runtime,
    config: &Configuration,
    batches: &[RecordBatch],
    iters: u64,
) -> Duration {
    let mut total = Duration::ZERO;
    for _ in 0..iters {
        let mut writer = rt.block_on(create_appending_writer(config));
        let entries: Vec<_> = batches
            .iter()
            .cloned()
            .map(|batch| (uuid::Uuid::new_v4(), batch))
            .collect();
        let start = Instant::now();
        rt.block_on(writer.append_batches_with_marker("bench", entries))
            .expect("Write failed");
        total += start.elapsed();
        black_box(&writer);
    }
    total
}

/// Time only the concurrent appends of `num_writers` independent tenants,
/// each iteration getting `num_writers` fresh writers created outside the
/// measured window.
fn time_concurrent_appends(
    rt: &Runtime,
    config: &Configuration,
    batch: &RecordBatch,
    num_writers: usize,
    iters: u64,
) -> Duration {
    let mut total = Duration::ZERO;
    for _ in 0..iters {
        let writers: Vec<IcebergTableWriter> = (0..num_writers)
            .map(|_| rt.block_on(create_appending_writer(config)))
            .collect();

        let start = Instant::now();
        rt.block_on(async {
            let handles: Vec<_> = writers
                .into_iter()
                .map(|mut writer| {
                    let batch = batch.clone();
                    tokio::spawn(async move {
                        writer
                            .append_batches_with_marker(
                                "bench",
                                vec![(uuid::Uuid::new_v4(), batch)],
                            )
                            .await
                            .expect("Write failed");
                    })
                })
                .collect();
            for handle in handles {
                handle.await.expect("Task failed");
            }
        });
        total += start.elapsed();
    }
    total
}

/// One batch per commit, across batch sizes.
fn bench_single_batch_writes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = create_benchmark_config();

    let mut group = c.benchmark_group("single_batch_writes");
    group.sample_size(SAMPLE_SIZE);
    group.warm_up_time(WARM_UP);

    for size in [100, 1_000, 10_000, 100_000] {
        let batch = create_benchmark_data(size);
        let batch_size_mb = (batch.get_array_memory_size() as f64) / (1024.0 * 1024.0);

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{size}_rows_{batch_size_mb:.1}MB")),
            &batch,
            |b, batch| {
                b.iter_custom(|iters| {
                    time_appends(&rt, &config, std::slice::from_ref(batch), iters)
                });
            },
        );
    }
    group.finish();
}

/// Several batches committed in ONE verified snapshot.
fn bench_multi_batch_writes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = create_benchmark_config();

    let mut group = c.benchmark_group("multi_batch_writes");
    group.sample_size(SAMPLE_SIZE);
    group.warm_up_time(WARM_UP);

    for num_batches in [2, 5, 10, 20] {
        let batches: Vec<RecordBatch> = (0..num_batches)
            .map(|_| create_benchmark_data(1_000))
            .collect();
        let total_rows = batches.len() * 1_000;

        group.throughput(Throughput::Elements(total_rows as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{num_batches}_batches_{total_rows}_rows")),
            &batches,
            |b, batches| {
                b.iter_custom(|iters| time_appends(&rt, &config, batches, iters));
            },
        );
    }
    group.finish();
}

/// The setup cost the append benches exclude: catalog manager + table
/// load-or-create for a brand-new tenant.
fn bench_writer_creation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = create_benchmark_config();

    let mut group = c.benchmark_group("writer");
    group.sample_size(SAMPLE_SIZE);
    group.warm_up_time(WARM_UP);
    group.bench_function("creation", |b| {
        b.to_async(&rt)
            .iter(|| async { black_box(create_writer(&config).await) });
    });
    group.finish();
}

/// `num_writers` independent tenants appending concurrently — the writer
/// service's steady state under multi-tenant load. Writers are created
/// outside the timed region; only the concurrent appends are measured.
fn bench_concurrent_writes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = create_benchmark_config();

    let mut group = c.benchmark_group("concurrent_writes");
    group.sample_size(SAMPLE_SIZE);
    group.warm_up_time(WARM_UP);

    for num_writers in [2, 4, 8] {
        let batch = create_benchmark_data(500);

        group.throughput(Throughput::Elements(
            (num_writers * batch.num_rows()) as u64,
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{num_writers}_writers")),
            &num_writers,
            |b, &num_writers| {
                b.iter_custom(|iters| {
                    time_concurrent_appends(&rt, &config, &batch, num_writers, iters)
                });
            },
        );
    }
    group.finish();
}

/// `batch` with its rows in a fixed pseudo-random order, so the sort has
/// real work to do. Ingest's usual input is already close to time order
/// (`create_benchmark_data` is monotonic in `timestamp`), which is the
/// cheap case for the sort kernel; this is the expensive one.
fn shuffle_rows(batch: &RecordBatch) -> RecordBatch {
    let mut indices: Vec<u32> = (0..batch.num_rows() as u32).collect();
    // Fisher–Yates with a fixed-seed LCG: deterministic across runs, so
    // Criterion compares the same permutation against its baseline.
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    for i in (1..indices.len()).rev() {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let j = (state >> 33) as usize % (i + 1);
        indices.swap(i, j);
    }
    take_record_batch(batch, &UInt32Array::from(indices)).expect("permute rows")
}

/// The columnar sort ingest runs on a commit group before writing it.
fn bench_ingest_sort(c: &mut Criterion) {
    let key = canonical_sort_columns("metrics");
    assert!(!key.is_empty(), "metrics declares a sort key");

    let mut group = c.benchmark_group("ingest_sort");
    for size in [1_000, 10_000, 100_000] {
        let in_order = create_benchmark_data(size);
        let shuffled = shuffle_rows(&in_order);
        group.throughput(Throughput::Elements(size as u64));
        for (input, batch) in [("in_order", &in_order), ("shuffled", &shuffled)] {
            group.bench_with_input(BenchmarkId::new(input, size), batch, |b, batch| {
                b.iter(|| black_box(sort_batch_by(batch, &key).expect("sort group")));
            });
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_single_batch_writes,
    bench_multi_batch_writes,
    bench_writer_creation,
    bench_concurrent_writes,
    bench_ingest_sort
);
criterion_main!(benches);
