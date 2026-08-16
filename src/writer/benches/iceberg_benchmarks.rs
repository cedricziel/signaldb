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
//! Numbers are for relative regression tracking, not absolute production
//! throughput.

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::CatalogManager;
use common::config::{Configuration, DefaultSchemas, SchemaConfig, StorageConfig};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::arrow::array::{
    Date32Array, Float64Array, Int32Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use tokio::runtime::Runtime;
use writer::IcebergTableWriter;
use writer::schema_transform::create_metrics_gauge_arrow_schema;

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
            default_schemas: DefaultSchemas {
                traces_enabled: true,
                logs_enabled: true,
                metrics_enabled: true,
                profiles_enabled: true,
                custom_schemas: Default::default(),
            },
            materialized_labels: Default::default(),
        },
        storage: StorageConfig {
            dsn: "memory://".to_string(),
        },
        ..Default::default()
    }
}

/// A fresh catalog + `metrics_gauge` writer under a unique tenant, so no two
/// iterations share table state.
async fn create_writer(config: &Configuration) -> IcebergTableWriter {
    let catalog_manager = CatalogManager::new(config.clone())
        .await
        .expect("Failed to create catalog manager");
    let object_store = Arc::new(object_store::memory::InMemory::new());
    IcebergTableWriter::new(
        &catalog_manager,
        object_store,
        format!("bench_tenant_{}", uuid::Uuid::new_v4().simple()),
        "bench_dataset".to_string(),
        "metrics_gauge".to_string(),
    )
    .await
    .expect("Failed to create writer")
}

/// A `metrics_gauge` batch in the STORED schema (so no v1->v2 transform runs
/// inside the timed append) with `num_rows` rows and ~100 distinct metric
/// names — realistic cardinality for a gauge table.
fn create_benchmark_data(num_rows: usize) -> RecordBatch {
    let timestamps: Vec<i64> = (0..num_rows)
        .map(|i| 1_700_000_000_000_000_000 + (i as i64 * 1_000_000_000))
        .collect();
    let service_names: Vec<&str> = (0..num_rows).map(|_| "benchmark-service").collect();
    let metric_names: Vec<String> = (0..num_rows)
        .map(|i| format!("benchmark.metric.{}", i % 100))
        .collect();
    let values: Vec<f64> = (0..num_rows).map(|i| (i as f64) * 1.5 + 10.0).collect();
    let hours: Vec<i32> = (0..num_rows).map(|i| (i % 24) as i32).collect();

    RecordBatch::try_new(
        create_metrics_gauge_arrow_schema(),
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps)),
            Arc::new(TimestampNanosecondArray::from(vec![None; num_rows])),
            Arc::new(StringArray::from(service_names)),
            Arc::new(StringArray::from(
                metric_names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec![
                Some(
                    "Benchmark metric for performance testing"
                );
                num_rows
            ])),
            Arc::new(StringArray::from(vec![Some("count"); num_rows])),
            Arc::new(Float64Array::from(values)),
            Arc::new(Int32Array::from(vec![None; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![
                Some(
                    "{\"service.version\":\"1.0\",\"host\":\"benchmark-host\"}"
                );
                num_rows
            ])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(Int32Array::from(vec![None; num_rows])),
            Arc::new(StringArray::from(vec![
                Some(
                    "{\"metric.type\":\"gauge\",\"benchmark\":\"true\"}"
                );
                num_rows
            ])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(Date32Array::from(vec![19700; num_rows])),
            Arc::new(Int32Array::from(hours)),
        ],
    )
    .expect("Failed to create benchmark data")
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
        let mut writer = rt.block_on(create_writer(config));
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
            .map(|_| rt.block_on(create_writer(config)))
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

criterion_group!(
    benches,
    bench_single_batch_writes,
    bench_multi_batch_writes,
    bench_writer_creation,
    bench_concurrent_writes
);
criterion_main!(benches);
