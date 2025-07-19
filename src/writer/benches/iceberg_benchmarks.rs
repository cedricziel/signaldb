use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use common::config::{Configuration, DefaultSchemas, SchemaConfig, StorageConfig};
use datafusion::arrow::array::{
    Date32Array, Float64Array, Int32Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use object_store::memory::InMemory;
use std::sync::Arc;
use tokio::runtime::Runtime;
use writer::create_iceberg_writer;

/// Create test configuration for benchmarking
fn create_benchmark_config() -> Configuration {
    let mut config = Configuration::default();
    config.schema = SchemaConfig {
        catalog_type: "memory".to_string(),
        catalog_uri: "memory://".to_string(),
        default_schemas: DefaultSchemas {
            traces_enabled: true,
            logs_enabled: true,
            metrics_enabled: true,
            custom_schemas: Default::default(),
        },
    };
    config.storage = StorageConfig {
        dsn: "memory://".to_string(),
    };
    config
}

/// Create test data for benchmarking with specified number of rows
fn create_benchmark_data(num_rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "start_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_description", DataType::Utf8, true),
        Field::new("metric_unit", DataType::Utf8, true),
        Field::new("value", DataType::Float64, false),
        Field::new("flags", DataType::Int32, true),
        Field::new("resource_schema_url", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_schema_url", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Utf8, true),
        Field::new("scope_dropped_attr_count", DataType::Int32, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("exemplars", DataType::Utf8, true),
        Field::new("date_day", DataType::Date32, false),
        Field::new("hour", DataType::Int32, false),
    ]));

    // Generate realistic test data
    let timestamps: Vec<i64> = (0..num_rows)
        .map(|i| 1_700_000_000_000_000_000 + (i as i64 * 1_000_000_000))
        .collect();
    let service_names: Vec<&str> = (0..num_rows).map(|_| "benchmark-service").collect();
    let metric_names: Vec<String> = (0..num_rows)
        .map(|i| format!("benchmark.metric.{}", i % 100)) // Simulate realistic cardinality
        .collect();
    let values: Vec<f64> = (0..num_rows).map(|i| (i as f64) * 1.5 + 10.0).collect();
    let hours: Vec<i32> = (0..num_rows).map(|i| (i % 24) as i32).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps)),
            Arc::new(TimestampNanosecondArray::from(vec![None; num_rows])),
            Arc::new(StringArray::from(service_names)),
            Arc::new(StringArray::from(
                metric_names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec![
                Some("Benchmark metric for performance testing");
                num_rows
            ])),
            Arc::new(StringArray::from(vec![Some("count"); num_rows])),
            Arc::new(Float64Array::from(values)),
            Arc::new(Int32Array::from(vec![None; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![
                Some("{\"service.version\":\"1.0\",\"host\":\"benchmark-host\"}");
                num_rows
            ])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(Int32Array::from(vec![None; num_rows])),
            Arc::new(StringArray::from(vec![
                Some("{\"metric.type\":\"gauge\",\"benchmark\":\"true\"}");
                num_rows
            ])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(Date32Array::from(vec![19700; num_rows])), // Fixed date for consistency
            Arc::new(Int32Array::from(hours)),
        ],
    )
    .expect("Failed to create benchmark data")
}

/// Benchmark single batch writes with varying sizes
fn bench_single_batch_writes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = create_benchmark_config();

    let mut group = c.benchmark_group("single_batch_writes");

    // Test different batch sizes
    for size in [100, 1_000, 10_000, 50_000].iter() {
        let batch = create_benchmark_data(*size);
        let batch_size_mb = (batch.get_array_memory_size() as f64) / (1024.0 * 1024.0);

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_rows_{:.1}MB", size, batch_size_mb)),
            size,
            |b, &_size| {
                b.to_async(&rt).iter(|| async {
                    let object_store = Arc::new(InMemory::new());
                    let mut writer = create_iceberg_writer(
                        &config,
                        object_store,
                        &format!("bench_tenant_{}", rand::random::<u32>()),
                        "metrics_gauge",
                    )
                    .await
                    .expect("Failed to create writer");

                    let batch_clone = batch.clone();
                    black_box(writer.write_batch(batch_clone).await.expect("Write failed"));
                });
            },
        );
    }
    group.finish();
}

/// Benchmark multi-batch transactional writes
fn bench_multi_batch_writes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = create_benchmark_config();

    let mut group = c.benchmark_group("multi_batch_writes");

    // Test different numbers of batches
    for num_batches in [2, 5, 10, 20].iter() {
        let batches: Vec<RecordBatch> = (0..*num_batches)
            .map(|_| create_benchmark_data(1_000))
            .collect();
        let total_rows = batches.len() * 1_000;

        group.throughput(Throughput::Elements(total_rows as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_batches_{}_rows", num_batches, total_rows)),
            num_batches,
            |b, &_num_batches| {
                b.to_async(&rt).iter(|| async {
                    let object_store = Arc::new(InMemory::new());
                    let mut writer = create_iceberg_writer(
                        &config,
                        object_store,
                        &format!("bench_tenant_{}", rand::random::<u32>()),
                        "metrics_gauge",
                    )
                    .await
                    .expect("Failed to create writer");

                    let batches_clone = batches.clone();
                    black_box(
                        writer
                            .write_batches(batches_clone)
                            .await
                            .expect("Write failed"),
                    );
                });
            },
        );
    }
    group.finish();
}

/// Benchmark transaction overhead
fn bench_transaction_overhead(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = create_benchmark_config();

    let mut group = c.benchmark_group("transaction_overhead");
    let batch = create_benchmark_data(1_000);

    // Benchmark immediate writes (no transaction)
    group.bench_function("immediate_write", |b| {
        b.to_async(&rt).iter(|| async {
            let object_store = Arc::new(InMemory::new());
            let mut writer = create_iceberg_writer(
                &config,
                object_store,
                &format!("bench_tenant_{}", rand::random::<u32>()),
                "metrics_gauge",
            )
            .await
            .expect("Failed to create writer");

            let batch_clone = batch.clone();
            black_box(writer.write_batch(batch_clone).await.expect("Write failed"));
        });
    });

    // Benchmark transactional writes
    group.bench_function("transactional_write", |b| {
        b.to_async(&rt).iter(|| async {
            let object_store = Arc::new(InMemory::new());
            let mut writer = create_iceberg_writer(
                &config,
                object_store,
                &format!("bench_tenant_{}", rand::random::<u32>()),
                "metrics_gauge",
            )
            .await
            .expect("Failed to create writer");

            let txn_id = writer.begin_transaction().await.expect("Begin failed");
            let batch_clone = batch.clone();
            writer.write_batch(batch_clone).await.expect("Write failed");
            black_box(
                writer
                    .commit_transaction(&txn_id)
                    .await
                    .expect("Commit failed"),
            );
        });
    });

    group.finish();
}

/// Benchmark writer creation and initialization
fn bench_writer_creation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = create_benchmark_config();

    c.bench_function("writer_creation", |b| {
        b.to_async(&rt).iter(|| async {
            let object_store = Arc::new(InMemory::new());
            black_box(
                create_iceberg_writer(
                    &config,
                    object_store,
                    &format!("bench_tenant_{}", rand::random::<u32>()),
                    "metrics_gauge",
                )
                .await
                .expect("Failed to create writer"),
            );
        });
    });
}

/// Benchmark concurrent writes (simulated)
fn bench_concurrent_writes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = create_benchmark_config();

    let mut group = c.benchmark_group("concurrent_writes");

    for num_writers in [2, 4, 8].iter() {
        let batch = create_benchmark_data(500); // Smaller batches for concurrent test

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_writers", num_writers)),
            num_writers,
            |b, &num_writers| {
                b.to_async(&rt).iter(|| async {
                    let mut handles = Vec::new();

                    for i in 0..num_writers {
                        let config = config.clone();
                        let batch = batch.clone();
                        let handle = tokio::spawn(async move {
                            let object_store = Arc::new(InMemory::new());
                            let mut writer = create_iceberg_writer(
                                &config,
                                object_store,
                                &format!("bench_tenant_{}_{}", i, rand::random::<u32>()),
                                "metrics_gauge",
                            )
                            .await
                            .expect("Failed to create writer");

                            writer.write_batch(batch).await.expect("Write failed");
                        });
                        handles.push(handle);
                    }

                    // Wait for all writers to complete
                    for handle in handles {
                        black_box(handle.await.expect("Task failed"));
                    }
                });
            },
        );
    }
    group.finish();
}

/// Benchmark memory usage patterns (simplified)
fn bench_memory_patterns(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = create_benchmark_config();

    let mut group = c.benchmark_group("memory_patterns");

    // Test batch size impact on memory
    for batch_size in [1_000, 10_000, 100_000].iter() {
        let batch = create_benchmark_data(*batch_size);
        let memory_mb = (batch.get_array_memory_size() as f64) / (1024.0 * 1024.0);

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_rows_{:.1}MB", batch_size, memory_mb)),
            batch_size,
            |b, &_batch_size| {
                b.to_async(&rt).iter(|| async {
                    let object_store = Arc::new(InMemory::new());
                    let mut writer = create_iceberg_writer(
                        &config,
                        object_store,
                        &format!("bench_tenant_{}", rand::random::<u32>()),
                        "metrics_gauge",
                    )
                    .await
                    .expect("Failed to create writer");

                    let batch_clone = batch.clone();
                    black_box(writer.write_batch(batch_clone).await.expect("Write failed"));
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
    bench_transaction_overhead,
    bench_writer_creation,
    bench_concurrent_writes,
    bench_memory_patterns
);
criterion_main!(benches);