use common::CatalogManager;
use common::config::{Configuration, DefaultSchemas, SchemaConfig, StorageConfig};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::{
    Date32Array, Float64Array, Int32Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;
use writer::IcebergTableWriter;

/// Create test configuration for benchmarking
fn create_benchmark_config() -> Configuration {
    Configuration {
        schema: SchemaConfig {
            catalog_type: "memory".to_string(),
            catalog_uri: "memory://".to_string(),
            default_schemas: DefaultSchemas {
                traces_enabled: true,
                logs_enabled: true,
                metrics_enabled: true,
                custom_schemas: Default::default(),
            },
        },
        storage: StorageConfig {
            dsn: "memory://".to_string(),
        },
        ..Default::default()
    }
}

async fn create_writer(config: &Configuration, tenant_id: String) -> IcebergTableWriter {
    let catalog_manager = CatalogManager::new(config.clone())
        .await
        .expect("Failed to create catalog manager");
    let object_store = Arc::new(object_store::memory::InMemory::new());
    IcebergTableWriter::new(
        &catalog_manager,
        object_store,
        tenant_id,
        "bench_dataset".to_string(),
        "metrics_gauge".to_string(),
    )
    .await
    .expect("Failed to create writer")
}

/// Create test data for benchmarking
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

    let timestamps: Vec<i64> = (0..num_rows)
        .map(|i| 1_700_000_000_000_000_000 + (i as i64 * 1_000_000_000))
        .collect();
    let service_names: Vec<&str> = (0..num_rows).map(|_| "benchmark-service").collect();
    let metric_names: Vec<String> = (0..num_rows)
        .map(|i| format!("benchmark.metric.{}", i % 10))
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
            Arc::new(StringArray::from(vec![Some("Benchmark metric"); num_rows])),
            Arc::new(StringArray::from(vec![Some("count"); num_rows])),
            Arc::new(Float64Array::from(values)),
            Arc::new(Int32Array::from(vec![None; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![
                Some("{\"service.version\":\"1.0\"}");
                num_rows
            ])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(Int32Array::from(vec![None; num_rows])),
            Arc::new(StringArray::from(vec![
                Some("{\"metric.type\":\"gauge\"}");
                num_rows
            ])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(Date32Array::from(vec![19700; num_rows])),
            Arc::new(Int32Array::from(hours)),
        ],
    )
    .expect("Failed to create benchmark data")
}

/// Benchmark writer creation
fn bench_writer_creation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = create_benchmark_config();

    c.bench_function("writer_creation", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(create_writer(&config, format!("tenant_{}", rand::random::<u32>())).await);
        });
    });
}

/// Benchmark concurrent writer creation performance
fn bench_concurrent_writer_creation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = create_benchmark_config();

    let mut group = c.benchmark_group("concurrent_writer_creation");

    for &num_writers in &[2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_writers),
            &num_writers,
            |b, &num_writers| {
                b.to_async(&rt).iter(|| async {
                    let mut handles = Vec::new();
                    for i in 0..num_writers {
                        let config = config.clone();
                        let handle = tokio::spawn(async move {
                            create_writer(
                                &config,
                                format!("tenant_{}_{}", i, rand::random::<u32>()),
                            )
                            .await
                        });
                        handles.push(handle);
                    }

                    for handle in handles {
                        let _ = black_box(handle.await.expect("Task failed"));
                    }
                });
            },
        );
    }
    group.finish();
}

/// Benchmark write performance
fn bench_write_performance(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = create_benchmark_config();
    let batch = create_benchmark_data(1000);

    c.bench_function("write_batch", |b| {
        b.to_async(&rt).iter(|| async {
            let mut writer =
                create_writer(&config, format!("tenant_{}", rand::random::<u32>())).await;

            let batch_clone = batch.clone();
            writer.write_batch(batch_clone).await.expect("Write failed");
            black_box(());
        });
    });
}

criterion_group!(
    benches,
    bench_writer_creation,
    bench_concurrent_writer_creation,
    bench_write_performance
);
criterion_main!(benches);
